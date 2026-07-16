//! The causal rewrite modes: `reconstruct` (product → reconstruction control) and
//! `vary` (reconstruction → one-variable child).
//!
//! `reconstruct` rebuilds the product's logical rows through the laboratory writer
//! under a resolved baseline policy, so rewrite bias (row-group boundaries, library
//! defaults, the ZSTD-1 rewrite level) is measured *once*, on its own, rather than
//! smuggled into every variant. `vary` then changes exactly one physical axis on top
//! of that reconstruction and records a delta whose resolved config differs from the
//! parent's only on the declared allowlist — the structural-diff gate the contract
//! enforces before any timing.

use std::collections::BTreeMap;
use std::path::Path;

use anyhow::{Context, Result, bail};
use parquet_lab_contract::{
    FileDigest, ReconstructionManifest, ResolvedConfig, RewriteBias, SnapshotManifest,
    VariantDeltaManifest, VariantFileMap, digest_bytes,
};
use parquet_lab_integrity::{LogicalColumn, LogicalRowMultiset, LogicalSchema, LogicalType};

use crate::config::{config_to_spec, spec_to_config};
use crate::rewrite::{ResolvedColumn, rewrite_file};
use crate::spec::VariantSpec;

/// The accumulated result of rewriting every file in a control.
struct RewriteAll {
    file_maps: Vec<VariantFileMap>,
    resolved: BTreeMap<String, ResolvedColumn>,
    total_input_bytes: u64,
    total_output_bytes: u64,
    total_compressed: i64,
    total_rows: u64,
    logical: LogicalRowMultiset,
}

/// Rewrite every `(input relative path, input digest)` from `src_dir` into `tmp_dir`
/// under `spec`, folding the logical fingerprint under the declared schema.
#[allow(clippy::too_many_arguments)]
fn rewrite_all(
    src_dir: &Path,
    inputs: &[(String, FileDigest)],
    spec: &VariantSpec,
    declared: &BTreeMap<String, LogicalType>,
    sort_key: &[String],
    logical_schema: &LogicalSchema,
    tmp_dir: &Path,
) -> Result<RewriteAll> {
    let projections = spec.projections()?;
    let mut logical = LogicalRowMultiset::default();
    let mut file_maps = Vec::with_capacity(inputs.len());
    let mut resolved: BTreeMap<String, ResolvedColumn> = BTreeMap::new();
    let mut total_input_bytes = 0u64;
    let mut total_output_bytes = 0u64;
    let mut total_compressed = 0i64;
    let mut total_rows = 0u64;

    for (i, (rel, digest)) in inputs.iter().enumerate() {
        let in_path = src_dir.join(rel);
        let in_bytes = std::fs::read(&in_path).with_context(|| format!("reading {rel}"))?;
        digest.verify(rel, &in_bytes)?;
        total_input_bytes += in_bytes.len() as u64;

        let out_rel = format!("parquet/part-{i:05}.parquet");
        let rewritten = rewrite_file(
            digest.clone(),
            &in_bytes,
            &out_rel,
            spec,
            &projections,
            declared,
            sort_key,
            logical_schema,
            &mut logical,
            tmp_dir,
        )?;
        total_output_bytes += rewritten.map.output.bytes;
        total_compressed += rewritten.compressed_bytes;
        total_rows += rewritten.map.output_rows;
        crate::merge_resolved_pub(&mut resolved, rewritten.columns);
        file_maps.push(rewritten.map);
    }

    Ok(RewriteAll {
        file_maps,
        resolved,
        total_input_bytes,
        total_output_bytes,
        total_compressed,
        total_rows,
        logical,
    })
}

/// Build a declared logical schema and name→type map from a projection map (column →
/// logical type name) and a physical schema (for column order), so `vary` can fold the
/// fingerprint without re-reading the product snapshot.
fn declared_from_projection(
    projection: &serde_json::Map<String, serde_json::Value>,
    physical_schema: &serde_json::Value,
) -> Result<(LogicalSchema, BTreeMap<String, LogicalType>)> {
    let fields = physical_schema
        .get("fields")
        .and_then(|f| f.as_array())
        .context("reconstruction physical schema has no fields")?;
    let mut columns = Vec::with_capacity(fields.len());
    let mut map = BTreeMap::new();
    for f in fields {
        let name = f
            .get("name")
            .and_then(|n| n.as_str())
            .context("physical field missing name")?;
        let type_name = projection
            .get(name)
            .and_then(|v| v.as_str())
            .with_context(|| format!("projection missing column '{name}'"))?;
        let logical =
            LogicalType::parse(type_name).map_err(|e| anyhow::anyhow!("column '{name}': {e}"))?;
        columns.push(LogicalColumn {
            name: name.to_string(),
            logical,
        });
        map.insert(name.to_string(), logical);
    }
    Ok((LogicalSchema::new(columns), map))
}

/// Refuse to write into (or under) an immutable control directory. Resolved against the
/// nearest existing ancestor so a not-yet-created output is still caught.
fn refuse_write_into(protected: &Path, output: &Path) -> Result<()> {
    let protected = std::fs::canonicalize(protected).unwrap_or_else(|_| protected.to_path_buf());
    let mut anc = output.to_path_buf();
    let mut tail: Vec<std::ffi::OsString> = Vec::new();
    let resolved = loop {
        if let Ok(c) = std::fs::canonicalize(&anc) {
            let mut p = c;
            for t in tail.iter().rev() {
                p.push(t);
            }
            break p;
        }
        match anc.file_name() {
            Some(n) => {
                tail.push(n.to_os_string());
                if !anc.pop() {
                    break output.to_path_buf();
                }
            }
            None => break output.to_path_buf(),
        }
    };
    if resolved == protected || resolved.starts_with(&protected) {
        bail!(
            "refusing to write into the immutable control directory {} — product and \
             reconstruction bytes are never rewritten in place",
            protected.display()
        );
    }
    Ok(())
}

/// The codec family a compression string names, e.g. `zstd(6)` → `ZSTD`. Used to
/// cross-check the footer actually applied the requested family (level is not
/// footer-observable, so the manifest stays authoritative for it).
fn codec_family(compression: &str) -> String {
    let s = compression.trim().to_lowercase();
    if s.starts_with("zstd") {
        "ZSTD".into()
    } else if s == "lz4_raw" {
        "LZ4_RAW".into()
    } else if s == "snappy" {
        "SNAPPY".into()
    } else if s == "uncompressed" {
        "UNCOMPRESSED".into()
    } else {
        s.to_uppercase()
    }
}

/// Cross-check the footer-resolved compression against the requested global family.
fn assert_codec_family(resolved: &BTreeMap<String, ResolvedColumn>, requested: &str) -> Result<()> {
    let want = codec_family(requested);
    for (name, col) in resolved {
        let got = col.compression.to_uppercase();
        // Column-level overrides may legitimately differ; only assert the family is not a
        // wholesale mismatch for the majority global codec. A footer that names a different
        // family than the global request on every column means the writer ignored it.
        if !got.contains(&want) && want != "UNCOMPRESSED" {
            // Allow a column that carries its own compression override; but if *no* column
            // matches the requested global family, that is a real mismatch.
            let any_match = resolved
                .values()
                .any(|c| c.compression.to_uppercase().contains(&want));
            if !any_match {
                bail!(
                    "requested global compression '{requested}' ({want}) but the footer resolved \
                     column '{name}' to '{}' and no column matches the requested family",
                    col.compression
                );
            }
        }
    }
    Ok(())
}

fn atomic_tmp(output_dir: &Path) -> std::path::PathBuf {
    output_dir.with_file_name(format!(
        "{}.tmp-causal",
        output_dir
            .file_name()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_default()
    ))
}

fn prepare_tmp(tmp_dir: &Path) -> Result<()> {
    if tmp_dir.exists() {
        std::fs::remove_dir_all(tmp_dir).ok();
    }
    std::fs::create_dir_all(tmp_dir)?;
    Ok(())
}

fn publish(tmp_dir: &Path, output_dir: &Path) -> Result<()> {
    if output_dir.exists() {
        let non_empty = std::fs::read_dir(output_dir)
            .map(|mut d| d.next().is_some())
            .unwrap_or(false);
        if non_empty {
            std::fs::remove_dir_all(tmp_dir).ok();
            bail!(
                "output directory {} already exists and is not empty",
                output_dir.display()
            );
        }
        std::fs::remove_dir_all(output_dir).ok();
    }
    std::fs::rename(tmp_dir, output_dir)
        .with_context(|| format!("publishing {}", output_dir.display()))
}

/// `reconstruct`: rebuild a product snapshot's logical rows under a baseline spec.
pub fn run_reconstruct(
    product_manifest_path: &Path,
    baseline_spec_path: &Path,
    output_dir: &Path,
) -> Result<std::path::PathBuf> {
    let product_dir = product_manifest_path.parent().unwrap_or(Path::new("."));
    refuse_write_into(product_dir, output_dir)?;

    let product_bytes = std::fs::read(product_manifest_path)
        .with_context(|| format!("reading {}", product_manifest_path.display()))?;
    let product_digest = digest_bytes(&product_bytes);
    let product =
        SnapshotManifest::parse(&product_manifest_path.display().to_string(), &product_bytes)?;

    let spec_bytes = std::fs::read(baseline_spec_path)
        .with_context(|| format!("reading {}", baseline_spec_path.display()))?;
    let baseline_spec_digest = digest_bytes(&spec_bytes);
    let spec = VariantSpec::parse(&spec_bytes, &baseline_spec_path.display().to_string())?;

    let (logical_schema, declared) = crate::declared_from_manifest(&product)?;
    let inputs: Vec<(String, FileDigest)> = product
        .files
        .iter()
        .map(|f| {
            (
                f.path.clone(),
                FileDigest {
                    path: f.path.clone(),
                    bytes: f.bytes,
                    digest: f.digest.clone(),
                },
            )
        })
        .collect();

    let tmp_dir = atomic_tmp(output_dir);
    prepare_tmp(&tmp_dir)?;

    let out = rewrite_all(
        product_dir,
        &inputs,
        &spec,
        &declared,
        &product.sort_key,
        &logical_schema,
        &tmp_dir,
    )?;

    let logical_mirror = crate::to_fingerprint(&out.logical);
    if !logical_mirror.agrees_with(&product.logical_fingerprint) {
        std::fs::remove_dir_all(&tmp_dir).ok();
        bail!(
            "reconstruction changed a logical value — the rebuilt rows do not reproduce the \
             product's logical fingerprint. Refusing to publish."
        );
    }
    assert_codec_family(&out.resolved, &spec.compression)?;

    let requested_config = spec_to_config(&spec);
    let n_files = out.file_maps.len();
    let logical_projection = product
        .logical_projection
        .as_ref()
        .map(|p| p.logical_types.clone())
        .unwrap_or_default();

    let manifest = ReconstructionManifest {
        reconstruction_version: parquet_lab_contract::RECONSTRUCTION_VERSION.to_string(),
        parent_product_digest: product_digest,
        baseline_spec_digest,
        label: spec.label.clone(),
        requested_config: requested_config.clone(),
        resolved_config: requested_config,
        logical_projection,
        sort_key: product.sort_key.clone(),
        physical_schema: product.physical_schema.clone(),
        logical_fingerprint: logical_mirror,
        files: out.file_maps,
        input_census: serde_json::json!({
            "total_rows": product.total_rows,
            "total_bytes": out.total_input_bytes,
            "files": inputs.len(),
        }),
        output_census: serde_json::json!({
            "total_rows": out.total_rows,
            "total_bytes": out.total_output_bytes,
            "total_compressed_bytes": out.total_compressed,
            "files": n_files,
        }),
        rewrite_bias: RewriteBias {
            product_total_bytes: product.total_bytes,
            reconstruction_total_bytes: out.total_output_bytes,
            per_column: serde_json::json!({}),
        },
    };
    manifest.validate("manifest.json")?;
    std::fs::write(
        tmp_dir.join("manifest.json"),
        serde_json::to_vec_pretty(&manifest)?,
    )
    .context("writing reconstruction manifest")?;
    publish(&tmp_dir, output_dir)?;
    Ok(output_dir.join("manifest.json"))
}

/// `vary`: apply a one-variable delta to a reconstruction control.
pub fn run_vary(
    reconstruction_manifest_path: &Path,
    delta_path: &Path,
    output_dir: &Path,
) -> Result<std::path::PathBuf> {
    let reco_dir = reconstruction_manifest_path
        .parent()
        .unwrap_or(Path::new("."));
    refuse_write_into(reco_dir, output_dir)?;

    let reco_bytes = std::fs::read(reconstruction_manifest_path)
        .with_context(|| format!("reading {}", reconstruction_manifest_path.display()))?;
    let reconstruction_digest = digest_bytes(&reco_bytes);
    let reconstruction = ReconstructionManifest::parse(
        &reconstruction_manifest_path.display().to_string(),
        &reco_bytes,
    )?;

    let delta_bytes =
        std::fs::read(delta_path).with_context(|| format!("reading {}", delta_path.display()))?;
    let delta = ParsedDelta::parse(&delta_bytes, &delta_path.display().to_string())?;

    // Apply the delta onto the reconstruction's resolved config → the child config.
    let mut child_fields = reconstruction.resolved_config.fields.clone();
    for (path, value) in &delta.changes {
        child_fields.insert(path.clone(), value.clone());
    }
    let child_config = ResolvedConfig::new(child_fields);
    let child_spec = config_to_spec(&delta.label, &child_config)?;

    let (logical_schema, declared) = declared_from_projection(
        &reconstruction.logical_projection,
        &reconstruction.physical_schema,
    )?;
    let inputs: Vec<(String, FileDigest)> = reconstruction
        .files
        .iter()
        .map(|f| (f.output.path.clone(), f.output.clone()))
        .collect();

    let tmp_dir = atomic_tmp(output_dir);
    prepare_tmp(&tmp_dir)?;

    let out = rewrite_all(
        reco_dir,
        &inputs,
        &child_spec,
        &declared,
        &reconstruction.sort_key,
        &logical_schema,
        &tmp_dir,
    )?;

    let logical_mirror = crate::to_fingerprint(&out.logical);
    if !logical_mirror.agrees_with(&reconstruction.logical_fingerprint) {
        std::fs::remove_dir_all(&tmp_dir).ok();
        bail!(
            "variant changed a logical value — the rewritten rows do not reproduce the \
             reconstruction's logical fingerprint. Refusing to publish."
        );
    }
    assert_codec_family(&out.resolved, &child_spec.compression)?;

    let manifest = VariantDeltaManifest {
        delta_version: parquet_lab_contract::VARIANT_DELTA_VERSION.to_string(),
        parent_reconstruction_digest: reconstruction_digest.clone(),
        label: delta.label.clone(),
        allowed_changes: delta.allowed_changes.clone(),
        changes: delta.changes.clone(),
        resolved_config: spec_to_config(&child_spec),
        logical_fingerprint: logical_mirror,
        files: out.file_maps,
        input_census: serde_json::json!({
            "total_bytes": out.total_input_bytes,
        }),
        output_census: serde_json::json!({
            "total_rows": out.total_rows,
            "total_bytes": out.total_output_bytes,
            "total_compressed_bytes": out.total_compressed,
        }),
    };
    manifest.validate("manifest.json")?;
    // The structural-diff gate: the resolved child differs from the parent only on the allowlist.
    manifest
        .check_against_reconstruction(&reconstruction_digest, &reconstruction.resolved_config)
        .inspect_err(|_| {
            std::fs::remove_dir_all(&tmp_dir).ok();
        })?;

    std::fs::write(
        tmp_dir.join("manifest.json"),
        serde_json::to_vec_pretty(&manifest)?,
    )
    .context("writing variant delta manifest")?;
    publish(&tmp_dir, output_dir)?;
    Ok(output_dir.join("manifest.json"))
}

/// A parsed delta TOML: label, allowlist, and the flattened `path → value` changes.
struct ParsedDelta {
    label: String,
    allowed_changes: Vec<String>,
    changes: BTreeMap<String, serde_json::Value>,
}

impl ParsedDelta {
    fn parse(bytes: &[u8], path: &str) -> Result<Self> {
        let text = std::str::from_utf8(bytes)?;
        let value: toml::Value =
            toml::from_str(text).map_err(|e| anyhow::anyhow!("{path}: {e}"))?;
        if let Some(v) = value.get("version").and_then(|v| v.as_str())
            && v != parquet_lab_contract::VARIANT_DELTA_VERSION
        {
            bail!(
                "{path}: delta declares version '{v}', expected '{}'",
                parquet_lab_contract::VARIANT_DELTA_VERSION
            );
        }
        let label = value
            .get("label")
            .and_then(|v| v.as_str())
            .context("delta has no label")?
            .to_string();
        let allowed_changes = value
            .get("allowed_changes")
            .and_then(|v| v.as_array())
            .context("delta has no allowed_changes array")?
            .iter()
            .map(|v| {
                v.as_str()
                    .map(str::to_string)
                    .context("allowed_changes entry is not a string")
            })
            .collect::<Result<Vec<_>>>()?;
        let mut changes = BTreeMap::new();
        if let Some(table) = value.get("changes") {
            flatten_toml("", table, &mut changes)?;
        }
        Ok(ParsedDelta {
            label,
            allowed_changes,
            changes,
        })
    }
}

/// Flatten a nested `[changes]` TOML table into dot-path keys, so
/// `changes.global.compression = "zstd(6)"` becomes `"global.compression" -> "zstd(6)"`.
fn flatten_toml(
    prefix: &str,
    value: &toml::Value,
    out: &mut BTreeMap<String, serde_json::Value>,
) -> Result<()> {
    match value {
        toml::Value::Table(t) => {
            for (k, v) in t {
                let path = if prefix.is_empty() {
                    k.clone()
                } else {
                    format!("{prefix}.{k}")
                };
                flatten_toml(&path, v, out)?;
            }
            Ok(())
        }
        leaf => {
            let json = serde_json::to_value(leaf)
                .map_err(|e| anyhow::anyhow!("converting delta value at '{prefix}': {e}"))?;
            out.insert(prefix.to_string(), json);
            Ok(())
        }
    }
}

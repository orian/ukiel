//! `parquet-rewrite` — rewrite one snapshot under one explicit variant spec.
//!
//! A variant preserves file membership, row order, sort semantics, and file count; it may
//! change row-group/page boundaries and the physical schema. The rewrite reads back what
//! the writer actually did from the output footers (dictionary fallback included), and
//! publishes the variant manifest only after the logical fingerprint over the rewritten
//! rows equals the snapshot's — a mismatch invalidates the entire variant.

pub mod config;
pub mod reconstruct;
pub mod rewrite;
pub mod spec;
pub mod types;

use std::collections::BTreeMap;
use std::path::Path;

use anyhow::{Context, Result, bail};
use parquet_lab_contract::{
    ColumnProperties, Digest, FileDigest, SnapshotManifest, VARIANT_MANIFEST_VERSION,
    VariantManifest, WriterProperties as ContractWriterProps, digest_bytes,
};
use parquet_lab_integrity::{LogicalColumn, LogicalRowMultiset, LogicalSchema, LogicalType};

use crate::rewrite::{ResolvedColumn, rewrite_file};
use crate::spec::VariantSpec;

pub use reconstruct::{run_reconstruct, run_vary};

/// Build the declared logical schema (physical column order) and the name→type map from a
/// snapshot manifest, so the rewrite folds the fingerprint under exactly the snapshot's
/// declared types.
pub(crate) fn declared_from_manifest(
    manifest: &SnapshotManifest,
) -> Result<(LogicalSchema, BTreeMap<String, LogicalType>)> {
    let proj = manifest
        .logical_projection
        .as_ref()
        .context("snapshot has no logical projection; cannot rewrite type variants")?;
    let fields = manifest
        .physical_schema
        .get("fields")
        .and_then(|f| f.as_array())
        .context("snapshot physical schema has no fields")?;
    let mut columns = Vec::with_capacity(fields.len());
    let mut map = BTreeMap::new();
    for f in fields {
        let name = f
            .get("name")
            .and_then(|n| n.as_str())
            .context("physical field missing name")?;
        let type_name = proj
            .logical_types
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

/// The end-to-end rewrite: snapshot + spec -> a verified variant.
pub fn run(
    snapshot_manifest_path: &Path,
    spec_path: &Path,
    output_dir: &Path,
    replace: bool,
) -> Result<std::path::PathBuf> {
    let snap_dir = snapshot_manifest_path.parent().unwrap_or(Path::new("."));
    let snap_bytes = std::fs::read(snapshot_manifest_path)
        .with_context(|| format!("reading {}", snapshot_manifest_path.display()))?;
    let snapshot_digest = digest_bytes(&snap_bytes);
    let snapshot =
        SnapshotManifest::parse(&snapshot_manifest_path.display().to_string(), &snap_bytes)?;

    let spec_bytes =
        std::fs::read(spec_path).with_context(|| format!("reading {}", spec_path.display()))?;
    let spec_digest = digest_bytes(&spec_bytes);
    let spec = VariantSpec::parse(&spec_bytes, &spec_path.display().to_string())?;
    let projections = spec.projections()?;

    let (logical_schema, declared) = declared_from_manifest(&snapshot)?;

    // Rewrite into a sibling temp tree; publish by rename only after logical equality.
    let tmp_dir = output_dir.with_file_name(format!(
        "{}.tmp-variant",
        output_dir
            .file_name()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_default()
    ));
    prepare_output(output_dir, &tmp_dir, replace)?;

    let mut logical = LogicalRowMultiset::default();
    let mut file_maps = Vec::with_capacity(snapshot.files.len());
    let mut resolved: BTreeMap<String, ResolvedColumn> = BTreeMap::new();
    let mut total_input_bytes = 0u64;
    let mut total_output_bytes = 0u64;
    let mut total_compressed = 0i64;
    let mut total_rows = 0u64;

    for (i, f) in snapshot.files.iter().enumerate() {
        let in_path = snap_dir.join(&f.path);
        let in_bytes = std::fs::read(&in_path).with_context(|| format!("reading {}", f.path))?;
        let input_digest = FileDigest {
            path: f.path.clone(),
            bytes: f.bytes,
            digest: f.digest.clone(),
        };
        input_digest.verify(&f.path, &in_bytes)?;
        total_input_bytes += in_bytes.len() as u64;

        let out_rel = format!("parquet/part-{i:05}.parquet");
        let rewritten = rewrite_file(
            input_digest,
            &in_bytes,
            &out_rel,
            &spec,
            &projections,
            &declared,
            &snapshot.sort_key,
            &logical_schema,
            &mut logical,
            &tmp_dir,
        )?;
        total_output_bytes += rewritten.map.output.bytes;
        total_compressed += rewritten.compressed_bytes;
        total_rows += rewritten.map.output_rows;
        merge_resolved(&mut resolved, rewritten.columns);
        file_maps.push(rewritten.map);
    }

    // The invariant: rewritten rows must reproduce the snapshot's logical fingerprint.
    let logical_mirror = crate::to_fingerprint(&logical);
    if !logical_mirror.agrees_with(&snapshot.logical_fingerprint) {
        // Clean up the temp tree; nothing is published.
        std::fs::remove_dir_all(&tmp_dir).ok();
        bail!(
            "the rewritten rows do not reproduce the snapshot's logical fingerprint — the variant \
             changed a logical value, not just the physical representation. Refusing to publish."
        );
    }

    let columns = build_column_properties(&spec, &resolved);
    let manifest = VariantManifest {
        manifest_version: VARIANT_MANIFEST_VERSION.to_string(),
        parent_snapshot_digest: snapshot_digest,
        spec_digest,
        label: spec.label.clone(),
        properties: ContractWriterProps {
            row_group_rows: spec.row_group_rows,
            key_boundary_flush: spec.key_boundary_flush,
            write_batch_rows: spec.write_batch_rows,
            data_page_bytes: spec.data_page_bytes,
            dictionary_page_bytes: spec.dictionary_page_bytes,
            statistics: spec.statistics.clone(),
            offset_index: spec.offset_index,
            compression: spec.compression.clone(),
        },
        columns,
        input_census: serde_json::json!({
            "total_rows": snapshot.total_rows,
            "total_bytes": total_input_bytes,
            "files": snapshot.files.len(),
        }),
        output_census: serde_json::json!({
            "total_rows": total_rows,
            "total_bytes": total_output_bytes,
            "total_compressed_bytes": total_compressed,
            "files": file_maps.len(),
        }),
        logical_fingerprint: logical_mirror,
        files: file_maps,
    };
    manifest.validate("manifest.json")?;

    std::fs::write(
        tmp_dir.join("manifest.json"),
        serde_json::to_vec_pretty(&manifest)?,
    )
    .context("writing variant manifest")?;
    if output_dir.exists() {
        std::fs::remove_dir_all(output_dir).ok();
    }
    std::fs::rename(&tmp_dir, output_dir)
        .with_context(|| format!("publishing {}", output_dir.display()))?;
    Ok(output_dir.join("manifest.json"))
}

fn prepare_output(output_dir: &Path, tmp_dir: &Path, replace: bool) -> Result<()> {
    if output_dir.exists() {
        let non_empty = std::fs::read_dir(output_dir)
            .map(|mut d| d.next().is_some())
            .unwrap_or(false);
        if non_empty && !replace {
            bail!(
                "output directory {} already exists and is not empty; pass --replace",
                output_dir.display()
            );
        }
    }
    if tmp_dir.exists() {
        std::fs::remove_dir_all(tmp_dir).ok();
    }
    std::fs::create_dir_all(tmp_dir)?;
    Ok(())
}

pub(crate) fn merge_resolved_pub(
    into: &mut BTreeMap<String, ResolvedColumn>,
    from: BTreeMap<String, ResolvedColumn>,
) {
    merge_resolved(into, from)
}

fn merge_resolved(
    into: &mut BTreeMap<String, ResolvedColumn>,
    from: BTreeMap<String, ResolvedColumn>,
) {
    for (name, col) in from {
        let entry = into.entry(name).or_default();
        entry.encodings.extend(col.encodings);
        entry.dictionary |= col.dictionary;
        entry.bloom |= col.bloom;
        entry.compressed_bytes += col.compressed_bytes;
        if entry.compression.is_empty() {
            entry.compression = col.compression;
        }
    }
}

/// Assemble the manifest's column records: requested from the spec, resolved from footers.
fn build_column_properties(
    spec: &VariantSpec,
    resolved: &BTreeMap<String, ResolvedColumn>,
) -> Vec<ColumnProperties> {
    let mut out = Vec::new();
    for (name, r) in resolved {
        let req = spec.column.iter().find(|c| &c.name == name);
        out.push(ColumnProperties {
            column: name.clone(),
            encoding: req.and_then(|c| c.encoding.clone()),
            dictionary: req.and_then(|c| c.dictionary),
            compression: req.and_then(|c| c.compression.clone()),
            bloom_fpp: req.and_then(|c| c.bloom_fpp),
            bloom_ndv: req.and_then(|c| c.bloom_ndv),
            physical_type: req.and_then(|c| c.physical_type.clone()),
            resolved_encodings: r.encodings.iter().cloned().collect(),
            resolved_dictionary: Some(r.dictionary),
            resolved_compression: Some(r.compression.clone()),
        });
    }
    out
}

/// Convert a logical accumulator into the contract fingerprint mirror.
pub fn to_fingerprint(m: &LogicalRowMultiset) -> parquet_lab_contract::Fingerprint {
    parquet_lab_contract::Fingerprint {
        version: parquet_lab_integrity::LOGICAL_ROW_MULTISET_VERSION.to_string(),
        count: m.count,
        xor: m.xor_hex(),
        sum: m.sum,
        digest: m.digest_hex(),
    }
}

/// Re-export for the binary's help text.
pub fn variant_label(spec_bytes: &[u8]) -> Option<String> {
    VariantSpec::parse(spec_bytes, "spec").ok().map(|s| s.label)
}

/// The digest type, re-exported for callers building on this crate.
pub type ContractDigest = Digest;

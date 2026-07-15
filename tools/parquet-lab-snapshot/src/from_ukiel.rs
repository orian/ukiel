//! The `from-ukiel` adapter: freeze a converged plan-46 load into a snapshot.
//!
//! This is the only service-aware path. It revalidates the receipt against a live
//! catalog and object store, refuses a fixture that has not converged or is not fully
//! marked, downloads each final object unchanged, cross-checks the physical
//! `row-multiset/v1` against the receipt, and publishes only after every local digest
//! equals the bytes downloaded. It compacts, vacuums, queries, and mutates nothing.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use object_store::{ObjectStore, ObjectStoreExt as _};
use parquet_lab_contract::{
    FileDigest, LogicalProjection, SNAPSHOT_MANIFEST_VERSION, SnapshotFile, SnapshotManifest,
    SourceKind,
};
use parquet_lab_integrity::{LogicalColumn, LogicalSchema, LogicalType};
use prod_synth_contract::{L0Manifest, PartShapeReceipt};
use ukiel_catalog::PostgresCatalog;
use ukiel_core::schema::{ColumnKind, TableColumns};

use crate::{PendingFile, SnapshotBuilder};

/// Map a Ukiel declared column type to the laboratory's logical type.
fn logical_of_ukiel(ukiel_type: &str) -> Result<(LogicalType, &'static str)> {
    Ok(match ukiel_type {
        "int64" => (LogicalType::SignedInt, "int64"),
        "timestamp_ms" => (LogicalType::TimestampMillis, "timestamp_ms"),
        "float64" => (LogicalType::Float64, "float64"),
        "utf8" => (LogicalType::Utf8, "utf8"),
        "bool" => (LogicalType::Boolean, "bool"),
        other => bail!("unsupported Ukiel column type '{other}' for a logical snapshot"),
    })
}

/// Build the declared logical schema (physical, non-alias columns in order) and the
/// projection map from a Ukiel table schema JSON.
pub fn logical_schema_from_ukiel(
    schema_json: &serde_json::Value,
) -> Result<(LogicalSchema, LogicalProjection)> {
    let cols = TableColumns::parse(schema_json)
        .map_err(|e| anyhow::anyhow!("parsing Ukiel table schema: {e}"))?;
    let mut columns = Vec::new();
    let mut proj = serde_json::Map::new();
    for spec in &cols.specs {
        // Only stored/default/materialized columns land in the physical Parquet file; an
        // alias is never written, so it is not part of the frozen bytes.
        if matches!(spec.kind, ColumnKind::Alias(_)) {
            continue;
        }
        let (logical, name) = logical_of_ukiel(&spec.ukiel_type)?;
        columns.push(LogicalColumn {
            name: spec.name.clone(),
            logical,
        });
        proj.insert(spec.name.clone(), serde_json::json!(name));
    }
    Ok((
        LogicalSchema::new(columns),
        LogicalProjection {
            logical_types: proj,
        },
    ))
}

/// Rebuild the logical schema from a published manifest, in the physical column order,
/// so `verify` folds rows under exactly the declared types the snapshot recorded.
pub fn logical_schema_from_manifest(manifest: &SnapshotManifest) -> Result<LogicalSchema> {
    let proj = manifest
        .logical_projection
        .as_ref()
        .context("manifest has no logical projection")?;
    let fields = manifest
        .physical_schema
        .get("fields")
        .and_then(|f| f.as_array())
        .context("manifest physical schema has no fields")?;
    let mut columns = Vec::with_capacity(fields.len());
    for f in fields {
        let name = f
            .get("name")
            .and_then(|n| n.as_str())
            .context("physical field missing name")?;
        let type_name = proj
            .logical_types
            .get(name)
            .and_then(|v| v.as_str())
            .with_context(|| format!("logical projection missing column '{name}'"))?;
        columns.push(LogicalColumn {
            name: name.to_string(),
            logical: LogicalType::parse(type_name)
                .map_err(|e| anyhow::anyhow!("column '{name}': {e}"))?,
        });
    }
    Ok(LogicalSchema::new(columns))
}

/// Read a receipt and the L0 manifest it references, verifying the L0 digest.
pub fn read_receipt_and_l0(
    receipt_path: &Path,
) -> Result<(PartShapeReceipt, L0Manifest, FileDigest)> {
    let dir = receipt_path.parent().unwrap_or(Path::new("."));
    let rb = std::fs::read(receipt_path)
        .with_context(|| format!("reading {}", receipt_path.display()))?;
    let receipt = PartShapeReceipt::parse(&receipt_path.display().to_string(), &rb)?;

    let l0_path = dir.join(&receipt.l0_manifest.path);
    let l0b = std::fs::read(&l0_path)
        .with_context(|| format!("reading L0 manifest {}", l0_path.display()))?;
    receipt
        .l0_manifest
        .verify(&l0_path.display().to_string(), &l0b)?;
    let l0 = L0Manifest::parse(&l0_path.display().to_string(), &l0b)?;
    let l0_digest = FileDigest::of(receipt.l0_manifest.path.clone(), &l0b);
    Ok((receipt, l0, l0_digest))
}

/// The end-to-end `from-ukiel` freeze, taking already-connected clients so the same code
/// serves the CLI and the in-harness integration test.
#[allow(clippy::too_many_arguments)]
pub async fn freeze(
    catalog: &PostgresCatalog,
    store: &Arc<dyn ObjectStore>,
    receipt: &PartShapeReceipt,
    l0: &L0Manifest,
    l0_digest: FileDigest,
    source_manifest: FileDigest,
    output_dir: &Path,
    creation_command: String,
    replace: bool,
) -> Result<std::path::PathBuf> {
    // Identity, not authority: the hypertable must exist under the receipt's name/id and
    // packing key, or this is a different load.
    let ht = catalog
        .get_hypertable(&receipt.hypertable)
        .await
        .map_err(|_| anyhow::anyhow!("no load named '{}' in this catalog", receipt.hypertable))?;
    if ht.id.0 != receipt.hypertable_id {
        bail!(
            "'{}' has id {} but the receipt records {}",
            receipt.hypertable,
            ht.id.0,
            receipt.hypertable_id
        );
    }
    if ht.packing_key != receipt.packing_key {
        bail!(
            "'{}' packing key '{}' != receipt '{}'",
            receipt.hypertable,
            ht.packing_key,
            receipt.packing_key
        );
    }

    let parts = catalog.live_parts(ht.id, None).await?;
    require_converged(&parts, receipt)?;

    // The declared logical schema, from the L0 manifest's table spec.
    let (logical_schema, projection) = logical_schema_from_ukiel(&l0.table.schema)?;

    let mut builder =
        SnapshotBuilder::new(output_dir, logical_schema, /* want_physical */ true);
    builder.begin(replace)?;

    // Deterministic order: by object path, so the same load always freezes identically.
    let mut ordered: Vec<_> = parts.iter().collect();
    ordered.sort_by(|a, b| a.meta.path.cmp(&b.meta.path));

    for (i, p) in ordered.iter().enumerate() {
        let key = object_store::path::Path::from(p.meta.path.clone());
        // HEAD first: prove the object exists and its size agrees before the GET.
        let head = store
            .head(&key)
            .await
            .with_context(|| format!("HEAD {}", p.meta.path))?;
        if head.size as i64 != p.meta.size_bytes {
            bail!(
                "{}: object HEAD size {} != catalog {}",
                p.meta.path,
                head.size,
                p.meta.size_bytes
            );
        }
        let bytes = store
            .get(&key)
            .await
            .with_context(|| format!("GET {}", p.meta.path))?
            .bytes()
            .await
            .with_context(|| format!("reading {}", p.meta.path))?;

        let pending = PendingFile {
            rel_path: format!("parquet/part-{i:05}.parquet"),
            object_key: Some(p.meta.path.clone()),
            source_part: Some(p.id.0.to_string()),
            expect_rows: Some(p.meta.row_count as u64),
            expect_bytes: Some(p.meta.size_bytes as u64),
        };
        builder.add_file(&pending, &bytes)?;
    }

    // The physical fingerprint MUST equal the staged input the receipt recorded — a
    // mismatch means a row changed, was duplicated, or was lost, and the snapshot would
    // be freezing a corruption. Refuse before publishing.
    let physical = builder
        .physical()
        .context("physical fingerprint was not computed")?;
    let physical_mirror = crate::physical_fingerprint(physical);
    if !fingerprint_agrees(&physical_mirror, receipt) {
        bail!(
            "the downloaded objects do not fingerprint to the staged input the receipt records. \
             This load is not the one the receipt describes, or a row was lost in compaction."
        );
    }

    let logical_mirror = crate::logical_fingerprint(builder.logical());
    let files: Vec<SnapshotFile> = builder.files().to_vec();
    let physical_schema = builder.physical_schema_json();
    let total_rows = builder.total_rows();
    let total_bytes: u64 = files.iter().map(|f| f.bytes).sum();

    let manifest = SnapshotManifest {
        manifest_version: SNAPSHOT_MANIFEST_VERSION.to_string(),
        source_kind: SourceKind::Plan46Receipt,
        source_digests: vec![source_manifest, l0_digest],
        tool_versions: crate::tool_versions(),
        creation_command,
        logical_schema: l0.table.schema.clone(),
        physical_schema,
        packing_key: receipt.packing_key.clone(),
        sort_key: receipt.sort_key.clone(),
        logical_projection: Some(projection),
        files,
        total_rows,
        total_bytes,
        physical_fingerprint: Some(physical_mirror),
        logical_fingerprint: logical_mirror,
        disclaimer: Some(receipt.disclaimer.clone()),
    };
    builder.finish(manifest)
}

/// Does the computed physical fingerprint agree with the receipt's?
fn fingerprint_agrees(
    mirror: &parquet_lab_contract::Fingerprint,
    receipt: &PartShapeReceipt,
) -> bool {
    mirror.version == receipt.fingerprint.version
        && mirror.count == receipt.fingerprint.count
        && mirror.xor == receipt.fingerprint.xor
        && mirror.sum == receipt.fingerprint.sum
}

/// Refuse a fixture that is not final: any L0, any multi-run partition, a wrong row
/// census, or any part missing the artifact marker means it is not the converged load the
/// receipt describes.
fn require_converged(parts: &[ukiel_core::Part], receipt: &PartShapeReceipt) -> Result<()> {
    if parts.is_empty() {
        bail!("'{}' has no live parts", receipt.hypertable);
    }
    let mut l0 = 0usize;
    let mut rows = 0i64;
    let mut runs: BTreeMap<String, BTreeSet<i64>> = BTreeMap::new();
    let mut all_marked = true;
    for p in parts {
        if p.meta.level == 0 {
            l0 += 1;
        }
        rows += p.meta.row_count;
        runs.entry(p.meta.partition_values.to_string())
            .or_default()
            .insert(p.created_by_commit.0);
        if !part_is_marked(p, &receipt.partition_marker_digest) {
            all_marked = false;
        }
    }
    let multi_run = runs.values().filter(|c| c.len() > 1).count();
    if l0 > 0 {
        bail!(
            "'{}' still has {l0} L0 part(s); run compaction to convergence before snapshotting",
            receipt.hypertable
        );
    }
    if multi_run > 0 {
        bail!(
            "'{}' has {multi_run} partition(s) with more than one run; it is not finalized",
            receipt.hypertable
        );
    }
    if rows != receipt.input_rows as i64 {
        bail!(
            "'{}' holds {rows} rows, the receipt records {}",
            receipt.hypertable,
            receipt.input_rows
        );
    }
    if !all_marked {
        bail!(
            "a live part of '{}' is missing the artifact marker; this is not the fixture the \
             receipt describes",
            receipt.hypertable
        );
    }
    Ok(())
}

fn part_is_marked(p: &ukiel_core::Part, marker_digest: &str) -> bool {
    p.meta
        .partition_values
        .get("l0_manifest")
        .and_then(|v| v.as_str())
        .map(|d| PartShapeReceipt::marker_digest(d) == marker_digest)
        .unwrap_or(false)
}

//! Load a staged L0 artifact as real compaction input.
//!
//! This is the seam between the offline half of plan 46 (generate → stage) and the live
//! half (compact → measure). It uploads the staged L0 files **unchanged** and commits
//! each one separately at level 0, so the fixture the real compactor sees is a set of
//! independent L0 runs — exactly what the ingest path would have produced — and the
//! actual ladder and finalizer, not a test shortcut, do the merging.
//!
//! Two things make a compacted final part provable descent from *this* staged artifact,
//! even though REPLACE destroys every original path and count:
//!
//! * every input part's `partition_values` carries `{l0_manifest, utc_day}`, and
//!   compaction preserves partition values; and
//! * the receipt records the L0 manifest digest, so the runner rebuilds the marker and
//!   checks a final part against it.

use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use object_store::path::Path as StorePath;
use object_store::{ObjectStore, ObjectStoreExt};
use prod_synth_contract::{
    ExpectedCompactorConfig, FileDigest, L0Manifest, PartShapeReceipt, PlacementSpec,
    SYNTHETIC_DISCLAIMER,
};
use ukiel_catalog::PostgresCatalog;
use ukiel_core::{CommitOp, HypertableId, PartMeta, Placement};

use crate::load::{create_logical_tables, queryable_tenants, read_part_facts, require_fresh_label};
use crate::{hypertable_name, object_prefix};

/// Read and verify the L0 artifact before any service access.
pub fn read_l0_artifact(l0_manifest_path: &Path) -> Result<(L0Manifest, Vec<u8>)> {
    let bytes = std::fs::read(l0_manifest_path)
        .with_context(|| format!("reading {}", l0_manifest_path.display()))?;
    let manifest = L0Manifest::parse(&l0_manifest_path.display().to_string(), &bytes)?;

    // Every staged file, by the digest the L0 manifest recorded. A file that changed
    // under the manifest would be loaded as a different fixture.
    let dir = l0_manifest_path.parent().unwrap_or(Path::new("."));
    for f in &manifest.files {
        let path = dir.join(&f.path);
        let raw = std::fs::read(&path).with_context(|| format!("reading {}", path.display()))?;
        FileDigest {
            path: f.path.clone(),
            bytes: f.bytes,
            digest: f.digest.clone(),
        }
        .verify(&path.display().to_string(), &raw)?;
    }
    Ok((manifest, bytes))
}

/// Create the hypertable at the selected placement, one `events` table per queryable
/// tenant, with a real UTC-day partition spec.
pub async fn create_hypertable(
    catalog: &PostgresCatalog,
    l0: &L0Manifest,
    placement: PlacementSpec,
    label: &str,
) -> Result<HypertableId> {
    let name = hypertable_name(label);

    // A real Ukiel partition field this time — the UTC day the staging derived, plus the
    // L0 manifest digest that identifies the artifact. Compaction groups and finalizes by
    // partition_values, so all of one day's parts become one run, and the marker proves
    // descent. This is NOT the source's ClickHouse partition hash, which chooses nothing.
    let partition_spec = serde_json::json!({
        "fields": [
            {"name": "utc_day", "type": "utf8"},
            {"name": "l0_manifest", "type": "utf8"}
        ],
        "note": "UTC day derived from the timestamp column, plus the staged-artifact identity."
    });

    let id = catalog
        .create_hypertable(
            &name,
            &l0.table.schema,
            &partition_spec,
            &l0.table.sort_key,
            &l0.table.packing_key,
        )
        .await
        .with_context(|| format!("creating hypertable '{name}'"))?;

    catalog
        .set_placement(id, placement_to_core(placement))
        .await
        .with_context(|| format!("setting placement {}", placement.as_str()))?;

    create_logical_tables(catalog, id, &queryable_tenants(&l0.representatives)).await?;
    Ok(id)
}

fn placement_to_core(p: PlacementSpec) -> Placement {
    match p {
        PlacementSpec::Packed => Placement::Packed,
        PlacementSpec::Separated => Placement::Separated,
        PlacementSpec::SizeTargeted(n) => Placement::SizeTargeted(n),
    }
}

/// Upload each staged file and commit it as its own level-0 run.
///
/// One commit per file is load-bearing: each input must be an independent L0 run, or the
/// compactor's ladder never triggers and the finalizer has nothing to fold. A single
/// bulk commit would make the whole fixture one run and measure nothing.
pub async fn load(
    catalog: &PostgresCatalog,
    store: &Arc<dyn ObjectStore>,
    l0: &L0Manifest,
    l0_manifest_bytes: &[u8],
    source_manifest: FileDigest,
    dir: &Path,
    label: &str,
    placement: PlacementSpec,
    hypertable_id: HypertableId,
    compactor: ExpectedCompactorConfig,
) -> Result<PartShapeReceipt> {
    let prefix = object_prefix(label);
    let l0_digest = prod_synth_contract::digest_bytes(l0_manifest_bytes);

    let mut input_bytes = 0i64;
    let mut input_rows = 0i64;

    for f in &l0.files {
        let local = dir.join(&f.path);
        let bytes =
            std::fs::read(&local).with_context(|| format!("reading {}", local.display()))?;

        // The digest, re-checked at the moment of upload — the object store is about to
        // hold these bytes for the life of the fixture.
        let digest = prod_synth_contract::digest_bytes(&bytes);
        if digest != f.digest {
            bail!("{}: digest changed between verification and upload", f.path);
        }

        let key = format!("{prefix}/{}", f.path);
        let store_path = StorePath::from(key.clone());

        // Write-ahead upload intent, then the object, then read its real size back.
        catalog
            .register_pending_objects(hypertable_id, std::slice::from_ref(&key))
            .await
            .with_context(|| format!("recording upload intent for {key}"))?;
        store
            .put(&store_path, bytes.clone().into())
            .await
            .with_context(|| format!("uploading {key}"))?;
        let head = store
            .head(&store_path)
            .await
            .with_context(|| format!("HEAD {key}"))?;
        if head.size != bytes.len() as u64 {
            bail!(
                "{key}: uploaded {} bytes, store reports {}",
                bytes.len(),
                head.size
            );
        }

        // Metadata from the file's own bytes, through the product's accumulators.
        let facts = read_part_facts(&key, &bytes, &l0.table.packing_key)?;
        if facts.rows != f.rows as i64 {
            bail!(
                "{key}: file holds {} rows, the L0 manifest says {}",
                facts.rows,
                f.rows
            );
        }
        if facts.key_min != f.key_min || facts.key_max != f.key_max {
            bail!(
                "{key}: file key range [{}, {}] != manifest [{}, {}]",
                facts.key_min,
                facts.key_max,
                f.key_min,
                f.key_max
            );
        }

        let meta = PartMeta {
            path: key.clone(),
            // The marker: the day this file's rows fall in, and the artifact identity.
            // Compaction preserves this, so a final part still carries it.
            partition_values: PartShapeReceipt::partition_marker(&l0_digest, &f.day),
            packing_key_min: facts.key_min,
            packing_key_max: facts.key_max,
            row_count: facts.rows,
            size_bytes: head.size as i64,
            // Level 0: a real ingest-shaped run. This IS an L0 file, not a fixed non-L0
            // level like the materialized loader uses — the whole point is to make the
            // compactor climb the ladder from 0.
            level: 0,
            column_stats: facts.column_stats,
        };

        // One commit per file. Each is its own `created_by_commit`, hence its own run.
        catalog
            .commit(hypertable_id, CommitOp::Add { parts: vec![meta] }, None)
            .await
            .with_context(|| format!("committing L0 run {key}"))?;

        input_bytes += head.size as i64;
        input_rows += facts.rows;
    }

    if input_rows as u64 != l0.output_rows {
        bail!(
            "loaded {input_rows} rows; the L0 manifest declares {}",
            l0.output_rows
        );
    }

    Ok(PartShapeReceipt {
        receipt_version: prod_synth_contract::PART_SHAPE_RECEIPT_VERSION.to_string(),
        disclaimer: SYNTHETIC_DISCLAIMER.to_string(),
        source_manifest,
        l0_manifest: FileDigest::of("l0-manifest.json", l0_manifest_bytes),
        label: label.to_string(),
        hypertable: hypertable_name(label),
        hypertable_id: hypertable_id.0,
        placement,
        input_parts: l0.files.len() as u64,
        input_rows: input_rows as u64,
        input_bytes: input_bytes as u64,
        packing_key: l0.table.packing_key.clone(),
        sort_key: l0.table.sort_key.clone(),
        partition_marker_digest: PartShapeReceipt::marker_digest(&l0_digest),
        compactor,
        fingerprint: l0.fingerprint.clone(),
    })
}

/// Write the receipt atomically, and only after the final successful commit — a failed
/// load must never leave a receipt claiming completion.
pub fn write_receipt(receipt: &PartShapeReceipt, path: &Path) -> Result<()> {
    let bytes = serde_json::to_vec_pretty(receipt).expect("serializable");
    let tmp = path.with_extension("receipt.tmp");
    std::fs::write(&tmp, &bytes).with_context(|| format!("writing {}", tmp.display()))?;
    std::fs::rename(&tmp, path).with_context(|| format!("publishing {}", path.display()))?;
    Ok(())
}

/// Refuse a label that already exists — a partial pre-existing load must be dropped
/// deliberately, never resumed under the same label.
pub async fn require_fresh(catalog: &PostgresCatalog, label: &str) -> Result<()> {
    require_fresh_label(catalog, label).await
}

//! `parquet-census` — inspect the physical Parquet structure of a laboratory snapshot.
//!
//! Standalone and offline. Given a snapshot (or variant) manifest, it reads every file's
//! footer — encodings actually written, codecs, dictionary pages, page/offset indices,
//! Bloom offsets, statistics truncation, sorting columns, and where the bytes live — and
//! rolls the per-column-chunk records up into per-column aggregates. Optional column
//! scans supply NDV and value width where the footer cannot, always labeled `scanned`.

pub mod census;

use std::collections::BTreeMap;
use std::path::Path;

use anyhow::{Context, Result, bail};
use parquet_lab_contract::{SnapshotManifest, digest_bytes};

pub use census::{
    CensusReport, ColumnAggregate, ColumnChunkCensus, FileCensus, Provenance, RowGroupCensus,
};

/// Build the declared-logical-type map (column name → logical type name) from a snapshot
/// manifest's projection, so the census can annotate each column chunk.
fn declared_types(manifest: &SnapshotManifest) -> BTreeMap<String, String> {
    let mut map = BTreeMap::new();
    if let Some(proj) = &manifest.logical_projection {
        for (name, v) in &proj.logical_types {
            if let Some(s) = v.as_str() {
                map.insert(name.clone(), s.to_string());
            }
        }
    }
    map
}

/// Census a published snapshot: verify each file against its manifest digest, read its
/// footer, and assemble the report. `scan` enables the bounded per-column value scan.
pub fn census_snapshot(manifest_path: &Path, scan: bool) -> Result<CensusReport> {
    let dir = manifest_path.parent().unwrap_or(Path::new("."));
    let mb = std::fs::read(manifest_path)
        .with_context(|| format!("reading {}", manifest_path.display()))?;
    let manifest_digest = digest_bytes(&mb);
    let manifest = SnapshotManifest::parse(&manifest_path.display().to_string(), &mb)?;
    let declared = declared_types(&manifest);

    let mut file_records = Vec::with_capacity(manifest.files.len());
    for f in &manifest.files {
        let path = dir.join(&f.path);
        let bytes = std::fs::read(&path).with_context(|| format!("reading {}", f.path))?;
        // Bind the census to the exact bytes the manifest recorded.
        parquet_lab_contract::FileDigest {
            path: f.path.clone(),
            bytes: f.bytes,
            digest: f.digest.clone(),
        }
        .verify(&f.path, &bytes)?;
        file_records.push(census::census_file(&f.path, &bytes, &declared, scan)?);
    }

    let report = census::aggregate("snapshot", &manifest_digest, scan, file_records);
    // Sanity: the census row total must equal the manifest's.
    if report.total_rows as u64 != manifest.total_rows {
        bail!(
            "census counted {} rows, manifest records {}",
            report.total_rows,
            manifest.total_rows
        );
    }
    Ok(report)
}

/// Write a report to `path` atomically, refusing to overwrite unless `replace`.
pub fn write_report(path: &Path, report: &CensusReport, replace: bool) -> Result<()> {
    if path.exists() && !replace {
        bail!(
            "report {} already exists; pass --replace to overwrite",
            path.display()
        );
    }
    let bytes = serde_json::to_vec_pretty(report)?;
    let tmp = path.with_extension("json.tmp");
    std::fs::write(&tmp, &bytes).with_context(|| format!("writing {}", tmp.display()))?;
    std::fs::rename(&tmp, path).with_context(|| format!("publishing {}", path.display()))?;
    Ok(())
}

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
use parquet_lab_contract::{SnapshotManifest, VariantManifest, digest_bytes};

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
    let name = manifest_path.display().to_string();

    // Census works on a snapshot (files by `path`, declared types from the projection) or a
    // variant (files by `output.path`; declared types come from the parent snapshot and are
    // not carried on the variant, so declared annotations are simply absent there).
    let (report_kind, files, declared, expected_rows): (
        &str,
        Vec<parquet_lab_contract::FileDigest>,
        std::collections::BTreeMap<String, String>,
        Option<u64>,
    ) = match SnapshotManifest::parse(&name, &mb) {
        Ok(snapshot) => (
            "snapshot",
            snapshot
                .files
                .iter()
                .map(|f| parquet_lab_contract::FileDigest {
                    path: f.path.clone(),
                    bytes: f.bytes,
                    digest: f.digest.clone(),
                })
                .collect(),
            declared_types(&snapshot),
            Some(snapshot.total_rows),
        ),
        Err(_) => {
            let variant = VariantManifest::parse(&name, &mb)
                .context("manifest is neither a snapshot nor a variant")?;
            (
                "variant",
                variant.files.iter().map(|f| f.output.clone()).collect(),
                std::collections::BTreeMap::new(),
                None,
            )
        }
    };

    let mut file_records = Vec::with_capacity(files.len());
    for f in &files {
        let path = dir.join(&f.path);
        let bytes = std::fs::read(&path).with_context(|| format!("reading {}", f.path))?;
        // Bind the census to the exact bytes the manifest recorded.
        f.verify(&f.path, &bytes)?;
        file_records.push(census::census_file(&f.path, &bytes, &declared, scan)?);
    }

    let report = census::aggregate(report_kind, &manifest_digest, scan, file_records);
    // Sanity: for a snapshot, the census row total must equal the manifest's.
    if let Some(expected) = expected_rows
        && report.total_rows as u64 != expected
    {
        bail!(
            "census counted {} rows, manifest records {expected}",
            report.total_rows
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

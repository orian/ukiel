//! `parquet-scan-bench` — L3 direct Arrow/Parquet decode with explicit row-group selection.
//!
//! No DataFusion session, SQL, logical plan, catalog, or result serialization is present.
//! It binds a projection role onto real columns through a workload manifest, compiles global
//! row-group ordinals into all/one/10%-contiguous/10%-sparse access plans, decodes exactly
//! those columns and row groups through the pinned Arrow/Parquet reader, and feeds every
//! decoded array to a typed deterministic checksum sink. The `zero` selection is an untimed
//! metadata negative control. Because a reconstruction and its one-variable child share
//! row-group boundaries, the same ordinals decode the same rows in both — the checksums must
//! agree, and an intentionally changed row is caught by the pre-timing digest check.

pub mod measure;
pub mod projection;
pub mod selection;
pub mod sink;

use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use arrow::datatypes::{Field, Schema};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet_lab_contract::{
    CacheReceipt, FileDigest, ProjectionRole, ReconstructionManifest, SnapshotManifest,
    VariantDeltaManifest, WorkloadBinding, digest_bytes,
};
use serde::{Deserialize, Serialize};

use crate::selection::Selection;

pub const SCAN_BENCH_VERSION: &str = "ukiel-parquet-scan-bench/v1";

/// One timed decode of the selected projection and row groups.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScanSample {
    pub wall_seconds: f64,
    pub user_seconds: f64,
    pub sys_seconds: f64,
    pub rows_decoded: u64,
    pub values_decoded: u64,
    pub batches: u64,
    pub files_opened: u64,
    pub row_groups_opened: u64,
    /// Pages opened, when observable. `null` here — page counts are not surfaced by the
    /// reader without the offset index, so absence is recorded rather than fabricated.
    pub pages_opened: Option<u64>,
    pub compressed_bytes: Option<u64>,
    pub rows_per_second: f64,
    pub values_per_second: f64,
    pub mib_per_second: f64,
    pub max_rss_bytes: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScanBenchReport {
    pub report_version: String,
    pub layer: String,
    pub role: String,
    pub selection: String,
    pub projected_columns: Vec<String>,
    pub artifact_digest: String,
    pub workload_digest: String,
    pub scenario_digest: Option<String>,
    pub cache_receipt_digest: Option<String>,
    /// The checksum over decoded rows — identical for the same logical rows regardless of
    /// codec. `None` for a metadata-only (zero-selection) run.
    pub checksum: Option<String>,
    pub metadata_only: bool,
    pub samples: Vec<ScanSample>,
    pub median_wall_seconds: f64,
    pub median_mib_per_second: f64,
    pub host: serde_json::Value,
}

fn artifact_files(bytes: &[u8], path: &str) -> Result<Vec<(String, FileDigest)>> {
    if let Ok(r) = ReconstructionManifest::parse(path, bytes) {
        return Ok(r.files.into_iter().map(|f| (f.output.path.clone(), f.output)).collect());
    }
    if let Ok(v) = VariantDeltaManifest::parse(path, bytes) {
        return Ok(v.files.into_iter().map(|f| (f.output.path.clone(), f.output)).collect());
    }
    if let Ok(s) = SnapshotManifest::parse(path, bytes) {
        return Ok(s
            .files
            .into_iter()
            .map(|f| (f.path.clone(), FileDigest { path: f.path, bytes: f.bytes, digest: f.digest }))
            .collect());
    }
    bail!("{path}: not a reconstruction, variant-delta, or snapshot manifest")
}

/// The per-file physical facts the scan needs.
struct FileMeta {
    bytes: bytes::Bytes,
    column_names: Vec<String>,
    row_group_count: u64,
    /// Compressed bytes per (row_group, column_index).
    compressed: Vec<Vec<i64>>,
}

fn read_file_meta(dir: &Path, rel: &str, fd: &FileDigest) -> Result<FileMeta> {
    let raw = std::fs::read(dir.join(rel)).with_context(|| format!("reading {rel}"))?;
    // Digest check before anything is decoded or timed: a changed row changes the bytes and
    // is rejected here.
    fd.verify(rel, &raw)?;
    let bytes = bytes::Bytes::from(raw);
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes.clone())
        .with_context(|| format!("opening {rel}"))?;
    let meta = builder.metadata().clone();
    let column_names: Vec<String> = meta
        .file_metadata()
        .schema_descr()
        .columns()
        .iter()
        .map(|c| c.name().to_string())
        .collect();
    let mut compressed = Vec::with_capacity(meta.num_row_groups());
    for rg in meta.row_groups() {
        compressed.push(rg.columns().iter().map(|c| c.compressed_size()).collect());
    }
    Ok(FileMeta {
        bytes,
        column_names,
        row_group_count: meta.num_row_groups() as u64,
        compressed,
    })
}

fn median_f64(mut xs: Vec<f64>) -> f64 {
    xs.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    if xs.is_empty() { 0.0 } else { xs[xs.len() / 2] }
}

fn host_block() -> serde_json::Value {
    serde_json::json!({
        "kernel": std::fs::read_to_string("/proc/sys/kernel/osrelease").ok().map(|s| s.trim().to_string()),
        "storage_kind": "local",
    })
}

/// Decode one sample over the prepared per-file selection, returning the sink and observed
/// counts. Only this is timed.
fn decode_once(
    metas: &[FileMeta],
    selected_indices: &[usize],
    projected_schema: &Schema,
    per_file_rgs: &[(usize, Vec<usize>)],
) -> Result<(sink::ChecksumSink, u64)> {
    let mut s = sink::ChecksumSink::new(projected_schema)?;
    let mut rows = 0u64;
    for (file_idx, local_rgs) in per_file_rgs {
        let meta = &metas[*file_idx];
        let builder = ParquetRecordBatchReaderBuilder::try_new(meta.bytes.clone())?;
        let mask = parquet::arrow::ProjectionMask::roots(
            builder.parquet_schema(),
            selected_indices.iter().copied(),
        );
        let reader = builder
            .with_projection(mask)
            .with_row_groups(local_rgs.clone())
            .build()?;
        for batch in reader {
            let batch = batch?;
            rows += batch.num_rows() as u64;
            s.consume(&batch)?;
        }
    }
    Ok((s, rows))
}

/// Run the scan bench and write a report.
#[allow(clippy::too_many_arguments)]
pub fn run_scan(
    manifest_path: &Path,
    workload_path: &Path,
    role: ProjectionRole,
    selection: Selection,
    samples: u32,
    scenario_path: Option<&Path>,
    cache_receipt_path: Option<&Path>,
    report_path: &Path,
) -> Result<ScanBenchReport> {
    if samples == 0 {
        bail!("--samples must be at least 1");
    }
    let dir = manifest_path.parent().unwrap_or(Path::new("."));
    let manifest_bytes = std::fs::read(manifest_path)
        .with_context(|| format!("reading {}", manifest_path.display()))?;
    let artifact_digest = digest_bytes(&manifest_bytes);
    let files = artifact_files(&manifest_bytes, &manifest_path.display().to_string())?;
    if files.is_empty() {
        bail!("the manifest lists no files to scan");
    }

    let workload_bytes = std::fs::read(workload_path)
        .with_context(|| format!("reading {}", workload_path.display()))?;
    let workload_digest = digest_bytes(&workload_bytes);
    let workload: WorkloadBinding = serde_json::from_slice(&workload_bytes)
        .with_context(|| format!("{}: not a workload binding", workload_path.display()))?;

    let scenario_digest = match scenario_path {
        Some(p) => Some(digest_bytes(&std::fs::read(p).with_context(|| format!("reading {}", p.display()))?)),
        None => None,
    };
    let cache_receipt_digest = match cache_receipt_path {
        Some(p) => {
            let rb = std::fs::read(p).with_context(|| format!("reading {}", p.display()))?;
            let receipt = CacheReceipt::parse(&p.display().to_string(), &rb)?;
            if !receipt.is_usable() {
                bail!("cache receipt {} is invalid; refusing to time under an unachieved state", p.display());
            }
            if receipt.target_manifest_digest != artifact_digest {
                bail!("cache receipt was prepared for a different artifact");
            }
            Some(digest_bytes(&rb))
        }
        None => None,
    };

    // Read every file's metadata and verify digests (before any timing).
    let metas: Vec<FileMeta> = files
        .iter()
        .map(|(rel, fd)| read_file_meta(dir, rel, fd))
        .collect::<Result<_>>()?;

    // Column universe (must be consistent across files).
    let all_columns = metas[0].column_names.clone();
    for m in &metas {
        if m.column_names != all_columns {
            bail!("files in the artifact do not share one schema; cannot bind roles");
        }
    }

    // Resolve the role → columns → selected indices (ascending, schema order).
    let selected_cols = projection::columns_for_role(role, &workload, &all_columns)?;
    let mut selected_indices: Vec<usize> = selected_cols
        .iter()
        .map(|c| all_columns.iter().position(|a| a == c).unwrap())
        .collect();
    selected_indices.sort_unstable();
    selected_indices.dedup();

    // The projected schema, in schema order, for the checksum sink.
    let full_schema = {
        let builder = ParquetRecordBatchReaderBuilder::try_new(metas[0].bytes.clone())?;
        builder.schema().as_ref().clone()
    };
    let projected_fields: Vec<Arc<Field>> = selected_indices
        .iter()
        .map(|&i| full_schema.field(i).clone().into())
        .collect();
    let projected_schema = Schema::new(projected_fields);

    // Compile global ordinals → per-file local row groups.
    let rg_counts: Vec<u64> = metas.iter().map(|m| m.row_group_count).collect();
    let (spans, total_rg) = selection::spans(&rg_counts);
    let ordinals = selection::global_ordinals(selection, total_rg);
    let per_file_rgs = selection::to_local(&spans, &ordinals);

    // Observable byte accounting for the selection.
    let mut compressed_bytes: u64 = 0;
    for (f, rgs) in &per_file_rgs {
        for &rg in rgs {
            for &ci in &selected_indices {
                compressed_bytes += metas[*f].compressed[rg][ci].max(0) as u64;
            }
        }
    }
    let files_opened = per_file_rgs.len() as u64;
    let row_groups_opened = ordinals.len() as u64;

    // Zero selection: an untimed metadata negative control — no decode happens.
    if selection == Selection::Zero {
        let report = ScanBenchReport {
            report_version: SCAN_BENCH_VERSION.to_string(),
            layer: "scan".to_string(),
            role: role_name(role).to_string(),
            selection: "zero".to_string(),
            projected_columns: selected_cols,
            artifact_digest,
            workload_digest,
            scenario_digest,
            cache_receipt_digest,
            checksum: None,
            metadata_only: true,
            samples: vec![],
            median_wall_seconds: 0.0,
            median_mib_per_second: 0.0,
            host: host_block(),
        };
        return publish(report_path, report);
    }

    let mut out = Vec::with_capacity(samples as usize);
    let mut checksum = String::new();
    for i in 0..samples {
        let (result, timing) =
            measure::time(|| decode_once(&metas, &selected_indices, &projected_schema, &per_file_rgs));
        let (s, rows) = result?;
        if i == 0 {
            checksum = s.checksum();
        } else if s.checksum() != checksum {
            bail!("scan checksum changed between samples — the decode is not deterministic");
        }
        let mib = compressed_bytes as f64 / (1024.0 * 1024.0);
        out.push(ScanSample {
            wall_seconds: timing.wall_seconds,
            user_seconds: timing.user_seconds,
            sys_seconds: timing.sys_seconds,
            rows_decoded: rows,
            values_decoded: s.values,
            batches: s.batches,
            files_opened,
            row_groups_opened,
            pages_opened: None,
            compressed_bytes: Some(compressed_bytes),
            rows_per_second: if timing.wall_seconds > 0.0 { rows as f64 / timing.wall_seconds } else { 0.0 },
            values_per_second: if timing.wall_seconds > 0.0 { s.values as f64 / timing.wall_seconds } else { 0.0 },
            mib_per_second: if timing.wall_seconds > 0.0 { mib / timing.wall_seconds } else { 0.0 },
            max_rss_bytes: timing.max_rss_bytes,
        });
    }

    let report = ScanBenchReport {
        report_version: SCAN_BENCH_VERSION.to_string(),
        layer: "scan".to_string(),
        role: role_name(role).to_string(),
        selection: selection_name(selection).to_string(),
        projected_columns: selected_cols,
        artifact_digest,
        workload_digest,
        scenario_digest,
        cache_receipt_digest,
        checksum: Some(checksum),
        metadata_only: false,
        median_wall_seconds: median_f64(out.iter().map(|s| s.wall_seconds).collect()),
        median_mib_per_second: median_f64(out.iter().map(|s| s.mib_per_second).collect()),
        samples: out,
        host: host_block(),
    };
    publish(report_path, report)
}

fn publish(report_path: &Path, report: ScanBenchReport) -> Result<ScanBenchReport> {
    if report_path.exists() {
        bail!("report {} already exists", report_path.display());
    }
    let tmp = report_path.with_extension("json.tmp");
    std::fs::write(&tmp, serde_json::to_vec_pretty(&report)?)
        .with_context(|| format!("writing {}", tmp.display()))?;
    std::fs::rename(&tmp, report_path)
        .with_context(|| format!("publishing {}", report_path.display()))?;
    Ok(report)
}

fn role_name(role: ProjectionRole) -> &'static str {
    match role {
        ProjectionRole::FixedWidthKey => "fixed_width_key",
        ProjectionRole::HighCardinalityString => "high_cardinality_string",
        ProjectionRole::WideText => "wide_text",
        ProjectionRole::HotSet => "hot_set",
        ProjectionRole::AllColumns => "all_columns",
    }
}

fn selection_name(s: Selection) -> &'static str {
    match s {
        Selection::All => "all",
        Selection::One => "one",
        Selection::TenPercentContiguous => "ten_percent_contiguous",
        Selection::TenPercentSparse => "ten_percent_sparse",
        Selection::Zero => "zero",
    }
}

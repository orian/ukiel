//! `file-read-bench` — L2 raw byte/range reads to a checksum sink.
//!
//! This establishes the backend/cache ceiling: how fast the bytes themselves move, with no
//! Parquet parsing in the way. It prevents a codec result from being mistaken for a disk or
//! page-cache fluctuation. It reads whole files, one file, registered contiguous ranges, or
//! registered sparse ranges with the *same returned-byte total*, feeding every byte to a
//! BLAKE3 sink. Each sample binds the cache-receipt digest it ran under, so a "cold" number
//! can be traced to a verified eviction.

pub mod measure;
pub mod ranges;
pub mod read;

use std::path::Path;

use anyhow::{Context, Result, bail};
use parquet_lab_contract::{
    CacheReceipt, FileDigest, ReconstructionManifest, SnapshotManifest, VariantDeltaManifest,
    digest_bytes,
};
use serde::{Deserialize, Serialize};

use crate::ranges::Plan;

pub const READ_BENCH_VERSION: &str = "ukiel-file-read-bench/v1";

pub fn parse_plan(s: &str) -> Result<Plan> {
    Ok(match s {
        "all" => Plan::All,
        "one" => Plan::One,
        "contiguous" => Plan::Contiguous,
        "sparse" => Plan::Sparse,
        other => bail!("unknown read plan '{other}' (all|one|contiguous|sparse)"),
    })
}

fn plan_name(p: Plan) -> &'static str {
    match p {
        Plan::All => "all",
        Plan::One => "one",
        Plan::Contiguous => "contiguous",
        Plan::Sparse => "sparse",
    }
}

/// One timed read of the whole plan.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadSample {
    pub wall_seconds: f64,
    pub user_seconds: f64,
    pub sys_seconds: f64,
    pub requested_bytes: u64,
    pub returned_bytes: u64,
    pub ranges: u64,
    pub reads: u64,
    pub mib_per_second: f64,
    pub max_rss_bytes: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadBenchReport {
    pub report_version: String,
    pub layer: String,
    pub plan: String,
    pub fraction: f64,
    pub artifact_digest: String,
    /// The cache receipt this ran under, by digest, so the state is provable. `None` if no
    /// receipt was bound (an exploratory run, never publishable).
    pub cache_receipt_digest: Option<String>,
    pub checksum: String,
    pub samples: Vec<ReadSample>,
    pub median_wall_seconds: f64,
    pub median_mib_per_second: f64,
    pub host: serde_json::Value,
}

fn artifact_files(bytes: &[u8], path: &str) -> Result<Vec<(String, FileDigest)>> {
    if let Ok(r) = ReconstructionManifest::parse(path, bytes) {
        return Ok(r
            .files
            .into_iter()
            .map(|f| (f.output.path.clone(), f.output))
            .collect());
    }
    if let Ok(v) = VariantDeltaManifest::parse(path, bytes) {
        return Ok(v
            .files
            .into_iter()
            .map(|f| (f.output.path.clone(), f.output))
            .collect());
    }
    if let Ok(s) = SnapshotManifest::parse(path, bytes) {
        return Ok(s
            .files
            .into_iter()
            .map(|f| {
                (
                    f.path.clone(),
                    FileDigest {
                        path: f.path,
                        bytes: f.bytes,
                        digest: f.digest,
                    },
                )
            })
            .collect());
    }
    bail!("{path}: not a reconstruction, variant-delta, or snapshot manifest")
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

/// Run the read bench and write a report.
#[allow(clippy::too_many_arguments)]
pub fn run_bench(
    manifest_path: &Path,
    plan: Plan,
    fraction: f64,
    samples: u32,
    cache_receipt_path: Option<&Path>,
    report_path: &Path,
) -> Result<ReadBenchReport> {
    if samples == 0 {
        bail!("--samples must be at least 1");
    }
    let dir = manifest_path.parent().unwrap_or(Path::new("."));
    let bytes = std::fs::read(manifest_path)
        .with_context(|| format!("reading {}", manifest_path.display()))?;
    let artifact_digest = digest_bytes(&bytes);
    let files = artifact_files(&bytes, &manifest_path.display().to_string())?;
    if files.is_empty() {
        bail!("the manifest lists no files to read");
    }
    let paths: Vec<std::path::PathBuf> = files.iter().map(|(rel, _)| dir.join(rel)).collect();
    for p in &paths {
        read::assert_regular(p)?;
    }
    // Verify each file against its recorded digest before timing.
    for ((rel, fd), p) in files.iter().zip(&paths) {
        let b = std::fs::read(p).with_context(|| format!("reading {rel}"))?;
        fd.verify(rel, &b)?;
    }

    // Bind (and validate) the cache receipt, if one was supplied.
    let cache_receipt_digest = match cache_receipt_path {
        Some(rp) => {
            let rb = std::fs::read(rp).with_context(|| format!("reading {}", rp.display()))?;
            let receipt = CacheReceipt::parse(&rp.display().to_string(), &rb)?;
            if !receipt.is_usable() {
                bail!(
                    "cache receipt {} is invalid ({:?} not achieved); refusing to bind an \
                     unusable cache state to a timing",
                    rp.display(),
                    receipt.requested_profile
                );
            }
            if receipt.target_manifest_digest != artifact_digest {
                bail!(
                    "cache receipt was prepared for a different artifact ({}); it does not bind \
                     these files",
                    receipt.target_manifest_digest
                );
            }
            Some(digest_bytes(&rb))
        }
        None => None,
    };

    let lengths = read::file_lengths(&paths)?;
    let plan_ranges = ranges::build(plan, &lengths, fraction);
    if plan_ranges.is_empty() {
        bail!("the {} plan produced no ranges", plan_name(plan));
    }

    let mut out = Vec::with_capacity(samples as usize);
    let mut checksum = String::new();
    for i in 0..samples {
        let (result, timing) = measure::time(|| read::execute(&paths, &plan_ranges));
        let outcome = result?;
        if i == 0 {
            checksum = outcome.checksum.clone();
        } else if outcome.checksum != checksum {
            bail!("read checksum changed between samples — the bytes are not immutable");
        }
        let mib = outcome.returned_bytes as f64 / (1024.0 * 1024.0);
        out.push(ReadSample {
            wall_seconds: timing.wall_seconds,
            user_seconds: timing.user_seconds,
            sys_seconds: timing.sys_seconds,
            requested_bytes: outcome.requested_bytes,
            returned_bytes: outcome.returned_bytes,
            ranges: outcome.ranges,
            reads: outcome.reads,
            mib_per_second: if timing.wall_seconds > 0.0 {
                mib / timing.wall_seconds
            } else {
                0.0
            },
            max_rss_bytes: timing.max_rss_bytes,
        });
    }

    let report = ReadBenchReport {
        report_version: READ_BENCH_VERSION.to_string(),
        layer: "raw_read".to_string(),
        plan: plan_name(plan).to_string(),
        fraction,
        artifact_digest,
        cache_receipt_digest,
        checksum,
        median_wall_seconds: median_f64(out.iter().map(|s| s.wall_seconds).collect()),
        median_mib_per_second: median_f64(out.iter().map(|s| s.mib_per_second).collect()),
        samples: out,
        host: host_block(),
    };
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

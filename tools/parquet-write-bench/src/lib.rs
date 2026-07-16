//! `parquet-write-bench` — L1 isolated Arrow-to-Parquet writer throughput.
//!
//! The writer is a *guardrail*, not a ranking dimension: a candidate that harms compaction
//! throughput can be vetoed, but a small writer win never compensates for larger files or
//! slower reads. So this tool measures only the write, cleanly:
//!
//! - the logical rows come from one frozen artifact (a reconstruction control);
//! - the writer configuration to time comes from a resolved config (the reconstruction's
//!   own, or a variant delta's `resolved_config`);
//! - input decode, projection, and the fingerprint check happen *before* the clock;
//! - the clock brackets exactly one `write_prepared`; and
//! - footer/fingerprint/output validation and sample cleanup happen *after* the clock.
//!
//! The product control has no invented writer time — L1 compares the reconstruction's
//! ZSTD-1 config against its ZSTD-6 child, both writing the reconstruction's rows.

pub mod measure;

use std::path::Path;

use anyhow::{Context, Result, bail};
use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use parquet_lab_contract::{Fingerprint, ReconstructionManifest, ResolvedConfig, digest_bytes};
use parquet_lab_integrity::{LogicalColumn, LogicalRowMultiset, LogicalSchema, LogicalType};
use parquet_lab_write_core::{
    WriterConfig, decode_batches, out_schema_for, project_batches, resolve_footer, sorting_columns,
    write_prepared,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// The write-bench report format.
pub const WRITE_BENCH_VERSION: &str = "ukiel-parquet-write-bench/v1";

/// One timed write.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriterSample {
    pub wall_seconds: f64,
    pub user_seconds: f64,
    pub sys_seconds: f64,
    pub rows: u64,
    pub logical_bytes: u64,
    pub output_bytes: u64,
    pub rows_per_second: f64,
    pub logical_mib_per_second: f64,
    pub max_rss_bytes: Option<u64>,
}

/// The full writer-bench report: identity, provenance, and every raw sample.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriterBenchReport {
    pub report_version: String,
    pub layer: String,
    pub artifact_digest: String,
    pub config_digest: String,
    pub compression: String,
    pub total_rows: u64,
    pub logical_input_bytes: u64,
    pub git_sha: String,
    pub host: serde_json::Value,
    pub samples: Vec<WriterSample>,
    pub median_wall_seconds: f64,
    pub median_output_bytes: u64,
}

/// Extract a `resolved_config` from a manifest JSON (reconstruction or variant delta).
fn resolved_config_from(bytes: &[u8], path: &str) -> Result<ResolvedConfig> {
    let value: serde_json::Value = serde_json::from_slice(bytes)
        .with_context(|| format!("{path}: not JSON"))?;
    let rc = value
        .get("resolved_config")
        .cloned()
        .with_context(|| format!("{path}: no resolved_config field"))?;
    serde_json::from_value(rc).with_context(|| format!("{path}: resolved_config is not a config map"))
}

/// Build the declared logical schema (physical column order + declared types) from a
/// reconstruction manifest, so the fingerprint folds under exactly the artifact's types.
fn logical_schema_from(reconstruction: &ReconstructionManifest) -> Result<LogicalSchema> {
    let fields = reconstruction
        .physical_schema
        .get("fields")
        .and_then(|f| f.as_array())
        .context("reconstruction physical schema has no fields")?;
    let mut columns = Vec::with_capacity(fields.len());
    for f in fields {
        let name = f
            .get("name")
            .and_then(|n| n.as_str())
            .context("physical field missing name")?;
        let type_name = reconstruction
            .logical_projection
            .get(name)
            .and_then(|v| v.as_str())
            .with_context(|| format!("projection missing column '{name}'"))?;
        let logical =
            LogicalType::parse(type_name).map_err(|e| anyhow::anyhow!("column '{name}': {e}"))?;
        columns.push(LogicalColumn {
            name: name.to_string(),
            logical,
        });
    }
    Ok(LogicalSchema::new(columns))
}

/// The prepared, in-memory input: everything decoded/projected/validated before the clock.
struct Prepared {
    batches: Vec<RecordBatch>,
    out_schema: Arc<Schema>,
    config: WriterConfig,
    sort_key: Vec<String>,
    total_rows: u64,
    logical_bytes: u64,
}

fn prepare(
    artifact_dir: &Path,
    reconstruction: &ReconstructionManifest,
    config: WriterConfig,
) -> Result<Prepared> {
    let projections = config.projections()?;
    let logical_schema = logical_schema_from(reconstruction)?;
    let mut logical = LogicalRowMultiset::default();
    let mut batches: Vec<RecordBatch> = Vec::new();
    let mut input_schema: Option<Schema> = None;
    let mut total_rows = 0u64;
    let mut logical_bytes = 0u64;

    for f in &reconstruction.files {
        let path = artifact_dir.join(&f.output.path);
        let bytes = std::fs::read(&path).with_context(|| format!("reading {}", f.output.path))?;
        f.output.verify(&f.output.path, &bytes)?;
        let (decoded, schema) = decode_batches(&bytes)?;
        if input_schema.is_none() {
            input_schema = Some(schema);
        }
        let projected = project_batches(&decoded, &projections)?;
        for b in &projected {
            logical
                .update(b, &logical_schema)
                .map_err(|e| anyhow::anyhow!("logical fingerprint: {e}"))?;
            total_rows += b.num_rows() as u64;
            logical_bytes += b.get_array_memory_size() as u64;
        }
        batches.extend(projected);
    }

    let mirror = Fingerprint {
        version: parquet_lab_integrity::LOGICAL_ROW_MULTISET_VERSION.to_string(),
        count: logical.count,
        xor: logical.xor_hex(),
        sum: logical.sum,
        digest: logical.digest_hex(),
    };
    if !mirror.agrees_with(&reconstruction.logical_fingerprint) {
        bail!(
            "prepared input does not reproduce the artifact's logical fingerprint — refusing to \
             time a write over the wrong rows"
        );
    }

    let input_schema = input_schema.context("artifact has no files")?;
    let out_schema = Arc::new(out_schema_for(&input_schema, &projections));
    Ok(Prepared {
        batches,
        out_schema,
        config,
        sort_key: reconstruction.sort_key.clone(),
        total_rows,
        logical_bytes,
    })
}

/// Time exactly one write of the prepared input under the resolved config. The clock
/// brackets only `write_prepared`; validation happens after it.
fn one_sample(prepared: &Prepared) -> Result<WriterSample> {
    let sorting = sorting_columns(&prepared.out_schema, &prepared.sort_key);
    let props = prepared.config.writer_properties(sorting)?;
    let mut sink: Vec<u8> = Vec::new();

    let (rows_result, timing) = measure::time(|| {
        write_prepared(&mut sink, &prepared.batches, prepared.out_schema.clone(), props)
    });
    let rows = rows_result?;

    // Validation — outside the clock.
    if rows != prepared.total_rows {
        bail!("write produced {rows} rows, expected {}", prepared.total_rows);
    }
    let outcome = resolve_footer(&sink)?;
    if outcome.output_rows != prepared.total_rows {
        bail!(
            "footer reports {} rows, expected {}",
            outcome.output_rows,
            prepared.total_rows
        );
    }
    let output_bytes = sink.len() as u64;
    let logical_mib = prepared.logical_bytes as f64 / (1024.0 * 1024.0);
    Ok(WriterSample {
        wall_seconds: timing.wall_seconds,
        user_seconds: timing.user_seconds,
        sys_seconds: timing.sys_seconds,
        rows,
        logical_bytes: prepared.logical_bytes,
        output_bytes,
        rows_per_second: if timing.wall_seconds > 0.0 {
            rows as f64 / timing.wall_seconds
        } else {
            0.0
        },
        logical_mib_per_second: if timing.wall_seconds > 0.0 {
            logical_mib / timing.wall_seconds
        } else {
            0.0
        },
        max_rss_bytes: timing.max_rss_bytes,
    })
}

fn median_f64(mut xs: Vec<f64>) -> f64 {
    xs.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    if xs.is_empty() {
        0.0
    } else {
        xs[xs.len() / 2]
    }
}

fn median_u64(mut xs: Vec<u64>) -> u64 {
    xs.sort_unstable();
    if xs.is_empty() { 0 } else { xs[xs.len() / 2] }
}

fn host_block() -> serde_json::Value {
    serde_json::json!({
        "kernel": std::fs::read_to_string("/proc/sys/kernel/osrelease")
            .ok().map(|s| s.trim().to_string()),
        "storage_kind": "local",
    })
}

/// Run the writer bench and write a report.
pub fn run_bench(
    artifact_manifest_path: &Path,
    config_source_path: &Path,
    samples: u32,
    report_path: &Path,
) -> Result<WriterBenchReport> {
    if samples == 0 {
        bail!("--samples must be at least 1");
    }
    let artifact_dir = artifact_manifest_path.parent().unwrap_or(Path::new("."));
    let artifact_bytes = std::fs::read(artifact_manifest_path)
        .with_context(|| format!("reading {}", artifact_manifest_path.display()))?;
    let artifact_digest = digest_bytes(&artifact_bytes);
    let reconstruction = ReconstructionManifest::parse(
        &artifact_manifest_path.display().to_string(),
        &artifact_bytes,
    )
    .context("the writer bench's logical artifact must be a reconstruction control")?;

    let config_bytes = std::fs::read(config_source_path)
        .with_context(|| format!("reading {}", config_source_path.display()))?;
    let config_digest = digest_bytes(&config_bytes);
    let resolved = resolved_config_from(&config_bytes, &config_source_path.display().to_string())?;
    let config = WriterConfig::from_resolved_config(&resolved)?;
    let compression = config.compression.clone();

    let prepared = prepare(artifact_dir, &reconstruction, config)?;

    let mut out = Vec::with_capacity(samples as usize);
    for _ in 0..samples {
        out.push(one_sample(&prepared)?);
    }

    let median_wall = median_f64(out.iter().map(|s| s.wall_seconds).collect());
    let median_output = median_u64(out.iter().map(|s| s.output_bytes).collect());
    let report = WriterBenchReport {
        report_version: WRITE_BENCH_VERSION.to_string(),
        layer: "writer".to_string(),
        artifact_digest,
        config_digest,
        compression,
        total_rows: prepared.total_rows,
        logical_input_bytes: prepared.logical_bytes,
        git_sha: std::env::var("UKIEL_GIT_SHA").unwrap_or_else(|_| "unknown".to_string()),
        host: host_block(),
        samples: out,
        median_wall_seconds: median_wall,
        median_output_bytes: median_output,
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

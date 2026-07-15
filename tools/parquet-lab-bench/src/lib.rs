//! `parquet-lab-bench` — run declared queries read-only over one laboratory artifact.
//!
//! READ-ONLY. It measures an artifact that already exists; it cannot generate, rewrite,
//! index, or compact one. It exposes a physical variant through the suite's logical
//! projection, proves every query's normalized answer equals the control's before timing,
//! runs a cold and several warm iterations, captures the physical plan with metrics, and —
//! in object-store mode — accounts for every read.

pub mod compare;
pub mod counting_store;
pub mod runner;
pub mod suites;

use std::path::Path;

use anyhow::{Context, Result, bail};
use parquet_lab_contract::{
    HostInfo, ReportIdentity, RunReport, SnapshotManifest, Suite, SuiteKind, ToolVersions,
    VariantManifest, digest_bytes,
};

use crate::runner::{Artifact, Mode, ReaderFlags, run_query};

/// The identity of the artifact being measured, for the report envelope.
pub struct ArtifactIdentity {
    pub snapshot_digest: String,
    pub variant_digest: Option<String>,
}

/// Load an artifact's files from a snapshot *or* variant manifest.
pub fn load_artifact(manifest_path: &Path) -> Result<(Artifact, ArtifactIdentity)> {
    let dir = manifest_path.parent().unwrap_or(Path::new("."));
    let bytes = std::fs::read(manifest_path)
        .with_context(|| format!("reading {}", manifest_path.display()))?;
    let digest = digest_bytes(&bytes);
    let name = manifest_path.display().to_string();

    // A snapshot manifest lists files by `path`; a variant by `output.path`.
    if let Ok(snapshot) = SnapshotManifest::parse(&name, &bytes) {
        let files = read_files(dir, snapshot.files.iter().map(|f| f.path.as_str()))?;
        return Ok((
            Artifact { files },
            ArtifactIdentity {
                snapshot_digest: digest,
                variant_digest: None,
            },
        ));
    }
    let variant = VariantManifest::parse(&name, &bytes)
        .context("manifest is neither a snapshot nor a variant")?;
    let files = read_files(dir, variant.files.iter().map(|f| f.output.path.as_str()))?;
    Ok((
        Artifact { files },
        ArtifactIdentity {
            snapshot_digest: variant.parent_snapshot_digest,
            variant_digest: Some(digest),
        },
    ))
}

fn read_files<'a>(
    dir: &Path,
    paths: impl Iterator<Item = &'a str>,
) -> Result<Vec<(String, Vec<u8>)>> {
    let mut files = Vec::new();
    for p in paths {
        let bytes = std::fs::read(dir.join(p)).with_context(|| format!("reading {p}"))?;
        files.push((p.to_string(), bytes));
    }
    if files.is_empty() {
        bail!("the artifact lists no files");
    }
    Ok(files)
}

/// Compile a suite from a snapshot control: bake the logical view and each query's expected
/// result digest.
pub async fn compile(
    snapshot_manifest_path: &Path,
    kind: SuiteKind,
    sql_path: &Path,
    suite_out: &Path,
    replace: bool,
) -> Result<()> {
    if suite_out.exists() && !replace {
        bail!("{} already exists; pass --replace", suite_out.display());
    }
    let snap_bytes = std::fs::read(snapshot_manifest_path)
        .with_context(|| format!("reading {}", snapshot_manifest_path.display()))?;
    let snapshot =
        SnapshotManifest::parse(&snapshot_manifest_path.display().to_string(), &snap_bytes)?;
    let view_sql = suites::build_view_sql(&snapshot)?;

    let sql_text = std::fs::read_to_string(sql_path)
        .with_context(|| format!("reading {}", sql_path.display()))?;
    let statements = suites::parse_sql_queries(&sql_text)?;

    // Run each query against the control to get its expected digest.
    let (artifact, _) = load_artifact(snapshot_manifest_path)?;
    let session = artifact
        .session(Mode::Local, ReaderFlags::default(), &view_sql)
        .await?;

    let mut queries = Vec::with_capacity(statements.len());
    for (name, sql) in statements {
        let run = run_query(&session, &sql).await?;
        let digest = compare::result_digest(&run.schema, &run.batches)?;
        queries.push(suites::to_query(name, sql, digest));
    }

    let suite = Suite {
        suite_version: parquet_lab_contract::SUITE_VERSION.to_string(),
        kind,
        view_sql,
        queries,
        probes: Vec::new(),
    };
    suite.validate(&suite_out.display().to_string())?;
    write_atomic(suite_out, &serde_json::to_vec_pretty(&suite)?)?;
    Ok(())
}

/// The run parameters.
pub struct RunParams {
    pub mode: Mode,
    pub cold_iters: usize,
    pub warm_iters: usize,
    pub reader_flags: ReaderFlags,
    pub run_order: u32,
}

/// Run a suite over an artifact and write the result report.
pub async fn run(
    manifest_path: &Path,
    suite_path: &Path,
    result_path: &Path,
    params: RunParams,
    replace: bool,
) -> Result<()> {
    if result_path.exists() && !replace {
        bail!("{} already exists; pass --replace", result_path.display());
    }
    let suite_bytes =
        std::fs::read(suite_path).with_context(|| format!("reading {}", suite_path.display()))?;
    let suite_digest = digest_bytes(&suite_bytes);
    let suite = Suite::parse(&suite_path.display().to_string(), &suite_bytes)?;

    let (artifact, identity) = load_artifact(manifest_path)?;

    // Queries and probes run through the identical path; a probe is a named query with a
    // selectivity band attached.
    struct Runnable {
        name: String,
        sql: String,
        expected: String,
    }
    let mut runnables: Vec<Runnable> = suite
        .queries
        .iter()
        .map(|q| Runnable {
            name: q.name.clone(),
            sql: q.sql.clone(),
            expected: q.expected_result_digest.clone(),
        })
        .collect();
    for p in &suite.probes {
        runnables.push(Runnable {
            name: p.name.clone(),
            sql: p.sql.clone(),
            expected: p.expected_result_digest.clone().unwrap_or_default(),
        });
    }

    let mut query_reports = Vec::new();
    for q in &runnables {
        // Cold iterations: a fresh session (empty metadata cache) each time.
        let mut cold_ms = Vec::with_capacity(params.cold_iters);
        let mut result_digest = String::new();
        let mut result_rows = 0usize;
        for _ in 0..params.cold_iters.max(1) {
            let session = artifact
                .session(params.mode, params.reader_flags, &suite.view_sql)
                .await?;
            let run = run_query(&session, &q.sql).await?;
            result_digest = compare::result_digest(&run.schema, &run.batches)?;
            result_rows = compare::row_count(&run.batches);
            cold_ms.push(run.elapsed_ms);
        }

        // Correctness gate: the answer must equal the control's before any warm timing.
        if !q.expected.is_empty() && result_digest != q.expected {
            bail!(
                "query '{}' returned a different answer than the control (digest {} != {}). The \
                 variant is rejected — a changed answer is never a performance result.",
                q.name,
                result_digest,
                q.expected
            );
        }

        // Warm iterations reuse one session.
        let session = artifact
            .session(params.mode, params.reader_flags, &suite.view_sql)
            .await?;
        let mut warm_ms = Vec::with_capacity(params.warm_iters);
        let mut io = None;
        for _ in 0..params.warm_iters.max(1) {
            let run = run_query(&session, &q.sql).await?;
            warm_ms.push(run.elapsed_ms);
            io = run.io;
        }
        let plan = runner::physical_plan(&session, &q.sql).await?;

        query_reports.push(serde_json::json!({
            "name": q.name,
            "sql": q.sql,
            "cold_ms": cold_ms,
            "warm_ms": warm_ms,
            "warm_median_ms": median(&warm_ms),
            "result_digest": result_digest,
            "result_rows": result_rows,
            "expected_match": q.expected.is_empty() || result_digest == q.expected,
            "io": io,
            "plan": plan,
        }));
    }

    let report = RunReport {
        identity: ReportIdentity {
            report_version: parquet_lab_contract::REPORT_VERSION.to_string(),
            tool_versions: tool_versions(),
            snapshot_digest: identity.snapshot_digest,
            variant_digest: identity.variant_digest,
            suite_digest: Some(suite_digest),
            skip_digest: None,
            run_order: params.run_order,
        },
        host: host_info(params.mode),
        body: serde_json::json!({
            "mode": match params.mode { Mode::Local => "local", Mode::ObjectStore => "object-store" },
            "reader_flags": params.reader_flags,
            "cold_iters": params.cold_iters,
            "warm_iters": params.warm_iters,
            "queries": query_reports,
        }),
    };
    write_atomic(result_path, &serde_json::to_vec_pretty(&report)?)?;
    Ok(())
}

fn median(v: &[f64]) -> f64 {
    if v.is_empty() {
        return 0.0;
    }
    let mut s = v.to_vec();
    s.sort_by(|a, b| a.partial_cmp(b).unwrap());
    s[s.len() / 2]
}

fn tool_versions() -> ToolVersions {
    ToolVersions {
        git_sha: std::env::var("UKIEL_GIT_SHA").unwrap_or_else(|_| "unknown".to_string()),
        arrow: "58.3".to_string(),
        parquet: "58.3".to_string(),
        datafusion: "54".to_string(),
    }
}

fn host_info(mode: Mode) -> HostInfo {
    HostInfo {
        target_cpu: option_env!("TARGET").map(|s| s.to_string()),
        host_cpu: None,
        host_ram_bytes: None,
        kernel: None,
        storage_kind: match mode {
            Mode::Local => "local".to_string(),
            Mode::ObjectStore => "object-store(in-memory)".to_string(),
        },
        // We do not drop the host page cache; do not claim we did.
        page_cache_dropped: None,
    }
}

fn write_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    let tmp = path.with_extension("json.tmp");
    std::fs::write(&tmp, bytes).with_context(|| format!("writing {}", tmp.display()))?;
    std::fs::rename(&tmp, path).with_context(|| format!("publishing {}", path.display()))?;
    Ok(())
}

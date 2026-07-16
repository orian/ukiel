//! `parquet-lab-bench` — run declared queries read-only over one laboratory artifact.
//!
//! READ-ONLY. It measures an artifact that already exists; it cannot generate, rewrite,
//! index, or compact one. It exposes a physical variant through the suite's logical
//! projection, proves every query's normalized answer equals the control's before timing,
//! runs a cold and several warm iterations, captures the physical plan with metrics, and —
//! in object-store mode — accounts for every read.

pub mod compare;
pub mod counting_store;
pub mod plan_guard;
pub mod runner;
pub mod skip;
pub mod skip_scan;
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

/// Load an artifact's file *paths* from a snapshot *or* variant manifest. Bytes are not read
/// here — the session's backing store reads them lazily (no preload in publishable modes).
pub fn load_artifact(manifest_path: &Path) -> Result<(Artifact, ArtifactIdentity)> {
    let dir = manifest_path
        .parent()
        .unwrap_or(Path::new("."))
        .to_path_buf();
    let bytes = std::fs::read(manifest_path)
        .with_context(|| format!("reading {}", manifest_path.display()))?;
    let digest = digest_bytes(&bytes);
    let name = manifest_path.display().to_string();

    // A snapshot manifest lists files by `path`; a variant by `output.path`.
    let (files, identity) = if let Ok(snapshot) = SnapshotManifest::parse(&name, &bytes) {
        (
            snapshot
                .files
                .iter()
                .map(|f| f.path.clone())
                .collect::<Vec<_>>(),
            ArtifactIdentity {
                snapshot_digest: digest,
                variant_digest: None,
            },
        )
    } else {
        let variant = VariantManifest::parse(&name, &bytes)
            .context("manifest is neither a snapshot nor a variant")?;
        (
            variant
                .files
                .iter()
                .map(|f| f.output.path.clone())
                .collect::<Vec<_>>(),
            ArtifactIdentity {
                snapshot_digest: variant.parent_snapshot_digest,
                variant_digest: Some(digest),
            },
        )
    };
    if files.is_empty() {
        bail!("the artifact lists no files");
    }
    Ok((
        Artifact {
            base_dir: dir,
            files,
        },
        identity,
    ))
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
        let digest = compare::result_digest(
            &run.schema,
            &run.batches,
            parquet_lab_contract::ResultSemantics::Ordered,
        )?;
        queries.push(suites::to_query(name, sql, digest));
    }

    let suite = Suite {
        suite_version: parquet_lab_contract::SUITE_VERSION.to_string(),
        kind,
        view_sql,
        queries,
        probes: Vec::new(),
        skipped_probes: Vec::new(),
    };
    suite.validate(&suite_out.display().to_string())?;
    write_atomic(suite_out, &serde_json::to_vec_pretty(&suite)?)?;
    Ok(())
}

/// Compile a suite with queries *and* structured selectivity probes against the control.
/// Each probe is compiled (typed literals, exact digest, observed selectivity) or recorded
/// with a stable skip reason; a non-empty probe request that compiles nothing is refused.
pub async fn compile_suite(
    control_manifest_path: &Path,
    kind: SuiteKind,
    queries_path: &Path,
    probes_path: &Path,
    suite_out: &Path,
    replace: bool,
) -> Result<()> {
    if suite_out.exists() && !replace {
        bail!("{} already exists; pass --replace", suite_out.display());
    }
    let snap_bytes = std::fs::read(control_manifest_path)
        .with_context(|| format!("reading {}", control_manifest_path.display()))?;
    let snapshot =
        SnapshotManifest::parse(&control_manifest_path.display().to_string(), &snap_bytes)?;
    let view_sql = suites::build_view_sql(&snapshot)?;
    let control_rows = snapshot.total_rows;

    let (artifact, _) = load_artifact(control_manifest_path)?;
    let session = artifact
        .session(Mode::Local, ReaderFlags::default(), &view_sql)
        .await?;

    // Queries.
    let sql_text = std::fs::read_to_string(queries_path)
        .with_context(|| format!("reading {}", queries_path.display()))?;
    let mut queries = Vec::new();
    for (name, sql) in suites::parse_sql_queries(&sql_text)? {
        let run = run_query(&session, &sql).await?;
        let digest = compare::result_digest(
            &run.schema,
            &run.batches,
            parquet_lab_contract::ResultSemantics::Ordered,
        )?;
        queries.push(suites::to_query(name, sql, digest));
    }

    // Probes.
    let probe_bytes =
        std::fs::read(probes_path).with_context(|| format!("reading {}", probes_path.display()))?;
    let requests =
        suites::probes::parse_requests(&probe_bytes, &probes_path.display().to_string())?;
    let mut probes = Vec::new();
    let mut skipped = Vec::new();
    for req in &requests {
        match suites::probes::compile_probe(&session, req, control_rows).await? {
            suites::probes::Compiled::Probe(p) => probes.push(*p),
            suites::probes::Compiled::Skipped(s) => skipped.push(s),
        }
    }
    if !requests.is_empty() && probes.is_empty() {
        bail!(
            "no requested probe compiled ({} skipped). Blocks A/D/E/F require a non-empty probe \
             set; check the probe columns exist and can realize a band. Skips: {:?}",
            skipped.len(),
            skipped
        );
    }

    let suite = Suite {
        suite_version: parquet_lab_contract::SUITE_VERSION.to_string(),
        kind,
        view_sql,
        queries,
        probes,
        skipped_probes: skipped,
    };
    suite.validate(&suite_out.display().to_string())?;
    write_atomic(suite_out, &serde_json::to_vec_pretty(&suite)?)?;
    Ok(())
}

/// Compile a suite from the seven registered Plan-49 query classes, bound onto real columns
/// by a class-bindings file. Each class carries its required columns, sink, and predicate
/// shape, so the run-time plan guard can prove the optimizer measured the declared work.
pub async fn compile_classes(
    control_manifest_path: &Path,
    kind: SuiteKind,
    bindings_path: &Path,
    suite_out: &Path,
    replace: bool,
) -> Result<()> {
    if suite_out.exists() && !replace {
        bail!("{} already exists; pass --replace", suite_out.display());
    }
    let snap_bytes = std::fs::read(control_manifest_path)
        .with_context(|| format!("reading {}", control_manifest_path.display()))?;
    let snapshot =
        SnapshotManifest::parse(&control_manifest_path.display().to_string(), &snap_bytes)?;
    let view_sql = suites::build_view_sql(&snapshot)?;

    let bindings_bytes = std::fs::read(bindings_path)
        .with_context(|| format!("reading {}", bindings_path.display()))?;
    let bindings: suites::classes::ClassBindings = serde_json::from_slice(&bindings_bytes)
        .with_context(|| format!("{}: not class bindings", bindings_path.display()))?;
    let classes = suites::classes::seven_classes(&bindings)?;

    let (artifact, _) = load_artifact(control_manifest_path)?;
    let session = artifact
        .session(Mode::Local, ReaderFlags::default(), &view_sql)
        .await?;

    let mut queries = Vec::with_capacity(classes.len());
    for c in classes {
        let run = run_query(&session, &c.sql).await?;
        let digest = compare::result_digest(
            &run.schema,
            &run.batches,
            parquet_lab_contract::ResultSemantics::Ordered,
        )?;
        // The plan guard must accept the control's own plan, or the class is mis-declared.
        let plan = runner::physical_plan(&session, &c.sql).await?;
        plan_guard::assert_physical_plan(&c.name, &plan, &c.required_columns, c.sink, c.predicate_shape)
            .with_context(|| format!("class '{}' is mis-declared against the control", c.name))?;
        queries.push(parquet_lab_contract::Query {
            name: c.name,
            sql: c.sql,
            expected_result_digest: digest,
            result_semantics: parquet_lab_contract::ResultSemantics::Ordered,
            required_columns: c.required_columns,
            sink: c.sink,
            predicate_shape: c.predicate_shape,
        });
    }

    let suite = Suite {
        suite_version: parquet_lab_contract::SUITE_VERSION.to_string(),
        kind,
        view_sql,
        queries,
        probes: Vec::new(),
        skipped_probes: Vec::new(),
    };
    suite.validate(&suite_out.display().to_string())?;
    write_atomic(suite_out, &serde_json::to_vec_pretty(&suite)?)?;
    Ok(())
}

/// The run parameters.
///
/// `cold_iters`/`warm_iters` name what they physically are — the number of *fresh-session*
/// and *reused-session* samples. The report vocabulary uses those honest names; a fresh
/// DataFusion session has an empty metadata cache but says nothing about the OS page cache,
/// which is why a `cache_receipt` (not the word "cold") is what backs a cache-state claim.
pub struct RunParams {
    pub mode: Mode,
    /// Fresh-session sample count (a new session, empty metadata cache, each time).
    pub cold_iters: usize,
    /// Reused-session sample count (one session, warm metadata cache).
    pub warm_iters: usize,
    pub reader_flags: ReaderFlags,
    pub run_order: u32,
    /// An optional experimental skip-index sidecar to price against native pruning. It is
    /// verified bound to the variant being measured before it is credited.
    pub skip_manifest: Option<std::path::PathBuf>,
    /// The verified OS-cache profile this run executed under. Its digest and residency are
    /// bound into the report so a cache-state claim is provable; a fresh session alone is
    /// never called "OS cold".
    pub cache_receipt: Option<std::path::PathBuf>,
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

    // Bind the verified OS-cache profile, if one was supplied. A fresh DataFusion session is
    // never dressed up as "OS cold" without a valid, matching receipt.
    let manifest_digest = digest_bytes(&std::fs::read(manifest_path)?);
    let cache_binding: Option<serde_json::Value> = match &params.cache_receipt {
        None => None,
        Some(rp) => {
            let rb = std::fs::read(rp).with_context(|| format!("reading {}", rp.display()))?;
            let receipt =
                parquet_lab_contract::CacheReceipt::parse(&rp.display().to_string(), &rb)?;
            if !receipt.is_usable() {
                bail!(
                    "cache receipt {} is invalid ({:?} was not achieved); refusing to bind an \
                     unachieved cache state to a timing",
                    rp.display(),
                    receipt.requested_profile
                );
            }
            if receipt.target_manifest_digest != manifest_digest {
                bail!(
                    "cache receipt was prepared for a different artifact ({} != {manifest_digest})",
                    receipt.target_manifest_digest
                );
            }
            Some(serde_json::json!({
                "receipt_digest": digest_bytes(&rb),
                "profile": format!("{:?}", receipt.requested_profile),
                "residency_after": receipt.residency_after.resident_fraction,
            }))
        }
    };

    // A skip sidecar, if given, is bound to the variant and priced. Only a variant carries
    // the output file digests a sidecar binds to.
    let skip_cost = match &params.skip_manifest {
        None => None,
        Some(path) => {
            let mbytes = std::fs::read(manifest_path)?;
            let variant = VariantManifest::parse(&manifest_path.display().to_string(), &mbytes)
                .context("a --skip-manifest run must measure a variant (a sidecar binds to variant files)")?;
            let files: Vec<parquet_lab_contract::FileDigest> =
                variant.files.iter().map(|f| f.output.clone()).collect();
            Some(skip::load_and_bind(path, &digest_bytes(&mbytes), &files)?)
        }
    };

    // Queries and probes run through the identical path; a probe is a named query with a
    // selectivity band attached.
    struct Runnable {
        name: String,
        sql: String,
        expected: String,
        semantics: parquet_lab_contract::ResultSemantics,
        required_columns: Vec<String>,
        sink: parquet_lab_contract::QuerySink,
        predicate_shape: parquet_lab_contract::PredicateShape,
    }
    let mut runnables: Vec<Runnable> = suite
        .queries
        .iter()
        .map(|q| Runnable {
            name: q.name.clone(),
            sql: q.sql.clone(),
            expected: q.expected_result_digest.clone(),
            semantics: q.result_semantics,
            required_columns: q.required_columns.clone(),
            sink: q.sink,
            predicate_shape: q.predicate_shape,
        })
        .collect();
    for p in &suite.probes {
        runnables.push(Runnable {
            name: p.name.clone(),
            sql: p.sql.clone(),
            expected: p.expected_result_digest.clone(),
            semantics: p.result_semantics,
            required_columns: Vec::new(),
            sink: parquet_lab_contract::QuerySink::default(),
            predicate_shape: parquet_lab_contract::PredicateShape::default(),
        });
    }

    let mut query_reports = Vec::new();
    for q in &runnables {
        // Fresh-session samples: a new session (empty metadata cache) each time.
        let mut fresh_session_ms = Vec::with_capacity(params.cold_iters);
        let mut result_digest = String::new();
        let mut result_rows = 0usize;
        for _ in 0..params.cold_iters.max(1) {
            let session = artifact
                .session(params.mode, params.reader_flags, &suite.view_sql)
                .await?;
            let run = run_query(&session, &q.sql).await?;
            result_digest = compare::result_digest(&run.schema, &run.batches, q.semantics)?;
            result_rows = compare::row_count(&run.batches);
            fresh_session_ms.push(run.elapsed_ms);
        }

        // Correctness gate: the answer must equal the control's before any timing is trusted.
        if !q.expected.is_empty() && result_digest != q.expected {
            bail!(
                "query '{}' returned a different answer than the control (digest {} != {}). The \
                 variant is rejected — a changed answer is never a performance result.",
                q.name,
                result_digest,
                q.expected
            );
        }

        // Reused-session samples reuse one session.
        let session = artifact
            .session(params.mode, params.reader_flags, &suite.view_sql)
            .await?;
        let mut reused_session_ms = Vec::with_capacity(params.warm_iters);
        let mut io = None;
        for _ in 0..params.warm_iters.max(1) {
            let run = run_query(&session, &q.sql).await?;
            reused_session_ms.push(run.elapsed_ms);
            io = run.io;
        }
        let plan = runner::physical_plan(&session, &q.sql).await?;

        // Physical-plan assertion: the optimizer must not have removed the declared work.
        // A failure rejects the timing (a query with no declared class is a no-op here).
        plan_guard::assert_physical_plan(
            &q.name,
            &plan,
            &q.required_columns,
            q.sink,
            q.predicate_shape,
        )?;

        query_reports.push(serde_json::json!({
            "name": q.name,
            "sql": q.sql,
            "fresh_session_ms": fresh_session_ms,
            "reused_session_ms": reused_session_ms,
            "reused_session_median_ms": median(&reused_session_ms),
            "result_digest": result_digest,
            "result_rows": result_rows,
            "expected_match": q.expected.is_empty() || result_digest == q.expected,
            "sink": format!("{:?}", q.sink),
            "predicate_shape": format!("{:?}", q.predicate_shape),
            "plan_assertion_passed": true,
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
            skip_digest: skip_cost.as_ref().map(|c| c.skip_digest.clone()),
            run_order: params.run_order,
            // Run-set fields (repetition/seed/order digest) are populated when a run set
            // drives the run (Task 47E); an ad-hoc single run leaves them null. The backend
            // identity is always known from the mode.
            repetition: None,
            seed: None,
            order_digest: None,
            backend: Some(match params.mode {
                Mode::Memory => "memory".to_string(),
                Mode::Local => "local".to_string(),
                Mode::ObjectStore => "object-store".to_string(),
            }),
        },
        host: host_info(params.mode),
        body: serde_json::json!({
            "mode": match params.mode { Mode::Memory => "memory", Mode::Local => "local", Mode::ObjectStore => "object-store" },
            "reader_flags": params.reader_flags,
            "fresh_session_samples": params.cold_iters,
            "reused_session_samples": params.warm_iters,
            // The OS-cache profile this run ran under, backed by a verified receipt. `null`
            // means no cache state was pinned — an exploratory run, never an "OS cold" claim.
            "cache": cache_binding,
            "skip_index": skip_cost,
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
            Mode::Memory => "memory".to_string(),
            Mode::Local => "local".to_string(),
            Mode::ObjectStore => "object-store".to_string(),
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

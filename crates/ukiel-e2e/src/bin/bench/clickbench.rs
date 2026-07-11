//! The official ClickBench suite (plan 32): the 43 queries, verbatim, over the
//! `hits_cb` fixture — plus a **raw-DataFusion reference runner** over the same
//! parquet files, so every query gets a `ukiel / raw-DataFusion` ratio.
//!
//! That ratio is the deliverable: raw DataFusion is the ceiling (same engine,
//! same version, zero catalog/provider/cache), so the gap is the measured cost
//! of ukiel's stack. Both runners execute the *same* adapted SQL text from
//! `bench/queries/clickbench/queries.sql` — every deviation from upstream is
//! logged in the ADAPTATIONS.md next to it.
//!
//! Methodology is ClickBench's own: per query, one **cold** run (fresh session,
//! fresh metadata cache) then N **hot** runs; the headline is the hot minimum.
//! "Cold" is honestly only cold at the ukiel layer — the OS page cache and
//! MinIO keep the bytes — which the report states rather than hides.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, bail};
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use serde::{Deserialize, Serialize};
use ukiel_e2e::Stack;
use ukiel_query::metadata_cache::ParquetMetadataCache;

use crate::hits::HITS_CB;

fn queries_path() -> PathBuf {
    PathBuf::from("bench/queries/clickbench/queries.sql")
}

/// The vendored suite: one query per line, `--` comments and blanks skipped.
/// Numbered q01..q43 in upstream order.
fn load_queries() -> anyhow::Result<Vec<(String, String)>> {
    let path = queries_path();
    let text = std::fs::read_to_string(&path)
        .with_context(|| format!("read {} (vendored ClickBench suite)", path.display()))?;
    let queries: Vec<(String, String)> = text
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with("--"))
        .enumerate()
        .map(|(i, sql)| {
            (
                format!("q{:02}", i + 1),
                sql.trim_end_matches(';').to_string(),
            )
        })
        .collect();
    if queries.len() != 43 {
        bail!(
            "expected ClickBench's 43 queries in {}, found {}",
            path.display(),
            queries.len()
        );
    }
    Ok(queries)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResult {
    pub name: String,
    /// Fresh-session run: ukiel's caches are empty.
    pub cold_ms: f64,
    /// The hot runs, in order.
    pub hot_ms: Vec<f64>,
    /// Headline (ClickBench's own): the fastest hot run.
    pub hot_min_ms: f64,
    pub hot_median_ms: f64,
    pub rows: usize,
    /// Present iff the query failed — partial coverage must be loud.
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SuiteReport {
    pub label: String,
    /// "ukiel" or "raw-datafusion".
    pub engine: String,
    pub table_rows: i64,
    pub queries: Vec<QueryResult>,
}

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    match v.len() {
        0 => 0.0,
        n if n % 2 == 1 => v[n / 2],
        n => (v[n / 2 - 1] + v[n / 2]) / 2.0,
    }
}

/// Runs `sql`, returning elapsed ms and the row count. Collects the full result
/// so the timing covers execution, not just planning.
async fn timed(ctx: &SessionContext, sql: &str) -> anyhow::Result<(f64, usize)> {
    let start = Instant::now();
    let batches = ctx.sql(sql).await?.collect().await?;
    let ms = start.elapsed().as_secs_f64() * 1000.0;
    Ok((ms, batches.iter().map(|b| b.num_rows()).sum()))
}

/// Builds a fresh ukiel operator session with an empty metadata cache — a cold
/// session, in the only sense ukiel controls.
async fn cold_ukiel_session(stack: &Stack) -> anyhow::Result<SessionContext> {
    Ok(ukiel_query::context::operator_session(
        &stack.catalog,
        stack.store.clone(),
        &stack.store_url,
        Arc::new(ParquetMetadataCache::default()),
    )
    .await?)
}

/// Runs the suite against a session factory: one cold run on a fresh session,
/// then `iters` hot runs on a session kept warm across all queries.
async fn run_suite<F, Fut>(
    engine: &str,
    label: &str,
    iters: usize,
    table_rows: i64,
    new_session: F,
) -> anyhow::Result<SuiteReport>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<SessionContext>>,
{
    let queries = load_queries()?;
    let warm = new_session().await?;
    let mut results = Vec::with_capacity(queries.len());

    for (name, sql) in &queries {
        // Cold: a brand-new session per query, so no earlier query has warmed
        // this one's footers.
        let cold_ctx = new_session().await?;
        let cold = match timed(&cold_ctx, sql).await {
            Ok((ms, _)) => ms,
            Err(e) => {
                println!("FAIL {engine}/{name}: {e}");
                results.push(QueryResult {
                    name: name.clone(),
                    cold_ms: 0.0,
                    hot_ms: vec![],
                    hot_min_ms: 0.0,
                    hot_median_ms: 0.0,
                    rows: 0,
                    error: Some(e.to_string()),
                });
                continue;
            }
        };

        let mut hot = Vec::with_capacity(iters);
        let mut rows = 0;
        for _ in 0..iters {
            let (ms, n) = timed(&warm, sql).await?;
            hot.push(ms);
            rows = n;
        }
        let hot_min = hot.iter().copied().fold(f64::INFINITY, f64::min);
        let hot_median = median(hot.clone());
        println!(
            "PERF {engine}/{name}={hot_min:.1} (cold {cold:.1}, median {hot_median:.1}, {rows} rows)"
        );
        results.push(QueryResult {
            name: name.clone(),
            cold_ms: cold,
            hot_ms: hot,
            hot_min_ms: hot_min,
            hot_median_ms: hot_median,
            rows,
            error: None,
        });
    }

    Ok(SuiteReport {
        label: label.to_string(),
        engine: engine.to_string(),
        table_rows,
        queries: results,
    })
}

/// Reports the run, writes the JSON, and fails loudly if any query errored —
/// *after* running the rest, so one broken query never hides the other 42.
fn finish(report: SuiteReport, file: &str) -> anyhow::Result<SuiteReport> {
    let failed: Vec<&str> = report
        .queries
        .iter()
        .filter(|q| q.error.is_some())
        .map(|q| q.name.as_str())
        .collect();
    let out = crate::report::write_json(file, &report)?;
    println!("wrote {}", out.display());
    if !failed.is_empty() {
        bail!(
            "{}/43 ClickBench queries failed on {}: {} — the suite is not fully covered",
            failed.len(),
            report.engine,
            failed.join(", ")
        );
    }
    Ok(report)
}

/// `bench clickbench run [--iters N] [--label L]`: the 43 queries through
/// ukiel's full read stack (catalog planning, provider, pruning, footer cache)
/// via the unscoped operator session.
pub async fn run(iters: usize, label: &str) -> anyhow::Result<()> {
    let stack = Stack::start().await;
    let ht = stack
        .catalog
        .get_hypertable(HITS_CB.table)
        .await
        .context("hits_cb not loaded — run `bench clickbench load` first")?;
    let parts = stack.catalog.live_parts(ht.id, None).await?;
    let table_rows: i64 = parts.iter().map(|p| p.meta.row_count).sum();
    println!(
        "hits_cb: {table_rows} rows in {} live parts (fan-out is the read-side cost the ratio exposes)",
        parts.len()
    );

    let report = run_suite("ukiel", label, iters, table_rows, || {
        cold_ukiel_session(&stack)
    })
    .await?;
    finish(report, &format!("clickbench-ukiel-{label}.json"))?;
    Ok(())
}

/// Registers the raw ClickBench parquet with **bare** DataFusion — no ukiel
/// anywhere. Mirrors upstream's own `create.sql`: `binary_as_string` so the
/// Binary string columns read as strings, then a view exposing exactly the 25
/// columns `hits_cb` stores, under the same name, so `SELECT *` (q24) means the
/// same thing on both sides. Types stay the source's (narrow ints) — that is
/// the engine's true ceiling, and the type-widening delta is logged in
/// ADAPTATIONS.md rather than engineered away.
async fn raw_session() -> anyhow::Result<SessionContext> {
    let ctx = SessionContext::new();
    let dir = "bench/datasets/hits/";
    let opts = ParquetReadOptions::default().parquet_pruning(true);
    ctx.register_parquet("hits_raw", dir, opts)
        .await
        .with_context(|| {
            format!("register raw parquet from {dir} — run `make bench-fetch-hits`")
        })?;

    let cols = HITS_CB
        .cols
        .iter()
        .map(|c| format!("\"{}\"", c.src))
        .collect::<Vec<_>>()
        .join(", ");
    ctx.sql(&format!(
        "CREATE OR REPLACE VIEW hits_cb AS SELECT {cols} FROM hits_raw"
    ))
    .await?
    .collect()
    .await?;
    Ok(ctx)
}

/// `bench clickbench raw [--iters N] [--label L]`: the same 43 queries on bare
/// DataFusion over the same parquet — the reference ceiling.
pub async fn raw(iters: usize, label: &str) -> anyhow::Result<()> {
    let probe = raw_session().await?;
    let rows = probe
        .sql("SELECT count(*) AS n FROM hits_cb")
        .await?
        .collect()
        .await?;
    let table_rows = rows
        .first()
        .and_then(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<datafusion::arrow::array::Int64Array>()
                .map(|a| a.value(0))
        })
        .unwrap_or(0);
    println!("raw parquet: {table_rows} rows (bare DataFusion, no ukiel)");

    let report = run_suite("raw-datafusion", label, iters, table_rows, || async {
        raw_session().await
    })
    .await?;
    finish(report, &format!("clickbench-raw-{label}.json"))?;
    Ok(())
}

fn read_report(file: &str) -> anyhow::Result<SuiteReport> {
    let path = PathBuf::from("bench/results").join(file);
    let f = std::fs::File::open(&path)
        .with_context(|| format!("read {} — run the matching suite first", path.display()))?;
    Ok(serde_json::from_reader(f)?)
}

/// `bench clickbench compare --ukiel L1 --raw L2`: the per-query overhead table.
/// The ratio is ukiel's hot-min over raw's — how many times slower the full
/// stack is than the bare engine on the identical query and data.
pub async fn compare(ukiel_label: &str, raw_label: &str) -> anyhow::Result<()> {
    let u = read_report(&format!("clickbench-ukiel-{ukiel_label}.json"))?;
    let r = read_report(&format!("clickbench-raw-{raw_label}.json"))?;
    if u.table_rows != r.table_rows {
        println!(
            "WARNING: row counts differ (ukiel {} vs raw {}) — the two fixtures are not the same data",
            u.table_rows, r.table_rows
        );
    }

    println!(
        "\n{:<5} {:>12} {:>12} {:>8}   verdict",
        "query", "ukiel (ms)", "raw (ms)", "ratio"
    );
    println!("{}", "-".repeat(60));
    let mut ratios = Vec::new();
    let (mut u_total, mut r_total) = (0.0, 0.0);
    for uq in &u.queries {
        let Some(rq) = r.queries.iter().find(|q| q.name == uq.name) else {
            continue;
        };
        if uq.error.is_some() || rq.error.is_some() {
            println!("{:<5} {:>12} {:>12} {:>8}   FAILED", uq.name, "-", "-", "-");
            continue;
        }
        let ratio = uq.hot_min_ms / rq.hot_min_ms.max(f64::MIN_POSITIVE);
        ratios.push(ratio);
        u_total += uq.hot_min_ms;
        r_total += rq.hot_min_ms;
        let verdict = if ratio < 1.0 { "ukiel faster" } else { "" };
        println!(
            "{:<5} {:>12.1} {:>12.1} {:>7.2}x   {verdict}",
            uq.name, uq.hot_min_ms, rq.hot_min_ms, ratio
        );
    }
    println!("{}", "-".repeat(60));
    println!(
        "{:<5} {:>12.1} {:>12.1} {:>7.2}x   (sum of hot minima)",
        "total",
        u_total,
        r_total,
        u_total / r_total.max(f64::MIN_POSITIVE)
    );
    println!(
        "{:<5} {:>12} {:>12} {:>7.2}x   (median per-query ratio)",
        "med",
        "",
        "",
        median(ratios)
    );
    println!(
        "\nRatio = ukiel / raw-DataFusion on the identical query and data: the cost of the\n\
         catalog, provider, and cache stack over the bare engine. See\n\
         bench/queries/clickbench/ADAPTATIONS.md for what else is (and is not) in that number."
    );
    Ok(())
}

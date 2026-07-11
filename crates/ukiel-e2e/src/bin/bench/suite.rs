//! Shared harness for the *official* benchmark suites (plan 32): ClickBench's
//! 43 queries and JSONBench's 5.
//!
//! Both are whole-table suites, so both run through ukiel's unscoped
//! [operator session](ukiel_query::context::operator_session), and both use
//! ClickBench's methodology: per query, one **cold** run (fresh session, fresh
//! metadata cache) then N **hot** runs; the headline is the hot minimum.
//!
//! "Cold" is honestly only cold at the ukiel layer — a fresh session drops the
//! parquet footer cache, but the OS page cache and MinIO still hold the bytes.
//! The reports say so rather than implying a cold-storage read.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, bail};
use datafusion::prelude::SessionContext;
use serde::{Deserialize, Serialize};
use ukiel_e2e::Stack;
use ukiel_query::metadata_cache::ParquetMetadataCache;

/// One vendored query: its upstream number (`q01`…) and its SQL.
pub type Query = (String, String);

/// Loads a vendored suite: one query per line, `--` comments and blanks
/// skipped, numbered in upstream order. `expect` is the suite's official query
/// count — a mismatch means the file was truncated or mis-edited, which must
/// fail loudly rather than silently benchmark a subset.
pub fn load_queries(path: &Path, expect: usize) -> anyhow::Result<Vec<Query>> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("read {} (vendored query suite)", path.display()))?;
    let queries: Vec<Query> = text
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
    if queries.len() != expect {
        bail!(
            "expected {expect} queries in {}, found {}",
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
    /// Rows in the fixture the suite ran against — results are self-describing.
    pub table_rows: i64,
    pub queries: Vec<QueryResult>,
}

pub fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    match v.len() {
        0 => 0.0,
        n if n % 2 == 1 => v[n / 2],
        n => (v[n / 2 - 1] + v[n / 2]) / 2.0,
    }
}

/// Runs `sql` to completion, returning elapsed ms and the row count. Collects
/// the full result, so the timing covers execution and not just planning.
async fn timed(ctx: &SessionContext, sql: &str) -> anyhow::Result<(f64, usize)> {
    let start = Instant::now();
    let batches = ctx.sql(sql).await?.collect().await?;
    let ms = start.elapsed().as_secs_f64() * 1000.0;
    Ok((ms, batches.iter().map(|b| b.num_rows()).sum()))
}

/// A fresh ukiel operator session with an empty metadata cache — cold, in the
/// only sense ukiel controls.
pub async fn cold_ukiel_session(stack: &Stack) -> anyhow::Result<SessionContext> {
    Ok(ukiel_query::context::operator_session(
        &stack.catalog,
        stack.store.clone(),
        &stack.store_url,
        Arc::new(ParquetMetadataCache::default()),
    )
    .await?)
}

/// Runs a suite against a session factory: one cold run on a brand-new session
/// per query (so no earlier query has warmed this one's footers), then `iters`
/// hot runs on a session kept warm across the whole suite.
///
/// A failing query is *recorded* and the suite continues — one broken query must
/// not hide the other 42. [`finish`] turns any failure into a non-zero exit.
pub async fn run_suite<F, Fut>(
    engine: &str,
    label: &str,
    iters: usize,
    table_rows: i64,
    queries: &[Query],
    new_session: F,
) -> anyhow::Result<SuiteReport>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<SessionContext>>,
{
    let warm = new_session().await?;
    let mut results = Vec::with_capacity(queries.len());

    for (name, sql) in queries {
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

/// Writes the JSON report, then fails if any query errored — *after* the whole
/// suite has run, so partial coverage is loud instead of silent.
pub fn finish(report: SuiteReport, file: &str) -> anyhow::Result<SuiteReport> {
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
            "{}/{} queries failed on {}: {} — the suite is not fully covered",
            failed.len(),
            report.queries.len(),
            report.engine,
            failed.join(", ")
        );
    }
    Ok(report)
}

pub fn read_report(file: &str) -> anyhow::Result<SuiteReport> {
    let path = PathBuf::from("bench/results").join(file);
    let f = std::fs::File::open(&path)
        .with_context(|| format!("read {} — run the matching suite first", path.display()))?;
    Ok(serde_json::from_reader(f)?)
}

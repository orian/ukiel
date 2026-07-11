//! The official ClickBench suite (plan 32): the 43 queries over the `hits_cb`
//! fixture — plus a **raw-DataFusion reference runner** over the same parquet,
//! so every query gets a `ukiel / raw-DataFusion` ratio.
//!
//! That ratio is the deliverable: raw DataFusion is the ceiling (same engine,
//! same version — upstream's own harness pins DataFusion 54, which is ukiel's
//! pin — with zero catalog, provider, or cache), so the gap is the measured cost
//! of ukiel's stack. Both runners execute the *same* adapted SQL from
//! `bench/queries/clickbench/queries.sql`; every deviation from upstream is a
//! diff against the pristine file next to it, logged in ADAPTATIONS.md.
//!
//! The shared cold/hot harness lives in [`crate::suite`].

use std::path::PathBuf;

use anyhow::{Context, bail};
use datafusion::prelude::{ParquetReadOptions, SessionContext};

use crate::hits::HITS_CB;
use crate::suite::{self, median};

/// ClickBench is 43 queries — no more, no less. A file that parses to any other
/// count is a mis-edit, and benchmarking a subset while calling it "ClickBench"
/// would be a lie.
const CLICKBENCH_QUERIES: usize = 43;

fn queries() -> anyhow::Result<Vec<suite::Query>> {
    suite::load_queries(
        &PathBuf::from("bench/queries/clickbench/queries.sql"),
        CLICKBENCH_QUERIES,
    )
}

/// `bench clickbench run [--iters N] [--label L]`: the 43 queries through
/// ukiel's full read stack (catalog planning, provider, part pruning, footer
/// cache) via the unscoped operator session.
pub async fn run(iters: usize, label: &str) -> anyhow::Result<()> {
    let stack = ukiel_e2e::Stack::start().await;
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

    let report = suite::run_suite("ukiel", label, iters, table_rows, &queries()?, || {
        suite::cold_ukiel_session(&stack)
    })
    .await?;
    suite::finish(report, &format!("clickbench-ukiel-{label}.json"))?;
    Ok(())
}

/// Registers the raw ClickBench parquet with **bare** DataFusion — no ukiel
/// anywhere. Mirrors upstream's own `create.sql`: `binary_as_string`, so the
/// Binary string columns read as strings, then a view exposing exactly the 25
/// columns `hits_cb` stores, under the same name — so `SELECT *` (q24) means the
/// same thing on both sides. Types stay the source's narrow ints: that is the
/// engine's true ceiling, and the type-widening delta is *logged* in
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
/// DataFusion over the same parquet files — the reference ceiling.
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

    let report = suite::run_suite(
        "raw-datafusion",
        label,
        iters,
        table_rows,
        &queries()?,
        || async { raw_session().await },
    )
    .await?;
    suite::finish(report, &format!("clickbench-raw-{label}.json"))?;
    Ok(())
}

/// `bench clickbench compare --ukiel L1 --raw L2`: the per-query overhead table.
/// The ratio is ukiel's hot minimum over raw's — how many times slower the full
/// stack is than the bare engine, on the identical query and data.
pub async fn compare(ukiel_label: &str, raw_label: &str) -> anyhow::Result<()> {
    let u = suite::read_report(&format!("clickbench-ukiel-{ukiel_label}.json"))?;
    let r = suite::read_report(&format!("clickbench-raw-{raw_label}.json"))?;
    if u.table_rows != r.table_rows {
        bail!(
            "row counts differ (ukiel {} vs raw {}) — the two runs are not over the same data, \
             so the ratios would be meaningless. Load the same number of files into both.",
            u.table_rows,
            r.table_rows
        );
    }

    println!(
        "\n{:<5} {:>12} {:>12} {:>8}   verdict",
        "query", "ukiel (ms)", "raw (ms)", "ratio"
    );
    println!("{}", "-".repeat(60));
    let mut ratios = Vec::new();
    let (mut u_total, mut r_total) = (0.0, 0.0);
    let mut mismatched = Vec::new();
    for uq in &u.queries {
        let Some(rq) = r.queries.iter().find(|q| q.name == uq.name) else {
            continue;
        };
        if uq.error.is_some() || rq.error.is_some() {
            println!("{:<5} {:>12} {:>12} {:>8}   FAILED", uq.name, "-", "-", "-");
            continue;
        }
        // Same query, same data: a row-count difference means the two engines
        // disagree on the *answer*, which invalidates the timing comparison.
        if uq.rows != rq.rows {
            mismatched.push(format!(
                "{} (ukiel {} vs raw {})",
                uq.name, uq.rows, rq.rows
            ));
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

    if !mismatched.is_empty() {
        bail!(
            "the two engines returned different row counts — the results disagree, so the \
             timings are not comparable:\n  {}",
            mismatched.join("\n  ")
        );
    }
    Ok(())
}

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
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};

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

/// Plan 37, H5: the same-binary lever for DataFusion's **sort-pushdown**
/// machinery (epic apache/datafusion#23036, phases 1–3 all live in our DF 54
/// pin; `enable_sort_pushdown` defaults to true).
///
/// Why this exists as a knob rather than a rebuild: the provider's declared
/// ordering feeds `eq_properties()` → `FileScanConfig::try_pushdown_sort` →
/// `ParquetSource::try_pushdown_sort` → **statistics-based file/row-group
/// reordering and runtime TopK dynamic filters** — none of which appears in
/// `EXPLAIN`. That is exactly the observed signature: identical displayed plans,
/// different execution. `UKIEL_BENCH_SORT_PUSHDOWN=0` turns the whole chain off
/// on *both* sessions in one toggle, without changing the binary — which the
/// measurement protocol requires, because rebuild-between-configs A/B is how the
/// earlier investigation misled itself.
fn apply_sort_pushdown_toggle(ctx: &SessionContext) {
    let enabled = std::env::var("UKIEL_BENCH_SORT_PUSHDOWN")
        .map(|v| v != "0")
        .unwrap_or(true);
    if !enabled {
        ctx.state_ref()
            .write()
            .config_mut()
            .options_mut()
            .optimizer
            .enable_sort_pushdown = false;
        eprintln!("note: datafusion.optimizer.enable_sort_pushdown = false (H5 lever)");
    }
}

/// The session an instrument runs against: ukiel's operator session, or the raw
/// DataFusion reference over a parquet directory. The whole method is
/// differential — same bytes, same binary, two stacks — so every instrument
/// takes this.
async fn instrument_session(raw_dir: Option<&str>) -> anyhow::Result<SessionContext> {
    let ctx = match raw_dir {
        Some(dir) => raw_session(dir).await?,
        None => {
            let stack = ukiel_e2e::Stack::start().await;
            let suite::Session::DataFusion(ctx) = suite::cold_ukiel_session(&stack).await? else {
                anyhow::bail!("the operator session is always a DataFusion session");
            };
            ctx
        }
    };
    apply_sort_pushdown_toggle(&ctx);
    Ok(ctx)
}

/// `bench clickbench plan "<SQL>" [--raw [--dir D]] [--max-files N]`: the
/// **complete** physical plan — every file group, every byte range, partitioning
/// and equivalence orderings.
///
/// This exists because `EXPLAIN` truncates its file-group list at five groups
/// and never prints equivalence properties at all. The gap-note investigation
/// concluded "textually identical plans" from that output, which was never
/// evidence: the part that could differ is the part that does not print.
pub async fn plan(statement: &str, raw_dir: Option<&str>, max_files: usize) -> anyhow::Result<()> {
    let ctx = instrument_session(raw_dir).await?;
    let plan = ctx.sql(statement).await?.create_physical_plan().await?;
    println!(
        "engine: {}
{}",
        raw_dir.map_or("ukiel (operator session)".to_string(), |d| format!(
            "raw DataFusion over {d}"
        )),
        crate::inspect::plan_report(&plan, max_files)
    );
    Ok(())
}

/// `bench clickbench analyze "<SQL>" [--raw [--dir D]] [--iters N]`: per-partition
/// compute, skew, and achieved parallelism — what `EXPLAIN ANALYZE` averages away.
pub async fn analyze(statement: &str, raw_dir: Option<&str>, iters: usize) -> anyhow::Result<()> {
    let ctx = instrument_session(raw_dir).await?;
    println!(
        "engine: {}",
        raw_dir.map_or("ukiel (operator session)".to_string(), |d| format!(
            "raw DataFusion over {d}"
        ))
    );
    for i in 1..=iters {
        // A fresh physical plan per run: metrics accumulate into the plan's own
        // nodes, so reusing one would sum every run together and hide the skew.
        let plan = ctx.sql(statement).await?.create_physical_plan().await?;
        println!("--- run {i}/{iters} ---");
        println!("{}", crate::inspect::analyze_report(&ctx, plan).await?);
    }
    Ok(())
}

/// `bench clickbench sql "<SQL>"`: one statement through the ukiel operator
/// session, results pretty-printed. The diagnosis door: `EXPLAIN`/`EXPLAIN
/// ANALYZE` on the exact stack the suite measures (partition counts, pruning,
/// pushdown) without editing the suite.
pub async fn sql(statement: &str) -> anyhow::Result<()> {
    let stack = ukiel_e2e::Stack::start().await;
    let suite::Session::DataFusion(ctx) = suite::cold_ukiel_session(&stack).await? else {
        anyhow::bail!("the operator session is always a DataFusion session");
    };
    let batches = ctx.sql(statement).await?.collect().await?;
    println!(
        "{}",
        datafusion::arrow::util::pretty::pretty_format_batches(&batches)?
    );
    Ok(())
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

    let report = suite::run_suite("ukiel", label, iters, table_rows, &queries()?, || async {
        let s = suite::cold_ukiel_session(&stack).await?;
        if let suite::Session::DataFusion(ctx) = &s {
            apply_sort_pushdown_toggle(ctx);
        }
        Ok(s)
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
///
/// **Parquet reader settings match ukiel's provider exactly** — `pushdown_filters`
/// and `reorder_filters`, both of which DataFusion leaves *off* by default.
/// This matters more than it looks: ukiel's provider opts into predicate
/// pushdown, so a reference running the stock defaults decodes rows for filtered
/// queries that ukiel skips, inflating the reference and flattering ukiel's ratio
/// on every query with a `WHERE` clause. Measured: it was worth 14.8x on q24 and
/// fabricated a "10x faster than DataFusion" result. The ratio must isolate
/// *ukiel's stack*, not ukiel's choice of a flag the reference could set too.
async fn raw_session(dir: &str) -> anyhow::Result<SessionContext> {
    let mut config = SessionConfig::new();
    config.options_mut().execution.parquet.pushdown_filters = true;
    config.options_mut().execution.parquet.reorder_filters = true;
    let ctx = SessionContext::new_with_config(config);
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

/// `bench clickbench raw [--iters N] [--label L] [--dir D]`: the same 43
/// queries on bare DataFusion over parquet files — the reference ceiling.
/// `--dir` defaults to the source dataset; pointing it at a local ukiel store
/// (`$UKIEL_E2E_STORE_DIR/ht/<id>/`) prices ukiel's *file layout* (int64
/// widening, ZSTD, sorting) with zero ukiel stack — only do that on a
/// freshly-loaded fixture, where every object in the dir is a live part
/// (after `compact`, superseded objects linger until GC and would be
/// double-counted).
pub async fn raw(iters: usize, label: &str, dir: &str) -> anyhow::Result<()> {
    let probe = raw_session(dir).await?;
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
        || async {
            let ctx = raw_session(dir).await?;
            apply_sort_pushdown_toggle(&ctx);
            Ok(suite::Session::DataFusion(ctx))
        },
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

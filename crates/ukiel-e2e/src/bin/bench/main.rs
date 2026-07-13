//! Macro perf harness (plan 30): volume-scale benchmarks on real datasets,
//! bridging the plan-11 micro harness (~100k synthetic rows) to the README's
//! PB+ intent. Manual-only — never run in CI. Build with `--release`.
//!
//! Subcommands (see docs/notes/2026-07-08-macro-perf.md for the runbook):
//!   bench hits load [--files N]                 load ClickBench hits into ukiel parts
//!   bench hits compact [--target-mb N]          fold the fixture (SizeTargeted whale-split)
//!   bench hits queries [--iters N] [--label L]  run the per-tenant read suite
//!   bench clickbench load [--files N]           load the verbatim-named hits_cb fixture
//!   bench bluesky produce --files N [...]        stream Bluesky ndjson to Kafka
//!   bench bluesky run --files N [...]            full ingest→ladder→finalization run
//!   bench bluesky jsonbench [--iters N]          official JSONBench 5-query suite
//!
//! The `hits*`/`bluesky run` suites are ukiel's own per-tenant SaaS scenarios
//! (plan 30); `clickbench` and `bluesky jsonbench` are the *official* suites,
//! vendored with a per-query adaptation log (plan 32).
//!
//! Datasets and results live under the gitignored `bench/` directory.

use std::process::ExitCode;

mod bluesky;
mod catalog;
mod catalog_load;
mod clickbench;
mod clickhouse;
mod hits;
mod inspect;
mod report;
mod suite;

const USAGE: &str = "\
bench — ukiel macro perf harness (plan 30, manual-only, run with --release)

USAGE:
    bench hits load [--files N]
    bench hits compact [--target-mb N]
    bench hits queries [--iters N] [--label LABEL]
    bench hits fanout                                      range vs bitmap pruning per tenant
    bench clickbench load [--files N] [--layout counter|ts]
    bench clickbench compact [--target-mb N]
    bench clickbench run [--iters N] [--label LABEL]        official 43 queries, ukiel stack
    bench clickbench raw [--iters N] [--label LABEL] [--dir D]  same 43, bare DataFusion
    bench clickbench plan \"SQL\" [--raw] [--dir D] [--max-files N]   full plan: groups, orderings
    bench clickbench analyze \"SQL\" [--raw] [--dir D] [--iters N]     per-partition compute + skew
    bench clickbench sql \"SQL\"                              one statement (EXPLAIN etc.) via the operator session
    bench clickbench compare --ukiel LABEL --raw LABEL      per-query overhead ratios
    bench bluesky produce --files N [--topic T] [--wave-files W]
    bench bluesky run --files N [--wave-files W] [--flush-ms MS] [--label LABEL]
    bench bluesky jsonbench [--iters N] [--label LABEL]     official JSONBench 5 queries
    bench catalog demand [--scenario steady|dashboard-surge|backfill]
    bench catalog seed --label L [--tables N] [--parts N] [--state S] [--packing-keys N]
    bench catalog read|write --label L [--mode closed|open] [--workers N] [--rate R]
        [--duration S] [--warmup S] [--timeout-ms N] [--pools N] [--connections-per-pool N]
        [--key-dist uniform|zipf] [--scenario S] [--parts-per-commit N]
    bench clickhouse load --table official|cb [--files N]   ClickHouse reference fixtures
    bench clickhouse run --table official|cb|parquet [--iters N] [--label LABEL]

Datasets: bench/datasets/{hits,bluesky}/ (fetch via `make bench-fetch-{hits,bluesky}`).
Queries:  bench/queries/{clickbench,jsonbench}/ (vendored + ADAPTATIONS.md).
Results:  bench/results/*.json (gitignored).";

#[tokio::main]
async fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match run(&args).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("bench: {e}");
            ExitCode::FAILURE
        }
    }
}

async fn run(args: &[String]) -> anyhow::Result<()> {
    match (
        args.first().map(String::as_str),
        args.get(1).map(String::as_str),
    ) {
        (None, _) | (Some("--help"), _) | (Some("-h"), _) => {
            println!("{USAGE}");
            Ok(())
        }
        (Some("clickbench"), Some("load")) => {
            hits::load_clickbench(opt_usize(args, "--files")?, opt_str(args, "--layout")).await
        }
        (Some("clickbench"), Some("compact")) => {
            hits::compact(&hits::HITS_CB, opt_usize(args, "--target-mb")?).await
        }
        (Some("clickbench"), Some("run")) => {
            let iters = opt_usize(args, "--iters")?.unwrap_or(3);
            let label = opt_str(args, "--label").unwrap_or("baseline");
            clickbench::run(iters, label).await
        }
        (Some("clickbench"), Some("raw")) => {
            let iters = opt_usize(args, "--iters")?.unwrap_or(3);
            let label = opt_str(args, "--label").unwrap_or("baseline");
            let dir = opt_str(args, "--dir").unwrap_or("bench/datasets/hits/");
            clickbench::raw(iters, label, dir).await
        }
        (Some("clickbench"), Some("plan")) => {
            let statement = args
                .get(2)
                .ok_or_else(|| anyhow::anyhow!("clickbench plan needs a statement"))?;
            let raw = args.iter().any(|a| a == "--raw");
            let dir = opt_str(args, "--dir").unwrap_or("bench/datasets/hits/");
            let max_files = opt_usize(args, "--max-files")?.unwrap_or(3);
            clickbench::plan(statement, raw.then_some(dir), max_files).await
        }
        (Some("clickbench"), Some("analyze")) => {
            let statement = args
                .get(2)
                .ok_or_else(|| anyhow::anyhow!("clickbench analyze needs a statement"))?;
            let raw = args.iter().any(|a| a == "--raw");
            let dir = opt_str(args, "--dir").unwrap_or("bench/datasets/hits/");
            let iters = opt_usize(args, "--iters")?.unwrap_or(1);
            clickbench::analyze(statement, raw.then_some(dir), iters).await
        }
        (Some("clickbench"), Some("sql")) => {
            let statement = args
                .get(2)
                .ok_or_else(|| anyhow::anyhow!("clickbench sql needs a statement"))?;
            clickbench::sql(statement).await
        }
        (Some("clickbench"), Some("compare")) => {
            let ukiel = opt_str(args, "--ukiel").unwrap_or("baseline");
            let raw = opt_str(args, "--raw").unwrap_or("baseline");
            clickbench::compare(ukiel, raw).await
        }
        (Some("clickbench"), sub) => {
            anyhow::bail!("unknown `clickbench` subcommand {sub:?}\n\n{USAGE}")
        }
        (Some("hits"), Some("load")) => hits::load(opt_usize(args, "--files")?).await,
        (Some("hits"), Some("compact")) => {
            hits::compact(&hits::HITS, opt_usize(args, "--target-mb")?).await
        }
        (Some("hits"), Some("queries")) => {
            let iters = opt_usize(args, "--iters")?.unwrap_or(10);
            let label = opt_str(args, "--label").unwrap_or("baseline");
            hits::queries(iters, label).await
        }
        (Some("hits"), Some("fanout")) => hits::fanout().await,
        (Some("hits"), sub) => anyhow::bail!("unknown `hits` subcommand {sub:?}\n\n{USAGE}"),
        (Some("catalog"), Some("demand")) => {
            catalog::demand(opt_str(args, "--scenario").unwrap_or("steady")).await
        }
        (Some("catalog"), Some("seed")) => {
            catalog::seed(
                opt_str(args, "--label").unwrap_or("default"),
                opt_usize(args, "--tables")?.unwrap_or(8) as u32,
                opt_usize(args, "--parts")?.unwrap_or(1_000_000) as i64,
                catalog::State::parse(opt_str(args, "--state").unwrap_or("mature"))?,
                opt_usize(args, "--packing-keys")?.unwrap_or(1_000_000) as u64,
                opt_str(args, "--dedicated-frac")
                    .unwrap_or("0.2")
                    .parse()
                    .map_err(|_| anyhow::anyhow!("--dedicated-frac must be a float"))?,
            )
            .await
        }
        (Some("catalog"), Some(w @ ("read" | "write"))) => {
            let cfg = catalog_load::LoadConfig {
                label: opt_str(args, "--label").unwrap_or("default").to_string(),
                mode: catalog_load::Mode::parse(opt_str(args, "--mode").unwrap_or("closed"))?,
                workers: opt_usize(args, "--workers")?.unwrap_or(16),
                rate: opt_str(args, "--rate")
                    .unwrap_or("500")
                    .parse()
                    .unwrap_or(500.0),
                duration_s: opt_str(args, "--duration")
                    .unwrap_or("20")
                    .parse()
                    .unwrap_or(20.0),
                warmup_s: opt_str(args, "--warmup")
                    .unwrap_or("5")
                    .parse()
                    .unwrap_or(5.0),
                timeout_ms: opt_usize(args, "--timeout-ms")?.unwrap_or(2000) as u64,
                pools: opt_usize(args, "--pools")?.unwrap_or(1),
                connections_per_pool: opt_usize(args, "--connections-per-pool")?.unwrap_or(16)
                    as u32,
                key_dist: catalog_load::KeyDist::parse(
                    opt_str(args, "--key-dist").unwrap_or("zipf"),
                )?,
                packing_keys: opt_usize(args, "--packing-keys")?.unwrap_or(1_000_000) as u64,
                scenario: opt_str(args, "--scenario").unwrap_or("steady").to_string(),
                max_inflight: opt_usize(args, "--max-inflight")?.unwrap_or(4096),
            };
            let table = opt_str(args, "--table").unwrap_or("bench_cat_0");
            match w {
                "read" => catalog_load::read(cfg, table).await,
                _ => {
                    let ppc = opt_usize(args, "--parts-per-commit")?.unwrap_or(4);
                    catalog_load::write(cfg, table, ppc).await
                }
            }
        }
        (Some("catalog"), sub) => {
            anyhow::bail!("unknown `catalog` subcommand {sub:?}\n\n{USAGE}")
        }
        (Some("bluesky"), Some("produce")) => {
            let files = opt_usize(args, "--files")?
                .ok_or_else(|| anyhow::anyhow!("bluesky produce needs --files N"))?;
            let topic = opt_str(args, "--topic").unwrap_or("bsky");
            bluesky::produce(files, topic).await
        }
        (Some("bluesky"), Some("run")) => {
            let files = opt_usize(args, "--files")?
                .ok_or_else(|| anyhow::anyhow!("bluesky run needs --files N"))?;
            let wave_files = opt_usize(args, "--wave-files")?;
            let flush_ms = opt_usize(args, "--flush-ms")?.unwrap_or(10_000) as u64;
            let label = opt_str(args, "--label").unwrap_or("baseline");
            bluesky::run(files, wave_files, flush_ms, label).await
        }
        (Some("bluesky"), Some("jsonbench")) => {
            let iters = opt_usize(args, "--iters")?.unwrap_or(3);
            let label = opt_str(args, "--label").unwrap_or("baseline");
            bluesky::jsonbench(iters, label).await
        }
        (Some("clickhouse"), Some("load")) => {
            let table =
                clickhouse::Table::parse(opt_str(args, "--table").ok_or_else(|| {
                    anyhow::anyhow!("clickhouse load needs --table official|cb")
                })?)?;
            clickhouse::load(table, opt_usize(args, "--files")?).await
        }
        (Some("clickhouse"), Some("run")) => {
            let table = clickhouse::Table::parse(opt_str(args, "--table").ok_or_else(|| {
                anyhow::anyhow!("clickhouse run needs --table official|cb|parquet")
            })?)?;
            let iters = opt_usize(args, "--iters")?.unwrap_or(3);
            let label = opt_str(args, "--label").unwrap_or("baseline");
            clickhouse::run(table, iters, label).await
        }
        (Some("clickhouse"), sub) => {
            anyhow::bail!("unknown `clickhouse` subcommand {sub:?}\n\n{USAGE}")
        }
        (Some("bluesky"), sub) => anyhow::bail!("unknown `bluesky` subcommand {sub:?}\n\n{USAGE}"),
        (Some(other), _) => anyhow::bail!("unknown command '{other}'\n\n{USAGE}"),
    }
}

/// Value of a `--flag VALUE` string option, if present.
fn opt_str<'a>(args: &'a [String], flag: &str) -> Option<&'a str> {
    let i = args.iter().position(|a| a == flag)?;
    args.get(i + 1).map(String::as_str)
}

/// Value of a `--flag N` option, if present. Errors on a non-integer value.
fn opt_usize(args: &[String], flag: &str) -> anyhow::Result<Option<usize>> {
    match args.iter().position(|a| a == flag) {
        Some(i) => {
            let raw = args
                .get(i + 1)
                .ok_or_else(|| anyhow::anyhow!("{flag} needs a value"))?;
            Ok(Some(raw.parse().map_err(|_| {
                anyhow::anyhow!("{flag} value '{raw}' is not a non-negative integer")
            })?))
        }
        None => Ok(None),
    }
}

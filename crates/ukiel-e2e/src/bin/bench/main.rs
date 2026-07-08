//! Macro perf harness (plan 30): volume-scale benchmarks on real datasets,
//! bridging the plan-11 micro harness (~100k synthetic rows) to the README's
//! PB+ intent. Manual-only — never run in CI. Build with `--release`.
//!
//! Subcommands (see docs/notes/2026-07-08-macro-perf.md for the runbook):
//!   bench hits load [--files N]                 load ClickBench hits into ukiel parts
//!   bench hits queries [--iters N] [--label L]  run the per-tenant read suite
//!   bench bluesky produce --files N [...]        stream Bluesky ndjson to Kafka
//!   bench bluesky run --files N [...]            full ingest→ladder→finalization run
//!
//! Datasets and results live under the gitignored `bench/` directory.

use std::process::ExitCode;

mod bluesky;
mod hits;
mod report;

const USAGE: &str = "\
bench — ukiel macro perf harness (plan 30, manual-only, run with --release)

USAGE:
    bench hits load [--files N]
    bench hits queries [--iters N] [--label LABEL]
    bench bluesky produce --files N [--topic T] [--wave-files W]
    bench bluesky run --files N [--wave-files W] [--flush-ms MS] [--label LABEL]

Datasets: bench/datasets/{hits,bluesky}/ (fetch via `make bench-fetch-{hits,bluesky}`).
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
        (Some("hits"), Some("load")) => hits::load(opt_usize(args, "--files")?).await,
        (Some("hits"), Some("queries")) => {
            let iters = opt_usize(args, "--iters")?.unwrap_or(10);
            let label = opt_str(args, "--label").unwrap_or("baseline");
            hits::queries(iters, label).await
        }
        (Some("hits"), sub) => anyhow::bail!("unknown `hits` subcommand {sub:?}\n\n{USAGE}"),
        (Some("bluesky"), Some("produce")) => {
            let files = opt_usize(args, "--files")?
                .ok_or_else(|| anyhow::anyhow!("bluesky produce needs --files N"))?;
            let topic = opt_str(args, "--topic").unwrap_or("bsky");
            bluesky::produce(files, topic).await
        }
        (Some("bluesky"), Some("run")) => anyhow::bail!("`bluesky run` lands in plan-30 task 5"),
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

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
    let mut it = args.iter().map(String::as_str);
    match it.next() {
        None | Some("--help") | Some("-h") => {
            println!("{USAGE}");
            Ok(())
        }
        Some(other) => anyhow::bail!("unknown command '{other}'\n\n{USAGE}"),
    }
}

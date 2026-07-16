//! The `file-read-bench` CLI: raw byte/range reads over a manifest's files to a checksum
//! sink, under a bound cache profile.

use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;

#[derive(Parser)]
#[command(
    name = "file-read-bench",
    version,
    about = "L2 raw byte/range reads to a checksum sink; never parses Parquet (plan 49)."
)]
struct Cli {
    /// The artifact manifest whose files to read (reconstruction, variant, or snapshot).
    #[arg(long)]
    manifest: PathBuf,
    /// The access plan: all | one | contiguous | sparse.
    #[arg(long)]
    plan: String,
    /// The fraction of each file the contiguous/sparse plans read.
    #[arg(long, default_value_t = 0.1)]
    fraction: f64,
    /// How many timed samples to collect.
    #[arg(long, default_value_t = 7)]
    samples: u32,
    /// The verified cache receipt this run executes under (bound into the report).
    #[arg(long)]
    receipt: Option<PathBuf>,
    /// Where to write the JSON report.
    #[arg(long)]
    report: PathBuf,
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let plan = match file_read_bench::parse_plan(&cli.plan) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("error: {e:#}");
            return ExitCode::FAILURE;
        }
    };
    match file_read_bench::run_bench(
        &cli.manifest,
        plan,
        cli.fraction,
        cli.samples,
        cli.receipt.as_deref(),
        &cli.report,
    ) {
        Ok(r) => {
            println!(
                "wrote {} ({} plan, {} ranges/sample, median {:.1} MiB/s)",
                cli.report.display(),
                r.plan,
                r.samples.first().map(|s| s.ranges).unwrap_or(0),
                r.median_mib_per_second
            );
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("error: {e:#}");
            ExitCode::FAILURE
        }
    }
}

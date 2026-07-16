//! The `parquet-write-bench` CLI: time only the Arrow-to-Parquet write of one frozen
//! logical artifact under one resolved writer configuration.

use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;

#[derive(Parser)]
#[command(
    name = "parquet-write-bench",
    version,
    about = "L1 isolated Parquet writer throughput over a frozen logical artifact (plan 49)."
)]
struct Cli {
    /// The frozen logical artifact: a reconstruction-control manifest.
    #[arg(long)]
    artifact: PathBuf,
    /// The manifest whose `resolved_config` supplies the writer configuration to time.
    /// Defaults to the artifact itself (times the reconstruction's own config).
    #[arg(long)]
    config: Option<PathBuf>,
    /// How many timed samples to collect.
    #[arg(long, default_value_t = 7)]
    samples: u32,
    /// Where to write the JSON report.
    #[arg(long)]
    report: PathBuf,
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let config = cli.config.unwrap_or_else(|| cli.artifact.clone());
    match parquet_write_bench::run_bench(&cli.artifact, &config, cli.samples, &cli.report) {
        Ok(r) => {
            println!(
                "wrote {} ({} samples, median {:.4}s, {} bytes, codec {})",
                cli.report.display(),
                r.samples.len(),
                r.median_wall_seconds,
                r.median_output_bytes,
                r.compression
            );
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("error: {e:#}");
            ExitCode::FAILURE
        }
    }
}

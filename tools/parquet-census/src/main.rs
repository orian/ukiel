//! The `parquet-census` CLI: inspect one snapshot's physical Parquet structure.

use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;

#[derive(Parser)]
#[command(
    name = "parquet-census",
    version,
    about = "Report the physical Parquet structure of a laboratory snapshot (plan 47)."
)]
struct Cli {
    /// The snapshot manifest to inspect.
    #[arg(long)]
    manifest: PathBuf,
    /// Where to write the JSON report.
    #[arg(long)]
    report: PathBuf,
    /// Stream every column for NDV and mean value width (footer-only otherwise).
    #[arg(long)]
    scan_columns: bool,
    /// Overwrite an existing report.
    #[arg(long)]
    replace: bool,
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let result = (|| {
        let report = parquet_census::census_snapshot(&cli.manifest, cli.scan_columns)?;
        parquet_census::write_report(&cli.report, &report, cli.replace)?;
        println!(
            "census: {} files, {} rows, {} compressed bytes, {} metadata overhead bytes -> {}",
            report.files,
            report.total_rows,
            report.total_compressed_bytes,
            report.total_metadata_overhead_bytes,
            cli.report.display()
        );
        anyhow::Ok(())
    })();
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e:#}");
            ExitCode::FAILURE
        }
    }
}

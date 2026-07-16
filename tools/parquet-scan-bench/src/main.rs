//! The `parquet-scan-bench` CLI: direct Parquet decode over a projection role and an
//! explicit row-group access plan, no DataFusion.

use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;

#[derive(Parser)]
#[command(
    name = "parquet-scan-bench",
    version,
    about = "L3 direct Arrow/Parquet decode with explicit row-group selection (plan 49)."
)]
struct Cli {
    /// The artifact manifest to scan (reconstruction, variant, or snapshot).
    #[arg(long)]
    manifest: PathBuf,
    /// The workload binding (role → column map).
    #[arg(long)]
    workload: PathBuf,
    /// The projection role: fixed_width_key | high_cardinality_string | wide_text | hot_set | all_columns.
    #[arg(long)]
    role: String,
    /// The row-group selection: all | one | ten_percent_contiguous | ten_percent_sparse | zero.
    #[arg(long)]
    selection: String,
    /// How many timed samples to collect.
    #[arg(long, default_value_t = 7)]
    samples: u32,
    /// The scenario manifest this run realizes (bound into the report).
    #[arg(long)]
    scenario: Option<PathBuf>,
    /// The verified cache receipt this run executes under (bound into the report).
    #[arg(long)]
    receipt: Option<PathBuf>,
    /// Where to write the JSON report.
    #[arg(long)]
    report: PathBuf,
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let role = match parquet_scan_bench::projection::parse_role(&cli.role) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("error: {e:#}");
            return ExitCode::FAILURE;
        }
    };
    let selection = match parquet_scan_bench::selection::parse_selection(&cli.selection) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("error: {e:#}");
            return ExitCode::FAILURE;
        }
    };
    match parquet_scan_bench::run_scan(
        &cli.manifest,
        &cli.workload,
        role,
        selection,
        cli.samples,
        cli.scenario.as_deref(),
        cli.receipt.as_deref(),
        &cli.report,
    ) {
        Ok(r) => {
            if r.metadata_only {
                println!("wrote {} (zero-selection metadata control)", cli.report.display());
            } else {
                println!(
                    "wrote {} ({} / {}, median {:.4}s, {:.1} MiB/s)",
                    cli.report.display(),
                    r.role,
                    r.selection,
                    r.median_wall_seconds,
                    r.median_mib_per_second
                );
            }
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("error: {e:#}");
            ExitCode::FAILURE
        }
    }
}

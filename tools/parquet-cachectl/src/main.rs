//! The `parquet-cachectl` CLI: prepare and verify one local cache profile.

use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;

#[derive(Parser)]
#[command(
    name = "parquet-cachectl",
    version,
    about = "Prepare and verify one local OS-page-cache profile over a manifest's files (plan 49)."
)]
struct Cli {
    /// The artifact manifest whose files to prepare (reconstruction, variant, or snapshot).
    #[arg(long)]
    manifest: PathBuf,
    /// The profile: decode-resident | local-os-warm | local-os-cold | local-reader-warm.
    #[arg(long)]
    profile: String,
    /// Where to write the cache receipt.
    #[arg(long)]
    receipt: PathBuf,
    /// The warm residency floor (fraction resident).
    #[arg(long, default_value_t = parquet_cachectl::DEFAULT_WARM_FLOOR)]
    warm_floor: f64,
    /// The cold residency ceiling (fraction resident).
    #[arg(long, default_value_t = parquet_cachectl::DEFAULT_COLD_CEILING)]
    cold_ceiling: f64,
    #[arg(long)]
    replace: bool,
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let profile = match parquet_cachectl::parse_profile(&cli.profile) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("error: {e:#}");
            return ExitCode::FAILURE;
        }
    };
    match parquet_cachectl::prepare(
        &cli.manifest,
        profile,
        &cli.receipt,
        cli.warm_floor,
        cli.cold_ceiling,
        cli.replace,
    ) {
        Ok((receipt, true)) => {
            println!(
                "prepared {:?}: {:.1}% resident (receipt {})",
                receipt.requested_profile,
                receipt.residency_after.resident_fraction * 100.0,
                cli.receipt.display()
            );
            ExitCode::SUCCESS
        }
        Ok((receipt, false)) => {
            eprintln!(
                "profile {:?} unavailable: {:.1}% resident after {}; receipt marked invalid",
                receipt.requested_profile,
                receipt.residency_after.resident_fraction * 100.0,
                receipt.preparation_method
            );
            ExitCode::FAILURE
        }
        Err(e) => {
            eprintln!("error: {e:#}");
            ExitCode::FAILURE
        }
    }
}

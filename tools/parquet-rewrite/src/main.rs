//! The `parquet-rewrite` CLI: rewrite one snapshot under one variant spec.

use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;

#[derive(Parser)]
#[command(
    name = "parquet-rewrite",
    version,
    about = "Rewrite a laboratory snapshot into a verified storage variant (plan 47)."
)]
struct Cli {
    /// The parent snapshot manifest.
    #[arg(long)]
    manifest: PathBuf,
    /// The variant spec (TOML).
    #[arg(long)]
    spec: PathBuf,
    /// A fresh output directory for the variant.
    #[arg(long)]
    output: PathBuf,
    #[arg(long)]
    replace: bool,
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let result = parquet_rewrite::run(&cli.manifest, &cli.spec, &cli.output, cli.replace);
    match result {
        Ok(m) => {
            println!("wrote variant manifest {}", m.display());
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("error: {e:#}");
            ExitCode::FAILURE
        }
    }
}

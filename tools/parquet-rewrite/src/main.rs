//! The `parquet-rewrite` CLI: rewrite laboratory snapshots into verified artifacts.
//!
//! Three modes:
//! - `variant`      (default): rewrite a plan-47 snapshot under a full variant spec;
//! - `reconstruct`: rebuild a product snapshot into a reconstruction control;
//! - `vary`:        apply one one-variable delta to a reconstruction control.

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(
    name = "parquet-rewrite",
    version,
    about = "Rewrite a laboratory snapshot into a verified storage artifact (plans 47/49)."
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Plan-47 variant rewrite: a full writer spec against a snapshot.
    Variant {
        #[arg(long)]
        manifest: PathBuf,
        #[arg(long)]
        spec: PathBuf,
        #[arg(long)]
        output: PathBuf,
        #[arg(long)]
        replace: bool,
    },
    /// Plan-49 reconstruction control: rebuild a product snapshot under a baseline policy.
    Reconstruct {
        #[arg(long)]
        manifest: PathBuf,
        #[arg(long)]
        baseline: PathBuf,
        #[arg(long)]
        output: PathBuf,
    },
    /// Plan-49 one-variable variant: apply a delta to a reconstruction control.
    Vary {
        #[arg(long)]
        manifest: PathBuf,
        #[arg(long)]
        delta: PathBuf,
        #[arg(long)]
        output: PathBuf,
    },
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let result = match cli.command {
        Command::Variant {
            manifest,
            spec,
            output,
            replace,
        } => parquet_rewrite::run(&manifest, &spec, &output, replace),
        Command::Reconstruct {
            manifest,
            baseline,
            output,
        } => parquet_rewrite::run_reconstruct(&manifest, &baseline, &output),
        Command::Vary {
            manifest,
            delta,
            output,
        } => parquet_rewrite::run_vary(&manifest, &delta, &output),
    };
    match result {
        Ok(m) => {
            println!("wrote manifest {}", m.display());
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("error: {e:#}");
            ExitCode::FAILURE
        }
    }
}

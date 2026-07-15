//! The `parquet-skip-index` CLI: build one experimental sidecar for a variant.

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(
    name = "parquet-skip-index",
    version,
    about = "Build conservative experimental skip-index sidecars for a laboratory variant (plan 47)."
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Build a sidecar from a variant manifest and an index spec.
    Build {
        #[arg(long)]
        manifest: PathBuf,
        #[arg(long)]
        spec: PathBuf,
        #[arg(long)]
        output: PathBuf,
        #[arg(long)]
        replace: bool,
    },
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let result = match cli.command {
        Command::Build {
            manifest,
            spec,
            output,
            replace,
        } => (|| {
            let spec_bytes = std::fs::read(&spec)?;
            let spec = parquet_skip_index::builder::SkipSpec::parse(
                &spec_bytes,
                &spec.display().to_string(),
            )?;
            let out = parquet_skip_index::builder::build(&manifest, &spec, &output, replace)?;
            println!("wrote sidecar {}", out.display());
            anyhow::Ok(())
        })(),
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e:#}");
            ExitCode::FAILURE
        }
    }
}

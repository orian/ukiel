//! The `parquet-lab-store` CLI: `publish` an artifact to a disposable namespace, `verify`
//! it read-only.

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(
    name = "parquet-lab-store",
    version,
    about = "Publish a laboratory artifact to a disposable object-store namespace and verify it (plan 47)."
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Upload an artifact's files under a disposable prefix and write a store receipt.
    Publish {
        #[arg(long)]
        manifest: PathBuf,
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        prefix: String,
        #[arg(long)]
        receipt: PathBuf,
    },
    /// Re-check a published namespace against its receipt. Read-only.
    Verify {
        #[arg(long)]
        receipt: PathBuf,
        #[arg(long)]
        config: PathBuf,
    },
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let result = rt.block_on(async {
        match cli.command {
            Command::Publish {
                manifest,
                config,
                prefix,
                receipt,
            } => {
                let cfg = parquet_lab_store::StoreConfig::load(&config)?;
                parquet_lab_store::publish::publish(&manifest, &cfg, &prefix, &receipt).await?;
                println!("published {} -> {}", manifest.display(), receipt.display());
                anyhow::Ok(())
            }
            Command::Verify { receipt, config } => {
                let cfg = parquet_lab_store::StoreConfig::load(&config)?;
                let r = parquet_lab_store::verify::verify(&receipt, &cfg).await?;
                println!("verified {} objects, {} bytes", r.objects, r.total_bytes);
                anyhow::Ok(())
            }
        }
    });
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e:#}");
            ExitCode::FAILURE
        }
    }
}

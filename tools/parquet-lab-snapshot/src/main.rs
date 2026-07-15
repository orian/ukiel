//! The `parquet-lab-snapshot` CLI: `from-ukiel`, `from-files`, `verify`.
//!
//! `from-ukiel` is service-aware; `from-files` and `verify` are fully offline.

use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(
    name = "parquet-lab-snapshot",
    version,
    about = "Freeze Parquet files into a verified, immutable laboratory snapshot (plan 47)."
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Freeze a converged plan-46 compacted load. Revalidates the receipt against a live
    /// catalog and object store and downloads each final object unchanged.
    FromUkiel {
        #[arg(long)]
        receipt: PathBuf,
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        output: PathBuf,
        /// Overwrite a non-empty output directory. A snapshot is immutable, so this is
        /// off by default.
        #[arg(long)]
        replace: bool,
    },
    /// Freeze an explicit, sorted local Parquet file list. Fully offline.
    FromFiles {
        /// The declared logical/table schema JSON, stored verbatim in the manifest.
        #[arg(long)]
        schema: PathBuf,
        /// A text file of source Parquet paths, one per line, in sort order.
        #[arg(long)]
        files_from: PathBuf,
        /// An ordered `{ "columns": [{ "name", "logical" }] }` declaration matching the
        /// physical column order.
        #[arg(long)]
        logical_schema: PathBuf,
        #[arg(long)]
        output: PathBuf,
        #[arg(long)]
        replace: bool,
    },
    /// Re-check a published snapshot against its manifest. Offline.
    Verify {
        #[arg(long)]
        manifest: PathBuf,
    },
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let result = match cli.command {
        Command::FromUkiel {
            receipt,
            config,
            output,
            replace,
        } => run_from_ukiel(receipt, config, output, replace),
        Command::FromFiles {
            schema,
            files_from,
            logical_schema,
            output,
            replace,
        } => {
            let cmd = format!(
                "parquet-lab-snapshot from-files --schema {} --files-from {} --logical-schema {} --output {}",
                schema.display(),
                files_from.display(),
                logical_schema.display(),
                output.display()
            );
            parquet_lab_snapshot::from_files::freeze(
                &schema,
                &files_from,
                &logical_schema,
                &output,
                cmd,
                replace,
            )
            .map(|m| println!("wrote snapshot manifest {}", m.display()))
        }
        Command::Verify { manifest } => parquet_lab_snapshot::verify(&manifest).map(|r| {
            println!(
                "snapshot verified: {} files, {} rows, logical digest {}",
                r.files, r.rows, r.logical_digest
            );
        }),
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e:#}");
            ExitCode::FAILURE
        }
    }
}

#[tokio::main(flavor = "current_thread")]
async fn run_from_ukiel(
    receipt: PathBuf,
    config: PathBuf,
    output: PathBuf,
    replace: bool,
) -> Result<()> {
    let cmd = format!(
        "parquet-lab-snapshot from-ukiel --receipt {} --config {} --output {}",
        receipt.display(),
        config.display(),
        output.display()
    );
    let (rcpt, l0, l0_digest) = parquet_lab_snapshot::from_ukiel::read_receipt_and_l0(&receipt)?;
    let source_manifest = {
        // The source manifest referenced by the receipt, relative to its directory.
        let dir = receipt.parent().unwrap_or(std::path::Path::new("."));
        let sp = dir.join(&rcpt.source_manifest.path);
        let sb = std::fs::read(&sp)
            .with_context(|| format!("reading source manifest {}", sp.display()))?;
        rcpt.source_manifest
            .verify(&sp.display().to_string(), &sb)?;
        parquet_lab_contract::FileDigest::of(rcpt.source_manifest.path.clone(), &sb)
    };

    let cfg = ukield::config::UkieldConfig::load(
        config.to_str().context("config path is not valid UTF-8")?,
    )
    .with_context(|| format!("loading config {}", config.display()))?;
    let catalog = ukiel_catalog::PostgresCatalog::connect(&cfg.catalog.url).await?;
    let (store, _url) = ukield::run::build_store(&cfg.object_store)?;
    let store: Arc<dyn object_store::ObjectStore> = store;

    println!("{}\n", rcpt.disclaimer);
    let manifest = parquet_lab_snapshot::from_ukiel::freeze(
        &catalog,
        &store,
        &rcpt,
        &l0,
        l0_digest,
        source_manifest,
        &output,
        cmd,
        replace,
    )
    .await?;
    println!("wrote snapshot manifest {}", manifest.display());
    Ok(())
}

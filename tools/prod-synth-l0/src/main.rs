//! The `prod-synth-l0` CLI: `stage`.
//!
//! Offline. Every input and output is named on the command line; the tool has no notion
//! of a repository and no default path.

use std::path::PathBuf;
use std::process::ExitCode;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(
    name = "prod-synth-l0",
    version,
    about = "Deterministically restage prod-synth rows as Ukiel day-partitioned, flush-sized L0 Parquet.",
    long_about = "Reads a plan-45 prod-synth artifact and writes the same rows regrouped into \
UTC-day-partitioned, flush-sized level-0 Parquet with Ukiel's own L0 writer properties, so the \
real compactor can turn them into final parts.\n\n\
It is offline: no database, object store, Kafka, HTTP, or DataFusion. It regroups rows and \
nothing else -- same rows, same values, proven by an order-independent fingerprint. The output is \
SYNTHETIC and profile-derived, exactly as its source is."
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Restage a source artifact's rows as L0 Parquet plus an L0 manifest.
    Stage {
        /// The source fixture's manifest.json.
        #[arg(long)]
        manifest: PathBuf,
        /// Output directory for l0-manifest.json and parquet/.
        #[arg(long)]
        output: PathBuf,
        /// Rows per flush before UTC-day partitioning splits it. Each resulting day slice
        /// becomes one committed L0 run.
        #[arg(long, default_value_t = 100_000)]
        flush_rows: u64,
        /// Replace an existing staged artifact at exactly this path.
        #[arg(long)]
        replace: bool,
    },
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("prod-synth-l0: {e:#}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<()> {
    match Cli::parse().command {
        Command::Stage {
            manifest,
            output,
            flush_rows,
            replace,
        } => {
            if flush_rows == 0 {
                anyhow::bail!("--flush-rows must be greater than 0");
            }
            let staged = prod_synth_l0::stage_source(&manifest, &output, flush_rows, replace)
                .with_context(|| format!("staging {}", manifest.display()))?;
            let m = &staged.manifest;
            println!("{}", m.disclaimer);
            println!(
                "\nstaged {} -> {} L0 files across {} UTC day(s), {} rows, flush {}",
                manifest.display(),
                m.files.len(),
                m.files
                    .iter()
                    .map(|f| f.day.as_str())
                    .collect::<std::collections::BTreeSet<_>>()
                    .len(),
                m.output_rows,
                m.staging.flush_rows,
            );
            println!(
                "  row fingerprint {} ({} rows) matches the source",
                &m.fingerprint.digest[..16],
                m.fingerprint.count,
            );
            println!("wrote {}", staged.dir.join("l0-manifest.json").display());
            Ok(())
        }
    }
}

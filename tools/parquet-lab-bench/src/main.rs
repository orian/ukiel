//! The `parquet-lab-bench` CLI: `compile` a suite from a control, `run` it over an artifact.

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand, ValueEnum};
use parquet_lab_bench::runner::{Mode, ReaderFlags};

#[derive(Parser)]
#[command(
    name = "parquet-lab-bench",
    version,
    about = "Run declared queries read-only over a laboratory artifact with exact I/O accounting (plan 47)."
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Copy, Clone, ValueEnum)]
enum SuiteKindArg {
    ProdSynth,
    ClickBench,
}

#[derive(Copy, Clone, ValueEnum)]
enum ModeArg {
    Local,
    ObjectStore,
}

#[derive(Subcommand)]
enum Command {
    /// Compile a suite from a snapshot control: bake the logical view and expected digests.
    Compile {
        #[arg(long)]
        manifest: PathBuf,
        #[arg(long, value_enum)]
        kind: SuiteKindArg,
        /// A `.sql` file of queries (`;`-separated, `-- name:` markers optional).
        #[arg(long)]
        sql: PathBuf,
        #[arg(long)]
        suite_out: PathBuf,
        #[arg(long)]
        replace: bool,
    },
    /// Run a suite over an artifact (snapshot or variant) and write a result report.
    Run {
        #[arg(long)]
        manifest: PathBuf,
        #[arg(long)]
        suite: PathBuf,
        #[arg(long)]
        result: PathBuf,
        #[arg(long, value_enum, default_value_t = ModeArg::Local)]
        mode: ModeArg,
        #[arg(long, default_value_t = 1)]
        cold_iters: usize,
        #[arg(long, default_value_t = 5)]
        warm_iters: usize,
        #[arg(long, default_value_t = 0)]
        run_order: u32,
        /// Reader A/B switches (all on by default). Turning one off is a reader A/B — a
        /// writer feature is not credited when its reader is disabled.
        #[arg(long)]
        no_page_index: bool,
        #[arg(long)]
        no_pruning: bool,
        #[arg(long)]
        no_pushdown_filters: bool,
        #[arg(long)]
        no_reorder_filters: bool,
        #[arg(long)]
        no_bloom_filter_on_read: bool,
        /// An experimental skip-index sidecar (`skip.json`) to price against native pruning.
        #[arg(long)]
        skip_manifest: Option<PathBuf>,
        #[arg(long)]
        replace: bool,
    },
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let result = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(dispatch(cli));
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e:#}");
            ExitCode::FAILURE
        }
    }
}

async fn dispatch(cli: Cli) -> anyhow::Result<()> {
    match cli.command {
        Command::Compile {
            manifest,
            kind,
            sql,
            suite_out,
            replace,
        } => {
            let kind = match kind {
                SuiteKindArg::ProdSynth => parquet_lab_contract::SuiteKind::ProdSynth,
                SuiteKindArg::ClickBench => parquet_lab_contract::SuiteKind::ClickBench,
            };
            parquet_lab_bench::compile(&manifest, kind, &sql, &suite_out, replace).await?;
            println!("wrote suite {}", suite_out.display());
            Ok(())
        }
        Command::Run {
            manifest,
            suite,
            result,
            mode,
            cold_iters,
            warm_iters,
            run_order,
            no_page_index,
            no_pruning,
            no_pushdown_filters,
            no_reorder_filters,
            no_bloom_filter_on_read,
            skip_manifest,
            replace,
        } => {
            let params = parquet_lab_bench::RunParams {
                mode: match mode {
                    ModeArg::Local => Mode::Local,
                    ModeArg::ObjectStore => Mode::ObjectStore,
                },
                cold_iters,
                warm_iters,
                reader_flags: ReaderFlags {
                    enable_page_index: !no_page_index,
                    pruning: !no_pruning,
                    pushdown_filters: !no_pushdown_filters,
                    reorder_filters: !no_reorder_filters,
                    bloom_filter_on_read: !no_bloom_filter_on_read,
                },
                run_order,
                skip_manifest,
            };
            parquet_lab_bench::run(&manifest, &suite, &result, params, replace).await?;
            println!("wrote result {}", result.display());
            Ok(())
        }
    }
}

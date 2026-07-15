//! The `ukiel-prod-load` CLI: `materialized`, `catalog-only`.
//!
//! Every side effect is declared by the command you ran. There is no `generate`, no
//! `repair`, and no "load it if it's missing": a fixture arrives from `prod-synth`,
//! verified, or it does not arrive at all.

use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use prod_synth_contract::Manifest;
use ukiel_catalog::PostgresCatalog;
use ukiel_prod_load::load::TenantGeometry;
use ukiel_prod_load::{catalog_only, hypertable_name, load};

#[derive(Parser)]
#[command(
    name = "ukiel-prod-load",
    version,
    about = "Load one prod-synth manifest into an explicitly selected Ukiel deployment.",
    long_about = "Loads a prod-synth artifact into Ukiel. It reads a manifest and writes to a \
catalog and an object store, and it does nothing else: it cannot parse a profile, compile a \
topology, generate a row, or benchmark anything. If the fixture is missing or its digests do not \
match, this fails — it never regenerates one.\n\n\
The fixture is SYNTHETIC and PROFILE-DERIVED. No report drawn from it may present it as a \
measurement of a real production system."
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Upload the real Parquet objects and register them through the product's write path.
    Materialized {
        /// Path to the fixture's manifest.json.
        #[arg(long)]
        manifest: PathBuf,
        /// The fixture's name in this deployment. Must be fresh.
        #[arg(long)]
        label: String,
        /// A ukield config file: the catalog and object store to write to.
        #[arg(long)]
        config: PathBuf,
    },
    /// Seed the same truthful topology with no objects, for a fast catalog measurement.
    CatalogOnly {
        #[arg(long)]
        manifest: PathBuf,
        #[arg(long)]
        label: String,
        /// A ukield config file. Its compactor/gc roles must be off — this arm writes
        /// part rows whose objects do not exist.
        #[arg(long)]
        config: PathBuf,
        /// Required, and not a formality: this arm seeds parts that point at nothing, so
        /// it only runs against a stack you are willing to throw away.
        #[arg(long)]
        ephemeral: bool,
        /// Keep the seeded catalog instead of cleaning it up — `ukiel-prod-bench catalog`
        /// needs it to still be there.
        #[arg(long)]
        keep: bool,
    },
    /// Load a staged L0 artifact as real compaction input (plan 46).
    CompactionInput {
        /// Path to the staged artifact's l0-manifest.json.
        #[arg(long)]
        l0_manifest: PathBuf,
        #[arg(long)]
        label: String,
        #[arg(long)]
        config: PathBuf,
        /// Where to write the load receipt. Published atomically, only on success.
        #[arg(long)]
        receipt: PathBuf,
        /// packed | separated | size-targeted.
        #[arg(long)]
        placement: String,
        /// Required only for `size-targeted`, rejected otherwise.
        #[arg(long)]
        target_file_mb: Option<i64>,
        /// Required: this load mutates a disposable stack and the real compactor then
        /// rewrites it.
        #[arg(long)]
        ephemeral: bool,
        /// A hypertable name to accept alongside the experiment's own — repeatable. The
        /// load refuses a catalog holding anything else.
        #[arg(long = "allow-hypertable")]
        allow_hypertable: Vec<String>,
    },
}

/// Parse the placement flags into a `PlacementSpec`, enforcing that `--target-file-mb`
/// is present exactly when the placement is size-targeted.
fn parse_placement(
    placement: &str,
    target_file_mb: Option<i64>,
) -> Result<prod_synth_contract::PlacementSpec> {
    use prod_synth_contract::PlacementSpec;
    match placement {
        "packed" | "separated" if target_file_mb.is_some() => {
            bail!("--target-file-mb is only valid with --placement size-targeted")
        }
        "packed" => Ok(PlacementSpec::Packed),
        "separated" => Ok(PlacementSpec::Separated),
        "size-targeted" => {
            let mb = target_file_mb.ok_or_else(|| {
                anyhow::anyhow!("--placement size-targeted requires --target-file-mb")
            })?;
            if mb <= 0 {
                bail!("--target-file-mb must be positive, got {mb}");
            }
            Ok(PlacementSpec::SizeTargeted(mb * 1024 * 1024))
        }
        other => bail!("unknown --placement '{other}' (packed | separated | size-targeted)"),
    }
}

fn main() -> ExitCode {
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    match rt.block_on(run()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("ukiel-prod-load: {e:#}");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<()> {
    match Cli::parse().command {
        Command::Materialized {
            manifest,
            label,
            config,
        } => materialized(&manifest, &label, &config).await,
        Command::CatalogOnly {
            manifest,
            label,
            config,
            ephemeral,
            keep,
        } => {
            if !ephemeral {
                bail!(
                    "catalog-only requires --ephemeral. It seeds part rows whose objects do not \
                     exist, which is only safe on a stack you are willing to throw away."
                );
            }
            catalog_only_cmd(&manifest, &label, &config, keep).await
        }
        Command::CompactionInput {
            l0_manifest,
            label,
            config,
            receipt,
            placement,
            target_file_mb,
            ephemeral,
            allow_hypertable,
        } => {
            if !ephemeral {
                bail!(
                    "compaction-input requires --ephemeral. It loads L0 parts a live compactor then \
                     rewrites, which is only safe on a stack you are willing to throw away."
                );
            }
            let placement = parse_placement(&placement, target_file_mb)?;
            compaction_input_cmd(
                &l0_manifest,
                &label,
                &config,
                &receipt,
                placement,
                &allow_hypertable,
            )
            .await
        }
    }
}

async fn compaction_input_cmd(
    l0_manifest_path: &Path,
    label: &str,
    config: &Path,
    receipt_path: &Path,
    placement: prod_synth_contract::PlacementSpec,
    allow_hypertable: &[String],
) -> Result<()> {
    use ukiel_prod_load::compaction_input as ci;

    // Verify the L0 artifact before touching a service.
    let (l0, l0_bytes) =
        ci::read_l0_artifact(l0_manifest_path).context("reading the L0 artifact")?;
    let dir = l0_manifest_path.parent().unwrap_or(Path::new("."));

    println!("{}", l0.disclaimer);
    println!(
        "\nloading {} L0 files ({} rows) as '{}' at placement {}",
        l0.files.len(),
        l0.output_rows,
        hypertable_name(label),
        placement.as_str(),
    );

    let cfg = load_config(config)?;
    let catalog = PostgresCatalog::connect(&cfg.catalog.url)
        .await
        .context("connecting to the catalog")?;
    catalog.migrate().await.context("applying the schema")?;
    ukiel_prod_load::require_disposable_catalog(&catalog, allow_hypertable).await?;
    ci::require_fresh(&catalog, label).await?;

    let (store, _url) = ukield::run::build_store(&cfg.object_store)?;
    let store: Arc<dyn object_store::ObjectStore> = store;

    // The compactor config the run is expected to use, recorded so the runner can prove
    // the fixture was converged under the intended settings.
    let expected = prod_synth_contract::ExpectedCompactorConfig {
        l0_fanout: cfg.compactor.l0_fanout,
        fanout: cfg.compactor.fanout,
        finalize_after_secs: cfg.compactor.finalize_after_secs,
        finalize_poll_interval_ms: cfg.compactor.finalize_poll_interval_ms,
        lease_ttl_secs: cfg.compactor.lease_ttl_secs,
        lease_renew_interval_secs: cfg.compactor.lease_renew_interval_secs,
        candidate_limit: cfg.compactor.candidate_limit,
    };

    let ht = ci::create_hypertable(&catalog, &l0, placement, label).await?;
    let source_manifest = l0.source_manifest.clone();

    let result = ci::load(
        &catalog,
        &store,
        &l0,
        &l0_bytes,
        source_manifest,
        dir,
        label,
        placement,
        ht,
        expected,
    )
    .await;

    match result {
        Ok(receipt) => {
            ci::write_receipt(&receipt, receipt_path)?;
            println!(
                "\nloaded '{}' (hypertable_id {}) — {} L0 runs, {} rows, {} bytes, placement {}",
                receipt.hypertable,
                receipt.hypertable_id,
                receipt.input_parts,
                receipt.input_rows,
                receipt.input_bytes,
                receipt.placement.as_str(),
            );
            println!(
                "  row fingerprint {} carried into the receipt; every input part stamped with the \
                 L0 manifest + UTC day so a compacted part can prove its descent",
                &receipt.fingerprint.digest[..16],
            );
            println!("wrote receipt {}", receipt_path.display());
            println!(
                "\nNext: start a compactor-only ukield against this catalog, wait for convergence \
                 with `ukiel-prod-bench wait-compacted`, then measure."
            );
            Ok(())
        }
        Err(e) => {
            // A failed load must leave no receipt claiming success, and no half-fixture
            // for a compactor to pick at. Print the reset command.
            eprintln!(
                "ukiel-prod-load: load failed; no receipt written. Reset the stack before retrying:\n{}",
                ukiel_prod_load::catalog_only::manual_reset(&hypertable_name(label))
            );
            Err(e)
        }
    }
}

/// ukield owns the config format. Parsing it here rather than reinventing a subset
/// means a fixture lands in the deployment the *product's own* configuration describes.
fn load_config(path: &Path) -> Result<ukield::config::UkieldConfig> {
    let text =
        std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    toml::from_str(&text).with_context(|| format!("parsing {}", path.display()))
}

async fn materialized(manifest_path: &Path, label: &str, config: &Path) -> Result<()> {
    // Read and verify the artifact BEFORE touching a service. A corrupt fixture must
    // fail on the filesystem, not halfway through writing to a catalog.
    let (manifest, topology) = load::read_artifact(manifest_path)?;
    let dir = manifest_path.parent().unwrap_or(Path::new("."));

    println!("{}", manifest.disclaimer);
    println!(
        "\nloading tier {} (seed {}, {} tenants, {} parts, {} rows) as '{}'",
        manifest.config.tier.as_str(),
        manifest.config.seed,
        manifest.generated.tenants,
        manifest.parts.len(),
        manifest.generated.rows,
        hypertable_name(label),
    );

    let cfg = load_config(config)?;
    let catalog = PostgresCatalog::connect(&cfg.catalog.url)
        .await
        .context("connecting to the catalog")?;
    catalog.migrate().await.context("applying the schema")?;
    load::require_fresh_label(&catalog, label).await?;

    let (store, _url) = ukield::run::build_store(&cfg.object_store)?;
    let store: Arc<dyn object_store::ObjectStore> = store;

    let ht = load::create_hypertable(&catalog, &manifest, label).await?;
    let metas =
        load::load_materialized(&catalog, &store, &manifest, &topology, dir, label, ht).await?;

    // The three-way agreement: object HEAD = manifest = catalog. Each was measured
    // independently, so agreeing means something.
    let live = catalog.live_parts(ht, None).await?;
    if live.len() != manifest.parts.len() {
        bail!(
            "the catalog holds {} live parts; the manifest declares {}",
            live.len(),
            manifest.parts.len()
        );
    }
    let cat_rows: i64 = live.iter().map(|p| p.meta.row_count).sum();
    let cat_bytes: i64 = live.iter().map(|p| p.meta.size_bytes).sum();
    if cat_rows as u64 != manifest.generated.rows {
        bail!(
            "the catalog holds {cat_rows} rows; the manifest declares {}",
            manifest.generated.rows
        );
    }
    if cat_bytes as u64 != manifest.generated.bytes {
        bail!(
            "the catalog records {cat_bytes} bytes; the manifest declares {}. These are measured \
             independently — the catalog's from the object store's HEAD, the manifest's from the \
             closed file — so a disagreement is real.",
            manifest.generated.bytes
        );
    }
    let written: i64 = metas.iter().map(|m| m.size_bytes).sum();
    if written != cat_bytes {
        bail!("the rows written ({written} bytes) are not the rows that landed ({cat_bytes})");
    }

    // Zero false negatives across the deterministic tenant sample. The only invariant
    // that, if broken, makes rows silently disappear from query results.
    let checked = load::assert_no_false_negatives(&catalog, ht, &manifest, &topology).await?;
    let geometry = load::measure_geometry(&catalog, ht, &manifest, &topology).await?;

    report(&manifest, label, ht, &geometry, true, checked);
    Ok(())
}

async fn catalog_only_cmd(
    manifest_path: &Path,
    label: &str,
    config: &Path,
    keep: bool,
) -> Result<()> {
    let (manifest, topology) = load::read_artifact(manifest_path)?;

    let cfg = load_config(config)?;
    // No compactor, no GC — see `catalog_only::require_disposable`.
    catalog_only::require_disposable(&cfg.roles)?;

    println!("{}", manifest.disclaimer);
    println!(
        "\nseeding CATALOG-ONLY topology (seed {}, {} tenants, {} parts) as '{}'",
        manifest.config.seed,
        manifest.generated.tenants,
        manifest.parts.len(),
        hypertable_name(label),
    );
    println!(
        "  No objects are written: size_bytes is 0 and paths are `catalog-only://…`, which no \
         object store can open. This arm measures catalog membership geometry, and nothing about \
         scans or bytes."
    );

    let catalog = PostgresCatalog::connect(&cfg.catalog.url)
        .await
        .context("connecting to the catalog")?;
    catalog.migrate().await.context("applying the schema")?;
    load::require_fresh_label(&catalog, label).await?;

    let seeded = async {
        let (ht, _parts) = catalog_only::seed(&catalog, &manifest, &topology, label).await?;
        let checked = load::assert_no_false_negatives(&catalog, ht, &manifest, &topology).await?;
        let geometry = load::measure_geometry(&catalog, ht, &manifest, &topology).await?;
        Ok::<_, anyhow::Error>((ht, geometry, checked))
    }
    .await;

    match seeded {
        Ok((ht, geometry, checked)) => {
            report(&manifest, label, ht, &geometry, false, checked);
            if keep {
                println!(
                    "\n--keep: the seeded catalog is left in place for `ukiel-prod-bench catalog`.\n\
                     Remove it when you are done:\n{}",
                    catalog_only::manual_reset(&hypertable_name(label))
                );
            } else if let Err(e) = catalog_only::cleanup(&catalog, label).await {
                bail!("{e}");
            } else {
                println!("\ncleaned up: no part rows without objects were left behind.");
            }
            Ok(())
        }
        Err(e) => {
            // The same cleanup on the failure path. Never leave a fake path for a
            // background compactor to trip over.
            if let Err(c) = catalog_only::cleanup(&catalog, label).await {
                eprintln!("ukiel-prod-load: {c}");
            }
            Err(e)
        }
    }
}

fn report(
    manifest: &Manifest,
    label: &str,
    ht: ukiel_core::HypertableId,
    geometry: &[TenantGeometry],
    materialized: bool,
    checked: u64,
) {
    println!(
        "\nloaded '{}' (hypertable_id {}) — seed {}, topology {}",
        hypertable_name(label),
        ht.0,
        manifest.config.seed,
        &manifest.topology.digest[..16],
    );
    println!(
        "  zero false negatives across {checked} sampled tenants: every part that holds a tenant \
         was returned for it"
    );

    println!(
        "\n{:<16} {:>12} {:>8} {:>8} {:>8} {:>10} {:>12}",
        "class", "tenant", "exact", "shipped", "range", "overfetch", "rows"
    );
    for g in geometry {
        println!(
            "{:<16} {:>12} {:>8} {:>8} {:>8} {:>9.1}x {:>12}",
            g.class,
            g.tenant,
            g.exact,
            g.shipped,
            g.range,
            g.range as f64 / g.exact.max(1) as f64,
            g.rows,
        );
    }
    println!(
        "\n  `range` is what min/max pruning alone would ship. `shipped` is what the catalog really \
         returns, once the issue-0014 key filter has rejected the parts it can prove do not hold \
         the tenant. `exact` is the truth from the topology."
    );
    if !materialized {
        println!(
            "  CATALOG-ONLY: no objects exist and size_bytes is 0. Nothing here says anything about \
             scans or bytes."
        );
    }
}

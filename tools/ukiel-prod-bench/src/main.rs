//! The `ukiel-prod-bench` CLI: `queries`, `catalog`.
//!
//! Both commands measure a load that already exists. Neither can create one.

use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use datafusion::prelude::SessionContext;
use prod_synth_contract::{Manifest, Topology};
use ukiel_catalog::PostgresCatalog;
use ukiel_core::{HypertableId, NamespaceId};
use ukiel_prod_bench::{catalog as cat, hypertable_name, queries, require_read_only};

#[derive(Parser)]
#[command(
    name = "ukiel-prod-bench",
    version,
    about = "Measure an existing prod-synth load: scoped Ukiel against raw DataFusion.",
    long_about = "Measures a prod-synth fixture that has ALREADY been loaded by ukiel-prod-load.\n\n\
This tool is read-only. It cannot generate a fixture, upload an object, create a table, mutate a \
catalog, or clean anything up. Generation and loading are facts it reads from the manifest and the \
catalog, not actions it may perform — which is what makes the numbers it prints reproducible.\n\n\
The fixture is SYNTHETIC and PROFILE-DERIVED. No number from it describes a real production system."
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run the six-query suite: scoped Ukiel against raw DataFusion over the same files.
    ///
    /// Two forms: `--manifest --label` (a plan-45 materialized load) or `--receipt` (a
    /// plan-46 compacted load, whose paths change under compaction).
    Queries {
        #[arg(long)]
        manifest: Option<PathBuf>,
        #[arg(long)]
        label: Option<String>,
        #[arg(long)]
        receipt: Option<PathBuf>,
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        result: PathBuf,
        /// Timed iterations after one warmup. The median is reported.
        #[arg(long, default_value_t = 5)]
        iters: usize,
    },
    /// The honest admission A/B: range-only vs the real filtered path, closed-loop (plan 46).
    Admission {
        #[arg(long)]
        receipt: PathBuf,
        #[arg(long)]
        config: PathBuf,
        /// `unvacuumed` or `vacuumed`. Recorded, never performed — VACUUM is an operator step.
        #[arg(long)]
        phase: String,
        #[arg(long)]
        result: PathBuf,
        #[arg(long, default_value_t = 16)]
        workers: usize,
        #[arg(long, default_value_t = 5)]
        warmup_secs: u64,
        #[arg(long, default_value_t = 30)]
        duration_secs: u64,
        /// `range-first` (default) or `filter-first`, recorded for interleaved repetitions.
        #[arg(long)]
        path_order: Option<String>,
    },
    /// The issue-0014 A/B: range candidates, filter candidates, exact members.
    Catalog {
        #[arg(long)]
        manifest: PathBuf,
        #[arg(long)]
        label: String,
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        result: PathBuf,
    },
    /// Poll a compaction-input load until it is final (plan 46). Read-only.
    WaitCompacted {
        #[arg(long)]
        receipt: PathBuf,
        #[arg(long)]
        config: PathBuf,
        #[arg(long, default_value_t = 600)]
        timeout_secs: u64,
    },
    /// Scan a final fixture's objects and report the actual part shape (plan 46).
    PartShape {
        #[arg(long)]
        receipt: PathBuf,
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        result: PathBuf,
    },
}

fn main() -> ExitCode {
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    match rt.block_on(run()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("ukiel-prod-bench: {e:#}");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<()> {
    match Cli::parse().command {
        Command::Queries {
            manifest,
            label,
            receipt,
            config,
            result,
            iters,
        } => {
            require_read_only("queries")?;
            match (manifest, label, receipt) {
                (Some(m), Some(l), None) => cmd_queries(&m, &l, &config, &result, iters).await,
                (None, None, Some(r)) => cmd_queries_receipt(&r, &config, &result, iters).await,
                _ => bail!(
                    "queries takes either --manifest and --label (a materialized load) or \
                     --receipt (a compacted load), not a mix"
                ),
            }
        }
        Command::Admission {
            receipt,
            config,
            phase,
            result,
            workers,
            warmup_secs,
            duration_secs,
            path_order,
        } => {
            require_read_only("admission")?;
            let filter_first = match path_order.as_deref() {
                None | Some("range-first") => false,
                Some("filter-first") => true,
                Some(other) => bail!("unknown --path-order '{other}' (range-first | filter-first)"),
            };
            cmd_admission(
                &receipt,
                &config,
                &phase,
                &result,
                workers,
                warmup_secs,
                duration_secs,
                filter_first,
            )
            .await
        }
        Command::Catalog {
            manifest,
            label,
            config,
            result,
        } => {
            require_read_only("catalog")?;
            cmd_catalog(&manifest, &label, &config, &result).await
        }
        Command::WaitCompacted {
            receipt,
            config,
            timeout_secs,
        } => {
            require_read_only("wait-compacted")?;
            cmd_wait_compacted(&receipt, &config, timeout_secs).await
        }
        Command::PartShape {
            receipt,
            config,
            result,
        } => {
            require_read_only("part-shape")?;
            cmd_part_shape(&receipt, &config, &result).await
        }
    }
}

async fn cmd_wait_compacted(receipt_path: &Path, config: &Path, timeout_secs: u64) -> Result<()> {
    use std::time::Duration;
    use ukiel_prod_bench::part_shape::{Convergence, observe};

    let receipt = ukiel_prod_bench::read_receipt(receipt_path)?;
    let cfg = load_config(config)?;
    let catalog = PostgresCatalog::connect(&cfg.catalog.url).await?;
    let ht = ukiel_prod_bench::find_loaded(&catalog, &receipt).await?;
    let expected_rows = receipt.input_rows as i64;

    println!(
        "waiting for '{}' to converge ({} L0 runs in, {} rows), timeout {timeout_secs}s",
        receipt.hypertable, receipt.input_parts, receipt.input_rows,
    );

    let deadline = Instant::now() + Duration::from_secs(timeout_secs);
    let mut prev = observe(&catalog, ht, &receipt.partition_marker_digest).await?;
    let mut last_report = Instant::now();

    loop {
        tokio::time::sleep(Duration::from_millis(500)).await;
        let cur = observe(&catalog, ht, &receipt.partition_marker_digest).await?;

        if Convergence::converged(&prev, &cur, expected_rows) {
            println!(
                "converged: {} live parts across {} partition(s), {} rows, all marked",
                cur.live_parts, cur.partitions, cur.live_rows,
            );
            return Ok(());
        }

        // A live L0 part, a multi-run partition, or a wrong census means the compactor is
        // still working; changing part ids mean it is still in flight. Report why, but not
        // more than once a second, so the wait is legible without being noisy.
        if last_report.elapsed() >= Duration::from_secs(1) {
            println!(
                "  still changing: {} ({} live parts, {} L0, {} multi-run partitions)",
                cur.still_changing(expected_rows),
                cur.live_parts,
                cur.l0_parts,
                cur.multi_run_partitions,
            );
            last_report = Instant::now();
        }

        if Instant::now() >= deadline {
            bail!(
                "timed out after {timeout_secs}s: {}. The fixture never reached one final run per \
                 partition — check the compactor is running against this catalog with \
                 finalize_after_secs = 0.",
                cur.still_changing(expected_rows)
            );
        }
        prev = cur;
    }
}

async fn cmd_part_shape(receipt_path: &Path, config: &Path, result: &Path) -> Result<()> {
    use prod_synth_integrity::RowMultiset;
    use ukiel_prod_bench::part_shape::{build_report, filter_storage, observe, scan_object};

    let receipt = ukiel_prod_bench::read_receipt(receipt_path)?;
    let cfg = load_config(config)?;
    let catalog = PostgresCatalog::connect(&cfg.catalog.url).await?;
    let ht = ukiel_prod_bench::find_loaded(&catalog, &receipt).await?;

    // Require convergence before measuring: a shape read mid-compaction measures a
    // transient. `part-shape` does not wait — that is `wait-compacted`'s job — it just
    // refuses to run against a fixture that has not settled.
    let now = observe(&catalog, ht, &receipt.partition_marker_digest).await?;
    if now.l0_parts > 0 || now.multi_run_partitions > 0 {
        bail!(
            "'{}' is not final ({}). Run `wait-compacted` first — measuring a shape mid-compaction \
             measures a transient.",
            receipt.hypertable,
            now.still_changing(receipt.input_rows as i64),
        );
    }
    if !now.all_marked {
        bail!(
            "a live part is missing the artifact marker; this is not the fixture the receipt describes"
        );
    }

    println!("{}\n", receipt.disclaimer);
    println!(
        "scanning {} final parts of '{}' (placement {})",
        now.live_parts,
        receipt.hypertable,
        receipt.placement.as_str(),
    );

    use object_store::ObjectStoreExt as _;
    let (store, _url) = ukield::run::build_store(&cfg.object_store)?;
    let store: Arc<dyn object_store::ObjectStore> = store;

    let parts = catalog.live_parts(ht, None).await?;

    // Runs per partition, from the catalog.
    let mut runs: std::collections::BTreeMap<String, std::collections::BTreeSet<i64>> =
        std::collections::BTreeMap::new();
    for p in &parts {
        runs.entry(p.meta.partition_values.to_string())
            .or_default()
            .insert(p.created_by_commit.0);
    }
    let runs_per_partition: Vec<usize> = runs.values().map(|s| s.len()).collect();

    // Scan every object: exact keys, sortedness, row/byte agreement, bitmap truth, and
    // the full-row fingerprint. One object at a time — a shape-tier partition can be a
    // large file, and the scan holds only the one it is reading.
    let mut fingerprint = RowMultiset::default();
    let mut scanned = Vec::with_capacity(parts.len());
    for p in &parts {
        let bytes = store
            .get(&object_store::path::Path::from(p.meta.path.clone()))
            .await
            .with_context(|| format!("GET {}", p.meta.path))?
            .bytes()
            .await
            .with_context(|| format!("reading {}", p.meta.path))?;
        scanned.push(scan_object(
            p,
            &bytes,
            &receipt.packing_key,
            &mut fingerprint,
        )?);
    }

    let storage = filter_storage(&catalog, ht).await?;
    let report = build_report(&receipt, scanned, runs_per_partition, storage, &fingerprint);

    if !report.fingerprint_matches_input {
        bail!(
            "the compacted rows do not fingerprint to the staged input. A row was changed, \
             duplicated, or lost during compaction — this is a correctness failure, not a shape."
        );
    }

    print_part_shape(&report);
    write_result(result, serde_json::to_value(&report)?)
}

fn print_part_shape(r: &ukiel_prod_bench::part_shape::PartShapeReport) {
    println!(
        "\n'{}' — {} final parts, {} rows, {} bytes; fingerprint matches the staged input",
        r.hypertable, r.live_parts, r.live_rows, r.live_bytes,
    );
    println!(
        "  distinct keys/part  p10 {:.0}  p50 {:.0}  p90 {:.0}  p99 {:.0}  max {:.0}",
        r.distinct_keys.p10,
        r.distinct_keys.p50,
        r.distinct_keys.p90,
        r.distinct_keys.p99,
        r.distinct_keys.max,
    );
    println!(
        "  key density         p10 {:.4}  p50 {:.4}  p90 {:.4}",
        r.key_density.p10, r.key_density.p50, r.key_density.p90,
    );
    println!(
        "  file bytes          p50 {:.0}  p90 {:.0}  max {:.0}",
        r.file_bytes.p50, r.file_bytes.p90, r.file_bytes.max,
    );
    println!(
        "  levels {:?}   partitions {}   runs/partition p50 {:.0} max {:.0}   dedicated {:.1}%",
        r.level_histogram,
        r.partitions,
        r.runs_per_partition.p50,
        r.runs_per_partition.max,
        r.dedicated_fraction * 100.0
    );
    println!("\n  key-count bands (issue-0014 tiers):");
    for (band, n) in &r.key_bands {
        println!("    {band:<18} {n}");
    }
    println!(
        "\n  exact bitmaps: {} present, {} omitted",
        r.exact_bitmap_present, r.exact_bitmap_omitted
    );
    println!(
        "  key filters: {} NULL, by size {:?}",
        r.key_filter_null, r.key_filter_by_size
    );
    println!(
        "  filter bytes {} = {:.2}% of the {}-byte parts table, {:.2}% of the {}-byte live index",
        r.key_filter_bytes,
        pct(r.key_filter_bytes, r.parts_table_bytes),
        r.parts_table_bytes,
        pct(r.key_filter_bytes, r.live_index_bytes),
        r.live_index_bytes,
    );
    println!(
        "\n  68 parts is a GEOMETRY measurement, not a saturation test — plan 40 owns capacity."
    );
}

fn pct(num: i64, den: i64) -> f64 {
    if den == 0 {
        0.0
    } else {
        num as f64 / den as f64 * 100.0
    }
}

/// Read the artifact. Never regenerate it: a missing or mismatched fixture is an error,
/// not a job to do.
fn read_artifact(manifest_path: &Path) -> Result<(Manifest, Topology)> {
    let dir = manifest_path.parent().unwrap_or(Path::new("."));
    let mb = std::fs::read(manifest_path)
        .with_context(|| format!("reading {}", manifest_path.display()))?;
    let manifest = Manifest::parse(&manifest_path.display().to_string(), &mb)?;

    let tp = dir.join(&manifest.topology.path);
    let tb = std::fs::read(&tp).with_context(|| format!("reading {}", tp.display()))?;
    manifest.topology.verify(&tp.display().to_string(), &tb)?;
    let topology = Topology::parse(&tp.display().to_string(), &tb)?;
    Ok((manifest, topology))
}

fn load_config(path: &Path) -> Result<ukield::config::UkieldConfig> {
    let text =
        std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    toml::from_str(&text).with_context(|| format!("parsing {}", path.display()))
}

/// Find the load. It must already be there, and it must be *this* fixture.
async fn find_load(
    catalog: &PostgresCatalog,
    manifest: &Manifest,
    label: &str,
) -> Result<HypertableId> {
    let name = hypertable_name(label);
    let ht = catalog.get_hypertable(&name).await.map_err(|_| {
        anyhow::anyhow!(
            "no load named '{name}' in this catalog. Load it first:\n  \
             ukiel-prod-load materialized --manifest <manifest> --label {label} --config <config>\n\
             This tool measures a load; it does not create one."
        )
    })?;

    let live = catalog.live_parts(ht.id, None).await?;
    if live.len() != manifest.parts.len() {
        bail!(
            "'{name}' holds {} live parts but the manifest declares {}. This is a different \
             fixture, or an incomplete load. Refusing to measure it — the alternative is a number \
             that describes something nobody chose.",
            live.len(),
            manifest.parts.len()
        );
    }
    Ok(ht.id)
}

/// The tenant classes a report is organized by.
fn classes(m: &Manifest) -> Vec<(i64, String)> {
    let r = &m.representatives;
    vec![
        (r.heavy, "heavy".to_string()),
        (r.median, "median".to_string()),
        (r.light, "light".to_string()),
        (r.high_overfetch, "high_overfetch".to_string()),
        (r.low_overfetch, "low_overfetch".to_string()),
    ]
}

// ---------------------------------------------------------------------------
// catalog
// ---------------------------------------------------------------------------

async fn cmd_catalog(
    manifest_path: &Path,
    label: &str,
    config: &Path,
    result: &Path,
) -> Result<()> {
    let (manifest, topology) = read_artifact(manifest_path)?;
    let cfg = load_config(config)?;
    let catalog = PostgresCatalog::connect(&cfg.catalog.url).await?;
    let ht = find_load(&catalog, &manifest, label).await?;

    println!("{}\n", manifest.disclaimer);

    let rows = cat::measure(&catalog, ht, &manifest, &topology, &classes(&manifest)).await?;
    let summary = cat::summarize(&rows);

    println!(
        "catalog geometry — '{}' (seed {}, {} parts, {} tenants)",
        hypertable_name(label),
        manifest.config.seed,
        manifest.parts.len(),
        manifest.generated.tenants,
    );
    println!(
        "\n{:<16} {:>12} {:>7} {:>8} {:>7} {:>11} {:>11} {:>10}",
        "class", "tenant", "exact", "shipped", "range", "range o/f", "shipped o/f", "lookup"
    );
    for r in &rows {
        println!(
            "{:<16} {:>12} {:>7} {:>8} {:>7} {:>10.1}x {:>10.1}x {:>8.2}ms",
            r.class,
            r.tenant,
            r.exact,
            r.shipped,
            r.range,
            r.range_overfetch,
            r.shipped_overfetch,
            r.lookup_ms,
        );
    }
    println!(
        "\n  Across these {} tenants the key filter removed {:.1}% of the candidates that range \
         pruning alone would have shipped and did not need to \
         ({} range -> {} shipped, against {} exact).",
        summary.tenants,
        summary.overfetch_removed * 100.0,
        summary.total_range,
        summary.total_shipped,
        summary.total_exact,
    );
    println!(
        "  `shipped` above `exact` is what the Bloom filter's false positives still cost — the \
         honest ceiling on how much better this could get."
    );
    println!(
        "\n  68 parts is a geometry measurement, NOT a catalog saturation test. Plan 40 remains \
         the capacity proof."
    );

    write_result(
        result,
        serde_json::json!({
            "kind": "catalog",
            "disclaimer": manifest.disclaimer,
            "label": label,
            "hypertable": hypertable_name(label),
            "manifest": {
                "seed": manifest.config.seed,
                "tier": manifest.config.tier.as_str(),
                "topology_digest": manifest.topology.digest,
                "profile": manifest.source.files,
            },
            "rows": rows,
            "summary": summary,
        }),
    )
}

// ---------------------------------------------------------------------------
// queries
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, serde::Serialize)]
struct QueryResult {
    query: String,
    class: String,
    tenant: i64,
    rows: usize,
    ukiel_ms: f64,
    raw_ms: f64,
    /// Parts the catalog shipped for this tenant — the planning input the scan got.
    planned_parts: u64,
    agreed: bool,
    error: Option<String>,
}

async fn cmd_queries(
    manifest_path: &Path,
    label: &str,
    config: &Path,
    result: &Path,
    iters: usize,
) -> Result<()> {
    let (manifest, topology) = read_artifact(manifest_path)?;
    let cfg = load_config(config)?;
    let catalog = PostgresCatalog::connect(&cfg.catalog.url).await?;
    let ht = find_load(&catalog, &manifest, label).await?;

    let (store, store_url) = ukield::run::build_store(&cfg.object_store)?;
    let cache = Arc::new(ukiel_query::metadata_cache::ParquetMetadataCache::new(256));

    println!("{}\n", manifest.disclaimer);
    println!(
        "query suite — '{}' (seed {}, {} rows, {} parts), median of {iters} after one warmup",
        hypertable_name(label),
        manifest.config.seed,
        manifest.generated.rows,
        manifest.parts.len(),
    );

    // The raw reference: a bare DataFusion session over the very same objects. It has no
    // Ukiel catalog, no pruning, and no scoping — it reads every file and filters. That
    // is exactly what makes it a reference: it is the answer with nothing clever in the
    // way of it.
    let raw = raw_session(&store, &store_url, &manifest, label).await?;

    let (results, failures) = run_suite(
        &catalog,
        ht,
        &store,
        &store_url,
        &cache,
        &raw,
        &classes(&manifest),
        iters,
    )
    .await?;

    write_result(
        result,
        serde_json::json!({
            "kind": "queries",
            "disclaimer": manifest.disclaimer,
            "label": label,
            "iters": iters,
            "manifest": {
                "seed": manifest.config.seed,
                "tier": manifest.config.tier.as_str(),
                "topology_digest": manifest.topology.digest,
                "profile": manifest.source.files,
            },
            "generated": manifest.generated,
            "results": results,
        }),
    )?;

    let _ = topology;
    if failures > 0 {
        // Every query was attempted; the failure is reported after the rest, not instead
        // of it.
        bail!("{failures} query/class pair(s) failed. See the table above.");
    }
    Ok(())
}

/// Run the six-query suite over each tenant against a shared raw reference, requiring
/// scoped/raw equality before timing. Shared by the manifest and receipt query commands.
#[allow(clippy::too_many_arguments)]
async fn run_suite(
    catalog: &PostgresCatalog,
    ht: HypertableId,
    store: &Arc<dyn object_store::ObjectStore>,
    store_url: &url::Url,
    cache: &Arc<ukiel_query::metadata_cache::ParquetMetadataCache>,
    raw: &SessionContext,
    tenants: &[(i64, String)],
    iters: usize,
) -> Result<(Vec<QueryResult>, usize)> {
    let mut results = Vec::new();
    let mut failures = 0usize;

    for (tenant, class) in tenants {
        let (tenant, class) = (*tenant, class.clone());
        let scoped = ukiel_query::context::session_for_namespace(
            catalog,
            NamespaceId(tenant),
            store.clone(),
            store_url,
            cache.clone(),
        )
        .await
        .with_context(|| format!("opening a scoped session for tenant {tenant}"))?;

        let planned = catalog
            .live_parts_pruned(ht, Some(tenant), &[])
            .await?
            .len() as u64;

        for q in queries::suite() {
            let raw_sql = queries::scoped_to_raw(&q.sql, tenant);
            let fail = |failures: &mut usize, results: &mut Vec<QueryResult>, rows, msg: String| {
                *failures += 1;
                results.push(QueryResult {
                    query: q.id.clone(),
                    class: class.clone(),
                    tenant,
                    rows,
                    ukiel_ms: f64::NAN,
                    raw_ms: f64::NAN,
                    planned_parts: planned,
                    agreed: false,
                    error: Some(msg),
                });
            };

            // Correctness first: a timing taken before the two arms agree is a timing of
            // two different questions.
            let a = match queries::run(&scoped, &q.sql).await {
                Ok(v) => v,
                Err(e) => {
                    fail(
                        &mut failures,
                        &mut results,
                        0,
                        format!("scoped Ukiel: {e:#}"),
                    );
                    continue;
                }
            };
            let b = match queries::run(raw, &raw_sql).await {
                Ok(v) => v,
                Err(e) => {
                    fail(
                        &mut failures,
                        &mut results,
                        0,
                        format!("raw DataFusion: {e:#}"),
                    );
                    continue;
                }
            };
            if let Err(e) = queries::same(&a, &b) {
                fail(
                    &mut failures,
                    &mut results,
                    queries::row_count(&a),
                    format!("{e}"),
                );
                continue;
            }

            let ukiel_ms = median_ms(&scoped, &q.sql, iters).await?;
            let raw_ms = median_ms(raw, &raw_sql, iters).await?;
            results.push(QueryResult {
                query: q.id.clone(),
                class: class.clone(),
                tenant,
                rows: queries::row_count(&a),
                ukiel_ms,
                raw_ms,
                planned_parts: planned,
                agreed: true,
                error: None,
            });
        }
    }

    println!(
        "\n{:<8} {:<16} {:>12} {:>7} {:>7} {:>11} {:>11} {:>7}",
        "query", "class", "tenant", "parts", "rows", "ukiel", "raw", "ratio"
    );
    for r in &results {
        if let Some(e) = &r.error {
            println!(
                "{:<8} {:<16} {:>12}  FAILED: {}",
                r.query,
                r.class,
                r.tenant,
                first_line(e)
            );
            continue;
        }
        println!(
            "{:<8} {:<16} {:>12} {:>7} {:>7} {:>9.1}ms {:>9.1}ms {:>6.2}x",
            r.query,
            r.class,
            r.tenant,
            r.planned_parts,
            r.rows,
            r.ukiel_ms,
            r.raw_ms,
            r.ukiel_ms / r.raw_ms.max(1e-9),
        );
    }
    println!(
        "\n  `parts` is what the catalog shipped for that tenant. `raw` reads every file and \
         filters, so a ratio below 1 is the value of the catalog's pruning."
    );
    Ok((results, failures))
}

/// A bare DataFusion session over an explicit list of object paths — the final compacted
/// parts, from the catalog. After compaction the object paths are whatever REPLACE wrote,
/// so the raw reference reads exactly the live parts rather than guessing a prefix.
async fn raw_session_from_paths(
    store: &Arc<dyn object_store::ObjectStore>,
    store_url: &url::Url,
    paths: &[String],
) -> Result<SessionContext> {
    use datafusion::datasource::file_format::parquet::ParquetFormat;
    use datafusion::datasource::listing::{
        ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
    };

    let ctx = SessionContext::new();
    ctx.register_object_store(store_url, store.clone());
    let base = store_url.as_str().trim_end_matches('/');
    let urls: Vec<ListingTableUrl> = paths
        .iter()
        .map(|p| {
            ListingTableUrl::parse(format!("{base}/{p}"))
                .with_context(|| format!("building a raw URL for {p}"))
        })
        .collect::<Result<_>>()?;
    if urls.is_empty() {
        bail!("the fixture has no live parts to read");
    }
    let options =
        ListingOptions::new(Arc::new(ParquetFormat::default())).with_file_extension(".parquet");
    let schema = options.infer_schema(&ctx.state(), &urls[0]).await.context(
        "reading the compacted objects for the raw reference. A materialized compaction-input \
         load must have run — this reads real objects.",
    )?;
    let config = ListingTableConfig::new_with_multi_paths(urls)
        .with_listing_options(options)
        .with_schema(schema);
    ctx.register_table("events", Arc::new(ListingTable::try_new(config)?))?;
    Ok(ctx)
}

/// The compacted query-equivalence command: run the suite after compaction and require
/// scoped-Ukiel == raw-DataFusion for every query and tenant.
async fn cmd_queries_receipt(
    receipt_path: &Path,
    config: &Path,
    result: &Path,
    iters: usize,
) -> Result<()> {
    let receipt = ukiel_prod_bench::read_receipt(receipt_path)?;
    let cfg = load_config(config)?;
    let catalog = PostgresCatalog::connect(&cfg.catalog.url).await?;
    let ht = ukiel_prod_bench::find_loaded(&catalog, &receipt).await?;

    // Refuse a fixture that has not converged: query timings on a half-compacted fixture
    // measure a transient.
    let now = ukiel_prod_bench::part_shape::observe(&catalog, ht, &receipt.partition_marker_digest)
        .await?;
    if now.l0_parts > 0 || now.multi_run_partitions > 0 {
        bail!(
            "'{}' is not final ({}). Run `wait-compacted` first.",
            receipt.hypertable,
            now.still_changing(receipt.input_rows as i64)
        );
    }

    let (store, store_url) = ukield::run::build_store(&cfg.object_store)?;
    let store: Arc<dyn object_store::ObjectStore> = store;
    let cache = Arc::new(ukiel_query::metadata_cache::ParquetMetadataCache::new(256));

    println!("{}\n", receipt.disclaimer);
    println!(
        "compacted query suite — '{}' ({} final parts, {} rows), median of {iters}",
        receipt.hypertable, now.live_parts, now.live_rows,
    );

    // The raw reference reads exactly the final compacted objects.
    let paths: Vec<String> = catalog
        .live_parts(ht, None)
        .await?
        .into_iter()
        .map(|p| p.meta.path)
        .collect();
    let raw = raw_session_from_paths(&store, &store_url, &paths).await?;

    // A deterministic spread of queryable tenants — the whole set is exercised if small,
    // else an even sample across it.
    let all = ukiel_prod_bench::queryable_tenants(&catalog, ht).await?;
    let tenants: Vec<(i64, String)> = pick_spread(&all, 8)
        .into_iter()
        .map(|t| (t, format!("t{t}")))
        .collect();

    let (results, failures) = run_suite(
        &catalog, ht, &store, &store_url, &cache, &raw, &tenants, iters,
    )
    .await?;

    write_result(
        result,
        serde_json::json!({
            "kind": "queries-compacted",
            "disclaimer": receipt.disclaimer,
            "hypertable": receipt.hypertable,
            "placement": receipt.placement.as_str(),
            "iters": iters,
            "receipt": { "l0_digest": receipt.l0_manifest.digest, "source": receipt.source_manifest.digest },
            "final_parts": now.live_parts,
            "final_rows": now.live_rows,
            "results": results,
        }),
    )?;
    if failures > 0 {
        bail!("{failures} query/tenant pair(s) failed after compaction. See the table above.");
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn cmd_admission(
    receipt_path: &Path,
    config: &Path,
    phase: &str,
    result: &Path,
    workers: usize,
    warmup_secs: u64,
    duration_secs: u64,
    filter_first: bool,
) -> Result<()> {
    use ukiel_prod_bench::admission::{AdmissionConfig, Phase, run};

    let phase = match phase {
        "unvacuumed" => Phase::Unvacuumed,
        "vacuumed" => Phase::Vacuumed,
        other => bail!("unknown --phase '{other}' (unvacuumed | vacuumed)"),
    };
    // Refuse to overwrite a report or merge two phases into one file.
    if result.exists() {
        bail!(
            "{} already exists. Each admission phase writes its own file; refusing to overwrite \
             or merge phases.",
            result.display()
        );
    }

    let receipt = ukiel_prod_bench::read_receipt(receipt_path)?;
    let cfg = load_config(config)?;
    let catalog = PostgresCatalog::connect(&cfg.catalog.url).await?;
    let ht = ukiel_prod_bench::find_loaded(&catalog, &receipt).await?;
    let tenants = ukiel_prod_bench::queryable_tenants(&catalog, ht).await?;

    println!("{}\n", receipt.disclaimer);
    println!(
        "admission A/B — '{}', phase {phase:?}, {workers} workers, {warmup_secs}s warmup + \
         {duration_secs}s measured, {} tenants",
        receipt.hypertable,
        tenants.len(),
    );
    println!(
        "  NOTE: this command records the phase; it never runs VACUUM. The vacuumed phase \
         requires an operator-issued `VACUUM (ANALYZE) parts` beforehand."
    );

    let admission_cfg = AdmissionConfig {
        workers,
        warmup: std::time::Duration::from_secs(warmup_secs),
        duration: std::time::Duration::from_secs(duration_secs),
        filter_first,
    };
    let report = run(&catalog, ht, tenants, phase, &admission_cfg).await?;

    let pr = |p: &ukiel_prod_bench::admission::PathResult| {
        println!(
            "  {:<11} {:>9.0} ops/s   p50 {:>6.2}  p95 {:>6.2}  p99 {:>6.2}  max {:>7.2} ms   \
             {:>6.1} parts  {:>8.0} tuple-bytes  residue {:.1}",
            p.path.label(),
            p.throughput_per_sec,
            p.p50_ms,
            p.p95_ms,
            p.p99_ms,
            p.max_ms,
            p.mean_parts,
            p.mean_tuple_bytes,
            p.mean_provider_residue,
        );
    };
    println!();
    pr(&report.range_only);
    pr(&report.filtered);
    println!(
        "\n  the pre-0014 range-only path shipped {:.1}x the tuple bytes for the same answers.",
        report.bytes_shipped_ratio,
    );

    write_result(result, serde_json::to_value(&report)?)
}

/// A deterministic even spread of at most `k` values from a sorted list.
fn pick_spread(all: &[i64], k: usize) -> Vec<i64> {
    if all.len() <= k {
        return all.to_vec();
    }
    let mut out: Vec<i64> = (0..k).map(|i| all[i * (all.len() - 1) / (k - 1)]).collect();
    out.dedup();
    out
}

/// One warmup, then the median of `iters`. The median, not the mean: a single slow run
/// (a page fault, a GC pause in the object store) should not become the number.
async fn median_ms(ctx: &SessionContext, sql: &str, iters: usize) -> Result<f64> {
    let _ = queries::run(ctx, sql).await?;

    let mut times = Vec::with_capacity(iters);
    for _ in 0..iters.max(1) {
        let start = Instant::now();
        let _ = queries::run(ctx, sql).await?;
        times.push(start.elapsed().as_secs_f64() * 1000.0);
    }
    times.sort_by(|a, b| a.partial_cmp(b).unwrap());
    Ok(times[times.len() / 2])
}

/// A bare DataFusion session over the fixture's objects, with no Ukiel in the way.
///
/// Built from the **manifest's explicit file list**, not from a prefix listing. The
/// reference must read exactly the fixture's parts — no more (a stray object under the
/// prefix would silently join the answer) and no fewer (a listing that failed to recurse
/// into the shard directories would quietly compare against a subset and call it
/// agreement, which is what the first version of this did).
async fn raw_session(
    store: &Arc<dyn object_store::ObjectStore>,
    store_url: &url::Url,
    manifest: &Manifest,
    label: &str,
) -> Result<SessionContext> {
    use datafusion::datasource::file_format::parquet::ParquetFormat;
    use datafusion::datasource::listing::{
        ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
    };

    let ctx = SessionContext::new();
    ctx.register_object_store(store_url, store.clone());

    let base = store_url.as_str().trim_end_matches('/');
    let urls: Vec<ListingTableUrl> = manifest
        .parts
        .iter()
        .map(|p| {
            ListingTableUrl::parse(format!("{base}/prod-synth/{label}/{}", p.path))
                .with_context(|| format!("building a raw URL for {}", p.path))
        })
        .collect::<Result<_>>()?;

    let options =
        ListingOptions::new(Arc::new(ParquetFormat::default())).with_file_extension(".parquet");
    let schema = options.infer_schema(&ctx.state(), &urls[0]).await.context(
        "reading the fixture's objects for the raw reference. The MATERIALIZED load must have \
             run: the catalog-only arm writes no objects, so there is nothing here to read.",
    )?;

    let config = ListingTableConfig::new_with_multi_paths(urls)
        .with_listing_options(options)
        .with_schema(schema);
    ctx.register_table("events", Arc::new(ListingTable::try_new(config)?))?;
    Ok(ctx)
}

fn first_line(s: &str) -> &str {
    s.lines().next().unwrap_or(s)
}

fn write_result(path: &Path, value: serde_json::Value) -> Result<()> {
    std::fs::write(path, serde_json::to_vec_pretty(&value)?)
        .with_context(|| format!("writing {}", path.display()))?;
    println!("\nwrote {}", path.display());
    Ok(())
}

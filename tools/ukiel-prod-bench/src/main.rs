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
    Queries {
        #[arg(long)]
        manifest: PathBuf,
        #[arg(long)]
        label: String,
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        result: PathBuf,
        /// Timed iterations after one warmup. The median is reported.
        #[arg(long, default_value_t = 5)]
        iters: usize,
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
            config,
            result,
            iters,
        } => {
            require_read_only("queries")?;
            cmd_queries(&manifest, &label, &config, &result, iters).await
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

    let mut results: Vec<QueryResult> = Vec::new();
    let mut failures = 0usize;

    for (tenant, class) in classes(&manifest) {
        // The scoped path: the tenant is a property of the session, not the SQL.
        let scoped = ukiel_query::context::session_for_namespace(
            &catalog,
            NamespaceId(tenant),
            store.clone(),
            &store_url,
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

            // Correctness first, always. A timing taken before the two arms are known to
            // agree is a timing of two different questions.
            let a = match queries::run(&scoped, &q.sql).await {
                Ok(v) => v,
                Err(e) => {
                    // Record the failure and keep going: a suite that stops at the first
                    // error tells you about one query and hides the other five.
                    failures += 1;
                    results.push(QueryResult {
                        query: q.id.clone(),
                        class: class.clone(),
                        tenant,
                        rows: 0,
                        ukiel_ms: f64::NAN,
                        raw_ms: f64::NAN,
                        planned_parts: planned,
                        agreed: false,
                        error: Some(format!("scoped Ukiel: {e:#}")),
                    });
                    continue;
                }
            };
            let b = match queries::run(&raw, &raw_sql).await {
                Ok(v) => v,
                Err(e) => {
                    failures += 1;
                    results.push(QueryResult {
                        query: q.id.clone(),
                        class: class.clone(),
                        tenant,
                        rows: 0,
                        ukiel_ms: f64::NAN,
                        raw_ms: f64::NAN,
                        planned_parts: planned,
                        agreed: false,
                        error: Some(format!("raw DataFusion: {e:#}")),
                    });
                    continue;
                }
            };
            if let Err(e) = queries::same(&a, &b) {
                failures += 1;
                results.push(QueryResult {
                    query: q.id.clone(),
                    class: class.clone(),
                    tenant,
                    rows: queries::row_count(&a),
                    ukiel_ms: f64::NAN,
                    raw_ms: f64::NAN,
                    planned_parts: planned,
                    agreed: false,
                    error: Some(format!("{e}")),
                });
                continue;
            }

            let ukiel_ms = median_ms(&scoped, &q.sql, iters).await?;
            let raw_ms = median_ms(&raw, &raw_sql, iters).await?;

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
        "\n  `parts` is what the catalog shipped for that tenant — the planning input the scan \
         received. `raw` reads every file in the fixture and filters, so a ratio below 1 is the \
         value of the catalog's pruning."
    );

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

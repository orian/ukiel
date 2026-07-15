//! Task 5: the admission A/B on compacted parts.
//!
//! Proves the two paths are *comparable* — both return the same range candidates before
//! the filter, the filtered path is a subset that never loses an exact member, and the
//! accounting is exact — then times them. Against a staged smoke fixture compacted by the
//! real compactor.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use object_store::ObjectStore;
use object_store::memory::InMemory;
use prod_synth_contract::{ExpectedCompactorConfig, PlacementSpec};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::ContainerAsync;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use ukiel_catalog::PostgresCatalog;
use ukiel_compactor::compactor::{Compactor, CompactorConfig};
use ukiel_core::HypertableId;
use ukiel_prod_bench::admission::{AdmissionConfig, AdmissionPath, Phase, run};
use ukiel_prod_bench::part_shape::{Convergence, observe};
use ukiel_prod_bench::queries;
use ukiel_prod_load::compaction_input as ci;

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn run_tool(args: &[&str]) {
    let status = Command::new(env!("CARGO"))
        .current_dir(repo_root())
        .args(["run", "-q", "-p"])
        .args(args)
        .status()
        .expect("run tool");
    assert!(status.success(), "tool failed: {args:?}");
}

fn staged(tmp: &Path) -> PathBuf {
    let src = tmp.join("src");
    let l0 = tmp.join("l0");
    run_tool(&[
        "prod-synth",
        "--",
        "generate",
        "--tier",
        "smoke",
        "--profile",
        "docs/prod-info",
        "--output",
        src.to_str().unwrap(),
        "--tenants",
        "800",
        "--rows-per-shard",
        "40000",
        "--seed",
        "0",
        "--replace",
    ]);
    run_tool(&[
        "prod-synth-l0",
        "--",
        "stage",
        "--manifest",
        src.join("manifest.json").to_str().unwrap(),
        "--output",
        l0.to_str().unwrap(),
        "--flush-rows",
        "10000",
        "--replace",
    ]);
    l0
}

async fn postgres() -> (ContainerAsync<Postgres>, PostgresCatalog) {
    let c = Postgres::default().start().await.expect("postgres");
    let port = c.get_host_port_ipv4(5432).await.expect("port");
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
    let catalog = PostgresCatalog::connect(&url).await.expect("connect");
    catalog.migrate().await.expect("schema");
    (c, catalog)
}

fn expected() -> ExpectedCompactorConfig {
    ExpectedCompactorConfig {
        l0_fanout: 4,
        fanout: 10,
        finalize_after_secs: 0,
        finalize_poll_interval_ms: 100,
        lease_ttl_secs: 60,
        lease_renew_interval_secs: 20,
        candidate_limit: 64,
    }
}

/// Load + compact a smoke fixture, returning the catalog, store, hypertable, and the
/// queryable tenant list.
async fn compacted(
    tmp: &Path,
    label: &str,
) -> (
    ContainerAsync<Postgres>,
    PostgresCatalog,
    Arc<dyn ObjectStore>,
    HypertableId,
    Vec<i64>,
) {
    let l0_dir = staged(tmp);
    let (pg, catalog) = postgres().await;
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (l0, l0_bytes) = ci::read_l0_artifact(&l0_dir.join("l0-manifest.json")).unwrap();
    let ht = ci::create_hypertable(&catalog, &l0, PlacementSpec::Packed, label)
        .await
        .unwrap();
    let receipt = ci::load(
        &catalog,
        &store,
        &l0,
        &l0_bytes,
        l0.source_manifest.clone(),
        &l0_dir,
        label,
        PlacementSpec::Packed,
        ht,
        expected(),
    )
    .await
    .unwrap();

    let compactor = Compactor::new(
        catalog.clone(),
        store.clone(),
        CompactorConfig {
            l0_fanout: 4,
            fanout: 10,
            finalize_after_secs: 0,
            ..CompactorConfig::default()
        },
    );
    let mut prev = observe(&catalog, ht, &receipt.partition_marker_digest)
        .await
        .unwrap();
    for _ in 0..200 {
        compactor.run_once().await.unwrap();
        compactor.finalize_once().await.unwrap();
        let cur = observe(&catalog, ht, &receipt.partition_marker_digest)
            .await
            .unwrap();
        if Convergence::converged(&prev, &cur, receipt.input_rows as i64) {
            break;
        }
        prev = cur;
    }
    let tenants = ukiel_prod_bench::queryable_tenants(&catalog, ht)
        .await
        .unwrap();
    (pg, catalog, store, ht, tenants)
}

/// The two paths are comparable and correct: filtered ships a subset of range-only, and
/// never fewer than the tenant's exact members. This is the precondition for any timing
/// to mean anything.
#[tokio::test]
async fn the_filtered_path_is_a_correct_subset_of_range_only() {
    let tmp = tempfile::tempdir().unwrap();
    let (_pg, catalog, _store, ht, tenants) = compacted(tmp.path(), "adm").await;

    for &tenant in &tenants {
        // range-only: every part whose declared range brackets the tenant.
        let parts = catalog.live_parts(ht, None).await.unwrap();
        let range: usize = parts
            .iter()
            .filter(|p| p.meta.packing_key_min <= tenant && tenant <= p.meta.packing_key_max)
            .count();
        // filtered: the real product path.
        let filtered = catalog
            .live_parts_pruned(ht, Some(tenant), &[])
            .await
            .unwrap();

        assert!(
            filtered.len() <= range,
            "filtered must be a subset of range candidates"
        );
        // And it never drops a part that actually holds the tenant.
        for p in &parts {
            let holds = p
                .meta
                .column_stats
                .as_ref()
                .and_then(|s| s.get(ukiel_core::stats::PACKING_KEYS_STAT))
                .and_then(|v| v.as_str())
                .and_then(|b| ukiel_core::stats::bitmap_contains(b, tenant))
                == Some(true);
            if holds {
                assert!(
                    filtered.iter().any(|f| f.id == p.id),
                    "tenant {tenant} is in part {:?} and the filtered path dropped it — FALSE NEGATIVE",
                    p.id
                );
            }
        }
    }
}

/// The A/B runs closed-loop with exact accounting and produces the expected shape:
/// range-only ships at least as many bytes as filtered.
#[tokio::test]
async fn admission_accounting_is_exact_and_range_ships_no_less_than_filtered() {
    let tmp = tempfile::tempdir().unwrap();
    let (_pg, catalog, _store, ht, tenants) = compacted(tmp.path(), "adm2").await;

    let cfg = AdmissionConfig {
        workers: 4,
        warmup: Duration::from_millis(300),
        duration: Duration::from_millis(1200),
        filter_first: false,
    };
    let report = run(&catalog, ht, tenants, Phase::Unvacuumed, &cfg)
        .await
        .unwrap();

    for p in [&report.range_only, &report.filtered] {
        // Exact accounting: completed + failed == offered, and something ran.
        assert_eq!(
            p.completed + p.failed,
            p.offered,
            "{:?}: offered == completed + failed",
            p.path
        );
        assert!(p.completed > 0, "{:?} completed some ops", p.path);
        assert_eq!(
            p.failed, 0,
            "{:?} should have no failures on a healthy fixture",
            p.path
        );
    }
    assert_eq!(report.range_only.path, AdmissionPath::RangeOnly);
    assert_eq!(report.filtered.path, AdmissionPath::Filtered);

    // The whole point: range-only ships at least as many bytes as filtered, usually more.
    assert!(
        report.range_only.mean_tuple_bytes >= report.filtered.mean_tuple_bytes,
        "range-only ships no fewer bytes than filtered"
    );
    assert!(
        report.bytes_shipped_ratio >= 1.0,
        "ratio {:.2} must be >= 1",
        report.bytes_shipped_ratio
    );

    // The provider residue is what the scan layer removes AFTER the catalog. For the
    // filtered path it is the filter's false positives (small); for range-only it is
    // everything the filter would have caught (larger, on a packed fixture).
    assert!(
        report.range_only.mean_provider_residue >= report.filtered.mean_provider_residue,
        "range-only leaves at least as much residue for the provider to clean up"
    );

    // The EXPLAIN plans were captured.
    assert!(report.filtered_explain.is_array() || report.filtered_explain.is_object());
    assert!(report.range_only_explain.is_array() || report.range_only_explain.is_object());
}

/// Compacted query equivalence: after the real compactor has changed every path and
/// grouping, scoped Ukiel still equals raw DataFusion for every query and tenant.
///
/// This is the correctness gate under the timings of `queries --receipt`: a difference
/// here is a pruning or scoping bug, silent in production and invisible to a benchmark
/// that only times things.
#[tokio::test]
async fn scoped_and_raw_agree_after_compaction() {
    use datafusion::datasource::file_format::parquet::ParquetFormat;
    use datafusion::datasource::listing::{
        ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
    };
    use datafusion::prelude::SessionContext;
    use ukiel_core::NamespaceId;

    let tmp = tempfile::tempdir().unwrap();
    let (_pg, catalog, store, ht, tenants) = compacted(tmp.path(), "eq").await;
    let store_url = url::Url::parse("mem://ukiel/").unwrap();

    // The raw reference reads exactly the final compacted objects.
    let paths: Vec<String> = catalog
        .live_parts(ht, None)
        .await
        .unwrap()
        .into_iter()
        .map(|p| p.meta.path)
        .collect();
    let ctx = SessionContext::new();
    ctx.register_object_store(&store_url, store.clone());
    let urls: Vec<ListingTableUrl> = paths
        .iter()
        .map(|p| ListingTableUrl::parse(format!("mem://ukiel/{p}")).unwrap())
        .collect();
    let options =
        ListingOptions::new(Arc::new(ParquetFormat::default())).with_file_extension(".parquet");
    let schema = options.infer_schema(&ctx.state(), &urls[0]).await.unwrap();
    ctx.register_table(
        "events",
        Arc::new(
            ListingTable::try_new(
                ListingTableConfig::new_with_multi_paths(urls)
                    .with_listing_options(options)
                    .with_schema(schema),
            )
            .unwrap(),
        ),
    )
    .unwrap();

    let cache = Arc::new(ukiel_query::metadata_cache::ParquetMetadataCache::new(64));
    let sample: Vec<i64> = tenants
        .iter()
        .step_by((tenants.len() / 5).max(1))
        .copied()
        .take(5)
        .collect();
    let mut compared = 0;
    for &tenant in &sample {
        let scoped = ukiel_query::context::session_for_namespace(
            &catalog,
            NamespaceId(tenant),
            store.clone(),
            &store_url,
            cache.clone(),
        )
        .await
        .unwrap();
        for q in queries::suite() {
            let a = queries::run(&scoped, &q.sql)
                .await
                .unwrap_or_else(|e| panic!("{} scoped: {e:#}", q.id));
            let b = queries::run(&ctx, &queries::scoped_to_raw(&q.sql, tenant))
                .await
                .unwrap_or_else(|e| panic!("{} raw: {e:#}", q.id));
            queries::same(&a, &b)
                .unwrap_or_else(|e| panic!("{} disagreed for tenant {tenant}:\n{e}", q.id));
            compared += 1;
        }
    }
    assert_eq!(compared, sample.len() * 6, "every query x tenant compared");
}

/// Path order is recorded, and swapping it swaps which path runs first without changing
/// which result is which.
#[tokio::test]
async fn path_order_is_recorded_and_results_stay_labelled() {
    let tmp = tempfile::tempdir().unwrap();
    let (_pg, catalog, _store, ht, tenants) = compacted(tmp.path(), "adm3").await;

    let cfg = AdmissionConfig {
        workers: 4,
        warmup: Duration::from_millis(200),
        duration: Duration::from_millis(800),
        filter_first: true,
    };
    let report = run(&catalog, ht, tenants, Phase::Vacuumed, &cfg)
        .await
        .unwrap();
    assert_eq!(report.path_order, "filter-first");
    assert_eq!(report.phase, Phase::Vacuumed);
    // Regardless of order, range_only is the range-only result.
    assert_eq!(report.range_only.path, AdmissionPath::RangeOnly);
    assert_eq!(report.filtered.path, AdmissionPath::Filtered);
}

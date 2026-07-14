//! Task 5: the end-to-end equivalence between scoped Ukiel and raw DataFusion.
//!
//! This is the check that makes every timing in the suite mean something. The two arms
//! read the same Parquet objects. One reaches them through the Ukiel catalog, with
//! range pruning, the issue-0014 key filter, and a namespace-scoped isolation predicate
//! standing between the query and the files. The other lists the objects and filters
//! them itself, with nothing clever in the way.
//!
//! If they ever return different rows, the difference is not a performance result — it
//! is a pruning bug (a part that holds rows was skipped) or a scoping bug (rows from
//! another tenant leaked in). Both are silent in production and both would be invisible
//! in a benchmark that only timed things.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::prelude::SessionContext;
use object_store::ObjectStore;
use object_store::memory::InMemory;
use prod_synth_contract::{Manifest, Topology};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::ContainerAsync;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use ukiel_catalog::PostgresCatalog;
use ukiel_core::NamespaceId;
use ukiel_prod_bench::{catalog as cat, queries};
use ukiel_prod_load::load;

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// The real generator binary. The tools are joined by the manifest on disk, and the test
/// honours that rather than linking around it.
fn generate(dir: &Path, tenants: u64, rows: u64) {
    let status = Command::new(env!("CARGO"))
        .current_dir(repo_root())
        .args([
            "run",
            "-q",
            "-p",
            "prod-synth",
            "--",
            "generate",
            "--tier",
            "smoke",
            "--profile",
            "docs/prod-info",
            "--output",
            dir.to_str().unwrap(),
            "--tenants",
            &tenants.to_string(),
            "--rows-per-shard",
            &rows.to_string(),
            "--seed",
            "0",
            "--replace",
        ])
        .status()
        .expect("run prod-synth");
    assert!(status.success(), "prod-synth generate failed");
}

async fn postgres() -> (ContainerAsync<Postgres>, PostgresCatalog) {
    let c = Postgres::default().start().await.expect("postgres");
    let port = c.get_host_port_ipv4(5432).await.expect("port");
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
    let catalog = PostgresCatalog::connect(&url).await.expect("connect");
    catalog.migrate().await.expect("schema");
    (c, catalog)
}

struct Fixture {
    _pg: ContainerAsync<Postgres>,
    catalog: PostgresCatalog,
    store: Arc<dyn ObjectStore>,
    store_url: url::Url,
    manifest: Manifest,
    topology: Topology,
    hypertable: ukiel_core::HypertableId,
}

/// Generate, load, and hand back everything a measurement needs.
async fn loaded(dir: &Path, tenants: u64, rows: u64) -> Fixture {
    generate(dir, tenants, rows);

    let (pg, catalog) = postgres().await;
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let store_url = url::Url::parse("mem://ukiel/").unwrap();

    let (manifest, topology) =
        load::read_artifact(&dir.join("manifest.json")).expect("the fixture verifies");
    let ht = load::create_hypertable(&catalog, &manifest, "bench")
        .await
        .expect("create");
    load::load_materialized(&catalog, &store, &manifest, &topology, dir, "bench", ht)
        .await
        .expect("load");

    Fixture {
        _pg: pg,
        catalog,
        store,
        store_url,
        manifest,
        topology,
        hypertable: ht,
    }
}

/// The raw reference: a bare DataFusion session over the same objects, with no Ukiel.
///
/// Built from the **manifest's explicit file list**, not from a prefix listing. The
/// reference must read exactly the fixture's parts — no more (a stray object under the
/// prefix would silently join the answer) and no fewer (a listing that failed to recurse
/// would quietly compare against a subset and call it agreement).
async fn raw_session(f: &Fixture) -> SessionContext {
    let ctx = SessionContext::new();
    ctx.register_object_store(&f.store_url, f.store.clone());

    let urls: Vec<ListingTableUrl> = f
        .manifest
        .parts
        .iter()
        .map(|p| {
            ListingTableUrl::parse(format!("mem://ukiel/prod-synth/bench/{}", p.path)).expect("url")
        })
        .collect();
    assert_eq!(urls.len(), f.manifest.parts.len());

    let options =
        ListingOptions::new(Arc::new(ParquetFormat::default())).with_file_extension(".parquet");
    let schema = options
        .infer_schema(&ctx.state(), &urls[0])
        .await
        .expect("infer the fixture's schema");
    let config = ListingTableConfig::new_with_multi_paths(urls)
        .with_listing_options(options)
        .with_schema(schema);
    ctx.register_table(
        "events",
        Arc::new(ListingTable::try_new(config).expect("listing table")),
    )
    .expect("register");
    ctx
}

/// **The** test. Every query, every class, scoped against raw.
#[tokio::test]
async fn scoped_ukiel_and_raw_datafusion_agree_on_every_query_and_class() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("fx");
    let f = loaded(&dir, 300, 40_000).await;
    let raw = raw_session(&f).await;

    let cache = Arc::new(ukiel_query::metadata_cache::ParquetMetadataCache::new(64));
    let r = &f.manifest.representatives;
    let classes = [
        (r.heavy, "heavy"),
        (r.median, "median"),
        (r.light, "light"),
        (r.high_overfetch, "high_overfetch"),
        (r.low_overfetch, "low_overfetch"),
    ];

    let mut compared = 0;
    for (tenant, class) in classes {
        // The scoped path: the tenant is a property of the session. The SQL below never
        // names it, and could not.
        let scoped = ukiel_query::context::session_for_namespace(
            &f.catalog,
            NamespaceId(tenant),
            f.store.clone(),
            &f.store_url,
            cache.clone(),
        )
        .await
        .unwrap_or_else(|e| panic!("scoped session for {class} tenant {tenant}: {e}"));

        for q in queries::suite() {
            let a = queries::run(&scoped, &q.sql)
                .await
                .unwrap_or_else(|e| panic!("{} scoped ({class}): {e:#}", q.id));
            let b = queries::run(&raw, &queries::scoped_to_raw(&q.sql, tenant))
                .await
                .unwrap_or_else(|e| panic!("{} raw ({class}): {e:#}", q.id));

            queries::same(&a, &b)
                .unwrap_or_else(|e| panic!("{} disagreed for the {class} tenant:\n{e}", q.id));
            compared += 1;
        }
    }
    assert_eq!(compared, 30, "6 queries x 5 classes");
}

/// The scoped count is the census: what the topology says the tenant has, the query
/// returns. Not "close to" — equal.
#[tokio::test]
async fn the_scoped_event_count_equals_the_generators_census() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("fx");
    let f = loaded(&dir, 300, 40_000).await;

    let cache = Arc::new(ukiel_query::metadata_cache::ParquetMetadataCache::new(64));
    let rows_of: std::collections::BTreeMap<i64, u64> =
        f.topology.tenants.iter().map(|t| (t.id, t.rows)).collect();

    for tenant in &f.manifest.representatives.sample {
        let scoped = ukiel_query::context::session_for_namespace(
            &f.catalog,
            NamespaceId(*tenant),
            f.store.clone(),
            &f.store_url,
            cache.clone(),
        )
        .await
        .expect("scoped session");

        let batches = queries::run(&scoped, "SELECT count(*) AS n FROM events")
            .await
            .expect("count");
        let n = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .expect("int64")
            .value(0) as u64;

        assert_eq!(
            n, rows_of[tenant],
            "tenant {tenant}: the scoped query returned {n} rows, the generator's census says {}",
            rows_of[tenant]
        );
    }
}

/// A tenant's scan sees only the parts the catalog shipped for it — and the catalog
/// shipped strictly fewer than the range predicate alone would have.
///
/// This is issue 0014, measured on production-shaped geometry rather than asserted.
#[tokio::test]
async fn the_key_filter_removes_range_candidates_that_hold_no_rows() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("fx");
    let f = loaded(&dir, 300, 40_000).await;

    let r = &f.manifest.representatives;
    let classes: Vec<(i64, String)> = vec![
        (r.heavy, "heavy".into()),
        (r.median, "median".into()),
        (r.light, "light".into()),
        (r.high_overfetch, "high_overfetch".into()),
        (r.low_overfetch, "low_overfetch".into()),
    ];

    let rows = cat::measure(&f.catalog, f.hypertable, &f.manifest, &f.topology, &classes)
        .await
        .expect("no false negatives, and no candidate the filter invented");

    for row in &rows {
        // The one-directional guarantee. Everything else here is performance; this is
        // correctness.
        assert!(
            row.shipped >= row.exact,
            "{}: shipped {} < exact {} — a part that holds rows was pruned away",
            row.class,
            row.shipped,
            row.exact
        );
        assert!(
            row.shipped <= row.range,
            "{}: a filter can only remove candidates, never add them",
            row.class
        );
    }

    let s = cat::summarize(&rows);
    assert!(
        s.total_shipped < s.total_range,
        "the key filter must actually reject range candidates on this geometry: \
         range {} -> shipped {} (exact {})",
        s.total_range,
        s.total_shipped,
        s.total_exact
    );

    // The high-overfetch tenant is the whole point of the fixture: range pruning ships it
    // nearly every part, and almost none of them hold its rows.
    let high = rows
        .iter()
        .find(|r| r.class == "high_overfetch")
        .expect("class");
    assert!(
        high.range_overfetch > 5.0,
        "the fixture must reproduce production's over-fetch, got {:.1}x",
        high.range_overfetch
    );
    assert!(
        high.shipped_overfetch < high.range_overfetch,
        "and the filter must collapse it: {:.1}x -> {:.1}x",
        high.range_overfetch,
        high.shipped_overfetch
    );
}

/// The runner is read-only, and says so before it does anything.
#[test]
fn a_mutating_command_is_refused_at_startup() {
    ukiel_prod_bench::require_read_only("queries").expect("measuring is what it does");
    ukiel_prod_bench::require_read_only("catalog").expect("measuring is what it does");

    for bad in ["generate", "load", "repair", "clean", "migrate"] {
        let e = ukiel_prod_bench::require_read_only(bad)
            .unwrap_err()
            .to_string();
        assert!(e.contains("read-only"), "{bad}: {e}");
        assert!(
            e.contains("prod-synth") && e.contains("ukiel-prod-load"),
            "{bad}: the refusal must say where the work actually belongs: {e}"
        );
    }
}

//! Task 4: the loader, end to end.
//!
//! Against a throwaway PostgreSQL (testcontainers) and an in-memory object store, so
//! this runs in `make test` rather than only against the compose stack. The plan
//! sketched these as `--ignored`; there is no reason for them to be, and a check that
//! only runs when someone remembers to run it is a check that rots.
//!
//! The fixture is produced by shelling out to the **real `prod-synth` binary**, not by
//! linking it. That is the tool boundary being exercised rather than described: the
//! loader cannot generate a fixture, so the test cannot either, and what gets loaded is
//! exactly what an operator would copy onto a benchmark host.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use object_store::memory::InMemory;
use object_store::{ObjectStore, ObjectStoreExt};
use prod_synth_contract::{Manifest, Topology};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::ContainerAsync;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use ukiel_catalog::PostgresCatalog;
use ukiel_prod_load::{catalog_only, load};

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// Generate a tiny fixture with the real generator binary.
fn generate(dir: &Path, tenants: u64, rows: u64, seed: u64) {
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
            dir.to_str().expect("utf8 path"),
            "--tenants",
            &tenants.to_string(),
            "--rows-per-shard",
            &rows.to_string(),
            "--seed",
            &seed.to_string(),
            "--replace",
        ])
        .status()
        .expect("run prod-synth");
    assert!(status.success(), "prod-synth generate failed");
}

async fn postgres() -> (ContainerAsync<Postgres>, PostgresCatalog) {
    let container = Postgres::default().start().await.expect("start postgres");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("mapped port");
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
    let catalog = PostgresCatalog::connect(&url).await.expect("connect");
    catalog.migrate().await.expect("schema");
    (container, catalog)
}

fn artifact(dir: &Path) -> (Manifest, Topology) {
    load::read_artifact(&dir.join("manifest.json")).expect("a fresh fixture verifies")
}

/// The load is *truthful*: object HEAD, manifest, and catalog all agree, and each was
/// measured independently.
#[tokio::test]
async fn a_materialized_load_agrees_with_the_object_store_and_the_manifest() {
    let tmp = tempfile::tempdir().unwrap();
    let fx = tmp.path().join("fx");
    generate(&fx, 200, 20_000, 0);

    let (_pg, catalog) = postgres().await;
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (manifest, topology) = artifact(&fx);

    let ht = load::create_hypertable(&catalog, &manifest, "t1")
        .await
        .expect("create");
    let metas = load::load_materialized(&catalog, &store, &manifest, &topology, &fx, "t1", ht)
        .await
        .expect("load");

    // The catalog's view.
    let live = catalog.live_parts(ht, None).await.expect("live parts");
    assert_eq!(live.len(), manifest.parts.len(), "every part is registered");

    let cat_rows: i64 = live.iter().map(|p| p.meta.row_count).sum();
    let cat_bytes: i64 = live.iter().map(|p| p.meta.size_bytes).sum();
    assert_eq!(cat_rows as u64, manifest.generated.rows, "rows");
    assert_eq!(cat_bytes as u64, manifest.generated.bytes, "bytes");

    // The object store's view — a HEAD per object, which is the only source that has
    // actually seen the bytes land.
    for m in &metas {
        let head = store
            .head(&object_store::path::Path::from(m.path.clone()))
            .await
            .unwrap_or_else(|e| panic!("HEAD {}: {e}", m.path));
        assert_eq!(
            head.size as i64, m.size_bytes,
            "{}: the catalog's size must be the object's size",
            m.path
        );
    }

    // Every part carries the key index the catalog needs, and it is the *file's* key
    // set — the loader reads it back out of the Parquet rather than trusting the graph.
    let multi: Vec<_> = live
        .iter()
        .filter(|p| p.meta.packing_key_min != p.meta.packing_key_max)
        .collect();
    assert!(
        !multi.is_empty(),
        "a production-shaped fixture has packed parts"
    );
    for p in &multi {
        let stats = p.meta.column_stats.as_ref().expect("column stats");
        let encoded = stats
            .get(ukiel_core::stats::PACKING_KEYS_STAT)
            .and_then(|v| v.as_str())
            .unwrap_or_else(|| panic!("part {} has no packing-key bitmap", p.meta.path));
        let keys = ukiel_core::stats::bitmap_keys(encoded).expect("decodable");

        // The decoded bitmap is exactly the manifest's key set for that part.
        let idx = manifest
            .parts
            .iter()
            .find(|mp| p.meta.path.ends_with(&mp.path))
            .expect("a manifest part");
        let members: Vec<i64> = topology
            .shards
            .iter()
            .flat_map(|s| s.memberships.iter())
            .find(|m| m.part == idx.index)
            .expect("topology part")
            .tenants
            .clone();
        assert_eq!(
            keys, members,
            "part {}: the bitmap is the graph's key set",
            idx.index
        );
    }
}

/// The one-directional guarantee: the key filter may keep a part it could have skipped,
/// but it may **never** drop one that holds the tenant.
///
/// A false negative here is a row that has silently vanished from a query result, with
/// nothing in any log. It is the only failure in this fixture that cannot be noticed by
/// looking at the answer.
#[tokio::test]
async fn every_member_is_returned_for_its_own_key_and_absent_parts_are_pruned() {
    let tmp = tempfile::tempdir().unwrap();
    let fx = tmp.path().join("fx");
    generate(&fx, 300, 30_000, 0);

    let (_pg, catalog) = postgres().await;
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (manifest, topology) = artifact(&fx);

    let ht = load::create_hypertable(&catalog, &manifest, "t2")
        .await
        .unwrap();
    load::load_materialized(&catalog, &store, &manifest, &topology, &fx, "t2", ht)
        .await
        .unwrap();

    // Zero false negatives, checked across the deterministic sample.
    let checked = load::assert_no_false_negatives(&catalog, ht, &manifest, &topology)
        .await
        .expect("no part that holds a tenant may be pruned away from it");
    assert!(checked >= 8, "the sample must actually span the population");

    // And the filter earns its place: it must ship materially fewer parts than the
    // range predicate alone would. This is the entire behaviour under test — if the
    // filter shipped everything the range shipped, issue 0014 bought nothing.
    let geometry = load::measure_geometry(&catalog, ht, &manifest, &topology)
        .await
        .expect("geometry");

    for g in &geometry {
        assert!(
            g.shipped >= g.exact,
            "{}: shipped {} < exact {} — FALSE NEGATIVE",
            g.class,
            g.shipped,
            g.exact
        );
        assert!(
            g.shipped <= g.range,
            "{}: the filter can only ever remove candidates, never add them",
            g.class
        );
    }

    let total_range: u64 = geometry.iter().map(|g| g.range).sum();
    let total_shipped: u64 = geometry.iter().map(|g| g.shipped).sum();
    assert!(
        total_shipped < total_range,
        "the key filter must reject parts the range predicate would have shipped: \
         range {total_range}, shipped {total_shipped}"
    );
}

/// A corrupt artifact fails on the filesystem, before a single byte reaches a service.
#[tokio::test]
async fn a_corrupt_artifact_is_refused_before_connecting_to_anything() {
    let tmp = tempfile::tempdir().unwrap();
    let fx = tmp.path().join("fx");
    generate(&fx, 200, 20_000, 0);

    // Swap in a topology from another seed. Without the manifest's digest this would be
    // a subtly wrong benchmark rather than an error.
    let other = tmp.path().join("other");
    generate(&other, 200, 20_000, 7);
    std::fs::copy(other.join("topology.json"), fx.join("topology.json")).unwrap();

    let e = load::read_artifact(&fx.join("manifest.json"))
        .expect_err("a topology from another seed must not load");
    assert!(
        e.to_string().contains("digest mismatch"),
        "must be a digest error, not a mysterious downstream failure: {e}"
    );
}

/// An unknown manifest version fails closed rather than being half-read.
#[tokio::test]
async fn an_unknown_manifest_version_is_refused() {
    let tmp = tempfile::tempdir().unwrap();
    let fx = tmp.path().join("fx");
    generate(&fx, 200, 20_000, 0);

    let path = fx.join("manifest.json");
    let mut v: serde_json::Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    v["manifest_version"] = serde_json::json!("ukiel-prod-synth/v99");
    std::fs::write(&path, serde_json::to_vec_pretty(&v).unwrap()).unwrap();

    let e = load::read_artifact(&path).expect_err("an unknown version must fail closed");
    assert!(e.to_string().contains("v99"), "{e}");
}

/// A label is not reusable. A fixture loaded over another produces a catalog that is a
/// mixture of two seeds, and no report drawn from it means anything.
#[tokio::test]
async fn a_reused_label_is_refused() {
    let tmp = tempfile::tempdir().unwrap();
    let fx = tmp.path().join("fx");
    generate(&fx, 200, 20_000, 0);

    let (_pg, catalog) = postgres().await;
    let (manifest, _topology) = artifact(&fx);

    load::require_fresh_label(&catalog, "dup")
        .await
        .expect("fresh");
    load::create_hypertable(&catalog, &manifest, "dup")
        .await
        .unwrap();

    let e = load::require_fresh_label(&catalog, "dup")
        .await
        .expect_err("the second load under one label must be refused");
    assert!(e.to_string().contains("already exists"), "{e}");
}

// ---------------------------------------------------------------------------
// The catalog-only arm.
// ---------------------------------------------------------------------------

/// The catalog-only arm seeds the *same graph* and therefore reproduces the *same*
/// membership geometry — with no objects at all. If it did not, the cheap arm and the
/// expensive arm would be measuring different things and only one of them would know.
#[tokio::test]
async fn catalog_only_reproduces_the_materialized_geometry_without_objects() {
    let tmp = tempfile::tempdir().unwrap();
    let fx = tmp.path().join("fx");
    generate(&fx, 300, 30_000, 0);
    let (manifest, topology) = artifact(&fx);

    // The expensive arm.
    let (_pg1, cat1) = postgres().await;
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let ht1 = load::create_hypertable(&cat1, &manifest, "mat")
        .await
        .unwrap();
    load::load_materialized(&cat1, &store, &manifest, &topology, &fx, "mat", ht1)
        .await
        .unwrap();
    let mat = load::measure_geometry(&cat1, ht1, &manifest, &topology)
        .await
        .unwrap();

    // The cheap arm.
    let (_pg2, cat2) = postgres().await;
    let (ht2, parts) = catalog_only::seed(&cat2, &manifest, &topology, "cat")
        .await
        .expect("seed");
    let cheap = load::measure_geometry(&cat2, ht2, &manifest, &topology)
        .await
        .unwrap();

    // Identical geometry: same tenants, same exact, same range, same shipped.
    assert_eq!(mat.len(), cheap.len());
    for (m, c) in mat.iter().zip(&cheap) {
        assert_eq!(m.tenant, c.tenant, "{}", m.class);
        assert_eq!(m.exact, c.exact, "{}: exact", m.class);
        assert_eq!(m.range, c.range, "{}: range", m.class);
        assert_eq!(
            m.shipped, c.shipped,
            "{}: the catalog-only arm must ship exactly what the materialized one ships — the \
             key filter is derived from the same key set either way",
            m.class
        );
    }

    // And it is honest about what it is: no objects, and no invented byte counts.
    assert!(
        parts.iter().all(|p| p.size_bytes == 0),
        "a catalog-only part records no size rather than a plausible fake one"
    );
    assert!(
        parts
            .iter()
            .all(|p| p.path.starts_with(ukiel_prod_load::CATALOG_ONLY_SCHEME)),
        "catalog-only paths must be un-fetchable, so nothing can read one by accident"
    );
    assert!(
        catalog_only::part_rows(&manifest, &topology)
            .unwrap()
            .iter()
            .all(|p| p.row_count > 0),
        "the row counts are real: nothing about a row count needs a file to be true"
    );
}

/// The catalog-only arm refuses to run where a background worker could pick up a part
/// that has no object.
#[test]
fn catalog_only_refuses_a_stack_with_a_compactor_or_gc() {
    use ukield::config::Role;

    catalog_only::require_disposable(&[Role::Query, Role::Ingest])
        .expect("a query/ingest-only stack is fine");

    let e = catalog_only::require_disposable(&[Role::Query, Role::Compactor])
        .expect_err("a compactor would try to merge a file that was never written");
    assert!(e.to_string().contains("Compactor"), "{e}");

    let e = catalog_only::require_disposable(&[Role::Gc])
        .expect_err("a GC sweeper would try to reap an object that does not exist");
    assert!(e.to_string().contains("Gc"), "{e}");
}

/// Cleanup removes everything, so a disposable stack really is left disposable.
#[tokio::test]
async fn catalog_only_cleans_up_after_itself() {
    let tmp = tempfile::tempdir().unwrap();
    let fx = tmp.path().join("fx");
    generate(&fx, 200, 20_000, 0);
    let (manifest, topology) = artifact(&fx);

    let (_pg, catalog) = postgres().await;
    let (ht, _) = catalog_only::seed(&catalog, &manifest, &topology, "gone")
        .await
        .unwrap();
    assert!(!catalog.live_parts(ht, None).await.unwrap().is_empty());

    catalog_only::cleanup(&catalog, "gone")
        .await
        .expect("cleanup must succeed");

    assert!(
        catalog
            .get_hypertable(&ukiel_prod_load::hypertable_name("gone"))
            .await
            .is_err(),
        "the hypertable is gone"
    );
    // And so is every part row: no fake path is left for a background compactor to
    // trip over.
    let left: i64 =
        sqlx::query_scalar("SELECT count(*) FROM parts WHERE path LIKE 'catalog-only://%'")
            .fetch_one(catalog.pool_for_tests())
            .await
            .unwrap();
    assert_eq!(left, 0, "no part rows without objects may survive cleanup");
}

/// And when cleanup cannot finish, the operator is handed the exact command rather than
/// a shrug.
#[test]
fn a_failed_cleanup_prints_the_manual_reset() {
    let sql = catalog_only::manual_reset("prod_synth_events_x");
    assert!(sql.contains("DELETE FROM parts"));
    assert!(sql.contains("DELETE FROM hypertables"));
    assert!(sql.contains("prod_synth_events_x"));
    assert!(
        sql.contains("BEGIN;") && sql.contains("COMMIT;"),
        "it must be atomic"
    );
}

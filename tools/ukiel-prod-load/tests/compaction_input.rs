//! Task 3: loading a staged L0 artifact as real compaction input.
//!
//! The fixture is produced by the real `prod-synth` and `prod-synth-l0` binaries — the
//! tools are joined by the artifacts on disk, and the loader links neither. Runs against
//! a throwaway PostgreSQL and an in-memory object store, so it is part of `make test`.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use object_store::memory::InMemory;
use object_store::{ObjectStore, ObjectStoreExt};
use prod_synth_contract::{ExpectedCompactorConfig, L0Manifest, PlacementSpec};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::ContainerAsync;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use ukiel_catalog::PostgresCatalog;
use ukiel_core::Placement;
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

/// Generate a tiny source and stage it to L0. Returns the L0 manifest directory.
fn staged(tmp: &Path, tenants: u64, rows: u64, flush: u64) -> PathBuf {
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
        &tenants.to_string(),
        "--rows-per-shard",
        &rows.to_string(),
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
        &flush.to_string(),
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

fn expected_config() -> ExpectedCompactorConfig {
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

async fn load(
    catalog: &PostgresCatalog,
    store: &Arc<dyn ObjectStore>,
    l0_dir: &Path,
    label: &str,
    placement: PlacementSpec,
) -> (
    ukiel_core::HypertableId,
    prod_synth_contract::PartShapeReceipt,
    L0Manifest,
) {
    let (l0, l0_bytes) = ci::read_l0_artifact(&l0_dir.join("l0-manifest.json")).expect("read L0");
    let ht = ci::create_hypertable(catalog, &l0, placement, label)
        .await
        .expect("create");
    let receipt = ci::load(
        catalog,
        store,
        &l0,
        &l0_bytes,
        l0.source_manifest.clone(),
        l0_dir,
        label,
        placement,
        ht,
        expected_config(),
    )
    .await
    .expect("load");
    (ht, receipt, l0)
}

/// The load is truthful and shaped as compaction input: one L0 run per staged file,
/// level 0, object/catalog bytes and rows agree, the placement is set, and every part
/// carries the artifact+day marker.
#[tokio::test]
async fn a_compaction_input_load_is_truthful_and_shaped_as_l0() {
    let tmp = tempfile::tempdir().unwrap();
    let l0_dir = staged(tmp.path(), 300, 30_000, 10_000);

    let (_pg, catalog) = postgres().await;
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (ht, receipt, l0) = load(&catalog, &store, &l0_dir, "packed", PlacementSpec::Packed).await;

    let live = catalog.live_parts(ht, None).await.unwrap();

    // One live part per staged file — each committed separately, each its own run.
    assert_eq!(live.len(), l0.files.len(), "one L0 run per staged file");
    assert_eq!(receipt.input_parts, l0.files.len() as u64);

    // Every part is level 0: this is real ingest-shaped input, so the compactor climbs
    // the ladder from the bottom.
    assert!(live.iter().all(|p| p.meta.level == 0), "all inputs are L0");

    // Distinct commits: one created_by_commit per file, not one bulk commit.
    let commits: std::collections::BTreeSet<i64> =
        live.iter().map(|p| p.created_by_commit.0).collect();
    assert_eq!(
        commits.len(),
        l0.files.len(),
        "one commit per file, so one run per file"
    );

    // Rows and bytes: object HEAD = catalog = manifest, each measured independently.
    let cat_rows: i64 = live.iter().map(|p| p.meta.row_count).sum();
    let cat_bytes: i64 = live.iter().map(|p| p.meta.size_bytes).sum();
    assert_eq!(cat_rows as u64, l0.output_rows, "rows");
    assert_eq!(cat_rows as u64, receipt.input_rows);
    assert_eq!(cat_bytes as u64, receipt.input_bytes);
    for p in &live {
        let head = store
            .head(&object_store::path::Path::from(p.meta.path.clone()))
            .await
            .unwrap();
        assert_eq!(
            head.size as i64, p.meta.size_bytes,
            "{}: HEAD == catalog",
            p.meta.path
        );
    }

    // Placement is set on the hypertable.
    let ht_row = catalog.get_hypertable_by_id(ht).await.unwrap();
    assert_eq!(ht_row.placement, Placement::Packed);

    // Every part carries the {l0_manifest, utc_day} marker, and the identity half is the
    // one the receipt records — this is what lets a compacted part prove its descent.
    let l0_digest =
        prod_synth_contract::digest_bytes(&std::fs::read(l0_dir.join("l0-manifest.json")).unwrap());
    for p in &live {
        let pv = &p.meta.partition_values;
        assert_eq!(
            pv["l0_manifest"].as_str().unwrap(),
            l0_digest,
            "{}",
            p.meta.path
        );
        assert!(
            pv["utc_day"].as_str().is_some(),
            "{}: has a day",
            p.meta.path
        );
    }
    assert_eq!(
        receipt.partition_marker_digest,
        prod_synth_contract::PartShapeReceipt::marker_digest(&l0_digest)
    );

    // The row fingerprint is carried through unchanged.
    assert_eq!(receipt.fingerprint, l0.fingerprint);
}

/// Placement is honoured. Each arm gets its own catalog — the matrix's "isolated
/// disposable stack per arm" is not a nicety: logical tables are keyed by
/// `(namespace_id, name)` globally, so a tenant's `events` table can belong to only one
/// fixture per catalog, and two fixtures cannot coexist in one.
#[tokio::test]
async fn placement_is_recorded_on_the_hypertable() {
    let tmp = tempfile::tempdir().unwrap();
    let l0_dir = staged(tmp.path(), 200, 20_000, 10_000);

    for (label, spec, expect) in [
        ("sep", PlacementSpec::Separated, Placement::Separated),
        (
            "st",
            PlacementSpec::SizeTargeted(64 * 1024 * 1024),
            Placement::SizeTargeted(64 * 1024 * 1024),
        ),
    ] {
        let (_pg, catalog) = postgres().await;
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (ht, _, _) = load(&catalog, &store, &l0_dir, label, spec).await;
        assert_eq!(
            catalog.get_hypertable_by_id(ht).await.unwrap().placement,
            expect
        );
    }
}

/// One `events` logical table per queryable tenant, so the scoped query path can be
/// exercised after compaction.
#[tokio::test]
async fn a_scoped_table_exists_for_every_queryable_tenant() {
    let tmp = tempfile::tempdir().unwrap();
    let l0_dir = staged(tmp.path(), 300, 30_000, 10_000);
    let (_pg, catalog) = postgres().await;
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (ht, _, l0) = load(&catalog, &store, &l0_dir, "q", PlacementSpec::Packed).await;

    let want = ukiel_prod_load::load::queryable_tenants(&l0.representatives);
    for tenant in &want {
        let t = catalog
            .get_logical_table(ukiel_core::NamespaceId(*tenant), "events")
            .await;
        assert!(t.is_ok(), "tenant {tenant} must have a scoped events table");
    }
    let _ = ht;
}

/// A reused label is refused — a partial pre-existing load is dropped deliberately, never
/// resumed.
#[tokio::test]
async fn a_reused_label_is_refused() {
    let tmp = tempfile::tempdir().unwrap();
    let l0_dir = staged(tmp.path(), 200, 20_000, 10_000);
    let (_pg, catalog) = postgres().await;
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    load(&catalog, &store, &l0_dir, "dup", PlacementSpec::Packed).await;
    let e = ci::require_fresh(&catalog, "dup")
        .await
        .expect_err("reused label");
    assert!(e.to_string().contains("already exists"), "{e}");
}

/// A tampered staged file is refused before service access.
#[tokio::test]
async fn a_tampered_staged_file_is_refused() {
    let tmp = tempfile::tempdir().unwrap();
    let l0_dir = staged(tmp.path(), 200, 20_000, 10_000);

    let l0: L0Manifest = L0Manifest::parse(
        "m",
        &std::fs::read(l0_dir.join("l0-manifest.json")).unwrap(),
    )
    .unwrap();
    let victim = l0_dir.join(&l0.files[0].path);
    let mut bytes = std::fs::read(&victim).unwrap();
    bytes[80] ^= 0xff;
    std::fs::write(&victim, bytes).unwrap();

    let e = ci::read_l0_artifact(&l0_dir.join("l0-manifest.json"))
        .expect_err("a changed staged file must not load");
    assert!(e.to_string().contains("digest mismatch"), "{e}");
}

/// The disposable-catalog guard refuses a catalog holding a stranger's hypertable.
#[tokio::test]
async fn a_non_disposable_catalog_is_refused() {
    let (_pg, catalog) = postgres().await;

    // A hypertable that is not one of the experiment's.
    catalog
        .create_hypertable(
            "someones_real_table",
            &serde_json::json!({"fields": [{"name": "team_id", "type": "int64"}]}),
            &serde_json::json!({"fields": [{"name": "day", "type": "utf8"}]}),
            &["team_id".to_string()],
            "team_id",
        )
        .await
        .unwrap();

    let e = ukiel_prod_load::require_disposable_catalog(&catalog, &[])
        .await
        .expect_err("a foreign hypertable must stop the load");
    assert!(e.to_string().contains("someones_real_table"), "{e}");

    // ...unless the operator explicitly allows it.
    ukiel_prod_load::require_disposable_catalog(&catalog, &["someones_real_table".to_string()])
        .await
        .expect("an allow-listed table is accepted");
}

//! Task 4: convergence and exact output-part inspection, end to end.
//!
//! The pure logic (transition counting, key bands, quantiles, convergence predicates) is
//! unit-tested inside the module. This drives the **real compactor** in the harness —
//! `Compactor::run_once`/`finalize_once`, the same code a `ukield` process runs — against
//! a staged smoke fixture loaded through `compaction-input`, then proves the output
//! census and the scanned membership graph.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use object_store::ObjectStore;
use object_store::memory::InMemory;
use prod_synth_contract::PlacementSpec;
use prod_synth_integrity::RowMultiset;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::ContainerAsync;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use ukiel_catalog::PostgresCatalog;
use ukiel_compactor::compactor::{Compactor, CompactorConfig};
use ukiel_core::HypertableId;
use ukiel_prod_bench::part_shape::{Convergence, observe, scan_object};
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
        "500",
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

/// Drive the real compactor until the fixture converges: no L0, one run per partition,
/// stable across two polls. `finalize_after_secs = 0` so a cold partition finalizes
/// immediately.
async fn compact_to_convergence(
    catalog: &PostgresCatalog,
    store: &Arc<dyn ObjectStore>,
    ht: HypertableId,
    marker_digest: &str,
    expected_rows: i64,
) -> Convergence {
    let compactor = Compactor::new(
        catalog.clone(),
        store.clone(),
        CompactorConfig {
            l0_fanout: 4,
            fanout: 10,
            finalize_after_secs: 0,
            candidate_limit: 64,
            ..CompactorConfig::default()
        },
    );

    let mut prev = observe(catalog, ht, marker_digest).await.unwrap();
    for _ in 0..200 {
        // The ladder, then the finalizer — the same two calls `Compactor::run` alternates.
        compactor.run_once().await.expect("run_once");
        compactor.finalize_once().await.expect("finalize_once");
        let cur = observe(catalog, ht, marker_digest).await.unwrap();
        if Convergence::converged(&prev, &cur, expected_rows) {
            return cur;
        }
        prev = cur;
    }
    panic!(
        "did not converge in 200 rounds: {}",
        prev.still_changing(expected_rows)
    );
}

/// The whole point of plan 46: the real compactor turns staged L0 into final parts, and
/// the output is provably the same rows in a new shape.
#[tokio::test]
async fn the_real_compactor_produces_final_parts_that_preserve_the_rows() {
    let tmp = tempfile::tempdir().unwrap();
    let l0_dir = staged(tmp.path());

    let (_pg, catalog) = postgres().await;
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (l0, l0_bytes) = ci::read_l0_artifact(&l0_dir.join("l0-manifest.json")).unwrap();
    let ht = ci::create_hypertable(&catalog, &l0, PlacementSpec::Packed, "e2e")
        .await
        .unwrap();
    let receipt = ci::load(
        &catalog,
        &store,
        &l0,
        &l0_bytes,
        l0.source_manifest.clone(),
        &l0_dir,
        "e2e",
        PlacementSpec::Packed,
        ht,
        prod_synth_contract::ExpectedCompactorConfig {
            l0_fanout: 4,
            fanout: 10,
            finalize_after_secs: 0,
            finalize_poll_interval_ms: 100,
            lease_ttl_secs: 60,
            lease_renew_interval_secs: 20,
            candidate_limit: 64,
        },
    )
    .await
    .unwrap();

    // Before compaction: many L0 runs.
    let before = observe(&catalog, ht, &receipt.partition_marker_digest)
        .await
        .unwrap();
    assert_eq!(before.l0_parts, l0.files.len(), "starts as pure L0");
    assert!(
        before.multi_run_partitions > 0,
        "starts with multiple runs per partition"
    );

    // Compact.
    let after = compact_to_convergence(
        &catalog,
        &store,
        ht,
        &receipt.partition_marker_digest,
        receipt.input_rows as i64,
    )
    .await;

    // After: no L0, one run per partition, and materially fewer parts than went in.
    assert_eq!(after.l0_parts, 0, "the ladder drained");
    assert_eq!(
        after.multi_run_partitions, 0,
        "every partition finalized to one run"
    );
    assert!(
        after.live_parts < before.live_parts,
        "compaction merged: {} -> {}",
        before.live_parts,
        after.live_parts
    );
    assert_eq!(
        after.live_rows, receipt.input_rows as i64,
        "no row lost or duplicated"
    );
    assert!(
        after.all_marked,
        "every final part still carries the artifact marker"
    );

    // The rows are provably the same multiset: scan every final object and fingerprint.
    use object_store::ObjectStoreExt as _;
    let parts = catalog.live_parts(ht, None).await.unwrap();
    let mut fp = RowMultiset::default();
    let mut total_distinct_bands = 0;
    for p in &parts {
        let bytes = store
            .get(&object_store::path::Path::from(p.meta.path.clone()))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let scanned = scan_object(p, &bytes, &receipt.packing_key, &mut fp).unwrap();
        // Every final part is a real merged part at level >= 1.
        assert!(scanned.level >= 1, "a final part is compacted, not L0");
        // The distinct-key count came from scanning the sorted column, and it is sane.
        assert!(scanned.distinct_keys >= 1);
        total_distinct_bands += scanned.distinct_keys;
    }
    assert!(total_distinct_bands > 0);

    // The fingerprint of the compacted output equals the staged input's — the strongest
    // possible statement that compaction changed the shape and nothing else.
    assert_eq!(fp.count, receipt.fingerprint.count, "row count");
    assert_eq!(
        fp.digest_hex(),
        receipt.fingerprint.digest,
        "the compacted rows ARE the staged rows"
    );
}

/// `part-shape`'s convergence gate: it refuses to measure a fixture that still has L0.
#[tokio::test]
async fn observe_reports_an_unconverged_fixture_as_still_changing() {
    let tmp = tempfile::tempdir().unwrap();
    let l0_dir = staged(tmp.path());

    let (_pg, catalog) = postgres().await;
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (l0, l0_bytes) = ci::read_l0_artifact(&l0_dir.join("l0-manifest.json")).unwrap();
    let ht = ci::create_hypertable(&catalog, &l0, PlacementSpec::Packed, "raw")
        .await
        .unwrap();
    let receipt = ci::load(
        &catalog,
        &store,
        &l0,
        &l0_bytes,
        l0.source_manifest.clone(),
        &l0_dir,
        "raw",
        PlacementSpec::Packed,
        ht,
        prod_synth_contract::ExpectedCompactorConfig {
            l0_fanout: 4,
            fanout: 10,
            finalize_after_secs: 0,
            finalize_poll_interval_ms: 100,
            lease_ttl_secs: 60,
            lease_renew_interval_secs: 20,
            candidate_limit: 64,
        },
    )
    .await
    .unwrap();

    // Freshly loaded, before any compaction: pure L0, definitely not converged.
    let obs = observe(&catalog, ht, &receipt.partition_marker_digest)
        .await
        .unwrap();
    assert!(obs.l0_parts > 0);
    assert!(
        obs.still_changing(receipt.input_rows as i64).contains("L0"),
        "an all-L0 fixture must report itself unconverged"
    );
    assert!(!Convergence::converged(
        &obs,
        &obs,
        receipt.input_rows as i64
    ));
}

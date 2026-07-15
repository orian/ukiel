//! The `from-ukiel` adapter, end to end. TESTCONTAINERS: needs Docker (Postgres). It
//! stages a smoke fixture, loads it through `compaction-input`, drives the REAL compactor
//! to convergence in-harness — the same code a `ukield` process runs — and then freezes
//! the converged load into a snapshot, proving the snapshot's physical fingerprint equals
//! the staged input and that it verifies offline afterwards.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use object_store::ObjectStore;
use object_store::memory::InMemory;
use prod_synth_contract::{ExpectedCompactorConfig, PlacementSpec};

/// Convert a prod-synth contract digest into the parquet-lab one (same shape, distinct type).
fn pc(d: &prod_synth_contract::FileDigest) -> parquet_lab_contract::FileDigest {
    parquet_lab_contract::FileDigest {
        path: d.path.clone(),
        bytes: d.bytes,
        digest: d.digest.clone(),
    }
}
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::ContainerAsync;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use ukiel_catalog::PostgresCatalog;
use ukiel_compactor::compactor::{Compactor, CompactorConfig};
use ukiel_core::HypertableId;
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

fn compactor_config() -> ExpectedCompactorConfig {
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

async fn compact_to_convergence(
    catalog: &PostgresCatalog,
    store: &Arc<dyn ObjectStore>,
    ht: HypertableId,
    expected_rows: i64,
) {
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
    for _ in 0..200 {
        compactor.run_once().await.expect("run_once");
        compactor.finalize_once().await.expect("finalize_once");
        let parts = catalog.live_parts(ht, None).await.unwrap();
        let l0 = parts.iter().filter(|p| p.meta.level == 0).count();
        let rows: i64 = parts.iter().map(|p| p.meta.row_count).sum();
        let mut runs: std::collections::BTreeMap<String, std::collections::BTreeSet<i64>> =
            std::collections::BTreeMap::new();
        for p in &parts {
            runs.entry(p.meta.partition_values.to_string())
                .or_default()
                .insert(p.created_by_commit.0);
        }
        let multi = runs.values().filter(|c| c.len() > 1).count();
        if l0 == 0 && multi == 0 && rows == expected_rows && !parts.is_empty() {
            return;
        }
    }
    panic!("did not converge in 200 rounds");
}

#[tokio::test]
async fn from_ukiel_freezes_a_converged_load_into_a_verified_snapshot() {
    let tmp = tempfile::tempdir().unwrap();
    let l0_dir = staged(tmp.path());

    let (_pg, catalog) = postgres().await;
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (l0, l0_bytes) = ci::read_l0_artifact(&l0_dir.join("l0-manifest.json")).unwrap();
    let ht = ci::create_hypertable(&catalog, &l0, PlacementSpec::Packed, "snap")
        .await
        .unwrap();
    let receipt = ci::load(
        &catalog,
        &store,
        &l0,
        &l0_bytes,
        l0.source_manifest.clone(),
        &l0_dir,
        "snap",
        PlacementSpec::Packed,
        ht,
        compactor_config(),
    )
    .await
    .unwrap();

    // Freezing an unconverged (all-L0) load must be refused.
    let out = tmp.path().join("snap-out");
    let l0_digest = parquet_lab_contract::FileDigest::of("l0-manifest.json".to_string(), &l0_bytes);
    let err = parquet_lab_snapshot::from_ukiel::freeze(
        &catalog,
        &store,
        &receipt,
        &l0,
        l0_digest.clone(),
        pc(&l0.source_manifest),
        &out,
        "test".into(),
        false,
    )
    .await
    .unwrap_err();
    assert!(
        err.to_string().contains("L0"),
        "an unconverged load is refused: {err}"
    );

    // Compact to convergence, then freeze.
    compact_to_convergence(&catalog, &store, ht, receipt.input_rows as i64).await;
    let manifest_path = parquet_lab_snapshot::from_ukiel::freeze(
        &catalog,
        &store,
        &receipt,
        &l0,
        l0_digest,
        pc(&l0.source_manifest),
        &out,
        "test".into(),
        false,
    )
    .await
    .unwrap();

    // The snapshot carries the physical fingerprint the receipt recorded, and a logical one.
    let manifest = parquet_lab_contract::SnapshotManifest::parse(
        &manifest_path.display().to_string(),
        &std::fs::read(&manifest_path).unwrap(),
    )
    .unwrap();
    assert_eq!(manifest.total_rows, receipt.input_rows);
    let physical = manifest
        .physical_fingerprint
        .expect("a from-ukiel snapshot carries one");
    assert_eq!(
        physical.digest, receipt.fingerprint.digest,
        "rows preserved through compaction"
    );
    assert!(
        manifest.disclaimer.is_some(),
        "synthetic source carries its disclaimer"
    );
    assert_eq!(manifest.packing_key, receipt.packing_key);

    // And it verifies offline.
    let report = parquet_lab_snapshot::verify(&manifest_path).unwrap();
    assert_eq!(report.rows, receipt.input_rows);
}

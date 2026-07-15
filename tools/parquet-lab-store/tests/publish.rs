//! Local publish + verify round-trip and the refusal behaviours. No object-store service
//! needed: a `local` store is a filesystem directory. The MinIO path is operator-gated.

use std::path::Path;

use parquet_lab_contract::{
    Fingerprint, LogicalProjection, SNAPSHOT_MANIFEST_VERSION, SnapshotFile, SnapshotManifest,
    SourceKind, digest_bytes,
};
use parquet_lab_store::StoreConfig;

/// Write a one-file snapshot (the file bytes need not be Parquet — the store copies bytes).
fn write_artifact(dir: &Path) -> std::path::PathBuf {
    let rel = "parquet/part-00000.parquet";
    let path = dir.join(rel);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let bytes = b"laboratory artifact bytes".to_vec();
    std::fs::write(&path, &bytes).unwrap();
    let manifest = SnapshotManifest {
        manifest_version: SNAPSHOT_MANIFEST_VERSION.into(),
        source_kind: SourceKind::ExplicitFiles,
        source_digests: vec![],
        tool_versions: parquet_lab_contract::ToolVersions {
            git_sha: "t".into(),
            arrow: "58.3".into(),
            parquet: "58.3".into(),
            datafusion: "54".into(),
        },
        creation_command: "t".into(),
        logical_schema: serde_json::json!({}),
        physical_schema: serde_json::json!({"fields": []}),
        packing_key: "team_id".into(),
        sort_key: vec![],
        logical_projection: Some(LogicalProjection {
            logical_types: serde_json::Map::new(),
        }),
        files: vec![SnapshotFile {
            path: rel.into(),
            object_key: None,
            digest: digest_bytes(&bytes),
            bytes: bytes.len() as u64,
            rows: 1,
            row_groups: 1,
            source_part: None,
        }],
        total_rows: 1,
        total_bytes: bytes.len() as u64,
        physical_fingerprint: None,
        logical_fingerprint: Fingerprint {
            version: parquet_lab_contract::LOGICAL_ROW_MULTISET_VERSION.into(),
            count: 1,
            xor: "00".repeat(32),
            sum: [0; 4],
            digest: "00".repeat(32),
        },
        disclaimer: None,
    };
    let mp = dir.join("manifest.json");
    std::fs::write(&mp, serde_json::to_vec_pretty(&manifest).unwrap()).unwrap();
    mp
}

fn local_config(dir: &Path, store_dir: &Path) -> std::path::PathBuf {
    let cfg = dir.join("store.toml");
    std::fs::write(
        &cfg,
        format!("kind = \"local\"\nbase_dir = \"{}\"\n", store_dir.display()),
    )
    .unwrap();
    cfg
}

#[tokio::test]
async fn publish_then_verify_round_trips_on_a_local_store() {
    let tmp = tempfile::tempdir().unwrap();
    let mp = write_artifact(tmp.path());
    let store_dir = tmp.path().join("store");
    let cfg_path = local_config(tmp.path(), &store_dir);
    let cfg = StoreConfig::load(&cfg_path).unwrap();
    let receipt = tmp.path().join("store-receipt.json");

    parquet_lab_store::publish::publish(&mp, &cfg, "run-1/snapshot", &receipt)
        .await
        .unwrap();
    assert!(receipt.exists());
    // The object landed under the prefix.
    assert!(
        store_dir
            .join("run-1/snapshot/parquet/part-00000.parquet")
            .exists()
    );

    let report = parquet_lab_store::verify::verify(&receipt, &cfg)
        .await
        .unwrap();
    assert_eq!(report.objects, 1);

    // A receipt cannot serialize a credential.
    let json = std::fs::read_to_string(&receipt).unwrap().to_lowercase();
    for bad in ["access_key", "secret", "session_token"] {
        assert!(!json.contains(bad), "receipt leaked {bad}");
    }
}

#[tokio::test]
async fn verify_detects_a_tampered_object() {
    let tmp = tempfile::tempdir().unwrap();
    let mp = write_artifact(tmp.path());
    let store_dir = tmp.path().join("store");
    let cfg = StoreConfig::load(&local_config(tmp.path(), &store_dir)).unwrap();
    let receipt = tmp.path().join("r.json");
    parquet_lab_store::publish::publish(&mp, &cfg, "p", &receipt)
        .await
        .unwrap();

    // Tamper with the stored object.
    let obj = store_dir.join("p/parquet/part-00000.parquet");
    std::fs::write(&obj, b"different bytes entirely!!").unwrap();
    let err = parquet_lab_store::verify::verify(&receipt, &cfg)
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("SHA-256") || err.to_string().contains("size"),
        "{err}"
    );
}

#[tokio::test]
async fn publish_refuses_bad_prefixes_existing_prefixes_and_overwrite() {
    let tmp = tempfile::tempdir().unwrap();
    let mp = write_artifact(tmp.path());
    let store_dir = tmp.path().join("store");
    let cfg = StoreConfig::load(&local_config(tmp.path(), &store_dir)).unwrap();

    for bad in ["", "/", "../escape", "/abs"] {
        let err = parquet_lab_store::publish::publish(&mp, &cfg, bad, &tmp.path().join("r.json"))
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("prefix"),
            "prefix '{bad}' should be refused: {err}"
        );
    }

    // Publish once, then a second publish to the same prefix is refused (existing objects).
    let r1 = tmp.path().join("r1.json");
    parquet_lab_store::publish::publish(&mp, &cfg, "run", &r1)
        .await
        .unwrap();
    let err = parquet_lab_store::publish::publish(&mp, &cfg, "run", &tmp.path().join("r2.json"))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("already contains"), "{err}");

    // Overwriting an existing receipt is refused.
    let err = parquet_lab_store::publish::publish(&mp, &cfg, "run-b", &r1)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("already exists"), "{err}");
}

#[tokio::test]
async fn publish_refuses_a_mutated_artifact() {
    let tmp = tempfile::tempdir().unwrap();
    let mp = write_artifact(tmp.path());
    // Mutate a file after the manifest recorded its digest.
    std::fs::write(tmp.path().join("parquet/part-00000.parquet"), b"mutated").unwrap();
    let store_dir = tmp.path().join("store");
    let cfg = StoreConfig::load(&local_config(tmp.path(), &store_dir)).unwrap();
    let err = parquet_lab_store::publish::publish(&mp, &cfg, "p", &tmp.path().join("r.json"))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("do not match"), "{err}");
}

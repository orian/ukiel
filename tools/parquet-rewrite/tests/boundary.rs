//! Boundaries: the logical-fingerprint backstop rejects a corrupted rewrite, output is
//! atomic and refuses overwrite, and the crate stays Arrow+Parquet only.

mod common;

use std::collections::HashSet;
use std::process::Command;

#[test]
fn a_fresh_output_directory_is_required_without_replace() {
    let tmp = tempfile::tempdir().unwrap();
    let mp = common::write_snapshot(tmp.path(), &[common::event_batch(40, 1)]);
    let spec = common::write_spec(
        tmp.path(),
        "v.toml",
        "label = \"x\"\nrow_group_rows = 131072\ncompression = \"snappy\"\n",
    );
    let out = tmp.path().join("variant");
    parquet_rewrite::run(&mp, &spec, &out, false).unwrap();
    let err = parquet_rewrite::run(&mp, &spec, &out, false).unwrap_err();
    assert!(err.to_string().contains("--replace"), "{err}");
    parquet_rewrite::run(&mp, &spec, &out, true).unwrap();
}

#[test]
fn a_snapshot_whose_recorded_fingerprint_is_wrong_is_rejected() {
    let tmp = tempfile::tempdir().unwrap();
    let mp = common::write_snapshot(tmp.path(), &[common::event_batch(40, 1)]);
    // Corrupt the manifest's logical fingerprint so the rewrite cannot reproduce it.
    let mut manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&mp).unwrap()).unwrap();
    manifest["logical_fingerprint"]["digest"] = serde_json::json!("00".repeat(32));
    manifest["logical_fingerprint"]["count"] = serde_json::json!(999);
    std::fs::write(&mp, serde_json::to_vec_pretty(&manifest).unwrap()).unwrap();

    let spec = common::write_spec(
        tmp.path(),
        "v.toml",
        "label = \"x\"\nrow_group_rows = 131072\ncompression = \"snappy\"\n",
    );
    let out = tmp.path().join("variant");
    let err = parquet_rewrite::run(&mp, &spec, &out, false).unwrap_err();
    assert!(err.to_string().contains("logical fingerprint"), "{err}");
    // Nothing published; the temp tree was cleaned up.
    assert!(!out.join("manifest.json").exists());
    assert!(!tmp.path().join("variant.tmp-variant").exists());
}

#[test]
fn the_rewriter_depends_on_no_datafusion_or_ukiel_crate() {
    let out = Command::new(env!("CARGO"))
        .args([
            "tree",
            "-p",
            "parquet-rewrite",
            "-e",
            "normal",
            "--prefix",
            "none",
            "--format",
            "{lib}",
            "--manifest-path",
            concat!(env!("CARGO_MANIFEST_DIR"), "/Cargo.toml"),
        ])
        .output()
        .expect("cargo tree");
    let deps: HashSet<String> = String::from_utf8(out.stdout)
        .unwrap()
        .lines()
        .map(|l| l.trim().to_string())
        .flat_map(|l| [l.clone(), l.replace('_', "-")])
        .collect();
    for forbidden in [
        "datafusion",
        "object_store",
        "ukiel-core",
        "ukiel-catalog",
        "tokio",
        "sqlx",
        "parquet-census",
        "parquet-lab-snapshot",
    ] {
        assert!(
            !deps.contains(forbidden),
            "parquet-rewrite must stay Arrow+Parquet only, but reaches {forbidden}"
        );
    }
    assert!(deps.contains("parquet") && deps.contains("arrow"));
}

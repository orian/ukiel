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

// -- Plan 49: causal reconstruct/vary modes ---------------------------------

use parquet_lab_contract::{ReconstructionManifest, VariantDeltaManifest};

/// Write a product snapshot under `tmp/product` so the reconstruction can be a sibling
/// directory (the guard refuses writing into the product's own directory).
fn product_snapshot(tmp: &std::path::Path) -> std::path::PathBuf {
    let product_dir = tmp.join("product");
    std::fs::create_dir_all(&product_dir).unwrap();
    common::write_snapshot(&product_dir, &[common::event_batch(200, 1)])
}

const BASELINE: &str =
    "label = \"reconstruction\"\nrow_group_rows = 131072\ncompression = \"zstd(1)\"\n";

const ZSTD6_DELTA: &str = r#"
version = "ukiel-parquet-variant-delta/v1"
label = "compression-zstd-6"
allowed_changes = ["global.compression"]

[changes.global]
compression = "zstd(6)"
"#;

#[test]
fn product_bytes_cannot_be_rewritten_in_place() {
    let tmp = tempfile::tempdir().unwrap();
    let mp = product_snapshot(tmp.path());
    let baseline = common::write_spec(tmp.path(), "baseline.toml", BASELINE);
    // Output == the product's own directory: refused before any file is opened.
    let product_dir = mp.parent().unwrap().to_path_buf();
    let err = parquet_rewrite::run_reconstruct(&mp, &baseline, &product_dir).unwrap_err();
    assert!(
        err.to_string().contains("immutable control directory"),
        "{err}"
    );
    // A file inside the product directory is equally refused.
    let inside = product_dir.join("reconstruction");
    let err = parquet_rewrite::run_reconstruct(&mp, &baseline, &inside).unwrap_err();
    assert!(err.to_string().contains("immutable"), "{err}");
}

#[test]
fn reconstruction_binds_its_exact_product_parent_and_preserves_the_fingerprint() {
    let tmp = tempfile::tempdir().unwrap();
    let mp = product_snapshot(tmp.path());
    let product_digest = parquet_lab_contract::digest_bytes(&std::fs::read(&mp).unwrap());
    let baseline = common::write_spec(tmp.path(), "baseline.toml", BASELINE);
    let out = tmp.path().join("reconstruction");
    let manifest_path = parquet_rewrite::run_reconstruct(&mp, &baseline, &out).unwrap();
    let reco = ReconstructionManifest::parse(
        &manifest_path.display().to_string(),
        &std::fs::read(&manifest_path).unwrap(),
    )
    .unwrap();
    assert!(reco.check_parent(&product_digest).is_ok());
    assert_eq!(
        reco.resolved_config.fields["global.compression"],
        serde_json::json!("zstd(1)")
    );
    assert_eq!(reco.files[0].output_rows, 200);
}

#[test]
fn a_zstd6_delta_reconstructs_then_varies_only_compression() {
    let tmp = tempfile::tempdir().unwrap();
    let mp = product_snapshot(tmp.path());
    let baseline = common::write_spec(tmp.path(), "baseline.toml", BASELINE);
    let reco_dir = tmp.path().join("reconstruction");
    let reco_manifest = parquet_rewrite::run_reconstruct(&mp, &baseline, &reco_dir).unwrap();

    let delta = common::write_spec(tmp.path(), "zstd6.toml", ZSTD6_DELTA);
    let vary_dir = tmp.path().join("zstd-6");
    let variant_path = parquet_rewrite::run_vary(&reco_manifest, &delta, &vary_dir).unwrap();
    let variant = VariantDeltaManifest::parse(
        &variant_path.display().to_string(),
        &std::fs::read(&variant_path).unwrap(),
    )
    .unwrap();
    assert_eq!(
        variant.allowed_changes,
        vec!["global.compression".to_string()]
    );
    assert_eq!(
        variant.resolved_config.fields["global.compression"],
        serde_json::json!("zstd(6)")
    );
    // Row count and membership preserved.
    assert_eq!(variant.files[0].output_rows, 200);

    // The footer really applied ZSTD.
    let out = vary_dir.join(&variant.files[0].output.path);
    let meta = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
        bytes::Bytes::from(std::fs::read(out).unwrap()),
    )
    .unwrap()
    .metadata()
    .clone();
    for c in meta.row_group(0).columns() {
        assert!(format!("{:?}", c.compression()).contains("ZSTD"));
    }
}

#[test]
fn a_delta_changing_a_second_axis_is_refused() {
    let tmp = tempfile::tempdir().unwrap();
    let mp = product_snapshot(tmp.path());
    let baseline = common::write_spec(tmp.path(), "baseline.toml", BASELINE);
    let reco_dir = tmp.path().join("reconstruction");
    let reco_manifest = parquet_rewrite::run_reconstruct(&mp, &baseline, &reco_dir).unwrap();

    // The delta changes row_group size but only allows compression: refused.
    let bad = r#"
version = "ukiel-parquet-variant-delta/v1"
label = "sneaky"
allowed_changes = ["global.compression"]

[changes.global]
compression = "zstd(6)"
row_group_rows = 64
"#;
    let delta = common::write_spec(tmp.path(), "bad.toml", bad);
    let err =
        parquet_rewrite::run_vary(&reco_manifest, &delta, &tmp.path().join("bad")).unwrap_err();
    assert!(
        err.to_string().contains("allowed_changes") || err.to_string().contains("allowlist"),
        "{err}"
    );
    assert!(!tmp.path().join("bad").join("manifest.json").exists());
}

#[test]
fn an_unknown_allowlist_path_is_refused() {
    let tmp = tempfile::tempdir().unwrap();
    let mp = product_snapshot(tmp.path());
    let baseline = common::write_spec(tmp.path(), "baseline.toml", BASELINE);
    let reco_dir = tmp.path().join("reconstruction");
    let reco_manifest = parquet_rewrite::run_reconstruct(&mp, &baseline, &reco_dir).unwrap();

    let bad = r#"
version = "ukiel-parquet-variant-delta/v1"
label = "unknown-path"
allowed_changes = ["global.magic_unicorn"]

[changes.global]
compression = "zstd(6)"
"#;
    let delta = common::write_spec(tmp.path(), "bad.toml", bad);
    let err =
        parquet_rewrite::run_vary(&reco_manifest, &delta, &tmp.path().join("bad2")).unwrap_err();
    // The change (global.compression) is not in the (unknown) allowlist, so it fails —
    // either as an allowlist violation or an unknown allowlist path.
    assert!(err.to_string().contains("allow"), "{err}");
}

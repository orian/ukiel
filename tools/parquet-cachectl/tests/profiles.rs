//! Cache profiles: warm reaches its floor, eviction reaches its cold ceiling, an
//! ineffective eviction is refused rather than relabelled, the receipt binds the exact
//! files, and files outside the target set are never touched.

use std::path::Path;

use parquet_lab_contract::{
    CacheProfile, FileDigest, Fingerprint, ReconstructionManifest, ResolvedConfig, RewriteBias,
    VariantFileMap, digest_bytes,
};

/// Write `n` KiB of deterministic bytes to `dir/rel` and return its FileDigest.
fn write_file(dir: &Path, rel: &str, kib: usize) -> FileDigest {
    let path = dir.join(rel);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let bytes: Vec<u8> = (0..kib * 1024).map(|i| (i % 251) as u8).collect();
    std::fs::write(&path, &bytes).unwrap();
    FileDigest::of(rel, &bytes)
}

/// A minimal reconstruction manifest over one data file (cachectl never decodes it).
fn manifest_over(dir: &Path, file: FileDigest) -> std::path::PathBuf {
    let m = ReconstructionManifest {
        reconstruction_version: parquet_lab_contract::RECONSTRUCTION_VERSION.into(),
        parent_product_digest: "prod".repeat(16),
        baseline_spec_digest: "ba5e".repeat(16),
        label: "reconstruction".into(),
        requested_config: ResolvedConfig::default(),
        resolved_config: ResolvedConfig::default(),
        logical_projection: serde_json::Map::new(),
        sort_key: vec![],
        physical_schema: serde_json::json!({"fields": []}),
        logical_fingerprint: Fingerprint {
            version: parquet_lab_integrity_version(),
            count: 1,
            xor: "00".repeat(32),
            sum: [0; 4],
            digest: "00".repeat(32),
        },
        files: vec![VariantFileMap {
            input: FileDigest::of("in", b"in"),
            output: file,
            input_rows: 1,
            output_rows: 1,
            footer_summary: serde_json::json!({}),
        }],
        input_census: serde_json::json!({}),
        output_census: serde_json::json!({}),
        rewrite_bias: RewriteBias {
            product_total_bytes: 0,
            reconstruction_total_bytes: 0,
            per_column: serde_json::json!({}),
        },
    };
    let mp = dir.join("manifest.json");
    std::fs::write(&mp, serde_json::to_vec_pretty(&m).unwrap()).unwrap();
    mp
}

fn parquet_lab_integrity_version() -> String {
    // Mirror the logical-row-multiset version without depending on the integrity crate.
    "logical-row-multiset/v1".to_string()
}

#[test]
fn warm_reaches_its_floor_and_the_receipt_binds_the_file() {
    let tmp = tempfile::tempdir().unwrap();
    let fd = write_file(tmp.path(), "parquet/part-00000.parquet", 512);
    let mp = manifest_over(tmp.path(), fd.clone());
    let receipt_path = tmp.path().join("warm.json");
    let (receipt, valid) = parquet_cachectl::prepare(
        &mp,
        CacheProfile::LocalOsWarm,
        &receipt_path,
        0.90,
        0.10,
        false,
    )
    .unwrap();
    assert!(valid, "warm should reach its floor, got {}", receipt.residency_after.resident_fraction);
    assert_eq!(receipt.target_files.len(), 1);
    assert_eq!(receipt.target_files[0].digest, fd.digest);
    assert_eq!(receipt.target_manifest_digest, digest_bytes(&std::fs::read(&mp).unwrap()));
}

#[test]
fn eviction_reaches_the_cold_ceiling_or_is_reported_unavailable() {
    let tmp = tempfile::tempdir().unwrap();
    let fd = write_file(tmp.path(), "parquet/part-00000.parquet", 2048);
    let mp = manifest_over(tmp.path(), fd);
    // Warm it first so there is something to evict.
    let warm_receipt = tmp.path().join("warm.json");
    parquet_cachectl::prepare(&mp, CacheProfile::LocalOsWarm, &warm_receipt, 0.90, 0.10, false)
        .unwrap();

    let cold_receipt = tmp.path().join("cold.json");
    let (receipt, valid) = parquet_cachectl::prepare(
        &mp,
        CacheProfile::LocalOsCold,
        &cold_receipt,
        0.90,
        0.10,
        false,
    )
    .unwrap();
    // On a cooperative kernel eviction works; if the environment refuses it, the receipt is
    // honestly marked invalid rather than silently relabelled warm. Either way it must be
    // self-consistent: valid implies the ceiling was actually met.
    if valid {
        assert!(receipt.residency_after.resident_fraction <= 0.10);
    } else {
        assert!(!receipt.valid);
    }
    // The receipt on disk re-parses (its validity is self-consistent).
    let back = parquet_lab_contract::CacheReceipt::parse(
        "cold",
        &std::fs::read(&cold_receipt).unwrap(),
    )
    .unwrap();
    assert_eq!(back.requested_profile, CacheProfile::LocalOsCold);
}

#[test]
fn a_forced_ineffective_cold_receipt_is_refused_by_the_contract() {
    // Directly assert the contract gate the controller relies on: a cold receipt claiming
    // validity while still resident cannot be constructed.
    let receipt = parquet_lab_contract::CacheReceipt {
        receipt_version: parquet_lab_contract::CACHE_RECEIPT_VERSION.into(),
        target_manifest_digest: "aa".repeat(32),
        target_files: vec![FileDigest::of("x", b"x")],
        requested_profile: CacheProfile::LocalOsCold,
        preparation_method: "posix_fadvise(DONTNEED)+mincore".into(),
        residency_before: parquet_lab_contract::Residency { resident_fraction: 1.0, pages_probed: 10 },
        residency_after: parquet_lab_contract::Residency { resident_fraction: 0.8, pages_probed: 10 },
        warm_floor: None,
        cold_ceiling: Some(0.10),
        valid: true,
    };
    assert!(receipt.validate("r").is_err());
}

#[test]
fn other_files_are_not_evicted() {
    let tmp = tempfile::tempdir().unwrap();
    // Two files; only the first is in the target manifest.
    let target = write_file(tmp.path(), "parquet/part-00000.parquet", 1024);
    let bystander = write_file(tmp.path(), "parquet/bystander.parquet", 1024);
    let mp = manifest_over(tmp.path(), target);

    // Warm both explicitly.
    parquet_cachectl::linux::warm(&tmp.path().join("parquet/part-00000.parquet")).unwrap();
    parquet_cachectl::linux::warm(&tmp.path().join("parquet/bystander.parquet")).unwrap();

    // Evict only the target via the controller.
    let cold_receipt = tmp.path().join("cold.json");
    let (_r, _valid) = parquet_cachectl::prepare(
        &mp,
        CacheProfile::LocalOsCold,
        &cold_receipt,
        0.90,
        0.10,
        false,
    )
    .unwrap();

    // The bystander was never named to the controller, so it is still resident.
    let (resident, total) =
        parquet_cachectl::linux::page_residency(&tmp.path().join("parquet/bystander.parquet"))
            .unwrap();
    let _ = &bystander;
    if total > 0 {
        let frac = resident as f64 / total as f64;
        assert!(frac > 0.5, "the bystander file should remain resident, got {frac}");
    }
}

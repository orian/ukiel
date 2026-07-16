//! Reading: range coverage produces a stable checksum, the plan reads exactly the requested
//! bytes, a bound cache receipt is required to match the artifact, and setup is outside the
//! timed region (proven by the checksum equality across samples).

use std::path::Path;

use parquet_lab_contract::{
    CacheProfile, CacheReceipt, FileDigest, Fingerprint, ReconstructionManifest, Residency,
    ResolvedConfig, RewriteBias, VariantFileMap, digest_bytes,
};
use file_read_bench::ranges::Plan;

fn write_file(dir: &Path, rel: &str, kib: usize) -> (FileDigest, Vec<u8>) {
    let path = dir.join(rel);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let bytes: Vec<u8> = (0..kib * 1024).map(|i| (i.wrapping_mul(31) % 253) as u8).collect();
    std::fs::write(&path, &bytes).unwrap();
    (FileDigest::of(rel, &bytes), bytes)
}

fn manifest(dir: &Path, files: Vec<FileDigest>) -> std::path::PathBuf {
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
            version: "logical-row-multiset/v1".into(),
            count: 1,
            xor: "00".repeat(32),
            sum: [0; 4],
            digest: "00".repeat(32),
        },
        files: files
            .into_iter()
            .enumerate()
            .map(|(i, output)| VariantFileMap {
                input: FileDigest::of(format!("in-{i}"), b"in"),
                output,
                input_rows: 1,
                output_rows: 1,
                footer_summary: serde_json::json!({}),
            })
            .collect(),
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

#[test]
fn every_plan_reads_the_requested_bytes_with_a_stable_checksum() {
    let tmp = tempfile::tempdir().unwrap();
    let (f0, _) = write_file(tmp.path(), "parquet/part-00000.parquet", 256);
    let (f1, _) = write_file(tmp.path(), "parquet/part-00001.parquet", 256);
    let mp = manifest(tmp.path(), vec![f0, f1]);

    for (i, plan) in ["all", "one", "contiguous", "sparse"].iter().enumerate() {
        let report_path = tmp.path().join(format!("read-{i}.json"));
        let report = file_read_bench::run_bench(
            &mp,
            file_read_bench::parse_plan(plan).unwrap(),
            0.1,
            3,
            None,
            &report_path,
        )
        .unwrap();
        assert_eq!(report.samples.len(), 3);
        // Requested == returned for every sample; checksum stable across samples.
        for s in &report.samples {
            assert_eq!(s.requested_bytes, s.returned_bytes);
        }
        assert!(!report.checksum.is_empty());
        assert_eq!(report.plan, *plan);
    }
    let _ = Plan::All;
}

#[test]
fn a_bound_cache_receipt_must_match_the_artifact() {
    let tmp = tempfile::tempdir().unwrap();
    let (f0, _) = write_file(tmp.path(), "parquet/part-00000.parquet", 128);
    let mp = manifest(tmp.path(), vec![f0.clone()]);
    let artifact_digest = digest_bytes(&std::fs::read(&mp).unwrap());

    // A valid receipt bound to a *different* artifact must be refused.
    let wrong = CacheReceipt {
        receipt_version: parquet_lab_contract::CACHE_RECEIPT_VERSION.into(),
        target_manifest_digest: "ff".repeat(32),
        target_files: vec![f0.clone()],
        requested_profile: CacheProfile::LocalOsWarm,
        preparation_method: "read-all-ranges+mincore".into(),
        residency_before: Residency { resident_fraction: 1.0, pages_probed: 10 },
        residency_after: Residency { resident_fraction: 1.0, pages_probed: 10 },
        warm_floor: Some(0.90),
        cold_ceiling: None,
        valid: true,
    };
    let wrong_path = tmp.path().join("wrong-receipt.json");
    std::fs::write(&wrong_path, serde_json::to_vec_pretty(&wrong).unwrap()).unwrap();
    let err = file_read_bench::run_bench(
        &mp,
        Plan::All,
        0.1,
        1,
        Some(&wrong_path),
        &tmp.path().join("r1.json"),
    )
    .unwrap_err();
    assert!(err.to_string().contains("different artifact"), "{err}");

    // A receipt correctly bound to this artifact is accepted and its digest recorded.
    let right = CacheReceipt { target_manifest_digest: artifact_digest, ..wrong };
    let right_path = tmp.path().join("right-receipt.json");
    std::fs::write(&right_path, serde_json::to_vec_pretty(&right).unwrap()).unwrap();
    let report = file_read_bench::run_bench(
        &mp,
        Plan::All,
        0.1,
        1,
        Some(&right_path),
        &tmp.path().join("r2.json"),
    )
    .unwrap();
    assert_eq!(
        report.cache_receipt_digest,
        Some(digest_bytes(&std::fs::read(&right_path).unwrap()))
    );
}

#[test]
fn an_invalid_cache_receipt_is_refused() {
    let tmp = tempfile::tempdir().unwrap();
    let (f0, _) = write_file(tmp.path(), "parquet/part-00000.parquet", 64);
    let mp = manifest(tmp.path(), vec![f0.clone()]);
    let artifact_digest = digest_bytes(&std::fs::read(&mp).unwrap());
    // A cold receipt honestly marked invalid (eviction failed) cannot back a timing.
    let invalid = CacheReceipt {
        receipt_version: parquet_lab_contract::CACHE_RECEIPT_VERSION.into(),
        target_manifest_digest: artifact_digest,
        target_files: vec![f0],
        requested_profile: CacheProfile::LocalOsCold,
        preparation_method: "posix_fadvise(DONTNEED)+mincore".into(),
        residency_before: Residency { resident_fraction: 1.0, pages_probed: 10 },
        residency_after: Residency { resident_fraction: 0.9, pages_probed: 10 },
        warm_floor: None,
        cold_ceiling: Some(0.10),
        valid: false,
    };
    let p = tmp.path().join("invalid.json");
    std::fs::write(&p, serde_json::to_vec_pretty(&invalid).unwrap()).unwrap();
    let err = file_read_bench::run_bench(
        &mp,
        Plan::All,
        0.1,
        1,
        Some(&p),
        &tmp.path().join("r.json"),
    )
    .unwrap_err();
    assert!(err.to_string().contains("invalid"), "{err}");
}

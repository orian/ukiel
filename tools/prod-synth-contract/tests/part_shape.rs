//! The plan-46 contracts: serde round-trips, version gating, and disclaimer refusal.

use prod_synth_contract::{
    ExpectedCompactorConfig, FileDigest, L0File, L0Manifest, L0StagingConfig, PartShapeReceipt,
    PlacementSpec, RowFingerprint, SYNTHETIC_DISCLAIMER, TableSpec,
};

fn table() -> TableSpec {
    TableSpec {
        packing_key: "team_id".into(),
        sort_key: vec!["team_id".into(), "timestamp".into()],
        ts_column: "timestamp".into(),
        schema: serde_json::json!({"fields": [{"name": "team_id", "type": "int64"}]}),
    }
}

fn fingerprint() -> RowFingerprint {
    RowFingerprint {
        version: "row-multiset/v1".into(),
        count: 100,
        xor: "aa".repeat(32),
        sum: [1, 2, 3, 4],
        digest: "bb".repeat(32),
    }
}

fn l0_manifest() -> L0Manifest {
    L0Manifest {
        manifest_version: prod_synth_contract::PROD_SYNTH_L0_VERSION.into(),
        disclaimer: SYNTHETIC_DISCLAIMER.into(),
        source_manifest: FileDigest::of("manifest.json", b"source"),
        source_topology: FileDigest::of("topology.json", b"topo"),
        staging: L0StagingConfig {
            flush_rows: 100_000,
            day_policy: "utc-day-from-timestamp".into(),
            seed: 0,
        },
        table: table(),
        input_rows: 100,
        output_rows: 100,
        fingerprint: fingerprint(),
        files: vec![L0File {
            path: "parquet/2026-07-01/flush-00000.parquet".into(),
            day: "2026-07-01".into(),
            flush_index: 0,
            rows: 100,
            bytes: 4096,
            digest: "cc".repeat(32),
            key_min: 1,
            key_max: 900,
            ts_min: 1_782_864_000_000,
            ts_max: 1_782_900_000_000,
        }],
    }
}

fn receipt() -> PartShapeReceipt {
    PartShapeReceipt {
        receipt_version: prod_synth_contract::PART_SHAPE_RECEIPT_VERSION.into(),
        disclaimer: SYNTHETIC_DISCLAIMER.into(),
        source_manifest: FileDigest::of("manifest.json", b"source"),
        l0_manifest: FileDigest::of("l0-manifest.json", b"l0"),
        label: "smoke-packed".into(),
        hypertable: "prod_synth_events_smoke_packed".into(),
        hypertable_id: 1,
        placement: PlacementSpec::Packed,
        input_parts: 12,
        input_rows: 100,
        input_bytes: 40960,
        packing_key: "team_id".into(),
        sort_key: vec!["team_id".into(), "timestamp".into()],
        partition_marker_digest: "dd".repeat(32),
        compactor: ExpectedCompactorConfig {
            l0_fanout: 4,
            fanout: 10,
            finalize_after_secs: 0,
            finalize_poll_interval_ms: 100,
            lease_ttl_secs: 60,
            lease_renew_interval_secs: 20,
            candidate_limit: 64,
        },
        fingerprint: fingerprint(),
    }
}

#[test]
fn l0_manifest_round_trips() {
    let m = l0_manifest();
    let bytes = serde_json::to_vec_pretty(&m).unwrap();
    let back = L0Manifest::parse("l0-manifest.json", &bytes).unwrap();
    assert_eq!(m, back);
    assert_eq!(
        back.input_rows, back.output_rows,
        "staging regroups, never adds or drops"
    );
}

#[test]
fn receipt_round_trips() {
    let r = receipt();
    let bytes = serde_json::to_vec_pretty(&r).unwrap();
    let back = PartShapeReceipt::parse("receipt.json", &bytes).unwrap();
    assert_eq!(r, back);
}

#[test]
fn an_unknown_l0_version_fails_closed() {
    let mut v: serde_json::Value = serde_json::to_value(l0_manifest()).unwrap();
    v["manifest_version"] = serde_json::json!("ukiel-prod-synth-l0/v99");
    let e = L0Manifest::parse("m", &serde_json::to_vec(&v).unwrap()).unwrap_err();
    assert!(
        matches!(e, prod_synth_contract::ContractError::Version { .. }),
        "{e}"
    );
}

#[test]
fn an_unknown_receipt_version_fails_closed() {
    let mut v: serde_json::Value = serde_json::to_value(receipt()).unwrap();
    v["receipt_version"] = serde_json::json!("ukiel-prod-part-shape/v99");
    let e = PartShapeReceipt::parse("r", &serde_json::to_vec(&v).unwrap()).unwrap_err();
    assert!(
        matches!(e, prod_synth_contract::ContractError::Version { .. }),
        "{e}"
    );
}

#[test]
fn an_artifact_that_will_not_admit_to_being_synthetic_is_refused() {
    let mut v: serde_json::Value = serde_json::to_value(l0_manifest()).unwrap();
    v["disclaimer"] = serde_json::json!("real production data");
    let e = L0Manifest::parse("m", &serde_json::to_vec(&v).unwrap()).unwrap_err();
    assert!(
        matches!(
            e,
            prod_synth_contract::ContractError::MissingDisclaimer { .. }
        ),
        "{e}"
    );

    let mut v: serde_json::Value = serde_json::to_value(receipt()).unwrap();
    v["disclaimer"] = serde_json::json!("real production data");
    let e = PartShapeReceipt::parse("r", &serde_json::to_vec(&v).unwrap()).unwrap_err();
    assert!(
        matches!(
            e,
            prod_synth_contract::ContractError::MissingDisclaimer { .. }
        ),
        "{e}"
    );
}

/// A digest mismatch on the referenced source/L0 manifests is catchable through the
/// shared `FileDigest::verify`, so a receipt cannot point at an artifact that has
/// changed under it.
#[test]
fn a_referenced_manifest_digest_mismatch_is_detectable() {
    let r = receipt();
    assert!(r.source_manifest.verify("manifest.json", b"source").is_ok());
    let e = r
        .source_manifest
        .verify("manifest.json", b"tampered")
        .unwrap_err();
    assert!(
        matches!(e, prod_synth_contract::ContractError::Digest { .. }),
        "{e}"
    );
}

/// Fingerprints agree iff they describe the same multiset; a version mismatch is a
/// disagreement, never a swallowed error.
#[test]
fn fingerprints_agree_only_within_a_version() {
    let a = fingerprint();
    assert!(a.agrees_with(&a));

    let mut changed = a.clone();
    changed.count = 101;
    assert!(
        !a.agrees_with(&changed),
        "a different count is a different multiset"
    );

    let mut other_version = a.clone();
    other_version.version = "row-multiset/v2".into();
    assert!(
        !a.agrees_with(&other_version),
        "fingerprints under different encodings are not comparable, so they do not agree"
    );
}

/// Placement maps to exactly the `target_file_bytes` the catalog stores.
#[test]
fn placement_maps_to_target_file_bytes() {
    assert_eq!(PlacementSpec::Packed.target_file_bytes(), None);
    assert_eq!(PlacementSpec::Separated.target_file_bytes(), Some(0));
    assert_eq!(
        PlacementSpec::SizeTargeted(268_435_456).target_file_bytes(),
        Some(268_435_456)
    );
    assert_eq!(PlacementSpec::SizeTargeted(1).as_str(), "size-targeted");
}

/// The partition marker names the artifact and the day, and the identity half is stable
/// across days — every part of one fixture shares it.
#[test]
fn the_partition_marker_identifies_the_artifact() {
    let m1 = PartShapeReceipt::partition_marker("abc123", "2026-07-01");
    let m2 = PartShapeReceipt::partition_marker("abc123", "2026-07-02");
    assert_eq!(
        m1["l0_manifest"], m2["l0_manifest"],
        "same artifact, different day"
    );
    assert_ne!(m1["utc_day"], m2["utc_day"]);
    assert_eq!(
        PartShapeReceipt::marker_digest("abc123"),
        PartShapeReceipt::marker_digest("abc123"),
        "the identity digest is stable"
    );
}

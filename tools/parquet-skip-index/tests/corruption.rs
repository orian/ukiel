//! Binding and corruption: a sidecar that is not provably bound to the exact variant and
//! files it was built for must never be trusted to skip, and a corrupt payload decodes to
//! nothing (keep), never a wrong skip.

use parquet_lab_contract::{FileDigest, IndexKind, IndexedColumn, RowGroupIndex, SkipManifest};
use parquet_skip_index::prefix_set::PrefixSet;
use parquet_skip_index::value_set::ValueSet;
use parquet_skip_index::zone_map::ZoneMap;
use parquet_skip_index::{Value, payload_digest};

fn sidecar(variant_digest: &str, files: Vec<FileDigest>) -> SkipManifest {
    SkipManifest {
        manifest_version: parquet_lab_contract::SKIP_MANIFEST_VERSION.into(),
        parent_variant_digest: variant_digest.into(),
        file_digests: files,
        payload_path: "payload.bin".into(),
        columns: vec![IndexedColumn {
            column: "team_id".into(),
            kind: IndexKind::ZoneMap,
            kind_version: "zone_map/v1".into(),
            file: "out/p0.parquet".into(),
            row_groups: vec![RowGroupIndex {
                row_group: 0,
                kind: IndexKind::ZoneMap,
                parameters: serde_json::json!({}),
                payload_offset: 0,
                payload_length: 8,
                payload_digest: "00".repeat(32),
            }],
        }],
        build_wall_ms: 1,
        payload_bytes: 8,
    }
}

#[test]
fn a_sidecar_is_bound_only_to_its_exact_variant_and_files() {
    let files = vec![FileDigest::of("out/p0.parquet", b"file-bytes")];
    let s = sidecar("variant-digest-A", files.clone());
    assert!(s.is_bound_to("variant-digest-A", &files));
    // Wrong variant digest.
    assert!(!s.is_bound_to("variant-digest-B", &files));
    // A file changed under it.
    let tampered = vec![FileDigest::of("out/p0.parquet", b"tampered-bytes")];
    assert!(!s.is_bound_to("variant-digest-A", &tampered));
    // A missing file.
    assert!(!s.is_bound_to("variant-digest-A", &[]));
}

#[test]
fn a_corrupt_payload_decodes_to_nothing() {
    // Truncated and garbage bytes must all decode to None (evaluation then abstains).
    for bytes in [
        &b""[..],
        &b"Z"[..],
        &b"Zx"[..],
        &b"garbage"[..],
        &[b'Z', 1, 1][..],
    ] {
        assert!(
            ZoneMap::decode(bytes).is_none(),
            "zone_map decoded corrupt {bytes:?}"
        );
    }
    assert!(
        ValueSet::decode(b"V\x05\x00\x00\x00").is_none(),
        "value_set truncated"
    );
    assert!(PrefixSet::decode(b"Pnope").is_none(), "prefix_set garbage");
}

#[test]
fn a_valid_payload_round_trips_and_its_digest_detects_tampering() {
    let values = vec![Some(Value::Int(3)), Some(Value::Int(9)), None];
    let zone = ZoneMap::build(&values);
    let encoded = zone.encode();
    assert_eq!(ZoneMap::decode(&encoded).unwrap(), zone);

    let digest = payload_digest(&encoded);
    let mut tampered = encoded.clone();
    *tampered.last_mut().unwrap() ^= 0xff;
    assert_ne!(
        payload_digest(&tampered),
        digest,
        "a flipped byte changes the payload digest"
    );

    let vset = ValueSet::build(&values, 4096).unwrap();
    assert_eq!(ValueSet::decode(&vset.encode()).unwrap(), vset);
    let strs = vec![Some(Value::Str(b"alpha".to_vec()))];
    let pset = PrefixSet::build(&strs, 3, 4096).unwrap();
    assert_eq!(PrefixSet::decode(&pset.encode()).unwrap(), pset);
}

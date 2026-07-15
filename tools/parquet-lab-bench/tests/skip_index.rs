//! The bench prices a bound skip sidecar and refuses an unbound one — it never credits a
//! sidecar that is not provably tied to the exact variant and files it is measuring.

use parquet_lab_contract::{
    FileDigest, IndexKind, IndexedColumn, RowGroupIndex, SKIP_MANIFEST_VERSION, SkipManifest,
};

fn write_sidecar(
    dir: &std::path::Path,
    variant_digest: &str,
    files: Vec<FileDigest>,
) -> std::path::PathBuf {
    std::fs::write(dir.join("payload.bin"), b"\x00\x00\x00\x00").unwrap();
    let m = SkipManifest {
        manifest_version: SKIP_MANIFEST_VERSION.into(),
        parent_variant_digest: variant_digest.into(),
        file_digests: files,
        payload_path: "payload.bin".into(),
        columns: vec![IndexedColumn {
            column: "team_id".into(),
            kind: IndexKind::ValueSet,
            kind_version: "value_set/v1".into(),
            file: "out/p0.parquet".into(),
            row_groups: vec![RowGroupIndex {
                row_group: 0,
                kind: IndexKind::ValueSet,
                parameters: serde_json::json!({}),
                payload_offset: 0,
                payload_length: 4,
                payload_digest: parquet_lab_contract::digest_bytes(b"\x00\x00\x00\x00"),
            }],
        }],
        build_wall_ms: 7,
        payload_bytes: 4,
    };
    let p = dir.join("skip.json");
    std::fs::write(&p, serde_json::to_vec_pretty(&m).unwrap()).unwrap();
    p
}

#[test]
fn a_bound_sidecar_is_priced_and_an_unbound_one_is_refused() {
    let tmp = tempfile::tempdir().unwrap();
    let files = vec![FileDigest::of("out/p0.parquet", b"variant-file-bytes")];
    let variant_digest = "variant-digest-1234";
    let sidecar = write_sidecar(tmp.path(), variant_digest, files.clone());

    // Bound: priced.
    let cost = parquet_lab_bench::skip::load_and_bind(&sidecar, variant_digest, &files).unwrap();
    assert_eq!(cost.payload_bytes, 4);
    assert_eq!(cost.build_wall_ms, 7);
    assert_eq!(cost.indexed_columns, 1);
    assert_eq!(cost.indexed_row_groups, 1);
    assert_eq!(cost.columns[0].column, "team_id");

    // Wrong variant digest: refused.
    let err =
        parquet_lab_bench::skip::load_and_bind(&sidecar, "other-variant", &files).unwrap_err();
    assert!(err.to_string().contains("not bound"), "{err}");

    // A file changed under it: refused.
    let tampered = vec![FileDigest::of("out/p0.parquet", b"changed-bytes")];
    let err =
        parquet_lab_bench::skip::load_and_bind(&sidecar, variant_digest, &tampered).unwrap_err();
    assert!(err.to_string().contains("not bound"), "{err}");
}

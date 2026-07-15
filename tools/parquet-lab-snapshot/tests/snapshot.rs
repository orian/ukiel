//! `from-files` and `verify`, fully offline: byte-identical copies, a deterministic
//! manifest, and round-trip verification.

mod common;

use parquet_lab_contract::{SnapshotManifest, SourceKind};

#[test]
fn from_files_freezes_byte_identical_copies_and_a_verifiable_manifest() {
    let tmp = tempfile::tempdir().unwrap();
    let src_dir = tmp.path().join("src");
    let a = src_dir.join("a.parquet");
    let b = src_dir.join("b.parquet");
    let a_len = common::write_event_parquet(&a, &[1, 1, 2, 5, 5, 9]);
    let b_len = common::write_event_parquet(&b, &[10, 10, 42, 900]);
    let (schema, files_from, logical) = common::write_inputs(tmp.path(), &[a.clone(), b.clone()]);

    let out = tmp.path().join("snap");
    let manifest_path = parquet_lab_snapshot::from_files::freeze(
        &schema,
        &files_from,
        &logical,
        &out,
        "test".into(),
        false,
    )
    .unwrap();

    let mb = std::fs::read(&manifest_path).unwrap();
    let manifest = SnapshotManifest::parse(&manifest_path.display().to_string(), &mb).unwrap();
    assert_eq!(manifest.source_kind, SourceKind::ExplicitFiles);
    assert_eq!(manifest.files.len(), 2);
    assert_eq!(manifest.total_rows, 10);
    assert_eq!(manifest.total_bytes, a_len + b_len);
    assert!(
        manifest.physical_fingerprint.is_none(),
        "no receipt to compare"
    );
    assert_eq!(
        manifest.logical_fingerprint.version,
        parquet_lab_contract::LOGICAL_ROW_MULTISET_VERSION
    );
    assert!(
        manifest.disclaimer.is_none(),
        "explicit dataset is not synthetic"
    );

    // The frozen bytes are identical to the sources.
    let frozen_a = std::fs::read(out.join(&manifest.files[0].path)).unwrap();
    assert_eq!(frozen_a, std::fs::read(&a).unwrap());

    // Verification passes on the fresh snapshot.
    let report = parquet_lab_snapshot::verify(&manifest_path).unwrap();
    assert_eq!(report.files, 2);
    assert_eq!(report.rows, 10);
}

#[test]
fn from_files_is_deterministic() {
    let tmp = tempfile::tempdir().unwrap();
    let a = tmp.path().join("src/a.parquet");
    common::write_event_parquet(&a, &[1, 2, 3]);
    let (schema, files_from, logical) = common::write_inputs(tmp.path(), std::slice::from_ref(&a));

    let run = |out: &std::path::Path| {
        let mp = parquet_lab_snapshot::from_files::freeze(
            &schema,
            &files_from,
            &logical,
            out,
            "test".into(),
            false,
        )
        .unwrap();
        std::fs::read_to_string(mp).unwrap()
    };
    let one = run(&tmp.path().join("s1"));
    let two = run(&tmp.path().join("s2"));
    assert_eq!(
        one, two,
        "same inputs and command produce an identical manifest"
    );
}

#[test]
fn a_fresh_output_directory_is_required_without_replace() {
    let tmp = tempfile::tempdir().unwrap();
    let a = tmp.path().join("src/a.parquet");
    common::write_event_parquet(&a, &[1, 2, 3]);
    let (schema, files_from, logical) = common::write_inputs(tmp.path(), &[a]);
    let out = tmp.path().join("snap");

    parquet_lab_snapshot::from_files::freeze(
        &schema,
        &files_from,
        &logical,
        &out,
        "t".into(),
        false,
    )
    .unwrap();
    // Second freeze into the same directory without --replace fails.
    let err = parquet_lab_snapshot::from_files::freeze(
        &schema,
        &files_from,
        &logical,
        &out,
        "t".into(),
        false,
    )
    .unwrap_err();
    assert!(err.to_string().contains("--replace"), "{err}");
    // With --replace it succeeds.
    parquet_lab_snapshot::from_files::freeze(
        &schema,
        &files_from,
        &logical,
        &out,
        "t".into(),
        true,
    )
    .unwrap();
}

//! Refusals: a snapshot is a chosen set of bytes, so anything that would let an altered,
//! added, missing, duplicated, or path-escaping file through must fail — offline, with no
//! service involved.

mod common;

use parquet_lab_contract::SnapshotManifest;

fn freeze_one(tmp: &std::path::Path) -> (std::path::PathBuf, std::path::PathBuf) {
    let a = tmp.join("src/a.parquet");
    common::write_event_parquet(&a, &[1, 2, 3, 4]);
    let (schema, files_from, logical) = common::write_inputs(tmp, &[a]);
    let out = tmp.join("snap");
    let mp = parquet_lab_snapshot::from_files::freeze(
        &schema,
        &files_from,
        &logical,
        &out,
        "t".into(),
        false,
    )
    .unwrap();
    (out, mp)
}

#[test]
fn a_duplicated_file_in_the_list_is_refused() {
    let tmp = tempfile::tempdir().unwrap();
    let a = tmp.path().join("src/a.parquet");
    common::write_event_parquet(&a, &[1, 2, 3]);
    let (schema, files_from, logical) = common::write_inputs(tmp.path(), &[a.clone(), a.clone()]);
    let err = parquet_lab_snapshot::from_files::freeze(
        &schema,
        &files_from,
        &logical,
        &tmp.path().join("snap"),
        "t".into(),
        false,
    )
    .unwrap_err();
    assert!(err.to_string().contains("more than once"), "{err}");
}

#[test]
fn a_missing_source_file_fails() {
    let tmp = tempfile::tempdir().unwrap();
    let a = tmp.path().join("src/a.parquet");
    common::write_event_parquet(&a, &[1, 2, 3]);
    let (schema, files_from, logical) = common::write_inputs(tmp.path(), std::slice::from_ref(&a));
    std::fs::remove_file(&a).unwrap();
    let err = parquet_lab_snapshot::from_files::freeze(
        &schema,
        &files_from,
        &logical,
        &tmp.path().join("snap"),
        "t".into(),
        false,
    )
    .unwrap_err();
    assert!(err.to_string().contains("reading"), "{err}");
}

#[test]
fn verify_detects_an_altered_file() {
    let tmp = tempfile::tempdir().unwrap();
    let (out, mp) = freeze_one(tmp.path());
    let manifest =
        SnapshotManifest::parse(&mp.display().to_string(), &std::fs::read(&mp).unwrap()).unwrap();
    // Corrupt a frozen file on disk.
    let victim = out.join(&manifest.files[0].path);
    let mut bytes = std::fs::read(&victim).unwrap();
    *bytes.last_mut().unwrap() ^= 0xff;
    std::fs::write(&victim, &bytes).unwrap();

    let err = parquet_lab_snapshot::verify(&mp).unwrap_err();
    assert!(err.to_string().contains("digest mismatch"), "{err}");
}

#[test]
fn verify_detects_an_alteration_that_also_rewrote_the_digest() {
    let tmp = tempfile::tempdir().unwrap();
    let (out, mp) = freeze_one(tmp.path());
    let mut manifest =
        SnapshotManifest::parse(&mp.display().to_string(), &std::fs::read(&mp).unwrap()).unwrap();

    // Rewrite the frozen file with genuinely different rows, and update the manifest's
    // recorded digest/size to match — the digest check now passes, but the logical
    // fingerprint over the rows must still catch it.
    let victim = out.join(&manifest.files[0].path);
    let replacement = tmp.path().join("replacement.parquet");
    common::write_event_parquet(&replacement, &[7, 8, 9, 10, 11]);
    let new_bytes = std::fs::read(&replacement).unwrap();
    std::fs::write(&victim, &new_bytes).unwrap();
    manifest.files[0].digest = parquet_lab_contract::digest_bytes(&new_bytes);
    manifest.files[0].bytes = new_bytes.len() as u64;
    manifest.files[0].rows = 5;
    manifest.total_rows = 5;
    std::fs::write(&mp, serde_json::to_vec_pretty(&manifest).unwrap()).unwrap();

    let err = parquet_lab_snapshot::verify(&mp).unwrap_err();
    assert!(
        err.to_string().contains("logical fingerprint"),
        "an alteration with a matching digest must still fail on the fingerprint: {err}"
    );
}

#[test]
fn verify_detects_an_added_file() {
    let tmp = tempfile::tempdir().unwrap();
    let (out, mp) = freeze_one(tmp.path());
    // Drop an extra file into the snapshot tree.
    std::fs::write(out.join("parquet/sneaked-in.parquet"), b"junk").unwrap();
    let err = parquet_lab_snapshot::verify(&mp).unwrap_err();
    assert!(err.to_string().contains("does not list"), "{err}");
}

#[test]
fn verify_refuses_a_manifest_with_a_traversing_path() {
    let tmp = tempfile::tempdir().unwrap();
    let (_out, mp) = freeze_one(tmp.path());
    let mut manifest =
        SnapshotManifest::parse(&mp.display().to_string(), &std::fs::read(&mp).unwrap()).unwrap();
    manifest.files[0].path = "../escape.parquet".into();
    std::fs::write(&mp, serde_json::to_vec_pretty(&manifest).unwrap()).unwrap();
    let err = parquet_lab_snapshot::verify(&mp).unwrap_err();
    assert!(err.to_string().contains("not relative"), "{err}");
}

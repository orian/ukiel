//! Determinism and refusals: stable output, atomic report writes, parent-manifest
//! verification, and a corrupt footer failing loudly.

mod common;

use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

fn snapshot_with_one_file(tmp: &std::path::Path) -> std::path::PathBuf {
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    common::write_parquet(
        &tmp.join("parquet/p0.parquet"),
        props,
        &[common::event_batch(40, 1)],
    );
    common::write_manifest(tmp, &[("parquet/p0.parquet".into(), 40)])
}

#[test]
fn the_report_is_deterministic() {
    let tmp = tempfile::tempdir().unwrap();
    let mp = snapshot_with_one_file(tmp.path());
    let a = parquet_census::census_snapshot(&mp, false).unwrap();
    let b = parquet_census::census_snapshot(&mp, false).unwrap();
    assert_eq!(
        serde_json::to_string(&a).unwrap(),
        serde_json::to_string(&b).unwrap(),
        "same snapshot censuses to identical JSON"
    );
}

#[test]
fn a_tampered_file_fails_the_parent_manifest_check() {
    let tmp = tempfile::tempdir().unwrap();
    let mp = snapshot_with_one_file(tmp.path());
    // Corrupt the file after the manifest recorded its digest.
    let victim = tmp.path().join("parquet/p0.parquet");
    let mut bytes = std::fs::read(&victim).unwrap();
    *bytes.last_mut().unwrap() ^= 0xff;
    std::fs::write(&victim, &bytes).unwrap();
    let err = parquet_census::census_snapshot(&mp, false).unwrap_err();
    assert!(err.to_string().contains("digest mismatch"), "{err}");
}

#[test]
fn a_corrupt_footer_fails_loudly() {
    let tmp = tempfile::tempdir().unwrap();
    // Write junk that is not Parquet, and a manifest that (dishonestly) matches its digest.
    let p = tmp.path().join("parquet/p0.parquet");
    std::fs::create_dir_all(p.parent().unwrap()).unwrap();
    std::fs::write(&p, b"not a parquet file at all").unwrap();
    let mp = common::write_manifest(tmp.path(), &[("parquet/p0.parquet".into(), 1)]);
    let err = parquet_census::census_snapshot(&mp, false).unwrap_err();
    assert!(
        err.to_string().to_lowercase().contains("open")
            || err.to_string().to_lowercase().contains("parquet")
            || err.to_string().to_lowercase().contains("magic"),
        "a non-parquet file must fail loudly: {err}"
    );
}

#[test]
fn the_census_depends_on_no_datafusion_or_ukiel_crate() {
    use std::collections::HashSet;
    use std::process::Command;
    let out = Command::new(env!("CARGO"))
        .args([
            "tree",
            "-p",
            "parquet-census",
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
    ] {
        assert!(
            !deps.contains(forbidden),
            "parquet-census must stay Arrow+Parquet only, but its graph reaches {forbidden}"
        );
    }
    assert!(deps.contains("parquet") && deps.contains("arrow"));
}

#[test]
fn the_report_write_is_atomic_and_refuses_overwrite() {
    let tmp = tempfile::tempdir().unwrap();
    let mp = snapshot_with_one_file(tmp.path());
    let report = parquet_census::census_snapshot(&mp, false).unwrap();
    let out = tmp.path().join("census.json");
    parquet_census::write_report(&out, &report, false).unwrap();
    assert!(out.exists());
    // A second write without --replace refuses.
    let err = parquet_census::write_report(&out, &report, false).unwrap_err();
    assert!(err.to_string().contains("--replace"), "{err}");
    // With --replace it succeeds.
    parquet_census::write_report(&out, &report, true).unwrap();
    // No stray temp file left behind.
    assert!(!tmp.path().join("census.json.tmp").exists());
}

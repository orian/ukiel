//! Property-style coverage: across a range of writer configurations, a rewrite always
//! preserves the logical fingerprint, the row count, and file membership, and the resolved
//! footer confirms the requested codec.

mod common;

use parquet_lab_contract::{SnapshotManifest, VariantManifest};

fn one(dir: &std::path::Path, mp: &std::path::Path, label: &str, toml: &str) -> VariantManifest {
    let spec = common::write_spec(dir, &format!("{label}.toml"), toml);
    let out = dir.join(format!("variant-{label}"));
    let manifest_path = parquet_rewrite::run(mp, &spec, &out, false).unwrap();
    VariantManifest::parse(
        &manifest_path.display().to_string(),
        &std::fs::read(&manifest_path).unwrap(),
    )
    .unwrap()
}

#[test]
fn every_configuration_preserves_the_fingerprint_and_row_count() {
    let tmp = tempfile::tempdir().unwrap();
    let mp = common::write_snapshot(
        tmp.path(),
        &[common::event_batch(150, 1), common::event_batch(150, 500)],
    );
    let snapshot =
        SnapshotManifest::parse(&mp.display().to_string(), &std::fs::read(&mp).unwrap()).unwrap();

    let configs = [
        (
            "rg32",
            "label=\"rg32\"\nrow_group_rows=32\ncompression=\"zstd(1)\"",
        ),
        (
            "rg512k",
            "label=\"rg512k\"\nrow_group_rows=524288\ncompression=\"lz4_raw\"",
        ),
        (
            "pages64k",
            "label=\"p\"\nrow_group_rows=131072\ndata_page_bytes=65536\ncompression=\"snappy\"",
        ),
        (
            "statschunk",
            "label=\"s\"\nrow_group_rows=131072\nstatistics=\"chunk\"\ncompression=\"zstd(6)\"",
        ),
        (
            "uncompressed",
            "label=\"u\"\nrow_group_rows=131072\ncompression=\"uncompressed\"",
        ),
    ];
    for (label, toml) in configs {
        let m = one(tmp.path(), &mp, label, toml);
        assert!(
            m.logical_fingerprint
                .agrees_with(&snapshot.logical_fingerprint),
            "config {label} changed the logical fingerprint"
        );
        assert_eq!(m.files.len(), 1, "membership preserved for {label}");
        assert_eq!(
            m.files[0].output_rows, 300,
            "row count preserved for {label}"
        );
    }
}

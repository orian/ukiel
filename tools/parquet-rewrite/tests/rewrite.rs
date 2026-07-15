//! The rewriter produces what the spec asked for, reads back what the writer actually did,
//! and preserves the logical fingerprint, file membership, and row count.

mod common;

use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet_lab_contract::VariantManifest;

fn open_output(
    dir: &std::path::Path,
    manifest: &VariantManifest,
) -> std::sync::Arc<parquet::file::metadata::ParquetMetaData> {
    let out = dir.join(&manifest.files[0].output.path);
    ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(std::fs::read(out).unwrap()))
        .unwrap()
        .metadata()
        .clone()
}

#[test]
fn a_rewrite_applies_codec_encoding_and_reads_resolved_from_the_footer() {
    let tmp = tempfile::tempdir().unwrap();
    let mp = common::write_snapshot(tmp.path(), &[common::event_batch(200, 1)]);
    let spec = common::write_spec(
        tmp.path(),
        "v.toml",
        r#"
label = "zstd6-ts-delta"
row_group_rows = 64
compression = "zstd(6)"
statistics = "page"

[[column]]
name = "timestamp"
encoding = "delta_binary_packed"
dictionary = false
"#,
    );
    let out = tmp.path().join("variant");
    let manifest_path = parquet_rewrite::run(&mp, &spec, &out, false).unwrap();
    let manifest = VariantManifest::parse(
        &manifest_path.display().to_string(),
        &std::fs::read(&manifest_path).unwrap(),
    )
    .unwrap();

    // Row count and membership preserved.
    assert_eq!(manifest.files.len(), 1);
    assert_eq!(manifest.files[0].output_rows, 200);

    // Footer says ZSTD and delta-packed ts with the dictionary off.
    let meta = open_output(&out, &manifest);
    assert!(
        meta.num_row_groups() >= 3,
        "row_group_rows=64 over 200 rows"
    );
    let rg0 = meta.row_group(0);
    for c in rg0.columns() {
        assert!(format!("{:?}", c.compression()).contains("ZSTD"));
    }
    let ts = rg0.column(1);
    let encs: Vec<_> = ts.encodings().map(|e| format!("{e:?}")).collect();
    assert!(
        encs.iter().any(|e| e.contains("DELTA_BINARY_PACKED")),
        "{encs:?}"
    );
    assert!(ts.dictionary_page_offset().is_none(), "ts dict off");
    assert!(rg0.sorting_columns().is_some(), "sorting columns stamped");

    // The manifest's resolved column props were read from the footer, not the request.
    let ts_col = manifest
        .columns
        .iter()
        .find(|c| c.column == "timestamp")
        .unwrap();
    assert_eq!(ts_col.encoding.as_deref(), Some("delta_binary_packed"));
    assert!(
        ts_col
            .resolved_encodings
            .iter()
            .any(|e| e.contains("DELTA_BINARY_PACKED"))
    );
    assert_eq!(ts_col.resolved_dictionary, Some(false));

    // The logical fingerprint is preserved and equals the snapshot's.
    assert_eq!(
        manifest.logical_fingerprint.version,
        parquet_lab_contract::LOGICAL_ROW_MULTISET_VERSION
    );
    assert_eq!(manifest.parent_snapshot_digest.len(), 64);
}

#[test]
fn a_dictionary_disabled_column_falls_back_to_plain_and_the_footer_shows_it() {
    let tmp = tempfile::tempdir().unwrap();
    let mp = common::write_snapshot(tmp.path(), &[common::event_batch(120, 1)]);
    let spec = common::write_spec(
        tmp.path(),
        "v.toml",
        r#"
label = "name-plain"
row_group_rows = 131072
compression = "snappy"

[[column]]
name = "name"
encoding = "plain"
dictionary = false
"#,
    );
    let out = tmp.path().join("variant");
    let manifest_path = parquet_rewrite::run(&mp, &spec, &out, false).unwrap();
    let manifest = VariantManifest::parse(
        &manifest_path.display().to_string(),
        &std::fs::read(&manifest_path).unwrap(),
    )
    .unwrap();
    let name = manifest
        .columns
        .iter()
        .find(|c| c.column == "name")
        .unwrap();
    assert_eq!(name.resolved_dictionary, Some(false), "dictionary disabled");
    assert!(
        name.resolved_encodings.iter().any(|e| e.contains("PLAIN")),
        "resolved encodings: {:?}",
        name.resolved_encodings
    );
}

#[test]
fn a_bloom_filter_request_is_confirmed_in_the_footer() {
    let tmp = tempfile::tempdir().unwrap();
    let mp = common::write_snapshot(tmp.path(), &[common::event_batch(300, 1)]);
    let spec = common::write_spec(
        tmp.path(),
        "v.toml",
        r#"
label = "name-bloom"
row_group_rows = 131072
compression = "zstd(3)"

[[column]]
name = "name"
bloom_fpp = 0.01
bloom_ndv = 300
"#,
    );
    let out = tmp.path().join("variant");
    let manifest_path = parquet_rewrite::run(&mp, &spec, &out, false).unwrap();
    let manifest = VariantManifest::parse(
        &manifest_path.display().to_string(),
        &std::fs::read(&manifest_path).unwrap(),
    )
    .unwrap();
    let meta = open_output(&out, &manifest);
    let name_idx = meta
        .file_metadata()
        .schema_descr()
        .columns()
        .iter()
        .position(|c| c.name() == "name")
        .unwrap();
    assert!(
        meta.row_group(0)
            .column(name_idx)
            .bloom_filter_offset()
            .is_some(),
        "the name column should carry a Bloom filter"
    );
}

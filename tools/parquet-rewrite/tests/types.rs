//! Type safety: a lossless narrowing keeps the logical fingerprint and normalized schema,
//! while an overflow, a float loss, or a date guessed from an integer is refused.

mod common;

use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet_lab_contract::{SnapshotManifest, VariantManifest};

fn run_spec(
    dir: &std::path::Path,
    mp: &std::path::Path,
    toml: &str,
) -> anyhow::Result<VariantManifest> {
    let spec = common::write_spec(dir, "v.toml", toml);
    let out = dir.join("variant");
    let _ = std::fs::remove_dir_all(&out);
    let manifest_path = parquet_rewrite::run(mp, &spec, &out, true)?;
    Ok(VariantManifest::parse(
        &manifest_path.display().to_string(),
        &std::fs::read(&manifest_path).unwrap(),
    )
    .unwrap())
}

#[test]
fn int64_narrowed_to_int32_preserves_the_logical_fingerprint() {
    let tmp = tempfile::tempdir().unwrap();
    let mp = common::write_snapshot(tmp.path(), &[common::event_batch(200, 1)]);
    let snapshot =
        SnapshotManifest::parse(&mp.display().to_string(), &std::fs::read(&mp).unwrap()).unwrap();

    let manifest = run_spec(
        tmp.path(),
        &mp,
        r#"
label = "team-int32"
row_group_rows = 131072
compression = "zstd(3)"

[[column]]
name = "team_id"
physical_type = "int32"
"#,
    )
    .unwrap();

    // The variant reproduces the snapshot's logical fingerprint exactly.
    assert!(
        manifest
            .logical_fingerprint
            .agrees_with(&snapshot.logical_fingerprint)
    );

    // And the physical Arrow type really is Int32 now.
    let out = tmp
        .path()
        .join("variant")
        .join(&manifest.files[0].output.path);
    let schema =
        ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(std::fs::read(out).unwrap()))
            .unwrap()
            .schema()
            .clone();
    assert_eq!(
        schema.field_with_name("team_id").unwrap().data_type(),
        &arrow::datatypes::DataType::Int32
    );
}

#[test]
fn a_timestamp_int64_to_arrow_timestamp_preserves_the_fingerprint() {
    let tmp = tempfile::tempdir().unwrap();
    let mp = common::write_snapshot(tmp.path(), &[common::event_batch(100, 1)]);
    let snapshot =
        SnapshotManifest::parse(&mp.display().to_string(), &std::fs::read(&mp).unwrap()).unwrap();
    let manifest = run_spec(
        tmp.path(),
        &mp,
        r#"
label = "ts-arrow"
row_group_rows = 131072
compression = "zstd(3)"

[[column]]
name = "timestamp"
physical_type = "timestamp_ms"
"#,
    )
    .unwrap();
    assert!(
        manifest
            .logical_fingerprint
            .agrees_with(&snapshot.logical_fingerprint)
    );
    let out = tmp
        .path()
        .join("variant")
        .join(&manifest.files[0].output.path);
    let schema =
        ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(std::fs::read(out).unwrap()))
            .unwrap()
            .schema()
            .clone();
    assert!(matches!(
        schema.field_with_name("timestamp").unwrap().data_type(),
        arrow::datatypes::DataType::Timestamp(_, _)
    ));
}

#[test]
fn an_overflowing_narrowing_is_refused() {
    let tmp = tempfile::tempdir().unwrap();
    // team_base large so team_id exceeds i8 range.
    let mp = common::write_snapshot(tmp.path(), &[common::event_batch(40, 1000)]);
    let err = run_spec(
        tmp.path(),
        &mp,
        r#"
label = "team-int8"
row_group_rows = 131072
compression = "zstd(3)"

[[column]]
name = "team_id"
physical_type = "int8"
"#,
    )
    .unwrap_err();
    assert!(
        err.to_string().contains("lossy") || err.to_string().contains("fit"),
        "{err}"
    );
}

#[test]
fn projecting_a_non_integer_to_an_int_is_refused() {
    let tmp = tempfile::tempdir().unwrap();
    let mp = common::write_snapshot(tmp.path(), &[common::event_batch(40, 1)]);
    // value is float64; narrowing it to int32 is not permitted.
    let err = run_spec(
        tmp.path(),
        &mp,
        r#"
label = "value-int32"
row_group_rows = 131072
compression = "zstd(3)"

[[column]]
name = "value"
physical_type = "int32"
"#,
    )
    .unwrap_err();
    assert!(err.to_string().contains("declared"), "{err}");
}

#[test]
fn guessing_a_date_from_an_integer_is_refused() {
    let tmp = tempfile::tempdir().unwrap();
    let mp = common::write_snapshot(tmp.path(), &[common::event_batch(40, 1)]);
    // team_id is a signed int, not a declared date; Date32 must be refused.
    let err = run_spec(
        tmp.path(),
        &mp,
        r#"
label = "team-date"
row_group_rows = 131072
compression = "zstd(3)"

[[column]]
name = "team_id"
physical_type = "date32"
"#,
    )
    .unwrap_err();
    assert!(err.to_string().contains("date"), "{err}");
}

#[test]
fn an_unsupported_codec_fails_validation_before_writing() {
    let tmp = tempfile::tempdir().unwrap();
    let mp = common::write_snapshot(tmp.path(), &[common::event_batch(40, 1)]);
    let err = run_spec(
        tmp.path(),
        &mp,
        r#"
label = "bad-codec"
row_group_rows = 131072
compression = "brotli(9)"
"#,
    )
    .unwrap_err();
    assert!(err.to_string().contains("compression"), "{err}");
    // Nothing was published.
    assert!(!tmp.path().join("variant").join("manifest.json").exists());
}

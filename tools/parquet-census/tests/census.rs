//! The census reads what was actually written: encodings, codec, dictionary state, page
//! index, Bloom filters, null counts, and multiple row groups.

mod common;

use parquet::basic::{Compression, Encoding, ZstdLevel};
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::schema::types::ColumnPath;

fn find_col<'a>(
    report: &'a parquet_census::CensusReport,
    file: usize,
    rg: usize,
    col: &str,
) -> &'a parquet_census::ColumnChunkCensus {
    report.file_records[file].row_group_records[rg]
        .columns
        .iter()
        .find(|c| c.column == col)
        .unwrap_or_else(|| panic!("column {col} not found"))
}

#[test]
fn the_census_reads_codec_encoding_dictionary_bloom_and_row_groups() {
    let tmp = tempfile::tempdir().unwrap();
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).unwrap()))
        // ts delta-packed, dictionary off — exactly the product policy for the ts column.
        .set_column_encoding(ColumnPath::from("timestamp"), Encoding::DELTA_BINARY_PACKED)
        .set_column_dictionary_enabled(ColumnPath::from("timestamp"), false)
        // A Bloom filter on the high-NDV name column.
        .set_column_bloom_filter_enabled(ColumnPath::from("name"), true)
        .set_statistics_enabled(EnabledStatistics::Page)
        // Force several row groups.
        .set_max_row_group_row_count(Some(50))
        .build();
    common::write_parquet(
        &tmp.path().join("parquet/p0.parquet"),
        props,
        &[common::event_batch(200, 1)],
    );
    let mp = common::write_manifest(tmp.path(), &[("parquet/p0.parquet".into(), 200)]);

    let report = parquet_census::census_snapshot(&mp, false).unwrap();
    assert_eq!(report.total_rows, 200);
    assert!(
        report.file_records[0].row_groups >= 4,
        "200 rows / 50 per group => several groups, got {}",
        report.file_records[0].row_groups
    );

    // ZSTD codec on every column.
    let team = find_col(&report, 0, 0, "team_id");
    assert!(team.compression.contains("ZSTD"), "{}", team.compression);

    // ts is delta-packed and carries no dictionary page.
    let ts = find_col(&report, 0, 0, "timestamp");
    assert!(
        ts.encodings
            .iter()
            .any(|e| e.contains("DELTA_BINARY_PACKED")),
        "ts encodings: {:?}",
        ts.encodings
    );
    assert!(!ts.has_dictionary_page, "ts dictionary should be off");

    // name carries a Bloom filter and (by default) a dictionary.
    let name = find_col(&report, 0, 0, "name");
    assert!(
        name.bloom_filter_offset.is_some(),
        "name should have a Bloom filter"
    );
    assert!(name.bloom_filter_length.unwrap() > 0);

    // Page statistics => offset/column index present.
    assert!(
        team.has_offset_index || team.has_column_index,
        "page index expected"
    );

    // The declared logical type flows through from the manifest projection.
    assert_eq!(ts.declared_logical_type.as_deref(), Some("timestamp_ms"));

    // Null counts come from the footer: name has nulls (every 5th), value/team none.
    assert!(name.null_count.unwrap() > 0, "name has nulls");
    assert_eq!(name.null_count_source, parquet_census::Provenance::Footer);
    assert_eq!(team.null_count, Some(0));
}

#[test]
fn statistics_none_yields_no_footer_null_counts() {
    let tmp = tempfile::tempdir().unwrap();
    let props = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::None)
        .set_compression(Compression::SNAPPY)
        .build();
    common::write_parquet(
        &tmp.path().join("parquet/p0.parquet"),
        props,
        &[common::event_batch(40, 1)],
    );
    let mp = common::write_manifest(tmp.path(), &[("parquet/p0.parquet".into(), 40)]);
    let report = parquet_census::census_snapshot(&mp, false).unwrap();
    let team = find_col(&report, 0, 0, "team_id");
    assert!(team.compression.contains("SNAPPY"));
    assert_eq!(
        team.null_count, None,
        "no statistics => no footer null count"
    );
    assert_eq!(
        team.null_count_source,
        parquet_census::Provenance::Unavailable
    );
}

#[test]
fn scanning_supplies_ndv_and_value_width_labeled_scanned() {
    let tmp = tempfile::tempdir().unwrap();
    let props = WriterProperties::builder()
        .set_compression(Compression::LZ4_RAW)
        .build();
    // 12 rows, team_id = base + i/4 => teams {1,2,3} distinct = 3.
    common::write_parquet(
        &tmp.path().join("parquet/p0.parquet"),
        props,
        &[common::event_batch(12, 1)],
    );
    let mp = common::write_manifest(tmp.path(), &[("parquet/p0.parquet".into(), 12)]);

    let footer_only = parquet_census::census_snapshot(&mp, false).unwrap();
    let team = find_col(&footer_only, 0, 0, "team_id");
    assert_eq!(
        team.distinct_count_source,
        parquet_census::Provenance::Unavailable
    );

    let scanned = parquet_census::census_snapshot(&mp, true).unwrap();
    assert!(scanned.scanned_columns);
    let team = find_col(&scanned, 0, 0, "team_id");
    assert_eq!(team.distinct_count, Some(3), "teams 1,2,3");
    assert_eq!(
        team.distinct_count_source,
        parquet_census::Provenance::Scanned
    );
    assert!(team.mean_value_bytes.unwrap() > 0.0);
    assert_eq!(team.value_width_source, parquet_census::Provenance::Scanned);
}

#[test]
fn aggregates_roll_up_across_files_and_sum_the_fractions() {
    let tmp = tempfile::tempdir().unwrap();
    let props = || {
        WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build()
    };
    common::write_parquet(
        &tmp.path().join("parquet/a.parquet"),
        props(),
        &[common::event_batch(30, 1)],
    );
    common::write_parquet(
        &tmp.path().join("parquet/b.parquet"),
        props(),
        &[common::event_batch(30, 100)],
    );
    let mp = common::write_manifest(
        tmp.path(),
        &[
            ("parquet/a.parquet".into(), 30),
            ("parquet/b.parquet".into(), 30),
        ],
    );
    let report = parquet_census::census_snapshot(&mp, false).unwrap();
    assert_eq!(report.files, 2);
    assert_eq!(report.total_rows, 60);
    assert_eq!(report.column_aggregates.len(), 4, "four columns");
    let sum: f64 = report
        .column_aggregates
        .iter()
        .map(|c| c.compressed_fraction)
        .sum();
    assert!((sum - 1.0).abs() < 1e-9, "fractions sum to 1, got {sum}");
    // Aggregates are ordered by compressed bytes descending.
    for w in report.column_aggregates.windows(2) {
        assert!(w[0].compressed_bytes >= w[1].compressed_bytes);
    }
}

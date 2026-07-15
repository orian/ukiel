//! The sidecar changes an actual Parquet scan: with statistics disabled (so native pruning
//! cannot help), a value-set sidecar skips a row group, the skipped group's data range is
//! never fetched, fewer bytes are read, and the answer is identical.

use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::scalar::ScalarValue;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet_lab_bench::skip_scan::scan_equality;
use parquet_lab_contract::{
    FileDigest, IndexKind, IndexedColumn, RowGroupIndex, SKIP_MANIFEST_VERSION, SkipManifest,
};
use parquet_skip_index_core::value_set::ValueSet;
use parquet_skip_index_core::{Predicate, Value, payload_digest};

/// Write a two-row-group file (team_id 1,1,2,2 | 5,5,9,9) with statistics DISABLED, so no
/// native row-group pruning is possible — the sidecar is the only thing that can skip.
fn write_file() -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("team_id", DataType::Int64, false),
        Field::new("event", DataType::Utf8, false),
    ]));
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(4))
        .set_statistics_enabled(EnabledStatistics::None)
        .build();
    let mut buf: Vec<u8> = Vec::new();
    let mut w = ArrowWriter::try_new(&mut buf, schema.clone(), Some(props)).unwrap();
    w.write(
        &RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 1, 2, 2, 5, 5, 9, 9])),
                Arc::new(StringArray::from(vec![
                    "a", "b", "c", "d", "e", "f", "g", "h",
                ])),
            ],
        )
        .unwrap(),
    )
    .unwrap();
    w.close().unwrap();
    buf
}

fn sidecar(rel: &str, bytes: &[u8]) -> (SkipManifest, Vec<u8>) {
    // Build the complete value set of each row group: rg0 {1,2}, rg1 {5,9}.
    let rg0 = ValueSet::build(&[Some(Value::Int(1)), Some(Value::Int(2))], 4096)
        .unwrap()
        .encode();
    let rg1 = ValueSet::build(&[Some(Value::Int(5)), Some(Value::Int(9))], 4096)
        .unwrap()
        .encode();
    let mut payload = Vec::new();
    let off0 = 0u64;
    payload.extend_from_slice(&rg0);
    let off1 = payload.len() as u64;
    payload.extend_from_slice(&rg1);
    let manifest = SkipManifest {
        manifest_version: SKIP_MANIFEST_VERSION.into(),
        parent_variant_digest: "variant".into(),
        file_digests: vec![FileDigest::of(rel.to_string(), bytes)],
        payload_path: "payload.bin".into(),
        columns: vec![IndexedColumn {
            column: "team_id".into(),
            kind: IndexKind::ValueSet,
            kind_version: "value_set/v1".into(),
            file: rel.into(),
            row_groups: vec![
                RowGroupIndex {
                    row_group: 0,
                    kind: IndexKind::ValueSet,
                    parameters: serde_json::json!({}),
                    payload_offset: off0,
                    payload_length: rg0.len() as u64,
                    payload_digest: payload_digest(&rg0),
                },
                RowGroupIndex {
                    row_group: 1,
                    kind: IndexKind::ValueSet,
                    parameters: serde_json::json!({}),
                    payload_offset: off1,
                    payload_length: rg1.len() as u64,
                    payload_digest: payload_digest(&rg1),
                },
            ],
        }],
        build_wall_ms: 0,
        payload_bytes: payload.len() as u64,
    };
    (manifest, payload)
}

#[tokio::test]
async fn a_sidecar_skips_a_row_group_and_its_data_is_never_fetched() {
    let bytes = write_file();
    let rel = "out/p0.parquet".to_string();
    let files = vec![(rel.clone(), bytes.clone())];
    let (manifest, payload) = sidecar(&rel, &bytes);

    let pred = Predicate::Eq(Value::Int(5));
    let val = || ScalarValue::Int64(Some(5));

    // Without the sidecar: no native stats, so both row groups are selected and read.
    let without = scan_equality(&files, "team_id", val(), &pred, None)
        .await
        .unwrap();
    assert_eq!(
        without.ledger.selected_row_groups, 2,
        "no pruning without the sidecar"
    );

    // With the sidecar: rg0 ({1,2}) provably excludes 5 -> skipped; rg1 kept.
    let with = scan_equality(&files, "team_id", val(), &pred, Some((&manifest, &payload)))
        .await
        .unwrap();
    assert_eq!(with.ledger.custom_no_match, 1, "rg0 is a NoMatch");
    assert_eq!(
        with.ledger.selected_row_groups, 1,
        "the sidecar skipped a row group"
    );

    // The answer is identical.
    assert_eq!(
        with.ledger.answer_digest, without.ledger.answer_digest,
        "skipping a provably-empty row group does not change the answer"
    );

    // The skipped row group's data byte range is never fetched.
    let rg0 = with.spans.iter().find(|s| s.row_group == 0).unwrap();
    let overlaps_rg0 = with.fetched_ranges.iter().any(|(f, r)| {
        f.ends_with("p0.parquet") && r.start < rg0.data_end && rg0.data_start < r.end
    });
    assert!(
        !overlaps_rg0,
        "a skipped row group's data range [{}, {}) must never be fetched; ranges: {:?}",
        rg0.data_start, rg0.data_end, with.fetched_ranges
    );

    // rg1's data IS fetched (it was kept), proving the scan really ran.
    let rg1 = with.spans.iter().find(|s| s.row_group == 1).unwrap();
    let overlaps_rg1 = with.fetched_ranges.iter().any(|(f, r)| {
        f.ends_with("p0.parquet") && r.start < rg1.data_end && rg1.data_start < r.end
    });
    assert!(overlaps_rg1, "the kept row group's data must be fetched");
}

#[tokio::test]
async fn an_unknown_decision_keeps_the_row_group() {
    let bytes = write_file();
    let rel = "out/p0.parquet".to_string();
    let files = vec![(rel.clone(), bytes.clone())];
    let (mut manifest, mut payload) = sidecar(&rel, &bytes);

    // Corrupt rg0's payload digest so the sidecar abstains (Unknown) on it.
    manifest.columns[0].row_groups[0].payload_digest = "00".repeat(32);
    let _ = &mut payload;

    let with = scan_equality(
        &files,
        "team_id",
        ScalarValue::Int64(Some(5)),
        &Predicate::Eq(Value::Int(5)),
        Some((&manifest, &payload)),
    )
    .await
    .unwrap();
    // rg0 abstains (kept), rg1 kept: no custom skip, both selected.
    assert_eq!(
        with.ledger.custom_no_match, 0,
        "a corrupt payload never skips"
    );
    assert_eq!(with.ledger.unknown, 1);
    assert_eq!(with.ledger.selected_row_groups, 2);
}

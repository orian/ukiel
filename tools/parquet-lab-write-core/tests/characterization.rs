//! Characterization: the two write entry points must produce byte-identical Parquet for
//! the same input and resolved config. `parquet-rewrite` writes through `write_projected`
//! (decode + project + fold + write in one pass); `parquet-write-bench` prepares batches
//! outside the clock and writes them through `write_prepared`. If those two ever diverged,
//! a writer timing would not describe the bytes the rewrite tool actually publishes.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use parquet_lab_integrity::{LogicalColumn, LogicalRowMultiset, LogicalSchema, LogicalType};
use parquet_lab_write_core::{
    WriterConfig, decode_batches, out_schema_for, project_batches, sorting_columns, write_prepared,
    write_projected,
};

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("team_id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]))
}

fn logical_schema() -> LogicalSchema {
    LogicalSchema::new(vec![
        LogicalColumn { name: "team_id".into(), logical: LogicalType::SignedInt },
        LogicalColumn { name: "name".into(), logical: LogicalType::Utf8 },
    ])
}

fn source_bytes() -> Vec<u8> {
    let teams: Vec<i64> = (0..500).map(|i| i / 3).collect();
    let names: Vec<Option<String>> =
        (0..500).map(|i| if i % 7 == 0 { None } else { Some(format!("n-{i}")) }).collect();
    let b = RecordBatch::try_new(
        schema(),
        vec![Arc::new(Int64Array::from(teams)), Arc::new(StringArray::from(names))],
    )
    .unwrap();
    let mut buf = Vec::new();
    let mut w = ArrowWriter::try_new(&mut buf, schema(), Some(WriterProperties::builder().build()))
        .unwrap();
    w.write(&b).unwrap();
    w.close().unwrap();
    buf
}

fn config() -> WriterConfig {
    WriterConfig {
        row_group_rows: 128,
        key_boundary_flush: false,
        write_batch_rows: 1024,
        data_page_bytes: 1 << 20,
        dictionary_page_bytes: 1 << 20,
        statistics: "page".into(),
        offset_index: true,
        compression: "zstd(3)".into(),
        columns: vec![],
    }
}

#[test]
fn write_projected_and_write_prepared_agree_byte_for_byte() {
    let src = source_bytes();
    let projections: BTreeMap<String, _> = config().projections().unwrap();
    let sorting = sorting_columns(&schema(), &["team_id".to_string()]);

    // Path A: the rewrite path — decode + project + fold + write in one pass.
    let mut a: Vec<u8> = Vec::new();
    let mut logical = LogicalRowMultiset::default();
    write_projected(
        &mut a,
        &src,
        &projections,
        config().writer_properties(sorting.clone()).unwrap(),
        Arc::new(out_schema_for(&schema(), &projections)),
        &logical_schema(),
        &mut logical,
    )
    .unwrap();

    // Path B: the bench path — prepare outside the clock, then write.
    let (decoded, input_schema) = decode_batches(&src).unwrap();
    let projected = project_batches(&decoded, &projections).unwrap();
    let mut b: Vec<u8> = Vec::new();
    write_prepared(
        &mut b,
        &projected,
        Arc::new(out_schema_for(&input_schema, &projections)),
        config().writer_properties(sorting).unwrap(),
    )
    .unwrap();

    assert_eq!(a, b, "the rewrite and bench write paths must produce identical bytes");
}

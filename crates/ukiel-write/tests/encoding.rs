use arrow::{
    array::{Array, Float64Array, Int64Array, RecordBatch, TimestampNanosecondArray},
    datatypes::{DataType, Field, Schema, TimeUnit},
};
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::json;
use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use ukiel_core::{Hypertable, HypertableId, Placement};
use ukiel_write::{WriteError, batches_to_l0_items, validate_output_schema};

fn target() -> Hypertable {
    Hypertable {
        id: HypertableId(1),
        name: "target".into(),
        table_schema: json!({"fields":[
            {"name":"tenant_id","type":"int64","nullable":false},
            {"name":"day","type":"int64"},
            {"name":"inserted_at","type":"timestamp_ms","nullable":false,"default":"now()"},
            {"name":"sample","type":"float64","nullable":false,"materialized":"random()"},
            {"name":"view_only","type":"int64","alias":"tenant_id + 1"}
        ]}),
        partition_spec: json!({"columns":["day"]}),
        sort_key: vec!["tenant_id".into(), "inserted_at".into()],
        packing_key: "tenant_id".into(),
        placement: Placement::Packed,
    }
}

fn input() -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("tenant_id", DataType::Int64, false),
            Field::new("day", DataType::Int64, true),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![3, 1, 2])),
            Arc::new(Int64Array::from(vec![1, 2, 1])),
        ],
    )
    .unwrap()
}

#[test]
fn partitions_sorts_and_materializes_nonnullable_clock_defaults_into_parquet() {
    let before = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let items = batches_to_l0_items(&[input()], &target(), 1024 * 1024).unwrap();
    let after = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    assert_eq!(items.len(), 2);
    let mut all_keys = Vec::new();
    for item in items {
        let mut reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(item.part.bytes))
            .unwrap()
            .build()
            .unwrap();
        let batch = reader.next().unwrap().unwrap();
        assert_eq!(batch.num_columns(), 4);
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let days = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let clocks = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let samples = batch
            .column(3)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!(keys.values().windows(2).all(|pair| pair[0] <= pair[1]));
        for row in 0..batch.num_rows() {
            assert_eq!(Some(days.value(row)), item.partition_values["day"].as_i64());
            assert!((before..=after).contains(&clocks.value(row)));
            assert!((0.0..1.0).contains(&samples.value(row)));
            all_keys.push(keys.value(row));
        }
    }
    all_keys.sort();
    assert_eq!(all_keys, vec![1, 2, 3]);
}

#[test]
fn sql_timestamp_changes_units_before_encoding_timestamp_ms() {
    let base = input();
    let mut fields = base.schema().fields().iter().cloned().collect::<Vec<_>>();
    fields.push(Arc::new(Field::new(
        "inserted_at",
        DataType::Timestamp(TimeUnit::Nanosecond, None),
        false,
    )));
    let mut arrays = base.columns().to_vec();
    arrays.push(Arc::new(TimestampNanosecondArray::from(
        vec![1_234_000_000_000i64; 3],
    )));
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap();
    for item in batches_to_l0_items(&[batch], &target(), 1024 * 1024).unwrap() {
        let batch = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(item.part.bytes))
            .unwrap()
            .build()
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        let clock = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert!(clock.values().iter().all(|v| *v == 1_234_000));
    }
}

#[test]
fn validates_target_contract_and_budget_before_output() {
    let target = target();
    validate_output_schema(input().schema().as_ref(), &target).unwrap();
    for name in ["tenant_id_missing", "sample", "view_only"] {
        let schema = Schema::new(vec![Field::new(name, DataType::Int64, true)]);
        assert!(matches!(
            validate_output_schema(&schema, &target),
            Err(WriteError::InvalidTarget(_))
        ));
    }
    assert!(matches!(
        batches_to_l0_items(&[input()], &target, 1),
        Err(WriteError::BudgetExceeded(1))
    ));
    assert!(batches_to_l0_items(&[], &target, 1).unwrap().is_empty());
}

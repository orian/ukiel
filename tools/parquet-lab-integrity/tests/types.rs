//! The logical fingerprint's defining property: equal declared values fingerprint
//! equal across different physical representations, and any lossy or unsupported cast
//! fails closed rather than blessing a value it cannot faithfully encode.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, Float32Array, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray,
    StringViewArray, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampSecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use parquet_lab_integrity::{
    LogicalColumn, LogicalFingerprintError, LogicalRowMultiset, LogicalSchema, LogicalType,
};

fn batch(field: Field, array: ArrayRef) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![field]));
    RecordBatch::try_new(schema, vec![array]).unwrap()
}

fn schema(name: &str, logical: LogicalType) -> LogicalSchema {
    LogicalSchema::new(vec![LogicalColumn {
        name: name.into(),
        logical,
    }])
}

fn fp(b: &RecordBatch, s: &LogicalSchema) -> LogicalRowMultiset {
    let mut m = LogicalRowMultiset::default();
    m.update(b, s).unwrap();
    m
}

#[test]
fn int32_and_int64_with_equal_values_fingerprint_equal() {
    let s = schema("n", LogicalType::SignedInt);
    let wide = batch(
        Field::new("n", DataType::Int64, false),
        Arc::new(Int64Array::from(vec![1_i64, 2, 3, -7])),
    );
    let narrow = batch(
        Field::new("n", DataType::Int32, false),
        Arc::new(Int32Array::from(vec![1_i32, 2, 3, -7])),
    );
    assert_eq!(
        fp(&wide, &s),
        fp(&narrow, &s),
        "a lossless Int64->Int32 narrowing keeps the same declared values"
    );
}

#[test]
fn a_narrowing_that_changes_a_value_fingerprints_differently() {
    let s = schema("n", LogicalType::SignedInt);
    let a = batch(
        Field::new("n", DataType::Int64, false),
        Arc::new(Int64Array::from(vec![1_i64, 2, 3])),
    );
    let b = batch(
        Field::new("n", DataType::Int32, false),
        Arc::new(Int32Array::from(vec![1_i32, 2, 4])),
    );
    assert_ne!(fp(&a, &s), fp(&b, &s), "a changed value must be detectable");
}

#[test]
fn timestamps_normalize_to_milliseconds_across_units() {
    let s = schema("t", LogicalType::TimestampMillis);
    let as_i64 = batch(
        Field::new("t", DataType::Int64, false),
        Arc::new(Int64Array::from(vec![1000_i64, 2000, 3000])),
    );
    let as_ms = batch(
        Field::new("t", DataType::Timestamp(TimeUnit::Millisecond, None), false),
        Arc::new(TimestampMillisecondArray::from(vec![1000_i64, 2000, 3000])),
    );
    let as_secs = batch(
        Field::new("t", DataType::Timestamp(TimeUnit::Second, None), false),
        Arc::new(TimestampSecondArray::from(vec![1_i64, 2, 3])),
    );
    assert_eq!(fp(&as_i64, &s), fp(&as_ms, &s));
    assert_eq!(
        fp(&as_i64, &s),
        fp(&as_secs, &s),
        "seconds convert exactly into the same milliseconds"
    );
}

#[test]
fn a_microsecond_timestamp_that_would_round_is_refused() {
    let s = schema("t", LogicalType::TimestampMillis);
    let lossy = batch(
        Field::new("t", DataType::Timestamp(TimeUnit::Microsecond, None), false),
        // 1_500 us = 1.5 ms — not exactly representable in whole milliseconds.
        Arc::new(TimestampMicrosecondArray::from(vec![1_500_i64])),
    );
    let mut m = LogicalRowMultiset::default();
    let err = m.update(&lossy, &s).unwrap_err();
    assert!(
        matches!(err, LogicalFingerprintError::LossyCast { .. }),
        "{err}"
    );
}

#[test]
fn utf8_and_utf8view_fingerprint_equal() {
    let s = schema("txt", LogicalType::Utf8);
    let plain = batch(
        Field::new("txt", DataType::Utf8, true),
        Arc::new(StringArray::from(vec![Some("alpha"), None, Some("")])),
    );
    let view = batch(
        Field::new("txt", DataType::Utf8View, true),
        Arc::new(StringViewArray::from(vec![Some("alpha"), None, Some("")])),
    );
    assert_eq!(fp(&plain, &s), fp(&view, &s));
}

#[test]
fn null_is_distinct_from_zero_and_from_empty() {
    let s = schema("n", LogicalType::SignedInt);
    let zero = batch(
        Field::new("n", DataType::Int64, true),
        Arc::new(Int64Array::from(vec![Some(0_i64)])),
    );
    let null = batch(
        Field::new("n", DataType::Int64, true),
        Arc::new(Int64Array::from(vec![None as Option<i64>])),
    );
    assert_ne!(fp(&zero, &s), fp(&null, &s), "null is not zero");

    let st = schema("txt", LogicalType::Utf8);
    let empty = batch(
        Field::new("txt", DataType::Utf8, true),
        Arc::new(StringArray::from(vec![Some("")])),
    );
    let null_str = batch(
        Field::new("txt", DataType::Utf8, true),
        Arc::new(StringArray::from(vec![None as Option<&str>])),
    );
    assert_ne!(fp(&empty, &st), fp(&null_str, &st), "null is not empty");
}

#[test]
fn row_and_batch_order_do_not_change_the_fingerprint() {
    let s = schema("n", LogicalType::SignedInt);
    let forward = batch(
        Field::new("n", DataType::Int64, false),
        Arc::new(Int64Array::from(vec![1_i64, 2, 3, 4])),
    );
    let reversed = batch(
        Field::new("n", DataType::Int64, false),
        Arc::new(Int64Array::from(vec![4_i64, 3, 2, 1])),
    );
    assert_eq!(fp(&forward, &s), fp(&reversed, &s));

    // Split across batches and merge: same union.
    let b1 = batch(
        Field::new("n", DataType::Int64, false),
        Arc::new(Int64Array::from(vec![1_i64, 2])),
    );
    let b2 = batch(
        Field::new("n", DataType::Int64, false),
        Arc::new(Int64Array::from(vec![3_i64, 4])),
    );
    let mut merged = LogicalRowMultiset::default();
    merged.update(&b1, &s).unwrap();
    let mut other = LogicalRowMultiset::default();
    other.update(&b2, &s).unwrap();
    merged.merge(&other);
    assert_eq!(merged, fp(&forward, &s), "fold-and-merge equals one pass");
}

#[test]
fn a_duplicated_row_changes_the_fingerprint() {
    let s = schema("n", LogicalType::SignedInt);
    let once = batch(
        Field::new("n", DataType::Int64, false),
        Arc::new(Int64Array::from(vec![7_i64, 8])),
    );
    let twice = batch(
        Field::new("n", DataType::Int64, false),
        Arc::new(Int64Array::from(vec![7_i64, 7, 8])),
    );
    assert_ne!(
        fp(&once, &s),
        fp(&twice, &s),
        "the wrapping sum catches a duplicate that XOR alone would cancel"
    );
}

#[test]
fn a_float32_column_declared_float64_is_refused_not_promoted() {
    let s = schema("m", LogicalType::Float64);
    let f32 = batch(
        Field::new("m", DataType::Float32, false),
        Arc::new(Float32Array::from(vec![1.5_f32, 2.5])),
    );
    let mut m = LogicalRowMultiset::default();
    let err = m.update(&f32, &s).unwrap_err();
    assert!(
        matches!(err, LogicalFingerprintError::UnsupportedCast { .. }),
        "no lossy Float32 promotion is accepted: {err}"
    );
}

#[test]
fn negative_zero_and_positive_zero_are_equal() {
    let s = schema("m", LogicalType::Float64);
    let pos = batch(
        Field::new("m", DataType::Float64, false),
        Arc::new(Float64Array::from(vec![0.0_f64])),
    );
    let neg = batch(
        Field::new("m", DataType::Float64, false),
        Arc::new(Float64Array::from(vec![-0.0_f64])),
    );
    assert_eq!(fp(&pos, &s), fp(&neg, &s));
}

#[test]
fn a_missing_or_extra_column_fails_closed() {
    let s = schema("n", LogicalType::SignedInt);
    // Batch has an extra column the declared schema does not mention.
    let two = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("n", DataType::Int64, false),
            Field::new("extra", DataType::Int64, false),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![1_i64])),
            Arc::new(Int64Array::from(vec![2_i64])),
        ],
    )
    .unwrap();
    let mut m = LogicalRowMultiset::default();
    let err = m.update(&two, &s).unwrap_err();
    assert!(
        matches!(err, LogicalFingerprintError::SchemaMismatch { .. }),
        "a column can never be silently dropped: {err}"
    );
}

#[test]
fn a_renamed_column_fails_closed() {
    let s = schema("n", LogicalType::SignedInt);
    let renamed = batch(
        Field::new("different", DataType::Int64, false),
        Arc::new(Int64Array::from(vec![1_i64])),
    );
    let mut m = LogicalRowMultiset::default();
    assert!(matches!(
        m.update(&renamed, &s).unwrap_err(),
        LogicalFingerprintError::SchemaMismatch { .. }
    ));
}

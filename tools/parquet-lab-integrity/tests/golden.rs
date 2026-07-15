//! Golden vectors pin the canonical encoding. If any of these digests changes, the
//! on-wire logical fingerprint changed and the version constant must change with it —
//! otherwise a snapshot fingerprinted under the old bytes and a variant under the new
//! bytes would silently disagree and reject a correct rewrite.

use std::sync::Arc;

use arrow::array::{
    BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray, TimestampMillisecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use parquet_lab_integrity::{
    LOGICAL_ROW_MULTISET_VERSION, LogicalColumn, LogicalRowMultiset, LogicalSchema, LogicalType,
};

/// A fixed multi-type, multi-row batch resembling a compacted event part.
fn golden_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("team_id", DataType::Int64, false),
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("name", DataType::Utf8, true),
        Field::new("value", DataType::Float64, true),
        Field::new("ok", DataType::Boolean, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1_i64, 1, 2, 900])),
            Arc::new(TimestampMillisecondArray::from(vec![
                1_782_864_000_000_i64,
                1_782_864_000_001,
                1_782_864_100_000,
                1_782_900_000_000,
            ])),
            Arc::new(StringArray::from(vec![
                Some("alpha"),
                None,
                Some(""),
                Some("Ω"),
            ])),
            Arc::new(Float64Array::from(vec![
                Some(1.5),
                Some(-0.0),
                None,
                Some(3.25),
            ])),
            Arc::new(BooleanArray::from(vec![
                Some(true),
                Some(false),
                None,
                Some(true),
            ])),
        ],
    )
    .unwrap()
}

fn golden_schema() -> LogicalSchema {
    LogicalSchema::new(vec![
        LogicalColumn {
            name: "team_id".into(),
            logical: LogicalType::SignedInt,
        },
        LogicalColumn {
            name: "timestamp".into(),
            logical: LogicalType::TimestampMillis,
        },
        LogicalColumn {
            name: "name".into(),
            logical: LogicalType::Utf8,
        },
        LogicalColumn {
            name: "value".into(),
            logical: LogicalType::Float64,
        },
        LogicalColumn {
            name: "ok".into(),
            logical: LogicalType::Boolean,
        },
    ])
}

#[test]
fn the_version_string_is_pinned() {
    assert_eq!(LOGICAL_ROW_MULTISET_VERSION, "logical-row-multiset/v1");
}

#[test]
fn the_golden_digest_is_stable() {
    let mut m = LogicalRowMultiset::default();
    m.update(&golden_batch(), &golden_schema()).unwrap();
    assert_eq!(m.count, 4);
    assert_eq!(
        m.digest_hex(),
        "0eb60453d88586f41eb2120298e0afe30b6f31c01752ece0a19452f4d8e43620",
        "golden digest drifted — bump LOGICAL_ROW_MULTISET_VERSION if the encoding changed on purpose"
    );
}

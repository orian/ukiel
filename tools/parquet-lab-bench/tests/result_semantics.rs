//! Answer semantics: `ordered` digests depend on result order; `multiset` digests do not,
//! but still preserve duplicate counts and detect a changed value.

use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use parquet_lab_bench::compare::result_digest;
use parquet_lab_contract::ResultSemantics;

fn batch(ns: &[i64], ss: &[&str]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("n", DataType::Int64, false),
        Field::new("s", DataType::Utf8, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ns.to_vec())),
            Arc::new(StringArray::from(ss.to_vec())),
        ],
    )
    .unwrap()
}

#[test]
fn ordered_digest_depends_on_row_order() {
    let a = batch(&[1, 2, 3], &["a", "b", "c"]);
    let b = batch(&[3, 2, 1], &["c", "b", "a"]);
    let da = result_digest(
        &a.schema(),
        std::slice::from_ref(&a),
        ResultSemantics::Ordered,
    )
    .unwrap();
    let db = result_digest(
        &b.schema(),
        std::slice::from_ref(&b),
        ResultSemantics::Ordered,
    )
    .unwrap();
    assert_ne!(da, db, "ordered semantics distinguishes row order");
}

#[test]
fn multiset_digest_ignores_order_but_preserves_duplicates() {
    let a = batch(&[1, 2, 3], &["a", "b", "c"]);
    let b = batch(&[3, 1, 2], &["c", "a", "b"]);
    let da = result_digest(
        &a.schema(),
        std::slice::from_ref(&a),
        ResultSemantics::Multiset,
    )
    .unwrap();
    let db = result_digest(
        &b.schema(),
        std::slice::from_ref(&b),
        ResultSemantics::Multiset,
    )
    .unwrap();
    assert_eq!(da, db, "multiset semantics ignores row and batch order");

    // Same rows split across two batches in a different grouping: still equal.
    let split_1 = batch(&[1], &["a"]);
    let split_2 = batch(&[2, 3], &["b", "c"]);
    let d_split = result_digest(
        &split_1.schema(),
        &[split_1, split_2],
        ResultSemantics::Multiset,
    )
    .unwrap();
    assert_eq!(da, d_split, "batch boundaries do not change a multiset");

    // A different multiplicity is a different multiset.
    let dup = batch(&[1, 1, 2, 3], &["a", "a", "b", "c"]);
    let d_dup = result_digest(
        &dup.schema(),
        std::slice::from_ref(&dup),
        ResultSemantics::Multiset,
    )
    .unwrap();
    assert_ne!(da, d_dup, "a duplicated row changes the multiset");
}

#[test]
fn multiset_digest_still_detects_a_changed_value() {
    let a = batch(&[1, 2, 3], &["a", "b", "c"]);
    let changed = batch(&[1, 2, 9], &["a", "b", "c"]);
    let da = result_digest(
        &a.schema(),
        std::slice::from_ref(&a),
        ResultSemantics::Multiset,
    )
    .unwrap();
    let dc = result_digest(
        &changed.schema(),
        std::slice::from_ref(&changed),
        ResultSemantics::Multiset,
    )
    .unwrap();
    assert_ne!(da, dc);
}

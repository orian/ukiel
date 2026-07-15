//! The properties the fingerprint must have, and a golden vector that pins the
//! encoding.
//!
//! If the golden vector changes, the canonical encoding changed, and
//! `ROW_MULTISET_VERSION` must change with it — a fixture fingerprinted under the old
//! encoding would no longer verify under the new one, and that must be a loud,
//! deliberate event rather than a silent drift.

use std::sync::Arc;

use arrow::array::{BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use prod_synth_integrity::{ROW_MULTISET_VERSION, RowMultiset};

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("team_id", DataType::Int64, false),
        Field::new("amount", DataType::Float64, true),
        Field::new("event", DataType::Utf8, true),
        Field::new("flag", DataType::Boolean, true),
    ]))
}

fn batch(
    ids: &[i64],
    amounts: &[Option<f64>],
    events: &[Option<&str>],
    flags: &[Option<bool>],
) -> RecordBatch {
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(ids.to_vec())),
            Arc::new(Float64Array::from(amounts.to_vec())),
            Arc::new(StringArray::from(events.to_vec())),
            Arc::new(BooleanArray::from(flags.to_vec())),
        ],
    )
    .unwrap()
}

fn fingerprint(b: &RecordBatch) -> RowMultiset {
    let mut m = RowMultiset::default();
    m.update(b).expect("supported types");
    m
}

/// The golden vector. Three fixed rows, and the digest they must always produce.
///
/// This is the anchor. Every other property test proves a *relationship*; this proves
/// the absolute bytes, so a change anywhere in the encoding path is caught even if it
/// happens to preserve the relationships.
#[test]
fn golden_vector_pins_the_encoding() {
    let b = batch(
        &[1, 2, 3],
        &[Some(1.5), None, Some(-0.0)],
        &[Some("pageview"), Some(""), None],
        &[Some(true), None, Some(false)],
    );
    let m = fingerprint(&b);

    assert_eq!(m.count, 3);
    // If either of these fails, the canonical encoding has changed. Update the vector
    // AND bump ROW_MULTISET_VERSION in the same commit, or every prior fixture's
    // recorded fingerprint becomes a lie.
    assert_eq!(
        m.digest_hex(),
        "4f6c8dc450eb1791a9b8d2d2dd24f9b89045a578a4095b378c9c845d8bef0551",
        "golden digest drift: bump ROW_MULTISET_VERSION"
    );
    assert_eq!(ROW_MULTISET_VERSION, "row-multiset/v1");
}

/// Order-independent: the same rows in any order fingerprint equal. This is the
/// property that lets L0 files, compacted parts, and the source artifact — which list
/// the same rows in three different orders — all agree.
#[test]
fn row_order_does_not_change_the_fingerprint() {
    let a = batch(
        &[1, 2, 3],
        &[Some(1.0), Some(2.0), Some(3.0)],
        &[Some("a"), Some("b"), Some("c")],
        &[Some(true), Some(false), Some(true)],
    );
    let b = batch(
        &[3, 1, 2],
        &[Some(3.0), Some(1.0), Some(2.0)],
        &[Some("c"), Some("a"), Some("b")],
        &[Some(true), Some(true), Some(false)],
    );
    assert_eq!(fingerprint(&a), fingerprint(&b));
}

/// Batch boundaries do not matter: folding one batch of N equals folding N batches of
/// one, which is the same thing the merge homomorphism guarantees.
#[test]
fn batching_does_not_change_the_fingerprint() {
    let whole = batch(
        &[10, 20, 30, 40],
        &[None, None, None, None],
        &[Some("w"), Some("x"), Some("y"), Some("z")],
        &[None, None, None, None],
    );
    let one = fingerprint(&whole);

    let mut split = RowMultiset::default();
    for i in 0..4 {
        let part = batch(
            &[[10, 20, 30, 40][i]],
            &[None],
            &[Some(["w", "x", "y", "z"][i])],
            &[None],
        );
        let mut m = RowMultiset::default();
        m.update(&part).unwrap();
        split.merge(&m);
    }
    assert_eq!(one, split, "fold-then-merge must equal fold-whole");
}

/// A duplicated row changes the result. XOR alone would cancel the pair; the wrapping
/// sum and the count are what catch it. This is the failure a plain checksum misses.
#[test]
fn a_duplicated_row_is_detected() {
    let once = batch(&[7], &[Some(1.0)], &[Some("k")], &[Some(true)]);
    let twice = batch(
        &[7, 7],
        &[Some(1.0), Some(1.0)],
        &[Some("k"), Some("k")],
        &[Some(true), Some(true)],
    );
    assert_ne!(
        fingerprint(&once),
        fingerprint(&twice),
        "duplication must show"
    );

    // And a lost-one/gained-one swap that keeps the count is caught too.
    let dropped_a = batch(
        &[7, 7, 8],
        &[Some(1.0), Some(1.0), Some(2.0)],
        &[Some("k"), Some("k"), Some("m")],
        &[Some(true), Some(true), Some(false)],
    );
    let dropped_b = batch(
        &[7, 8, 8],
        &[Some(1.0), Some(2.0), Some(2.0)],
        &[Some("k"), Some("m"), Some("m")],
        &[Some(true), Some(false), Some(false)],
    );
    assert_ne!(
        fingerprint(&dropped_a),
        fingerprint(&dropped_b),
        "same count, different multiset — must differ"
    );
}

/// One changed value in one row changes the fingerprint.
#[test]
fn a_single_changed_value_is_detected() {
    let a = batch(
        &[1, 2],
        &[Some(1.0), Some(2.0)],
        &[Some("a"), Some("b")],
        &[Some(true), Some(false)],
    );
    let b = batch(
        &[1, 2],
        &[Some(1.0), Some(2.000001)],
        &[Some("a"), Some("b")],
        &[Some(true), Some(false)],
    );
    assert_ne!(
        fingerprint(&a),
        fingerprint(&b),
        "a float bit flip must show"
    );

    let c = batch(
        &[1, 2],
        &[Some(1.0), Some(2.0)],
        &[Some("a"), Some("B")],
        &[Some(true), Some(false)],
    );
    assert_ne!(
        fingerprint(&a),
        fingerprint(&c),
        "a string change must show"
    );
}

/// Null is distinct from every present value, including the empty string and zero.
#[test]
fn null_is_distinct_from_empty_and_zero() {
    let null_str = batch(&[1], &[Some(0.0)], &[None], &[None]);
    let empty_str = batch(&[1], &[Some(0.0)], &[Some("")], &[None]);
    assert_ne!(
        fingerprint(&null_str),
        fingerprint(&empty_str),
        "NULL != \"\""
    );

    let null_amt = batch(&[1], &[None], &[Some("x")], &[None]);
    let zero_amt = batch(&[1], &[Some(0.0)], &[Some("x")], &[None]);
    assert_ne!(
        fingerprint(&null_amt),
        fingerprint(&zero_amt),
        "NULL != 0.0"
    );
}

/// The type tag distinguishes values that would share bytes across columns: adjacent
/// string columns cannot be confused because they are length-delimited, and an int is
/// not its float or its decimal string.
#[test]
fn adjacent_strings_cannot_be_confused() {
    let s = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Utf8, false),
        Field::new("b", DataType::Utf8, false),
    ]));
    let ab_c = RecordBatch::try_new(
        s.clone(),
        vec![
            Arc::new(StringArray::from(vec!["ab"])),
            Arc::new(StringArray::from(vec!["c"])),
        ],
    )
    .unwrap();
    let a_bc = RecordBatch::try_new(
        s,
        vec![
            Arc::new(StringArray::from(vec!["a"])),
            Arc::new(StringArray::from(vec!["bc"])),
        ],
    )
    .unwrap();

    let mut m1 = RowMultiset::default();
    m1.update(&ab_c).unwrap();
    let mut m2 = RowMultiset::default();
    m2.update(&a_bc).unwrap();
    assert_ne!(
        m1, m2,
        "length delimiters must keep ('ab','c') != ('a','bc')"
    );
}

/// `Utf8` and `Utf8View` encode identically — the query path reads strings as views
/// (plan 39), and a fixture must fingerprint the same whichever representation a reader
/// happens to hand back.
#[test]
fn utf8_and_utf8_view_fingerprint_identically() {
    let plain = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, true)]));
    let view = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8View, true)]));

    let b_plain = RecordBatch::try_new(
        plain,
        vec![Arc::new(StringArray::from(vec![
            Some("hello"),
            None,
            Some(""),
        ]))],
    )
    .unwrap();
    let b_view = RecordBatch::try_new(
        view,
        vec![Arc::new(arrow::array::StringViewArray::from(vec![
            Some("hello"),
            None,
            Some(""),
        ]))],
    )
    .unwrap();

    let mut m1 = RowMultiset::default();
    m1.update(&b_plain).unwrap();
    let mut m2 = RowMultiset::default();
    m2.update(&b_view).unwrap();
    assert_eq!(m1, m2, "the same declared type must fingerprint alike");
}

/// The empty multiset is the identity for merge.
#[test]
fn empty_is_the_merge_identity() {
    let b = batch(
        &[1, 2],
        &[None, None],
        &[Some("x"), Some("y")],
        &[None, None],
    );
    let m = fingerprint(&b);

    let mut with_empty = m.clone();
    with_empty.merge(&RowMultiset::default());
    assert_eq!(m, with_empty);

    let mut empty = RowMultiset::default();
    empty.merge(&m);
    assert_eq!(m, empty);
}

/// An unsupported column type is a loud error, not a silently-skipped column — a
/// dropped column would make two different tables fingerprint equal.
#[test]
fn an_unsupported_type_is_refused() {
    let s = Arc::new(Schema::new(vec![Field::new("d", DataType::Date32, false)]));
    let b = RecordBatch::try_new(
        s,
        vec![Arc::new(arrow::array::Date32Array::from(vec![1, 2]))],
    )
    .unwrap();
    let mut m = RowMultiset::default();
    let e = m
        .update(&b)
        .expect_err("Date32 is not in the closed encoding");
    assert!(e.to_string().contains("Date32"), "{e}");
}

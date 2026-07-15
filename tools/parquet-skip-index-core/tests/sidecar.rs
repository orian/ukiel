//! The sidecar dispatch: a valid payload evaluates through its kind; a digest mismatch or a
//! corrupt payload abstains (Unknown = keep), never a skip.

use parquet_lab_contract::IndexKind;
use parquet_skip_index_core::value_set::ValueSet;
use parquet_skip_index_core::{Decision, Predicate, Value, evaluate, payload_digest};

#[test]
fn evaluate_dispatches_and_fails_open_on_corruption() {
    let vs = ValueSet::build(&[Some(Value::Int(1)), Some(Value::Int(2))], 4096).unwrap();
    let payload = vs.encode();
    let digest = payload_digest(&payload);

    // A value in the set: Maybe. A value absent: NoMatch.
    assert_eq!(
        evaluate(
            IndexKind::ValueSet,
            &payload,
            &digest,
            &Predicate::Eq(Value::Int(2))
        ),
        Decision::Maybe
    );
    assert_eq!(
        evaluate(
            IndexKind::ValueSet,
            &payload,
            &digest,
            &Predicate::Eq(Value::Int(9))
        ),
        Decision::NoMatch
    );

    // A wrong recorded digest: Unknown (never trust the payload).
    assert_eq!(
        evaluate(
            IndexKind::ValueSet,
            &payload,
            &"00".repeat(32),
            &Predicate::Eq(Value::Int(9))
        ),
        Decision::Unknown
    );

    // A payload corrupted on disk, checked against the manifest's ORIGINAL digest: the
    // mismatch abstains before decode is even attempted.
    let mut bad = payload.clone();
    *bad.last_mut().unwrap() ^= 0xff;
    assert_eq!(
        evaluate(
            IndexKind::ValueSet,
            &bad,
            &digest,
            &Predicate::Eq(Value::Int(9))
        ),
        Decision::Unknown
    );

    // A truncated payload (with its own matching digest) fails to decode -> Unknown.
    let trunc = &payload[..payload.len() / 2];
    assert_eq!(
        evaluate(
            IndexKind::ValueSet,
            trunc,
            &payload_digest(trunc),
            &Predicate::Eq(Value::Int(9))
        ),
        Decision::Unknown
    );
}

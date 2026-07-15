//! Decode one row group's payload and evaluate a predicate against it.
//!
//! This is the single dispatch point both the builder and the benchmark use, so the
//! conservative rule — any doubt is `Unknown` (keep), never a skip — lives once. A corrupt
//! or truncated payload decodes to nothing and therefore abstains; it can never become a
//! `NoMatch`.

use parquet_lab_contract::IndexKind;

use crate::prefix_set::PrefixSet;
use crate::value_set::ValueSet;
use crate::zone_map::ZoneMap;
use crate::{Decision, Predicate, payload_digest};

/// Verify a payload against its recorded digest, then decode and evaluate. A digest mismatch
/// or a decode failure is `Unknown` (keep) — never a skip.
pub fn evaluate(
    kind: IndexKind,
    payload: &[u8],
    expected_digest: &str,
    pred: &Predicate,
) -> Decision {
    if payload_digest(payload) != expected_digest {
        return Decision::Unknown;
    }
    match kind {
        IndexKind::ZoneMap => ZoneMap::decode(payload).map(|z| z.evaluate(pred)),
        IndexKind::ValueSet => ValueSet::decode(payload).map(|v| v.evaluate(pred)),
        IndexKind::PrefixSet => PrefixSet::decode(payload).map(|p| p.evaluate(pred)),
    }
    .unwrap_or(Decision::Unknown)
}

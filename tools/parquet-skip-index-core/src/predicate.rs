//! The laboratory's scalar value, predicate, and pruning-decision types.
//!
//! These are pure: they carry no Arrow and no Parquet. A builder produces `Value`s from a
//! row group; a benchmark produces a `Predicate` from a compiled probe's typed literals; the
//! index kinds turn `(Value bounds/set, Predicate)` into a conservative `Decision`.

/// A scalar value the indices reason about — signed integers and UTF-8 strings, the event
/// packing key, timestamps, and promoted string columns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Value {
    Int(i64),
    Str(Vec<u8>),
}

/// A predicate a sidecar can be asked about. Anything outside this closed set is
/// [`Decision::Unknown`].
#[derive(Debug, Clone)]
pub enum Predicate {
    /// `col = v`.
    Eq(Value),
    /// `col IN (..)`.
    In(Vec<Value>),
    /// `lo <= col <= hi` (inclusive).
    Range(Value, Value),
    /// `col LIKE 'literal%'` — the literal prefix bytes.
    Prefix(Vec<u8>),
}

/// The verdict for one row group. `NoMatch` is the only value that permits a skip, and only
/// the index kinds prove it can never hide a matching row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    /// The row group provably holds no matching row — safe to skip.
    NoMatch,
    /// The row group may hold a matching row — must be kept.
    Maybe,
    /// The index could not decide (unsupported predicate, missing/corrupt payload, budget
    /// exceeded) — keep, exactly as `Maybe`, but recorded distinctly so a report can price
    /// how often the index abstained.
    Unknown,
}

impl Decision {
    /// Whether this verdict permits skipping the row group. Only `NoMatch` does.
    pub fn skippable(self) -> bool {
        matches!(self, Decision::NoMatch)
    }
}

fn hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

/// The digest of a payload slice, for the manifest and the corruption check.
pub fn payload_digest(bytes: &[u8]) -> String {
    hex(blake3::hash(bytes).as_bytes())
}

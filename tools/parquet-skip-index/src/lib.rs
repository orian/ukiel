//! `parquet-skip-index` — build conservative experimental skip-index sidecars.
//!
//! A sidecar prices *custom* pruning against native Parquet statistics and Bloom filters.
//! It is bound to an exact variant manifest digest and every file digest; any unknown
//! version, schema mismatch, file mismatch, missing row group, corrupt payload, or
//! unsupported predicate resolves to [`Decision::Unknown`] (keep), never a skip. A
//! false-negative skip would silently drop matching rows — the one failure a laboratory
//! measuring pruning can never tolerate.
//!
//! This is not a proposed production format. Its whole job is to make the build time,
//! bytes, metadata requests, and pruning of a candidate index *measurable* before deciding
//! whether a real implementation belongs in Parquet metadata, an object sidecar, or the
//! catalog.

pub mod builder;
pub mod prefix_set;
pub mod value_set;
pub mod zone_map;

/// A scalar value the indices reason about. The laboratory covers signed integers and
/// UTF-8 strings — the event packing key, timestamps, and promoted string columns.
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

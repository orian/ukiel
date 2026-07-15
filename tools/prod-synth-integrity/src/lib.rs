//! `row-multiset/v1` — an order-independent fingerprint over a set of rows.
//!
//! # What it is for
//!
//! Plan 46 stages `prod-synth` rows as L0 Parquet, loads them, and runs the real
//! compactor until the fixture is one final run per partition. Compaction rewrites
//! every file, every object path, and every part grouping. The one thing it must
//! **not** change is the rows themselves: the multiset of physical row values after
//! compaction has to equal the multiset that went in, or the experiment is measuring
//! a corruption rather than a shape.
//!
//! A row *count* cannot prove that — a merge that dropped one row and duplicated
//! another keeps the count. So this hashes the rows into an aggregate that is:
//!
//! * **order-independent** — L0 files, compacted parts, and the source artifact list
//!   the same rows in different orders, and all three must fingerprint equal;
//! * **duplicate-sensitive** — a row appearing twice where it appeared once must
//!   change the result (plain XOR would cancel the pair; the wrapping sum does not);
//! * **change-sensitive** — one flipped byte in one value changes the aggregate; and
//! * **mergeable** — two accumulators over disjoint row sets combine into the
//!   accumulator over their union, so the runner can fold batch by batch and shard by
//!   shard without holding all rows at once.
//!
//! # How
//!
//! Each row is canonically encoded (schema order; a type tag, a null marker, and
//! length-delimited little-endian value bytes per column) and BLAKE3-hashed to a
//! 256-bit row digest. The digests are folded into three accumulators — a count, a
//! 256-bit XOR, and four 64-bit wrapping-sum lanes. The triple
//! `(count, xor, sum)` is a homomorphic multiset hash: XOR alone cancels even
//! duplicates, the wrapping sum alone is weak against transpositions, and the count
//! pins the cardinality; together they detect a changed, lost, or duplicated row
//! with negligible collision risk.
//!
//! # What it is NOT
//!
//! Not a cryptographic commitment (an adversary with the encoding could construct a
//! collision), not a database page checksum, and not a stable on-disk format for
//! anyone outside this experiment. It is an integrity guard for a benchmark, versioned
//! so a change to the encoding is a change to [`ROW_MULTISET_VERSION`].

use arrow::array::{Array, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::DataType;
use serde::{Deserialize, Serialize};

/// The encoding version. Any change to the canonical row bytes, the accumulators, or
/// the type tags is a new version — the golden vectors in `tests/fingerprint.rs` are
/// what make that non-negotiable.
pub const ROW_MULTISET_VERSION: &str = "row-multiset/v1";

/// Type tags, one leading byte per column value. A tag distinguishes an integer 0
/// from a float 0.0 from the string "0", none of which are the same value.
mod tag {
    pub const NULL: u8 = 0;
    pub const INT64: u8 = 1;
    pub const FLOAT64: u8 = 2;
    pub const UTF8: u8 = 3;
    pub const BOOL: u8 = 4;
}

#[derive(Debug, Clone)]
pub enum FingerprintError {
    /// A column type the canonical encoding does not cover. Deliberately closed: a
    /// silently-skipped column would make two different tables fingerprint equal.
    UnsupportedType { column: String, data_type: String },
}

impl std::fmt::Display for FingerprintError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FingerprintError::UnsupportedType { column, data_type } => write!(
                f,
                "row-multiset fingerprint does not encode column '{column}' of type {data_type}; \
                 the encoding is closed so a new type cannot be silently dropped"
            ),
        }
    }
}

impl std::error::Error for FingerprintError {}

/// The order-independent aggregate over a set of rows.
///
/// `Default` is the empty multiset. Fold rows in with [`RowMultiset::update`], combine
/// disjoint sets with [`RowMultiset::merge`], and read the result with
/// [`RowMultiset::digest`] — or compare two directly, which is what every caller
/// actually wants.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RowMultiset {
    /// Number of rows folded in. Pins the cardinality, so a lost or added row shows.
    pub count: u64,
    /// XOR of every row digest. Catches any value that appears an odd number of times
    /// more than it should.
    pub xor: [u8; 32],
    /// Four 64-bit wrapping sums over the row digest's four little-endian lanes.
    /// Catches even-count duplication, which XOR cancels.
    pub sum: [u64; 4],
}

impl Default for RowMultiset {
    fn default() -> Self {
        RowMultiset {
            count: 0,
            xor: [0u8; 32],
            sum: [0u64; 4],
        }
    }
}

impl RowMultiset {
    /// Fold one batch's rows in.
    pub fn update(&mut self, batch: &RecordBatch) -> Result<(), FingerprintError> {
        let encoders = column_encoders(batch)?;
        let mut buf = Vec::with_capacity(256);
        for row in 0..batch.num_rows() {
            buf.clear();
            for enc in &encoders {
                enc(row, &mut buf);
            }
            self.fold(blake3::hash(&buf).into());
        }
        Ok(())
    }

    /// Combine two accumulators over disjoint row sets into the accumulator over their
    /// union. The homomorphism is the whole point: fold shards independently, merge.
    pub fn merge(&mut self, other: &RowMultiset) {
        self.count = self.count.wrapping_add(other.count);
        for (a, b) in self.xor.iter_mut().zip(&other.xor) {
            *a ^= *b;
        }
        for (a, b) in self.sum.iter_mut().zip(&other.sum) {
            *a = a.wrapping_add(*b);
        }
    }

    fn fold(&mut self, digest: [u8; 32]) {
        self.count = self.count.wrapping_add(1);
        for (a, b) in self.xor.iter_mut().zip(&digest) {
            *a ^= *b;
        }
        for (lane, chunk) in self.sum.iter_mut().zip(digest.chunks_exact(8)) {
            let v = u64::from_le_bytes(chunk.try_into().expect("8 bytes"));
            *lane = lane.wrapping_add(v);
        }
    }

    /// A single 32-byte digest of the whole accumulator, for a manifest field or a
    /// golden vector. Two multisets are equal iff their digests are — the digest is a
    /// convenience, `==` on the struct is the same test.
    pub fn digest(&self) -> [u8; 32] {
        let mut h = blake3::Hasher::new();
        h.update(ROW_MULTISET_VERSION.as_bytes());
        h.update(&self.count.to_le_bytes());
        h.update(&self.xor);
        for lane in &self.sum {
            h.update(&lane.to_le_bytes());
        }
        h.finalize().into()
    }

    /// Lowercase-hex of [`RowMultiset::digest`].
    pub fn digest_hex(&self) -> String {
        let d = self.digest();
        let mut s = String::with_capacity(64);
        for b in d {
            s.push_str(&format!("{b:02x}"));
        }
        s
    }
}

/// A per-column closure that appends one row's canonical bytes. Built once per batch
/// (the downcasts happen here, not per row), so folding a batch is a tight loop.
type ColumnEncoder<'a> = Box<dyn Fn(usize, &mut Vec<u8>) + 'a>;

fn column_encoders(batch: &RecordBatch) -> Result<Vec<ColumnEncoder<'_>>, FingerprintError> {
    let schema = batch.schema();
    let mut encoders: Vec<ColumnEncoder<'_>> = Vec::with_capacity(batch.num_columns());

    for (i, field) in schema.fields().iter().enumerate() {
        let col = batch.column(i);
        // `timestamp_ms` is physically Int64 in Ukiel; there is no separate Arrow
        // timestamp column to handle. The tag is the physical type, which is what the
        // bytes on disk are.
        let enc: ColumnEncoder = match field.data_type() {
            DataType::Int64 => {
                let a = col
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("int64")
                    .clone();
                Box::new(move |row, buf| {
                    if a.is_null(row) {
                        buf.push(tag::NULL);
                    } else {
                        buf.push(tag::INT64);
                        buf.extend_from_slice(&a.value(row).to_le_bytes());
                    }
                })
            }
            DataType::Float64 => {
                let a = col
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .expect("f64")
                    .clone();
                Box::new(move |row, buf| {
                    if a.is_null(row) {
                        buf.push(tag::NULL);
                    } else {
                        buf.push(tag::FLOAT64);
                        // Canonicalize the zero sign so -0.0 and 0.0 encode alike;
                        // NaN bit patterns are left as-is (a NaN is not equal to a NaN
                        // anyway, and the generator never produces one).
                        let v = a.value(row);
                        let v = if v == 0.0 { 0.0 } else { v };
                        buf.extend_from_slice(&v.to_le_bytes());
                    }
                })
            }
            DataType::Utf8 => {
                let a = col
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("utf8")
                    .clone();
                Box::new(move |row, buf| encode_str(a.is_null(row), a.value(row), buf))
            }
            DataType::Utf8View => {
                let a = col
                    .as_any()
                    .downcast_ref::<arrow::array::StringViewArray>()
                    .expect("utf8view")
                    .clone();
                // Utf8 and Utf8View are the same declared type in different in-memory
                // shapes (plan 39). They MUST encode identically, or a fixture read as
                // views and the same fixture read as strings would fingerprint apart.
                Box::new(move |row, buf| encode_str(a.is_null(row), a.value(row), buf))
            }
            DataType::Boolean => {
                let a = col
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .expect("bool")
                    .clone();
                Box::new(move |row, buf| {
                    if a.is_null(row) {
                        buf.push(tag::NULL);
                    } else {
                        buf.push(tag::BOOL);
                        buf.push(a.value(row) as u8);
                    }
                })
            }
            other => {
                return Err(FingerprintError::UnsupportedType {
                    column: field.name().clone(),
                    data_type: format!("{other:?}"),
                });
            }
        };
        encoders.push(enc);
    }
    Ok(encoders)
}

/// A string value, length-delimited. The length prevents `("ab","c")` and `("a","bc")`
/// encoding to the same bytes — without it, adjacent string columns would be
/// indistinguishable and two different rows could collide by construction.
fn encode_str(is_null: bool, value: &str, buf: &mut Vec<u8>) {
    if is_null {
        buf.push(tag::NULL);
        return;
    }
    buf.push(tag::UTF8);
    buf.extend_from_slice(&(value.len() as u64).to_le_bytes());
    buf.extend_from_slice(value.as_bytes());
}

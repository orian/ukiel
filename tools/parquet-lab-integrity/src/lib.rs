//! `logical-row-multiset/v1` — an order-independent fingerprint over the *declared
//! logical values* of a set of rows, regardless of their physical representation.
//!
//! # Why a second fingerprint
//!
//! Plan 46's physical `row-multiset/v1` tags each column by its Arrow type and hashes
//! the bytes on disk. That is exactly right for compaction integrity: if `Int64`
//! silently became `Int32`, the physical fingerprint *must* notice. But plan 47's
//! type experiment does the opposite — it deliberately rewrites `Int64` as `Int32`
//! and needs to prove the *logical values are unchanged*. The physical fingerprint
//! would (correctly, for its purpose) report a difference; conflating the two would
//! either weaken plan 46 or make plan 47 impossible.
//!
//! So this hashes each column under the manifest's **declared logical type**, not its
//! physical Arrow shape:
//!
//! * signed integers normalize losslessly to signed 128-bit canonical bytes, so an
//!   `Int8`, `Int16`, `Int32`, and `Int64` column with equal values fingerprint alike;
//! * timestamps normalize to epoch milliseconds, and only when the physical unit is
//!   exactly convertible without rounding;
//! * dates normalize to epoch days;
//! * `Utf8`, `Utf8View`, and `LargeUtf8` normalize to the same length-delimited bytes;
//! * booleans are one byte; floats preserve canonical `Float64` bits with
//!   `-0.0 == 0.0` and reject any `Float32` promotion; and
//! * null stays distinct from every value.
//!
//! Like `row-multiset/v1`, it carries `(count, xor, sum)` over BLAKE3 row hashes and
//! is order-independent, duplicate-sensitive, and mergeable. Unsupported or lossy
//! casts fail closed; a declared column can never be silently omitted. Golden vectors
//! in `tests/golden.rs` pin the format — any change to the canonical bytes is a change
//! to [`LOGICAL_ROW_MULTISET_VERSION`].

use arrow::array::{
    Array, BooleanArray, Date32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
    LargeStringArray, RecordBatch, StringArray, StringViewArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
};
use arrow::datatypes::{DataType, TimeUnit};

/// The encoding version. Any change to the canonical bytes, accumulators, or tags is
/// a new version — the golden vectors are what make that non-negotiable.
pub const LOGICAL_ROW_MULTISET_VERSION: &str = "logical-row-multiset/v1";

/// The declared logical type of a column, independent of its physical Arrow shape.
///
/// The snapshot manifest declares one of these per column; the fingerprint accepts any
/// physical representation that maps to it losslessly and refuses any that does not.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogicalType {
    /// A signed integer of any width; canonicalized to 128-bit signed bytes.
    SignedInt,
    /// A millisecond-epoch timestamp; physical `Int64` ms or an exactly-convertible
    /// Arrow `Timestamp`.
    TimestampMillis,
    /// A day-epoch date; physical integer days or Arrow `Date32`.
    DateDays,
    /// Unicode text in any Arrow string representation.
    Utf8,
    /// A boolean.
    Boolean,
    /// A 64-bit float; physical `Float64` only, no `Float32` promotion.
    Float64,
}

impl LogicalType {
    /// Parse a declared logical type name as it appears in a snapshot manifest.
    pub fn parse(name: &str) -> Result<Self, LogicalFingerprintError> {
        match name {
            "int8" | "int16" | "int32" | "int64" | "signed_int" => Ok(LogicalType::SignedInt),
            "timestamp_millis" | "timestamp_ms" => Ok(LogicalType::TimestampMillis),
            "date" | "date_days" | "date32" => Ok(LogicalType::DateDays),
            "utf8" | "string" => Ok(LogicalType::Utf8),
            "boolean" | "bool" => Ok(LogicalType::Boolean),
            "float64" | "double" => Ok(LogicalType::Float64),
            other => Err(LogicalFingerprintError::UnknownLogicalType {
                spec: other.to_string(),
            }),
        }
    }
}

/// A column's declared name and logical type. Order is significant — the fingerprint
/// encodes columns in schema order, so `(a, b)` and `(b, a)` are different rows.
#[derive(Debug, Clone)]
pub struct LogicalColumn {
    pub name: String,
    pub logical: LogicalType,
}

/// The declared logical schema the fingerprint encodes under.
#[derive(Debug, Clone)]
pub struct LogicalSchema {
    pub columns: Vec<LogicalColumn>,
}

impl LogicalSchema {
    pub fn new(columns: Vec<LogicalColumn>) -> Self {
        LogicalSchema { columns }
    }
}

#[derive(Debug, Clone)]
pub enum LogicalFingerprintError {
    /// The declared schema does not line up with the batch: a declared column is
    /// missing, an extra physical column is present, or a name/order disagrees.
    /// Deliberately closed — a mismatch could let a dropped column pass unnoticed.
    SchemaMismatch { expected: String, found: String },
    /// A physical type that cannot represent the declared logical type without loss.
    UnsupportedCast {
        column: String,
        logical: String,
        physical: String,
    },
    /// A conversion that would round (e.g. microsecond timestamps not divisible into
    /// milliseconds). Refused so the fingerprint never blesses a lossy value.
    LossyCast { column: String, detail: String },
    /// A declared logical type name the parser does not recognise.
    UnknownLogicalType { spec: String },
}

impl std::fmt::Display for LogicalFingerprintError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LogicalFingerprintError::SchemaMismatch { expected, found } => write!(
                f,
                "logical fingerprint schema mismatch: declared [{expected}] but batch has [{found}]"
            ),
            LogicalFingerprintError::UnsupportedCast {
                column,
                logical,
                physical,
            } => write!(
                f,
                "column '{column}' declared logical {logical} cannot be encoded from physical \
                 {physical} without loss; the encoding is closed"
            ),
            LogicalFingerprintError::LossyCast { column, detail } => {
                write!(f, "column '{column}': lossy conversion refused — {detail}")
            }
            LogicalFingerprintError::UnknownLogicalType { spec } => {
                write!(f, "unknown declared logical type '{spec}'")
            }
        }
    }
}

impl std::error::Error for LogicalFingerprintError {}

/// Type tags, one leading byte per column value. A tag distinguishes an integer 0
/// from a timestamp 0 from a date 0 from the string "0", none of which are equal.
mod tag {
    pub const NULL: u8 = 0;
    pub const SIGNED_INT: u8 = 1;
    pub const TIMESTAMP_MS: u8 = 2;
    pub const DATE_DAYS: u8 = 3;
    pub const UTF8: u8 = 4;
    pub const BOOL: u8 = 5;
    pub const FLOAT64: u8 = 6;
}

/// The order-independent aggregate over the logical values of a set of rows.
///
/// `Default` is the empty multiset. Fold rows in with [`LogicalRowMultiset::update`],
/// combine disjoint sets with [`LogicalRowMultiset::merge`], and read the result with
/// [`LogicalRowMultiset::digest`] — or compare two directly.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LogicalRowMultiset {
    pub count: u64,
    pub xor: [u8; 32],
    pub sum: [u64; 4],
}

impl LogicalRowMultiset {
    /// Fold one batch's rows in, encoded under the declared logical schema.
    pub fn update(
        &mut self,
        batch: &RecordBatch,
        schema: &LogicalSchema,
    ) -> Result<(), LogicalFingerprintError> {
        let encoders = column_encoders(batch, schema)?;
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

    /// Combine two accumulators over disjoint row sets into their union.
    pub fn merge(&mut self, other: &LogicalRowMultiset) {
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
        for (lane, chunk) in self.sum.iter_mut().zip(digest.as_chunks::<8>().0) {
            let v = u64::from_le_bytes(*chunk);
            *lane = lane.wrapping_add(v);
        }
    }

    /// A single 32-byte digest of the whole accumulator, for a manifest field or a
    /// golden vector. Two multisets are equal iff their digests are.
    pub fn digest(&self) -> [u8; 32] {
        let mut h = blake3::Hasher::new();
        h.update(LOGICAL_ROW_MULTISET_VERSION.as_bytes());
        h.update(&self.count.to_le_bytes());
        h.update(&self.xor);
        for lane in &self.sum {
            h.update(&lane.to_le_bytes());
        }
        h.finalize().into()
    }

    /// Lowercase-hex of [`LogicalRowMultiset::digest`].
    pub fn digest_hex(&self) -> String {
        hex(&self.digest())
    }

    /// Lowercase-hex of the raw 32-byte XOR accumulator, for the contract mirror.
    pub fn xor_hex(&self) -> String {
        hex(&self.xor)
    }
}

fn hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

/// A per-column closure that appends one row's canonical bytes. Built once per batch
/// (the downcasts and lossless-conversion checks happen here, not per row).
type ColumnEncoder<'a> = Box<dyn Fn(usize, &mut Vec<u8>) + 'a>;

fn column_encoders<'a>(
    batch: &'a RecordBatch,
    schema: &LogicalSchema,
) -> Result<Vec<ColumnEncoder<'a>>, LogicalFingerprintError> {
    let arrow_schema = batch.schema();
    if arrow_schema.fields().len() != schema.columns.len() {
        return Err(schema_mismatch(&arrow_schema, schema));
    }
    let mut encoders: Vec<ColumnEncoder<'a>> = Vec::with_capacity(schema.columns.len());
    for (i, decl) in schema.columns.iter().enumerate() {
        let field = arrow_schema.field(i);
        if field.name() != &decl.name {
            return Err(schema_mismatch(&arrow_schema, schema));
        }
        let col = batch.column(i);
        encoders.push(encoder_for(&decl.name, decl.logical, col.as_ref())?);
    }
    Ok(encoders)
}

fn schema_mismatch(
    arrow_schema: &arrow::datatypes::Schema,
    schema: &LogicalSchema,
) -> LogicalFingerprintError {
    LogicalFingerprintError::SchemaMismatch {
        expected: schema
            .columns
            .iter()
            .map(|c| c.name.as_str())
            .collect::<Vec<_>>()
            .join(", "),
        found: arrow_schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect::<Vec<_>>()
            .join(", "),
    }
}

fn encoder_for<'a>(
    name: &str,
    logical: LogicalType,
    col: &'a dyn Array,
) -> Result<ColumnEncoder<'a>, LogicalFingerprintError> {
    match logical {
        LogicalType::SignedInt => signed_int_encoder(name, col),
        LogicalType::TimestampMillis => timestamp_ms_encoder(name, col),
        LogicalType::DateDays => date_days_encoder(name, col),
        LogicalType::Utf8 => utf8_encoder(name, col),
        LogicalType::Boolean => boolean_encoder(name, col),
        LogicalType::Float64 => float64_encoder(name, col),
    }
}

/// Emit a signed 128-bit canonical value, so every physical integer width collapses.
fn emit_signed(buf: &mut Vec<u8>, is_null: bool, v: i128) {
    if is_null {
        buf.push(tag::NULL);
    } else {
        buf.push(tag::SIGNED_INT);
        buf.extend_from_slice(&v.to_le_bytes());
    }
}

fn signed_int_encoder<'a>(
    name: &str,
    col: &'a dyn Array,
) -> Result<ColumnEncoder<'a>, LogicalFingerprintError> {
    macro_rules! int_enc {
        ($arr:ty) => {{
            let a = col.as_any().downcast_ref::<$arr>().expect("int");
            Ok(Box::new(move |row, buf: &mut Vec<u8>| {
                emit_signed(buf, a.is_null(row), a.value(row) as i128)
            }))
        }};
    }
    match col.data_type() {
        DataType::Int8 => int_enc!(Int8Array),
        DataType::Int16 => int_enc!(Int16Array),
        DataType::Int32 => int_enc!(Int32Array),
        DataType::Int64 => int_enc!(Int64Array),
        other => Err(unsupported(name, "signed_int", other)),
    }
}

fn timestamp_ms_encoder<'a>(
    name: &str,
    col: &'a dyn Array,
) -> Result<ColumnEncoder<'a>, LogicalFingerprintError> {
    fn emit(buf: &mut Vec<u8>, is_null: bool, ms: i64) {
        if is_null {
            buf.push(tag::NULL);
        } else {
            buf.push(tag::TIMESTAMP_MS);
            buf.extend_from_slice(&ms.to_le_bytes());
        }
    }
    match col.data_type() {
        // Physical Int64 is already milliseconds in Ukiel's declared contract.
        DataType::Int64 => {
            let a = col.as_any().downcast_ref::<Int64Array>().expect("i64");
            Ok(Box::new(move |row, buf| {
                emit(buf, a.is_null(row), a.value(row))
            }))
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let a = col
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .expect("ts_ms");
            Ok(Box::new(move |row, buf| {
                emit(buf, a.is_null(row), a.value(row))
            }))
        }
        DataType::Timestamp(TimeUnit::Second, _) => {
            let a = col
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .expect("ts_s");
            // seconds -> ms is always exact.
            Ok(Box::new(move |row, buf| {
                emit(buf, a.is_null(row), a.value(row).wrapping_mul(1000))
            }))
        }
        // Micro/nanosecond timestamps only convert when exactly divisible; a lossy
        // value is caught per row and would surface as a fingerprint disagreement,
        // but here we reject the whole column build so the loss is loud, not hidden.
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let a = col
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .expect("ts_us");
            for row in 0..a.len() {
                if !a.is_null(row) && a.value(row) % 1000 != 0 {
                    return Err(lossy(name, "microsecond timestamp not divisible into ms"));
                }
            }
            Ok(Box::new(move |row, buf| {
                emit(buf, a.is_null(row), a.value(row) / 1000)
            }))
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let a = col
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .expect("ts_ns");
            for row in 0..a.len() {
                if !a.is_null(row) && a.value(row) % 1_000_000 != 0 {
                    return Err(lossy(name, "nanosecond timestamp not divisible into ms"));
                }
            }
            Ok(Box::new(move |row, buf| {
                emit(buf, a.is_null(row), a.value(row) / 1_000_000)
            }))
        }
        other => Err(unsupported(name, "timestamp_millis", other)),
    }
}

fn date_days_encoder<'a>(
    name: &str,
    col: &'a dyn Array,
) -> Result<ColumnEncoder<'a>, LogicalFingerprintError> {
    fn emit(buf: &mut Vec<u8>, is_null: bool, days: i64) {
        if is_null {
            buf.push(tag::NULL);
        } else {
            buf.push(tag::DATE_DAYS);
            buf.extend_from_slice(&days.to_le_bytes());
        }
    }
    match col.data_type() {
        DataType::Date32 => {
            let a = col.as_any().downcast_ref::<Date32Array>().expect("date32");
            Ok(Box::new(move |row, buf| {
                emit(buf, a.is_null(row), a.value(row) as i64)
            }))
        }
        DataType::Int32 => {
            let a = col.as_any().downcast_ref::<Int32Array>().expect("i32");
            Ok(Box::new(move |row, buf| {
                emit(buf, a.is_null(row), a.value(row) as i64)
            }))
        }
        DataType::Int64 => {
            let a = col.as_any().downcast_ref::<Int64Array>().expect("i64");
            Ok(Box::new(move |row, buf| {
                emit(buf, a.is_null(row), a.value(row))
            }))
        }
        other => Err(unsupported(name, "date_days", other)),
    }
}

fn utf8_encoder<'a>(
    name: &str,
    col: &'a dyn Array,
) -> Result<ColumnEncoder<'a>, LogicalFingerprintError> {
    match col.data_type() {
        DataType::Utf8 => {
            let a = col.as_any().downcast_ref::<StringArray>().expect("utf8");
            Ok(Box::new(move |row, buf| {
                encode_str(a.is_null(row), a.value(row), buf)
            }))
        }
        DataType::LargeUtf8 => {
            let a = col
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .expect("large_utf8");
            Ok(Box::new(move |row, buf| {
                encode_str(a.is_null(row), a.value(row), buf)
            }))
        }
        DataType::Utf8View => {
            let a = col
                .as_any()
                .downcast_ref::<StringViewArray>()
                .expect("utf8view");
            Ok(Box::new(move |row, buf| {
                encode_str(a.is_null(row), a.value(row), buf)
            }))
        }
        other => Err(unsupported(name, "utf8", other)),
    }
}

fn boolean_encoder<'a>(
    name: &str,
    col: &'a dyn Array,
) -> Result<ColumnEncoder<'a>, LogicalFingerprintError> {
    match col.data_type() {
        DataType::Boolean => {
            let a = col.as_any().downcast_ref::<BooleanArray>().expect("bool");
            Ok(Box::new(move |row, buf| {
                if a.is_null(row) {
                    buf.push(tag::NULL);
                } else {
                    buf.push(tag::BOOL);
                    buf.push(a.value(row) as u8);
                }
            }))
        }
        other => Err(unsupported(name, "boolean", other)),
    }
}

fn float64_encoder<'a>(
    name: &str,
    col: &'a dyn Array,
) -> Result<ColumnEncoder<'a>, LogicalFingerprintError> {
    match col.data_type() {
        // Float64 only: a Float32 physical column is rejected, never promoted. A
        // narrowing that lost precision must be caught, and blessing f32 here would
        // let it pass. The rewrite tool proves round-trip separately before it writes.
        DataType::Float64 => {
            let a = col.as_any().downcast_ref::<Float64Array>().expect("f64");
            Ok(Box::new(move |row, buf| {
                if a.is_null(row) {
                    buf.push(tag::NULL);
                } else {
                    buf.push(tag::FLOAT64);
                    let v = a.value(row);
                    let v = if v == 0.0 { 0.0 } else { v };
                    buf.extend_from_slice(&v.to_le_bytes());
                }
            }))
        }
        other => Err(unsupported(name, "float64", other)),
    }
}

/// A string value, length-delimited so adjacent string columns cannot alias.
fn encode_str(is_null: bool, value: &str, buf: &mut Vec<u8>) {
    if is_null {
        buf.push(tag::NULL);
        return;
    }
    buf.push(tag::UTF8);
    buf.extend_from_slice(&(value.len() as u64).to_le_bytes());
    buf.extend_from_slice(value.as_bytes());
}

fn unsupported(column: &str, logical: &str, physical: &DataType) -> LogicalFingerprintError {
    LogicalFingerprintError::UnsupportedCast {
        column: column.to_string(),
        logical: logical.to_string(),
        physical: format!("{physical:?}"),
    }
}

fn lossy(column: &str, detail: &str) -> LogicalFingerprintError {
    LogicalFingerprintError::LossyCast {
        column: column.to_string(),
        detail: detail.to_string(),
    }
}

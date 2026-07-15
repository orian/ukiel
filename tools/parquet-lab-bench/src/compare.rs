//! Result equivalence: a canonical, encoding-independent digest of a query's answer.
//!
//! Every timed query first passes exact schema/result equivalence — a physical variant is
//! only comparable to the control if it returns the identical logical answer. But two runs
//! can return the *same logical values* in different Arrow encodings: a high-cardinality
//! string group-by may come back dictionary-encoded from one artifact and plain `Utf8` (or
//! `Utf8View`) from another, purely because the input Parquet encodings differed. Those are
//! the same answer. So the digest canonicalizes each column — dictionaries decoded, string
//! views widened to `Utf8`, schema metadata and nullability stripped — before hashing, so
//! it compares logical content and never an incidental physical representation.

use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::array::RecordBatch;
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::StreamWriter;

/// Coerce a column to its canonical logical type, collapsing physical encodings.
fn canonical_type(dt: &DataType) -> DataType {
    match dt {
        DataType::Dictionary(_, value) => canonical_type(value),
        DataType::Utf8View | DataType::LargeUtf8 => DataType::Utf8,
        DataType::BinaryView | DataType::LargeBinary => DataType::Binary,
        other => other.clone(),
    }
}

/// Rebuild a batch under a canonical schema: each column cast to its canonical type, all
/// field metadata dropped, and nullability unified — so equal answers hash equal whatever
/// their incidental Arrow encoding.
fn canonicalize(batch: &RecordBatch) -> Result<(Arc<Schema>, RecordBatch)> {
    let mut fields = Vec::with_capacity(batch.num_columns());
    let mut columns = Vec::with_capacity(batch.num_columns());
    for (i, field) in batch.schema().fields().iter().enumerate() {
        let target = canonical_type(field.data_type());
        let col = batch.column(i);
        let col = if col.data_type() == &target {
            col.clone()
        } else {
            cast(col, &target).with_context(|| {
                format!("canonicalizing column '{}' to {target:?}", field.name())
            })?
        };
        fields.push(Field::new(field.name(), target, true));
        columns.push(col);
    }
    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(schema.clone(), columns)?;
    Ok((schema, batch))
}

pub use parquet_lab_contract::ResultSemantics;

/// A canonical digest of a query result under the declared answer semantics.
///
/// `Ordered` hashes the canonicalized Arrow rows *in result order* — for a scalar aggregate
/// or a query with a deterministic total `ORDER BY`. `Multiset` hashes the canonical rows
/// independent of batch and row order while preserving duplicate counts — for a multi-row
/// answer whose row order is not itself part of the declared result.
pub fn result_digest(
    schema: &Schema,
    batches: &[RecordBatch],
    semantics: ResultSemantics,
) -> Result<String> {
    match semantics {
        ResultSemantics::Ordered => ordered_digest(schema, batches),
        ResultSemantics::Multiset => multiset_digest(batches),
    }
}

fn ordered_digest(_schema: &Schema, batches: &[RecordBatch]) -> Result<String> {
    if batches.is_empty() {
        return Ok(blake3::hash(b"parquet-lab-empty-result")
            .to_hex()
            .to_string());
    }
    let (schema, first) = canonicalize(&batches[0])?;
    let mut buf: Vec<u8> = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, &schema)
            .context("opening IPC writer for result digest")?;
        writer
            .write(&first)
            .context("writing batch for result digest")?;
        for b in &batches[1..] {
            let (_s, canon) = canonicalize(b)?;
            writer
                .write(&canon)
                .context("writing batch for result digest")?;
        }
        writer.finish().context("finishing IPC stream")?;
    }
    Ok(blake3::hash(&buf).to_hex().to_string())
}

/// Hash the canonical rows independent of order: encode each row to canonical bytes, sort
/// the row-byte vector (duplicates preserved), and hash the schema + sorted rows.
fn multiset_digest(batches: &[RecordBatch]) -> Result<String> {
    if batches.is_empty() {
        return Ok(blake3::hash(b"parquet-lab-empty-result")
            .to_hex()
            .to_string());
    }
    // Fix the canonical schema from the first batch; every batch canonicalizes to it.
    let (schema, _first) = canonicalize(&batches[0])?;
    let mut rows: Vec<Vec<u8>> = Vec::new();
    for b in batches {
        let (_s, canon) = canonicalize(b)?;
        encode_rows(&canon, &mut rows)?;
    }
    rows.sort_unstable();
    let mut h = blake3::Hasher::new();
    // Bind the schema (names + canonical types) so two different shapes never collide.
    for f in schema.fields() {
        h.update(f.name().as_bytes());
        h.update(format!("{:?}", f.data_type()).as_bytes());
    }
    h.update(&(rows.len() as u64).to_le_bytes());
    for r in &rows {
        h.update(&(r.len() as u64).to_le_bytes());
        h.update(r);
    }
    Ok(h.finalize().to_hex().to_string())
}

/// Encode each row of a canonicalized batch to type-tagged bytes.
fn encode_rows(batch: &RecordBatch, out: &mut Vec<Vec<u8>>) -> Result<()> {
    use arrow::array::*;
    for row in 0..batch.num_rows() {
        let mut buf = Vec::with_capacity(64);
        for col in batch.columns() {
            if col.is_null(row) {
                buf.push(0u8);
                continue;
            }
            match col.data_type() {
                DataType::Int64 => {
                    buf.push(1);
                    buf.extend_from_slice(
                        &col.as_any()
                            .downcast_ref::<Int64Array>()
                            .unwrap()
                            .value(row)
                            .to_le_bytes(),
                    );
                }
                DataType::Int32 => {
                    buf.push(2);
                    buf.extend_from_slice(
                        &col.as_any()
                            .downcast_ref::<Int32Array>()
                            .unwrap()
                            .value(row)
                            .to_le_bytes(),
                    );
                }
                DataType::Float64 => {
                    buf.push(3);
                    let v = col
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .unwrap()
                        .value(row);
                    let v = if v == 0.0 { 0.0 } else { v };
                    buf.extend_from_slice(&v.to_le_bytes());
                }
                DataType::Boolean => {
                    buf.push(4);
                    buf.push(
                        col.as_any()
                            .downcast_ref::<BooleanArray>()
                            .unwrap()
                            .value(row) as u8,
                    );
                }
                DataType::Utf8 => {
                    buf.push(5);
                    let s = col
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap()
                        .value(row);
                    buf.extend_from_slice(&(s.len() as u32).to_le_bytes());
                    buf.extend_from_slice(s.as_bytes());
                }
                other => {
                    // Fall back to the debug form of a one-row slice for any type the canonical
                    // coercion left (rare in these suites); still deterministic.
                    buf.push(6);
                    let s = format!("{:?}", col.slice(row, 1));
                    buf.extend_from_slice(&(s.len() as u32).to_le_bytes());
                    buf.extend_from_slice(s.as_bytes());
                    let _ = other;
                }
            }
        }
        out.push(buf);
    }
    Ok(())
}

/// The row count across all batches.
pub fn row_count(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, DictionaryArray, StringArray, StringViewArray};
    use arrow::datatypes::{Field, Int32Type};
    use std::sync::Arc;

    /// The same logical strings in Utf8, Utf8View, and Dictionary encodings must all hash to
    /// the same digest — the regression the smoke matrix exposed, where a high-cardinality
    /// group-by came back dictionary-encoded from one artifact and plain from another.
    #[test]
    fn encoding_does_not_change_the_digest() {
        let values = vec!["alpha", "beta", "alpha", "gamma"];
        let plain = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, false)])),
            vec![Arc::new(StringArray::from(values.clone()))],
        )
        .unwrap();
        let view = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "s",
                DataType::Utf8View,
                false,
            )])),
            vec![Arc::new(StringViewArray::from(values.clone()))],
        )
        .unwrap();
        let dict: DictionaryArray<Int32Type> = values.iter().copied().collect();
        let dict = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "s",
                dict.data_type().clone(),
                false,
            )])),
            vec![Arc::new(dict)],
        )
        .unwrap();

        let a = result_digest(
            &plain.schema(),
            std::slice::from_ref(&plain),
            ResultSemantics::Ordered,
        )
        .unwrap();
        let b = result_digest(&view.schema(), &[view], ResultSemantics::Ordered).unwrap();
        let c = result_digest(&dict.schema(), &[dict], ResultSemantics::Ordered).unwrap();
        assert_eq!(a, b, "Utf8 and Utf8View must digest equal");
        assert_eq!(a, c, "Utf8 and Dictionary must digest equal");

        // A genuinely different value still changes the digest.
        let other = RecordBatch::try_new(
            plain.schema(),
            vec![Arc::new(StringArray::from(vec![
                "alpha", "beta", "alpha", "delta",
            ]))],
        )
        .unwrap();
        assert_ne!(
            a,
            result_digest(&other.schema(), &[other], ResultSemantics::Ordered).unwrap()
        );
    }
}

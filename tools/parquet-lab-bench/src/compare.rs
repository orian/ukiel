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

/// A canonical digest of a query result: the canonical schema plus every row, in result
/// order. BLAKE3 over the canonicalized Arrow IPC stream.
pub fn result_digest(_schema: &Schema, batches: &[RecordBatch]) -> Result<String> {
    if batches.is_empty() {
        // An empty result still has an identity: hash the marker.
        return Ok(blake3::hash(b"parquet-lab-empty-result")
            .to_hex()
            .to_string());
    }
    // Canonicalize the first batch to fix the schema, then all batches under it.
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

/// The row count across all batches.
pub fn row_count(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

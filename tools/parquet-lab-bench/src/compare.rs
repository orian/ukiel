//! Result equivalence: a canonical digest of a query's normalized answer.
//!
//! Every timed query first passes exact schema/result equivalence — a physical variant is
//! only comparable to the control if it returns the identical logical answer. Because the
//! logical-projection view presents the same schema and the same values whatever the
//! physical representation, a variant and the control produce byte-identical Arrow, so a
//! digest over the batches' Arrow IPC bytes is an exact equivalence test.

use anyhow::{Context, Result};
use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use arrow::ipc::writer::StreamWriter;

/// A canonical digest of a query result: the schema plus every row, in result order.
///
/// Ordered queries (`ORDER BY`) compare in their emitted order; unordered aggregates in
/// the suite are written with a deterministic `ORDER BY`, so the order is itself part of
/// the declared answer. The digest is BLAKE3 over the Arrow IPC stream bytes.
pub fn result_digest(schema: &Schema, batches: &[RecordBatch]) -> Result<String> {
    let mut buf: Vec<u8> = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, schema)
            .context("opening IPC writer for result digest")?;
        for b in batches {
            writer.write(b).context("writing batch for result digest")?;
        }
        writer.finish().context("finishing IPC stream")?;
    }
    Ok(blake3::hash(&buf).to_hex().to_string())
}

/// The row count across all batches.
pub fn row_count(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

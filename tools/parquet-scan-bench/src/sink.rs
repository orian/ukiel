//! A typed deterministic checksum sink over decoded batches.
//!
//! Decoded arrays must feed *something* the compiler cannot elide and that large result
//! arrays cannot dominate — but that something must also be type-aware and stable, so a
//! reconstruction and its variant produce the same digest over the same logical rows. The
//! Arrow row format gives exactly that: `RowConverter` turns each batch into canonical,
//! null-aware comparison bytes, which we fold into a BLAKE3 hash. It also lets us count
//! decoded rows and values without materializing a giant output.

use anyhow::{Context, Result};
use arrow::array::RecordBatch;
use arrow::row::{RowConverter, SortField};

/// Accumulates a deterministic checksum plus row/value/batch counts.
pub struct ChecksumSink {
    converter: RowConverter,
    hasher: blake3::Hasher,
    pub rows: u64,
    pub values: u64,
    pub batches: u64,
}

impl ChecksumSink {
    /// Build a sink for a projected schema (the columns that will actually be decoded).
    pub fn new(projected_schema: &arrow::datatypes::Schema) -> Result<Self> {
        let fields: Vec<SortField> = projected_schema
            .fields()
            .iter()
            .map(|f| SortField::new(f.data_type().clone()))
            .collect();
        let converter = RowConverter::new(fields).context("building row converter for checksum")?;
        Ok(ChecksumSink {
            converter,
            hasher: blake3::Hasher::new(),
            rows: 0,
            values: 0,
            batches: 0,
        })
    }

    /// Fold one decoded batch into the checksum and counts.
    pub fn consume(&mut self, batch: &RecordBatch) -> Result<()> {
        let rows = self
            .converter
            .convert_columns(batch.columns())
            .context("converting batch to rows for checksum")?;
        for row in rows.iter() {
            self.hasher.update(row.as_ref());
        }
        self.rows += batch.num_rows() as u64;
        self.values += (batch.num_rows() as u64) * (batch.num_columns() as u64);
        self.batches += 1;
        Ok(())
    }

    /// The final checksum, lowercase hex.
    pub fn checksum(&self) -> String {
        self.hasher.finalize().to_hex().to_string()
    }
}

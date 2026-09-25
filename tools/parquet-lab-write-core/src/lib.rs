//! `parquet-lab-write-core` — the pure Arrow-to-Parquet write and property-resolution
//! core shared by `parquet-rewrite` and `parquet-write-bench`.
//!
//! It resolves a writer configuration into concrete Parquet 58.3 properties, applies a
//! lossless physical-type projection, writes logical batches to a caller-supplied sink
//! while folding the logical fingerprint, and reads back what the writer actually did.
//! It owns no CLI paths, no timing, no reports, and no experiment selection — a rewrite
//! tool wraps it with validation and atomic publication; a bench tool wraps exactly the
//! write with a clock. Keeping the write in one library is what lets the two never
//! disagree about what a config means.

pub mod project;
pub mod write;

pub use project::{
    PhysicalType, parse_physical_type, project_batch, projected_schema, validate_projections,
};
pub use write::{
    ColumnConfig, ResolvedColumn, WriteOutcome, WriterConfig, decode_batches,
    input_schema_and_rows, out_schema_for, parse_compression, parse_encoding, parse_statistics,
    project_batches, resolve_footer, sorting_columns, write_prepared, write_projected,
};

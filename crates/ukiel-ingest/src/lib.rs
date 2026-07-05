//! Kafka -> sorted Parquet L0 -> catalog commit.

mod error;
pub mod flusher;
pub mod writer;

pub use error::IngestError;
pub use writer::{EncodedPart, rows_to_parquet};

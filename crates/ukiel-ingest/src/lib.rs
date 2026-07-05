//! Kafka -> sorted Parquet L0 -> catalog commit.

pub mod config;
pub mod consumer;
mod error;
pub mod flusher;
pub mod writer;

pub use error::IngestError;
pub use writer::{EncodedPart, encode_rows, rows_to_parquet};

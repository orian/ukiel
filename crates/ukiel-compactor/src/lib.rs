//! Background rewrite workers: L0→L1 compaction and key deletion.

mod error;
pub mod rewrite;

pub use error::CompactorError;

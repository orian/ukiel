//! `parquet-skip-index` — build conservative experimental skip-index sidecars.
//!
//! The index *semantics* (the three prototypes and the no-false-negative evaluation) live in
//! the pure [`parquet_skip_index_core`] library, shared with the benchmark. This crate is
//! the *builder*: it reads a variant's Arrow row groups and writes a `ukiel-parquet-skip/v1`
//! sidecar bound to the exact variant and file digests. Any doubt at evaluation time
//! resolves to keep, never a skip.

pub mod builder;

// Re-export the core semantics so existing call sites (and this crate's builder) keep using
// `parquet_skip_index::{Value, zone_map, ...}` unchanged after the move to the core library.
pub use parquet_skip_index_core::{
    Decision, Predicate, Value, payload_digest, prefix_set, sidecar, value_set, zone_map,
};

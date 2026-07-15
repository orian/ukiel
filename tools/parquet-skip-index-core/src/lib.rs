//! `parquet-skip-index-core` — pure sidecar parsing and conservative pruning evaluation.
//!
//! The single home of skip-index semantics: the three bounded prototypes (zone map, value
//! set, prefix set) and the payload decode/evaluate dispatch, over the laboratory's own
//! [`Value`] type. It carries no Arrow and no Parquet, so the builder executable (which
//! reads Arrow row groups) and the benchmark (which applies access plans to a scan) both
//! depend on it and the property-tested no-false-negative guarantee lives in exactly one
//! place — a `NoMatch` can never hide a matching row.

pub mod predicate;
pub mod prefix_set;
pub mod sidecar;
pub mod value_set;
pub mod zone_map;

pub use predicate::{Decision, Predicate, Value, payload_digest};
pub use sidecar::evaluate;

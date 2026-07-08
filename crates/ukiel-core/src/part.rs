use serde::{Deserialize, Serialize};

use crate::ids::{CommitId, HypertableId, PartId};

/// Metadata describing one immutable Parquet file, before it has a catalog identity.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PartMeta {
    /// Object-store key of the Parquet file.
    pub path: String,
    /// Partition values, e.g. `{"key_bucket": 17, "day": "2026-07-05"}`.
    pub partition_values: serde_json::Value,
    /// Smallest packing-key value contained in the file (packed files span a key range).
    pub packing_key_min: i64,
    /// Largest packing-key value contained in the file.
    pub packing_key_max: i64,
    pub row_count: i64,
    pub size_bytes: i64,
    /// Compaction level: 0 = fresh ingest, higher = more compacted.
    pub level: i16,
    /// Optional per-column min/max stats for pruning.
    pub column_stats: Option<serde_json::Value>,
}

/// A per-column bound extracted from query predicates, used for part-level
/// pruning against `PartMeta::column_stats`. Either bound may be open.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnRange {
    pub column: String,
    pub min: Option<i64>,
    pub max: Option<i64>,
}

/// A part as registered in the catalog.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Part {
    pub id: PartId,
    pub hypertable_id: HypertableId,
    pub meta: PartMeta,
    pub created_by_commit: CommitId,
}

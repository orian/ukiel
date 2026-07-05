use serde::{Deserialize, Serialize};

use crate::ids::{HypertableId, LogicalTableId, NamespaceId};

/// Where a hypertable's data lives at rest (L1+). L0 is always packed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Placement {
    /// Small keys share files, sorted by packing key (default).
    Packed,
    /// Compaction gives every packing key dedicated files; key deletion and
    /// retention become metadata-only at rest.
    Separated,
}

impl Placement {
    pub fn as_str(&self) -> &'static str {
        match self {
            Placement::Packed => "packed",
            Placement::Separated => "separated",
        }
    }
}

/// A physical table family: many logical tables' data packed into shared
/// Parquet files, ordered by the packing key.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Hypertable {
    pub id: HypertableId,
    pub name: String,
    /// JSON-serialized column schema, e.g. `{"fields":[{"name":"ts","type":"timestamp_ms"}, ...]}`.
    pub table_schema: serde_json::Value,
    /// e.g. `{"columns": ["key_bucket", "day"]}`.
    pub partition_spec: serde_json::Value,
    /// File sort order, e.g. `["tenant_id", "ts"]`.
    pub sort_key: Vec<String>,
    /// The i64 column whose min/max every part records for range pruning
    /// (e.g. `"tenant_id"` in a multitenant deployment).
    pub packing_key: String,
    /// Placement policy at rest (L1+); L0 is always packed.
    pub placement: Placement,
}

/// A namespaced view over a slice of a hypertable. In a multitenant
/// deployment, namespace = tenant.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LogicalTable {
    pub id: LogicalTableId,
    pub namespace_id: NamespaceId,
    pub name: String,
    pub hypertable_id: HypertableId,
    /// Optional per-table column mapping; `None` = identity.
    pub column_mapping: Option<serde_json::Value>,
}

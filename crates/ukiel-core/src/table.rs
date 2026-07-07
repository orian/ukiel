use serde::{Deserialize, Serialize};

use crate::ids::{HypertableId, LogicalTableId, NamespaceId};

/// Where a hypertable's data lives at rest (L1+). L0 is always packed.
/// Stored as one nullable BIGINT (`hypertables.target_file_bytes`):
/// NULL = packed, 0 = separated, N > 0 = size-targeted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Placement {
    /// Each merge output is one packed file, sorted by packing key (default).
    Packed,
    /// Every packing key gets dedicated files; key deletion and retention
    /// become metadata-only at rest.
    Separated,
    /// Merge outputs are cut at key boundaries into files of roughly this
    /// many bytes: small keys share files, a key bigger than the target gets
    /// files of its own (`min == max`) — heavy keys separate organically,
    /// in proportion to their size.
    SizeTargeted(i64),
}

impl Placement {
    pub fn from_db(target_file_bytes: Option<i64>) -> Self {
        match target_file_bytes {
            None => Placement::Packed,
            Some(n) if n <= 0 => Placement::Separated,
            Some(n) => Placement::SizeTargeted(n),
        }
    }

    pub fn to_db(&self) -> Option<i64> {
        match self {
            Placement::Packed => None,
            Placement::Separated => Some(0),
            Placement::SizeTargeted(n) => Some(*n),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn placement_db_round_trip() {
        assert_eq!(Placement::from_db(None), Placement::Packed);
        assert_eq!(Placement::from_db(Some(0)), Placement::Separated);
        assert_eq!(
            Placement::from_db(Some(268_435_456)),
            Placement::SizeTargeted(268_435_456)
        );
        // Defensive: non-positive targets mean "every key alone".
        assert_eq!(Placement::from_db(Some(-5)), Placement::Separated);

        for p in [
            Placement::Packed,
            Placement::Separated,
            Placement::SizeTargeted(1024),
        ] {
            assert_eq!(Placement::from_db(p.to_db()), p);
        }
        assert_eq!(Placement::Packed.to_db(), None);
        assert_eq!(Placement::Separated.to_db(), Some(0));
        assert_eq!(Placement::SizeTargeted(7).to_db(), Some(7));
    }
}

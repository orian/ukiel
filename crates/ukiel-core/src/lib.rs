//! Shared types for the Ukiel data lake.

pub mod commit;
pub mod ids;
pub mod part;
pub mod schema;
pub mod table;

pub use commit::{ChangeEvent, CommitOp, CommitResult};
pub use ids::{CommitId, HypertableId, LogicalTableId, NamespaceId, PartId};
pub use part::{Part, PartMeta};
pub use schema::{SchemaError, arrow_schema_from_json};
pub use table::{Hypertable, LogicalTable};

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_meta() -> PartMeta {
        PartMeta {
            path: "ht/1/part-0001.parquet".to_string(),
            partition_values: serde_json::json!({"day": "2026-07-05"}),
            packing_key_min: 10,
            packing_key_max: 20,
            row_count: 100,
            size_bytes: 4096,
            level: 0,
            column_stats: None,
        }
    }

    #[test]
    fn part_meta_serde_round_trip() {
        let meta = sample_meta();
        let json = serde_json::to_string(&meta).unwrap();
        let back: PartMeta = serde_json::from_str(&json).unwrap();
        assert_eq!(meta, back);
    }

    #[test]
    fn commit_op_kind_strings() {
        assert_eq!(
            CommitOp::Add {
                parts: vec![sample_meta()]
            }
            .kind(),
            "add"
        );
        assert_eq!(
            CommitOp::Replace {
                old: vec![PartId(1)],
                new: vec![sample_meta()]
            }
            .kind(),
            "replace"
        );
        assert_eq!(
            CommitOp::Delete {
                parts: vec![PartId(1)]
            }
            .kind(),
            "delete"
        );
    }
}

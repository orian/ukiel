use ukiel_core::{CommitId, HypertableId, Part, PartId, PartMeta};

use crate::{CatalogError, PostgresCatalog};

#[derive(sqlx::FromRow)]
pub(crate) struct PartRow {
    pub id: i64,
    pub hypertable_id: i64,
    pub path: String,
    pub partition_values: serde_json::Value,
    pub packing_key_min: i64,
    pub packing_key_max: i64,
    pub row_count: i64,
    pub size_bytes: i64,
    pub level: i16,
    pub column_stats: Option<serde_json::Value>,
    pub created_by_commit: i64,
}

impl From<PartRow> for Part {
    fn from(r: PartRow) -> Self {
        Part {
            id: PartId(r.id),
            hypertable_id: HypertableId(r.hypertable_id),
            meta: PartMeta {
                path: r.path,
                partition_values: r.partition_values,
                packing_key_min: r.packing_key_min,
                packing_key_max: r.packing_key_max,
                row_count: r.row_count,
                size_bytes: r.size_bytes,
                level: r.level,
                column_stats: r.column_stats,
            },
            created_by_commit: CommitId(r.created_by_commit),
        }
    }
}

pub(crate) const PART_COLUMNS: &str = "id, hypertable_id, path, partition_values, packing_key_min, \
     packing_key_max, row_count, size_bytes, level, column_stats, created_by_commit";

impl PostgresCatalog {
    /// Live parts of a hypertable, optionally pruned to files whose
    /// packing-key range contains `key`.
    pub async fn live_parts(
        &self,
        hypertable_id: HypertableId,
        key: Option<i64>,
    ) -> Result<Vec<Part>, CatalogError> {
        let sql = format!(
            "SELECT {PART_COLUMNS} FROM parts
             WHERE hypertable_id = $1 AND deleted_by_commit IS NULL
               AND ($2::BIGINT IS NULL OR (packing_key_min <= $2 AND packing_key_max >= $2))
             ORDER BY id"
        );
        let rows: Vec<PartRow> = sqlx::query_as(sqlx::AssertSqlSafe(sql))
            .bind(hypertable_id.0)
            .bind(key)
            .fetch_all(&self.pool)
            .await?;
        Ok(rows.into_iter().map(Part::from).collect())
    }
}

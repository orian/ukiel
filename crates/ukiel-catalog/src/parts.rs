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

    /// `live_parts` plus stats-based pruning: a part is excluded only when its
    /// column_stats PROVE it cannot overlap a requested range. Missing stats
    /// (or a missing column entry) keep the part — pruning is an optimization,
    /// never a correctness dependency.
    pub async fn live_parts_pruned(
        &self,
        hypertable_id: HypertableId,
        key: Option<i64>,
        ranges: &[ukiel_core::ColumnRange],
    ) -> Result<Vec<Part>, CatalogError> {
        let mut sql = format!(
            "SELECT {PART_COLUMNS} FROM parts
             WHERE hypertable_id = $1 AND deleted_by_commit IS NULL
               AND ($2::BIGINT IS NULL OR (packing_key_min <= $2 AND packing_key_max >= $2))"
        );
        // Three binds per range: column name, min, max. NULL bind = open bound.
        for i in 0..ranges.len() {
            let col = 3 + i * 3; // column-name bind
            let min = col + 1;
            let max = col + 2;
            sql.push_str(&format!(
                " AND ( column_stats -> ${col} IS NULL
                        OR ( (${min}::BIGINT IS NULL
                              OR (column_stats -> ${col} ->> 'max')::bigint >= ${min})
                         AND (${max}::BIGINT IS NULL
                              OR (column_stats -> ${col} ->> 'min')::bigint <= ${max}) ) )"
            ));
        }
        sql.push_str(" ORDER BY id");

        let mut query = sqlx::query_as::<_, PartRow>(sqlx::AssertSqlSafe(sql));
        query = query.bind(hypertable_id.0).bind(key);
        for range in ranges {
            query = query.bind(&range.column).bind(range.min).bind(range.max);
        }
        let rows: Vec<PartRow> = query.fetch_all(&self.pool).await?;
        Ok(rows.into_iter().map(Part::from).collect())
    }

    /// True iff no L0 part was committed to this partition within the last
    /// `quiet_secs` (rows are never deleted, so tombstoned/purged L0 parts
    /// still count as arrivals). Drives cold-partition finalization.
    pub async fn partition_l0_quiet_since(
        &self,
        hypertable_id: HypertableId,
        partition_values: &serde_json::Value,
        quiet_secs: f64,
    ) -> Result<bool, CatalogError> {
        let quiet: bool = sqlx::query_scalar(
            "SELECT coalesce(
                 max(c.created_at) < now() - make_interval(secs => $3),
                 true)
             FROM parts p JOIN commits c ON c.id = p.created_by_commit
             WHERE p.hypertable_id = $1 AND p.partition_values = $2
               AND p.level = 0",
        )
        .bind(hypertable_id.0)
        .bind(partition_values)
        .bind(quiet_secs)
        .fetch_one(&self.pool)
        .await?;
        Ok(quiet)
    }

    /// Max live-L0 count across the given partitions, in one round-trip —
    /// the ingest backpressure input (issue 0006: the per-day probe loop
    /// was most expensive exactly during backfills). Each L0 part is one
    /// ingest commit, so counts are also L0 run counts. Empty slice → 0.
    pub async fn max_live_l0_parts(
        &self,
        hypertable_id: HypertableId,
        partitions: &[serde_json::Value],
    ) -> Result<i64, CatalogError> {
        if partitions.is_empty() {
            return Ok(0);
        }
        let max: i64 = sqlx::query_scalar(
            "SELECT coalesce(max(cnt), 0) FROM (
                 SELECT count(*) AS cnt FROM parts
                 WHERE hypertable_id = $1 AND deleted_by_commit IS NULL
                   AND level = 0 AND partition_values = ANY($2)
                 GROUP BY partition_values) per_partition",
        )
        .bind(hypertable_id.0)
        .bind(partitions)
        .fetch_one(&self.pool)
        .await?;
        Ok(max)
    }

    /// Partitions holding more than one live run (distinct commits) —
    /// finalization candidates, aggregated in Postgres so the sweep never
    /// ships every live part row to the worker.
    pub async fn finalize_candidates(
        &self,
        hypertable_id: HypertableId,
    ) -> Result<Vec<serde_json::Value>, CatalogError> {
        let rows: Vec<serde_json::Value> = sqlx::query_scalar(
            "SELECT partition_values FROM parts
             WHERE hypertable_id = $1 AND deleted_by_commit IS NULL
             GROUP BY partition_values
             HAVING count(DISTINCT created_by_commit) >= 2
             ORDER BY partition_values::text",
        )
        .bind(hypertable_id.0)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    /// Live parts of one partition (what a finalization merge consumes).
    pub async fn live_partition_parts(
        &self,
        hypertable_id: HypertableId,
        partition_values: &serde_json::Value,
    ) -> Result<Vec<Part>, CatalogError> {
        let sql = format!(
            "SELECT {PART_COLUMNS} FROM parts
             WHERE hypertable_id = $1 AND deleted_by_commit IS NULL
               AND partition_values = $2
             ORDER BY id"
        );
        let rows: Vec<PartRow> = sqlx::query_as(sqlx::AssertSqlSafe(sql))
            .bind(hypertable_id.0)
            .bind(partition_values)
            .fetch_all(&self.pool)
            .await?;
        Ok(rows.into_iter().map(Part::from).collect())
    }
}

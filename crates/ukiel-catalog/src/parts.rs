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

    /// Partitions holding at least one level whose live run count has reached
    /// that level's merge trigger — the compactor's work list, aggregated in
    /// Postgres so a sweep never ships part rows for partitions it will not
    /// touch. Derived from *current live state*, never from the change feed: a
    /// shared fleet cursor may have moved past an event whose compaction was
    /// abandoned when its worker died, and that backlog must stay discoverable
    /// with no new event to wake anyone (plan 41).
    ///
    /// Distinct partitions, deterministic order, bounded by `limit`. The
    /// migration-0007 `(hypertable_id, partition_values, level)` index serves
    /// the GROUP BY.
    pub async fn compaction_candidates(
        &self,
        hypertable_id: HypertableId,
        l0_fanout: i64,
        fanout: i64,
        limit: i64,
    ) -> Result<Vec<serde_json::Value>, CatalogError> {
        let rows: Vec<serde_json::Value> = sqlx::query_scalar(
            "SELECT partition_values FROM (
                 SELECT partition_values, level, count(DISTINCT created_by_commit) AS runs
                 FROM parts
                 WHERE hypertable_id = $1 AND deleted_by_commit IS NULL
                 GROUP BY partition_values, level) g
             WHERE runs >= CASE WHEN level = 0 THEN $2 ELSE $3 END
             GROUP BY partition_values
             ORDER BY partition_values::text
             LIMIT $4",
        )
        .bind(hypertable_id.0)
        .bind(l0_fanout)
        .bind(fanout)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    /// Live part counts per `(hypertable, level)` — the `catalog_live_parts`
    /// gauge (metrics P2). The scan is bounded by live parts, exactly the
    /// quantity being measured (partial `parts_live_idx` covers live rows).
    pub async fn live_part_counts(&self) -> Result<Vec<(HypertableId, i16, i64)>, CatalogError> {
        let rows: Vec<(i64, i16, i64)> = sqlx::query_as(
            "SELECT hypertable_id, level, count(*) FROM parts
             WHERE deleted_by_commit IS NULL
             GROUP BY hypertable_id, level",
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|(ht, level, n)| (HypertableId(ht), level, n))
            .collect())
    }

    /// Count of `(partition, level)` groups at or over their merge trigger —
    /// the `compactor_backlog_groups` gauge (metrics P2). The migration-0007
    /// index `(hypertable_id, partition_values, level)` serves the GROUP BY.
    pub async fn backlog_groups(&self, l0_fanout: i64, fanout: i64) -> Result<i64, CatalogError> {
        let count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM (
                 SELECT level, count(DISTINCT created_by_commit) AS runs FROM parts
                 WHERE deleted_by_commit IS NULL
                 GROUP BY hypertable_id, partition_values, level) g
             WHERE runs >= CASE WHEN level = 0 THEN $1 ELSE $2 END",
        )
        .bind(l0_fanout)
        .bind(fanout)
        .fetch_one(&self.pool)
        .await?;
        Ok(count)
    }

    /// Partitions past `quiet_secs` with no L0 arrival still holding >1 live
    /// run, plus the oldest such partition's quiet age in seconds — the
    /// plan-17 terminal invariant ("cold partition = one run") as production
    /// gauges (`compactor_unfinalized_partitions`, `_oldest_..._age_seconds`).
    /// A partition with live runs but no L0 parts counts as quiet (matches
    /// `partition_l0_quiet_since`'s coalesce-to-true). 0007 index.
    pub async fn unfinalized_partitions(
        &self,
        quiet_secs: f64,
    ) -> Result<(i64, f64), CatalogError> {
        let (count, oldest): (i64, f64) = sqlx::query_as(
            "WITH live_runs AS (
                 SELECT hypertable_id, partition_values
                 FROM parts
                 WHERE deleted_by_commit IS NULL
                 GROUP BY hypertable_id, partition_values
                 HAVING count(DISTINCT created_by_commit) >= 2),
             last_arrival AS (
                 SELECT p.hypertable_id, p.partition_values, max(c.created_at) AS last_l0
                 FROM parts p JOIN commits c ON c.id = p.created_by_commit
                 WHERE p.level = 0
                 GROUP BY p.hypertable_id, p.partition_values)
             SELECT
                 count(*) FILTER (
                     WHERE a.last_l0 IS NULL
                        OR a.last_l0 < now() - make_interval(secs => $1)),
                 coalesce(max(EXTRACT(EPOCH FROM (now() - a.last_l0))) FILTER (
                     WHERE a.last_l0 IS NULL
                        OR a.last_l0 < now() - make_interval(secs => $1)), 0)::float8
             FROM live_runs r
             LEFT JOIN last_arrival a
               ON a.hypertable_id = r.hypertable_id
              AND a.partition_values = r.partition_values",
        )
        .bind(quiet_secs)
        .fetch_one(&self.pool)
        .await?;
        Ok((count, oldest))
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

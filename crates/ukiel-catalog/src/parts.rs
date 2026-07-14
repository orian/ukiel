use ukiel_core::{CommitId, HypertableId, Part, PartId, PartMeta};
use uuid::Uuid;

use crate::{CatalogError, PostgresCatalog};

/// One bounded, claimable slice of a partition sweep, plus what the sweep saw
/// around it (issue 0012). The counts describe the *whole* eligible set, not
/// the window: a worker cannot tell a healthy fleet from a starved sweep by
/// looking only at the partitions it was handed.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CandidateWindow {
    /// Claimable partitions, in sweep order, starting after the caller's cursor.
    pub partitions: Vec<serde_json::Value>,
    /// The cursor to resume from — Postgres's own `partition_values::text` for
    /// the last partition above. Never rebuilt in Rust: `jsonb` text rendering
    /// (key order, spacing) is Postgres's, and it is what the ordering and the
    /// `after` bound compare.
    pub next_cursor: Option<String>,
    /// Eligible partitions in this hypertable — the backlog the sweep is
    /// rotating through, cursor position irrelevant.
    pub eligible: i64,
    /// Of those, held by a *peer's* unexpired lease: work that exists but
    /// belongs to someone else right now, and so never occupies a window slot.
    pub leased: i64,
}

impl CandidateWindow {
    /// Eligible partitions no peer currently holds — what this sweep could take
    /// if its window were unbounded.
    pub fn claimable(&self) -> i64 {
        self.eligible - self.leased
    }

    fn from_rows(rows: Vec<WindowRow>) -> Self {
        // The counts row always comes back, even with nothing claimable to join
        // to it (LEFT JOIN LATERAL), so "no window" and "no backlog" stay
        // distinguishable.
        let (eligible, leased) = rows.first().map_or((0, 0), |r| (r.0, r.1));
        let next_cursor = rows.last().and_then(|r| r.3.clone());
        Self {
            partitions: rows.into_iter().filter_map(|r| r.2).collect(),
            next_cursor,
            eligible,
            leased,
        }
    }
}

/// `(eligible, leased, partition_values, cursor)`; the last two are NULL on the
/// single row that comes back when the window is empty.
type WindowRow = (i64, i64, Option<serde_json::Value>, Option<String>);

/// Wraps an `eligible` sub-select — one `partition_values` column, scoped to the
/// hypertable in `$1` — in the shared fair selector: drop the partitions a peer
/// holds, count the whole eligible set for the fairness metrics, and return one
/// window of the rest after the caller's cursor. `owner`, `limit` and `after`
/// name the bind parameters the caller left for them.
///
/// "A peer holds it" is the exact complement of what `try_acquire_compaction_lease`
/// accepts: unexpired *and* someone else's. A worker's own lease must keep its
/// partition visible, or a process rebuilt after a blip would be locked out of
/// the work it already owns until its own lease expired.
///
/// Both sweeps share this so neither can quietly regain an unbounded or
/// lease-blind candidate query.
fn window_sql(eligible: &str, owner: &str, limit: &str, after: &str) -> String {
    format!(
        "WITH eligible AS ({eligible}),
         marked AS (
             SELECT e.partition_values,
                    EXISTS (SELECT 1 FROM compaction_leases l
                            WHERE l.hypertable_id = $1
                              AND l.partition_values = e.partition_values
                              AND l.expires_at > clock_timestamp()
                              AND l.owner_id <> {owner}::uuid) AS leased
             FROM eligible e
         ),
         counts AS (
             SELECT count(*) AS eligible, count(*) FILTER (WHERE leased) AS leased
             FROM marked
         )
         SELECT counts.eligible, counts.leased, w.partition_values, w.cursor
         FROM counts
         LEFT JOIN LATERAL (
             SELECT partition_values, partition_values::text AS cursor
             FROM marked
             WHERE NOT leased
               AND ({after}::text IS NULL OR partition_values::text > {after}::text)
             ORDER BY partition_values::text
             LIMIT {limit}
         ) w ON true"
    )
}

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
        // Range first, then a membership test the index can answer (issue 0014).
        //
        // The range predicate alone is a *superset*: a packed file whose keys span
        // 100..4500 "contains" tenant 3000 even when it holds none of its rows,
        // which is the ordinary case. The key filter rejects exactly those — and it
        // rides in the range index's INCLUDE payload, so the candidate pass is an
        // **index-only scan** that never touches the heap for a part it rejects.
        // Only the survivors are fetched.
        //
        // Hence the two-phase shape: an inner pass over the index (`id` and the
        // filter are both in it), then the surviving rows by primary key. Written as
        // one flat query, PostgreSQL would have to fetch every candidate row just to
        // reach the filter column, which is the entire cost this is removing.
        //
        // The filter is not *interpreted* here, and the database is not asked to
        // hash. `keyfilter::sql_predicate` and `keyfilter::sql_binds` are the two
        // halves of one handoff: the key becomes (byte, bit) pairs once per query,
        // and SQL merely reads those bytes. The first cut did ask the database to
        // hash, in plpgsql, and a call per candidate row made the query *slower* than
        // the scan it replaced while reading a sixth of the buffers.
        //
        // Undecidable is keep — a NULL filter (a pre-0012 part, or a single-key part
        // whose range is already exact), a blob of some size this reader does not
        // know, an unknown version. Only a *proven* absence prunes, and the provider's
        // exact bitmap still runs behind this as the backstop.
        use ukiel_core::keyfilter;
        // With no key there is no membership question, so the clause is not built and
        // its binds do not exist. $3.. are the filter's when there is a key; the range
        // binds follow whatever came before them.
        let key_binds = key.map(keyfilter::sql_binds);
        let key_clause = match &key_binds {
            Some(_) => format!(
                "AND packing_key_min <= $2 AND packing_key_max >= $2
                   AND {}",
                keyfilter::sql_predicate("key_filter", 3)
            ),
            None => String::new(),
        };
        let after_probes = 3 + key_binds.as_ref().map_or(0, |b| b.len());
        let mut sql = format!(
            "SELECT {PART_COLUMNS} FROM parts WHERE id IN (
                 SELECT id FROM parts
                 WHERE hypertable_id = $1 AND deleted_by_commit IS NULL
                   {key_clause}
             )"
        );
        // Three binds per range: column name, min, max. NULL bind = open bound.
        for i in 0..ranges.len() {
            let col = after_probes + i * 3; // column-name bind
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
        for b in key_binds.iter().flatten() {
            query = query.bind(*b);
        }
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

    /// Cold partitions holding more than one live run (distinct commits) —
    /// finalization candidates, aggregated in Postgres so the sweep never ships
    /// every live part row to the worker. Same bounded, lease-aware, resumable
    /// window as the ladder (see `compaction_candidates`).
    ///
    /// Quietness is part of *eligibility*, not a post-claim filter. ">= 2 live
    /// runs" alone describes almost every partition that has been written to and
    /// not yet finalized — hot ones included, since a ladder merge normally
    /// leaves runs across levels. A window bounded on that set would spend most
    /// of its slots claiming, probing and discarding partitions that cannot be
    /// finalized, and would make a genuinely cold partition wait for the cursor
    /// to rotate past all of them. `finalize_partition` still rechecks after the
    /// claim: that one is the race guard, not the filter.
    ///
    /// `quiet_secs` matches `partition_l0_quiet_since` exactly, including that
    /// arrivals count tombstoned L0 parts (part rows are never deleted, and this
    /// measures arrivals, not liveness) and that a partition with no L0 part at
    /// all is quiet.
    ///
    /// The runs are counted as a `DISTINCT (partition, commit)` set rather than
    /// `count(DISTINCT created_by_commit)` per partition: same answer — the
    /// column is `NOT NULL` — but it aggregates in `parts_compaction_idx` order
    /// (migration 0009) instead of sorting every live part row on disk.
    pub async fn finalize_candidates(
        &self,
        hypertable_id: HypertableId,
        owner_id: Uuid,
        quiet_secs: f64,
        limit: i64,
        after: Option<&str>,
    ) -> Result<CandidateWindow, CatalogError> {
        let sql = window_sql(
            "SELECT r.partition_values FROM (
                 SELECT DISTINCT partition_values, created_by_commit FROM parts
                 WHERE hypertable_id = $1 AND deleted_by_commit IS NULL) r
             GROUP BY r.partition_values
             HAVING count(*) >= 2
                AND NOT EXISTS (
                    SELECT 1 FROM parts p JOIN commits c ON c.id = p.created_by_commit
                    WHERE p.hypertable_id = $1 AND p.partition_values = r.partition_values
                      AND p.level = 0
                      AND c.created_at >= now() - make_interval(secs => $5))",
            "$2",
            "$3",
            "$4",
        );
        let rows: Vec<WindowRow> = sqlx::query_as(sqlx::AssertSqlSafe(sql))
            .bind(hypertable_id.0)
            .bind(owner_id)
            .bind(limit)
            .bind(after)
            .bind(quiet_secs)
            .fetch_all(&self.pool)
            .await?;
        Ok(CandidateWindow::from_rows(rows))
    }

    /// Partitions holding at least one level whose live run count has reached
    /// that level's merge trigger — the compactor's work list, aggregated in
    /// Postgres so a sweep never ships part rows for partitions it will not
    /// touch. Derived from *current live state*, never from the change feed: a
    /// shared fleet cursor may have moved past an event whose compaction was
    /// abandoned when its worker died, and that backlog must stay discoverable
    /// with no new event to wake anyone (plan 41).
    ///
    /// The window is bounded by `limit`, ordered deterministically, and
    /// *resumable*: `after` is an exclusive lower bound in that same order, so
    /// a caller rotating its cursor sweeps the whole eligible set rather than
    /// re-serving one hot prefix forever (issue 0012). Partitions a peer holds
    /// under an unexpired lease are left out — acquisition remains the atomic
    /// authority, this only stops the window being spent on work `owner_id`
    /// cannot take.
    ///
    /// The run counts stream out of `parts_compaction_idx` (migration 0009) in
    /// group order — an index-only scan, no sort. The 0007 index, which the old
    /// docstring here credited, covers neither `created_by_commit` nor the
    /// liveness predicate, so this aggregation was in fact a full sequential
    /// scan of every live part with an external merge sort behind it.
    pub async fn compaction_candidates(
        &self,
        hypertable_id: HypertableId,
        owner_id: Uuid,
        l0_fanout: i64,
        fanout: i64,
        limit: i64,
        after: Option<&str>,
    ) -> Result<CandidateWindow, CatalogError> {
        let sql = window_sql(
            "SELECT partition_values FROM (
                 SELECT partition_values, level, count(DISTINCT created_by_commit) AS runs
                 FROM parts
                 WHERE hypertable_id = $1 AND deleted_by_commit IS NULL
                 GROUP BY partition_values, level) g
             WHERE runs >= CASE WHEN level = 0 THEN $3 ELSE $4 END
             GROUP BY partition_values",
            "$2",
            "$5",
            "$6",
        );
        let rows: Vec<WindowRow> = sqlx::query_as(sqlx::AssertSqlSafe(sql))
            .bind(hypertable_id.0)
            .bind(owner_id)
            .bind(l0_fanout)
            .bind(fanout)
            .bind(limit)
            .bind(after)
            .fetch_all(&self.pool)
            .await?;
        Ok(CandidateWindow::from_rows(rows))
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
    /// the `compactor_backlog_groups` gauge (metrics P2). Same run counting as
    /// the candidate sweeps, so `parts_compaction_idx` (migration 0009) serves
    /// it as an index-only scan across every hypertable.
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

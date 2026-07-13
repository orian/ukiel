//! GC bookkeeping: which tombstoned parts may have their objects deleted,
//! and which object paths the catalog knows about at all.

use ukiel_core::{HypertableId, Part, PartId};

use crate::parts::{PART_COLUMNS, PartRow};
use crate::{CatalogError, PostgresCatalog};

impl PostgresCatalog {
    /// Tombstoned, unpurged parts that are safe to reap: the deleting commit
    /// is older than `grace_seconds` AND every registered worker cursor for
    /// this hypertable has passed it (no cursors = no fence).
    pub async fn reapable_parts(
        &self,
        hypertable_id: HypertableId,
        grace_seconds: f64,
    ) -> Result<Vec<Part>, CatalogError> {
        let sql = format!(
            "SELECT {PART_COLUMNS} FROM parts WHERE id IN (
                SELECT p.id FROM parts p
                JOIN commits c ON c.id = p.deleted_by_commit
                WHERE p.hypertable_id = $1
                  AND p.purged_at IS NULL
                  AND c.created_at <= now() - make_interval(secs => $2)
                  AND p.deleted_by_commit <= COALESCE(
                      (SELECT MIN(last_commit_id) FROM worker_cursors
                        WHERE hypertable_id = $1),
                      9223372036854775807)
            ) ORDER BY id"
        );
        let rows: Vec<PartRow> = sqlx::query_as(sqlx::AssertSqlSafe(sql))
            .bind(hypertable_id.0)
            .bind(grace_seconds)
            .fetch_all(&self.pool)
            .await?;
        Ok(rows.into_iter().map(Part::from).collect())
    }

    /// The next single reapable part, re-derived from authority (plan 42).
    ///
    /// GC deletes objects **one authorized candidate at a time**: fetch one,
    /// delete it, stamp it, ask again. The alternative — read a list, then work
    /// through it — means that when the catalog dies mid-pass, the worker keeps
    /// deleting from a list nothing can still vouch for. After a failover the
    /// new primary may not even have the commit that tombstoned those parts, in
    /// which case the "safe to delete" list describes objects that are still
    /// live. Destructive work must be authorized by state we can confirm *now*.
    ///
    /// The cost is one extra round trip per object. It is worth paying: the S3
    /// DELETE dominates the latency, and the thing being protected is data.
    pub async fn next_reapable_part(
        &self,
        hypertable_id: HypertableId,
        grace_seconds: f64,
    ) -> Result<Option<Part>, CatalogError> {
        let sql = format!(
            "SELECT {PART_COLUMNS} FROM parts WHERE id IN (
                SELECT p.id FROM parts p
                JOIN commits c ON c.id = p.deleted_by_commit
                WHERE p.hypertable_id = $1
                  AND p.purged_at IS NULL
                  AND c.created_at <= now() - make_interval(secs => $2)
                  AND p.deleted_by_commit <= COALESCE(
                      (SELECT MIN(last_commit_id) FROM worker_cursors
                        WHERE hypertable_id = $1),
                      9223372036854775807)
            ) ORDER BY id LIMIT 1"
        );
        let row: Option<PartRow> = sqlx::query_as(sqlx::AssertSqlSafe(sql))
            .bind(hypertable_id.0)
            .bind(grace_seconds)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(Part::from))
    }

    /// The next single orphaned upload intent, re-derived from authority.
    /// Same rule as `next_reapable_part`: one authorized deletion at a time.
    pub async fn next_orphaned_pending_object(
        &self,
        hypertable_id: HypertableId,
        grace_seconds: f64,
    ) -> Result<Option<String>, CatalogError> {
        Ok(sqlx::query_scalar(
            "SELECT path FROM pending_objects
             WHERE hypertable_id = $1
               AND created_at <= now() - make_interval(secs => $2)
             ORDER BY path LIMIT 1",
        )
        .bind(hypertable_id.0)
        .bind(grace_seconds)
        .fetch_optional(&self.pool)
        .await?)
    }

    pub async fn mark_purged(&self, parts: &[PartId]) -> Result<(), CatalogError> {
        let ids: Vec<i64> = parts.iter().map(|p| p.0).collect();
        sqlx::query("UPDATE parts SET purged_at = now() WHERE id = ANY($1)")
            .bind(&ids)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Every object path the catalog currently accounts for under this
    /// hypertable: live and tombstoned-but-not-yet-purged parts. Purged parts
    /// are excluded — their objects are already deleted, and excluding them
    /// keeps this set bounded by real object count, not all-time history.
    pub async fn all_part_paths(
        &self,
        hypertable_id: HypertableId,
    ) -> Result<Vec<String>, CatalogError> {
        Ok(sqlx::query_scalar(
            "SELECT path FROM parts WHERE hypertable_id = $1 AND purged_at IS NULL",
        )
        .bind(hypertable_id.0)
        .fetch_all(&self.pool)
        .await?)
    }

    /// Every path with an outstanding upload intent (in-flight or orphaned).
    pub async fn all_pending_paths(
        &self,
        hypertable_id: HypertableId,
    ) -> Result<Vec<String>, CatalogError> {
        Ok(
            sqlx::query_scalar("SELECT path FROM pending_objects WHERE hypertable_id = $1")
                .bind(hypertable_id.0)
                .fetch_all(&self.pool)
                .await?,
        )
    }

    /// Records upload intent for `paths` (idempotent). Callers MUST do this
    /// before uploading, so a crash between upload and commit leaves a
    /// discoverable orphan.
    pub async fn register_pending_objects(
        &self,
        hypertable_id: HypertableId,
        paths: &[String],
    ) -> Result<(), CatalogError> {
        if paths.is_empty() {
            return Ok(());
        }
        sqlx::query(
            "INSERT INTO pending_objects (hypertable_id, path)
             SELECT $1, p FROM UNNEST($2::text[]) AS p
             ON CONFLICT (path) DO NOTHING",
        )
        .bind(hypertable_id.0)
        .bind(paths)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Pending-object paths older than `grace_seconds` whose commit never
    /// landed — i.e. orphaned uploads. Ordered by path for deterministic tests.
    pub async fn orphaned_pending_objects(
        &self,
        hypertable_id: HypertableId,
        grace_seconds: f64,
    ) -> Result<Vec<String>, CatalogError> {
        Ok(sqlx::query_scalar(
            "SELECT path FROM pending_objects
             WHERE hypertable_id = $1
               AND created_at <= now() - make_interval(secs => $2)
             ORDER BY path",
        )
        .bind(hypertable_id.0)
        .bind(grace_seconds)
        .fetch_all(&self.pool)
        .await?)
    }

    /// Removes intent rows for the given paths (after their objects are gone).
    pub async fn clear_pending_objects(&self, paths: &[String]) -> Result<(), CatalogError> {
        if paths.is_empty() {
            return Ok(());
        }
        sqlx::query("DELETE FROM pending_objects WHERE path = ANY($1)")
            .bind(paths)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// GC backlog scalars in one round-trip — the metrics-P2 GC gauges.
    /// `pending_objects_sweep_idx` and `parts_reapable_idx` serve the counts;
    /// the age minima are the oldest-in-backlog fence-stall proxies.
    pub async fn gc_backlogs(&self) -> Result<GcBacklogs, CatalogError> {
        let (pending, pending_oldest_secs, unpurged_tombstones, oldest_tombstone_secs): (
            i64,
            f64,
            i64,
            f64,
        ) = sqlx::query_as(
            "SELECT
                 (SELECT count(*) FROM pending_objects),
                 (SELECT coalesce(EXTRACT(EPOCH FROM (now() - min(created_at))), 0)
                    FROM pending_objects)::float8,
                 (SELECT count(*) FROM parts
                   WHERE deleted_by_commit IS NOT NULL AND purged_at IS NULL),
                 (SELECT coalesce(EXTRACT(EPOCH FROM (now() - min(c.created_at))), 0)
                    FROM parts p JOIN commits c ON c.id = p.deleted_by_commit
                   WHERE p.purged_at IS NULL)::float8",
        )
        .fetch_one(&self.pool)
        .await?;
        Ok(GcBacklogs {
            pending,
            pending_oldest_secs,
            unpurged_tombstones,
            oldest_tombstone_secs,
        })
    }
}

/// GC backlog gauges (metrics P2): pending-object count and oldest age, and
/// unpurged-tombstone count and oldest deleting-commit age (a reap-fence
/// stall shows here as a growing `oldest_tombstone_secs`).
#[derive(Debug, Clone, PartialEq)]
pub struct GcBacklogs {
    pub pending: i64,
    pub pending_oldest_secs: f64,
    pub unpurged_tombstones: i64,
    pub oldest_tombstone_secs: f64,
}

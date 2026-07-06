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
}

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

    /// Every object path the catalog has ever committed for this hypertable,
    /// in any state (live, tombstoned, purged). The sweeper treats anything
    /// under the hypertable's prefix that is NOT in this set as an orphan.
    pub async fn all_part_paths(
        &self,
        hypertable_id: HypertableId,
    ) -> Result<Vec<String>, CatalogError> {
        Ok(
            sqlx::query_scalar("SELECT path FROM parts WHERE hypertable_id = $1")
                .bind(hypertable_id.0)
                .fetch_all(&self.pool)
                .await?,
        )
    }
}

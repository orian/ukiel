//! Durable change-feed cursors for background workers.

use ukiel_core::{CommitId, HypertableId};

use crate::{CatalogError, PostgresCatalog};

impl PostgresCatalog {
    /// The last commit id this worker processed for this hypertable
    /// (CommitId(0) = start of the feed).
    pub async fn get_cursor(
        &self,
        worker: &str,
        hypertable_id: HypertableId,
    ) -> Result<CommitId, CatalogError> {
        let id: Option<i64> = sqlx::query_scalar(
            "SELECT last_commit_id FROM worker_cursors WHERE worker = $1 AND hypertable_id = $2",
        )
        .bind(worker)
        .bind(hypertable_id.0)
        .fetch_optional(&self.pool)
        .await?;
        Ok(CommitId(id.unwrap_or(0)))
    }

    pub async fn set_cursor(
        &self,
        worker: &str,
        hypertable_id: HypertableId,
        cursor: CommitId,
    ) -> Result<(), CatalogError> {
        sqlx::query(
            "INSERT INTO worker_cursors (worker, hypertable_id, last_commit_id)
             VALUES ($1, $2, $3)
             ON CONFLICT (worker, hypertable_id)
             DO UPDATE SET last_commit_id = EXCLUDED.last_commit_id, updated_at = now()",
        )
        .bind(worker)
        .bind(hypertable_id.0)
        .bind(cursor.0)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}

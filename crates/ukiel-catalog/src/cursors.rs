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

    /// Advances a worker cursor; never retreats it. A stale or equal write is a
    /// successful no-op that leaves `updated_at` alone — a delayed replica must
    /// not be able to move a cursor backwards (prolonging the GC reap fence and
    /// falsifying feed lag) nor to impersonate progress with a replayed write.
    ///
    /// Several processes may share one cursor name: the row then means "the
    /// fleet has observed this feed prefix", never "every derived rewrite
    /// completed" — durable completion is the live part set, not this number.
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
             DO UPDATE SET last_commit_id = EXCLUDED.last_commit_id, updated_at = now()
             WHERE worker_cursors.last_commit_id < EXCLUDED.last_commit_id",
        )
        .bind(worker)
        .bind(hypertable_id.0)
        .bind(cursor.0)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Feed lag per `(worker, hypertable)`: head commit id minus the worker's
    /// cursor, floored at 0 — the `catalog_feed_lag` gauge (metrics P2).
    /// `commits_feed_idx (hypertable_id, id)` serves the per-hypertable max.
    pub async fn feed_lags(&self) -> Result<Vec<(String, HypertableId, i64)>, CatalogError> {
        let rows: Vec<(String, i64, i64)> = sqlx::query_as(
            "SELECT c.worker, c.hypertable_id,
                    greatest(coalesce(m.head, 0) - c.last_commit_id, 0)
             FROM worker_cursors c
             LEFT JOIN LATERAL (
                 SELECT max(id) AS head FROM commits WHERE hypertable_id = c.hypertable_id
             ) m ON true",
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|(worker, ht, lag)| (worker, HypertableId(ht), lag))
            .collect())
    }
}

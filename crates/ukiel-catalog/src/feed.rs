use ukiel_core::{ChangeEvent, CommitId, HypertableId, Part, PartId};

use crate::parts::{PART_COLUMNS, PartRow};
use crate::{CatalogError, PostgresCatalog};

impl PostgresCatalog {
    /// A complete committed feed prefix (0 when the hypertable has none).
    /// Waits for in-flight catalog writers on this hypertable; other hypertables
    /// and ordinary queries remain independent. A stalled writer can delay this
    /// call until the configured catalog lock timeout (a retryable error).
    /// Uses `commits_feed_idx` without materializing any part metadata.
    pub async fn feed_head(&self, hypertable_id: HypertableId) -> Result<CommitId, CatalogError> {
        // Writers take a shared advisory lock before allocating IDs and hold it
        // through transaction completion. The exclusive lock drains those writers and
        // prevents new ID allocations until we have captured the safe prefix.
        // READ COMMITTED and a SEPARATE SELECT are essential: a snapshot taken
        // before waiting could still omit a writer that committed during the
        // wait. Sequence CACHE 1 is also part of the contract (see the schema).
        let mut tx = self
            .pool
            .begin_with("BEGIN ISOLATION LEVEL READ COMMITTED")
            .await?;
        // Reserve negative one-bigint advisory keys for feed barriers: bitwise
        // NOT maps positive hypertable IDs injectively, without i64 overflow.
        // Use advisory locks rather than the hypertable row: repeated KEY SHARE
        // lockers can starve a queued FOR UPDATE reader under sustained writes.
        sqlx::query("SELECT pg_advisory_xact_lock(~$1::bigint)")
            .bind(hypertable_id.0)
            .execute(&mut *tx)
            .await?;
        let head: Option<i64> =
            sqlx::query_scalar("SELECT max(id) FROM commits WHERE hypertable_id = $1")
                .bind(hypertable_id.0)
                .fetch_one(&mut *tx)
                .await?;
        tx.commit().await?;
        Ok(CommitId(head.unwrap_or(0)))
    }

    /// Ordered change feed: commits with id > `after`, oldest first.
    /// Only returns events inside a complete committed prefix. Consumers persist
    /// the last processed commit id as their cursor. IDs can have gaps and are
    /// ordered for delivery, not necessarily by wall-clock transaction commit.
    pub async fn changes_since(
        &self,
        hypertable_id: HypertableId,
        after: CommitId,
        limit: i64,
    ) -> Result<Vec<ChangeEvent>, CatalogError> {
        let head = self.feed_head(hypertable_id).await?;
        let commits: Vec<(i64, String)> = sqlx::query_as(
            "SELECT id, kind FROM commits
             WHERE hypertable_id = $1 AND id > $2 AND id <= $4
             ORDER BY id
             LIMIT $3",
        )
        .bind(hypertable_id.0)
        .bind(after.0)
        .bind(limit)
        .bind(head.0)
        .fetch_all(&self.pool)
        .await?;

        let mut events = Vec::with_capacity(commits.len());
        for (commit_id, kind) in commits {
            let added_sql = format!(
                "SELECT {PART_COLUMNS} FROM parts WHERE created_by_commit = $1 ORDER BY id"
            );
            let added: Vec<PartRow> = sqlx::query_as(sqlx::AssertSqlSafe(added_sql))
                .bind(commit_id)
                .fetch_all(&self.pool)
                .await?;

            let removed: Vec<i64> =
                sqlx::query_scalar("SELECT id FROM parts WHERE deleted_by_commit = $1 ORDER BY id")
                    .bind(commit_id)
                    .fetch_all(&self.pool)
                    .await?;

            events.push(ChangeEvent {
                commit_id: CommitId(commit_id),
                kind,
                added: added.into_iter().map(Part::from).collect(),
                removed: removed.into_iter().map(PartId).collect(),
            });
        }
        Ok(events)
    }
}

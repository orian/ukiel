use ukiel_core::{ChangeEvent, CommitId, HypertableId, Part, PartId};

use crate::parts::{PART_COLUMNS, PartRow};
use crate::{CatalogError, PostgresCatalog};

impl PostgresCatalog {
    /// Newest commit id for this hypertable (0 when it has none) — the feed
    /// prefix a worker can declare observed. One index probe on
    /// `commits_feed_idx`, unlike `changes_since`, which materializes every
    /// commit's parts; workers that plan from live state want this instead.
    pub async fn feed_head(&self, hypertable_id: HypertableId) -> Result<CommitId, CatalogError> {
        let head: Option<i64> =
            sqlx::query_scalar("SELECT max(id) FROM commits WHERE hypertable_id = $1")
                .bind(hypertable_id.0)
                .fetch_one(&self.pool)
                .await?;
        Ok(CommitId(head.unwrap_or(0)))
    }

    /// Ordered change feed: commits with id > `after`, oldest first.
    /// Consumers persist the last processed commit id as their cursor.
    pub async fn changes_since(
        &self,
        hypertable_id: HypertableId,
        after: CommitId,
        limit: i64,
    ) -> Result<Vec<ChangeEvent>, CatalogError> {
        let commits: Vec<(i64, String)> = sqlx::query_as(
            "SELECT id, kind FROM commits
             WHERE hypertable_id = $1 AND id > $2
             ORDER BY id
             LIMIT $3",
        )
        .bind(hypertable_id.0)
        .bind(after.0)
        .bind(limit)
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

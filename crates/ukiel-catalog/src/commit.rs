use sqlx::{Postgres, Transaction};
use ukiel_core::{CommitId, CommitOp, CommitResult, HypertableId, PartId, PartMeta};

use crate::{CatalogError, PostgresCatalog};

impl PostgresCatalog {
    /// Atomically apply `op`. REPLACE/DELETE fail with `CatalogError::Conflict`
    /// if any input part is no longer live (another commit tombstoned it first);
    /// the caller should re-read live parts and retry.
    pub async fn commit(
        &self,
        hypertable_id: HypertableId,
        op: CommitOp,
        idempotency_key: Option<&str>,
    ) -> Result<CommitResult, CatalogError> {
        let mut tx = self.pool.begin().await?;

        let inserted: Option<i64> = sqlx::query_scalar(
            "INSERT INTO commits (hypertable_id, kind, idempotency_key)
             VALUES ($1, $2, $3)
             ON CONFLICT (hypertable_id, idempotency_key) WHERE idempotency_key IS NOT NULL
             DO NOTHING
             RETURNING id",
        )
        .bind(hypertable_id.0)
        .bind(op.kind())
        .bind(idempotency_key)
        .fetch_optional(&mut *tx)
        .await?;

        let commit_id = match inserted {
            Some(id) => CommitId(id),
            None => {
                // Idempotency-key collision: the work was already done.
                let existing: i64 = sqlx::query_scalar(
                    "SELECT id FROM commits WHERE hypertable_id = $1 AND idempotency_key = $2",
                )
                .bind(hypertable_id.0)
                .bind(idempotency_key)
                .fetch_one(&mut *tx)
                .await?;
                return Ok(CommitResult::AlreadyApplied(CommitId(existing)));
            }
        };

        match op {
            CommitOp::Add { parts } => {
                insert_parts(&mut tx, hypertable_id, commit_id, &parts).await?;
            }
            CommitOp::Replace { old, new } => {
                tombstone_parts(&mut tx, hypertable_id, commit_id, &old).await?;
                insert_parts(&mut tx, hypertable_id, commit_id, &new).await?;
            }
            CommitOp::Delete { parts } => {
                tombstone_parts(&mut tx, hypertable_id, commit_id, &parts).await?;
            }
        }

        tx.commit().await?;
        Ok(CommitResult::Committed(commit_id))
    }
}

async fn insert_parts(
    tx: &mut Transaction<'_, Postgres>,
    hypertable_id: HypertableId,
    commit_id: CommitId,
    parts: &[PartMeta],
) -> Result<(), CatalogError> {
    for p in parts {
        sqlx::query(
            "INSERT INTO parts (hypertable_id, path, partition_values, packing_key_min, packing_key_max,
                                row_count, size_bytes, level, column_stats, created_by_commit)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
        )
        .bind(hypertable_id.0)
        .bind(&p.path)
        .bind(&p.partition_values)
        .bind(p.packing_key_min)
        .bind(p.packing_key_max)
        .bind(p.row_count)
        .bind(p.size_bytes)
        .bind(p.level)
        .bind(&p.column_stats)
        .bind(commit_id.0)
        .execute(&mut **tx)
        .await?;
    }
    Ok(())
}

/// Tombstones exactly the given live parts. If another committed transaction
/// already tombstoned any of them, fewer rows match and we return `Conflict`
/// (the transaction is rolled back by drop).
async fn tombstone_parts(
    tx: &mut Transaction<'_, Postgres>,
    hypertable_id: HypertableId,
    commit_id: CommitId,
    parts: &[PartId],
) -> Result<(), CatalogError> {
    let ids: Vec<i64> = parts.iter().map(|p| p.0).collect();
    let updated = sqlx::query(
        "UPDATE parts SET deleted_by_commit = $1
         WHERE id = ANY($2) AND hypertable_id = $3 AND deleted_by_commit IS NULL",
    )
    .bind(commit_id.0)
    .bind(&ids)
    .bind(hypertable_id.0)
    .execute(&mut **tx)
    .await?
    .rows_affected();

    if updated as usize != ids.len() {
        return Err(CatalogError::Conflict {
            expected: ids.len(),
            live_matched: updated as usize,
        });
    }
    Ok(())
}

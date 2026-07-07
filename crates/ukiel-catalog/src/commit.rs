use sqlx::{Postgres, Transaction};
use ukiel_core::{CommitId, CommitOp, CommitResult, HypertableId, PartId, PartMeta};

use crate::offsets::OffsetRange;
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
        self.commit_inner(hypertable_id, op, idempotency_key, &[])
            .await
    }

    /// Like `commit`, additionally advancing ingest offsets in the same
    /// transaction. Used by ingest workers for exactly-once delivery: on a
    /// failed commit, offsets do not move and the worker replays from the
    /// catalog's stored position.
    pub async fn commit_with_offsets(
        &self,
        hypertable_id: HypertableId,
        op: CommitOp,
        offsets: &[OffsetRange],
    ) -> Result<CommitResult, CatalogError> {
        self.commit_inner(hypertable_id, op, None, offsets).await
    }

    async fn commit_inner(
        &self,
        hypertable_id: HypertableId,
        op: CommitOp,
        idempotency_key: Option<&str>,
        offsets: &[OffsetRange],
    ) -> Result<CommitResult, CatalogError> {
        // Time the whole transaction — success, conflict, or offset-race all
        // cost latency (metrics P1). `kind` captured before `op` is moved.
        let kind = op.kind();
        let started = std::time::Instant::now();
        let result = self
            .commit_txn(hypertable_id, op, idempotency_key, offsets)
            .await;
        metrics::histogram!("catalog_commit_duration_seconds", "kind" => kind)
            .record(started.elapsed().as_secs_f64());
        result
    }

    async fn commit_txn(
        &self,
        hypertable_id: HypertableId,
        op: CommitOp,
        idempotency_key: Option<&str>,
        offsets: &[OffsetRange],
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

        for range in offsets {
            // CAS, not upsert (issue 0003): the update only applies while the
            // stored cursor has not advanced past this range's start. `<=`
            // (not `=`) keeps offset gaps legal (compacted topics). Zero rows
            // affected = another writer got here first: roll everything back.
            let affected = sqlx::query(
                "INSERT INTO ingest_offsets (hypertable_id, topic, kafka_partition, next_offset)
                 VALUES ($1, $2, $3, $4)
                 ON CONFLICT (hypertable_id, topic, kafka_partition)
                 DO UPDATE SET next_offset = EXCLUDED.next_offset, updated_at = now()
                 WHERE ingest_offsets.next_offset <= $5",
            )
            .bind(hypertable_id.0)
            .bind(&range.topic)
            .bind(range.partition)
            .bind(range.last + 1)
            .bind(range.first)
            .execute(&mut *tx)
            .await?
            .rows_affected();

            if affected == 0 {
                return Err(CatalogError::OffsetRace {
                    topic: range.topic.clone(),
                    partition: range.partition,
                });
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
    // A committed part's object is now catalog-tracked; drop its upload-intent
    // row so the GC sweeper stops treating it as a potential orphan.
    let paths: Vec<String> = parts.iter().map(|p| p.path.clone()).collect();
    sqlx::query("DELETE FROM pending_objects WHERE path = ANY($1)")
        .bind(&paths)
        .execute(&mut **tx)
        .await?;
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

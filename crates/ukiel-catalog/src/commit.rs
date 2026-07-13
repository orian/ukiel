use sqlx::{Postgres, Transaction};
use ukiel_core::{
    CommitId, CommitOp, CommitResult, HypertableId, OperationIdentity, PartId, PartMeta,
};

use crate::CompactionLease;
use crate::offsets::OffsetRange;
use crate::{CatalogError, PostgresCatalog};

/// What the catalog knows about one operation. There is no third answer: a
/// rolled-back attempt left nothing behind, and an object in the store proves
/// nothing (uploads precede the transaction, and orphans are ordinary).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperationLookup {
    /// No commit carries this key. The work did not land — but the *old worker*
    /// is gone too, so this is permission to replan from authority, never to
    /// resume a stale payload.
    Absent,
    /// This exact intent is durable, under this commit.
    Committed(CommitId),
}

impl PostgresCatalog {
    /// Atomically apply `op`. REPLACE/DELETE fail with `CatalogError::Conflict`
    /// if any input part is no longer live (another commit tombstoned it first);
    /// the caller should re-read live parts and retry.
    ///
    /// With an `identity`, the commit is *reconcilable*: a replay of the same
    /// intent returns the original commit instead of applying anything, and a
    /// lost acknowledgement becomes `AmbiguousMutation` rather than an unknown.
    pub async fn commit(
        &self,
        hypertable_id: HypertableId,
        op: CommitOp,
        identity: Option<&OperationIdentity>,
    ) -> Result<CommitResult, CatalogError> {
        self.commit_inner(hypertable_id, op, identity, &[], None)
            .await
    }

    /// Like `commit`, additionally advancing ingest offsets in the same
    /// transaction. Used by ingest workers for exactly-once delivery: on a
    /// failed commit, offsets do not move and the worker replays from the
    /// catalog's stored position.
    ///
    /// The identity is **required**. An ingest commit whose acknowledgement is
    /// lost is precisely the case plan 43 exists to decide, and one that carries
    /// no key cannot be decided at all — so the type system refuses it rather
    /// than leaving it to a convention nobody checks at 3am.
    pub async fn commit_with_offsets(
        &self,
        hypertable_id: HypertableId,
        op: CommitOp,
        offsets: &[OffsetRange],
        identity: &OperationIdentity,
    ) -> Result<CommitResult, CatalogError> {
        self.commit_inner(hypertable_id, op, Some(identity), offsets, None)
            .await
    }

    /// A compaction REPLACE fenced by the partition lease the worker has held
    /// since before it opened a Parquet file (plan 41).
    ///
    /// The lease row is locked and validated — owner, generation, and unexpired
    /// by PostgreSQL's clock — inside the same transaction that tombstones the
    /// inputs, so a zombie that stalled past its TTL cannot commit over the
    /// successor that took its partition. The lock is held for this short
    /// catalog transaction only, never across object work: it serializes
    /// takeover against commit, so a tenancy still valid at this instant is
    /// allowed to finish and a takeover simply waits and gets the next
    /// generation.
    ///
    /// The fence is *scheduling* ownership, not data integrity. Ingest,
    /// deletion, and operator actions take no lease, so the optimistic conflict
    /// check underneath stays the final protection and can still return
    /// `Conflict` after the fence passes.
    ///
    /// The identity is **required**, and it is what the replay path keys on: a
    /// merge whose acknowledgement was lost is reconciled here *before* the
    /// lease is judged, so a worker whose lease expired while it was away is
    /// told its work is durable rather than told it lost.
    pub async fn commit_compaction_replace(
        &self,
        hypertable_id: HypertableId,
        lease: &CompactionLease,
        old: Vec<PartId>,
        new: Vec<PartMeta>,
        identity: &OperationIdentity,
    ) -> Result<CommitResult, CatalogError> {
        self.commit_inner(
            hypertable_id,
            CommitOp::Replace { old, new },
            Some(identity),
            &[],
            Some(lease),
        )
        .await
    }

    /// Did this exact operation land? The authoritative answer, and the only
    /// one: object-store state cannot be used to infer it (uploads precede the
    /// transaction), and neither can a worker's memory of what it was doing.
    ///
    /// A stored key whose fingerprint differs — or is unknowable, as every
    /// pre-plan-43 keyed row's is — is `OperationKeyCollision`, never
    /// `Committed`. Answering "yes" for somebody else's commit is the one
    /// failure this must not have.
    pub async fn lookup_operation(
        &self,
        identity: &OperationIdentity,
    ) -> Result<OperationLookup, CatalogError> {
        let row: Option<(i64, Option<Vec<u8>>)> = sqlx::query_as(
            "SELECT id, operation_fingerprint FROM commits
             WHERE hypertable_id = $1 AND idempotency_key = $2",
        )
        .bind(identity.hypertable_id().0)
        .bind(identity.key())
        .fetch_optional(&self.pool)
        .await?;
        match reconcile(identity, row)? {
            Some(id) => Ok(OperationLookup::Committed(id)),
            None => Ok(OperationLookup::Absent),
        }
    }

    async fn commit_inner(
        &self,
        hypertable_id: HypertableId,
        op: CommitOp,
        identity: Option<&OperationIdentity>,
        offsets: &[OffsetRange],
        lease: Option<&CompactionLease>,
    ) -> Result<CommitResult, CatalogError> {
        // Time the whole transaction — success, conflict, or offset-race all
        // cost latency (metrics P1). `kind` captured before `op` is moved.
        let kind = op.kind();
        let started = std::time::Instant::now();
        let result = self
            .commit_txn(hypertable_id, op, identity, offsets, lease)
            .await;
        metrics::histogram!("catalog_commit_duration_seconds", "kind" => kind)
            .record(started.elapsed().as_secs_f64());

        // Plan 42: outcomes carry their *class*, so an operator can tell a
        // partition being handed off (ownership) from a primary going away
        // (transport) without reading log text. A transport failure here is the
        // one that matters most: the commit's outcome is unknown, not "no".
        let error_class = match &result {
            Ok(_) => "none",
            Err(e) => e.class().as_label(),
        };
        let outcome = if result.is_ok() { "ok" } else { "error" };
        metrics::counter!(
            "catalog_operation_total",
            "operation" => kind,
            "outcome" => outcome,
            "error_class" => error_class,
        )
        .increment(1);
        result
    }

    async fn commit_txn(
        &self,
        hypertable_id: HypertableId,
        op: CommitOp,
        identity: Option<&OperationIdentity>,
        offsets: &[OffsetRange],
        lease: Option<&CompactionLease>,
    ) -> Result<CommitResult, CatalogError> {
        // An identity is scoped to its hypertable. One that travels between
        // tables is not an identity, and it would make the catalog's uniqueness
        // scope and the caller's disagree.
        if let Some(identity) = identity
            && identity.hypertable_id() != hypertable_id
        {
            return Err(CatalogError::OperationHypertableMismatch {
                identity: identity.hypertable_id().0,
                mutation: hypertable_id.0,
            });
        }

        let mut tx = self.pool.begin().await?;

        if let Some(lease) = lease {
            // Ordering, deliberately: a replay of an operation that already
            // landed is *reconciled* before a lost lease is called a failure.
            // A worker whose commit acknowledgement was lost must learn that its
            // work is durable, not be told it lost the race and redo it. Its
            // lease has very likely expired by then — that is what a lost
            // acknowledgement plus a recovery looks like — and judging the lease
            // first would answer `LeaseLost` for work that is already committed,
            // sending a second worker to redo a merge that is already durable.
            if let Some(identity) = identity
                && let Some(existing) = lookup_in_tx(&mut tx, identity).await?
            {
                return Ok(CommitResult::AlreadyApplied(existing));
            }
            fence_lease(&mut tx, hypertable_id, lease, &op).await?;
        }

        let inserted: Option<i64> = sqlx::query_scalar(
            "INSERT INTO commits (hypertable_id, kind, idempotency_key, operation_fingerprint)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (hypertable_id, idempotency_key) WHERE idempotency_key IS NOT NULL
             DO NOTHING
             RETURNING id",
        )
        .bind(hypertable_id.0)
        .bind(op.kind())
        .bind(identity.map(|i| i.key()))
        .bind(identity.map(|i| i.fingerprint().to_vec()))
        .fetch_optional(&mut *tx)
        .await?;

        let commit_id = match inserted {
            Some(id) => CommitId(id),
            None => {
                // The key is taken. Same intent → the work is already done and
                // this attempt applies nothing. Different (or unknowable) intent
                // → a collision, and the caller hears about it.
                let identity =
                    identity.expect("a conflict requires a key, and a key requires an identity");
                let existing = lookup_in_tx(&mut tx, identity).await?.ok_or_else(|| {
                    CatalogError::NotFound(format!(
                        "commit for operation {} vanished between conflict and read",
                        identity.key()
                    ))
                })?;
                return Ok(CommitResult::AlreadyApplied(existing));
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
            //
            // Idempotency does not replace this. A *replay* of the same intent
            // never reaches here (it returned AlreadyApplied above); reaching
            // here with an advanced cursor means a genuinely different operation
            // overlaps an already-consumed range, which is a duplicate ingest
            // worker — an OffsetRace, not a retry.
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

        // The boundary. Everything before this point failed *definitely*: a
        // conflict, an offset race, a lost lease, a bad statement — all of them
        // rolled back, all of them "no". Only here can the transaction be
        // durable while the answer is lost on the way home.
        if let Err(e) = tx.commit().await {
            return Err(ambiguous_or_definite(identity, e));
        }
        Ok(CommitResult::Committed(commit_id))
    }
}

/// Classifies a failure returned by `COMMIT` itself.
///
/// Only a *transport* failure is ambiguous: the server may have committed and
/// lost the acknowledgement. A retryable-database error (serialization failure,
/// deadlock) is a definite rollback — PostgreSQL is explicitly telling us the
/// transaction did not happen — and anything else is a definite failure too. All
/// of those keep their existing meaning; calling them ambiguous would send the
/// supervisor looking up an operation that provably is not there.
fn ambiguous_or_definite(identity: Option<&OperationIdentity>, e: sqlx::Error) -> CatalogError {
    let definite = CatalogError::Db(e);
    if definite.class() != crate::CatalogErrorClass::Transport {
        return definite;
    }
    let CatalogError::Db(e) = definite else {
        unreachable!("just constructed as Db")
    };
    match identity {
        Some(identity) => CatalogError::AmbiguousMutation {
            key: identity.key().to_string(),
            identity: Box::new(identity.clone()),
            source: e,
        },
        // Unknown *and* undecidable. Loud, and never quietly retried: inventing
        // a key now would be fabricating the evidence for the answer.
        None => CatalogError::UnidentifiedAmbiguousMutation { source: e },
    }
}

/// `Some(commit)` when this exact intent is already recorded, `None` when the
/// key is free. A key under different — or unknowable — intent is a collision,
/// which is the whole reason the fingerprint is stored beside the key.
fn reconcile(
    identity: &OperationIdentity,
    row: Option<(i64, Option<Vec<u8>>)>,
) -> Result<Option<CommitId>, CatalogError> {
    let Some((id, stored)) = row else {
        return Ok(None);
    };
    let collision = || CatalogError::OperationKeyCollision {
        key: identity.key().to_string(),
    };
    // A historical keyed commit has no fingerprint and its intent cannot be
    // recovered. "Probably the same thing" is not an answer a reconciliation
    // protocol is allowed to give.
    let stored = stored.ok_or_else(collision)?;
    if stored.as_slice() != identity.fingerprint().as_slice() {
        return Err(collision());
    }
    Ok(Some(CommitId(id)))
}

async fn lookup_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    identity: &OperationIdentity,
) -> Result<Option<CommitId>, CatalogError> {
    let row: Option<(i64, Option<Vec<u8>>)> = sqlx::query_as(
        "SELECT id, operation_fingerprint FROM commits
         WHERE hypertable_id = $1 AND idempotency_key = $2",
    )
    .bind(identity.hypertable_id().0)
    .bind(identity.key())
    .fetch_optional(&mut **tx)
    .await?;
    reconcile(identity, row)
}

/// Locks the lease row and proves this worker still owns the partition it is
/// about to rewrite, then proves the rewrite stays inside that partition.
/// Everything the caller passed is checked against the database, not trusted:
/// the `CompactionLease` value is a token, not an authority.
async fn fence_lease(
    tx: &mut Transaction<'_, Postgres>,
    hypertable_id: HypertableId,
    lease: &CompactionLease,
    op: &CommitOp,
) -> Result<(), CatalogError> {
    let partition = &lease.partition_values;
    if hypertable_id != lease.hypertable_id {
        return Err(CatalogError::LeasePartitionMismatch {
            partition: partition.to_string(),
        });
    }

    // FOR UPDATE: a concurrent takeover blocks here until this transaction
    // ends, then reads the post-commit truth. No object work happens under it.
    let held: Option<(uuid::Uuid, i64, bool)> = sqlx::query_as(
        "SELECT owner_id, generation, expires_at > clock_timestamp()
         FROM compaction_leases
         WHERE hypertable_id = $1 AND partition_values = $2
         FOR UPDATE",
    )
    .bind(hypertable_id.0)
    .bind(partition)
    .fetch_optional(&mut **tx)
    .await?;

    let ours = held.is_some_and(|(owner, generation, unexpired)| {
        owner == lease.owner_id && generation == lease.generation && unexpired
    });
    if !ours {
        return Err(CatalogError::LeaseLost {
            partition: partition.to_string(),
        });
    }

    // A lease on partition A must not license a rewrite of partition B: without
    // this, one caller bug would turn the fence into a rubber stamp for parts
    // nobody claimed.
    let CommitOp::Replace { old, new } = op else {
        return Err(CatalogError::LeasePartitionMismatch {
            partition: partition.to_string(),
        });
    };
    if new.iter().any(|p| &p.partition_values != partition) {
        return Err(CatalogError::LeasePartitionMismatch {
            partition: partition.to_string(),
        });
    }
    let ids: Vec<i64> = old.iter().map(|p| p.0).collect();
    let inside: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM parts
         WHERE id = ANY($1) AND hypertable_id = $2 AND partition_values = $3",
    )
    .bind(&ids)
    .bind(hypertable_id.0)
    .bind(partition)
    .fetch_one(&mut **tx)
    .await?;
    if inside != ids.len() as i64 {
        return Err(CatalogError::LeasePartitionMismatch {
            partition: partition.to_string(),
        });
    }
    Ok(())
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

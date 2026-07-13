//! Compaction partition leases (plan 41).
//!
//! One renewable lease per `(hypertable, partition)`, claimed *before* a worker
//! reads a single Parquet byte. Postgres time is authoritative for expiry, so a
//! stalled worker's tenancy ends on schedule whatever its own clock believes.
//!
//! Contention is not an error: a partition already owned by a live peer returns
//! `Ok(None)` and the caller moves to the next candidate. Only a *proven* loss
//! of a lease we held (`renew` → `None`) or an unknown outcome (an `Err`) must
//! stop object work.

use std::time::Duration;

use chrono::{DateTime, Utc};
use serde_json::Value;
use ukiel_core::HypertableId;
use uuid::Uuid;

use crate::{CatalogError, PostgresCatalog};

/// A held claim on one partition's compaction. `owner_id` + `generation`
/// identify the tenancy; the REPLACE transaction re-reads and fences on both,
/// so this struct is a token, never a cached authority.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactionLease {
    pub hypertable_id: HypertableId,
    pub partition_values: Value,
    pub owner_id: Uuid,
    pub generation: i64,
    pub expires_at: DateTime<Utc>,
}

#[derive(sqlx::FromRow)]
struct LeaseRow {
    hypertable_id: i64,
    partition_values: Value,
    owner_id: Uuid,
    generation: i64,
    expires_at: DateTime<Utc>,
}

impl From<LeaseRow> for CompactionLease {
    fn from(r: LeaseRow) -> Self {
        CompactionLease {
            hypertable_id: HypertableId(r.hypertable_id),
            partition_values: r.partition_values,
            owner_id: r.owner_id,
            generation: r.generation,
            expires_at: r.expires_at,
        }
    }
}

const LEASE_COLUMNS: &str = "hypertable_id, partition_values, owner_id, generation, expires_at";

/// Whole milliseconds, rejecting a TTL that is zero or too large for a
/// PostgreSQL interval. Integer milliseconds keep the interval arithmetic exact
/// (`$n * INTERVAL '1 millisecond'`), unlike a float `secs =>` argument.
fn ttl_millis(ttl: Duration) -> Result<i64, CatalogError> {
    let ms = ttl.as_millis();
    if ms == 0 {
        return Err(CatalogError::InvalidLeaseTtl(
            "must be greater than zero".into(),
        ));
    }
    i64::try_from(ms).map_err(|_| CatalogError::InvalidLeaseTtl("exceeds i64 milliseconds".into()))
}

impl PostgresCatalog {
    /// Claims a partition for `ttl`, or reports that a live peer holds it.
    ///
    /// One statement, so the claim is atomic under any number of contenders.
    /// It takes the row when it is free or expired (reclamation, generation+1),
    /// and refreshes it idempotently when we already hold it (generation
    /// preserved — the caller's in-flight merge keeps its fence). Any other
    /// unexpired owner is `Ok(None)`: ordinary scheduling contention.
    pub async fn try_acquire_compaction_lease(
        &self,
        hypertable_id: HypertableId,
        partition: &Value,
        owner_id: Uuid,
        ttl: Duration,
    ) -> Result<Option<CompactionLease>, CatalogError> {
        let ms = ttl_millis(ttl)?;
        let sql = format!(
            "INSERT INTO compaction_leases
                 (hypertable_id, partition_values, owner_id, generation, expires_at)
             VALUES ($1, $2, $3, 1, clock_timestamp() + $4 * INTERVAL '1 millisecond')
             ON CONFLICT (hypertable_id, partition_values) DO UPDATE
             SET owner_id = EXCLUDED.owner_id,
                 generation = CASE
                     WHEN compaction_leases.owner_id = EXCLUDED.owner_id
                      AND compaction_leases.expires_at > clock_timestamp()
                     THEN compaction_leases.generation
                     ELSE compaction_leases.generation + 1
                 END,
                 expires_at = EXCLUDED.expires_at,
                 updated_at = clock_timestamp()
             WHERE compaction_leases.expires_at <= clock_timestamp()
                OR compaction_leases.owner_id = EXCLUDED.owner_id
             RETURNING {LEASE_COLUMNS}"
        );
        let row: Option<LeaseRow> = sqlx::query_as(sqlx::AssertSqlSafe(sql))
            .bind(hypertable_id.0)
            .bind(partition)
            .bind(owner_id)
            .bind(ms)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(CompactionLease::from))
    }

    /// Extends a held lease by `ttl`, keeping its generation (an in-flight
    /// merge's fence must survive renewal).
    ///
    /// `Ok(None)` is a *proven* loss — the row was taken over, released, or had
    /// already expired when this statement ran — and the caller must abandon
    /// its object work. An `Err` leaves ownership unknown, which is not weaker:
    /// the caller must stop just the same.
    pub async fn renew_compaction_lease(
        &self,
        lease: &CompactionLease,
        ttl: Duration,
    ) -> Result<Option<CompactionLease>, CatalogError> {
        let ms = ttl_millis(ttl)?;
        let sql = format!(
            "UPDATE compaction_leases
             SET expires_at = clock_timestamp() + $5 * INTERVAL '1 millisecond',
                 updated_at = clock_timestamp()
             WHERE hypertable_id = $1 AND partition_values = $2
               AND owner_id = $3 AND generation = $4
               AND expires_at > clock_timestamp()
             RETURNING {LEASE_COLUMNS}"
        );
        let row: Option<LeaseRow> = sqlx::query_as(sqlx::AssertSqlSafe(sql))
            .bind(lease.hypertable_id.0)
            .bind(&lease.partition_values)
            .bind(lease.owner_id)
            .bind(lease.generation)
            .bind(ms)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(CompactionLease::from))
    }

    /// Hands a partition back early. Best-effort courtesy on a clean exit — a
    /// crashed worker's lease simply expires — and scoped to the exact
    /// owner/generation, so a stale worker cannot release its successor's
    /// claim. `false` = the lease was already gone or already someone else's.
    pub async fn release_compaction_lease(
        &self,
        lease: &CompactionLease,
    ) -> Result<bool, CatalogError> {
        let deleted = sqlx::query(
            "DELETE FROM compaction_leases
             WHERE hypertable_id = $1 AND partition_values = $2
               AND owner_id = $3 AND generation = $4",
        )
        .bind(lease.hypertable_id.0)
        .bind(&lease.partition_values)
        .bind(lease.owner_id)
        .bind(lease.generation)
        .execute(&self.pool)
        .await?
        .rows_affected();
        Ok(deleted > 0)
    }

    /// `(unexpired leases, age in seconds of the oldest expired one)` — the
    /// `catalog_compaction_leases_active` /
    /// `catalog_compaction_lease_oldest_expired_age_seconds` gauges. An expired
    /// row lingering far past its TTL means nobody is reclaiming that partition:
    /// the compactor fleet is wedged or gone. Database time on both sides; the
    /// table holds one row per partition under compaction.
    pub async fn compaction_lease_stats(&self) -> Result<(i64, f64), CatalogError> {
        let (active, oldest): (i64, f64) = sqlx::query_as(
            "SELECT count(*) FILTER (WHERE expires_at > clock_timestamp()),
                    coalesce(max(EXTRACT(EPOCH FROM (clock_timestamp() - expires_at)))
                             FILTER (WHERE expires_at <= clock_timestamp()), 0)::float8
             FROM compaction_leases",
        )
        .fetch_one(&self.pool)
        .await?;
        Ok((active, oldest))
    }
}

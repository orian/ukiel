# Ukiel Plan 41: Compaction Partition Leases and Monotonic Worker Cursors Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make active-active compaction efficient and failover-safe. Plan 40 measured that 32 contenders on one partition complete the same **58** useful replacements as 2 contenders while discarding **55.7%** of attempts; on a Zipf-hot partition useful throughput falls from 971 swaps at 8 contenders to 678 at 32. The catalog-only benchmark understates the product cost: `Compactor::merge_parts` reads Parquet, performs the streaming merge, and uploads every output before REPLACE discovers the loser. This plan assigns one renewable, database-time lease per `(hypertable_id, partition_values)` before object work, fences that lease generation in the REPLACE transaction, and makes worker cursors monotonic without allowing a shared cursor to hide abandoned compaction.

**Architecture — lease the stable scheduling unit, keep REPLACE as correctness.** A compaction lease covers a whole partition, not individual parts and not one level. Part sets and levels change while compaction runs; individual-part leases would require multi-row acquisition, permit overlapping level/finalization work, and fragment as soon as a REPLACE lands. The partition is the unit already used by merge planning and finalization, and serializing work inside it loses no measured throughput: plan 40 shows same-partition concurrency adds no useful work, while partition-disjoint work scales.

Workers discover candidate partitions from **current live state**, atomically try to claim them, then load and recheck only the claimed partition. The change feed remains a wake-up/observability mechanism, never a correctness gate. This distinction is necessary for a shared fleet cursor: worker B may advance the cursor past an event while worker A holds its partition; if A dies, the expired lease must leave discoverable work even when no new event arrives.

Each lease row contains an opaque process-instance owner id, a monotonically increasing generation, and `expires_at` derived only from PostgreSQL time. Acquisition/reclamation increments the generation. Renewal preserves it. The compactor renews independently of merge/commit progress and cancels object work when renewal proves ownership lost. The final REPLACE locks and validates the lease row, owner, generation, and expiry in the same catalog transaction before tombstoning inputs. A stale worker therefore cannot commit after handoff. Optimistic REPLACE stays in place because ingest, deletion, and operator actions do not take compaction leases; it remains the final protection against every race outside the scheduler.

**Cursor model.** `set_cursor` becomes monotonic (`GREATEST`/conditional update semantics) for every worker family: a delayed replica can never move a cursor backwards and prolong GC fences or falsify feed lag. Compactors may share one logical cursor name for fleet-level wake progress, but candidate discovery does not depend on it. Cursor advancement means “the fleet observed this feed prefix,” not “every derived rewrite completed.” Durable completion is represented by current live parts, not the cursor.

**HA boundary.** This plan supplies the HA design's optional compactor scheduling leases and monotonic cursor prerequisite. It does not replace the HA plan's ambiguous-mutation work: production active-active rollout remains gated on deterministic compactor operation identities/reconciliation so a lost commit acknowledgement is resolved rather than guessed. Lease loss, catalog unavailability, or an ambiguous renewal stops new merge work; none authorizes stale state.

**Tech stack:** PostgreSQL 17 row locking and database time; existing SQLx pool; `uuid` already used by compactor output paths; Tokio cancellation/tickers; no new service and no long-held database connection during object work.

**Prerequisites:** Plans 17 and 29 (current ladder/finalization and streaming merge), plan 40 (measured contention), and the HA design. All are present. HA phase 2's compactor operation-key reconciliation is a production rollout gate, not a compile-time prerequisite for implementing and testing this plan.

## Global Constraints

- Rust edition 2024, toolchain >= 1.96; workspace dependency pins stay unchanged.
- PostgreSQL time is authoritative. Never calculate lease expiry from a worker clock.
- No SQL transaction, row lock, or pooled connection is held while reading or writing object storage.
- A lease is not a data-integrity substitute. REPLACE conflict checking remains mandatory.
- Both ordinary ladder compaction and cold-partition finalization use the same partition lease.
- Deletion and ingest remain lease-free and race safely through REPLACE/current-state replanning.
- On catalog/renewal uncertainty, stop or cancel object work; do not assume the lease remains valid.
- Existing single-compactor behavior stays valid with default configuration.
- Every task ends with focused tests plus `cargo fmt --check` and `cargo clippy --all-targets -- -D warnings`.
- Conventional commits; no AI attribution.

---

### Task 1: Monotonic cursors and live-state candidate discovery

**Files:**
- Modify: `crates/ukiel-catalog/src/cursors.rs`
- Modify: `crates/ukiel-catalog/src/parts.rs`
- Modify: `crates/ukiel-compactor/src/compactor.rs`
- Test: `crates/ukiel-catalog/tests/catalog_test.rs`
- Test: `crates/ukiel-compactor/tests/compactor_test.rs`

**Interfaces:**

```rust
/// Advances a logical worker cursor, never retreats it. A stale/equal write is
/// a successful no-op and does not refresh updated_at.
pub async fn set_cursor(
    &self,
    worker: &str,
    hypertable_id: HypertableId,
    cursor: CommitId,
) -> Result<(), CatalogError>;

/// Partitions containing at least one level whose distinct live run count
/// meets that level's trigger. Aggregated in PostgreSQL; returns no part rows.
pub async fn compaction_candidates(
    &self,
    hypertable_id: HypertableId,
    l0_fanout: i64,
    fanout: i64,
    limit: i64,
) -> Result<Vec<serde_json::Value>, CatalogError>;
```

- `set_cursor` uses `ON CONFLICT ... DO UPDATE ... WHERE worker_cursors.last_commit_id < EXCLUDED.last_commit_id`. A losing/equal update must not change `updated_at`; otherwise a stale replica impersonates progress. Keep the existing API so MV/GC consumers inherit the safety property.
- `compaction_candidates` groups the partial live set by `(partition_values, level)`, counts distinct `created_by_commit`, applies `l0_fanout` to level 0 and `fanout` above it, then returns distinct partition values in deterministic order with a bounded limit. Migration 0007's `(hypertable_id, partition_values, level)` live-part path is the starting index; record `EXPLAIN (ANALYZE, BUFFERS)` before adding another index.
- Refactor the compactor so each tick asks current state for candidate partitions. After claiming is added in Task 4 it will fetch `live_partition_parts` only for a claimed candidate and recompute the eligible level from that snapshot. In this task, preserve behavior without leases. Remove `events.is_empty() => return` as a correctness gate; feed polling/cursor advancement may remain as cheap wake telemetry.

- [ ] **Step 1: Failing cursor tests.** Advance 7 -> 9 -> 8 and assert 9; race concurrent writers of 7/11/9 behind a barrier and assert 11; assert a stale/equal write leaves `updated_at` unchanged. Verify GC's minimum-cursor behavior cannot regress.
- [ ] **Step 2: Failing candidate tests.** Mixed partitions and levels: only threshold-satisfying partitions return, distinct once even if two levels qualify, limit is honored, tombstoned/sealed-in-future rows do not count (today only tombstoned is representable).
- [ ] **Step 3: Implement the SQL and refactor planning.** Add a compactor regression test that first advances its cursor to feed head, leaves a mergeable live-state backlog, and proves `run_once` still compacts it without a new feed event.
- [ ] **Step 4: Verify:** `cargo test -p ukiel-catalog -- --test-threads=2 && cargo test -p ukiel-compactor -- --test-threads=2`.
- [ ] **Step 5: Commit:**

```bash
git commit -m "fix: monotonic worker cursors and live-state compaction discovery"
```

---

### Task 2: Catalog lease schema and atomic ownership primitives

**Files:**
- Create: `crates/ukiel-catalog/migrations/0008_compaction_leases.sql`
- Create: `crates/ukiel-catalog/src/leases.rs`
- Modify: `crates/ukiel-catalog/src/lib.rs`
- Modify: `crates/ukiel-catalog/src/error.rs`
- Test: `crates/ukiel-catalog/tests/catalog_test.rs`

**Schema:**

```sql
CREATE TABLE compaction_leases (
    hypertable_id BIGINT NOT NULL REFERENCES hypertables(id) ON DELETE CASCADE,
    partition_values JSONB NOT NULL,
    owner_id UUID NOT NULL,
    generation BIGINT NOT NULL CHECK (generation > 0),
    expires_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (hypertable_id, partition_values)
);
CREATE INDEX compaction_leases_expiry_idx ON compaction_leases (expires_at);
```

**Interfaces:**

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactionLease {
    pub hypertable_id: HypertableId,
    pub partition_values: serde_json::Value,
    pub owner_id: uuid::Uuid,
    pub generation: i64,
    pub expires_at: chrono::DateTime<chrono::Utc>,
}

pub async fn try_acquire_compaction_lease(
    &self, hypertable_id: HypertableId, partition: &Value,
    owner_id: Uuid, ttl: Duration,
) -> Result<Option<CompactionLease>, CatalogError>;
pub async fn renew_compaction_lease(
    &self, lease: &CompactionLease, ttl: Duration,
) -> Result<Option<CompactionLease>, CatalogError>;
pub async fn release_compaction_lease(
    &self, lease: &CompactionLease,
) -> Result<bool, CatalogError>;
```

- Acquisition is one `INSERT ... ON CONFLICT DO UPDATE` statement. It may replace only an expired row (or idempotently return the caller's still-current ownership); reclamation increments generation. A different unexpired owner receives `Ok(None)`, not an error.
- Renewal updates only the exact `(key, owner_id, generation)` while it is still unexpired and preserves generation. Zero rows means proven lease loss. SQL errors mean ownership is unknown and the worker must stop object work.
- Release deletes only the exact owner/generation. It is best-effort on shutdown and never required for correctness.
- All expiry expressions use `clock_timestamp() + make_interval(...)`. Validate TTL in Rust (`> 0`, fits PostgreSQL interval); bind integer milliseconds to avoid floating-point interval ambiguity.
- Do not use advisory locks or `SELECT ... FOR UPDATE SKIP LOCKED` across the merge. No connection remains checked out during object I/O.

- [ ] **Step 1: Write failing catalog integration tests:** one winner under 32 simultaneous acquirers; loser sees `None`; same owner idempotently reacquires without generation churn; forced expiry permits takeover and increments generation; stale renew/release cannot touch the new owner; renewal extends expiry without changing generation; different partitions acquire independently.
- [ ] **Step 2: Implement migration, types, and APIs.** Add `CatalogError::LeaseLost` only for fenced mutations in Task 3; acquisition contention itself remains an ordinary `None`.
- [ ] **Step 3: Verify migration from a pre-0008 catalog and fresh bootstrap:** `cargo test -p ukiel-catalog -- --test-threads=2`.
- [ ] **Step 4: Commit:**

```bash
git commit -m "feat: renewable catalog leases for compaction partitions"
```

---

### Task 3: Fence lease generation inside the REPLACE transaction

**Files:**
- Modify: `crates/ukiel-catalog/src/commit.rs`
- Modify: `crates/ukiel-catalog/src/error.rs`
- Modify: `crates/ukiel-catalog/src/leases.rs`
- Test: `crates/ukiel-catalog/tests/catalog_test.rs`

**Interface:**

```rust
pub async fn commit_compaction_replace(
    &self,
    hypertable_id: HypertableId,
    lease: &CompactionLease,
    old: Vec<PartId>,
    new: Vec<PartMeta>,
    idempotency_key: Option<&str>,
) -> Result<CommitResult, CatalogError>;
```

- Begin the normal catalog commit transaction, lock the exact lease row `FOR UPDATE`, then validate owner, generation, and `expires_at > clock_timestamp()`. This lock is held only for the short catalog transaction, never object work. It serializes lease takeover with the commit: ownership valid at that point may finish; a takeover waits and receives the next generation afterward.
- Validate that every old live part and every new `PartMeta` belongs to the lease's hypertable/partition. A worker must not be able to fence partition A while replacing partition B due to a caller bug.
- On missing, expired, wrong-owner, or wrong-generation ownership, return `CatalogError::LeaseLost` and insert no commit/parts. Preserve the ordinary `CatalogError::Conflict` path for ingest/deletion races after lease validation.
- Route through the existing commit machinery rather than duplicate part insertion, idempotency, intent clearing, and feed emission. The generic `commit` API remains unchanged for ingest/deletion/tests.

- [ ] **Step 1: Failing tests:** current generation commits; expired/stale generation cannot commit; owner mismatch cannot commit; partition mismatch cannot commit; two stale/current commits serialize with only the current owner accepted; an ingest ADD racing a leased replacement remains correct; deletion can still make the replacement conflict.
- [ ] **Step 2: Implement the transactional fence and named error.** Ensure an idempotency-key replay is reconciled before treating an expired lease as failure once HA phase 2 supplies canonical compactor operation identities; document/test the intended ordering now so the later splice is unambiguous.
- [ ] **Step 3: Verify:** `cargo test -p ukiel-catalog -- --test-threads=2`.
- [ ] **Step 4: Commit:**

```bash
git commit -m "feat: fence compaction replace commits by partition lease generation"
```

---

### Task 4: Lease-aware compactor, renewal, handoff, and finalization

**Files:**
- Modify: `crates/ukiel-compactor/src/compactor.rs`
- Modify: `crates/ukiel-compactor/src/error.rs`
- Test: `crates/ukiel-compactor/tests/compactor_test.rs`
- Test support: counting/delaying object-store wrapper in the existing test module

**Behavior:**

1. A process-stable UUID is created once in `Compactor::new` (allow deterministic injection in tests).
2. For each live-state candidate, try to acquire the partition lease before loading parts or opening Parquet.
3. Load `live_partition_parts`, re-evaluate eligible levels, and perform at most one ladder merge for the partition in the pass.
4. A renewal loop ticks independently at `lease_renew_interval`; its first proven loss or catalog error cancels/drops the merge future. Do not wait for the commit to renew.
5. `merge_parts` commits through `commit_compaction_replace` with the held generation.
6. Release after success, conflict, no-longer-eligible replan, or ordinary failure; expiry is the crash fallback.
7. `finalize_once` claims the same partition lease before its quiet/live-state rechecks and merge. It cannot overlap a ladder merge for that partition.

Treat `LeaseLost` as expected scheduling churn, not a fatal worker exit. A genuine catalog error still stops/degrades the role according to the HA state machine. Uploaded output from a loss near commit remains registered pending/orphan data and is handled by existing GC. Never clear upload intents merely because the lease was lost.

- [ ] **Step 1: Replace the competing-compactors test's weak assertion with an efficiency assertion.** Two workers synchronized on the same candidate: exactly one enters Parquet reads/upload, one useful merge lands, zero catalog conflicts, all rows remain. Separate partitions still let both workers perform useful work.
- [ ] **Step 2: Add adversarial handoff tests.** Force owner A's lease to expire while paused before commit, let B acquire generation N+1, then resume A: A receives `LeaseLost`, B commits, data is intact. Kill A without release; after forced expiry B takes over and the backlog converges without a new feed event.
- [ ] **Step 3: Add renewal coverage.** A deliberately slow merge survives beyond one initial TTL because renewals succeed. Forced renewal loss cancels before REPLACE; no live inputs are tombstoned and completed uploads remain intent-covered.
- [ ] **Step 4: Integrate both ladder and finalizer paths.** Preserve one-merge-per-partition-per-pass, placement, streaming memory bounds, and current conflict replanning for non-compactor races.
- [ ] **Step 5: Verify:** `cargo test -p ukiel-compactor -- --test-threads=2 && make e2e` (S6 equivalence, S8 object hygiene).
- [ ] **Step 6: Commit:**

```bash
git commit -m "feat: lease partition compaction before parquet work"
```

---

### Task 5: ukield configuration, fleet identity, and observability

**Files:**
- Modify: `crates/ukield/src/config.rs`
- Modify: `crates/ukield/src/run.rs`
- Modify: `crates/ukield/src/collector.rs`
- Modify: `ukield.example.toml`
- Test: ukield config/unit tests

**Configuration:**

```toml
[compactor]
lease_ttl_secs = 60
lease_renew_interval_secs = 20
candidate_limit = 64
```

- Generate one owner UUID per ukield process start and pass it to the compactor. Log it and attach it to lease-loss/acquisition traces; do not use pod name alone because names can be reused after restart.
- Validate `lease_ttl_secs > 0`, `renew_interval > 0`, and `renew_interval <= ttl / 3`. Defaults tolerate transient scheduler/network stalls without making failover unreasonably slow. TTL affects efficiency/failover delay, not correctness.
- Respect plan 40's fleet-wide connection budget: lease renewal/acquisition uses the existing catalog pool and adds no dedicated connection per lease.
- Add metrics:
  - `compactor_lease_acquire_total{outcome="acquired|held"}`;
  - `compactor_lease_renew_total{outcome="ok|lost|error"}`;
  - `compactor_lease_release_total{outcome="released|stale|error"}`;
  - `compactor_lease_lost_total{stage="merge|commit"}`;
  - `compactor_partitions_claimed_total{kind="ladder|finalize"}`;
  - catalog gauge for active/unexpired compaction leases and oldest expired lease age.
- Existing `compactor_conflicts_total` stays: it should collapse for compactor/compactor races but still reveal ingest/deletion races.

- [ ] **Step 1: Failing config tests** for defaults and invalid TTL/renewal combinations.
- [ ] **Step 2: Wire owner/config and metrics.** Collector SQL must use database time and remain bounded by the small lease table/index.
- [ ] **Step 3: Verify:** `cargo test -p ukield -- --test-threads=2 && cargo test -p ukiel-compactor -- --test-threads=2`.
- [ ] **Step 4: Commit:**

```bash
git commit -m "feat: configure and observe active-active compaction leases"
```

---

### Task 6: Re-run contention, failure, and HA acceptance; close out

**Files:**
- Modify: `crates/ukiel-e2e/src/bin/bench/catalog_load.rs` (lease-enabled conflict mode)
- Modify: `bench/README.md`
- Modify: `docs/notes/2026-07-13-catalog-scalability.md`
- Modify: `docs/superpowers/specs/2026-07-13-ukiel-high-availability.md`
- Modify: `docs/superpowers/plans/2026-07-05-ukiel-v1-roadmap.md`

**Measurement and acceptance:**

- Extend, do not replace, plan 40's conflict workload with `--coordination optimistic|partition-lease`. Re-run same-partition, Zipf-hot, and spread shapes at 2/8/32 contenders against the same backlogged fixture.
- Catalog-only acceptance: same-partition useful swaps do not regress; compactor/compactor REPLACE conflicts fall to zero (allow only deliberately injected stale-owner cases); rollback tax falls below 5%; spread throughput stays within the benchmark's measured repeatability band of optimistic mode. Report lease acquisition/renewal QPS and PostgreSQL CPU so coordination cost is priced.
- Product-path acceptance with the counting store: at most one worker reads/uploads a given claimed partition generation; record duplicate bytes avoided. Run a slow merge across several renewals and a forced handoff across two compactor replicas.
- HA acceptance: kill lease owner during merge, wait no longer than `ttl + poll interval`, prove a peer claims and converges; fail catalog during renewal, prove no new merge begins and no unfenced REPLACE commits; restore catalog, replan from live state, and converge without a new feed event.
- Connection acceptance: repeat the process-equivalent pool surge with the intended ukield replica count. The lease feature must not evade plan 40's first wall; document the fleet pool budget/RDS Proxy posture.

- [ ] **Step 1: Full verification:** `cargo fmt --check && cargo clippy --all-targets -- -D warnings && make test && make e2e`.
- [ ] **Step 2: Run and record the A/B matrix and HA fault cases.** At least twice per benchmark point; reports append under the existing catalog-results convention.
- [ ] **Step 3: Update docs.** Mark HA Phase 3 compactor scheduling leases delivered, keep REPLACE named as correctness, record the failover bound and ambiguous-operation rollout gate. Mark roadmap row 41 Executed with measured numbers.
- [ ] **Step 4: Commit:**

```bash
git commit -m "docs: compaction partition leases measured - duplicate merge work eliminated"
```

---

## Exit Criteria

- Worker cursors cannot regress under delayed or concurrent updates, and stale writes do not refresh progress timestamps.
- A compaction backlog is discoverable from live catalog state even with no unread feed event.
- At most one unexpired owner/generation exists per `(hypertable, partition)`; expiry/reclamation uses PostgreSQL time.
- A stale owner cannot commit a replacement after handoff; the fence is checked in the REPLACE transaction.
- Ladder compaction and finalization cannot work the same partition concurrently.
- Lease renewal holds no database connection during object work and proven/unknown loss stops that work.
- Ingest/deletion races remain safe through optimistic REPLACE.
- Two replicas converge after owner death within `ttl + poll interval`, without a new feed event.
- The 2/8/32 contention A/B demonstrates the measured win and no material spread-work regression.
- Existing compaction equivalence, GC hygiene, and workspace test suites pass.
- Production active-active enablement explicitly remains gated on HA phase 2 ambiguous compactor mutation reconciliation.

## Non-goals and Follow-ups

- Ingest partition leases (roadmap row 26) use a different ownership key and offset-CAS correctness layer.
- GC singleton ownership is separate because its destructive candidate lifecycle differs.
- Query/catalog caching, replicas, catalog sharding, and PgBouncer are unchanged by this plan.
- Cross-partition parallelism inside one compactor process is not required; the first scale-out unit is multiple replicas. Add bounded in-process concurrency only after the lease A/B shows it is useful and the fleet connection budget includes it.
- Leases do not coordinate user deletion or ingest. Making those operations acquire compaction leases would enlarge the critical path without a correctness need.

## Self-review Notes

**Why partition, not part.** The current merge consumes a mutable group of parts and finalization consumes every live part in a partition. A part lease stops representing the job after its first REPLACE and cannot prevent a level merge from overlapping finalization. Partition ownership matches the actual exclusion domain and plan 40 proves serialization inside that domain costs no useful throughput.

**Why fence when REPLACE is already safe.** REPLACE prevents data loss, but without a generation check an expired zombie may still win after a new owner starts, recreating the expensive duplicate-work race the lease exists to prevent. The lease fence establishes scheduling ownership; REPLACE continues to protect against actors outside that ownership protocol.

**Why cursor and candidate discovery are coupled.** Monotonicity prevents backward progress, but monotonic shared progress can move past unfinished leased work. Therefore a cursor cannot be the work ledger. Current live parts are authoritative, candidate discovery is periodic, and the feed only accelerates/observes the loop. The crash-after-cursor regression test is the proof.

**Failure ordering.** Upload intent precedes bytes; lease validation precedes tombstoning inside the commit; commit/reconciliation precedes intent cleanup. Lease loss never deletes objects or clears intent. This preserves the existing orphan-GC safety story across every handoff point.

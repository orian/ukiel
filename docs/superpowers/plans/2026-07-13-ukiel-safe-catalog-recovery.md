# Ukiel Plan 42: Safe Catalog Transient Recovery Implementation Plan

> **For agentic workers:** Execute this plan task-by-task. Use checkbox (`- [ ]`) progress, run each task's focused tests before its commit, and do not combine Plan 43's ambiguous-operation identity work into this plan.

> **Status: Executed 2026-07-13.** All eight tasks landed (commits `e2f7d30` ..
> the close-out). HA design Phase 1 is marked delivered; measured recovery and
> the bug the fault injection found are recorded there and in roadmap row 42.
> Plan 43's ambiguous-mutation reconciliation remains unclaimed and remains the
> production gate for active-active compaction.

**Goal:** A temporary PostgreSQL/Aurora writer outage must degrade ukield, not crash-loop the fleet. Queries that cannot obtain an authoritative catalog plan return a bounded, retryable 503; ingest stops consuming and later reloads catalog offsets before seeking Kafka; compaction abandons stale plans and replans from live parts; GC performs no deletion from a stale candidate list. Roles automatically resume after connectivity and role-specific reconciliation, while liveness remains healthy and readiness exposes the degradation.

**Driver:** `docs/superpowers/specs/2026-07-13-ukiel-high-availability.md`, implementation phase 1. Plan 41 made active-active compaction ownership-safe and efficient, but the current process still cancels every role when one worker returns an error, catalog errors have no stable classification, SQLx pool behavior is mostly fixed, query catalog failures become HTTP 400, and startup catalog failure exits before health endpoints exist.

## Architecture

### Recover by restarting a role from authority, not by retrying arbitrary SQL

The low-level `PostgresCatalog` remains a direct API. It gains a stable error classification and configurable pool construction, but **not** a generic “retry every method” wrapper. Retrying an unknown mutation is unsafe until Plan 43 gives it a stable operation identity.

ukield owns a role recovery supervisor. When a worker returns a recoverable catalog failure, the supervisor:

1. marks that role `Degraded`;
2. drops the worker future, which drops its stale in-memory plan/buffers;
3. probes the writer endpoint with bounded exponential backoff and per-process jitter;
4. marks the role `Reconciling` once connectivity returns;
5. reconstructs the worker and waits for its role-specific authoritative reload;
6. marks it `Healthy` only after that reload succeeds.

This is safe for each current worker:

- ingest drops its consumer/buffers, reloads catalog offsets, and reassigns/seeks Kafka; committed data is skipped, uncommitted data replays, and unreferenced uploads remain pending/orphans;
- compaction drops the lease and live-part plan, then discovers candidates from current live parts (plan 41); it never retries the failed REPLACE in place;
- GC drops the current pass and rereads candidates/fences before another deletion;
- the collector already isolates each family and remains non-fatal.

### Two levels of error meaning

`CatalogError::class()` classifies facts available from the error itself:

```rust
pub enum CatalogErrorClass {
    SemanticConflict,
    OwnershipViolation,
    PermanentInput,
    AbsentResource,
    Transport,
    RetryableDatabase,
    PermanentDatabase,
}
```

Operation context then decides the action. A transport failure during a read is transient. The same failure around a non-idempotent mutation has an unknown outcome and must trigger authoritative role reconstruction, never an in-place retry. Plan 42 records that as an ambiguous event in health/metrics, but does not claim logical-operation reconciliation; Plan 43 supplies canonical keys, fingerprints, and lookup.

SQLSTATE/structured SQLx classification only—no message matching:

- `Conflict` -> semantic conflict;
- `LeaseLost`, `OffsetRace` -> ownership violation;
- schema/sort/lease-partition/invalid-TTL errors -> permanent input;
- `NotFound` -> absent resource;
- `sqlx::Error::Io`, `PoolTimedOut`, SQLSTATE class `08`, `57P01`, `57P02`, `57P03`, and `53300` -> transport/transient;
- SQLSTATE `40001` and `40P01` -> retryable database;
- `PoolClosed`, TLS/configuration/migration errors, unknown SQLSTATEs, and all unknown errors -> permanent by default.

### State and health

Catalog reachability and role recoverability are separate:

```rust
pub enum CatalogState { Healthy, Unavailable }
pub enum RoleState { Starting, Healthy, Degraded, Reconciling, Failed, Stopped }
```

A successful ping proves reachability, not role readiness. A role leaves `Reconciling` only after its own authoritative reload. The shared registry exposes immutable snapshots to `/readyz` and Prometheus; it never holds a lock across I/O.

### Retry discipline

- Reads/probes: bounded attempt deadline, exponential backoff capped by configuration, deterministic per-process jitter, shutdown-aware.
- Query admission: one short total budget, then 503 + `Retry-After`.
- Background roles: retry recovery indefinitely with a capped delay until shutdown.
- Semantic conflict/ownership violation: preserve existing replan/stop behavior; never send through generic transport retry.
- Non-idempotent mutation with unknown outcome: discard role state and reconcile from catalog authority; never replay the call in place.
- Permanent failures: mark the role `Failed` and preserve today's default of terminating the process; configuration may later relax this, but this plan adds no silent partial-service mode.

## Scope Boundary

Plan 42 does **not** add deterministic mutation keys, fingerprints, commit lookup, or lost-ack replay. That is Plan 43 and remains the production gate for active-active compaction. Plan 42 may safely recover current single-owner ingest by reloading transactional catalog offsets, and may safely replan compaction from live parts, but metrics must still call a transport failure around a commit “ambiguous.”

Also out of scope: GC singleton lease, horizontal ingest leases, dedicated migration/bootstrap ownership, RDS Proxy/Aurora staging, read replicas, cached manifests, and automatic retry of object-store/Kafka errors.

## Prerequisites and Constraints

- Plans 19, 40, and 41 are executed. In particular, offset CAS is authoritative, compactor cursors are monotonic, compaction candidates come from live state, and partition leases are fenced.
- Rust edition 2024, toolchain >= 1.96; pinned dependency versions remain unchanged.
- Use SQLx 0.9 APIs verified locally: `PoolOptions::{max_connections,min_connections,acquire_timeout,max_lifetime,idle_timeout,test_before_acquire,connect_lazy_with}`.
- No new retry crate. Backoff jitter is derived from the process UUID plus attempt using a pure helper; tests inject a seed.
- No database connection is held while sleeping, probing backoff, consuming Kafka, or doing object-store work.
- Existing queries whose physical plans are already constructed may finish during catalog outage. No new plan is built from stale metadata.
- Every task ends with focused tests and `cargo fmt --check && cargo clippy --all-targets -- -D warnings`.
- Conventional commits; no AI attribution.

---

### Task 1: Stable catalog error taxonomy and configurable pool

**Files:**
- Modify: `crates/ukiel-catalog/src/error.rs`
- Modify: `crates/ukiel-catalog/src/lib.rs`
- Modify: `crates/ukiel-catalog/tests/catalog_test.rs`
- Modify: `crates/ukield/src/config.rs`
- Modify: `ukield.example.toml`

**Interfaces:**

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatalogErrorClass { /* seven variants above */ }

impl CatalogError {
    pub fn class(&self) -> CatalogErrorClass;
    pub fn is_recoverable_transport(&self) -> bool;
}

#[derive(Debug, Clone)]
pub struct CatalogPoolConfig {
    pub max_connections: u32,
    pub min_connections: u32,
    pub acquire_timeout: Duration,
    pub idle_timeout: Option<Duration>,
    pub max_lifetime: Option<Duration>,
    pub test_before_acquire: bool,
}

impl PostgresCatalog {
    pub fn connect_lazy_with_config(
        url: &str,
        config: &CatalogPoolConfig,
    ) -> Result<Self, CatalogError>;
    pub async fn connect_with_config(
        url: &str,
        config: &CatalogPoolConfig,
    ) -> Result<Self, CatalogError>;
}
```

`connect` and `connect_with_pool_size` remain source-compatible and delegate to the new builder. The benchmark's explicit pool-size topology must keep exactly its current behavior. Runtime ukield uses the lazy constructor so a promoted writer can be reached by the existing pool and the process can expose health before the first successful connection.

Add `[catalog]` settings with defaults preserving current capacity unless named otherwise:

```toml
max_connections = 16
min_connections = 0
acquire_timeout_ms = 2000
idle_timeout_secs = 600
max_lifetime_secs = 1800
test_before_acquire = true
retry_base_ms = 100
retry_max_ms = 5000
retry_jitter_ratio = 0.20
recovery_probe_timeout_ms = 2000
query_admission_timeout_ms = 1000
retry_after_secs = 1
```

Validate min <= max, all nonzero timeouts, retry base <= max, and jitter in `[0, 1]`. The fleet-wide connection budget remains a deployment invariant from plan 40; log `max_connections * process_count` in deployment docs, not as a value one process can validate.

- [x] **Step 1: Failing pure classification tests.** Construct every `CatalogError` variant plus representative SQLx database errors with test doubles where possible. Pin SQLSTATE mappings and unknown-defaults-permanent. Do not assert display strings.
- [x] **Step 2: Failing pool/config tests.** Defaults, invalid combinations, lazy construction against an unavailable address, and existing `connect_with_pool_size` delegation.
- [x] **Step 3: Implement.** Emit `catalog_operation_total{operation,outcome,error_class}` at the catalog call boundary only where an operation name already exists; do not hand-edit every method in this task. Full role metrics land in Task 2.
- [x] **Step 4: Verify:** `cargo test -p ukiel-catalog -- --test-threads=2 && cargo test -p ukield --lib`.
- [x] **Step 5: Commit:**

```bash
git commit -m "feat: classify catalog failures and configure SQLx pool lifecycle"
```

---

### Task 2: Recovery backoff, catalog reachability, and role-state registry

**Files:**
- Create: `crates/ukield/src/recovery.rs`
- Create: `crates/ukield/src/health.rs`
- Modify: `crates/ukield/src/lib.rs`
- Modify: `crates/ukield/src/metrics.rs` if metric description/registration is centralized there

**Interfaces:**

```rust
#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum RoleState { Starting, Healthy, Degraded, Reconciling, Failed, Stopped }

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum CatalogState { Healthy, Unavailable }

pub struct HealthRegistry { /* catalog + configured roles */ }
pub struct RoleHandle { /* one role's transition capability */ }
pub struct HealthSnapshot { /* serializable, no secrets */ }

pub struct Backoff { /* base, cap, jitter, seed, attempt */ }
impl Backoff {
    pub fn next_delay(&mut self) -> Duration;
    pub fn reset(&mut self);
}

pub async fn wait_for_catalog(
    catalog: &PostgresCatalog,
    health: &HealthRegistry,
    policy: &RecoveryPolicy,
    shutdown: &CancellationToken,
) -> Result<(), Shutdown>;
```

Transitions are validated: `Healthy -> Degraded -> Reconciling -> Healthy`; any -> `Failed`; shutdown -> `Stopped`. A repeated transient while reconciling returns to `Degraded`. Store state, transition timestamp, last catalog success, current backoff, last error class, and a sanitized error summary. Never store URLs, SQL, tenant IDs, or credentials.

Backoff is exponential with saturation and symmetric jitter. Its test takes a fixed seed and proves bounds, cap, reset, and that two process seeds do not synchronize. `wait_for_catalog` wraps each ping in `recovery_probe_timeout`, records failures, releases the pool connection before sleeping, and is immediately shutdown-cancellable.

Metrics:

- `catalog_retry_total{role,reason}`;
- `catalog_outage_seconds` and `catalog_last_success_timestamp`;
- `ukield_role_state{role,state}` as one-hot gauges (clear old state on transition);
- `worker_pause_seconds{role}`;
- pool size/idle plus acquire timeout totals available from classified errors.

- [x] **Step 1: Failing pure tests** for legal/illegal transitions, snapshot serialization, secret-free summaries, backoff bounds/cap/reset/jitter, and cancellation during sleep.
- [x] **Step 2: Implement registry and probe loop.** Use `std::sync::RwLock` only for tiny snapshot mutations; clone a snapshot before serialization and never hold the lock across await.
- [x] **Step 3: Metrics tests** using the existing recorder pattern; state transition must clear the previous one-hot gauge.
- [x] **Step 4: Verify:** `cargo test -p ukield --lib`.
- [x] **Step 5: Commit:**

```bash
git commit -m "feat: catalog recovery state, jittered probes, and role health registry"
```

---

### Task 3: Recoverable ukield supervision and startup initialization

**Files:**
- Modify: `crates/ukield/src/run.rs`
- Modify: `crates/ukield/src/bootstrap.rs`
- Modify: `crates/ukield/src/config.rs`
- Test: `crates/ukield/tests/server_test.rs`
- Test: `crates/ukield/tests/bootstrap_test.rs`

**Supervisor shape:**

```rust
pub enum RoleExit<E> {
    Shutdown,
    RecoverableCatalog { error: CatalogError, outcome_unknown: bool },
    Permanent(E),
}

async fn supervise_role<Build, Fut, E>(
    role: Role,
    build: Build,
    catalog: PostgresCatalog,
    health: RoleHandle,
    policy: RecoveryPolicy,
    shutdown: CancellationToken,
) -> anyhow::Result<&'static str>;
```

The real signature may use boxed futures because each role has a different error type; preserve the semantics, not necessarily this exact generic spelling. Classification must be typed (`IngestError::Catalog`, `CompactorError::Catalog`, `GcError::Catalog`) and never search an anyhow string.

Recoverable catalog exit stays inside `supervise_role`; it does not return to the top-level `JoinSet`, so the existing “first failure cancels everything” path is retained only for permanent role/infrastructure failures. Rebuild closures create a fresh worker each attempt. The compactor owner UUID is allocated once outside the closure and reused for the process lifetime.

Startup uses the lazy catalog pool. Bind health/query/metrics listeners and expose `Starting` before catalog initialization. Run migrate + existing bootstrap through the same transient probe/backoff policy; permanent migration/schema/bootstrap errors still terminate. Ordinary replicas still run migrations/bootstrap in this phase—the dedicated job is Phase 3 and must remain a clearly documented follow-up—but a database failover during rollout no longer creates a tight crash loop.

Do not mark a role Healthy merely because its future was spawned. Task 5 adds the worker-ready handshake. Until then it remains `Reconciling`.

- [x] **Step 1: Failing supervisor tests** with a scripted fake role: transient failure -> degraded -> probe -> reconciling -> rebuilt; permanent failure still cancels; shutdown interrupts backoff; another healthy role remains running throughout recoverable failure.
- [x] **Step 2: Refactor `run.rs`.** Remove only the recoverable-error global cancellation behavior. Infrastructure task panic/bind failure and permanent worker failure keep fail-fast semantics.
- [x] **Step 3: Startup test:** start ukield pointed at a closed port, prove `/healthz` responds 200 and the process remains alive; then make catalog reachable (test seam may swap a scripted probe rather than move a real port) and prove initialization advances.
- [x] **Step 4: Verify:** `cargo test -p ukield -- --test-threads=2`.
- [x] **Step 5: Commit:**

```bash
git commit -m "feat: recover catalog-failed roles without cancelling ukield"
```

---

### Task 4: Bounded query admission, 503 semantics, and structured health

**Files:**
- Modify: `crates/ukiel-query/src/error.rs`
- Modify: `crates/ukiel-query/src/context.rs`
- Modify: `crates/ukiel-query/src/server.rs`
- Modify: `crates/ukield/src/run.rs`
- Test: `crates/ukiel-query/tests/server_test.rs`
- Test: `crates/ukield/tests/server_test.rs`

Split catalog-dependent admission from execution:

1. build the namespace session;
2. parse/optimize SQL and build the physical plan (this is where provider catalog reads occur);
3. after the plan is constructed, execute it under the existing statement timeout.

Use DataFusion 54's physical-plan path rather than `DataFrame::collect` hiding admission and execution inside one future. A plan that already owns its `PartitionedFile` list may finish during catalog outage. No implicit live-parts cache is added.

Add a typed extractor that finds `CatalogError` through `QueryError` and `DataFusionError::External`; no string matching. Outcomes:

- transient catalog failure or admission budget exhausted -> HTTP 503, JSON `{ "error": "catalog unavailable", "retryable": true, "code": "catalog_unavailable" }`, and `Retry-After` from config;
- statement execution timeout -> existing 408;
- invalid SQL/DDL -> 400;
- unknown logical table -> 400;
- permanent catalog/configuration error -> 500 and role `Failed`/not-ready, without leaking SQLx internals.

`/healthz` remains unconditional 200 while the runtime/server works. `/readyz` becomes structured JSON containing overall readiness, catalog state/last success, configured role states, and object-store reachability. It returns 503 while the query role cannot admit new plans; a worker-only deployment reports its structured degraded role without pretending to be a query load-balancer target.

Metrics:

- `query_catalog_admission_rejected_total{reason="timeout|transport"}`;
- `catalog_operation_duration_seconds{operation="query_admission"}`;
- existing query request counters retain one outcome per request.

- [x] **Step 1: Failing HTTP tests:** transport error -> 503 + Retry-After + stable JSON; zero admission budget -> deterministic 503; bad SQL remains 400; zero statement timeout remains 408; liveness remains 200 during catalog failure.
- [x] **Step 2: Physical-plan split test:** construct a query plan, break catalog access, execute the already-built plan successfully from its object-store inputs. A new query during the same outage returns 503.
- [x] **Step 3: Structured readiness tests** for Starting, Healthy, Degraded, Reconciling, Failed, and object-store failure. Assert bodies contain no catalog URL/error detail.
- [x] **Step 4: Implement and verify:** `cargo test -p ukiel-query -- --test-threads=2 && cargo test -p ukield --test server_test -- --test-threads=2`.
- [x] **Step 5: Commit:**

```bash
git commit -m "feat: bound catalog query admission and expose role-aware readiness"
```

---

### Task 5: Role-specific ingest and compactor reconciliation

**Files:**
- Modify: `crates/ukiel-ingest/src/consumer.rs`
- Modify: `crates/ukiel-ingest/src/error.rs`
- Modify: `crates/ukiel-compactor/src/compactor.rs`
- Modify: `crates/ukiel-compactor/src/error.rs`
- Modify: `crates/ukield/src/run.rs`
- Test: `crates/ukiel-ingest/tests/ingest_test.rs` or existing component-test module
- Test: `crates/ukiel-compactor/tests/compactor_test.rs`

**Ingest contract:**

- A catalog failure from initial route lookup, offset load, backpressure probe, upload-intent registration, or commit tears down the entire `IngestWorker` attempt. Dropping its `JoinSet` aborts every route consumer so no sibling keeps polling/committing while the role is degraded.
- Consumer drop/unassign is the pause mechanism. Buffered rows/ranges may be discarded because Kafka group offsets are never committed; the reconstructed worker reloads catalog offsets and seeks exactly from authority.
- Add a ready barrier: every configured route reports ready only after hypertable validation, catalog offset reload, metadata discovery, and assignment/seek. The ingest role becomes Healthy only after all routes report.
- If a commit transport error may have an unknown outcome, increment `catalog_ambiguous_mutation_total{role="ingest"}`. Do not call `flush` again with the drained batch. Reconstruct: if the commit landed, catalog offsets seek past it; if absent, Kafka replays it. Uploads from the absent case remain intent-covered orphans.
- Preserve OffsetRace as an ownership violation that stops the role; never generic-retry it.

**Compactor contract:**

- Any transient catalog failure ends the current worker attempt. Plan 41 renewal already cancels object work when ownership is unknown.
- Reconstructed compactor reuses the process owner UUID, rereads hypertables/candidates/live parts, and acquires a fresh/current partition generation. No old `group`, cursor result, or output list crosses attempts.
- Ready means one successful current-state discovery pass, not merely `ping`.
- A transport error around `commit_compaction_replace` increments `compactor_ambiguous_commit_total`, drops the plan, and relies on live-state replanning. It is **not** retried in place. Keep the Plan 43 production gate explicit.
- Conflict and LeaseLost retain their current in-pass replan/scheduling behavior and do not degrade the role.

- [x] **Step 1: Ingest recovery test.** Fail catalog at flush, destroy the worker attempt, recover, reload offsets, and prove the batch is represented exactly once whether the scripted commit outcome is “not applied” or “applied but response lost.” Assert no Kafka group offset commit is introduced.
- [x] **Step 2: Multi-route test.** One route's catalog failure drops/pauses all route consumers; no sibling advances while degraded; all routes reload before Healthy.
- [x] **Step 3: Compactor recovery tests.** Catalog loss during candidate read and during lease renewal causes attempt restart; after recovery it converges from live state without a new feed event. A commit-transport failure is counted ambiguous and never directly replayed.
- [x] **Step 4: Verify:** `cargo test -p ukiel-ingest -- --test-threads=2 && cargo test -p ukiel-compactor -- --test-threads=2 && cargo test -p ukield --lib`.
- [x] **Step 5: Commit:**

```bash
git commit -m "feat: reconcile ingest offsets and compaction state after catalog recovery"
```

---

### Task 6: GC fail-closed recovery and non-fatal collector behavior

**Files:**
- Modify: `crates/ukiel-catalog/src/gc.rs`
- Modify: `crates/ukiel-gc/src/lib.rs`
- Modify: `crates/ukield/src/run.rs`
- Modify: `crates/ukield/src/collector.rs`
- Test: `crates/ukiel-gc/tests/gc_test.rs`
- Test: `crates/ukield/tests/collector_test.rs`

GC must not keep deleting from a large in-memory list after catalog state becomes unavailable. Replace bulk destructive iteration with one-authoritative-candidate-at-a-time APIs (or an equivalently strict bounded cursor): fetch one eligible part/path with all grace/cursor predicates, delete it idempotently, stamp/clear it, then query authority again before the next deletion. A catalog error after one object's delete may leave that one unstamped; the next recovered pass sees `NotFound`, stamps it, and continues. It never authorizes deletion of a second object from the stale list.

Apply the same rule to tombstone reaping and pending-object sweeping. The rare listing reconcile may finish only the object whose deletion already started; before each subsequent unknown object, verify catalog health/current known-state or abort and rebuild the known set. Keep object-store `NotFound` idempotent.

On transient catalog error, the GC attempt returns to the ukield recovery supervisor, state becomes Degraded, and a fresh `Gc` begins only after recovery. Add `gc_destructive_pass_aborted_total{phase="reap|sweep|reconcile"}`. GC never becomes Healthy on ping alone; its first authoritative candidate/fence query must succeed.

Collector families remain non-fatal exactly as today. Catalog-family failure may update catalog reachability telemetry but must not independently flip healthy data-plane roles or cancel the process. After recovery the next tick overwrites gauges; add `collector_errors_total` assertions rather than a new collector recovery loop.

- [x] **Step 1: Failing GC tests:** fail catalog after deleting candidate 1; candidate 2 remains present; recovery stamps/reprocesses candidate 1 idempotently then handles candidate 2. Repeat for orphan sweep and listing reconcile.
- [x] **Step 2: Implement one-at-a-time authoritative APIs.** Record query count and explain why the extra round trip is accepted: S3 deletion dominates and correctness is the goal; batch claims are a measured follow-up only.
- [x] **Step 3: Collector test:** several failed catalog-family ticks never stop metrics/query roles; a later success refreshes gauges.
- [x] **Step 4: Verify:** `cargo test -p ukiel-gc -- --test-threads=2 && cargo test -p ukield --test collector_test -- --test-threads=2`.
- [x] **Step 5: Commit:**

```bash
git commit -m "feat: fail GC closed and rebuild destructive state after catalog outage"
```

---

### Task 7: Compose fault proxy and catalog-outage acceptance suite

**Files:**
- Modify: `docker-compose.yml`
- Modify: `Makefile`
- Create: `crates/ukiel-e2e/src/fault_proxy.rs`
- Modify: `crates/ukiel-e2e/src/lib.rs`
- Create: `crates/ukiel-e2e/tests/s10_catalog_recovery.rs`
- Modify: `crates/ukiel-e2e/Cargo.toml` only if an already-workspace dependency must be exposed

Add Toxiproxy as an opt-in compose `ha` profile. Expose its control API and a PostgreSQL proxy port; target `postgres:5432` on the compose network. Use existing `reqwest` to create/reset the proxy and apply/remove a cut—no new client dependency. Normal `make e2e` remains unchanged.

Add `make e2e-ha` that starts the normal stack plus `--profile ha`, points the S10 ukield catalog URL at the proxy, runs only the HA scenario serially, and always tears down with `docker compose down -v` (shell trap so test failure cannot leave a toxic proxy for later runs).

S10 sequence:

1. start a full ukield with query, ingest, compactor, and GC roles through the proxy;
2. establish healthy baseline and record structured readiness;
3. cut PostgreSQL traffic for longer than several retry intervals;
4. assert process/liveness stays up, readiness becomes 503, a new query returns bounded 503 + Retry-After, ingest stops advancing catalog offsets, compactor starts no new merge, and GC deletes no second candidate;
5. keep producing Kafka events while catalog is down, bounded below broker/disk limits;
6. restore traffic;
7. assert states pass through Reconciling to Healthy, ingest reloads offsets and stores every event exactly once, compaction converges from current state, GC resumes, and no new process was started;
8. assert retry rate/backoff is bounded and role/health metrics were emitted.

This suite covers disconnect-before/read/in-flight behavior, not commit-after-acknowledgement loss—that requires the Plan 43 fault point inside the transaction response path. State this in the test module and HA design close-out.

- [x] **Step 1: Implement proxy helper with idempotent create/reset.** A prior failed run must not leave toxics affecting the next run.
- [x] **Step 2: Implement S10 and `make e2e-ha`.** Use deadlines with diagnostic snapshots, never unbounded sleeps.
- [x] **Step 3: Run S10 at least three consecutive times** to catch timing flake; record observed outage/recovery duration and retry count.
- [x] **Step 4: Run normal `make e2e`** to prove the optional profile changed nothing.
- [x] **Step 5: Commit:**

```bash
git commit -m "test: catalog outage degrades and recovers ukield without restart"
```

---

### Task 8: Full verification, documentation, and roadmap close-out

**Files:**
- Modify: `docs/superpowers/specs/2026-07-13-ukiel-high-availability.md`
- Modify: `docs/superpowers/plans/2026-07-05-ukiel-v1-roadmap.md`
- Modify: `ukield.example.toml`
- Modify: `README.md` only if its operational run instructions mention readiness/startup

- [x] **Step 1: Full verification:**

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
make test
make e2e
make e2e-ha
```

- [x] **Step 2: Record measured compose recovery:** outage duration, time to 503, recovery-to-Healthy, retries per process, maximum backoff, ingest pause duration/buffer posture, and whether any role/process exited. These are local fault-proxy numbers, not an Aurora RTO claim.
- [x] **Step 3: Update HA design:** mark Phase 1 delivered, document exact state transitions and query error contract, retain Phase 2 as the active-active production gate, retain dedicated migration/bootstrap ownership in Phase 3, and retain Aurora/RDS Proxy proof in Phase 4.
- [x] **Step 4: Mark roadmap row 42 Executed** with the measured result and point to S10. Do not mark the overall HA design Implemented.
- [x] **Step 5: Commit:**

```bash
git commit -m "docs: safe catalog transient recovery delivered and fault-tested"
```

---

## Exit Criteria

- Catalog failures have structured, tested classification with unknown errors defaulting permanent.
- ukield exposes liveness before the first successful catalog connection and keeps it healthy during an outage.
- A recoverable catalog failure in one role does not cancel unrelated roles or terminate the process.
- Retries are bounded, jittered, deadline-aware, shutdown-aware, and release pool connections while sleeping.
- Query admission returns stable 503 + Retry-After within its configured budget; invalid SQL remains 400 and execution timeout remains 408.
- Readiness is structured and role-aware; ping alone never marks a recovering worker Healthy.
- Ingest stops polling on catalog failure and reconstructs from authoritative offsets; every event is visible exactly once after recovery without Kafka group-offset commits.
- Compaction cancels/abandons stale work and replans from live state/leases after recovery; no mutation call is blindly replayed.
- GC performs at most the already-authorized current object deletion after catalog loss and never continues through a stale candidate list.
- Collector catalog failures remain non-fatal.
- S10 proves outage -> Degraded -> Reconciling -> Healthy without process restart, with bounded retry load.
- Normal unit/component/e2e suites remain green.
- Plan 43's ambiguous-mutation reconciliation gate remains explicit and unclaimed.

## Implementation Warnings for the Executing Agent

1. **Do not wrap `PostgresCatalog::commit*` in a generic retry helper.** A connection error may mean it committed.
2. **Do not map every DataFusion error to 503.** Extract an actual catalog transport error; bad SQL stays 400 and object-store/engine failures keep their own behavior.
3. **Do not mark Healthy on ping.** Ingest must reload/seek, compactor must discover current state, and GC must reread a candidate/fence.
4. **Do not preserve ingest buffers across worker reconstruction.** Kafka plus catalog offsets are the recovery log; keeping a half-drained buffer invites duplication and makes ambiguous state harder.
5. **Do not make `/healthz` depend on PostgreSQL.** That creates a restart storm during managed failover.
6. **Do not let collector failure drive readiness.** Monitoring is not the data plane.
7. **Do not claim Aurora/RDS Proxy readiness from Toxiproxy.** Plan 42 proves application behavior under connection loss; Phase 4 prices managed failover and proxy behavior.

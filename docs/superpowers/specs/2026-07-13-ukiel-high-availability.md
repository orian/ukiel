# Ukiel High Availability Design

Status: Proposed
Date: 2026-07-13
Scope: ukield and its worker crates when the catalog runs on a managed,
single-writer PostgreSQL service such as Amazon Aurora PostgreSQL.

## Context

Ukiel delegates database replication, storage durability, primary election,
backup, and database-instance failover to a managed PostgreSQL service. This
document specifies the remaining application-level HA contract.

The catalog is not ordinary metadata. One transaction can be the sole durable
authority for:

- which Parquet parts are live;
- which Kafka offsets have been consumed;
- whether an optimistic REPLACE won;
- the ordered change feed;
- worker progress and the GC safety fence;
- pending upload ownership.

A database failover is therefore modeled as a bounded period in which
connections fail and transaction outcomes may be unknown. ukield must recover
without guessing, silently using stale destructive state, or blindly repeating
a mutation.

The current implementation does not yet meet this contract:

- most catalog errors propagate out of worker loops;
- ukield cancels every role after the first task failure;
- commit_with_offsets has no idempotency key;
- worker cursor updates can move backward;
- readiness proves reachability, not role recoverability;
- every ukield startup attempts migrations and bootstrap;
- SQLx pool behavior is mostly fixed rather than deployment-configurable.

## Managed database contract

Ukiel assumes the managed service provides:

1. One authoritative writable endpoint at a time.
2. Atomic PostgreSQL transactions and ordinary PostgreSQL isolation.
3. A stable writer or proxy endpoint that follows database failover.
4. A documented in-region RPO for acknowledged transactions compatible with
   Ukiel's durability claim.
5. Backups and point-in-time recovery operated independently from instance HA.

If the selected service permits acknowledged transactions to disappear during
failover, Ukiel cannot claim exactly-once durability across that failure. The
deployment must state that weaker RPO explicitly.

Aurora cluster failover and RDS Proxy are examples, not hard dependencies. The
application protocol in this document must also work with another managed
PostgreSQL provider.

Initially all catalog traffic uses the writer endpoint. Read replicas are a
later optimization, not part of the HA baseline.

## Goals

- Query replicas remain horizontally active/active.
- A transient catalog outage does not crash-loop the ukield fleet.
- Every role either makes correct progress or becomes explicitly degraded.
- No acknowledged ingest range creates a second live result after retry.
- Ambiguous mutations are reconciled, never guessed.
- Feed cursors never regress.
- Compaction remains correct across duplicate workers and failover.
- GC performs no destructive work from stale or uncertain catalog state.
- Recovery is automatic after the managed writer endpoint becomes usable.
- Fleet reconnection is bounded and jittered.
- Health and metrics expose the difference between alive, degraded,
  reconciling, and ready.

## Non-goals

- Implementing PostgreSQL replication or leader election inside Ukiel.
- Active/active writable catalogs.
- Transparent cross-region RPO zero.
- Serving query plans from stale cached manifests during catalog outage.
- Making every background role continuously available at any cost.
- Replacing the optimistic REPLACE correctness mechanism with leases.

## Failure model

ukield must tolerate:

- connection reset before a catalog operation starts;
- failure while a query is in flight;
- failure while a transaction is executing;
- failure after PostgreSQL commits but before the response reaches ukield;
- the writer endpoint rejecting connections during promotion;
- pools retaining dead connections after promotion;
- many ukield processes reconnecting simultaneously;
- process termination during recovery;
- duplicate worker processes during a rolling deployment;
- object upload succeeding while the catalog commit is unavailable.

The baseline does not assume that an in-flight transaction survives failover or
that a client can determine its outcome from the connection error.

## HA invariants

### HA1: acknowledged catalog state is authoritative

Part visibility, offsets, commits, cursors, and GC fences are read from the
catalog after recovery. In-memory state never overrides recovered catalog state.

### HA2: offsets and part visibility remain atomic

The ingest operation's part changes, upload-intent clearing, and Kafka offset
advance remain one transaction.

### HA3: ambiguous mutations have a stable identity

Every externally retryable mutation has a stable logical operation key. Retrying
the same logical operation returns Committed or AlreadyApplied. Reusing a key
for a different logical operation fails loudly.

### HA4: semantic failures are not transport retries

Conflict, OffsetRace, schema errors, and invalid ownership cause replan, stop,
or operator action. They are never hidden by generic SQL retry logic.

### HA5: cursors are monotonic

A stale worker cannot move a durable feed cursor backward.

### HA6: recovery discards stale plans

After catalog reconnection, ingest reloads offsets, compaction reloads live
parts and cursors, and GC reloads every candidate and fence.

### HA7: GC fails closed

If authoritative catalog state is unavailable or uncertain, object deletion
stops. Delayed reclamation is acceptable; deleting referenced data is not.

### HA8: unavailable admission is explicit

A query that cannot obtain a catalog plan within its admission deadline returns
a retryable service-unavailable response. It never runs from an implicit stale
manifest.

### HA9: liveness does not depend on catalog availability

A temporary managed-database failover degrades readiness and roles; it does not
make the process liveness probe fail and trigger a fleet restart storm.

### HA10: retry load is bounded

Retries have deadlines, exponential backoff, jitter, and concurrency limits.
Recovery cannot become a thundering herd against the new writer.

## Catalog error taxonomy

CatalogError gains a stable classification independent of SQLx's display text:

| class | examples | action |
|---|---|---|
| semantic conflict | Conflict | re-read and replan |
| ownership violation | OffsetRace, lost lease | stop/reconcile ownership |
| permanent input | schema, sort key, validation | fail role/configuration |
| absent resource | NotFound | caller-specific; usually permanent |
| transient pre-outcome | connection/acquire failure before execution | bounded retry |
| ambiguous mutation | connection loss or server shutdown around commit | reconcile by operation key |
| retryable database | serialization/deadlock if introduced | retry transaction by policy |
| permanent database | permissions, syntax, incompatible schema | fail loudly |

Classification uses SQLSTATE and structured SQLx variants where possible, not
message matching. Unknown database errors default to non-retryable until
classified.

The low-level PostgresCatalog remains a direct transactional API. A resilience
layer owns deadlines, retry budgets, metrics, and reconciliation so callers do
not independently invent policies.

## Logical operation identity

The existing commits.idempotency_key becomes the basis for mutation
reconciliation. commit_with_offsets must accept the same logical operation key
as commit.

Keys are deterministic and scoped by hypertable. Suggested canonical inputs:

| author | logical identity |
|---|---|
| ingest | pipeline, topic, partition, first offset, last offset |
| compactor | output level, sorted input part IDs, schema and placement version |
| deletion | external deletion request ID or sorted input IDs plus policy version |
| materialized view | pipeline ID and source commit ID |
| retention | policy version, partition, sorted input part IDs |

The key represents logical intent, not random output object paths. Two workers
may upload different physical outputs for the same deterministic compaction
intent; only one logical operation becomes authoritative and the losing uploads
remain pending/orphaned.

The catalog must detect accidental key reuse for different intent. Either:

- derive the idempotency key as a cryptographic digest of a canonical logical
  operation, including its kind; or
- store and compare a separate operation fingerprint.

On an ambiguous response:

1. Reconnect through the writer endpoint.
2. Retry or look up the same operation key.
3. If already applied, reload authoritative catalog state.
4. If absent, redo the operation according to the role policy.
5. Never infer success from object-store state alone.

## Cursor semantics

set_cursor becomes monotonic:

    last_commit_id = greatest(current.last_commit_id, excluded.last_commit_id)

A feed consumer follows:

1. Read events after the durable cursor.
2. Apply effects idempotently.
3. Advance the cursor after effects are durable.
4. On connection loss, restart from the old durable cursor.

For catalog-to-catalog effects, prefer committing destination changes and source
progress atomically when they share a catalog. For external effects, use source
commit ID in the destination idempotency key before advancing the cursor.

## Catalog resilience state machine

Each role-facing catalog client exposes:

| state | meaning |
|---|---|
| Healthy | operations succeeding normally |
| Degraded | transient failures; no new unsafe work |
| Reconciling | connectivity restored; authoritative state being reloaded |
| Healthy | reconciliation complete and work resumed |

Transition rules:

- A transient failure enters Degraded and starts bounded jittered probes.
- Destructive/background work pauses.
- The process stays alive.
- Successful ping alone is insufficient to leave Degraded.
- The role must complete its reconciliation procedure.
- Sustained healthy operation resets backoff and failure counters.

Retry policy is operation-specific:

- Read admission: short deadline, then HTTP 503 with Retry-After.
- Worker polling reads: retry indefinitely with capped backoff until shutdown.
- Definite pre-execution mutation failure: retry within the role budget.
- Ambiguous mutation: reconcile by operation key.
- Semantic conflict: return to the role's replan path.

## ukield supervision

Current supervision cancels all roles when any task returns an error. HA changes
this behavior:

- Infrastructure tasks whose failure makes the process unusable may still
  terminate the process.
- Role tasks report permanent versus recoverable failure.
- Recoverable catalog failures remain inside the role state machine.
- A permanent role failure marks that role failed and may terminate the process
  according to configuration; it is never restarted in a tight loop.
- Shutdown cancellation remains global and graceful.

The supervisor records role state centrally for readiness and metrics.

## Role behavior

### Query role

Deployment: active/active replicas spread across failure domains.

- Existing queries with an already constructed plan may finish while the
  catalog is unavailable, bounded by statement timeout and object grace.
- New query admission retries only within a small catalog-admission budget.
- Exhaustion returns HTTP 503 and Retry-After.
- No cached live-parts snapshot is used implicitly.
- Catalog recovery requires no query-side state replay; the next request builds
  from authoritative state.
- Load-balancer readiness is false while new queries cannot be admitted.

### Ingest role

Before horizontal ingest leases exist, run one authoritative owner for each
Kafka partition set and rely on platform restart for process HA. Starting
duplicate unfenced ingest workers is not an HA mechanism; OffsetRace only makes
the mistake loud.

During catalog outage:

1. Pause or stop Kafka polling before the memory valve is exceeded.
2. Do not commit Kafka consumer-group offsets.
3. Do not discard catalog-acknowledged offsets from memory based on an
   ambiguous response.
4. If the process terminates, discard uncommitted buffers and replay.
5. After reconnect, reload catalog offsets before seeking and resuming.

An ingest flush uses a deterministic operation key. Offset CAS remains the final
correctness check.

After the horizontal-ingest plan, ownership leases are renewed independently of
flush commits and have database-time expiry plus fencing generations.

### Compactor and finalizer roles

Multiple compactors are correctness-safe because optimistic REPLACE selects one
winner, but duplicate workers can read, merge, and upload the same bytes.

**Delivered (plan 41).** Partition scheduling leases now exist, and the lease
generation *is* validated inside the REPLACE transaction, so they are more than
scheduling hints — a zombie owner cannot commit over its successor. They are
still not the correctness layer: ingest, deletion, and operator actions take no
lease, so optimistic REPLACE remains what protects the data.

- One renewable lease per `(hypertable, partition)` — the exclusion domain both
  ladder compaction and finalization actually have — claimed *before* any object
  work, with PostgreSQL-time expiry and a fencing generation.
- Renewal runs independently of merge/commit progress; a proven loss or an
  unreachable catalog cancels the in-flight merge before REPLACE.
- Candidate discovery reads current live parts, so an abandoned merge stays
  discoverable with no new feed event (a shared fleet cursor can pass it).
- Measured: compactor/compactor REPLACE conflicts fall to zero and the wasted
  object work with them; partition-disjoint throughput is unchanged. Numbers in
  `docs/notes/2026-07-13-catalog-scalability.md`.

Failover bound: a dead owner's partitions idle for at most
`compactor.lease_ttl_secs` (default 60s) plus one poll interval before a peer
reclaims them. The TTL trades failover delay against duplicate work; it is not a
correctness knob at any value.

On catalog failure:

- stop starting new merges;
- allow or cancel current object work according to shutdown budget;
- reconcile an ambiguous commit by its operation key;
- abandon any pre-failure live-part plan;
- re-read cursor and live parts before resuming;
- leave uncommitted uploads for pending-object/orphan GC.

Shared logical worker cursors are safe only after cursor updates become
monotonic.

### GC role

GC is active/passive by default using a renewable catalog lease or deployment
singleton. Object deletion remains idempotent.

On any transient catalog error:

- stop the current destructive pass;
- do not continue deleting from an in-memory candidate list;
- enter Degraded;
- after recovery, reacquire ownership and reread candidates, grace conditions,
  worker cursors, and purge state.

The GC lease prevents duplicate work; the cursor fence and grace period remain
the correctness mechanism.

### Collector role

Collectors may run on every replica. Fleet-global gauges must be labeled or
aggregated so replicas do not appear as independent global totals. Collector
failure never changes data-plane readiness.

## Connection and proxy behavior

The catalog configuration adds:

- maximum and minimum pool connections;
- acquire timeout;
- connect timeout;
- idle timeout;
- maximum connection lifetime;
- retry base, maximum delay, jitter, and operation deadlines;
- optional direct versus proxy endpoint labels for metrics.

When using RDS Proxy:

- set client connection maximum lifetime below the proxy's enforced maximum;
- test SQLx prepared-statement behavior and proxy pinning;
- avoid session-level SET, advisory locks, cursors, and other state where
  unnecessary;
- prefer transaction-scoped settings and locks;
- monitor proxy client connections, database connections, borrow latency,
  pinning, and connection-borrow timeout.

RDS Proxy can reduce DNS/failover and connection-storm exposure, but it does not
make in-flight transactions unambiguous. All mutation rules still apply.

## Health endpoints

### Liveness

Liveness reports only that the process runtime and health server function. A
catalog outage does not fail liveness.

### Readiness

Readiness is role-aware and returns a structured body:

- overall ready;
- catalog state and time since last success;
- each configured role's state;
- ingest ownership/paused state;
- last successful flush, compaction, GC, and collector timestamps;
- current retry backoff;
- catalog schema compatibility.

A query-serving replica is not ready when it cannot admit new queries. A
worker-only process may remain infrastructure-ready while exposing a degraded
worker, depending on deployment routing; alerts use the structured role state.

## Migrations and bootstrap

Ordinary ukield replicas do not all run migrations during startup.

- A dedicated deployment job owns catalog migrations.
- Migrations use locking and are compatible with rolling application versions.
- ukield verifies a supported schema-version range before starting roles.
- Idempotent table bootstrap moves to a designated control-plane/init job.
- A deployment cannot mix application versions whose catalog contracts are
  incompatible.

This prevents fleet rollout or post-failover reconnection from becoming a
migration/bootstrap storm.

## Graceful shutdown

### Query

Stop accepting new requests, then drain active queries within the termination
budget.

### Ingest

Stop Kafka polling. Flush only when catalog connectivity and ownership are
authoritative. Otherwise discard uncommitted buffers and replay from catalog
offsets on the replacement.

### Compactor

Stop starting merges. Completed uploads without a committed swap remain
registered pending objects and are collected later.

### GC

Stop before starting another deletion. Never begin destructive recovery work
during process termination.

## Object-store interaction

Object upload and catalog commit remain deliberately separate:

- upload intent before upload;
- object upload;
- catalog commit clears intent atomically;
- ambiguous catalog outcome reconciled by operation key;
- losing or abandoned outputs remain pending/orphaned;
- object deletion is idempotent.

A worker must not decide that a commit succeeded merely because an output object
exists. Conversely, catalog failure after upload is not data corruption; it is
an orphan-lifecycle case.

## Read replicas and stale manifests

Read replicas are not enabled by this design. A later design must satisfy both:

1. deletion safety: replica/cache lag plus query lifetime is less than tombstone
   grace;
2. freshness: replica/cache lag is within the declared query-freshness SLO.

Tombstone grace does not prevent a stale replica from omitting a newly committed
part. Read-your-writes additionally needs a commit/WAL token and a replay
barrier. Until catalog performance measurements justify this complexity, all
planning uses the writer endpoint.

## Observability

Add metrics:

- catalog_operation_total by operation, outcome, and error class;
- catalog_operation_duration_seconds;
- catalog_retry_total by role and reason;
- catalog_ambiguous_mutation_total;
- catalog_reconciliation_total by role and outcome;
- catalog_outage_seconds and last_success_timestamp;
- ukield_role_state with healthy/degraded/reconciling/failed;
- worker_pause_seconds;
- query_catalog_admission_rejected_total;
- ingest_catalog_pause_total and buffered rows while paused;
- compactor_ambiguous_commit_total;
- gc_destructive_pass_aborted_total;
- cursor_regression_prevented_total;
- pool acquire latency, size, idle, and timeout totals;
- proxy endpoint/pinning/borrow metrics when available.

Alerts:

- any write role degraded beyond its RTO;
- query admission rejection above threshold;
- ingest paused near the memory valve;
- ambiguous mutations not reconciled;
- GC running while catalog state is not Healthy is a correctness alarm;
- cursor lag or cursor regression attempts;
- fleet connection surge after recovery;
- migration/schema incompatibility.

Logs include operation key hashes, role, attempt, error class, backoff, and
reconciliation result without logging credentials or tenant payload.

## Fault-injection acceptance suite

Run against the compose fault proxy and, separately, an Aurora/RDS Proxy staging
cluster.

Scenarios:

1. Disconnect before a catalog read.
2. Disconnect while a catalog read executes.
3. Disconnect before an ingest transaction commits.
4. Disconnect immediately after commit but before response delivery.
5. Fail over during offset CAS.
6. Fail over during compactor REPLACE.
7. Fail over after compactor upload but before commit.
8. Fail over halfway through changes_since.
9. Fail over after an external worker effect but before cursor advancement.
10. Fail over during GC candidate enumeration.
11. Keep the catalog unavailable beyond ingest buffer capacity.
12. Restore it while many process-equivalent pools reconnect.
13. Roll ukield replicas during database failover.
14. Repeat through RDS Proxy with pinned and unpinned connection populations.

Assertions:

- every query is correct or explicitly retryable/unavailable;
- no acknowledged ingest range creates duplicate live data;
- catalog offsets and parts remain atomic;
- ambiguous operations converge to one logical commit;
- semantic conflicts still replan rather than generic-retry;
- cursors never move backward;
- feeds replay without loss;
- compaction leaves at most one authoritative output set;
- orphan uploads remain reclaimable;
- GC performs no deletion from stale state;
- roles return automatically to Healthy;
- retry and reconnection load stays within configured bounds.

## Performance validation

Catalog performance results must include the production connection path:

- direct writer endpoint baseline;
- RDS Proxy endpoint;
- proxy pinning and borrow behavior;
- process-equivalent pools;
- connection surge;
- normal operation versus managed failover;
- open-loop read/write/mixed load during recovery.

Report commit TPS/p99, query-admission p99, timed-out and ambiguous operations,
failover RTO, retry volume, connection recovery, and post-failover headroom.

## Implementation phases

### Phase 1: safe transient recovery

- Catalog error classification.
- Retry/deadline/backoff policy.
- Role state machine and supervision changes.
- Role-aware health and metrics.
- Configurable SQLx pool lifecycle.
- Query 503 behavior.
- Ingest pause/reconcile behavior.
- GC fail-closed behavior.

### Phase 2: ambiguous mutation safety

- commit_with_offsets operation keys.
- Canonical operation fingerprints.
- Compactor/deletion/MV operation identities.
- Lookup/reconciliation API.
- ~~Monotonic cursor update.~~ **Delivered (plan 41)**: `set_cursor` never
  retreats, and a stale or equal write does not refresh `updated_at` — so a
  delayed replica can neither prolong the GC fence nor impersonate progress.
- Integration tests for lost commit acknowledgments.

**This phase is the gate on active-active compaction in production.** Plan 41
made the fleet efficient and failover-safe, not ambiguity-safe: a compactor whose
commit acknowledgement is lost still cannot tell whether its REPLACE landed. The
leased commit path already reconciles an idempotency-key replay *before* it
judges the lease (a worker whose work is durable is told so, rather than being
told it lost the race and redoing it), so phase 2's canonical operation
identities splice into a path whose ordering is already fixed and tested.

### Phase 3: replicated workers

- Dedicated migration/bootstrap job.
- GC singleton lease.
- ~~Optional compactor scheduling leases.~~ **Delivered (plan 41)**: renewable
  partition leases, generation-fenced inside REPLACE, with HA handoff tests.
- Horizontal ingest leases and fencing from its dedicated roadmap plan.
- Rolling-deployment overlap tests.

### Phase 4: managed failover proof

- Aurora/RDS Proxy staging setup.
- Fault-injection suite.
- Connection pinning audit.
- Performance suite during failover.
- Runbooks for degraded roles and ambiguous-operation investigation.
- RTO/SLO sign-off.

## Deployment recommendation

Initial production shape:

- query role: multiple active replicas across zones;
- ingest role: one authoritative owner per partition set until leases land;
- compactor role: two replicas allowed; partition leases (plan 41) remove the
  optimistic-conflict waste, but enabling active-active in production still waits
  on phase 2's ambiguous-mutation reconciliation;
- GC role: two deployed replicas with one renewable owner;
- collector: every process, with instance-aware metrics;
- catalog: Aurora writer endpoint through RDS Proxy if pinning measurements are
  acceptable;
- migrations/bootstrap: separate deployment job.

The governing rule is:

> Catalog unavailability pauses admission and background mutation. It never
> authorizes stale destructive state, guessed transaction outcomes, or
> unbounded retries.

## References

- Amazon RDS Proxy for Aurora:
  https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/rds-proxy.html
- RDS Proxy connection considerations:
  https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/rds-proxy-connections.html
- RDS Proxy pinning behavior:
  https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/rds-proxy-pinning.html
- Aurora Global Database failover and RPO:
  https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/aurora-global-database-disaster-recovery.html

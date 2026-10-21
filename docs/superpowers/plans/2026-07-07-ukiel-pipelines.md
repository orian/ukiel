# Ukiel Pipelines v1 Implementation Plan

> **For agentic workers:** Execute task by task against current main. Run each
> task's focused tests before its commit. The HA, operation-identity, upload-
> intent, guardrail, and metrics requirements below are acceptance criteria, not
> optional adaptations.

**Status:** Under revision, 2026-10-21. R1–R2 are resolved in the plan; R3–R7 remain
open. Implementation and acceptance tests are still pending.

**Goal:** Add catalog entities for Kafka stream tables and pipelines;
Kafka-to-Parquet SQL pipelines replacing `TableRoute`; and
Parquet-to-Parquet aggregation MVs consuming the catalog change feed. Kafka
egress is outside v1.

The current catalog migrations are consolidated into `0001_init.sql`; this
plan owns `0002_pipelines.sql`. E2E scenarios S9–S11 already exist; this plan
adds S12. MV publication uses the canonical operation domain `mv`.

## Review findings — 2026-10-21

These findings record the plan review. A checked item means its design decision
and required acceptance tests have been incorporated into the affected tasks;
it does not mean the implementation or tests have been completed.

- [x] **R1 [P1]: Hold the feed barrier through first MV cursor registration
  (Tasks 1 and 6).** Resolved in the
  [registration transaction contract](#first-mv-cursor-registration-holds-the-feed-barrier-through-commit)
  and the affected tasks below. Use one explicit READ COMMITTED transaction for
  the exclusive source feed barrier, fresh head, insert-if-absent, durable cursor
  read, and commit. Never initialize with the public standalone `feed_head()`
  followed by `set_cursor()`. Concurrency, rollback, timeout, retry, and GC
  acceptance tests are specified; their implementation remains pending.

- [x] **R2 [P1]: Protect slower MV replicas when a peer advances the shared
  cursor (Tasks 1 and 6 and the active-active contract).** Resolved by
  [catalog reconciliation of superseded attempts](#slower-mv-replicas-reconcile-superseded-attempts).
  The shared cursor protects the unresolved feed prefix, not every replica's
  object reads. Check durable completion before reading and again after a
  source-read failure; discard an obsolete attempt only when the catalog proves
  completion. A committed target operation also permits advancing that event's
  cursor. Missing input without either proof remains an error, never an implicit
  skip. No reader leases or GC grace increase are required. Acceptance tests
  include actual GC during a paused read, empty/skip outcomes, and catalog
  outages; implementation remains pending.

- [ ] **R3 [P1]: Define replay stability beyond function immutability (Tasks 3
  and 4).** One source scan and immutable functions still admit unordered
  `LIMIT`/`OFFSET` and other order-sensitive operations. The same Kafka ranges
  can arrive in a different cross-partition order, and Arrow batch boundaries
  and DataFusion scheduling can change on replay. Reject unsupported operators
  or require a stable total ordering, including tie handling. DataFusion
  explicitly documents the ordering requirement for
  [LIMIT and OFFSET](https://datafusion.apache.org/user-guide/sql/select.html#limit-and-offset-clauses).
  Validate the entire transformation, including target defaults and materialized
  expressions: the current
  [`ukiel-expr` validator](../../../crates/ukiel-expr/src/lib.rs) rejects
  `Volatile` functions but accepts `Stable`; a schema default of
  `CAST(now() AS BIGINT)` passed a validation probe. That probe establishes a
  validation gap, not successful execution of the default. Require early
  rejection of disallowed expressions and replay tests with reordered inputs,
  different batch boundaries, and tied sort keys.

- [ ] **R4 [P1]: Persist physical Kafka topic identity to detect recreation
  (Tasks 1 and 5).** The proposed stream-table row stores a topic name, but
  startup promises to reject topic recreation. A replacement topic with the
  same name, partition count, and LogAppendTime configuration can contain the
  stored offset within its new valid range, silently skipping new records.
  Persist and compare a broker topic identifier or equivalent generation before
  using saved offsets; define legacy adoption when the original generation is
  unknown. Kafka's
  [topic identifiers design](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/127406437/KIP-516%2BTopic%2BIdentifiers)
  addresses same-name recreation, but the currently used rdkafka metadata API
  does not expose a topic UUID, so identify the required client/API path.
  Test delete-and-recreate with the old offset still valid in the new log.

- [ ] **R5 [P1]: Reconcile one logical source commit with the single-batch SQL
  interface (Tasks 3 and 6).** `execute_pipeline(prepared, input: RecordBatch)`
  and a fresh context per input batch conflict with executing SQL once over an
  entire ADD commit containing multiple files and batches. For the same key,
  values 1 and 2 in separate batches should pass `HAVING SUM(value) > 2` when
  processed together, but neither batch passes independently. Concatenating an
  entire commit into one batch instead introduces an unbounded allocation;
  one commit is not a numeric memory limit. Specify a batch sequence or stream
  representing one logical SQL execution, with explicit input, output, and
  in-flight budgets and defined spill/cancellation behavior. Test a multi-part
  ADD with aggregation and HAVING under different batch boundaries, plus the
  behavior when an input exceeds its budget.

- [ ] **R6 [P2]: Resolve the shared writer's expression-engine dependency
  (Task 4 and Architecture).** The writer is required to apply defaults and
  materialized expressions while also having no DataFusion dependency. The
  existing writer calls `ukiel_expr::apply_defaults_and_materialized`, and
  [`ukiel-expr`](../../../crates/ukiel-expr/Cargo.toml) depends on DataFusion.
  Decide whether the shared writer may depend on that expression engine or
  whether callers evaluate expressions before encoding. Update the ownership
  contract and dependency checks accordingly; avoid duplicating expression
  evaluation semantics in a second implementation.

- [ ] **R7 [P2]: Specify a fleet-wide replacement handoff (startup selection,
  Tasks 5–7, and operations docs).** Rejecting duplicate source/target pairs in
  one process does not prevent old and replacement definitions from running in
  different processes. Kafka offset CAS does not determine which SQL version
  wins; MV replacements have distinct pipeline identities and cursors and can
  both append overlapping work. For the operational v1 rollout, require stopping
  and draining all old workers before replacements load authoritative offsets
  or initialize their MV cursors. If rolling overlap is required, design a
  fenced generation instead. Document the handoff and test the chosen behavior;
  do not treat a per-process name list as fleet-wide exclusion.

## Architecture

A pipeline is an immutable catalog definition:

```text
source endpoint -> deterministic DataFusion SELECT -> target endpoint
```

V1 supports:

| source | target | execution |
|---|---|---|
| Kafka stream table | Parquet hypertable | one bounded Kafka flush at a time |
| Parquet hypertable | Parquet hypertable | one source ADD commit at a time |

`ukiel-pipeline` is a pure SQL-over-Arrow crate. Kafka ownership remains in
`ukiel-ingest`. A new `ukiel-write` crate owns target batch-to-L0 encoding,
upload intents, upload, and identified ADD commits without depending on Kafka
or DataFusion. Both `ukiel-ingest` and the new change-feed worker crate
`ukiel-mv` consume it. No query/catalog mutation code is hidden inside the pure
SQL crate, and MV does not inherit Kafka through a convenience dependency.

All role workers run under the existing supervisor. A recoverable catalog
failure drops the entire role worker and every buffer/plan/session it owned. A
rebuilt Kafka worker reloads its configured pipeline names and offsets, then
re-seeks Kafka. A rebuilt MV worker reloads its configured pipeline names and
monotonic cursors. An ambiguous target commit carries an `OperationIdentity`;
the supervisor remains
`Reconciling` until `lookup_operation` answers, discards the old attempt, and
rebuilds from authority.

## Decisions

### Pipeline definitions are immutable in v1

There is no `UPDATE pipeline`. SQL, endpoints, error policy, delivery mode, and
event-time output are immutable. An operator creates a new pipeline name for a
semantic change. Bootstrap accepts an existing definition only when every field
matches; a same-name mismatch is a permanent startup error.

This is load-bearing for deterministic replay and operation identity: a
`PipelineId` always names one transformation. The catalog may contain multiple
immutable definitions for the same source and target so a replacement can be
created before rollout. Schema/ALTER is outside v1.

### Workers execute an explicit startup list

V1 has no catalog-wide discovery, activation state, notification, or live
reload. Each role receives an explicit list of pipeline names at process
startup:

```toml
[ingest]
pipelines = ["events_ingest_v1"]

[mv]
pipelines = ["events_hourly_v1"]
```

An enabled ingest or MV role requires a non-empty list. Worker construction
resolves exactly those names, rejects missing or duplicate names and the wrong
source kind, prepares every selected pipeline, and becomes ready only after all
selected pipelines are positioned. It never calls a catalog method that means
"run every pipeline". A recovery rebuild resolves the same configured names
again so it sees catalog authority without changing the selected set.

Rollout is deliberately operational: create the immutable replacement, change
the configured name list, and restart the affected worker. At most one selected
pipeline may have a given source/target pair, preventing an accidental old+new
double execution during rollout. An MV definition gets its cursor only when it
is first selected, so a never-selected definition creates no GC fence.
Deselecting a previously started MV pauses it: its cursor and GC fence remain so
it can resume without loss. Permanent retirement and cursor removal are outside
v1; operators must not leave abandoned paused MVs indefinitely. Definitions
stay in the catalog as replay/audit history.

### The pipeline graph is acyclic

A pipeline may not use the same Parquet hypertable as both source and target.
Creating a Parquet-to-Parquet definition must also reject any edge that would
close a directed cycle across catalog pipeline definitions. The check and
insert run in one transaction under a catalog-wide pipeline-graph advisory lock
so concurrent `A -> B` and `B -> A` creates cannot both succeed. Worker startup
revalidates that its selected definitions are acyclic and treats a violation as
permanent catalog corruption. Kafka sources are graph roots and Parquet targets
are vertices; v1 has no Kafka target edges.

### A Kafka topic is an external stream-table binding

The topic name does not live on a pipeline. A `stream_table` is the logical SQL
source and owns the immutable physical topic binding, JSON format, and decoding
schema. A Kafka-source pipeline refers to the stream-table ID; its SQL refers to
the stream-table name. The target remains a Parquet hypertable.

V1 assumes one Kafka cluster per catalog deployment. Brokers, TLS, and
credentials remain process configuration rather than catalog data, so a topic
name is unique within the catalog. Supporting multiple clusters later requires
an explicit cluster identity; it must not overload the topic string.

Ukiel does not provision or alter topics. The ingest worker disables Kafka
auto-creation and, before readiness, verifies that every selected topic exists,
has at least one partition, and is configured with
`message.timestamp.type=LogAppendTime`. Missing permissions to inspect that
configuration are a startup error because stable append time is part of the
replay contract. Partition IDs are discovered from Kafka and are not catalog
entities.

Kafka progress belongs to the logical ingestion edge, not to one immutable SQL
version:

```text
(stream_table_id, target_hypertable_id, kafka_partition) -> next_offset
```

This is why at most one pipeline version for a source/target pair may be
selected. Replacing `events_ingest_v1` with `events_ingest_v2` resumes at the
next offset for the same edge, while fan-out to another target has independent
progress. A new edge begins at offset zero. Kafka group offsets are never read
or committed; workers explicitly assign partitions and seek to catalog state.

The existing `ingest_offsets` rows use `(hypertable_id, topic, partition)`.
Migration adds a nullable `stream_table_id`, backfills it transactionally when
the matching stream table is bootstrapped, and adds a unique partial index on
`(stream_table_id, hypertable_id, kafka_partition)`. New pipeline writes require
the ID and retain the topic text only for compatibility and diagnostics. An
unmapped offset row is never used by a pipeline worker.

### Deterministic event-time bounds use stable Kafka metadata

Deterministic SQL needs a replay-stable clock for event-time bounds. V1 provides
one explicitly.

Every Kafka source batch exposes a reserved nullable Int64 column:

```text
__ukiel_kafka_append_time_ms
```

It is populated only from broker-assigned `LogAppendTime`, which is stable when
the same offset is replayed and is not controlled by the event producer. A
producer `CreateTime`, unavailable timestamp, or topic not configured for
`LogAppendTime` becomes SQL NULL; it must never masquerade as broker time.
Stream schemas may not declare names beginning `__ukiel_`. The pipeline decides
whether to reject NULL. The default event bound is ordinary immutable SQL:

```sql
WHERE __ukiel_kafka_append_time_ms IS NOT NULL
  AND ts >= __ukiel_kafka_append_time_ms - 315360000000
  AND ts <= __ukiel_kafka_append_time_ms + 3600000
```

The engine never parses partitions or knows what “old” means. The legacy
`max_event_age_days` and `max_event_future_secs` knobs are deleted only after
this equivalent pipeline predicate and its regression tests land.

An optional pipeline `event_time_column` names a target output column used only
for the existing `ingest_last_flush_event_ts_seconds` freshness metric. This
keeps freshness observable without making event time a hypertable-core concept.

### SQL must be replay-stable

Pipeline validation accepts one query with exactly one scan of the declared
source. DDL, DML, joins, cross joins, extra table scans, and non-immutable
functions are rejected. Inspect DataFusion function volatility and allow only
`Immutable`; do not maintain a short denylist that misses a new clock/random
function. Stable Kafka metadata is how a pipeline expresses time-relative
policy without `now()`.

### MV progress is one source commit at a time

Each Parquet-source pipeline processes one ordered change event at a time:

- `kind == "add"`: read exactly that commit's added parts, run SQL, publish one
  idempotent target ADD, then advance the cursor;
- `replace` or `delete`: do not run the MV and advance the cursor; those commits
  rewrite/remove rows already represented by earlier ADD intent; and
- an empty SQL result advances the cursor without a target commit.

One-commit operations avoid batch-window ambiguity: retries always reconstruct
the same identity. Aggregates are partial per source commit. Consumers query
them with a final aggregation; semantic merge-state compaction is future work.

The first worker startup that selects a Parquet-source pipeline initializes its
`worker_cursors` row atomically at the source's current feed head. Concurrent
replicas use insert-if-absent and then read the one shared cursor. V1 MVs are
therefore future-facing and never claim to backfill history whose objects may
already have been reaped. Selecting one before source ingestion naturally
initializes at zero. Explicit historical backfill is outside v1.

### First MV cursor registration holds the feed barrier through commit

`ensure_mv_cursor(pipeline_id) -> CommitId` derives the source hypertable and
canonical worker name `mv/{pipeline_id}` from the immutable catalog definition.
It rejects a missing pipeline or a non-Parquet source before inserting anything.
Workers call it only after validating their complete startup selection, SQL,
and target contracts. It runs the following sequence on **one connection in one
explicit READ COMMITTED transaction**:

1. Acquire `pg_advisory_xact_lock(~source_hypertable_id::bigint)`, the same
   exclusive transaction-scoped barrier used by `feed_head`. Do not introduce a
   second lock namespace or acquire the pipeline-graph lock on this path.
2. In a **separate statement after lock acquisition**, select the source's
   `max(commits.id)`, or zero for an empty feed. The fresh statement snapshot
   includes writers that committed while this transaction waited. Preserve the
   existing pre-allocation writer locking and sequence `CACHE 1` contract; IDs
   may have gaps.
3. Insert `(worker, hypertable_id, last_commit_id)` at that head with
   `ON CONFLICT (worker, hypertable_id) DO NOTHING`. An existing cursor is never
   reset or advanced by initialization, even if this attempt observes a newer
   head. A cursor at zero is an existing row, not an absent registration.
4. In another statement, select the stored `last_commit_id` for that key. Do not
   substitute the sampled head or rely on `get_cursor`'s absent-row zero default.
   Do not combine insert-if-absent and the fallback read into a single CTE:
   conflict handling must be followed by a fresh READ COMMITTED snapshot.
5. Commit, releasing the barrier only when the cursor is durable. Return the
   stored cursor only after commit succeeds. It may have been advanced further
   by another replica; this API does not pin a replica's subsequent object reads
   (see the superseded-attempt reconciliation contract below).

Factor a crate-private transaction-taking feed-head helper in `feed.rs`, used
by both this method and the public standalone `feed_head`. The caller owns the
transaction and its explicit isolation level; the helper acquires the barrier
and reads the head but never commits or releases it. The standalone public API
continues to begin and commit its own transaction. Wrapping a call to that
public API in an outer transaction does not extend its independent lock.

**Why this closes the GC gap:** after the barrier drains source writers and
samples head H, no new source commit can allocate an ID until registration
commits. GC may still reap objects deleted at or before H, which this
future-facing MV intentionally skips. A deletion after H cannot become visible
before the new cursor fence does. A GC candidate selected before registration
therefore belongs to the skipped prefix; a later GC snapshot that sees a
post-H deletion also sees the committed cursor. No new GC lock or object-store
operation belongs inside this transaction. Subsequent slower-replica races use
the separate reconciliation contract below.

**Failure and retry:** advisory-lock or cursor-row lock timeout (`55P03`) is a
retryable catalog error. Roll back the entire transaction and keep the worker
unready; never continue with a cached head or partially initialized cursor.
If a commit acknowledgement is lost, retry `ensure_mv_cursor` from catalog
authority under role recovery. If the first commit landed, insert-if-absent
preserves its original cursor even when newer source commits now exist; if it
rolled back, the successful retry defines the future-facing start point. No
data processing starts until a call returns after successful commit. The
configured lock timeout applies per lock acquisition, not to the whole
transaction, and does not terminate the blocking writer.

**Cost:** this extends the existing exclusive feed barrier across cursor insert,
read, and commit. Same-source writers and concurrent initializers wait for that
short catalog transaction; other hypertables remain independent. V1 uses the
same path for startup and recovery even when a cursor already exists, keeping
one initialization contract. Keep SQL preparation, object reads, and all target
work outside the barrier. This is startup/recovery work, not a per-event lock.

### Active-active MV is correct before it is efficient

All replicas share cursor name `mv/{pipeline_id}`. Two replicas may race the
same source commit; canonical identity makes one target ADD durable and the
other `AlreadyApplied`, and the monotonic cursor makes both advances safe.
A replica whose inputs have already been reaped instead reconciles the obsolete
attempt from catalog authority as specified below; it need not finish the read.
Attempt-specific uploads remain pending/orphan-covered and GC-reapable.

V1 does not add a second lease subsystem. Operators should run one MV role per
fleet until contention evidence justifies a pipeline lease. Record
`AlreadyApplied`/duplicate-work counters so that follow-up has a trigger. Do not
claim duplicate object work is eliminated.

### Slower MV replicas reconcile superseded attempts

The shared cursor represents a **resolved prefix for the pipeline**, including
published ADDs, empty results, configured skips, and ignored REPLACE/DELETE
events. Every worker must process its source events in order and advance only
after resolving each one. The cursor is a GC fence for that unresolved prefix;
it is not a read pin for each replica. V1 permits GC to remove an obsolete
replica's inputs and makes that replica abandon unnecessary work safely.

Use the following contract for ADD event C. Read catalog authority from the
primary, never from a cached local cursor or a lagging read replica:

1. Construct the canonical `OperationIdentity::mv` before opening source files.
   Read the registered shared cursor with `read_mv_cursor(pipeline_id)`. If its
   value S is at least C, the event is already resolved: discard this attempt
   and its cached feed page, then fetch again after S. Do not write the cursor
   merely to acknowledge this observation.
2. Otherwise call `lookup_operation` for this exact identity. `Committed`
   proves target publication even if a peer crashed before cursor advancement:
   monotonically advance the cursor through C without reading or publishing
   again, then reload progress. `Absent` permits attempting the source read;
   it is not a guarantee that objects remain available or that another replica
   will not finish in the meantime. A fingerprint collision is permanent.
3. On **any source-part read failure**, including a NotFound wrapped by Parquet
   or Arrow, cancel/join remaining reads for this attempt and discard partial
   SQL input/output. Re-read the shared cursor and reconcile the exact target
   identity as above. Cursor S >= C proves resolution even when the peer
   produced no target commit because its result was empty or policy allowed a
   skip. `Committed` with a cursor below C permits the same monotonic advancement
   as step 2. If neither proves completion, re-read the cursor once more after
   an `Absent` lookup, covering an empty/skip outcome that committed between
   those reads. No object absence or local processing state is completion proof.
4. If the source is still required after these fresh checks, retain the typed
   source error. A missing required part is a permanent `SourceInputUnavailable`
   error with source commit, part ID, and path; it must not advance the cursor,
   loop through automatic retries forever, or be consumed by the configured
   `skip` policy. Other storage failures keep their appropriate error classification.
   A catalog failure during any completion check goes through role recovery;
   it must not be converted into a skip or a permanent missing-input diagnosis.
5. After successful reads, recheck the shared cursor before target publication
   and abandon the attempt if already resolved. This reduces duplicate work,
   but the check is not a lock: a race after it is handled by canonical target
   identity and Committed/AlreadyApplied. A target mutation with an ambiguous
   acknowledgement still follows the existing supervisor reconciliation path;
   supersession must not bypass an unresolved target mutation.

`read_mv_cursor` must distinguish an existing zero cursor from a missing row.
An absent registration after successful initialization is a catalog invariant
error, not permission to recreate a cursor at today's head and skip history.
Keep this read separate from `ensure_mv_cursor`: ordinary progress checks need
one SELECT and do not reacquire the startup feed barrier.

On supersession, cancel and join attempt-owned work before starting another
event; no detached task may later publish the discarded attempt. Uncommitted
attempt-specific uploads remain pending and follow ordinary orphan GC. Do not
delete objects that a peer may have committed. Reload the durable cursor and
fetch a new page rather than replaying a stale batch of events. Supersession is
normal progress and leaves the role healthy; catalog recovery temporarily
degrades only that role. Preserve underlying error causes through storage,
Parquet, and Arrow wrappers so a reader error cannot bypass reconciliation.

**Why the GC race is safe:** if the slow worker's cursor check precedes the fast
worker's progress, its source read may fail after GC. Legitimate GC deletion
requires the shared cursor to have passed the deleting commit, and therefore
the source ADD being attempted. A fresh cursor read after that failure observes
the resolved prefix. If the catalog is unavailable, wait for authority through
recovery. If authority offers neither cursor nor operation proof, treat the
input as genuinely required and report its loss. There is no assumption that a
read completes within the GC grace period; the grace may already have expired.

**Tradeoff:** this avoids per-replica leases, lease expiry/fencing, and GC
changes, and a stalled replica cannot retain obsolete objects indefinitely.
It adds cursor and operation lookups to each attempted ADD and extra lookups
on a source-read error. A slow worker can waste reads, SQL work, and orphan
uploads before noticing peer completion. Record
`mv_superseded_attempts_total{phase,proof}` with fixed phase values
`before_read`, `source_read`, `before_publish` and proof values
`cursor`, `operation`; do not count an inferred operation completion as a real
`AlreadyApplied` commit response or a configured skip. This contract depends
on the replay-stability requirements tracked by R3 and does not resolve them.

## Canonical operation identities

Keep the existing ingest v1 vectors pinned. Add factories rather than changing
their bytes:

```rust
pub struct PipelineIngestIntent<'a> {
    pub pipeline_id: PipelineId,
    pub ranges: &'a [IngestRange],
    pub transformation_version: u32,
}

pub struct MvIntent {
    pub pipeline_id: PipelineId,
    pub source_hypertable_id: HypertableId,
    pub source_commit_id: CommitId,
    pub transformation_version: u32,
}

OperationIdentity::pipeline_ingest(target_hypertable_id, intent) // ingest/v2
OperationIdentity::mv(target_hypertable_id, intent)              // mv/v1
```

The target hypertable is the identity scope. Pipeline ID names the immutable
definition. Kafka ranges or the exact source commit name the consumed log
prefix. Generated object paths, output bytes, process/attempt IDs, cursor value,
wall time, and lease state are absent. Bump transformation version only when
the same immutable definition and source input would legitimately map to
different logical rows.

## Global constraints

- Rust edition 2024, toolchain >= 1.98; keep DataFusion 54 and Arrow/Parquet
  58.3 pinned in lockstep.
- Migration is exactly `crates/ukiel-catalog/migrations/0002_pipelines.sql` on
  the current consolidated tree. Refuse to edit `0001_init.sql`.
- Kafka group offsets are never read or committed. Edge-scoped catalog offsets
  remain atomic with target parts and retain the existing offset CAS.
- Kafka topics are externally provisioned; auto-creation is disabled and
  selected topics must pass existence, partition, and `LogAppendTime` checks
  before readiness.
- Every object path is registered in `pending_objects` before upload. Commit
  clears the intent transactionally; failed attempts remain ordinary GC orphans.
- Kafka and MV target writes use the shared L0 writer properties and
  materialized/default-column path. No ad-hoc Parquet writer.
- Preserve the existing backpressure, metrics, sort validation, role-local
  recovery/readiness, and ambiguous-commit reconciliation behavior.
- `stall` never advances offsets/cursor. `skip` advances visibly and increments
  a bounded-label counter. Creation-time errors are permanent, not skippable.
  Missing required MV source objects are likewise not policy-skippable; a stale
  replica may abandon them only with durable completion proof.
- Unit tests run without Docker. Catalog/component tests use testcontainers.
  S12 is ignored e2e and runs through the existing Makefile targets.
- Conventional commits; no AI attribution.

---

### Task 1: Catalog entities and immutable bootstrap contract

**Files:**

- Create: `crates/ukiel-catalog/migrations/0002_pipelines.sql`
- Create: `crates/ukiel-core/src/pipeline.rs`
- Modify: `crates/ukiel-core/src/{ids,lib}.rs`
- Create: `crates/ukiel-catalog/src/pipelines.rs`
- Modify: `crates/ukiel-catalog/src/feed.rs`
- Modify: `crates/ukiel-catalog/src/lib.rs`
- Modify: `crates/ukiel-catalog/tests/catalog_test.rs`
- Create: `crates/ukiel-catalog/tests/mv_cursor_test.rs`

Add `PipelineId`, `StreamTableId`, `EndpointKind`, `ErrorPolicy`, `Delivery`,
`StreamTable`, and `Pipeline`. The pipeline carries source/target endpoint refs,
SQL, delivery, error policy, and optional `event_time_column`.

Migration tables:

- `stream_tables`: identity, unique name, unique topic for v1, `format = json`,
  JSONB schema, created timestamp;
- `pipelines`: identity, unique name, typed source/target kind+id, SQL, delivery,
  error policy, optional event-time output, created timestamp.

Extend `ingest_offsets` with nullable `stream_table_id`, a foreign key to
`stream_tables`, and a unique partial index over the logical edge and Kafka
partition. Creating a stream table adopts matching legacy topic rows by filling
that ID in the same transaction. Pipeline offset reads and writes require the
ID; the legacy topic column remains for compatibility and diagnostics.

V1 target kind is Parquet. Catalog creation validates that polymorphic endpoint
IDs exist in the correct table inside one transaction. It rejects Parquet
self-loops and, under a transaction-scoped pipeline-graph advisory lock, any
edge that would create a directed cycle. Multiple immutable definitions may
share an endpoint pair; there is no update/delete API.

Expose exact-match create/get-by-name/list methods. Workers use only
get-by-name for their configured startup list; list is administrative and must
never imply activation. Unknown TEXT enum values are catalog corruption errors,
never defaults. Add `ensure_mv_cursor(pipeline_id)` with the exact
[registration transaction contract](#first-mv-cursor-registration-holds-the-feed-barrier-through-commit)
above. Factor the transaction-taking feed-head helper and reuse it in the
standalone `feed_head`; preserve that API's existing prefix-ordering tests.
Add `read_mv_cursor(pipeline_id)` for progress reconciliation: resolve the
canonical key, return an existing row including zero, and reject a missing
registration without inserting anything or acquiring the feed barrier.

- [ ] Failing tests: round trip every field; duplicate names/topics;
  same endpoint pair under different names; missing/wrong-kind endpoints;
  reserved stream column; exact-match idempotent create; same-name mismatch;
  self-loop and multi-edge cycle rejection; concurrent opposite-edge creation;
  legacy offset adoption; edge-scoped offset independence and replacement
  continuity;
  unselected MV has no cursor; first selection initializes its cursor to current
  head atomically; concurrent first selections share one cursor.
- [ ] Registration concurrency tests use deterministic SQL gates and observed
  lock waits (following `feed_ordering_test.rs`), not sleep-based timing:
  - hold a real source writer after ID allocation; registration waits and its
    fresh head includes the writer on commit or omits it on rollback;
  - pause registration at cursor insert after sampling H; a same-source writer
    cannot allocate its commit ID until registration commits, while a writer
    on another hypertable completes;
  - race two first selections with a source commit between their barrier
    acquisitions; both preserve the first stored cursor, even though the
    second initializer samples a newer head;
  - on an empty source, verify a physical cursor row at zero; repeat after
    writes and verify initialization never advances it; an already advanced
    cursor is likewise preserved;
  - after registration, ADD then REPLACE a source part and expire the deletion
    grace; both GC candidate APIs must exclude it until the cursor passes the
    deleting commit, while pre-start tombstones remain eligible;
  - force a feed-lock timeout and separately cursor-row contention timeout:
    retryable classification, no partial registration, and no source writes
    blocked by a transaction left open after the error;
  - force rollback after the insert, then retry; simulate a lost successful
    commit acknowledgement, add newer source commits, then retry and verify the
    original cursor is returned instead of the newer head;
  - missing/wrong-kind pipelines create no cursor and acquire no source barrier.
- [ ] Cursor-read tests: an existing zero and an advanced cursor round-trip;
  missing registration is an explicit invariant error and creates nothing;
  progress reads complete while a source writer holds the shared feed lock.
- [ ] Implement migration, core types, CRUD, structural validation, and exports.
- [ ] Verify and commit:

```bash
cargo test -p ukiel-core -p ukiel-catalog -- --test-threads=2
cargo clippy -p ukiel-core -p ukiel-catalog --all-targets -- -D warnings
git commit -m "feat: add immutable stream tables and pipelines"
```

### Task 2: Canonical pipeline-ingest and MV identities

**Files:**

- Modify: `crates/ukiel-core/src/operation.rs`
- Modify: `crates/ukiel-core/src/lib.rs`
- Modify: `crates/ukiel-catalog/tests/catalog_test.rs`

Implement the `ingest/v2` and `mv/v1` factories above with the existing
type-tagged, length-delimited canonical encoder. Reject empty ranges, duplicate
or invalid IDs/commits, and out-of-scope identities. Preserve every existing
ingest/compaction/delete pinned vector byte-for-byte.

The catalog needs no new reconciliation API: both identities travel through
`commit`/`commit_with_offsets`, `AmbiguousMutation`, supervisor
`lookup_operation`, and the fingerprint collision check already shipped.

- [ ] Pinned-vector tests for both new domains; ordering normalization for Kafka
  ranges; every semantic input changes the fingerprint; attempt details cannot
  be supplied.
- [ ] Catalog tests: same key+fingerprint is `AlreadyApplied`; forged
  fingerprint is a permanent collision; before/after-COMMIT fault lookup returns
  Absent/Committed for both domains.
- [ ] Verify and commit:

```bash
cargo test -p ukiel-core operation
cargo test -p ukiel-catalog --features fault-injection -- --test-threads=2
cargo clippy -p ukiel-core -p ukiel-catalog --all-targets -- -D warnings
git commit -m "feat: identify pipeline ingest and mv mutations"
```

### Task 3: Pure deterministic SQL-over-batches crate

**Files:**

- Create: `crates/ukiel-pipeline/{Cargo.toml,src/{lib,error,validate,execute}.rs}`
- Create: `crates/ukiel-pipeline/tests/{validation,execution,boundary}.rs`
- Modify: root `Cargo.toml`

Interfaces:

```rust
pub struct TargetContract { /* writable fields, required fields, target schema */ }
pub struct PreparedPipeline { /* validated SQL/logical contract */ }

pub async fn prepare_pipeline(
    pipeline: &Pipeline,
    source_schema: SchemaRef,
    target: &TargetContract,
) -> Result<PreparedPipeline, PipelineError>;

pub async fn execute_pipeline(
    prepared: &PreparedPipeline,
    input: RecordBatch,
) -> Result<Vec<RecordBatch>, PipelineError>;
```

Register only the declared source. Walk the logical expressions and reject any
function whose volatility is not immutable. Reject joins/cross joins/multiple
table scans and every DDL/DML/statement class outside one SELECT. Validate the
output against the full writable target contract: required non-default fields,
packing/sort/partition columns, compatible types, no alias writes, no unknown
extra fields, and a valid optional event-time column.

Execution uses a fresh single-use DataFusion context per input batch. The input
schema and prepared schema must match exactly. Empty output retains the planned
schema. This crate has no catalog, Kafka, object-store, Ukiel worker, or CLI
dependency.

- [ ] Tests: filter/rename/derived partition; aggregation; missing key; wrong
  type; alias/extra output; immutable function accepted; `now`, random, DDL,
  DML, join, self-join, and second scan rejected; Kafka metadata predicate is
  replay-stable.
- [ ] Boundary test enforces the dependency rule.
- [ ] Verify and commit:

```bash
cargo test -p ukiel-pipeline
cargo clippy -p ukiel-pipeline --all-targets -- -D warnings
git commit -m "feat: validate and execute deterministic pipeline sql"
```

### Task 4: Shared batch-to-L0 publication path

**Files:**

- Create: `crates/ukiel-write/{Cargo.toml,src/{lib,error,writer,publisher}.rs}`
- Create: `crates/ukiel-write/tests/{writer,publisher,boundary}.rs`
- Modify: `crates/ukiel-ingest/src/{writer,flusher,lib,error}.rs`
- Modify: `crates/ukiel-ingest/tests/consumer_test.rs`
- Modify: root `Cargo.toml`

Extract target writing without changing existing output. Source JSON decoding
remains in `ukiel-ingest`:

```rust
pub fn rows_to_batch(..., rows: Vec<Value>) -> Result<RecordBatch, IngestError>;
```

`ukiel-write` exposes:

```rust
pub fn batches_to_l0_items(
    batches: &[RecordBatch],
    target: &Hypertable,
) -> Result<Vec<FlushItem>, WriteError>;

pub enum AddCommit<'a> {
    Ingest { offsets: &'a [OffsetRange], identity: &'a OperationIdentity },
    Plain { identity: &'a OperationIdentity, metric_role: &'static str },
}

pub async fn publish_add(
    &self,
    target: &Hypertable,
    items: Vec<FlushItem>,
    commit: AddCommit<'_>,
) -> Result<CommitResult, WriteError>;
```

`batches_to_l0_items` applies defaults/materialized columns, sorts by the
target sort key, groups by declared partition columns, and uses the shared L0
writer properties. `publish_add` assigns paths, registers every pending object
before upload, uploads, then calls `commit_with_offsets` or `commit`. It accepts
a prebuilt identity so callers prove intent before object work. Empty ingest
items still allow an offset-only ADD; empty MV output does not call it.

Keep ingest metrics at the ingest caller boundary; `ukiel-write` emits
catalog/object failure metrics with bounded `metric_role` only. The object-store
wrapper continues to own cache prewarming. `ukiel-write` has no rdkafka,
DataFusion, query-server, worker-supervisor, or CLI dependency.

- [ ] Characterization tests prove existing ingest Parquet bytes, stats,
  materialized columns, upload-intent ordering, offset CAS, and result are
  unchanged.
- [ ] Plain ADD test proves pending intents clear on success, survive failed
  commit, and ambiguous errors carry the caller's identity.
- [ ] Boundary test proves both ingest and MV can consume `ukiel-write` without
  either depending on the other's source runtime.
- [ ] Verify and commit:

```bash
cargo test -p ukiel-write -p ukiel-ingest -- --test-threads=2
cargo clippy -p ukiel-write -p ukiel-ingest --all-targets -- -D warnings
git commit -m "refactor: share idempotent l0 publication"
```

### Task 5: Kafka-to-Parquet pipeline worker

**Files:**

- Modify: `crates/ukiel-ingest/Cargo.toml`
- Modify: `crates/ukiel-ingest/src/{config,consumer,flusher,error}.rs`
- Modify: `crates/ukiel-ingest/tests/consumer_test.rs`

Replace `TableRoute`/`RouteIngest` with catalog-loaded Kafka pipeline tasks
inside one `IngestWorker`. `IngestConfig` carries a non-empty explicit pipeline
name list. A role rebuild creates a fresh worker, resolves exactly those names,
rejects duplicate names, wrong source kinds, duplicate selected source/target
pairs, and cycles, prepares SQL, creates fresh consumers, reloads catalog
offsets, and explicitly seeks Kafka. It does not discover unlisted catalog
pipelines. Readiness fires only after every selected pipeline has prepared and
positioned itself.

Every selected pipeline resolves its source stream table to an externally
provisioned topic on the one configured Kafka cluster. Set
`allow.auto.create.topics=false`. Use Kafka metadata and topic-config APIs to
require a non-empty partition set and `LogAppendTime` before loading edge-scoped
catalog offsets, assigning every partition, and seeking. A new partition with
no catalog row starts at offset zero. Missing topics, inaccessible topic config,
timestamp mismatch, out-of-range stored offsets, and topic recreation are
permanent startup errors; never silently reset to `earliest` or `latest`.

Each buffered row includes the stable reserved Kafka broker append time. Flush:

1. decode source JSON plus reserved metadata;
2. execute prepared SQL;
3. encode/sort/group target L0 items;
4. run the existing L0 backpressure checks against the derived target partitions;
5. build `OperationIdentity::pipeline_ingest` before any upload;
6. publish parts and offsets atomically; and
7. emit existing ingest/freshness metrics plus pipeline outcome metrics.

`stall` retains rows and ranges. `skip` publishes an empty ADD with offsets and
canonical identity, increments `pipeline_batches_skipped_total`, and drops the
buffer only after commit/AlreadyApplied. Poison JSON remains the current visible
per-message skip. No Kafka group offset is committed.

Delete `TableRoute`, hardcoded day grouping, and the age/future knobs only in
the same commit that lands the stable-metadata bounds tests. Keep generic flush,
backpressure, and partition-spread settings.

- [ ] Component tests: explicit selection ignores unlisted pipelines; missing,
  duplicate, wrong-kind, duplicate-pair, and cyclic selections fail startup;
  topic auto-creation disabled; absent/empty topic and topic-config denial fail;
  new edge and new partition start at zero; replacement resumes the edge offset;
  fan-out targets advance independently; out-of-range offsets fail rather than
  reset; derived partition/filter; stable append-time predicate;
  `LogAppendTime` accepted; `CreateTime`/unavailable become NULL; crash/rebuild
  seeks catalog offsets; exact replay returns AlreadyApplied; changed pipeline
  identity hits OffsetRace rather than double-applying; slowdown/stop/memory
  valve; stall/skip; poison; readiness.
- [ ] Verify and commit:

```bash
cargo test -p ukiel-ingest -- --test-threads=2
cargo clippy -p ukiel-ingest --all-targets -- -D warnings
git commit -m "feat: ingest through kafka sql pipelines"
```

### Task 6: Parquet-to-Parquet MV fleet

**Files:**

- Create: `crates/ukiel-mv/{Cargo.toml,src/{lib,error,reader,worker}.rs}`
- Create: `crates/ukiel-mv/tests/{mv,recovery,boundary}.rs`
- Modify: root `Cargo.toml`

One `MvFleet` owns only the explicitly configured Parquet-source pipeline names
under one supervised role. On every construction/rebuild it resolves the same
names and reloads their definitions, target contracts, prepared SQL, and
`mv/{pipeline_id}` cursors via `ensure_mv_cursor(pipeline_id)` and its
barrier-through-commit contract before reporting ready. Do not compose public
`feed_head` and `set_cursor` calls to initialize a cursor. Validate the complete
selection and prepare its SQL/target contracts before cursor registration.
Missing or duplicate names, wrong source kinds,
duplicate selected source/target pairs, and a cyclic selected graph are
permanent startup errors. Unlisted catalog definitions remain dormant. Ready
fires only after every selected pipeline is reconstructed from authority.

`ukiel-mv` depends on `ukiel-pipeline` for transformation and `ukiel-write` for
target publication. It does not depend on `ukiel-ingest`.

For each pipeline, read ordered `changes_since(source, cursor, limit)` but
process one event at a time. For ADD, build `OperationIdentity::mv` and check
shared progress and operation completion before reading exactly `event.added`
with schema adaptation. Execute SQL, encode target items, recheck progress, and
call shared `publish_add(Plain)`. Advance the monotonic cursor only after a
Committed/AlreadyApplied response or authoritative Committed operation lookup.
An already advanced shared cursor requires no write. For REPLACE/DELETE or
empty output, advance without target mutation. `stall` preserves cursor; an
allowed `skip` advances and counts, except for missing required source objects.

The cursor row created on first selection fences the unresolved pipeline prefix.
It does not pin objects for a slow replica after a peer resolves that prefix.
Implement the full
[superseded-attempt contract](#slower-mv-replicas-reconcile-superseded-attempts),
including checks after source read failures and the no-target-commit cases.
Never infer completion from object-store state. A target commit followed by a
lost cursor acknowledgement is safe: rebuild sees the advanced cursor or
reconciles the canonical operation before re-reading inputs.

Use bounded one-commit input and DataFusion's configured memory/spill behavior;
record input rows/bytes, output rows/bytes, duration, feed lag, errors,
AlreadyApplied, and skipped batches. Do not duplicate the ingest upload-intent
path. No Kafka dependency in `ukiel-mv`.

- [ ] Tests: explicit selection ignores unlisted pipelines; invalid selections
  fail before readiness; two ADDs yield correct partial aggregates; source REPLACE and
  DELETE do not double-count; cursor fence blocks GC; empty output; stall/skip;
  crash after target commit before cursor; before/after-COMMIT ambiguity;
  collision permanent; two workers converge exactly once while duplicate work
  is counted honestly.
- [ ] Registration/recovery tests: timeout keeps the MV role unready, retries
  through role recovery without a cached head, and leaves sibling roles healthy;
  a lost successful registration acknowledgement followed by newer source
  commits resumes at the original durable cursor. Exercise real GC after
  post-registration ADD/REPLACE and verify unread objects remain available to a
  single consumer.
- [ ] Superseded-attempt tests use deterministic read/SQL gates and real GC:
  pause slow B after its initial cursor/operation checks, let fast A publish ADD
  C and pass the replacing commit D, expire the deletion grace, run GC until
  the original source object is gone, then resume B. B reconciles the resulting
  read failure, cancels partial work, stays healthy, reloads the durable cursor,
  and does not publish again; the target contains exactly one logical result.
- [ ] Repeat that race for partial multi-part reads, wrapped Parquet/Arrow
  NotFound errors, empty SQL output, and a configured skip with no target
  operation. Separately inject a transient source read error and gate an
  empty/skip advancement between the first error-time cursor read and an Absent
  operation lookup to exercise the final cursor recheck. Also pause B after
  successful reads but before publication and verify the progress recheck
  abandons its work; a later race still converges by identity.
- [ ] Completion-proof tests: target operation committed but cursor unadvanced
  avoids source reads and advances once; genuinely missing required input with
  no proof raises `SourceInputUnavailable` and cannot be policy-skipped;
  operation fingerprint collision remains permanent. An unavailable catalog
  during post-error checks triggers role recovery with no cursor movement,
  then converges from fresh authority, without failing sibling roles.
- [ ] Cancellation/metrics tests: no attempt-owned read/upload/publish task
  survives supersession; pending uploads remain GC-covered; progress observations
  do not refresh cursor timestamps; bounded supersession labels distinguish
  cursor proof, operation proof, configured skips, and actual AlreadyApplied.
- [ ] Boundary test enforces no Kafka and no query-server dependency.
- [ ] Verify and commit:

```bash
cargo test -p ukiel-mv -p ukiel-gc -- --test-threads=2
cargo clippy -p ukiel-mv --all-targets -- -D warnings
git commit -m "feat: consume parquet changes into mv pipelines"
```

### Task 7: ukield configuration, bootstrap, recovery, health, and metrics

**Files:**

- Modify: `crates/ukield/src/{config,bootstrap,run,health,collector}.rs`
- Modify: `crates/ukield/tests/{bootstrap_test,health_metrics_test,startup_recovery_test}.rs`
- Modify: `ukield.example.toml`

Configuration:

- `[[tables]]` retains hypertable schema, packing/sort/partition/placement and
  namespaces; remove `topic` and the ingest age/future knobs;
- add `[[stream_tables]]` with immutable name/topic/format/columns; topics are
  unique because v1 configures one Kafka cluster per catalog deployment;
- add `[[pipelines]]` with name/from/to/sql, optional delivery/error policy, and
  optional `event_time_column` as immutable definitions;
- add required `pipelines = [...]` startup name lists to `[ingest]` and `[mv]`;
  enabled roles reject an empty list and there is no implicit catalog-wide
  discovery; and
- add bounded MV poll/feed settings.

Bootstrap order under `with_catalog_recovery` is hypertables, stream tables,
then validated exact-match pipelines. Validate SQL before creating a new
pipeline so an invalid definition is not stored. Cursor creation happens later,
when an MV worker first selects a valid definition. Existing mismatch is
permanent.

Add `Role::Mv`, health label, default role membership, `WorkerUp`, readiness,
and one `spawn_supervised` rebuild closure for `MvFleet`. Ingest and MV build
closures must resolve only their configured names inside the new worker future
on every attempt; capturing boot-time definitions across recovery or listing all
catalog pipelines is forbidden. The collector derives Kafka lag sources from
the selected Kafka pipelines and continues reporting catalog feed lag from the
selected MV cursors.

- [ ] Config/bootstrap tests: exact idempotency, mismatch refusal, invalid SQL
  leaves no pipeline/cursor, required non-empty role selections, unlisted
  definitions stay dormant, replacement rollout selects only one definition,
  duplicate-pair and cycle refusal, role defaults, reserved metadata, example
  parses.
- [ ] Recovery tests: ingest and MV each pass
  Starting→Healthy→Degraded→Reconciling→Healthy independently; siblings stay
  healthy; unknown/permanent errors fail the process.
- [ ] Metrics tests cover bounded role/outcome labels and no operation key as a
  metric label.
- [ ] Verify and commit:

```bash
cargo test -p ukield -- --test-threads=2
cargo clippy -p ukield --all-targets -- -D warnings
git commit -m "feat: supervise pipeline ingest and mv roles"
```

### Task 8: End-to-end S12 and compatibility suite

**Files:**

- Modify: `crates/ukiel-e2e/src/lib.rs`
- Modify: existing `crates/ukiel-e2e/tests/s0_*.rs` through `s11_*.rs` fixtures
  as required; preserve their assertions
- Create: `crates/ukiel-e2e/tests/s12_pipelines.rs`
- Modify: `Makefile`
- Modify: `docs/superpowers/specs/2026-07-05-ukiel-testing-design.md`

Move the shared e2e stack from table routes to one stream table plus Kafka
pipeline. S0–S11 must keep their existing semantic assertions, including S10
catalog outage and S11 lost-ack recovery.

S12 has two arms:

1. **Pipeline/MV equivalence:** pipeline filtering and derived partitioning;
   two source ADD commits; target partial aggregates re-aggregated to the oracle;
   source compaction REPLACE causes no target change.
2. **MV lost acknowledgement:** arm the existing commit-boundary seam for an MV
   target ADD; before/after-COMMIT cases make the role Reconciling, rebuild from
   cursor/feed authority, converge exactly once, and perform no second durable
   target mutation. A collision fails permanently.

The normal arm runs under `make e2e`; the fault arm joins `make e2e-ha`. Do not
renumber S9–S11 or reuse their files.

- [ ] Focused S12 passes repeatedly, then `make e2e-ha` passes at least eight
  consecutive runs without process exit or duplicate logical target rows.
- [ ] Full `make test` and `make e2e` pass.
- [ ] Commit:

```bash
git commit -m "test: prove pipeline and mv recovery end to end"
```

### Task 9: Documentation and roadmap close-out

**Files:**

- Modify: `docs/notes/2026-07-06-pipelines.md`
- Modify: `docs/superpowers/specs/2026-07-05-ukiel-design.md`
- Modify: `docs/superpowers/specs/2026-07-06-ukiel-monitoring.md`
- Modify: `README.md`
- Modify: `docs/superpowers/plans/2026-07-05-ukiel-v1-roadmap.md`

Mark v1 implemented; document immutable definitions, explicit startup
selection and restart-based rollout, acyclic pipeline graphs, stable Kafka
metadata, future-facing MV creation, partial aggregate semantics, shared-cursor
active-active correctness/duplicate-work limitation, role recovery, config, and
metrics. Remove the old age/future knob documentation only after its replacement
is live. Keep egress, backfill, pipeline updates, aggregating compaction, and MV
leases explicit follow-ups.

Explain that the MV cursor fences unresolved pipeline work rather than each
replica's reads. Document normal supersession after peer progress, its bounded
metrics, and the distinction between catalog-proven completion and a genuinely
missing required source object that fails without skipping. Include the
registration barrier and commit-acknowledgement retry contract.

Roadmap row 8 becomes Executed. Row 23 becomes ready to plan because all cursor
consumers now exist; rows 24 and 26 become writable after this interface lands.

- [ ] Documentation examples parse and names match the shipped metrics/config.
- [ ] Final `cargo fmt --check`, full tests, and doc-link checks pass.
- [ ] Commit separately from code:

```bash
git commit -m "docs: record pipelines v1 delivery"
```

## Final acceptance

This plan is complete only when:

- Kafka routing is represented by immutable catalog pipelines, not `TableRoute`;
- Kafka topics are external resources represented by immutable stream tables,
  while brokers and credentials remain deployment configuration;
- Kafka progress is edge-scoped and survives an immutable pipeline replacement;
- ingest and MV workers execute only their explicit startup name lists;
- Parquet pipeline self-loops and directed cycles are rejected;
- retries execute deterministic SQL over the same stable source metadata;
- parts+offsets and MV target ADDs carry canonical operation identities;
- every upload is intent-covered and every cursor is monotonic/GC-fencing;
- a slow MV replica survives peer progress and GC by reconciling durable
  completion, while genuinely missing required input never silently advances;
- ingest and MV reconstruct independently under the role supervisor;
- ambiguous MV commits reconcile through operation lookup before rebuild;
- REPLACE/DELETE never double-count an append-derived MV;
- existing guardrails and observability remain live;
- S0–S11 still pass and S12 proves ordinary plus lost-ack behavior; and
- no claim is made that v1 supports historical MV backfill, in-place pipeline
  updates, semantic aggregate merging, egress, or duplicate-work-free MV HA.

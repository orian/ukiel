# Ukiel Plan 8: Table Engines and Pipelines

> **For agentic workers:** Execute task by task against current main. Run each
> task's focused tests before its commit. The HA, operation-identity, upload-
> intent, guardrail, and metrics requirements below are acceptance criteria, not
> optional adaptations.

**Status:** Refreshed and ready to execute, 2026-07-16.

**Goal:** Implement the v1 scope from
`docs/notes/2026-07-06-pipelines.md`: catalog entities for Kafka stream tables
and pipelines; Kafka-to-Parquet SQL pipelines replacing `TableRoute`; and
Parquet-to-Parquet aggregation MVs consuming the catalog change feed. Egress to
Kafka remains roadmap row 24.

This refresh supersedes the 2026-07-07 plan. It is written against the current
tree after Plans 41–43 and the million-logical-table catalog proof. The current
catalog migrations are consolidated into `0001_init.sql`; this plan owns
`0002_pipelines.sql`. E2E scenarios S9–S11 are already taken; this plan owns
S12. The canonical operation domain `mv` was reserved by Plan 43 for this work.

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

All role workers run under Plan 42's supervisor. A recoverable catalog failure
drops the entire role worker and every buffer/plan/session it owned. A rebuilt
Kafka worker reloads pipeline definitions and offsets, then re-seeks Kafka. A
rebuilt MV worker reloads definitions and monotonic cursors. An ambiguous target
commit carries a Plan-43 `OperationIdentity`; the supervisor remains
`Reconciling` until `lookup_operation` answers, discards the old attempt, and
rebuilds from authority.

## Decisions fixed by this refresh

### Pipeline definitions are immutable in v1

There is no `UPDATE pipeline`. SQL, endpoints, error policy, delivery mode, and
event-time output are immutable. An operator creates a new pipeline identity for
a semantic change. Bootstrap accepts an existing definition only when every
field matches; a same-name mismatch is a permanent startup error.

This is load-bearing for deterministic replay and operation identity: a
`PipelineId` always names one transformation. Schema/ALTER and pipeline rollout
belong to a follow-up plan.

### Deterministic event-time bounds use stable Kafka metadata

The old plan said both “delete time-relative knobs” and “allow only deterministic
SQL,” but did not provide a deterministic clock. V1 resolves that explicitly.

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

The engine never parses partitions or knows what “old” means. The plan-19
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

Parquet-source pipeline creation initializes `worker_cursors` atomically at the
source's current feed head. V1 MVs are therefore future-facing and never claim
to backfill history whose objects may already have been reaped. Creating them
before source ingestion naturally initializes at zero. Explicit historical
backfill is a separate plan.

### Active-active MV is correct before it is efficient

All replicas share cursor name `mv/{pipeline_id}`. Two replicas may race the
same source commit; canonical identity makes one target ADD durable and the
other `AlreadyApplied`, and the monotonic cursor makes both advances safe.
Attempt-specific uploads remain pending/orphan-covered and GC-reapable.

V1 does not add a second lease subsystem. Operators should run one MV role per
fleet until contention evidence justifies a pipeline lease. Record
`AlreadyApplied`/duplicate-work counters so that follow-up has a trigger. Do not
claim duplicate object work is eliminated.

## Canonical operation identities

Keep the existing Plan-43 ingest v1 vectors pinned. Add factories rather than
changing their bytes:

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

- Rust edition 2024, toolchain >= 1.96; keep DataFusion 54 and Arrow/Parquet
  58.3 pinned in lockstep.
- Migration is exactly `crates/ukiel-catalog/migrations/0002_pipelines.sql` on
  the current consolidated tree. Refuse to edit `0001_init.sql`.
- Kafka group offsets are never committed. Catalog offsets remain atomic with
  target parts and retain the issue-0003 CAS.
- Every object path is registered in `pending_objects` before upload. Commit
  clears the intent transactionally; failed attempts remain ordinary GC orphans.
- Kafka and MV target writes use the shared L0 writer properties and
  materialized/default-column path. No ad-hoc Parquet writer.
- Preserve Plan-18 backpressure, Plan-20/21 metrics, Plan-27 sort validation,
  Plan-42 role-local recovery/readiness, and Plan-43 ambiguous reconciliation.
- `stall` never advances offsets/cursor. `skip` advances visibly and increments
  a bounded-label counter. Creation-time errors are permanent, not skippable.
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
- Modify: `crates/ukiel-catalog/src/lib.rs`
- Modify: `crates/ukiel-catalog/tests/catalog_test.rs`

Add `PipelineId`, `StreamTableId`, `EndpointKind`, `ErrorPolicy`, `Delivery`,
`StreamTable`, and `Pipeline`. The pipeline carries source/target endpoint refs,
SQL, delivery, error policy, and optional `event_time_column`.

Migration tables:

- `stream_tables`: identity, unique name, unique topic for v1, `format = json`,
  JSONB schema, created timestamp;
- `pipelines`: identity, unique name, typed source/target kind+id, SQL, delivery,
  error policy, optional event-time output, created timestamp; and
- a unique `(source_kind, source_id, target_kind, target_id)` definition.

V1 target kind is Parquet. Catalog creation validates that polymorphic endpoint
IDs exist in the correct table inside one transaction. For a Parquet source it
also inserts `worker_cursors(worker = 'mv/{id}', hypertable_id = source_id,
last_commit_id = current feed head)` in that transaction. No update/delete API.

Expose exact-match create/get/list methods and filtered
`list_pipelines_by_source_kind`. Unknown TEXT enum values are catalog corruption
errors, never defaults.

- [ ] Failing tests: round trip every field; duplicate names/topics/pairs;
  missing/wrong-kind endpoints; reserved stream column; exact-match idempotent
  create; same-name mismatch; MV cursor initialized to current head atomically.
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
inside one `IngestWorker`. A role rebuild creates a fresh worker, lists current
immutable definitions, prepares SQL, creates fresh consumers, reloads catalog
offsets, and explicitly seeks Kafka. Readiness fires only after every pipeline
has prepared and positioned itself.

Each buffered row includes the stable reserved Kafka broker append time. Flush:

1. decode source JSON plus reserved metadata;
2. execute prepared SQL;
3. encode/sort/group target L0 items;
4. run Plan-18 backpressure against the derived target partitions;
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

- [ ] Component tests: derived partition/filter; stable append-time predicate;
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

One `MvFleet` owns all catalog-listed Parquet-source pipelines under one
supervised role. On every construction/rebuild it reloads definitions, target
contracts, prepared SQL, and `mv/{pipeline_id}` cursors. Ready fires only after
all are reconstructed from authority.

`ukiel-mv` depends on `ukiel-pipeline` for transformation and `ukiel-write` for
target publication. It does not depend on `ukiel-ingest`.

For each pipeline, read ordered `changes_since(source, cursor, limit)` but
process one event at a time. For ADD, read exactly `event.added` with schema
adaptation, execute SQL, encode target items, build `OperationIdentity::mv`, and
call shared `publish_add(Plain)`. Only after Committed/AlreadyApplied advance the
monotonic cursor. For REPLACE/DELETE or empty output, advance without target
mutation. `stall` preserves cursor; `skip` advances and counts.

The cursor row created with the pipeline is the GC fence that keeps unread ADD
objects alive even if compaction tombstones them. Never infer completion from
object-store state. A target commit followed by a lost cursor acknowledgement
is safe: rebuild either sees the advanced cursor or replays the same source
commit into AlreadyApplied, then advances.

Use bounded one-commit input and DataFusion's configured memory/spill behavior;
record input rows/bytes, output rows/bytes, duration, feed lag, errors,
AlreadyApplied, and skipped batches. Do not duplicate the ingest upload-intent
path. No Kafka dependency in `ukiel-mv`.

- [ ] Tests: two ADDs yield correct partial aggregates; source REPLACE and
  DELETE do not double-count; cursor fence blocks GC; empty output; stall/skip;
  crash after target commit before cursor; before/after-COMMIT ambiguity;
  collision permanent; two workers converge exactly once while duplicate work
  is counted honestly.
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
- add `[[stream_tables]]` with name/topic/format/columns;
- add `[[pipelines]]` with name/from/to/sql, optional delivery/error policy, and
  optional `event_time_column`; and
- add bounded MV poll/feed settings.

Bootstrap order under `with_catalog_recovery` is hypertables, stream tables,
then validated exact-match pipelines. Validate SQL before creating a new
Parquet-source pipeline so its initial cursor is not installed for an invalid
definition. Existing mismatch is permanent.

Add `Role::Mv`, health label, default role membership, `WorkerUp`, readiness,
and one `spawn_supervised` rebuild closure for `MvFleet`. Ingest and MV build
closures must load definitions inside the new worker future on every attempt;
capturing boot-time definitions across recovery is forbidden. The collector
derives Kafka lag sources from Kafka pipelines and continues reporting catalog
feed lag from `worker_cursors`.

- [ ] Config/bootstrap tests: exact idempotency, mismatch refusal, invalid SQL
  leaves no pipeline/cursor, role defaults, reserved metadata, example parses.
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

Mark v1 implemented; document immutable definitions, stable Kafka metadata,
future-facing MV creation, partial aggregate semantics, shared-cursor
active-active correctness/duplicate-work limitation, role recovery, config, and
metrics. Remove the old age/future knob documentation only after its replacement
is live. Keep egress, backfill, pipeline updates, aggregating compaction, and MV
leases explicit follow-ups.

Roadmap row 8 becomes Executed. Row 23 becomes ready to plan because all cursor
consumers now exist; rows 24 and 26 become writable after this interface lands.

- [ ] Documentation examples parse and names match the shipped metrics/config.
- [ ] Final `cargo fmt --check`, full tests, and doc-link checks pass.
- [ ] Commit separately from code:

```bash
git commit -m "docs: record pipelines v1 delivery"
```

## Final acceptance

Plan 8 is complete only when:

- Kafka routing is represented by immutable catalog pipelines, not `TableRoute`;
- retries execute deterministic SQL over the same stable source metadata;
- parts+offsets and MV target ADDs carry canonical operation identities;
- every upload is intent-covered and every cursor is monotonic/GC-fencing;
- ingest and MV reconstruct independently under the Plan-42 supervisor;
- ambiguous MV commits reconcile through Plan 43 before rebuild;
- REPLACE/DELETE never double-count an append-derived MV;
- Plan-18/20/21 guardrails and observability remain live;
- S0–S11 still pass and S12 proves ordinary plus lost-ack behavior; and
- no claim is made that v1 supports historical MV backfill, in-place pipeline
  updates, semantic aggregate merging, egress, or duplicate-work-free MV HA.

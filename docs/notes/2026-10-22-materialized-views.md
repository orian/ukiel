# Materialized views over Parquet

The opt-in `mv` role runs immutable Parquet-to-Parquet SQL pipelines. It reads
one source ADD commit, executes SQL once across all its files and Arrow batches,
writes sorted L0 Parquet files through the shared writer, publishes one target
ADD, and then advances its durable source cursor. Kafka ingestion continues to
use the existing `[[tables]].topic` routes; Kafka SQL pipelines are separate
unfinished work in [Plan 8](../superpowers/plans/2026-07-07-ukiel-pipelines.md).

## Configuration

Add `"mv"` to the process's `roles` and select the pipeline names explicitly:

```toml
roles = ["ingest", "mv", "query", "compactor", "gc"]

[mv]
pipelines = ["event_counts_v1"]
poll_interval_ms = 1000
input_bytes = 67108864
output_bytes = 67108864
output_rows = 1000000
operator_memory_bytes = 134217728

# Alongside an existing "events" source with tenant_id and ts columns:
[[tables]]
name = "event_counts"
ts_column = "ts"
packing_key = "tenant_id"
sort_key = ["tenant_id", "ts"]
partition_columns = []
namespaces = [7]

[[tables.columns]]
name = "tenant_id"
type = "int64"

[[tables.columns]]
name = "ts"
type = "timestamp_ms"

[[tables.columns]]
name = "n"
type = "int64"

[[tables.columns]]
name = "inserted_at"
type = "timestamp_ms"
nullable = false
default = "now()"

[[pipelines]]
name = "event_counts_v1"
source = "events"
target = "event_counts"
sql = "SELECT tenant_id, MIN(ts) AS ts, COUNT(*) AS n FROM events GROUP BY tenant_id"
error_policy = "stall"
```

`[[pipelines]]` bootstraps definitions even when the MV role is disabled. Only
names selected in `[mv].pipelines` register consumers and run. Existing
definitions must match exactly; changing SQL, endpoints, or error policy
requires a new name. Enabled MV requires a nonempty selection. Missing names,
duplicate names or selected source/target pairs, wrong endpoint kinds, cycles,
unsupported SQL, and incompatible target columns fail startup before readiness.

The SELECT must scan its declared source exactly once. Joins, additional scans,
DDL/DML, and other statements are outside v1. Output can omit default,
materialized, and nullable non-key columns; target aliases and materialized
columns cannot be supplied by SQL. Packing, sort, and partition fields must be
available from output or target expressions. Target partition values are taken
from their named columns; use SQL to derive them when needed. SQL timestamps
written to `timestamp_ms` are converted to epoch milliseconds.

## Processing and replay

First selection creates `worker_cursors["mv/<pipeline-id>", source]` at the
source's current complete feed head. The head read and cursor insert hold the
same exclusive feed barrier through transaction commit. MV is **future-facing**:
it does not backfill historical ADDs. Later starts retain the existing cursor,
including zero. To capture the beginning of a new source, select its MV before
starting source writes. In an all-in-one process, initial MV registration
finishes before that process starts ingest or GC; other processes still need
the same deployment ordering.

Every attempt derives `mv/v1` identity from the pipeline ID, source table and
commit IDs, target table, and transformation version. Output values are not
part of the identity. `now()`, `random()`, LIMIT, and order-sensitive SQL are
allowed: attempts may differ, and the **first committed outcome wins**.

An empty result or configured skip publishes an identified empty ADD with no
Parquet objects before advancing progress. A later nonempty attempt cannot
replace it. Downstream MVs propagate these completion-only ADDs without running
SQL, including `COUNT(*)`, so empty markers cannot manufacture rows.

Source REPLACE and DELETE events only advance progress. They do not retrigger
SQL or retract prior results. Aggregates are partial per source ADD; query the
example with `SELECT SUM(n) FROM event_counts`. Source deletions and retention
do not cascade to targets. Replacing/aggregating table merge policies are not
implemented by this work.

Defaults and materialized values are computed when writing output. Compaction
and deletion rewrites preserve stored values, including NULLs, and backfill
expressions only for columns absent from the original file. Native clock
expressions therefore do not change stored timestamps during compaction.

## Failure, limits, and shutdown

`stall` leaves that pipeline's cursor at the failed event and retries; other
selected pipelines continue. `skip` applies only to runtime SQL/data errors and
publishes an empty identified outcome. Invalid contracts, budgets, catalog or
storage failures, and missing required source objects cannot be policy-skipped.

Input bytes cover retained decoded batches for the whole ADD. Output row/byte
limits cover the whole SQL result, with another byte check after target
defaults/materialization. DataFusion operators have a separate managed-memory
limit; spilling is disabled. Reads are sequential by part with 8192-row decode
batches. These limits are not a process RSS cap: Parquet metadata/row-group
buffers, one decode batch, sorting, concatenation, and encoding add temporary
memory. Oversized events fail without target publication or cursor advancement.

An MV runs under the existing role supervisor. Recoverable catalog errors drop
and rebuild the worker from immutable definitions and durable progress. An
ambiguous target commit carries its identity through the shared writer; the
supervisor resolves its outcome before rebuilding. A committed target operation
with an unadvanced source cursor is sufficient to advance without rereading
source files.

Replicas share a monotonic cursor and can perform duplicate work. Before reads,
after read failures, and before publication, a replica checks durable completion.
If another replica completed the event, it discards obsolete work. This covers
GC reclaiming the slow replica's input after the fast replica passed its
replacement. Without cursor or exact-operation proof, missing input is an error.
Losing uploads remain protected by pending-object intents until orphan GC.

Normal shutdown drains the current pass, including owned reads and publication,
before returning. Allow enough shutdown grace for an event; no detached upload
or decode task survives a reported clean stop.

## Changing or retiring a pipeline

V1 has no fleet-wide activation fence or rolling semantic replacement. Creating
a second definition does not stop the first. For a replacement on the same
source/target pair:

1. Pause source writers across the fleet.
2. Let every old worker drain through the source feed head, then stop all old
   workers. Remove the old name from every process configuration.
3. Bootstrap the new immutable definition and start its selected workers while
   source writes remain paused. Wait for MV readiness and initial registration.
4. Resume source writers. Do not restart old workers.

The new cursor begins at the paused source head, so it neither reprocesses the
old definition's input nor skips a gap during handoff. Each definition keeps its
own identity and progress; output already written by the old definition remains.
For multiple chained MVs, drain upstream workers before downstream workers.

Retired cursor rows continue fencing GC. After every old worker has stopped,
explicitly remove its `worker_cursors` row as an administrative operation if
that definition will never resume. Removing it forfeits its old progress: a later
selection starts at the then-current head. Do not remove a cursor for an active
worker.

## Observability and checks

Readiness includes the MV role. `ukield_worker_up{role="mv"}` and the existing
role recovery/reconciliation metrics cover its lifecycle.
`catalog_feed_lag{worker="mv/<id>",hypertable="..."}` is collected through the
existing feed-lag collector. MV emits:

- `mv_events_committed_total`, `mv_already_applied_total`;
- `mv_superseded_attempts_total{phase,proof}` with fixed phase/proof labels;
- `pipeline_batches_skipped_total{role="mv"}` for committed configured skips;
- `mv_errors_total{phase="transform",policy="stall"}`;
- `mv_attempt_input_rows`, `mv_attempt_input_bytes`, `mv_attempt_output_rows`,
  `mv_attempt_output_bytes`, and `mv_event_duration_seconds` histograms.

Attempt histograms include duplicate work. Committed, skipped, AlreadyApplied,
and superseded counters describe distinct outcomes.

Component tests exercise real PostgreSQL, in-memory object storage, commit
boundary faults, and GC during a paused read. The daemon integration test runs
Kafka -> ingest -> MV -> HTTP queries with a native `now()` target default:

```sh
cargo test -p ukiel-mv -- --test-threads=2
cargo test -p ukield --test server_test -- --test-threads=2
cargo test -p ukiel-e2e --test s12_materialized_views -- --ignored --test-threads=1
```

S12 uses the existing compose stack, including MinIO. For an isolated run,
`UKIEL_E2E_PG` selects its database and `UKIEL_E2E_BUCKET` selects an already
provisioned bucket (default `ukiel`). S12 races two MV replicas and verifies
exactly one target ADD per source ADD and unchanged results after compaction.

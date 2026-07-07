# Pipelines: table engines, SQL ingest, MVs, and egress

ClickHouse-style factoring of data movement: **a pipeline is a SQL query
between two tables**, where a table has an *engine* — `parquet` (a hypertable,
the only storage engine) or `kafka` (a topic endpoint with a schema). This
replaces the bespoke `TableRoute` ingest config and generalizes the MV design;
roadmap plan 8 is authored against this note.

## Table engines

- **`engine = parquet`** — a hypertable: owns parts, packing key, sort order,
  placement, change feed. Exists today.
- **`engine = kafka`** — a topic endpoint: `name`, topic, format (v1:
  JSON-per-message), schema (reuses `TableColumns`). Owns no parts. Stored in
  a separate catalog entity (`stream_tables`) — hypertable columns like
  packing key and placement are meaningless here, and nullable-column soup is
  how catalogs rot. **No direct `SELECT` from a kafka table** (ClickHouse
  allows it; it is a destructive-read footgun) — only pipelines may read one.

## Pipelines

A catalog row: `source (kind, id)` → SQL transform → `target (kind, id)`.
Executed **batch-at-a-time** (ClickHouse block-at-a-time semantics): the
worker materializes a batch from the source, registers it as an in-memory
table in a DataFusion `SessionContext`, runs the SELECT, and hands the result
batch to the target's write path.

| FROM \ TO | `parquet` (hypertable) | `kafka` |
|---|---|---|
| **`kafka`** | **ingest** (replaces `TableRoute`; batch = one flush buffer) | stream transform (out of scope) |
| **`parquet`** (change feed) | **aggregation MV** (batch = new parts since cursor) | **egress / CDC publish** |

Example (ingest):

```
PIPELINE ingest_events FROM kafka_events TO events AS
  SELECT tenant_id, ts,
         coalesce(amount, 0.0) AS amount,
         date_trunc_day(ts)    AS day
  FROM kafka_events
  WHERE tenant_id IS NOT NULL
```

The `ts_column` route config and hardcoded day-partition derivation dissolve
into the SELECT — and so does boundary validation: the plan-19 event-time
bounds (`max_event_age_days` / future bound, interim route-level knobs)
become ordinary per-pipeline predicates here and the knobs are deleted, not
ported. That is what keeps the engine core time-agnostic (design doc,
"Catalog data model"): a migration backfill is just a pipeline with a
different predicate, not a config exception. The pipeline output must
contain the target's packing-key and sort columns; validated at pipeline
creation (SQL compiles against the source schema, deterministic functions
only — same `ukiel-expr` rules).

### Division of labor vs materialized columns

Both exist (as in ClickHouse); the rule:

- **Pipeline SQL** — source-specific parsing, filtering, renaming. Computed
  once at ingest, for that source only. Cannot backfill.
- **Materialized column** (`docs/notes/2026-07-06-materialized-columns.md`) —
  row-shaping that must hold for *every* write path and is recomputed on
  rewrites, i.e. heals old files (organic backfill).

## Delivery semantics (per-pipeline `delivery` property)

**Inbound (kafka → parquet): exactly-once, unchanged.** The pipeline SQL runs
between decode and write; Kafka offsets still commit in the same Postgres
transaction as the parts (`ingest_offsets`), and Kafka group offsets are never
used. The pipeline model does not touch this mechanism.

**Outbound (parquet → kafka): three levels, choose per pipeline:**

- **`at_most_once`** — no duplicates ever, losses on crash possible. Advance
  the change-feed cursor *before* producing. Cheapest; for gap-tolerant,
  idempotence-hostile consumers.
- **`at_least_once`** — no losses, duplicates on crash possible. Produce,
  await acks, then advance the cursor. Messages carry a
  `(commit_id, part_id, row_index)` key/header so consumers *can* dedup
  ("effectively once"). Recommended default.
- **`exactly_once`** — Kafka transactional producer; the *same Kafka
  transaction* produces the data messages and a cursor-position record to a
  compacted control topic (the Kafka Streams pattern). Startup reads the
  control topic with `read_committed` to find the true cursor. The Postgres
  cursor row becomes a mirror. Implementation work, not research — rdkafka
  supports transactional producers.

**GC fence nuance:** the reaper fences on `worker_cursors`, so an outbound
pipeline must keep its Postgres cursor mirror fresh even in `exactly_once`
mode where Kafka holds the truth — otherwise a stalled publisher's unread
parts could be reaped. A mirror lagging behind the Kafka-committed cursor is
safe (it fences more conservatively, never less).

## Semantics caveats (inherited from the block-at-a-time model)

- A pipeline SELECT sees **only the current batch**. `GROUP BY` in a
  kafka-sourced pipeline yields partial aggregates per flush. ClickHouse
  answers with AggregatingMergeTree; our equivalent would be an "aggregating"
  placement mode where the *compactor* re-aggregates on merge — a future
  feature, not v1. Aggregation MVs use the parquet-sourced variant, which can
  re-read parts.
- **No joins in v1** (would embed the query stack in ingest). Later.
- **Fan-out** (N pipelines per source topic) is supported by the model but is
  not atomic across targets (one commit per target — same as ClickHouse).
  v1: one pipeline per topic→table pair.
- **Runtime error policy** is explicit per pipeline: `stall` (default —
  retry the flush; exactly-once preserved) or `skip` (advance past the batch,
  count it). Creation-time validation catches schema/determinism errors.

## v1 scope (plan 8)

Catalog entities (`stream_tables`, `pipelines`), the kafka→parquet pipeline
replacing `TableRoute`, and the parquet→parquet MV pipeline on the existing
cursor machinery. Egress (parquet→kafka) is designed-in via the delivery
ladder above but implemented in a follow-up plan, starting `at_least_once`,
adding `exactly_once` when a consumer needs it.

# Ukiel Plan 8: Table Engines & Pipelines Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement `docs/notes/2026-07-06-pipelines.md` v1: catalog entities `stream_tables` (engine=kafka) and `pipelines`; the **kafka→parquet pipeline** replacing `TableRoute`/`RouteIngest` (transform, filtering, partition derivation, and boundary validation move into the pipeline SELECT; the plan-19 event-time knobs are **deleted, not ported** — design doc "time-agnostic core"); and the **parquet→parquet aggregation-MV pipeline** on the existing change-feed cursor machinery with idempotency-key exactly-once. Egress (parquet→kafka) stays a follow-up (roadmap row 24).

**Architecture:** A pipeline is a catalog row `source (kind, id) → SQL → target (kind, id)`, executed batch-at-a-time: materialize a source batch → register it in a DataFusion `SessionContext` under the source table's name → run the SELECT → hand the result to the target's write path. A new crate **`ukiel-pipeline`** owns SQL validation and execution (DataFusion dep stays out of `ukiel-ingest`); `ukiel-ingest` keeps Kafka mechanics and gains a batch-input write path (`rows_to_batch` + `batch_to_l0_items`). Inbound exactly-once is untouched: pipeline SQL runs between decode and write, offsets still commit transactionally with parts (incl. the plan-19 CAS). The MV worker is a change-feed cursor consuming **`kind == "add"` commits only** (REPLACE outputs are rewrites of already-processed data — consuming them would double-count), writing to the target with `idempotency_key = "pipeline:{id}:through:{commit_id}"` so a crash between commit and cursor-advance replays into `AlreadyApplied`. Guardrail continuity is a hard requirement: the plan-18 backpressure decision and the plan-20 ingest metrics move to the pipeline flush path, same shapes, same names.

**Tech Stack:** DataFusion 54 (pinned; `SessionContext::register_batch`, `sql_with_options` + `SQLOptions`), rdkafka 0.39, sqlx/Postgres migration, arrow 58.3.

**Prerequisites:** Written 2026-07-07 against main (plans 1–7, 9, 10, 19 executed). **Execute last, after plans 11–18 and 20**, per the roadmap's recommended order — see the adaptation constraints below for what each landed plan changes here.

## Global Constraints

- Rust edition 2024, toolchain ≥ 1.96; `make test` (Docker) per task; unit tests in Tasks 2–3 pass without Docker.
- **Migration numbering:** use the next free number at execution time (0006 if plan 17's `0005_target_file_bytes` has landed per the recommended order; renumber otherwise).
- **Guardrail continuity (non-negotiable):**
  - Port plan-18 backpressure into the pipeline ingest flush path: same `flush_decision(max_live_l0, deferred, total_rows, cfg)` shape, `live_l0_parts` probed on the *derived* partitions before commit; deferral keeps the raw buffer (offsets untouched → exactly-once preserved).
  - **Delete** the plan-19 event-time knobs (`max_event_age_days`, `max_event_future_secs`, `TableRoute.max_event_age_days`) and their config surface — bounds are now per-pipeline `WHERE` predicates. Update `ukield.example.toml`, issue-0004-adjacent doc lines, and any fixtures that set them.
  - Keep every plan-20 ingest metric emitting from the new flush path under the same names (`ingest_flush_*`, `ingest_rows_total`, `ingest_poison_messages_total`, `ingest_last_flush_event_ts_seconds`, …). `ingest_out_of_bounds_total` is deleted with the bounds (its job moves to pipeline predicates; note it in the monitoring spec).
- **Adaptation constraints:** plan 14 (if landed): encode via the shared `ukiel_core::writer_props`/`WriteOpts` instead of ad-hoc ZSTD props. Plan 17 (if landed): L0 output is unaffected (L0 is always packed; level ladder is compactor-side). Plan 11: preserve `sorting_columns` metadata on written files if its Task 4 landed.
- **Time-agnostic core:** nothing in this plan may interpret event time or parse partition values — derivation and validation are SQL in the pipeline row; the workers apply results opaquely.
- Commit messages: conventional style, no Claude/AI attribution.
- Every task ends with `cargo fmt && cargo clippy --all-targets -- -D warnings` clean.

---

### Task 1: Catalog entities — `stream_tables`, `pipelines`

**Files:**
- Create: `crates/ukiel-catalog/migrations/000N_pipelines.sql` (N = next free)
- Create: `crates/ukiel-core/src/pipeline.rs`; modify `crates/ukiel-core/src/lib.rs` (module + re-exports)
- Create: `crates/ukiel-catalog/src/pipelines.rs`; modify `crates/ukiel-catalog/src/lib.rs`
- Test: `crates/ukiel-catalog/tests/catalog_test.rs`

**Interfaces (Produces):**

Core types (`ukiel-core/src/pipeline.rs`):

```rust
use serde::{Deserialize, Serialize};

use crate::ids::{PipelineId, StreamTableId};

/// A kafka-engine table: a topic endpoint with a schema. Owns no parts;
/// only pipelines may read it (no direct SELECT — destructive-read footgun).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StreamTable {
    pub id: StreamTableId,
    pub name: String,
    pub topic: String,
    /// v1: "json" (JSON object per message).
    pub format: String,
    /// Same JSON schema shape as hypertables (`TableColumns::parse`).
    pub table_schema: serde_json::Value,
}

/// Which catalog entity a pipeline endpoint refers to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EndpointKind {
    Kafka,
    Parquet,
}

/// What to do when a batch fails at runtime (SQL error, encode error).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ErrorPolicy {
    /// Retry the same batch next tick; offsets/cursor do not advance.
    Stall,
    /// Advance past the batch, count it (poison-batch semantics).
    Skip,
}

/// A SQL transform between two tables (docs/notes/2026-07-06-pipelines.md).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Pipeline {
    pub id: PipelineId,
    pub name: String,
    pub source_kind: EndpointKind,
    pub source_id: i64, // StreamTableId or HypertableId per source_kind
    pub target_kind: EndpointKind, // v1: always Parquet
    pub target_id: i64,
    pub sql: String,
    /// Outbound delivery level; stored now, consumed by the egress plan.
    pub delivery: String,
    pub error_policy: ErrorPolicy,
}
```

Add `PipelineId` and `StreamTableId` to the id macro invocations in `ids.rs`.

Migration:

```sql
-- Table engines & pipelines (docs/notes/2026-07-06-pipelines.md).
-- stream_tables: engine=kafka endpoints. Deliberately NOT hypertable rows —
-- packing key / placement are meaningless for a topic, and nullable-column
-- soup is how catalogs rot.
CREATE TABLE stream_tables (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    topic TEXT NOT NULL,
    format TEXT NOT NULL DEFAULT 'json' CHECK (format IN ('json')),
    table_schema JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE pipelines (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    source_kind TEXT NOT NULL CHECK (source_kind IN ('kafka', 'parquet')),
    source_id BIGINT NOT NULL,
    target_kind TEXT NOT NULL CHECK (target_kind IN ('parquet')),
    target_id BIGINT NOT NULL,
    sql TEXT NOT NULL,
    delivery TEXT NOT NULL DEFAULT 'at_least_once'
        CHECK (delivery IN ('at_most_once', 'at_least_once', 'exactly_once')),
    error_policy TEXT NOT NULL DEFAULT 'stall'
        CHECK (error_policy IN ('stall', 'skip')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

Catalog CRUD (`pipelines.rs`): `create_stream_table(name, topic, format, schema) -> StreamTableId`, `get_stream_table(name)`, `get_stream_table_by_id(id)`, `create_pipeline(&Pipeline-shaped args) -> PipelineId`, `get_pipeline(name)`, `list_pipelines() -> Vec<Pipeline>` — same query/row-mapping style as `tables.rs`.

- [ ] **Step 1 (failing test):** in `catalog_test.rs`: create a stream table + a kafka→parquet pipeline pointing at the fixture hypertable; `list_pipelines` round-trips every field; duplicate names error (unique violation surfaces as a `CatalogError`).
- [ ] **Step 2:** migration + types + CRUD as specced. `ErrorPolicy`/`EndpointKind` map to/from the TEXT columns with exhaustive `match` (no stringly fallthrough — unknown DB value is a `CatalogError::NotFound`-style corruption error, not a default).
- [ ] **Step 3:** verify (`cargo test -p ukiel-catalog -- --test-threads=2`), fmt, clippy, commit `"feat: stream_tables and pipelines catalog entities"`.

---

### Task 2: Batch-input write path in ukiel-ingest

**Files:** `crates/ukiel-ingest/src/writer.rs` (+ its tests)

**Interfaces (Produces):**
- `pub fn rows_to_batch(cols: &TableColumns, rows: Vec<serde_json::Value>) -> Result<RecordBatch, IngestError>` — extracted from the body of `encode_rows` (the JSON→Arrow step that already exists inside it); `encode_rows` becomes a thin wrapper (rows_to_batch → defaults/materialized → sort → encode) so its signature and every existing test stay unchanged.
- `pub fn batch_to_l0_items(batch: &RecordBatch, cols: &TableColumns, partition_columns: &[String], packing_key: &str, sort_key: &[String]) -> Result<Vec<(serde_json::Value, EncodedPart)>, IngestError>` — the pipeline write path: apply defaults/materialized (`ukiel_expr::apply_defaults_and_materialized`), lexsort by `sort_key`, split into contiguous groups per distinct `partition_columns` value-tuple (group rows by the partition values, then slice), and encode each group as one L0 `EncodedPart` with its `{"col": value, ...}` partition JSON. Partition column values are read from the batch opaquely (Int64/Utf8 → JSON scalar) — no date logic anywhere.

- [ ] **Step 1 (failing tests, no Docker):** `rows_to_batch` produces the same batch `encode_rows` would encode (assert column values for a 3-row fixture); `batch_to_l0_items` on a batch with a `day` utf8 column holding two distinct values returns two items with correct `partition_values` JSON, per-item `key_min/max`, rows sorted by `(tenant_id, ts)` within each item, total rows preserved.
- [ ] **Step 2:** implement by extraction + the grouping described; encoding goes through the same props path `encode_rows` uses at execution time (plan-14-aware per the adaptation constraint).
- [ ] **Step 3:** verify `cargo test -p ukiel-ingest --lib` (all existing writer tests green — extraction is behavior-preserving), fmt, clippy, commit `"feat: batch-input L0 write path (rows_to_batch, batch_to_l0_items)"`.

---

### Task 3: `ukiel-pipeline` crate — validate & run pipeline SQL

**Files:**
- Create: `crates/ukiel-pipeline/` (Cargo.toml: datafusion workspace dep, arrow, ukiel-core, thiserror; add to workspace members)
- Create: `src/lib.rs`, `src/sql.rs`, `src/error.rs`

**Interfaces (Produces):**
- `pub fn validate_pipeline_sql(sql: &str, source_name: &str, source_schema: &Schema, target: &TargetContract) -> Result<(), PipelineError>` where `TargetContract { required_columns: Vec<String> }` (target's packing key + sort columns + partition columns). Validation: register an **empty** `RecordBatch` under `source_name`, plan via `sql_with_options` with DDL/DML/statements disallowed, check the output schema contains every required column, and walk the logical plan rejecting volatile functions (denylist: `now`, `current_date`, `current_time`, `current_timestamp`, `random`, `uuid` — the full-SQL analog of `ukiel-expr`'s determinism rule; extend the denylist if clippy of DataFusion 54's function set surfaces more volatile names).
- `pub async fn run_pipeline_sql(sql: &str, source_name: &str, batch: RecordBatch) -> Result<RecordBatch, PipelineError>` — fresh single-use `SessionContext`, `register_batch`, plan with the same `SQLOptions`, `collect`, `concat_batches` to one output batch (empty result = empty batch with the planned schema).

- [ ] **Step 1 (failing tests, no Docker):** the pipelines-note example SELECT over a 3-row source batch produces derived `day` + filtered rows; a SELECT missing the packing key fails validation naming the column; `SELECT now()` fails validation as non-deterministic; `INSERT` fails via `SQLOptions`.
- [ ] **Step 2:** implement; keep the crate free of catalog/kafka deps (pure SQL-over-batches).
- [ ] **Step 3:** verify `cargo test -p ukiel-pipeline`, fmt, clippy, commit `"feat: ukiel-pipeline crate - validated batch-at-a-time SQL execution"`.

---

### Task 4: kafka→parquet pipeline worker (replaces `RouteIngest`)

**Files:** `crates/ukiel-ingest/Cargo.toml` (add `ukiel-pipeline`), `crates/ukiel-ingest/src/consumer.rs`, `crates/ukiel-ingest/src/config.rs`, tests in `crates/ukiel-ingest/tests/consumer_test.rs`

**Interfaces:**
- `PipelineIngest` replaces `RouteIngest`: constructed from a `Pipeline` + its `StreamTable` + target `Hypertable`. The Kafka mechanics are unchanged (explicit assignment from catalog offsets, poison rows counted and skipped at decode, `max_buffer_rows` early flush). The buffer becomes a single `Vec<serde_json::Value>` (no per-day map — partition derivation is post-SQL now) plus the offset ranges and `max_event_ts_ms` tracker (plan 20).
- Flush path: `rows_to_batch(source_cols, rows)` → `run_pipeline_sql` → `batch_to_l0_items(result, target_cols, partition_columns, packing_key, sort_key)` → **backpressure check** (plan-18 `flush_decision` over `live_l0_parts` of the derived partitions; `Defer` keeps the raw row buffer and returns) → `Flusher::flush(hypertable, items, offsets)` (exactly-once + CAS unchanged) → plan-20 metrics + freshness gauge.
- Runtime failure (SQL error, encode error) honors `pipeline.error_policy`: `Stall` = log error + return without draining (same batch retries next tick); `Skip` = offset-only flush (drop the rows, advance offsets — the existing offsets-without-parts path), `counter!("pipeline_batches_skipped_total", "pipeline" => name)`.
- `IngestConfig.tables: Vec<TableRoute>` and everything only it used (`ts_column`, the plan-19 bounds knobs) are **deleted**; the worker enumerates kafka-sourced pipelines from the catalog at startup.

- [ ] **Step 1 (failing component test):** rewrite the existing consumer tests' fixtures to create a stream table + pipeline (SELECT with a `WHERE` filter and a derived `day`) instead of a `TableRoute`; assert produce→query round trip, the filter's effect, exactly-once crash/resume (the existing test's shape, now through the pipeline), and error-policy `Skip` on a pipeline whose SQL fails at runtime for a poisoned batch (offsets advance, no parts).
- [ ] **Step 2:** implement `PipelineIngest`; delete `TableRoute`, `RouteIngest`, the bounds knobs and their config surface. Creation-time validation runs in Task 6's bootstrap (`validate_pipeline_sql`), not here.
- [ ] **Step 3:** verify `cargo test -p ukiel-ingest -- --test-threads=2`, fmt, clippy, commit `"feat: kafka pipelines replace table routes; event-time knobs deleted"`.

---

### Task 5: parquet→parquet aggregation-MV worker

**Files:** `crates/ukiel-pipeline/src/mv.rs` (worker; add catalog/object_store deps to this crate for the worker module only), test `crates/ukiel-pipeline/tests/mv_test.rs`

**Interfaces:**
- `MvWorker { catalog, store, pipeline, poll_interval_ms }::run(shutdown)` ticking `run_once`:
  1. `events = changes_since(source_ht, cursor("pipeline:{name}"), limit)`; take parts from `event.added` **where `event.kind == "add"` only** (REPLACE outputs are rewrites of data already processed — consuming them double-counts; DELETE events are ignored: v1 MVs are append-only, documented caveat).
  2. Read those parts to one batch (a local `read_parts_to_batch` mirroring the compactor's ~30-line reader + schema-adapt; deliberate small duplication, noted, until a shared home exists).
  3. `run_pipeline_sql` → `batch_to_l0_items` → `catalog.commit(target_ht, Add, Some("pipeline:{id}:through:{last_commit_id}"))` — the **existing idempotency-key machinery** makes this exactly-once: crash after commit, before cursor-advance → replay returns `AlreadyApplied`, then the cursor advances.
  4. `set_cursor("pipeline:{name}", source_ht, last_commit_id)` — a real `worker_cursors` row, which also makes the MV a first-class GC fence (a stalled MV blocks reaping of its unread parts, by design).
  - Empty result batches still advance the cursor (commit only when there are items). `error_policy` as in Task 4 (`Stall` = don't advance; `Skip` = advance cursor past the batch, count it).
- [ ] **Step 1 (failing component test):** events hypertable + `daily_counts` target hypertable + pipeline `SELECT tenant_id, day, count(*) AS n FROM events GROUP BY tenant_id, day`; commit two ADD batches, run to quiescence, assert target contents equal the per-batch aggregates; run a compactor REPLACE on the source, run the MV again, assert **no double-count**; kill-and-rerun between commit and cursor-advance (drive `run_once` manually) asserts `AlreadyApplied` idempotency.
- [ ] **Step 2:** implement; document the partial-aggregate caveat (per-batch `GROUP BY` yields partial aggregates; the aggregating-placement compactor mode is future work) in the module doc.
- [ ] **Step 3:** verify, fmt, clippy, commit `"feat: parquet-to-parquet aggregation MV pipelines on the change feed"`.

---

### Task 6: ukield — config, bootstrap, roles, example

**Files:** `crates/ukield/src/config.rs`, `bootstrap.rs`, `run.rs`, `main.rs` (role enum if needed), `ukield.example.toml`, `crates/ukield/tests/bootstrap_test.rs`

**Interfaces:**
- `[[tables]]` keeps only hypertable concerns (schema, packing/sort/partition columns, placement/target_file_mb, namespaces) — `topic`/`ts_column` deleted.
- New `[[stream_tables]]` (`name`, `topic`, columns) and `[[pipelines]]` (`name`, `from`, `to`, `sql`, optional `error_policy`) sections; bootstrap creates both idempotently and runs `validate_pipeline_sql` (source schema from the stream table / source hypertable; `TargetContract` from the target hypertable) — a config with invalid pipeline SQL refuses to start.
- Roles: `ingest` runs one `PipelineIngest` per kafka-sourced pipeline; new role `mv` runs one `MvWorker` per parquet-sourced pipeline (default role set gains `mv`).
- `ukield.example.toml` rewritten accordingly — the events pipeline carries the pipelines-note SELECT including a `WHERE ts` predicate with a comment: this is where the deleted plan-19 bounds live now, and a migration backfill is the same pipeline with a different predicate.

- [ ] Steps: failing bootstrap test (stream table + pipeline created idempotently; invalid SQL refused with a naming error) → implement → `cargo test -p ukield` + `make test` → commit `"feat: pipeline bootstrap and mv role in ukield"`.

---

### Task 7: e2e adaptation + S9 (MV scenario)

**Files:** `crates/ukiel-e2e/src/lib.rs` (stack bootstraps stream table + pipeline instead of a route), scenario module for S9, `docs/superpowers/specs/2026-07-05-ukiel-testing-design.md` (S9 row)

- [ ] Adapt the `Stack` harness to the pipeline config shape; all existing scenarios (S0–S8) must pass unmodified in their assertions — they prove the pipeline path preserves ingest semantics end-to-end.
- [ ] Add **S9 — MV equivalence**: S1-style load; an aggregation pipeline into `daily_counts`; after quiescence (ingest + MV + compactor), the MV contents equal the oracle model's aggregation, including after a compaction pass on the source (no double-count — the `kind == "add"` rule under fire).
- [ ] `make e2e` green; add the S9 row to the testing design's scenario table; commit `"test: e2e on pipelines; S9 MV equivalence scenario"`.

---

### Task 8: Docs

- [ ] Pipelines note: status header → implemented (v1 scope), egress still follow-up.
- [ ] Design doc: Pipelines section marked implemented; "Backfills & migrations" updated (the per-table knob escape hatch is replaced by per-pipeline predicates — simplify the paragraph); MV bullet in the architecture diagram section now real.
- [ ] Monitoring spec: `ingest_out_of_bounds_total` row replaced by a note (bounds are pipeline predicates now; skipped batches surface as `pipeline_batches_skipped_total{pipeline}` — add that row); README crate list (`ukiel-pipeline`) and quickstart updated to the pipeline config.
- [ ] Roadmap row 8 → **Executed**; remove the "write after 18" note; row 23 (feed horizon) becomes writable — flip its gate note.
- [ ] Full `make test` + `make e2e` first; commit `"docs: pipelines v1 shipped"`.

---

## Self-review notes

- **Note coverage:** engines (T1), no-direct-SELECT (kafka tables are not in the query provider's namespace resolution — nothing to do, they're a different entity), kafka→parquet incl. exactly-once and knob deletion (T4), parquet→parquet MV with add-only consumption + idempotency (T5), delivery column stored for egress (T1), error policy stall/skip (T1/T4/T5), creation-time validation incl. determinism and required-columns contract (T3/T6), fan-out explicitly deferred (one pipeline per topic→table in v1 — bootstrap rejects duplicates per source topic), joins rejected naturally (single registered table; anything else fails planning).
- **Guardrail continuity:** backpressure port (T4), metrics continuity incl. freshness (T4), bounds deleted with doc trail (T4/T8), offset CAS untouched (flusher path reused verbatim).
- **Double-count proof:** the S9 + T5 tests both exercise REPLACE-during-MV; the `kind == "add"` rule is load-bearing and tested twice.
- **Type consistency:** `batch_to_l0_items` (T2) is consumed by both T4 and T5 with the same signature; `validate_pipeline_sql`'s `TargetContract` is built identically in T6 bootstrap and T3 tests; cursor/idempotency string formats (`pipeline:{name}` / `pipeline:{id}:through:{commit}`) are stated once and reused.

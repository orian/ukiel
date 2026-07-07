# Ukiel Plan 20: Metrics P1 (`ukiel-metrics-p1`) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship phase P1 of the monitoring spec (`docs/superpowers/specs/2026-07-06-ukiel-monitoring.md`): wire the `metrics` crate facade into the worker crates so the stats already produced at call sites (`Flusher::flush`, `commit_inner`, `run_query_inner`, compactor/gc `run_once`, the cache layer) become counters/histograms, then expose them from `ukield` via a Prometheus exporter on the query listener (`GET /metrics`) and add `GET /readyz`. **Only the metrics emittable at their call site are in scope** — the periodic-collector gauges that need catalog queries (live-part counts, feed lag, consumer lag, pending/tombstone backlogs, finalization age) are P2 and explicitly deferred.

**Architecture:** Library crates emit through the `metrics` facade (`counter!`/`gauge!`/`histogram!`); with no recorder installed the macros are near-free no-ops, so tests and embedders pay nothing (monitoring spec "Stack"). Only `ukield` installs a recorder — `metrics-exporter-prometheus` — exactly once per process (an idempotent `OnceLock` handle so repeated `run_with_bound_addr` calls across integration tests don't double-install the global recorder). The `/metrics` route (no `AppState` needed) is added by `ukield` when it builds the router; `/readyz` (catalog `SELECT 1` + object-store reachability) lives in `ukiel-query`'s `router()` because it needs `AppState`. Label discipline from the spec: `hypertable`/`topic`/`kind`/`format`/`class`/`role` are bounded and fine; **namespace is never a label**.

**Tech Stack:** `metrics` 0.24 facade (workspace dep), `metrics-exporter-prometheus` 0.16 (ukield only), `metrics-util` 0.19 (dev-dep, `DebuggingRecorder` for pure-helper assertions). axum 0.8, tokio, sqlx/Postgres. Pin the compatible trio together — bump all three if any needs a newer minor. Exporter operational choices are explicit: `*_duration_seconds` histograms get real Prometheus buckets (`set_buckets_for_metric`) so they aggregate across nodes — the exporter's default quantile summaries cannot be aggregated — and `run.rs` runs a periodic `PrometheusHandle::run_upkeep()` tick so histogram buffers drain between scrapes.

**Prerequisites:** Plans 1–7, 9, 10, 19 executed (current `main`). Plan 18 (backpressure) and plan 17 (LSM) are **not** landed, so their metrics (`ingest_backpressure_deferrals_total`, `compactor_backlog_groups`, `compactor_unfinalized_partitions`) are out of scope — they land with those plans. Plan 11 (scan pushdown) and 13 (metadata cache) aren't landed, so `query_parts_scanned/pruned`, `query_objectstore_requests_total`, and the metadata-cache tier of `cache_*` are deferred to those plans (noted at each site).

## Global Constraints

- Rust edition 2024, toolchain ≥ 1.96.
- Library crates depend on `metrics` only (the facade). **Never** add `metrics-exporter-prometheus` to a library crate — the binary owns the recorder.
- **Emission-test rule:** `metrics::with_local_recorder` is *synchronous* — you cannot `.await` inside it, so async emission paths are never unit-tested with local recorders. Pure sync helpers (`query_class`, bookkeeping like the max-event-ts tracker) get direct or `DebuggingRecorder` unit tests; every async emission site is proven exactly once, end-to-end, by Task 5's `/metrics` assertions. Do not write a local-recorder test around an async call — it cannot compile into something that fails meaningfully.
- Component tests via `make test` (Docker required); pure/facade unit tests pass without Docker (`cargo test -p <crate> --lib`).
- Label cardinality: allowed labels are `hypertable`, `topic`, `partition`, `kind`, `worker`, `format`, `status`, `class`, `op`, `role`, `tier`. **Never** label by `namespace_id` or raw SQL text.
- Commit messages: conventional style, no Claude/AI attribution.
- Every task ends with `cargo fmt && cargo clippy --all-targets -- -D warnings` clean.

## Metric inventory (P1 scope)

Emitted at call sites this plan touches. Deferred rows list the plan that lands them.

| Metric | Type | Labels | Site |
|---|---|---|---|
| `ingest_flush_duration_seconds` | histogram | — | `Flusher::flush` |
| `ingest_flush_rows` / `ingest_flush_bytes` | histogram | — | `Flusher::flush` |
| `ingest_flush_partitions` | histogram | — | `Flusher::flush` |
| `ingest_rows_total` | counter | `hypertable` | `Flusher::flush` (committed rows) |
| `ingest_poison_messages_total` | counter | `topic` | `buffer_message` skip sites |
| `ingest_out_of_bounds_total` | counter | `hypertable` | `buffer_message` bounds skip (0004) |
| `ingest_commit_failures_total` | counter | — | `Flusher::flush` error path |
| `ingest_last_flush_event_ts_seconds` | gauge | `hypertable` | consumer flush — max event ts of the flushed rows, unix secs. **Pulled into P1**: the consumer already holds every row's `ts` (no catalog query needed), and FreshnessBreach is the spec's top alert. Timestamp form on purpose: `time() − gauge` keeps aging when ingest hangs; a computed freshness gauge freezes at a healthy value in exactly that failure |
| `catalog_commit_duration_seconds` | histogram | `kind` | `commit_inner` |
| `query_requests_total` | counter | `format`,`status` | `run_query` |
| `query_duration_seconds` | histogram | `class` | `run_query_inner` |
| `query_rows_returned` | histogram | — | `run_query_inner` |
| `query_timeouts_total` | counter | — | timeout branch |
| `query_sessions_built_total` | counter | — | `run_query_inner` |
| `cache_hits_total` / `cache_misses_total` | counter | `tier="file"` | `CachingObjectStore::ensure_cached` |
| `compactor_merges_total` | counter | — | `run_once` (from `CompactionStats`) |
| `compactor_parts_in_total` / `compactor_parts_out_total` | counter | — | `run_once` |
| `compactor_conflicts_total` | counter | — | `run_once` |
| `compactor_pass_duration_seconds` | histogram | — | `run_once` |
| `compactor_last_success_timestamp` | gauge | — | end of successful `run_once` |
| `gc_reaped_parts_total` / `gc_swept_orphans_total` | counter | — | `run_once` (from `GcStats`) |
| `gc_reconcile_orphans_total` | counter | — | `reconcile_once` |
| `ukield_worker_up` | gauge | `role` | task spawn/exit |
| **Deferred** | | | |
| `catalog_commit_conflicts_total{worker}` | counter | — | needs worker label threaded through `commit`; worker-side conflict counts already covered by `compactor_conflicts_total`. P2. |
| `query_parts_scanned/pruned`, `query_objectstore_requests_total{op}` | — | — | plan 11 (scan pushdown / counting store) |
| metadata-cache tier of `cache_*` | — | — | plan 13 |
| `ingest_backpressure_deferrals_total` | — | — | plan 18 |
| `compactor_backlog_groups`, `*_unfinalized_*` | — | — | plan 17 |
| periodic gauges (`*_lag`, `*live_parts`, `pending_objects`, `pool_connections`, cache-dir disk) | — | — | **P2** collector (freshness is NOT here — see `ingest_last_flush_event_ts_seconds` above) |
| standalone `/metrics` listener for processes without the query role | — | — | **P2** — P1 accepts and documents the limitation (Task 6): the recorder installs but nothing serves it |
| `ukield_worker_restarts_total` | — | — | needs a restart supervisor (today first failure cancels all); P2 |

---

### Task 1: Workspace deps + ingest emission

**Files:**
- Modify: root `Cargo.toml` (`[workspace.dependencies]`)
- Modify: `crates/ukiel-ingest/Cargo.toml` (add `metrics`; `metrics-util` dev-dep)
- Modify: `crates/ukiel-ingest/src/flusher.rs`, `crates/ukiel-ingest/src/consumer.rs`
- Test: `crates/ukiel-ingest/src/consumer.rs` (`mod tests`)

**Interfaces:**
- Produces: the eight `ingest_*` metrics from the inventory. `Flusher::flush` times the whole encode+upload+commit span and records rows/bytes/partitions + `ingest_rows_total{hypertable}` on success, `ingest_commit_failures_total` on error. `buffer_message` increments `ingest_poison_messages_total{topic}` at every skip that isn't a bounds skip, and `ingest_out_of_bounds_total{hypertable}` at the 0004 bounds skip. `Buffers` gains `max_event_ts_ms: Option<i64>` (updated as rows are accepted); after a successful flush that contained rows, the consumer emits `ingest_last_flush_event_ts_seconds{hypertable}` and resets the tracker — offset-only flushes leave the gauge untouched.

- [ ] **Step 1: Add deps.** In `[workspace.dependencies]`: `metrics = "0.24"` and `metrics-util = "0.19"`. In `crates/ukiel-ingest/Cargo.toml` add `metrics = { workspace = true }` to `[dependencies]` and `metrics-util = { workspace = true }` to `[dev-dependencies]`.

- [ ] **Step 2: Tests (per the emission-test rule).** No local-recorder test around `buffer_message`/`flush` — they are async-adjacent `&self` paths; their emission proof is Task 5's `/metrics` assertions. What IS unit-tested here: the `max_event_ts_ms` bookkeeping as a plain sync test in `consumer.rs` `mod tests` — feed the tracker three timestamps (out of order), assert it holds the max; assert the reset-on-flush contract by driving the field directly.

- [ ] **Step 3: Instrument `Flusher::flush`.** Wrap the body: capture `std::time::Instant::now()`, count `rows`/`bytes`/`partitions` from `items` before the move, and on the `Ok` path emit `histogram!("ingest_flush_duration_seconds").record(elapsed.as_secs_f64())`, `histogram!("ingest_flush_rows").record(rows as f64)`, `histogram!("ingest_flush_bytes").record(bytes as f64)`, `histogram!("ingest_flush_partitions").record(partitions as f64)`, `counter!("ingest_rows_total", "hypertable" => hypertable.name.clone()).increment(rows)`. On the `Err` path emit `counter!("ingest_commit_failures_total").increment(1)`. An empty (offset-only) flush records a zero-row flush — fine.

- [ ] **Step 4: Instrument `buffer_message`.** At the unparseable-payload / missing-ts / missing-packing-key `return`s: `counter!("ingest_poison_messages_total", "topic" => self.route.topic.clone()).increment(1)`. At the 0004 bounds-skip `return`: `counter!("ingest_out_of_bounds_total", "hypertable" => self.route.hypertable.clone()).increment(1)` (route carries the hypertable name). Keep the existing `tracing::warn!`.

- [ ] **Step 4b: Freshness gauge.** In `buffer_message`, after a row is accepted into `rows_by_day`: `buffers.max_event_ts_ms = Some(buffers.max_event_ts_ms.map_or(ts, |m| m.max(ts)))`. In `RouteIngest::flush`, after `flusher.flush` succeeds and if the flush contained at least one row: `gauge!("ingest_last_flush_event_ts_seconds", "hypertable" => self.route.hypertable.clone()).set(max_ts_ms as f64 / 1000.0)`, then reset the tracker to `None`. Timestamp form on purpose (see inventory): the alert computes `time() − gauge`, which keeps growing when ingest hangs.

- [ ] **Step 5: Verify, lint, commit.** `cargo test -p ukiel-ingest -- --test-threads=2`. Then fmt+clippy.
  `git commit -m "feat: metrics facade + ingest flush/poison/out-of-bounds metrics (metrics P1)"`

---

### Task 2: Catalog commit histogram

**Files:** `crates/ukiel-catalog/Cargo.toml`, `crates/ukiel-catalog/src/commit.rs`, test in `mod`/`tests`.

**Interfaces:** `commit_inner` records `catalog_commit_duration_seconds{kind}` for the whole transaction (both success and rollback paths — a slow conflict still costs latency). `kind` = `op.kind()` (the existing `&'static str`).

- [ ] **Step 1:** Add `metrics = { workspace = true }` (dep) and `metrics-util` (dev-dep) to `crates/ukiel-catalog/Cargo.toml`.
- [ ] **Step 2 (test):** Per the emission-test rule: none here — the commit is async, so a local-recorder test cannot wrap it. The emission proof is Task 5's `/metrics` assertion, which requires the `catalog_commit_duration_seconds` family to appear after a produce+query round trip.
- [ ] **Step 3:** In `commit_inner`, capture `Instant::now()` at entry; compute `kind = op.kind()` before `op` is moved into the `match`; on both the early `Conflict`/`OffsetRace` returns and the success return, `histogram!("catalog_commit_duration_seconds", "kind" => kind).record(started.elapsed().as_secs_f64())`. Use a small guard closure or record at each exit — prefer an explicit `record` before each `return`/`Ok` (no Drop guard, to keep it obvious).
- [ ] **Step 4:** Verify, fmt, clippy, commit `"feat: catalog commit-duration histogram (metrics P1)"`.

---

### Task 3: Query metrics + cache counters

**Files:** `crates/ukiel-query/Cargo.toml`, `crates/ukiel-query/src/server.rs`, `crates/ukiel-query/src/cache.rs`, test in `server.rs` `mod tests` (pure `query_class`).

**Interfaces:** `run_query`/`run_query_inner` emit `query_requests_total{format,status}`, `query_duration_seconds{class}`, `query_rows_returned`, `query_timeouts_total`, `query_sessions_built_total`. `query_class(sql) -> &'static str` is a pure coarse classifier (`introspection` if it references `information_schema`, `aggregate` if it contains a `group by` / known aggregate, else `select`). `CachingObjectStore::ensure_cached` emits `cache_hits_total{tier="file"}` / `cache_misses_total{tier="file"}`.

- [ ] **Step 1:** Add `metrics` dep to `crates/ukiel-query/Cargo.toml`; `metrics-util` dev-dep.
- [ ] **Step 2 (failing test):** In `server.rs` `mod tests`, assert `query_class` classification:
  ```rust
  assert_eq!(query_class("SELECT * FROM events"), "select");
  assert_eq!(query_class("select count(*) from events group by tenant_id"), "aggregate");
  assert_eq!(query_class("SELECT table_name FROM information_schema.tables"), "introspection");
  ```
  Fails to compile (fn missing).
- [ ] **Step 3:** Add `pub(crate) fn query_class(sql: &str) -> &'static str` (lowercase-scan; `information_schema` → introspection; `" group by "` or an aggregate token → aggregate; else select). In `run_query_inner`: capture `Instant`; after `session_for_namespace` succeeds, `counter!("query_sessions_built_total").increment(1)`; on the timeout branch `counter!("query_timeouts_total").increment(1)`; on success record `query_duration_seconds{class}` and `query_rows_returned` (batch row sum). In `run_query` (the outer fn that maps to a `Response`), record `query_requests_total{format,status}` — `format` from the resolved `ResultFormat` (or `"unknown"` when it errored before formatting), `status` = `"ok"`/`"error"` from the `Result`. Keep namespace out of all labels.
- [ ] **Step 4 (cache):** In `ensure_cached`, emit `counter!("cache_hits_total", "tier" => "file").increment(1)` on the `try_exists` hot path and `counter!("cache_misses_total", "tier" => "file").increment(1)` before the download. (Metadata-cache tier lands with plan 13.)
- [ ] **Step 5:** Verify `cargo test -p ukiel-query -- --test-threads=2`, fmt, clippy, commit `"feat: query request/duration/rows metrics + file-cache counters (metrics P1)"`.

---

### Task 4: Compactor & GC metrics

**Files:** `crates/ukiel-compactor/Cargo.toml`, `crates/ukiel-compactor/src/compactor.rs`, `crates/ukiel-gc/Cargo.toml`, `crates/ukiel-gc/src/lib.rs`.

**Interfaces:** `Compactor::run_once` records `compactor_pass_duration_seconds`, adds `CompactionStats` deltas to `compactor_merges_total`/`compactor_parts_in_total`/`compactor_parts_out_total`/`compactor_conflicts_total`, and sets `compactor_last_success_timestamp` (unix secs) at a clean pass end. `Gc::run_once` adds `GcStats.reaped_parts`/`swept_orphans` to their counters; `reconcile_once` adds `reconciled_orphans` to `gc_reconcile_orphans_total`.

- [ ] **Step 1:** Add `metrics` dep to both crates' `Cargo.toml` (+ `metrics-util` dev-dep where a facade test is added).
- [ ] **Step 2:** Compactor `run_once`: time the pass; after the per-hypertable loop, `counter!("compactor_merges_total").increment(stats.merged_groups as u64)` etc. for `parts_in`/`parts_out`/`conflicts`; `histogram!("compactor_pass_duration_seconds").record(elapsed)`; on `Ok` set `gauge!("compactor_last_success_timestamp").set(unix_secs)` (compute via `std::time::SystemTime::now().duration_since(UNIX_EPOCH)`; the `Date`-ban is a workflow-script constraint, not a runtime one — `SystemTime` is fine in crate code). Emit from `run_once` (not `run`) so a single manual pass is also measured.
- [ ] **Step 3:** GC `run_once`: after the hypertable loop, increment `gc_reaped_parts_total`/`gc_swept_orphans_total` by the stats deltas. In `reconcile_once`, increment `gc_reconcile_orphans_total` by `stats.reconciled_orphans`. (Keep the existing `tracing::info!`.)
- [ ] **Step 4:** Verify `cargo test -p ukiel-compactor -p ukiel-gc -- --test-threads=2`, fmt, clippy, commit `"feat: compactor and gc counters/histograms (metrics P1)"`.

---

### Task 5: ukield — Prometheus exporter, `/metrics`, `/readyz`, worker_up

**Files:**
- Modify: `crates/ukield/Cargo.toml` (add `metrics`, `metrics-exporter-prometheus = "0.16"`)
- Create: `crates/ukield/src/metrics.rs` (idempotent recorder handle)
- Modify: `crates/ukiel-query/src/server.rs` (add `/readyz` to `router`; add a catalog ping)
- Modify: `crates/ukiel-catalog/src/...` (add `PostgresCatalog::ping()` → `SELECT 1`)
- Modify: `crates/ukield/src/run.rs` (install recorder, add `/metrics` route, wrap each spawned task with `ukield_worker_up{role}`)
- Test: `crates/ukield/tests/server_test.rs`

**Interfaces:**
- `ukield::metrics::handle() -> PrometheusHandle` — installs the exporter on first call via a `OnceLock`, returns the stored handle thereafter (test-safe across repeated `run_with_bound_addr`).
- `GET /metrics` (added by ukield to the query router) → `handle().render()` as `text/plain`.
- `GET /readyz` (in `ukiel-query::router`) → 200 when catalog `SELECT 1` succeeds and object-store is reachable (`store.head(sentinel)` returns `Ok` or `NotFound`); 503 otherwise, JSON body.
- `PostgresCatalog::ping(&self) -> Result<(), CatalogError>` running `SELECT 1`.
- Each worker task sets `gauge!("ukield_worker_up", "role" => role).set(1.0)` on spawn and `.set(0.0)` on exit.

- [ ] **Step 1 (failing test):** In `crates/ukield/tests/server_test.rs`, extend the full-stack test (or add one) to `GET /metrics` and assert 200 + body contains `ingest_flush_duration_seconds`, `ingest_last_flush_event_ts_seconds`, `catalog_commit_duration_seconds`, and `query_requests_total` after a produce+query — this single assertion is the emission proof for every async site in Tasks 1–4 (the emission-test rule). Also assert `ingest_flush_duration_seconds` renders with `_bucket` lines (buckets, not summary quantiles). `GET /readyz` → 200. Fails (routes 404).
- [ ] **Step 2:** `crates/ukield/src/metrics.rs`:
  ```rust
  use std::sync::OnceLock;
  use metrics_exporter_prometheus::{Matcher, PrometheusBuilder, PrometheusHandle};

  static HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();

  /// Installs the Prometheus recorder once per process; returns the render
  /// handle. Idempotent so repeated `run_with_bound_addr` (tests) is safe.
  /// Duration histograms get explicit buckets: the exporter's default
  /// quantile summaries cannot be aggregated across query nodes.
  pub fn handle() -> PrometheusHandle {
      HANDLE
          .get_or_init(|| {
              PrometheusBuilder::new()
                  .set_buckets_for_metric(
                      Matcher::Suffix("_duration_seconds".to_string()),
                      &[
                          0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25,
                          0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0,
                      ],
                  )
                  .expect("duration buckets")
                  .install_recorder()
                  .expect("install prometheus recorder")
          })
          .clone()
  }
  ```
  Add `pub mod metrics;` to `crates/ukield/src/lib.rs`.
- [ ] **Step 3:** `PostgresCatalog::ping`: `sqlx::query("SELECT 1").execute(&self.pool).await?; Ok(())`.
- [ ] **Step 4:** `ukiel-query::router` gains `.route("/readyz", get(readyz))`; `readyz(State(state))` runs `state.catalog.ping()` and `state.store.head(&Path::from("__ukiel_readyz_sentinel"))` treating `Ok`/`NotFound` as ready, returns `(StatusCode::OK, "ready")` or `(StatusCode::SERVICE_UNAVAILABLE, Json{error})`.
- [ ] **Step 5:** `run.rs`: call `let prom = crate::metrics::handle();` early (installs recorder before workers emit), then spawn the upkeep tick — a 5s `tokio::time::interval` loop calling `prom.run_upkeep()` (drains histogram buffers between scrapes; without it they grow unboundedly), cancelled by the shutdown token like the workers. Build the served router as `crate::metrics::route(router(state), prom)` where a small helper adds `.route("/metrics", get(move || { let h = prom.clone(); async move { h.render() } }))`. Wrap each `tasks.spawn(...)` body to set `ukield_worker_up{role}` to 1 at start and 0 before returning (a scope-guard or explicit set on all exit paths). **Role-split limitation, accepted for P1:** when the query role is absent there is no listener — the recorder still installs and workers still emit, but nothing serves `/metrics`; documented in Task 6, standalone listener rowed for P2.
- [ ] **Step 6:** Verify `cargo test -p ukiel-catalog -p ukiel-query -p ukield -- --test-threads=2`, fmt, clippy, commit `"feat: ukield prometheus /metrics, /readyz, worker_up gauge (metrics P1)"`.

---

### Task 6: Full verification + docs

**Files:** `README.md`, `docs/superpowers/specs/2026-07-06-ukiel-monitoring.md`, `docs/superpowers/plans/2026-07-05-ukiel-v1-roadmap.md`, `ukield.example.toml` (note `/metrics` + `/readyz` on the query listener).

- [ ] **Step 1:** `cargo fmt --check && cargo clippy --all-targets -- -D warnings && make test`.
- [ ] **Step 2:** Monitoring spec: mark P1 done in "Implementation phases"; annotate which inventory rows shipped vs. remain P2. README `## Development`: note `ukield` exposes `GET /metrics` (Prometheus) and `GET /readyz` on the query listener — **and state the P1 limitation plainly: a process running without the query role exposes no `/metrics`** (standalone metrics listener is P2). Add the same one-liner to the monitoring spec's Stack section if plan-20 review hasn't already placed it there.
- [ ] **Step 3:** Roadmap row 20 → **Executed** (one line on what shipped); drop 20 from the remaining-order line (`11 → 17 → 18 → …`). Update the "executed so far" list.
- [ ] **Step 4:** Commit `"docs: metrics P1 shipped — /metrics, /readyz, worker-crate counters; roadmap row 20 executed"`.

---

## Self-review notes

- **Scope guard:** every metric in the spec that needs a *query* to compute (lag, live-part counts, backlog, pool, cache-dir disk) is P2 and appears in the deferred table with its owning phase/plan — P1 is strictly call-site emission + exporter + readyz, independent of plans 11/13/17/18. **Freshness is the deliberate exception**: it looked like P2 but the consumer already holds every row's `ts`, so it ships in P1 as the timestamp gauge `ingest_last_flush_event_ts_seconds` — FreshnessBreach is the spec's top alert and must not stay dark until an unscheduled P2, and the timestamp form keeps aging when ingest hangs (a computed freshness gauge freezes healthy in exactly that failure).
- **No test pays for metrics:** library crates depend only on the `metrics` facade (no recorder). `with_local_recorder` is sync-only, so async emission is proven exactly once by Task 5's `/metrics` end-to-end assertions; unit tests cover only pure sync helpers. The process-wide exporter is installed only by `ukield`, guarded by a `OnceLock` so the ukield integration binary can call `run_with_bound_addr` in multiple tests without a double-install panic.
- **Exporter ops:** `*_duration_seconds` families use explicit buckets (summaries can't be aggregated across the stateless query nodes) and `run_upkeep` ticks every 5s (histogram draining). A process without the query role exposes no `/metrics` in P1 — documented in Task 6, standalone listener rowed for P2.
- **Label discipline:** no `namespace_id` label anywhere; `format`/`status`/`class`/`kind`/`role`/`topic`/`hypertable` are all bounded. `query_class` is a pure fn with a direct unit test.
- **Conflict counting:** `catalog_commit_conflicts_total{worker}` is deferred (worker identity isn't visible in `commit_inner`); the operational signal is preserved by `compactor_conflicts_total` at the worker call site.
- **Liveness gauge honesty:** `ukield_worker_up` is implemented; `ukield_worker_restarts_total` is deferred because there is no restart supervisor yet (first failure cancels the JoinSet) — emitting a counter that can only ever read 0 would be misleading.

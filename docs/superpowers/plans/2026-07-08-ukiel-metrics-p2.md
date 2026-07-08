# Ukiel Plan 21: Metrics P2 (Periodic Collector, Standalone Listener) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Monitoring phase P2 per the monitoring spec (`docs/superpowers/specs/2026-07-06-ukiel-monitoring.md`) and the sketch (`docs/notes/2026-07-07-metrics-p2.md`): (a) the **periodic collector** in `ukield` — the gauge families that need queries: consumer lag (Kafka high-watermarks vs catalog offsets), feed lag per `{worker, hypertable}`, `catalog_live_parts{hypertable,level}`, compactor backlog/unfinalized gauges (the plan-17/18 regimes are currently dark — the design doc's "alarms standing in for hard limits"), GC backlogs, pool saturation, cache-dir disk (we have lived one disk-full incident; the cache dir has no eviction); (b) the **standalone `/metrics` listener** closing the P1 role-split gap (a process without the query role emits metrics nothing serves); (c) the two **P1-deferred call-site metrics plans 17/18 unblocked** — `ingest_backpressure_deferrals_total{hypertable}` and `ingest_flush_partitions` (verified missing: plan 18 shipped only the `tracing::warn`s; the IngestBackpressured alert has no signal today). Issue 0008's revisit trigger becomes real data.

**Architecture:** One collector task in `ukield` (`src/collector.rs`), spawned in `run.rs` like the metrics-upkeep task — role-independent, `CancellationToken`-cancelled, ticking every `collector.interval_ms` (default 30s). Each gauge family is one indexed catalog query or API call behind its own `async fn`; a failing family logs, increments `collector_errors_total{family}`, and **never** crashes the process — a monitoring outage must not become a data-plane outage (the Kafka family in a query-only process fails every tick by design and must be quiet about it). New catalog probes are aggregate SQL over existing indexes (audited per-family below); none ships row sets to the worker. Label discipline unchanged: `hypertable`/`worker`/`topic,partition`/`level` are bounded labels; **namespace never is**.

**Tech Stack:** Rust edition 2024; existing pins (rdkafka 0.39 for watermarks, metrics/metrics-exporter-prometheus already in ukield). One new direct dep in `ukield`: `libc` (statvfs for cache-dir free space; already in every dependency tree).

**Prerequisites:** Rows 20 (recorder, `/metrics`, upkeep — the `OnceLock` handle in `ukield::metrics` is reused as-is), 17/18 (the probes and regimes being instrumented), 28 (`finalize_candidates`-shape SQL + migration-0007 index the new aggregates lean on): all **Executed**. Ungated otherwise; recommended first in the remaining order (before 30, whose bench runs these gauges then instrument).

**Deviations from the sketch (deliberate):** (1) the standalone listener binds only when `metrics_listen` is explicitly set — the sketch's "or when the query role is absent" would auto-open a port nobody configured; instead, a query-role-less process without `metrics_listen` logs a startup warning. (2) `gc_reap_fence_age_seconds` is emitted as the age of the oldest *unreaped tombstone* (deleting-commit `created_at` of reapable-but-unpurged parts) — a fence-stall shows here identically and it is one indexed scalar instead of re-running the reaper's fence logic.

## Global Constraints

- Rust edition 2024, toolchain ≥ 1.96; workspace dep pins per the roadmap.
- **Every existing test stays green after every task.** The collector is additive; default config changes nothing observable except new gauge families on `/metrics`.
- Every collector SQL family must be served by an existing index or state explicitly what bounds its scan (per the sketch: no gauge may become a seq scan at 10⁶ parts).
- Unit tests without Docker where logic is pure; catalog probes tested in `catalog_test.rs` (testcontainers); one ukield integration test proves end-to-end scrape.
- Commit messages: conventional style, no Claude/AI attribution.
- Every task ends with `cargo fmt && cargo clippy --all-targets -- -D warnings` clean.

---

### Task 1: P1-deferred call-site metrics in ingest

**Files:**
- Modify: `crates/ukiel-ingest/src/consumer.rs`

**Interfaces:**
- Produces (both listed in the monitoring spec's ingest table, deferred from P1 as "plan 18"):
  - `ingest_backpressure_deferrals_total{hypertable}` counter — incremented in `RouteIngest::flush`'s `Defer` branch, next to the existing `tracing::warn!`. Feeds the IngestBackpressured alert and issue 0008's revisit trigger.
  - `ingest_flush_partitions` histogram — recorded once per non-empty flush (`buffers.rows_by_day.len()`), before draining; the spread-warning's metric twin.
- Not included: `catalog_commit_conflicts_total{worker}` stays deferred — `PostgresCatalog::commit` has no worker identity in its signature; adding one is an API change across every writer, wrong scope for this plan (note it in the monitoring spec's deferred list, still).
- Emission call sites follow the P1 convention (no unit test for emission — the Task 5 integration test asserts presence in the rendered scrape; `consumer.rs` already imports `metrics::{counter, gauge}`, add `histogram`).

- [ ] **Step 1: Implement** (two call sites, ~6 lines).
- [ ] **Step 2: Verify** — `cargo test -p ukiel-ingest --lib && cargo test -p ukiel-ingest -- --test-threads=2` (all green; emission without a recorder is a no-op, so nothing observable changes in tests).
- [ ] **Step 3: Lint and commit**

```bash
cargo fmt && cargo clippy -p ukiel-ingest --all-targets -- -D warnings
git add crates/ukiel-ingest
git commit -m "feat: backpressure deferral counter and flush-partitions histogram (metrics P2)"
```

---

### Task 2: Catalog collector probes

**Files:**
- Modify: `crates/ukiel-catalog/src/parts.rs`, `src/cursors.rs`, `src/gc.rs`, `src/lib.rs`
- Test: `crates/ukiel-catalog/tests/catalog_test.rs`

**Interfaces (all on `PostgresCatalog`; index audit inline):**

```rust
/// Live part counts per (hypertable, level) — `catalog_live_parts`.
/// Scan is bounded by live parts, i.e. exactly the quantity being measured.
pub async fn live_part_counts(&self) -> Result<Vec<(HypertableId, i16, i64)>, CatalogError>;
// SELECT hypertable_id, level, count(*) FROM parts
// WHERE deleted_by_commit IS NULL GROUP BY 1, 2

/// Feed lag per (worker, hypertable): head commit id minus cursor.
/// commits_feed_idx (hypertable_id, id) serves the per-hypertable max.
pub async fn feed_lags(&self) -> Result<Vec<(String, HypertableId, i64)>, CatalogError>;
// SELECT c.worker_name, c.hypertable_id,
//        greatest(coalesce(m.head, 0) - c.last_commit_id, 0)
// FROM worker_cursors c
// LEFT JOIN LATERAL (SELECT max(id) AS head FROM commits
//                    WHERE hypertable_id = c.hypertable_id) m ON true
// (adapt column names to worker_cursors' actual schema, migration 0003)

/// Partition×level groups at/over their merge trigger — `compactor_backlog_groups`.
/// Migration-0007 index (hypertable_id, partition_values, level) serves the GROUP BY.
pub async fn backlog_groups(&self, l0_fanout: i64, fanout: i64) -> Result<i64, CatalogError>;
// SELECT count(*) FROM (
//   SELECT level, count(DISTINCT created_by_commit) AS runs FROM parts
//   WHERE deleted_by_commit IS NULL
//   GROUP BY hypertable_id, partition_values, level) g
// WHERE runs >= CASE WHEN level = 0 THEN $1 ELSE $2 END

/// Partitions past finalize_after_secs still holding >1 live run, plus the
/// oldest such partition's quiet age — the plan-17 terminal invariant as a
/// production signal (`compactor_unfinalized_partitions`, `_oldest_..._age`).
/// Same join shape as partition_l0_quiet_since, aggregated; 0007 index.
pub async fn unfinalized_partitions(&self, quiet_secs: f64)
    -> Result<(i64, f64), CatalogError>;

/// GC backlog scalars in one call (pending_objects_sweep_idx +
/// parts_reapable_idx): pending count, oldest pending age, unpurged
/// tombstone count, oldest unreaped tombstone age (fence-stall proxy).
pub struct GcBacklogs { pub pending: i64, pub pending_oldest_secs: f64,
                        pub unpurged_tombstones: i64, pub oldest_tombstone_secs: f64 }
pub async fn gc_backlogs(&self) -> Result<GcBacklogs, CatalogError>;

/// sqlx pool saturation, read directly — `catalog_pool_connections{state}`.
pub fn pool_stats(&self) -> (u32, usize); // (size, num_idle)
```

- [ ] **Step 1: Write the failing tests** — one test seeding two hypertables with live/tombstoned parts across levels + cursors + pending objects, asserting each probe's numbers (reuse the file's fixture and the plan-18 capacity-probe test's seeding helpers).
- [ ] **Step 2: Verify they fail** — `cargo test -p ukiel-catalog --test catalog_test collector_probes` (compile error).
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Verify** — `cargo test -p ukiel-catalog -- --test-threads=2`.
- [ ] **Step 5: Lint and commit**

```bash
git commit -m "feat: catalog collector probes - live counts, feed lag, backlogs, pool stats (metrics P2)"
```

---

### Task 3: The collector task in ukield

**Files:**
- Create: `crates/ukield/src/collector.rs`
- Modify: `crates/ukield/src/lib.rs` (module), `src/run.rs` (spawn), `src/config.rs` (`[collector]` section), `crates/ukield/Cargo.toml` (`libc`, `rdkafka`)

**Interfaces:**
- Config: `CollectorSection { interval_ms: u64 /* 30_000 */, cache_walk_every: u32 /* 10 */ }`, `#[serde(default)]` like the other sections.
- `collector::spawn(tasks, cfg, catalog, shutdown)` — one task in the existing `JoinSet`, spawned unconditionally in `run.rs` next to the metrics-upkeep task (role-independent, like it). Each tick runs the families; per family: `Err` → `tracing::warn!` + `counter!("collector_errors_total", "family" => …)`, continue. Never returns `Err` except on cancellation.
- Families → gauges (names from the monitoring spec):
  - **catalog**: `live_part_counts` → `catalog_live_parts{hypertable, level}` (id → name via `list_hypertables`, cached per tick); `feed_lags` → `catalog_feed_lag{worker, hypertable}`; `pool_stats` → `catalog_pool_connections{state="size"|"idle"}`.
  - **compactor regime**: `backlog_groups(cfg.compactor.l0_fanout, cfg.compactor.fanout)` → `compactor_backlog_groups`; `unfinalized_partitions(cfg.compactor.finalize_after_secs)` → `compactor_unfinalized_partitions` + `compactor_oldest_unfinalized_age_seconds`. (Gauges reflect *this process's configured* triggers — document in the section comment; a compactor-role-less process still reports them, same config source.)
  - **gc**: `gc_backlogs` → `catalog_pending_objects`, `gc_pending_object_age_seconds`, `catalog_unpurged_tombstones`, `gc_reap_fence_age_seconds`.
  - **consumer lag**: lazily-built rdkafka `BaseConsumer` from `cfg.ingest.brokers`; per configured route: `fetch_metadata(topic)` for partitions, `fetch_watermarks(topic, p, timeout)`, minus `ingest_offsets(ht, topic)` → `ingest_consumer_lag{topic, partition}`. Skipped entirely (one startup-level `info!`, no per-tick warn) when no `[[tables]]` declares a topic; broker errors hit the family error counter but log at `debug!` after the first occurrence — a query-only node must not spam.
  - **cache dir** (only when `cfg.query.cache_dir` is non-empty): statvfs (`libc`, one small `unsafe` wrapper, unit-tested against `/tmp`) → `ukield_cache_dir_free_ratio` every tick; bounded directory walk → `ukield_cache_dir_bytes` every `cache_walk_every`-th tick (the walk is the expensive one — sketch rule).
- Gauge families are set-only (no unset of vanished label sets); cardinality is bounded (hypertables ~thousands, partitions never labeled), so stale zeros are acceptable — noted in the module doc.

- [ ] **Step 1: Write the pure-logic unit tests** — statvfs wrapper returns sane ratio for an existing dir and errors for a missing one; family-dispatch error isolation (a closure-injected failing family increments the error counter and doesn't stop the others — structure families as a `Vec` of named closures to make this testable).
- [ ] **Step 2: Verify they fail**, implement, verify — `cargo test -p ukield --lib`.
- [ ] **Step 3: Lint and commit**

```bash
git commit -m "feat: periodic collector - lag, backlog, pool, and disk gauges in ukield (metrics P2)"
```

---

### Task 4: Standalone metrics listener

**Files:**
- Modify: `crates/ukield/src/config.rs` (top-level `metrics_listen: String`, default `""`), `src/run.rs`, `src/metrics.rs`

**Interfaces:**
- When `metrics_listen` is set (e.g. `"127.0.0.1:9187"`): bind a minimal axum router — `GET /metrics` (reusing `metrics::route` on `Router::new()`) + `GET /healthz` (`"ok"`) — as another JoinSet task with graceful shutdown, independent of roles. The P1 `OnceLock` handle is reused; the query listener's `/metrics` is unchanged (both can serve simultaneously — same handle, harmless).
- When unset **and** the query role is absent: one startup `tracing::warn!("metrics have no listener; set metrics_listen")` — the P1 gap becomes loud instead of silent.

- [ ] **Step 1: Implement.**
- [ ] **Step 2: Verify** — `cargo test -p ukield --lib`; config-default test extends the existing config tests (empty = disabled).
- [ ] **Step 3: Lint and commit**

```bash
git commit -m "feat: standalone /metrics listener for query-role-less processes (metrics P2)"
```

---

### Task 5: End-to-end scrape test, example config

**Files:**
- Create or extend: `crates/ukield/tests/` (follow `bootstrap_test.rs`'s testcontainers-Postgres pattern; ukield dev-deps already include testcontainers + reqwest)
- Modify: `ukield.example.toml`

**Interfaces:**
- Integration test: `run_with_bound_addr` with memory object store, roles `["query", "compactor", "gc"]`, `collector.interval_ms: 100`, no Kafka tables (consumer-lag family silent by design); seed one hypertable + parts through the catalog; after ≥2 ticks scrape `GET {addr}/metrics` and assert the scrape contains `catalog_live_parts`, `catalog_feed_lag` or its absence-with-no-cursor, `compactor_backlog_groups`, `catalog_pending_objects`, `catalog_pool_connections`, and — from Task 1, by running one flush-deferral-free ingest-less path — at minimum the families' *names* render (Prometheus renders touched families only; assert on the ones the seeded state guarantees, list them explicitly in the test).
- `ukield.example.toml`: `[collector]` block (interval comment), `metrics_listen` commented-out line with the query-role-less explanation.

- [ ] **Step 1: Write the test (failing only until Tasks 2–4 merge — this task lands last).**
- [ ] **Step 2: Verify** — `cargo test -p ukield -- --test-threads=2`; then full: `cargo fmt --check && cargo clippy --all-targets -- -D warnings && make test`; `make e2e` (additive — must stay green).
- [ ] **Step 3: Lint and commit**

```bash
git add crates/ukield ukield.example.toml
git commit -m "test: collector end-to-end scrape; example config for metrics P2"
```

---

### Task 6: Docs & status flips

**Files:**
- Modify: `docs/superpowers/specs/2026-07-06-ukiel-monitoring.md`
- Modify: `docs/superpowers/plans/2026-07-05-ukiel-v1-roadmap.md` (row 21 → Executed)
- Modify: `docs/superpowers/specs/2026-07-05-ukiel-design.md` ("What already works" Operations bullet)
- Modify: `docs/issues/0008-flat-slowdown-band.md` (revisit trigger now has its data source; status stays Accepted)
- Modify: `docs/notes/2026-07-07-metrics-p2.md` (mark executed, point at the plan)

- [ ] **Step 1: Flip** — monitoring spec: P2 phase → DONE with what shipped, deferred list updated (`conflicts{worker}` still deferred with the API-change reason; `feed_horizon_margin_seconds` noted as arriving with row 23); roadmap row 21 → **Executed**; design-doc bullet; issue 0008 + sketch-note pointers.
- [ ] **Step 2: Commit**

```bash
git add docs/
git commit -m "docs: mark metrics P2 executed - collector gauges, standalone listener"
```

---

## Self-review notes

- **Coverage vs sketch/spec:** all six sketch families land (consumer lag, feed lag, live parts + the plan-17/18 backlog/unfinalized additions, GC backlogs, pool, cache dir) → Task 3; standalone listener → Task 4; P1-deferred ingest call-sites → Task 1 (verified missing by grep — plan 18 shipped warns only). Deliberately still deferred: `catalog_commit_conflicts_total{worker}` (commit API has no worker identity — an API change across all writers, tracked in the spec's deferred list), `feed_horizon_margin_seconds` (row 23), `ukield_worker_restarts_total` (needs a restart supervisor; workers currently fail the process by design).
- **Type consistency:** `feed_lags` shape matches `worker_cursors` (migration 0003: `worker_name`-keyed, `last_commit_id BIGINT`); `gc_backlogs` fields map 1:1 to the four spec gauges; `pool_stats` uses sqlx `Pool::{size, num_idle}` (sync, no query); collector reads compactor triggers from `cfg.compactor` (present regardless of role — `#[serde(default)]` sections); `metrics::route` is reused verbatim for the standalone listener.
- **Index audit (sketch rule):** live counts — bounded by live parts (the measured quantity); feed lag — `commits_feed_idx` lateral max; backlog/unfinalized — migration-0007 `(hypertable_id, partition_values, level)`; GC — `pending_objects_sweep_idx` + `parts_reapable_idx`; pool — no query; watermarks — Kafka metadata, not SQL. No new index needed; none ships row sets.
- **Never-crash rule:** families are isolated closures; per-family error counter + degradable Kafka family (silent when unconfigured, debug-level after first failure) — tested by injection in Task 3.
- **Sequencing:** 1/2 independent; 3 needs 2; 4 independent; 5 needs 1–4; 6 last. Plan slots first in the remaining order — row 30's bench runs are the first consumers of these gauges.
- **Safety argument:** the collector only reads (SQL aggregates, Kafka metadata, statvfs); the one write surface is the metrics recorder. A wedged collector family can at worst stale its own gauges — alerts built on `time() - timestamp`-style signals (freshness) come from P1 call sites and are unaffected.

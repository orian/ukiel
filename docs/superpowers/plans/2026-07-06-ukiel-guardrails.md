# Ukiel Plan 18: Capacity Guardrails (Ingest Backpressure, Sweep Aggregation, Spread Warning) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the enforcement half of the design doc's "Limits & guardrails" section: (a) **ingest backpressure** — the flusher defers flushes when live L0 count in its target partitions crosses a slowdown threshold (halving flush rate ⇒ fewer, bigger L0 files) and stops past a stop threshold with a memory safety valve; (b) **finalization sweep aggregation in SQL** — `finalize_once` stops fetching every live part of every hypertable and instead asks Postgres for candidate partitions; (c) a **partition-spread warning** when one flush covers many partitions (a hard cap is impossible: a flush's Kafka offset range covers all consumed rows, so exactly-once forbids flushing some days and deferring others); (d) the **S8 GC-hygiene e2e scenario** the testing design specs (invariant I8) but which was never implemented when plan 6 landed.

**Architecture:** Three new catalog probes (`live_l0_parts`, `finalize_candidates`, `live_partition_parts`). Backpressure is a pure decision function in `ukiel-ingest` (`flush_decision`) unit-testable without Docker, wired into `RouteIngest::flush` before buffers are drained (deferring must not lose rows); Kafka offsets only advance with flushes, so deferral is automatically exactly-once-safe. Shutdown flushes bypass the guardrail (`force`). Observability is `tracing` warnings + existing stats-struct patterns; metric emission (`ingest_backpressure_deferrals_total`, `ingest_flush_partitions`, `compactor_unfinalized_partitions`) lands with monitoring phase P1 (`docs/superpowers/specs/2026-07-06-ukiel-monitoring.md`), not here.

**Tech Stack:** Rust edition 2024, sqlx/Postgres, testcontainers (Postgres + Kafka) component tests.

**Prerequisites:** Tasks 1, 3, 4, 5 are independent of plan 17. Task 2 (sweep aggregation) requires plan 17 (`2026-07-06-ukiel-lsm-hierarchy.md`) executed — it rewrites `finalize_once`. If plan 17 has not landed, execute Tasks 1, 3, 4, 5 and leave Task 2 for after.

## Global Constraints

- Rust edition 2024, toolchain ≥ 1.96.
- Component tests via `make test` (Docker required); the Task 3 decision-function tests must pass without Docker (`cargo test -p ukiel-ingest --lib`).
- **Every existing test stays green after every task.** Default thresholds are high enough (`l0_slowdown_parts = 30`) that no existing ingest test ever trips them.
- Commit messages: conventional style, no Claude/AI attribution.
- Every task ends with `cargo fmt && cargo clippy --all-targets -- -D warnings` clean.

---

### Task 1: Catalog probes

**Files:**
- Modify: `crates/ukiel-catalog/src/parts.rs`
- Test: `crates/ukiel-catalog/tests/catalog_test.rs`

**Interfaces:**
- Produces (all on `PostgresCatalog`):
  - `live_l0_parts(hypertable_id: HypertableId, partition_values: &serde_json::Value) -> Result<i64, CatalogError>` — count of live L0 parts in one partition (each L0 part is one ingest commit, so this is also the L0 *run* count). Backpressure input.
  - `finalize_candidates(hypertable_id: HypertableId) -> Result<Vec<serde_json::Value>, CatalogError>` — partition values holding ≥ 2 live runs (distinct `created_by_commit`), aggregated in SQL.
  - `live_partition_parts(hypertable_id: HypertableId, partition_values: &serde_json::Value) -> Result<Vec<Part>, CatalogError>` — live parts of one partition (what a finalization merge consumes).

- [ ] **Step 1: Write the failing tests**

Append to `crates/ukiel-catalog/tests/catalog_test.rs` (reuse the file's existing setup/part-commit helpers, adapting names to its actual ones — same convention as the plan-17 catalog tests):

```rust
#[tokio::test]
async fn capacity_probes_count_l0_runs_and_candidates() {
    let env = setup().await;
    let day1 = serde_json::json!({ "day": "2026-07-01" });
    let day2 = serde_json::json!({ "day": "2026-07-02" });

    // Empty: no pressure, no candidates.
    assert_eq!(env.catalog.live_l0_parts(env.ht, &day1).await.unwrap(), 0);
    assert!(env.catalog.finalize_candidates(env.ht).await.unwrap().is_empty());

    // Two L0 commits on day1, one on day2.
    add_l0_part(&env, &day1).await;
    add_l0_part(&env, &day1).await;
    add_l0_part(&env, &day2).await;

    assert_eq!(env.catalog.live_l0_parts(env.ht, &day1).await.unwrap(), 2);
    assert_eq!(env.catalog.live_l0_parts(env.ht, &day2).await.unwrap(), 1);

    // Only day1 has >= 2 live runs.
    let candidates = env.catalog.finalize_candidates(env.ht).await.unwrap();
    assert_eq!(candidates, vec![day1.clone()]);

    // live_partition_parts scopes to one partition.
    let parts = env.catalog.live_partition_parts(env.ht, &day1).await.unwrap();
    assert_eq!(parts.len(), 2);
    assert!(parts.iter().all(|p| p.meta.partition_values == day1));

    // Tombstoned parts stop counting everywhere (commit a REPLACE of day1's
    // two parts into one, then recheck).
    replace_partition_parts(&env, &day1, &parts).await; // one-output REPLACE
    assert_eq!(env.catalog.live_l0_parts(env.ht, &day1).await.unwrap(), 0);
    assert!(env.catalog.finalize_candidates(env.ht).await.unwrap().is_empty());
}
```

(`replace_partition_parts` = a `CommitOp::Replace` of the given part ids into one new `PartMeta { level: 1, .. }` — inline it or add a tiny helper following the file's commit pattern.)

- [ ] **Step 2: Verify they fail**

Run: `cargo test -p ukiel-catalog --test catalog_test capacity_probes`
Expected: compile error (the three methods don't exist).

- [ ] **Step 3: Implement**

Append to the `impl PostgresCatalog` block in `crates/ukiel-catalog/src/parts.rs`:

```rust
    /// Live L0 parts in one partition. Each L0 part is one ingest commit,
    /// so this is also the partition's L0 run count — the ingest
    /// backpressure input (plan 18).
    pub async fn live_l0_parts(
        &self,
        hypertable_id: HypertableId,
        partition_values: &serde_json::Value,
    ) -> Result<i64, CatalogError> {
        let count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM parts
             WHERE hypertable_id = $1 AND deleted_by_commit IS NULL
               AND partition_values = $2 AND level = 0",
        )
        .bind(hypertable_id.0)
        .bind(partition_values)
        .fetch_one(&self.pool)
        .await?;
        Ok(count)
    }

    /// Partitions holding more than one live run (distinct commits) —
    /// finalization candidates, aggregated in Postgres so the sweep never
    /// ships every live part row to the worker.
    pub async fn finalize_candidates(
        &self,
        hypertable_id: HypertableId,
    ) -> Result<Vec<serde_json::Value>, CatalogError> {
        let rows: Vec<serde_json::Value> = sqlx::query_scalar(
            "SELECT partition_values FROM parts
             WHERE hypertable_id = $1 AND deleted_by_commit IS NULL
             GROUP BY partition_values
             HAVING count(DISTINCT created_by_commit) >= 2
             ORDER BY partition_values::text",
        )
        .bind(hypertable_id.0)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    /// Live parts of one partition (what a finalization merge consumes).
    pub async fn live_partition_parts(
        &self,
        hypertable_id: HypertableId,
        partition_values: &serde_json::Value,
    ) -> Result<Vec<Part>, CatalogError> {
        let sql = format!(
            "SELECT {PART_COLUMNS} FROM parts
             WHERE hypertable_id = $1 AND deleted_by_commit IS NULL
               AND partition_values = $2
             ORDER BY id"
        );
        let rows: Vec<PartRow> = sqlx::query_as(sqlx::AssertSqlSafe(sql))
            .bind(hypertable_id.0)
            .bind(partition_values)
            .fetch_all(&self.pool)
            .await?;
        Ok(rows.into_iter().map(Part::from).collect())
    }
```

- [ ] **Step 4: Verify pass**

Run: `cargo test -p ukiel-catalog -- --test-threads=2`
Expected: new + existing tests PASS.

- [ ] **Step 5: Lint and commit**

```bash
cargo fmt && cargo clippy -p ukiel-catalog --all-targets -- -D warnings
git add crates/ukiel-catalog
git commit -m "feat: catalog capacity probes - l0 pressure, finalize candidates, partition parts"
```

---

### Task 2: Finalization sweep via SQL candidates *(requires plan 17)*

**Files:**
- Modify: `crates/ukiel-compactor/src/compactor.rs` (`finalize_once`)

**Interfaces:**
- Consumes: `finalize_candidates`, `live_partition_parts`, `partition_l0_quiet_since` (plan 17), `merge_parts` (plan 17).
- Behavior unchanged from the outside: `finalize_once() -> Result<usize, CompactorError>`; the plan-17 test `cold_partitions_finalize_into_a_single_run` must pass unmodified — it is the regression net for this rewrite.

- [ ] **Step 1: Rewrite `finalize_once`**

Replace the body of `finalize_once` in `crates/ukiel-compactor/src/compactor.rs` with:

```rust
        let mut finalized = 0;
        for hypertable in self.catalog.list_hypertables().await? {
            // Aggregated in SQL: only partitions with >1 live run come back,
            // as bare partition values — no part rows cross the wire here.
            for partition in self.catalog.finalize_candidates(hypertable.id).await? {
                let quiet = self
                    .catalog
                    .partition_l0_quiet_since(
                        hypertable.id,
                        &partition,
                        self.config.finalize_after_secs as f64,
                    )
                    .await?;
                if !quiet {
                    continue;
                }
                let parts = self
                    .catalog
                    .live_partition_parts(hypertable.id, &partition)
                    .await?;
                // Raced since the candidate query: recheck before merging.
                let runs: HashSet<i64> =
                    parts.iter().map(|p| p.created_by_commit.0).collect();
                if runs.len() < 2 {
                    continue;
                }
                let top = parts.iter().map(|p| p.meta.level).max().unwrap_or(0);
                match self.merge_parts(&hypertable, &parts, top + 1).await {
                    Ok(_) => finalized += 1,
                    // Raced by a merge/deletion: replan next sweep.
                    Err(CompactorError::Catalog(CatalogError::Conflict { .. })) => {}
                    Err(e) => return Err(e),
                }
            }
        }
        Ok(finalized)
```

(The `HashMap` partition-grouping import may become unused in this function — keep imports tidy per clippy.)

- [ ] **Step 2: Verify against the plan-17 tests**

Run: `cargo test -p ukiel-compactor -- --test-threads=2`
Expected: all PASS unmodified — especially `cold_partitions_finalize_into_a_single_run` (same observable behavior, cheaper query shape).

- [ ] **Step 3: Lint and commit**

```bash
cargo fmt && cargo clippy -p ukiel-compactor --all-targets -- -D warnings
git add crates/ukiel-compactor
git commit -m "perf: finalization sweep pulls SQL-aggregated candidates instead of all live parts"
```

---

### Task 3: Ingest backpressure

**Files:**
- Modify: `crates/ukiel-ingest/src/config.rs`
- Modify: `crates/ukiel-ingest/src/consumer.rs`
- Test: `crates/ukiel-ingest/tests/consumer_test.rs`

**Interfaces:**
- Consumes: `live_l0_parts` (Task 1).
- Produces: `IngestConfig { l0_slowdown_parts: usize, l0_stop_parts: usize, .. }` (defaults 30 / 200); `pub(crate) fn flush_decision(max_live_l0: i64, deferred: u32, total_rows: usize, config: &IngestConfig) -> FlushDecision` in `consumer.rs`; `Buffers.deferred_flushes: u32`; `RouteIngest::flush(hypertable, buffers, force: bool)` — shutdown passes `force = true`.
- Semantics: below slowdown → flush normally. At/over slowdown → defer every other tick (flush cadence halves ⇒ L0 files roughly double in size — the corrective is structural, not just throttling). At/over stop → defer until the memory valve (`total_rows ≥ 2 × max_buffer_rows`) forces a flush. Kafka offsets advance only with flushes, so deferral never risks loss or duplication.

- [ ] **Step 1: Write the failing decision-function tests (no Docker)**

Append to `crates/ukiel-ingest/src/consumer.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> IngestConfig {
        IngestConfig {
            brokers: String::new(),
            group_id: String::new(),
            flush_interval_ms: 10_000,
            max_buffer_rows: 100_000,
            l0_slowdown_parts: 30,
            l0_stop_parts: 200,
            tables: Vec::new(),
        }
    }

    #[test]
    fn flush_decision_thresholds() {
        let c = cfg();
        // Healthy: always flush.
        assert!(matches!(flush_decision(0, 0, 10, &c), FlushDecision::Flush));
        assert!(matches!(flush_decision(29, 5, 10, &c), FlushDecision::Flush));

        // Slowdown band: defer, then flush (every other tick).
        assert!(matches!(flush_decision(30, 0, 10, &c), FlushDecision::Defer));
        assert!(matches!(flush_decision(30, 1, 10, &c), FlushDecision::Flush));
        assert!(matches!(flush_decision(199, 0, 10, &c), FlushDecision::Defer));
        assert!(matches!(flush_decision(199, 1, 10, &c), FlushDecision::Flush));

        // Stop band: defer regardless of how long we've deferred...
        assert!(matches!(flush_decision(200, 7, 10, &c), FlushDecision::Defer));
        // ...until the memory valve (2x max_buffer_rows) forces a flush.
        assert!(matches!(
            flush_decision(200, 7, 200_000, &c),
            FlushDecision::Flush
        ));
    }
}
```

- [ ] **Step 2: Verify it fails**

Run: `cargo test -p ukiel-ingest --lib`
Expected: compile error (`flush_decision`, `FlushDecision`, and the two config fields don't exist).

- [ ] **Step 3: Implement**

1. `crates/ukiel-ingest/src/config.rs` — add to `IngestConfig` (after `max_buffer_rows`), with serde defaults so existing TOML/tests parse unchanged:

```rust
    /// Backpressure (plan 18): once a target partition holds this many live
    /// L0 parts, flush only every other tick (bigger, fewer L0 files).
    #[serde(default = "default_l0_slowdown_parts")]
    pub l0_slowdown_parts: usize,
    /// Hard stop: at this many live L0 parts, defer flushes entirely until
    /// the memory valve (2x max_buffer_rows) forces one.
    #[serde(default = "default_l0_stop_parts")]
    pub l0_stop_parts: usize,
```

and at the bottom of the file:

```rust
fn default_l0_slowdown_parts() -> usize {
    30
}

fn default_l0_stop_parts() -> usize {
    200
}
```

(If `IngestConfig` is constructed positionally anywhere in tests, compile errors point at them; add the two fields.)

2. `crates/ukiel-ingest/src/consumer.rs`:

Add after the `Buffers` struct:

```rust
/// Backpressure verdict for one flush tick (plan 18). Pure so it is
/// testable without Kafka or Postgres.
#[derive(Debug)]
pub(crate) enum FlushDecision {
    Flush,
    Defer,
}

pub(crate) fn flush_decision(
    max_live_l0: i64,
    deferred: u32,
    total_rows: usize,
    config: &IngestConfig,
) -> FlushDecision {
    // Memory valve: never hold more than 2x the buffer cap, whatever the
    // catalog says — a full stop must degrade to big flushes, not OOM.
    if total_rows >= config.max_buffer_rows.saturating_mul(2) {
        return FlushDecision::Flush;
    }
    if max_live_l0 >= config.l0_stop_parts as i64 {
        return FlushDecision::Defer;
    }
    if max_live_l0 >= config.l0_slowdown_parts as i64 && deferred == 0 {
        return FlushDecision::Defer; // every other tick in the slowdown band
    }
    FlushDecision::Flush
}
```

Add `deferred_flushes: u32` to `Buffers`:

```rust
    /// Consecutive flush ticks deferred by backpressure.
    deferred_flushes: u32,
```

Change `flush` to take `force: bool` and consult the guardrail before draining:

```rust
    async fn flush(
        &self,
        hypertable: &Hypertable,
        buffers: &mut Buffers,
        force: bool,
    ) -> Result<(), IngestError> {
        if buffers.ranges.is_empty() {
            return Ok(());
        }
        if !force && !buffers.rows_by_day.is_empty() {
            // Backpressure: probe live-L0 pressure on the partitions this
            // flush would touch. Deferring is exactly-once-safe — offsets
            // only advance with a flush.
            let mut max_live = 0i64;
            for day in buffers.rows_by_day.keys() {
                let pv = serde_json::json!({ "day": day });
                max_live =
                    max_live.max(self.catalog.live_l0_parts(hypertable.id, &pv).await?);
            }
            if let FlushDecision::Defer = flush_decision(
                max_live,
                buffers.deferred_flushes,
                buffers.total_rows,
                &self.config,
            ) {
                buffers.deferred_flushes += 1;
                tracing::warn!(
                    hypertable = %hypertable.name,
                    max_live_l0 = max_live,
                    deferred = buffers.deferred_flushes,
                    buffered_rows = buffers.total_rows,
                    "backpressure: flush deferred (compaction behind)"
                );
                return Ok(());
            }
        }
        buffers.deferred_flushes = 0;
        // ... existing body unchanged from here (parse cols, drain
        // rows_by_day into FlushItems, drain ranges, flusher.flush)
    }
```

Update the three call sites in `RouteIngest::run`: the shutdown arm passes `force = true`; the ticker arm and the `max_buffer_rows` arm pass `force = false`.

- [ ] **Step 4: Verify unit + component**

Run: `cargo test -p ukiel-ingest --lib`
Expected: `flush_decision_thresholds` PASS.

Run: `cargo test -p ukiel-ingest -- --test-threads=2`
Expected: all existing consumer/flusher tests PASS untouched (their L0 counts never approach 30).

- [ ] **Step 5: Write the failing component test**

Append to `crates/ukiel-ingest/tests/consumer_test.rs` (reuse its existing Kafka+Postgres fixture, `produce`, and `wait_for_parts` helpers; adapt names to the file's actual ones):

```rust
#[tokio::test]
async fn backpressure_defers_flushes_until_pressure_clears() {
    // Same fixture as consumes_flushes_and_resumes_exactly_once, but with
    // l0_stop_parts: 3 and today's partition pre-seeded with 3 fake live L0
    // parts (CommitOp::Add of tiny PartMeta rows with level 0 and
    // partition_values {"day": <today of the produced ts>}).
    // 1. Produce rows; wait 3+ flush intervals.
    //    Assert: part count unchanged (still the 3 seeds) and
    //    ingest_offsets for the topic are absent/unadvanced.
    // 2. Tombstone the 3 seeds (CommitOp::Delete).
    // 3. Assert wait_for_parts reaches 4 (the deferred rows flush) and the
    //    produced payloads are all present in the new part.
}
```

Write it fully against the harness's real helper names — the assertions that matter: **no offset advance while deferred** (exactly-once holds through backpressure) and **automatic recovery** once pressure clears.

- [ ] **Step 6: Verify component test passes**

Run: `cargo test -p ukiel-ingest --test consumer_test backpressure -- --test-threads=2`
Expected: PASS (Docker: Kafka + Postgres).

- [ ] **Step 7: Lint and commit**

```bash
cargo fmt && cargo clippy -p ukiel-ingest --all-targets -- -D warnings
git add crates/ukiel-ingest
git commit -m "feat: ingest backpressure on live L0 pressure with memory valve"
```

---

### Task 4: Partition-spread warning

**Files:**
- Modify: `crates/ukiel-ingest/src/config.rs`
- Modify: `crates/ukiel-ingest/src/consumer.rs`

**Interfaces:**
- Produces: `IngestConfig.warn_partitions_per_flush: usize` (serde default 64). A flush spanning more distinct partitions than this logs one `tracing::warn!` with the count — observability only. A hard cap is deliberately **not** implemented: a flush's offset ranges cover every row consumed since the last flush, so exactly-once forbids splitting a flush by partition (this is the CH `max_partitions_per_insert_block` analog, minus the rejection we can't afford).

- [ ] **Step 1: Implement (config + one warn)**

Config field (serde default like Task 3's):

```rust
    /// Warn when one flush spans more than this many partitions (backfill
    /// detector). No hard cap: offsets cover all consumed rows, so a flush
    /// cannot be split by partition without breaking exactly-once.
    #[serde(default = "default_warn_partitions_per_flush")]
    pub warn_partitions_per_flush: usize,
```

```rust
fn default_warn_partitions_per_flush() -> usize {
    64
}
```

In `RouteIngest::flush`, right after the backpressure block (before draining):

```rust
        if buffers.rows_by_day.len() > self.config.warn_partitions_per_flush {
            tracing::warn!(
                hypertable = %hypertable.name,
                partitions = buffers.rows_by_day.len(),
                "flush spans many partitions (backfill?) — L0 file fan-out"
            );
        }
```

Extend the Task 3 unit-test `cfg()` helper with the new field (compile-driven).

- [ ] **Step 2: Verify, lint, commit**

Run: `cargo test -p ukiel-ingest --lib && cargo test -p ukiel-ingest -- --test-threads=2`
Expected: all PASS.

```bash
cargo fmt && cargo clippy -p ukiel-ingest --all-targets -- -D warnings
git add crates/ukiel-ingest
git commit -m "feat: warn on partition-heavy flushes (backfill fan-out detector)"
```

---

### Task 5: ukield wiring, example config, docs status flips

**Files:**
- Modify: `crates/ukield/src/config.rs` (`IngestSection`)
- Modify: `crates/ukield/src/run.rs` (ingest config construction)
- Modify: `ukield.example.toml`
- Modify: `docs/superpowers/specs/2026-07-05-ukiel-design.md` (status wording only)
- Modify: `docs/superpowers/plans/2026-07-05-ukiel-v1-roadmap.md`

- [ ] **Step 1: Wire config through ukield**

Add to `IngestSection` in `crates/ukield/src/config.rs` (mirroring its existing `#[serde(default)]` struct pattern) and to its `Default`:

```rust
    pub l0_slowdown_parts: usize,   // default 30
    pub l0_stop_parts: usize,       // default 200
    pub warn_partitions_per_flush: usize, // default 64
```

Pass them through wherever `run.rs` builds the `ukiel_ingest::IngestConfig` (compile errors point at the literal).

Append to the `[ingest]` section of `ukield.example.toml`:

```toml
l0_slowdown_parts = 30        # halve flush rate when a partition holds this many live L0 files
l0_stop_parts = 200           # stop flushing (memory valve at 2x max_buffer_rows)
warn_partitions_per_flush = 64 # backfill fan-out warning threshold
```

- [ ] **Step 2: Full verification**

Run: `cargo fmt --check && cargo clippy --all-targets -- -D warnings && make test`
Expected: whole workspace green. Optionally `make e2e`.

- [ ] **Step 3: Docs status flips**

- Design doc `2026-07-05-ukiel-design.md`, "Limits & guardrails" section: change "(plan 18, `2026-07-06-ukiel-guardrails.md`)" phrasing from planned to implemented.
- Roadmap `2026-07-05-ukiel-v1-roadmap.md`: set row 18's status to **Executed**.

- [ ] **Step 4: Commit**

```bash
git add crates/ukield ukield.example.toml docs/
git commit -m "feat: guardrail config in ukield; docs mark plan 18 executed"
```

---

### Task 6: S8 — GC hygiene e2e scenario (testing-design debt)

**Files:**
- Create or extend: the scenario module in `crates/ukiel-e2e` (follow the existing S0–S7 file/module layout; all e2e tests are `#[ignore]`d and run via `make e2e`)

**Interfaces:**
- Implements S8 exactly as specced in `docs/superpowers/specs/2026-07-05-ukiel-testing-design.md` (scenario table, row S8): after an S6-style load-plus-compaction run **and a key deletion**, run GC to quiescence with **zero graces** (`tombstone_grace_secs: 0`, `orphan_grace_secs: 0` in the test's GC config), then assert the storage-lifecycle bijection.

- [ ] **Step 1: Write the scenario**

Using the `Stack` harness and the in-memory oracle model exactly as S6/S7 do:

1. Run S6's load shape (multi-tenant ingest, compactor running concurrently, drain to quiescence).
2. Delete one packing key via the deletion worker (as S7 does), updating the oracle model.
3. Run GC to quiescence with zero graces (reaper + orphan sweep until a full pass reaps and sweeps nothing).
4. Assert, in both directions:
   - every object in the bucket is the `path` of a **live** part (list the bucket once — allowed here, this is a test, not a hot path);
   - every live part's object exists (`head` each);
   - every namespace's query results still equal the oracle model (no referenced object was removed).

- [ ] **Step 2: Verify**

Run: `make e2e`
Expected: S8 passes alongside S0–S7. If S8 exposes a real reaper/sweeper bug (that is what it exists to do), fix it in `ukiel-gc` before proceeding — a red S8 is a finding, not a flake.

- [ ] **Step 3: Update the testing-design status and commit**

In `docs/superpowers/specs/2026-07-05-ukiel-testing-design.md`, mark S8 implemented (matching however S6/S7 are annotated), and in `docs/superpowers/plans/2026-07-05-ukiel-v1-roadmap.md` row 5, extend the scenario list to S8.

```bash
git add crates/ukiel-e2e docs/
git commit -m "test: S8 gc-hygiene e2e scenario - bucket and live parts are a bijection after gc"
```

---

## Self-review notes

- **Spec coverage** (design doc "Limits & guardrails" → guardrails paragraph): backpressure slowdown/stop/valve (T3), spread warning + why-no-cap (T4), SQL-side finalization candidates (T1+T2), config surface (T5). Monitoring metric *names* for these already exist in the monitoring spec; emission is monitoring P1's job, not this plan's.
- **Type consistency**: `live_l0_parts(.., &serde_json::Value) -> i64` matches T3's probe loop; `finalize_candidates -> Vec<serde_json::Value>` and `live_partition_parts -> Vec<Part>` match T2's rewritten `finalize_once`; `flush_decision(i64, u32, usize, &IngestConfig)` matches its unit tests and the T3 wiring; `flush(.., force: bool)` has exactly three call sites in `RouteIngest::run`.
- **Safety argument**: deferral never touches offsets (they move only inside `flusher.flush`), so backpressure inherits exactly-once for free; the memory valve bounds buffered rows at 2× `max_buffer_rows`; shutdown forces a flush so clean stops don't replay.

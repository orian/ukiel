# Ukiel Plan 28: Review Hardening (Probe Index, Grouped Probe, Merge Cap, Default Dedup) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the actionable findings of the plans-17/18 post-landing review (`docs/notes/2026-07-08-plan17-18-review.md`): (a) **issue 0007** — a composite index so the partition probes stop scanning all-time part history; (b) **issue 0006** — the ingest backpressure probe becomes one grouped catalog query per flush tick instead of one per buffered day; (c) **issue 0005 (interim)** — a merge-input size cap so an oversized cold partition stays unfinalized instead of OOM-crash-looping the compactor; (d) **issue 0009** — one source of truth for the ingest guardrail defaults (currently *four* places: the review note undercounted — `IngestConfig` serde fns, `IngestSection::Default`, the ingest test `cfg()`, and the e2e `Stack` fixture).

**Architecture:** No new components. One migration (`0007`), one catalog probe swap (`live_l0_parts` → `max_live_l0_parts`, since the only production caller ever wanted the max), a pure size check at the two `merge_parts` call sites (skip + warn + counter — never an error), and exported default constants. Everything is behavior-preserving at default settings: no existing test's data approaches the cap (1 GiB) or the thresholds.

**Tech Stack:** Rust edition 2024, sqlx/Postgres, testcontainers component tests.

**Prerequisites:** None (plans 17/18 executed). Must land **before plan 8**, which ports the backpressure code this plan touches into `PipelineIngest`.

## Global Constraints

- Rust edition 2024, toolchain ≥ 1.96; workspace dep pins per the roadmap.
- **Every existing test stays green after every task.** Defaults are chosen so nothing trips: cap 1 GiB, thresholds 30/200/64 unchanged.
- Unit tests (`cargo test -p <crate> --lib`) must pass without Docker; component tests via testcontainers.
- Commit messages: conventional style, no Claude/AI attribution.
- Every task ends with `cargo fmt && cargo clippy --all-targets -- -D warnings` clean.

---

### Task 1: Migration 0007 — partition-probe index (issue 0007)

**Files:**
- Create: `crates/ukiel-catalog/migrations/0007_partition_probe_index.sql`

**Interfaces:**
- No API change. Serves `partition_l0_quiet_since` (worst case: aggregates over every L0 part *ever* committed to a partition — deliberately including tombstoned rows, so the live-only partial `parts_live_idx` can never apply), `max_live_l0_parts` (Task 2), `live_partition_parts`, and the `GROUP BY partition_values` in `finalize_candidates`.

- [ ] **Step 1: Write the migration**

```sql
-- Issue 0007: the plan-17/18 partition probes filter parts by
-- (hypertable_id, partition_values[, level]), but parts_live_idx is keyed
-- on packing-key range and partial on live rows — partition_l0_quiet_since
-- (which counts tombstoned rows too: it measures arrivals, not liveness)
-- degrades to a per-hypertable scan growing with all-time history.
-- Deliberately non-partial so dead rows are covered.
CREATE INDEX parts_partition_idx
    ON parts (hypertable_id, partition_values, level);
```

(`partition_values` is JSONB; the default btree opclass gives the equality
matching every probe uses. `sqlx::migrate!` picks the file up automatically —
`PostgresCatalog::migrate` runs `./migrations`.)

- [ ] **Step 2: Verify**

Run: `cargo test -p ukiel-catalog -- --test-threads=2`
Expected: all PASS (every test runs the migration; a bad index definition fails at `migrate()`).

- [ ] **Step 3: Lint and commit**

```bash
git add crates/ukiel-catalog/migrations
git commit -m "perf: index partition probes - parts by (hypertable, partition, level)"
```

---

### Task 2: Grouped backpressure probe (issue 0006)

**Files:**
- Modify: `crates/ukiel-catalog/src/parts.rs`
- Modify: `crates/ukiel-catalog/tests/catalog_test.rs`
- Modify: `crates/ukiel-ingest/src/consumer.rs` (`RouteIngest::flush` probe loop)

**Interfaces:**
- Produces: `PostgresCatalog::max_live_l0_parts(hypertable_id: HypertableId, partitions: &[serde_json::Value]) -> Result<i64, CatalogError>` — max live-L0 count across the given partitions, one round-trip. Empty slice → 0.
- **Removes** `live_l0_parts` — its only production caller is the flush probe (`consumer.rs:297`), and `flush_decision` only ever consumed the max. The catalog test (`catalog_test.rs:897–940`) is rewritten against the new probe. This refines issue 0006's sketched `Vec<(Value, i64)>` return: nothing needs per-partition counts, so don't ship them.

- [ ] **Step 1: Rewrite the catalog test (failing)**

In `crates/ukiel-catalog/tests/catalog_test.rs`, replace the four `live_l0_parts` assertions inside `capacity_probes_count_l0_runs_and_candidates` with the grouped shape (same fixture — two L0 commits on `day1`, one on `day2`):

```rust
    // Grouped probe: max live-L0 count across the probed partitions.
    assert_eq!(catalog.max_live_l0_parts(ht, &[]).await.unwrap(), 0);
    assert_eq!(
        catalog.max_live_l0_parts(ht, &[day1.clone()]).await.unwrap(),
        2
    );
    assert_eq!(
        catalog.max_live_l0_parts(ht, &[day2.clone()]).await.unwrap(),
        1
    );
    assert_eq!(
        catalog
            .max_live_l0_parts(ht, &[day1.clone(), day2.clone()])
            .await
            .unwrap(),
        2
    );
```

and the post-REPLACE recheck (`catalog_test.rs:940`) with
`max_live_l0_parts(ht, &[day1.clone()])` → `0`.

- [ ] **Step 2: Verify it fails**

Run: `cargo test -p ukiel-catalog --test catalog_test capacity_probes`
Expected: compile error (method doesn't exist).

- [ ] **Step 3: Implement the probe**

In `crates/ukiel-catalog/src/parts.rs`, replace `live_l0_parts` with:

```rust
    /// Max live-L0 count across the given partitions, in one round-trip —
    /// the ingest backpressure input (issue 0006: the per-day probe loop
    /// was most expensive exactly during backfills). Each L0 part is one
    /// ingest commit, so counts are also L0 run counts.
    pub async fn max_live_l0_parts(
        &self,
        hypertable_id: HypertableId,
        partitions: &[serde_json::Value],
    ) -> Result<i64, CatalogError> {
        if partitions.is_empty() {
            return Ok(0);
        }
        let max: i64 = sqlx::query_scalar(
            "SELECT coalesce(max(cnt), 0) FROM (
                 SELECT count(*) AS cnt FROM parts
                 WHERE hypertable_id = $1 AND deleted_by_commit IS NULL
                   AND level = 0 AND partition_values = ANY($2)
                 GROUP BY partition_values) per_partition",
        )
        .bind(hypertable_id.0)
        .bind(partitions)
        .fetch_one(&self.pool)
        .await?;
        Ok(max)
    }
```

(sqlx binds `&[serde_json::Value]` as `jsonb[]`; if inference balks, cast
`ANY($2::jsonb[])`. The Task 1 index serves the inner `GROUP BY` lookup.)

- [ ] **Step 4: Rewire the flush probe**

In `RouteIngest::flush` (`crates/ukiel-ingest/src/consumer.rs`), replace the per-day loop:

```rust
            let partitions: Vec<serde_json::Value> = buffers
                .rows_by_day
                .keys()
                .map(|day| serde_json::json!({ "day": day }))
                .collect();
            let max_live = self
                .catalog
                .max_live_l0_parts(hypertable.id, &partitions)
                .await?;
```

(`flush_decision` call and the deferral warn below it are unchanged —
they already consume a single `max_live`.)

- [ ] **Step 5: Verify**

Run: `cargo test -p ukiel-catalog -- --test-threads=2 && cargo test -p ukiel-ingest -- --test-threads=2`
Expected: all PASS — including the plan-18 component test `backpressure_defers_flushes_until_pressure_clears`, which is the behavioral regression net for this rewiring.

- [ ] **Step 6: Lint and commit**

```bash
cargo fmt && cargo clippy -p ukiel-catalog -p ukiel-ingest --all-targets -- -D warnings
git add crates/ukiel-catalog crates/ukiel-ingest
git commit -m "perf: backpressure probes live-L0 pressure in one grouped catalog query"
```

---

### Task 3: Merge-input size cap (issue 0005 interim)

**Files:**
- Modify: `crates/ukiel-compactor/src/compactor.rs`
- Test: `crates/ukiel-compactor/tests/compactor_test.rs`

**Interfaces:**
- Produces: `CompactorConfig.max_merge_input_bytes: i64` — library default `1 << 30` (1 GiB); `0` = unlimited (escape hatch, and how a deployment opts out). Bytes, not MB, so component tests can set a 1-byte cap; ukield exposes MB (Task 5).
- Semantics: before either `merge_parts` call site, if the group's `sum(size_bytes) > cap`, **skip** — `tracing::warn!` + `compactor_capped_merges_total` counter, never an error. In the ladder loop skip means `continue` to the next level (a capped L1 group must not block that partition's L0 merges); in `finalize_once` it means `continue` to the next candidate (the partition stays multi-run: correct, queryable, just not terminal — see issue 0005). L0 pressure is unaffected in practice: L0 groups are small by construction (`l0_fanout` tiny files), so backpressure (which reads live-L0 counts) does not see a capped partition as pressure.

- [ ] **Step 1: Write the failing component test**

Append to `crates/ukiel-compactor/tests/compactor_test.rs` (reuse the file's existing fixture/commit helpers, adapting to its actual names — same convention as the plan-17 tests):

```rust
#[tokio::test]
async fn oversized_merges_are_capped_not_attempted() {
    // Same fixture as the ladder tests, but max_merge_input_bytes: 1 —
    // every real part exceeds it.
    // 1. Commit two L0 parts in one partition (>= l0_fanout, so the ladder
    //    would merge them).
    // 2. compact_once: assert merged_groups == 0 and the live parts are
    //    unchanged (both L0 parts still live, no new part).
    // 3. finalize_once: assert it returns 0 and live parts are unchanged.
    // 4. Flip the config to max_merge_input_bytes: 0 (unlimited) on a new
    //    Compactor over the same catalog: compact_once now merges (1 part
    //    out), proving 0 disables the cap.
}
```

- [ ] **Step 2: Verify it fails**

Run: `cargo test -p ukiel-compactor --test compactor_test oversized`
Expected: compile error (`max_merge_input_bytes` doesn't exist).

- [ ] **Step 3: Implement**

1. `CompactorConfig` — add after `fanout`:

```rust
    /// Skip any merge whose inputs' total size_bytes exceeds this (issue
    /// 0005: merges are in-memory until streaming merge lands; finalization
    /// would otherwise pull an arbitrarily large cold partition into RAM
    /// and OOM-crash-loop, since the sweep is stateless). Skipped merges
    /// warn and count `compactor_capped_merges_total`; the partition stays
    /// multi-run. `0` = unlimited.
    pub max_merge_input_bytes: i64,
```

with `Default` value `1 << 30`.

2. Private helper on `Compactor`:

```rust
    /// Issue 0005 interim guardrail (real fix: streaming merge, post-v1).
    fn merge_capped(&self, hypertable: &Hypertable, group: &[Part]) -> bool {
        let cap = self.config.max_merge_input_bytes;
        if cap <= 0 {
            return false;
        }
        let total: i64 = group.iter().map(|p| p.meta.size_bytes).sum();
        if total <= cap {
            return false;
        }
        tracing::warn!(
            hypertable = %hypertable.name,
            partition = %group[0].meta.partition_values,
            input_bytes = total,
            cap_bytes = cap,
            parts = group.len(),
            "merge skipped: input exceeds max_merge_input_bytes (issue 0005)"
        );
        metrics::counter!("compactor_capped_merges_total").increment(1);
        true
    }
```

3. Call sites — in `compact_hypertable`'s per-level loop, before the
`merge_parts` match:

```rust
                if self.merge_capped(hypertable, &group) {
                    continue; // next level; don't break the ladder for this partition
                }
```

and in `finalize_once`, after the `runs.len() < 2` recheck:

```rust
                if self.merge_capped(&hypertable, &parts) {
                    continue; // stays multi-run until the cap is raised
                }
```

- [ ] **Step 4: Verify**

Run: `cargo test -p ukiel-compactor -- --test-threads=2`
Expected: new test PASS; every existing ladder/finalization test PASS untouched (their parts are orders of magnitude under 1 GiB).

- [ ] **Step 5: Lint and commit**

```bash
cargo fmt && cargo clippy -p ukiel-compactor --all-targets -- -D warnings
git add crates/ukiel-compactor
git commit -m "feat: merge-input size cap - oversized merges skip instead of OOM (issue 0005 interim)"
```

---

### Task 4: Guardrail-default single source of truth (issue 0009)

**Files:**
- Modify: `crates/ukiel-ingest/src/config.rs`
- Modify: `crates/ukield/src/config.rs` (`IngestSection::Default`)
- Modify: `crates/ukiel-e2e/src/lib.rs` (the `Stack` ingest-config literals)

**Interfaces:**
- Produces: `pub mod defaults` in `ukiel_ingest::config` with `pub const L0_SLOWDOWN_PARTS: usize = 30`, `L0_STOP_PARTS: usize = 200`, `WARN_PARTITIONS_PER_FLUSH: usize = 64`. The private serde `default_*` fns return these consts; `ukield`'s `IngestSection::Default` and the e2e `Stack` fixture reference them instead of repeating literals. The ingest unit-test `cfg()` **keeps explicit literals** — its comment says the band thresholds should read clearly in the assertions, and drifting there breaks a test, not production.

- [ ] **Step 1: Implement**

`crates/ukiel-ingest/src/config.rs`:

```rust
/// Guardrail defaults (issue 0009: one source of truth — ukield's
/// `IngestSection` and the e2e fixture reference these, so the production
/// defaults cannot drift between the crate and its embedders).
pub mod defaults {
    pub const L0_SLOWDOWN_PARTS: usize = 30;
    pub const L0_STOP_PARTS: usize = 200;
    pub const WARN_PARTITIONS_PER_FLUSH: usize = 64;
}
```

and change the three `default_*` fns to return them. In
`crates/ukield/src/config.rs` (`IngestSection::Default`, lines ~143–145) and
`crates/ukiel-e2e/src/lib.rs` (lines ~242–244), replace `30` / `200` / `64`
with `ukiel_ingest::config::defaults::…`.

- [ ] **Step 2: Verify, lint, commit**

Run: `cargo test -p ukiel-ingest --lib && cargo test -p ukield --lib 2>/dev/null || cargo build -p ukield -p ukiel-e2e`
Expected: green (pure refactor; values identical).

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add crates/ukiel-ingest crates/ukield crates/ukiel-e2e
git commit -m "refactor: single source of truth for ingest guardrail defaults"
```

---

### Task 5: ukield wiring, example config, docs & issue status flips

**Files:**
- Modify: `crates/ukield/src/config.rs` (`CompactorSection`), `crates/ukield/src/run.rs`
- Modify: `ukield.example.toml`
- Modify: `docs/superpowers/specs/2026-07-06-ukiel-monitoring.md` (new counter)
- Modify: `docs/issues/0005…0007, 0009` + `docs/notes/2026-07-06-lsm-hierarchy.md` (status flips)
- Modify: `docs/superpowers/plans/2026-07-05-ukiel-v1-roadmap.md` (row 28 → Executed)

- [ ] **Step 1: Wire the cap through ukield**

`CompactorSection` gets `max_merge_input_mb: u64` (default `1024`, `0` =
unlimited), converted in `run.rs`'s `CompactorConfig` literal:

```rust
                max_merge_input_bytes: (cfg.compactor.max_merge_input_mb as i64) * 1024 * 1024,
```

(with a `0 → 0` passthrough — `0 * 1024 * 1024 == 0`, so the plain multiply
is the passthrough). Append to `ukield.example.toml`'s `[compactor]`:

```toml
max_merge_input_mb = 1024 # skip merges bigger than this (issue 0005; 0 = unlimited)
```

- [ ] **Step 2: Full verification**

Run: `cargo fmt --check && cargo clippy --all-targets -- -D warnings && make test`
Expected: whole workspace green. Then `make e2e` — S0–S8 all pass (the cap
default is far above e2e data sizes; S8's bijection is the regression net
for the skip paths introducing no orphans).

- [ ] **Step 3: Docs and issue status flips**

- Monitoring spec: add `compactor_capped_merges_total` (counter, call-site)
  to the metrics table; note it as the issue-0005 visibility signal.
- Issues: 0006, 0007, 0009 → **Resolved (plan 28)** with one line saying
  what shipped; 0005 → **Interim cap shipped (plan 28)** — streaming merge
  remains the open fix, post-v1 after plan 14.
- `docs/notes/2026-07-06-lsm-hierarchy.md`: the non-goals pointer gains
  "(cap shipped, plan 28)".
- Roadmap row 28 → **Executed** (list what landed, per the house style).

- [ ] **Step 4: Commit**

```bash
git add crates/ukield ukield.example.toml docs/
git commit -m "feat: merge cap config in ukield; docs mark plan 28 executed"
```

---

## Self-review notes

- **Issue coverage:** 0007 → Task 1, 0006 → Task 2, 0005 interim → Task 3 (+ Task 5 wiring), 0009 → Task 4. Issue 0008 (flat slowdown band) is deliberately absent — accepted, revisit gated on metrics P2 gauges.
- **Type consistency:** `max_live_l0_parts(HypertableId, &[serde_json::Value]) -> i64` matches the one call site's needs (`flush_decision` takes `max_live_l0: i64`); `live_l0_parts` has no callers left after Task 2, so removing it is clean (verified: only `consumer.rs:297` + the catalog test). `merge_capped(&Hypertable, &[Part])` matches both call sites' in-scope bindings (`hypertable: &Hypertable` in the ladder, `hypertable` owned in `finalize_once` — pass `&hypertable`).
- **Cap semantics reviewed:** `continue` (not `break`) in the ladder loop so a capped upper level never blocks L0 merges below it in the same partition; the sweep's `finalized` count simply excludes capped partitions, so the `run` loop's "finalized cold partitions" log stays truthful.
- **Sequencing:** Task 1 before Task 2 so the grouped probe lands on an indexed query shape. Task ordering otherwise independent. Whole plan before plan 8 (backpressure port) — recorded in the roadmap order rationale.
- **Ordering-stability note:** removing `live_l0_parts` is an API break within the workspace only; no external consumers exist (pre-1.0 experimental repo, README).

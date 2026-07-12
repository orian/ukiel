# Ukiel Plan 35: Catalog Aggregate Pushdown (issue 0010 via exact scan statistics) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close issue 0010 — metadata-answerable aggregates rescan data the catalog already holds. `SELECT COUNT(*)`, `MIN(col)`, `MAX(col)` over an unfiltered table are fully determined by `parts.row_count` and `parts.column_stats` (plan 12), yet ukiel scans parquet for them: the official suite's worst same-bytes ratios (q07 `MIN/MAX(EventDate)` **12.9×**, q01 `COUNT(*)` **9.7×** over raw DataFusion on identical local files; `docs/notes/2026-07-12-official-suite-gap.md`). Driver: roadmap row 35, issue 0010.

**Architecture — statistics, not a rewrite rule.** Issue 0010 sketched two mechanisms (a session optimizer rule; a provider plan-shape check). Both are superseded by what verification against the vendored DataFusion 54 sources found: **the engine already contains the entire rewrite** — the `AggregateStatistics` physical-optimizer rule asks each aggregate UDF to answer from plan statistics (`agg_expr.fun().value_from_stats(...)`, `datafusion-physical-optimizer-54.0.0/src/aggregate_statistics.rs:150`), and it fires when the scan reports **exact** statistics. Ukiel's provider reports none today (`PartitionedFile::new(path, size)` only). So the plan is: build one exact `Statistics` from the catalog part rows the provider already fetched, attach it via `FileScanConfigBuilder::with_statistics` (`datafusion-datasource-54.0.0/src/file_scan_config/mod.rs:417`), and let the engine do the elimination.

Why this is sound *by construction*, with the engine enforcing issue 0010's own conditions:

- **Any pushed filter auto-downgrades to inexact**: `FileScanConfig::statistics()` returns `to_inexact()` whenever the file source carries a filter (`file_scan_config/mod.rs:1107` — the doc comment says exactly why). Every scoped product scan carries the isolation predicate and every WHERE query pushes its filter, so the rewrite can only ever fire on **unfiltered, unscoped** scans — and everywhere else the statistics still flow as (correctly inexact) planner input. The namespaced `COUNT(*)` case issue 0010 flags as harder stays out of scope (its dedicated-parts-only exactness would mean weakening the `FilterExec` isolation backstop on catalog claims — recorded as the issue's follow-up line, not attempted).
- **Conservatism is per-column**: a column's min/max is `Precision::Exact` only when **every** surviving part carries that column's stat (plan-12 JSON `{"col": {"min", "max"}}`); any part missing it → `Precision::Absent` for that column → no rewrite for that column, scan as today. Parts written before plan 12 are the permanent degradation case, same rule as the pruning path. Row count is always exact: `PartMeta.row_count` is NOT NULL and the deletion path rewrites live parts with recounted rows, so `sum(row_count)` over live parts is the true live count.
- **`timestamp_ms` needs no special case**: v1 maps it to Arrow `Int64` (`crates/ukiel-core/src/schema.rs:3-4`), so `ScalarValue::Int64` covers every stats-carrying column; `utf8`/`float64`/`bool` columns get `Absent` min/max (our stats are Int64-only) and simply never rewrite.
- **Statistics describe the surviving part set**: computed after catalog pruning *and* plan-16's bitmap filter (the scan must report what it will read). In the only case where exactness matters (unscoped, no filters) no part is ever dropped by either, so exactness is unaffected; everywhere else the set is smaller and the statistics are inexact anyway.

Behavior-preserving beyond the rewrite: attaching statistics changes no result (the `AggregateStatistics` rule replaces an aggregate with the identical value or does nothing), and existing tests see either an unchanged plan (their fixtures commit `column_stats: None` parts → `Absent` columns; scoped fixtures carry the predicate → inexact) or an equivalent one — pinned explicitly in Task 2. **Per-file** `PartitionedFile::with_statistics` (which would additionally feed sort-pushdown Phase-2 file reordering) is deliberately **not** in scope: byte-range-split file groups may duplicate per-file stats across partitions (unverified DF behavior), and the aggregate rewrite needs only table-level statistics — noted as a follow-up experiment for plan 37 / the sort-pushdown opportunity row instead.

**Tech Stack:** no new dependencies. `datafusion_common::stats::{Statistics, ColumnStatistics, Precision}` (construct from `Statistics::new_unknown(&schema)` and set fields); verified anchors as cited above. Statistics are attached **unprojected, in physical-schema column order** (`FileScanConfig::statistics()` is documented as "unprojected table statistics"; projection is applied downstream).

**Prerequisites:** Plans 12 (column stats), 16 (executed — the scan region this splices into: `provider.rs:232` `scan`, `:245` `live_parts_pruned`, `:264–280` bitmap filter + access-plan loop, `:396` builder), 32 (operator session — the unscoped path where the rewrite fires). All executed. Independent of plan 37's in-flight execution (37's instruments are bench-side; if its F-fixes land first, re-anchor line numbers only).

## Global Constraints

- Rust edition 2024, toolchain ≥ 1.96; datafusion 54 / arrow+parquet 58.3 / object_store 0.13 pinned; docs.rs adapt-minimally rule on API drift.
- **Every existing test stays green after every task** — the isolation tests and `catalog_prunes_parts_by_ts_stats` exercise this exact scan path; Task 1 is pure (no Docker), Task 2's integration tests extend `query_test.rs`'s existing harness.
- The `FilterExec` isolation backstop is untouched — statistics feed planning, never row visibility.
- Conventional commits, no Claude/AI attribution; `cargo fmt && cargo clippy --all-targets -- -D warnings` per task.

---

### Task 1: Pure statistics builder in ukiel-query

**Files:**
- Create: `crates/ukiel-query/src/statistics.rs`
- Modify: `crates/ukiel-query/src/lib.rs` (module)

**Interfaces (Produces):**
- `pub fn parts_statistics(parts: &[ukiel_core::Part], schema: &arrow::datatypes::Schema) -> datafusion::common::Statistics` — pure; unit-testable without Docker. Semantics:
  - `num_rows`: `Precision::Exact(sum of row_count)` — including `Exact(0)` for an empty part set (an empty table's `COUNT(*)` is exactly 0; MIN/MAX rewrite to NULL or fall through — Task 2 pins whichever the engine does, both are correct).
  - `total_byte_size`: `Precision::Inexact(sum of size_bytes)` — encoded parquet bytes are not the in-memory size the planner means, so never claim exactness for it.
  - per column, in `schema` order: `min_value`/`max_value` = `Precision::Exact(ScalarValue::Int64(...))` folded over the parts' `column_stats` **iff every part has the column's entry**; else `Absent`. Non-Int64-backed columns: `Absent`. `null_count`/`distinct_count`: `Absent` (never tracked; an all-null column in any part has no stats entry there, which correctly forces `Absent` — state this at the decision site).

- [ ] **Step 1: Write the failing tests** (in-module `#[cfg(test)]`, `cargo test -p ukiel-query --lib`): exact fold across two parts (min of mins / max of maxes / summed rows); one part missing one column's entry → that column `Absent`, others still `Exact`; one part with `column_stats: None` → all columns `Absent`, rows still `Exact`; empty slice → `Exact(0)` rows; utf8 column → `Absent`.
- [ ] **Step 2: Verify they fail** (compile error — module doesn't exist).
- [ ] **Step 3: Implement.** Build test `Part`s with the same literal-JSON style the provider tests use (adapt to `ukiel_core::Part`'s actual construction).
- [ ] **Step 4: `cargo test -p ukiel-query --lib`. Step 5: lint and commit**

```bash
git commit -m "feat: exact table statistics from catalog part rows"
```

---

### Task 2: Provider attaches statistics; the engine's rule does the rest

**Files:**
- Modify: `crates/ukiel-query/src/provider.rs`
- Test: `crates/ukiel-query/tests/query_test.rs`

**Interfaces:**
- In `scan()`: track the parts that survive the plan-16 bitmap filter (the loop at `provider.rs:264–280` currently drops excluded parts without collecting them — keep survivor refs alongside `files`), then at the builder (`provider.rs:396`): `.with_statistics(crate::statistics::parts_statistics(&surviving, &self.physical_schema))`. One comment at the site: *statistics describe exactly the files this scan reads; DataFusion downgrades them to inexact under any pushed filter (`FileScanConfig::statistics`), which is what keeps the built-in `AggregateStatistics` rewrite sound — it can only fire on unfiltered, unscoped scans, where the catalog's numbers are the whole truth.*
- Integration tests (extend `query_test.rs`'s `setup()` fixtures; note they commit `column_stats: None` — the new tests commit parts **with** stats, following the plan-16 bitmap-test pattern):
  1. **The rewrite fires**: unscoped operator session, parts carrying full stats; `EXPLAIN SELECT count(*) FROM events` contains **no** `DataSourceExec`/`.parquet` (the aggregate became a literal) and the value is correct; same for `MIN(ts)`/`MAX(ts)`.
  2. **Any filter disarms it**: `... WHERE ts > 0` plans a scan and returns correct rows; a **scoped** session's `COUNT(*)` also plans a scan (isolation predicate ⇒ inexact) and still returns only its namespace's count — isolation re-asserted next to the new path.
  3. **Missing stats disarm it**: one part with `column_stats: None` in the mix → `COUNT(*)` may still rewrite (row_count is meta, always exact) but `MIN(ts)` must scan — pin both, they demonstrate the per-column rule.
  4. **Empty table**: unscoped `COUNT(*)` on a fresh hypertable = 0; `MIN(ts)` = NULL (whether by rewrite or empty scan — assert the value, not the plan).
- Delete nothing; the existing tests are the no-regression pin (their stats-less fixtures keep their current plans).

- [ ] **Step 1: failing tests → Step 2: verify fail → Step 3: implement → Step 4: `cargo test -p ukiel-query --test query_test` + full `cargo test -p ukiel-query -- --test-threads=2` → Step 5: lint and commit**

```bash
git commit -m "feat: catalog-exact scan statistics - metadata-answerable aggregates stop scanning"
```

---

### Task 3: Measure, record, close out

**Files:**
- Modify: `docs/issues/0010-aggregates-rescan-what-the-catalog-already-knows.md` (→ **Resolved**, one line on the mechanism; keep the namespaced-`COUNT(*)` paragraph as the recorded follow-up)
- Modify: `docs/notes/2026-07-12-official-suite-gap.md` (fix list item 3 → shipped, with numbers)
- Modify: `bench/README.md` (results addendum), `docs/notes/2026-07-06-parquet-read-performance.md` (the catalog-side Tier-2 story now covers aggregates)
- Modify: `docs/superpowers/plans/2026-07-05-ukiel-v1-roadmap.md` (row 35 → **Executed**)

No ukield config (no knobs), no migration, no new metrics (the rewrite is visible as plan shape; `query_parts_scanned` drops to zero on affected queries).

- [ ] **Step 1:** full-workspace check — `cargo fmt --check && cargo clippy --all-targets -- -D warnings && make test`. `make e2e` optional (no storage-lifecycle change; the query-path integration tests are the net).
- [ ] **Step 2: measure** — on the loaded local-store `hits_cb` fixture: q01 and q07 before/after (recorded anchors: 43.2 ms and 69.0 ms ukiel-local vs 1.0 ms raw; expected after: ~1 ms, zero object-store requests), plus a full `clickbench run` so the suite totals stay comparable (coordinate with plan 37's runs — same fixture, don't interleave with its A/Bs). Record labels + profile per the bench README rule.
- [ ] **Step 3:** docs + flips, commit:

```bash
git commit -m "docs: issue 0010 resolved - metadata-answerable aggregates answer from the catalog"
```

---

## Self-review notes

**Coverage mapping.** Issue 0010's two query classes → Task 1 (the numbers) + Task 2 (the wiring that lets the engine use them); its soundness conditions → enforced by `FileScanConfig::statistics()` filter-downgrade (verified, `mod.rs:1107`) + the per-column every-part rule (Task 1 tests) + the row-count/deletion argument (Architecture; deletion rewrites recount, so live-parts sum is exact); its "namespaced case is harder" paragraph → explicitly out of scope, kept in the issue as follow-up. Row 35's "session optimizer rule or provider-level check" wording → superseded by the statistics mechanism; the roadmap row text is updated at flip time (reality-beats-doc rule).

**Type consistency.** `parts_statistics(&[Part], &Schema) -> Statistics` consumes exactly what `scan()` holds at the splice point (`parts: Vec<Part>` from `live_parts_pruned`, `self.physical_schema: SchemaRef`); its output feeds `FileScanConfigBuilder::with_statistics(Statistics)` (by-value, `mod.rs:417`). `ScalarValue::Int64` matches every stats-carrying column because `timestamp_ms` is physically Int64 (`schema.rs:3-4`) and plan-12 stats are Int64-only by construction.

**Sequencing.** Task 1 (pure) → Task 2 (wiring + integration) → Task 3 (measure/flip). Independent of plan 37's remaining tasks except fixture coordination in Task 3 Step 2. Statistics are computed post-pruning/post-bitmap so plan 16's filter and this plan compose without ordering constraints between their code paths.

**Safety argument.** The rewrite path never sees a filtered or scoped scan with exact statistics — not by our discipline but by the engine's: any pushed filter (isolation predicate included) makes `FileScanConfig::statistics()` return inexact, and the `AggregateStatistics` rule only consumes exactness. Our conservatism handles the remaining edge (stats-less parts → `Absent` per column). Isolation is untouched: statistics influence planning only, and the `FilterExec` boundary remains above every scoped scan. Worst failure mode of a wrong statistic is therefore a wrong *plan cost estimate* on filtered queries (inexact anyway) — and on unfiltered unscoped queries the statistics are provably the data (row_count recounted on rewrite, min/max folded from per-part write-time stats over the complete live set).

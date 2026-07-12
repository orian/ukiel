# Ukiel Plan 37: Read-Stack Residual (attribution instruments + conditional fixes) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.
>
> **This is an investigation-first plan.** Tasks 1–3 build permanent measurement instruments (ordinary TDD tasks). Task 4 runs the attribution and fills in a decision table. Task 5's fixes are **pre-registered and conditional** — each states the finding that triggers it; a refuted fix is *recorded as refuted* in Task 6's write-up, never silently skipped. Do not invent fixes outside the pre-registered list mid-execution: a new hypothesis goes into the gap note and, if big, its own roadmap row (the plan-37 sizing rule).

**Goal:** Attribute — and close what is generically closable of — the read-stack residual, the largest remaining official-suite lever after plan 15: on **identical local bytes**, ukiel runs the 43 ClickBench queries in 38.6 s where raw DataFusion over ukiel's own files runs 27.7 s (**1.39×, ~10.9 s**; `docs/notes/2026-07-12-official-suite-gap.md`). Driver: roadmap row 37. Explicitly **out of scope**: q01/q07-class metadata-answerable aggregates (row 35 owns them — exclude both queries from every target number in this plan), the MergeTree format tax (row 34's named finding), transport (plan 15, done), and anything whose only beneficiary is the unscoped whole-table shape (the gap note's Stance section governs; every fix here must be generic, and the product suite guards it).

**Exit criteria (one must hold at Task 6):** the same-bytes residual, measured on the 41 in-scope queries, drops to **≤ 1.15×**; or every remaining component is attributed to a *named* mechanism with a disposition — fixed here, refuted, filed as its own row, or deferred to row 38 (DataFusion upgrade) with the specific upstream change identified.

**Architecture — instruments, then a gate, then conditional fixes:**

- **Instruments (Tasks 1–3)** are permanent bench capabilities, committed regardless of findings: a planning-vs-execution time split in the suite harness, a full physical-plan inspector (`clickbench plan`), and a per-partition execution profiler (`clickbench analyze`). Each runs against **both** sessions — the ukiel operator session and the raw reference (`--raw [--dir D]`) — because the whole method is differential: same bytes, same binary, two stacks.
- **The gate (Task 4)** runs the instruments over a fixed query panel and fills the hypothesis table below. The unresolved observation from the gap note is the anchor: dropping the (projected-away) ordering declaration cost 15–25% on predicated GROUP BYs with textually identical plans, identical operator metrics, identical total CPU, but ~1130% vs ~1450% achieved CPU. The gap note's EXPLAIN comparison was **display-truncated** (file-group lists print 5 groups; equivalence properties don't print at all) — H1 exists precisely because "identical displayed plan" never proved identical partition composition.
- **Fixes (Task 5)** are pre-registered against the hypotheses, each with its own trigger, design, guard, and A/B protocol.

| # | Hypothesis | Instrument that settles it | Pre-registered fix |
|---|---|---|---|
| H1 | Partition composition/order differs invisibly (group building + `FileGroupPartitioner` mode) → skew → lower achieved concurrency | `clickbench plan` full-group dump + `clickbench analyze` per-partition skew, ordering-toggle pairs | F1 |
| H2 | Per-query planning cost (catalog round-trip, provider construction, physical planning) is material | Task-1 plan/execute split across the suite | F2 (or a new row if the fix is a catalog snapshot cache) |
| H3 | 1,102 one-file partitions cost per-partition setup (opener, stream, readahead) vs 24 balanced groups | `clickbench analyze` on loaded (1,102 parts) vs the same data compacted; flamegraph diff | F1 |
| H4 | Per-file reader-factory/schema-adaptation overhead (`CachingParquetFileReaderFactory` wrapping, physical-schema mapping) | flamegraph diff ukiel-vs-raw on the uniform-band queries | F3 |
| H5 | The concurrency effect is DataFusion-54-internal (not reachable from provider code) | whatever H1–H4 leave unexplained, pinned to a DF code path | defer to row 38 with the path named |

**Tech Stack:** verified against vendored sources — `DataFrame::create_physical_plan(&self) -> Result<Arc<dyn ExecutionPlan>>` (`datafusion-54.0.0/src/dataframe/mod.rs:301`); plan walking via `ExecutionPlan::children()` with `DataSourceExec::data_source()` + `Arc::downcast`/`downcast_ref::<FileScanConfig>()` (`datafusion-datasource-54.0.0/src/source.rs:559,298`) exposing `file_groups`, and node `properties()` exposing partitioning + equivalence orderings; per-partition attribution via `ExecutionPlan::metrics() -> Option<MetricsSet>` whose `Metric.partition: Option<usize>` labels values per partition (`datafusion-physical-expr-common-54.0.0/src/metrics/mod.rs:76–197`); `/usr/bin/perf` is present on the bench box (fallback: any sampling profiler; record which). The suite harness is plan-34's `Session` enum (`suite.rs`) — the split and instruments apply to the `Session::DataFusion` arm; the ClickHouse arm records `None`.

**Prerequisites:** Plans 13, 15, 29, 30, 32, 34 executed (all are). The `hits_cb` fixture loaded via `UKIEL_E2E_STORE_DIR` (local store — this plan measures the stack, so transport must stay out of every number). **Independent of rows 16 and 35**: 16's bitmap/access-plan pruning is gated to the scoped-slice path and cannot affect the unscoped suite; 35's queries (q01/q07) are excluded from scope. If 16 lands first, only textual re-anchoring in `provider.rs::scan` may be needed. Open question named rather than settled: H5's mechanism may be unfixable at our layer — that is an acceptable exit (deferral with the DF path named), not a failure.

## Global Constraints

- Rust edition 2024, toolchain ≥ 1.96; datafusion 54 / arrow+parquet 58.3 / object_store 0.13 pinned; docs.rs adapt-minimally rule on API drift.
- **Measurement protocol (from the gap note, now binding):** every perf claim is a **same-binary A/B** (env-toggled or flag-toggled, never rebuild-between-configs), interleaved configs, ≥ 2 runs each, changes claimed only beyond the measured identical-config noise floor (last measured: median 1.5% / p90 7.9% per query on this box — re-measure, don't assume). State the build profile with every number.
- **Generic-only:** each Task-5 fix must either help or not touch the product path; `bench hits queries` (per-tenant suite, HTTP endpoint) runs before/after every fix as the guard — a product-suite regression beyond noise reverts the fix regardless of the official-suite win.
- Every existing test stays green after every task; component tests via `make test` (`--test-threads=2`); instruments must not perturb the timed path when unused (the split in Task 1 is two `Instant::now()` reads, not a feature flag).
- Conventional commits, no Claude/AI attribution; `cargo fmt && cargo clippy --all-targets -- -D warnings` per task.

---

### Task 1: Planning/execution split in the suite harness

**Files:**
- Modify: `crates/ukiel-e2e/src/bin/bench/suite.rs`
- Test: same file's test module (pure, no Docker)

**Interfaces:**
- `timed` (`suite.rs:126`) splits on the DataFusion arm: `ctx.sql(sql).await` (parse + logical plan) and `df.create_physical_plan().await` are timed together as `plan_ms`; execution via `datafusion::physical_plan::collect(plan, ctx.task_ctx())` as the remainder. Returns `(total_ms, plan_ms: Option<f64>, rows)`; the ClickHouse arm returns `plan_ms: None`. (Adapt to the `Session` enum's current method names — `session.query` today; add a sibling that exposes the split rather than changing `query`'s contract for the ClickHouse arm.)
- `QueryResult` gains `#[serde(default)] pub plan_ms: Option<f64>` and per-query output lines print it — **`serde(default)` so every recorded report still parses** (`read_report`/`compare` untouched; assert this in a test that deserializes a pre-37 JSON literal).
- Semantics note at the site: `collect` on a prepared plan re-runs no planning, so plan+execute ≈ the old single timing; the split must not change what the suite measures (assert `total ≈ plan + execute` is *not* required — they are measured independently; the headline stays the old total).

- [ ] **Step 1: failing test** — a `QueryResult` JSON without `plan_ms` deserializes (`None`); one with it round-trips.
- [ ] **Step 2: verify fail (compile error). Step 3: implement. Step 4: `cargo test -p ukiel-e2e` + one manual smoke (`clickbench run --iters 1` on any loaded fixture prints plan_ms). Step 5: lint and commit**

```bash
git commit -m "bench: suite records planning vs execution time per query"
```

---

### Task 2: `clickbench plan` — full physical-plan inspector

**Files:**
- Modify: `crates/ukiel-e2e/src/bin/bench/clickbench.rs`, `main.rs` (subcommand + usage)

**Interfaces:**
- `bench clickbench plan "<SQL>" [--raw [--dir D]]` — builds the physical plan (ukiel operator session by default; the raw reference session with `--raw`, same `--dir` semantics as `clickbench raw`) and prints, per plan node: name, `properties().output_partitioning` (count + kind), equivalence orderings (**the thing EXPLAIN never displays**), and for every `DataSourceExec` whose source downcasts to `FileScanConfig`: the **complete** file-group table — group index, file count, total bytes, and per-file `(path, byte_range)` — plus the source's declared `output_ordering`. No truncation; this exists because the display's 5-group cutoff hid whatever H1 is.
- Walk via `ExecutionPlan::children()` recursively; downcast per Tech Stack. Read-only — no execution.

- [ ] **Step 1: failing test** — unit-test the formatter on a hand-built two-group `FileScanConfig` (pure; the group-table formatter is an extracted fn taking `&FileScanConfig`).
- [ ] **Step 2–4: fail → implement → test + manual smoke against the loaded fixture** (`plan` on q28's SQL shows 24 groups with real byte ranges; `--raw` shows the reference's groups). **Step 5: lint and commit**

```bash
git commit -m "bench: clickbench plan - untruncated file groups, partitioning, orderings"
```

---

### Task 3: `clickbench analyze` — per-partition execution profile

**Files:**
- Modify: `crates/ukiel-e2e/src/bin/bench/clickbench.rs`, `main.rs`

**Interfaces:**
- `bench clickbench analyze "<SQL>" [--raw [--dir D]] [--iters N]` — executes the plan (`collect`), then walks it reading `metrics()` and prints, for each metrics-bearing node, a per-partition table (`Metric.partition()` labels): `output_rows`, `elapsed_compute`, and for the scan node the parquet timing metrics present in this DF version (opening/scanning — take what `MetricsSet` exposes; name them as found). Headline lines: wall ms, sum-of-elapsed-compute, **achieved parallelism** = compute/wall, and per-partition skew = max/mean partition compute. Repeats `--iters` times, reports each (skew is per-run, not aggregated away).
- This is the instrument that turns "~1130% vs ~1450% CPU" from `/usr/bin/time` folklore into per-operator, per-partition numbers.

- [ ] **Steps: failing test (formatter over a hand-built MetricsSet) → fail → implement → manual smoke (analyze q12 twice, tables print) → lint and commit**

```bash
git commit -m "bench: clickbench analyze - per-partition compute, skew, achieved parallelism"
```

---

### Task 4: Attribution runs — fill the hypothesis table

No product code. Fixed panel, chosen from the gap note's stack ratios: **uniform band** q14, q23, q29 (stack ≈ 1.6–2.1× on same bytes); **ordering-effect band** q12, q26 (the 15–25% identical-plan losers); **TopK** q24 (4.3×); **control** q38 (stack < 1 — the catalog win must survive any fix). All on the local-store fixture, loaded state; the ordering toggle pairs use `UKIEL_SCAN_DECLARE_ORDERING`-style env gating **only if reintroduced for the experiment via a temporary toggle as in the gap-note methodology — removed before commit** (the shipped predicate gate stays; the toggle exists to reproduce H1's two populations, not to change the product).

- [ ] **Step 1 (H1):** `clickbench plan` on each panel query, ukiel vs `--raw`, and ukiel-with vs -without the ordering toggle: diff **complete** group tables and orderings. Either the compositions differ (H1 confirmed — record how) or they are byte-identical at full resolution (H1 refuted for composition; skew moves to `analyze`).
- [ ] **Step 2 (H1/H3):** `clickbench analyze` same pairs: per-partition skew and achieved parallelism. H3 additionally: loaded (1,102 parts) vs packed-compacted local, same queries.
- [ ] **Step 3 (H2):** full suite once with Task 1's split; sum plan_ms over 41 in-scope queries; compare against the 10.9 s residual. (Prior expectation: small on the suite, product-latency-relevant regardless — record both framings.)
- [ ] **Step 4 (H4/H5):** `perf record -g` on `bench clickbench sql "<q14>"` and the same via `--raw`-equivalent (single-query, one process each, ≥ 3 runs); diff flamegraphs; attribute self-time deltas to named frames (factory, schema adaptation, catalog, DF internals).
- [ ] **Step 5:** write the filled hypothesis table + panel numbers into the gap note (new section "Residual attribution, plan 37"), each hypothesis: confirmed/refuted/partial + evidence pointer. Commit:

```bash
git commit -m "bench: stack-residual attribution - hypothesis table filled on the fixed panel"
```

---

### Task 5: Pre-registered conditional fixes

Each fix: execute **iff its trigger held in Task 4**; otherwise record the refutation and move on. Every fix follows the measurement protocol (same-binary toggle A/B, panel + full suite + `bench hits queries` guard) and lands as its own commit.

- [ ] **F1 (iff H1/H3): provider-side balanced file groups.** When the scan declares no ordering (the predicate-gate's "no pushdown predicate" arm — exactly where repartitioning is free anyway), the provider pre-packs the pruned file list into `min(target_partitions, files)` size-balanced multi-file groups instead of 1,102 one-file groups, matching `repartition_evenly_by_size` outcomes deterministically at plan time (files sorted by size descending, greedy into the lightest group — extract the pure fn, unit-test it: house `plan_chunks` precedent). Ordering-declared scans keep one-file-per-group (the sound-order claim requires it — comment stays). Trigger detail: only if Task 4 shows composition/count of groups explains ≥ a third of the affected band. Guard: q38 control + product suite unmoved.
- [ ] **F2 (iff H2 ≥ ~0.5 s suite-wide or ≥ ~5 ms per product query):** cheapest confirmed component only — e.g. reuse one `TableColumns::parse`/schema build per provider instead of per `scan()` (`provider.rs` constructs some of this per call; verify against findings). **A catalog snapshot cache is out of scope** — if the catalog round-trip itself is the cost, file it as its own roadmap row with the measured number (plan-sizing rule).
- [ ] **F3 (iff H4): named-frame micro-fixes** from the flamegraph diff (e.g. per-range lock or clone in `CachingParquetFileReaderFactory`), each ≤ ~50 lines, each with a frame-level before/after. Anything larger: own row.
- [ ] **H5 leftover:** whatever the fixes don't recover, pin to a DataFusion code path (module/function from the flamegraph or DF source read) and add it to row 38's motivation list with the number attached.

Commit per landed fix, message pattern:

```bash
git commit -m "perf: <fix> - <panel delta>, product suite unmoved (plan 37 F<n>)"
```

---

### Task 6: Close out — record, flip, verify exit criteria

**Files:**
- Modify: `docs/notes/2026-07-12-official-suite-gap.md` (residual section: final attribution table, fixes shipped/refuted, new same-bytes ratio)
- Modify: `bench/README.md` (results addendum: new totals with labels; instruments documented in §3 commands)
- Modify: `docs/superpowers/plans/2026-07-05-ukiel-v1-roadmap.md` (row 37 → **Executed**, listing instruments + landed fixes + H5 disposition; row 38's motivation updated with anything deferred)

- [ ] **Step 1:** final measurement — full 43-query suite (local store), 2 runs, plus `bench hits queries`; compute the 41-query in-scope residual vs `clickbench-raw-ukiel-files`. Check the exit criteria; if neither holds, the plan is **not done** — return to Task 4 with the unexplained component as a new named hypothesis (one iteration; if still unexplained, that is itself the H5 disposition).
- [ ] **Step 2:** full-workspace check — `cargo fmt --check && cargo clippy --all-targets -- -D warnings && make test` (`make e2e` only if any fix touched provider code — F1/F2 do — since the isolation tests plus S-suite are the net).
- [ ] **Step 3:** docs + flips, commit:

```bash
git commit -m "docs: stack residual attributed and closed out - plan 37 executed"
```

---

## Self-review notes

**Coverage mapping.** Row 37's charter (attribution first, fixes second, generic-only) → Tasks 1–3 instruments / Task 4 gate / Task 5 conditional fixes / Task 6 exit criteria. The gap note's three named suspects each have a hypothesis row (H1 concurrency/composition, H2 planning, H3 partition count) plus the two this re-derivation added (H4 per-file overhead, H5 DF-internal). The stance constraint is enforced structurally: q38 control query, product-suite guard on every fix, and the out-of-scope list keeps rows 34/35/36/38 material where it belongs.

**Type consistency.** `plan_ms: Option<f64>` with `serde(default)` — old reports parse (pinned by test); `compare` reads only fields it already read. Instruments consume verified APIs: `create_physical_plan` (dataframe/mod.rs:301), `data_source()`/`downcast_ref::<FileScanConfig>` (source.rs:559/298), `Metric::partition()` (metrics/mod.rs:197); the `Session` enum split matches plan 34's harness (`suite.rs:126` `timed(&Session, …)`). F1's pure grouping fn feeds the existing `FileGroup::new(vec![…])` construction site (`provider.rs:277–278`), multi-file groups being exactly what `FileGroup` already models.

**Sequencing.** 1→2→3 independent instruments (any order, all before 4); 4 gates 5; 5 gates 6's numbers. Independent of rows 16/35 (scoped-only / excluded queries) — stated in Prerequisites so the executor doesn't block on them. Each task is committable alone: instruments are inert additions; fixes are self-guarded.

**Safety argument.** No task touches correctness-bearing paths except F1/F2, and F1 only changes *grouping* of the same pruned file list on scans that declare no ordering — every file is still scanned exactly once (the pure fn's unit test asserts the partition is a permutation-partition of the input), isolation's FilterExec is untouched, and ordering-declared scans keep one-file-per-group so no false total-order claim can arise. The measurement protocol (same-binary toggles, interleaved runs, noise floor, product guard) is a Global Constraint precisely because the gap-note investigation showed rebuild-A/B and truncated EXPLAINs both mislead.

**Sizing.** 6 tasks, ~6–9 commits depending on how many fixes trigger — at the envelope's edge, kept inside it by the hard rule that any fix outgrowing its trigger files a row instead of growing here.

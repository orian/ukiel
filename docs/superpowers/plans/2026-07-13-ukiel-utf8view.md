# Ukiel Plan 39: Stop Defeating Utf8View (plan 37's H6) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

> **EXECUTED 2026-07-13** (commits `da28263`…`e5d4d3e`).
>
> **The same-bytes read-stack residual is CLOSED: 41 in-scope queries 35.3 s → 27.4 s = 0.99× raw DataFusion** (median per-query 0.98×), reproduced across two runs, **every answer identical**, and ukiel now beats raw outright on **24 of the 41**. Plan 37's unmet ≤ 1.15× exit arm is met.
>
> q34 — the query H6 was named on — now matches raw on **every** metric: wall 3,864 → **2,479 ms** (raw 2,533), peak memory 14.9 → **9.17 GB** (raw 9.2), repartition 3,449 → **1,055 ms** (raw 1,086), byte-identical scan. The string-heavy band collapses onto raw (q23 −44%, q22 −43%, q28 −42%, q26 −40%, q35 −37%, q34 −34%, q21 −33%).
>
> Deviations from the plan, all deliberate:
> - **Task 3 became simpler than designed.** `ukiel_expr::compile` already casts to its target, so the "infer the type, conditionally wrap in a cast" guard was unnecessary: passing the *query schema's* declared type (rather than `TableColumns`') is the whole fix, and the cast lands on the alias output by construction.
> - **One thing the plan did not anticipate: the wire contract.** The flip made `schema_block` report `"utf8view"` to HTTP clients — an internal representation escaping the public API. `ukiel_core::ukiel_type_of` now maps `Utf8View → "utf8"`, and a test pins it. Caught by the equal-JSON test in Task 2, which is exactly what it was for.
> - **The product guard needed its own noise floor.** The small (2–10 ms) per-tenant scenarios moved ±10–18%, which looks like a regression against plan 37's 7.9% p90 — but that floor was measured on 100–4,000 ms official-suite queries. Measured *at product scale*, the same-config spread is **up to 22%**, so the movement is noise. The scenarios with a tight floor all improve: heavy `search_phrases` **−35%** (3% spread), `top_urls` **−12%** (5% spread).
>
> **This does not mean the stack is free.** It means the catalog's pruning advantage on key-filtered queries now roughly cancels what the stack costs elsewhere, because the two *systematic* overheads on top of it — F3's doubled predicate and this plan's string-representation opt-out — are gone.

**Goal:** Close plan 37's H6 — the largest remaining official-suite lever. DataFusion 54 defaults `schema_force_view_types = true` (verified: `datafusion-common-54.0.0/src/config.rs:809`), so the raw reference reads string columns as **`Utf8View`** (zero-copy views over decode buffers); ukiel's provider hands `ParquetSource` an explicit `Utf8`-typed schema and thereby opts out — `arrow_typeof("URL")` is `Utf8` on ukiel, `Utf8View` on raw, *over the same files*. Measured cost (gap note §H6): q34 (`GROUP BY "URL"`, byte-identical scan) **+62% peak memory, +218% repartition time, 1.53× wall**; the ten string-heavy queries hold **91% of the post-F3 same-bytes residual** (7.5 s / 41 queries). Driver: roadmap row 39. Success also likely completes plan 37's unmet ≤ 1.15× exit arm — Task 5 checks and records it.

**Architecture — the view boundary sits at the ukiel-query crate line.** `TableColumns::physical_schema()`/`query_schema()` (`crates/ukiel-core/src/schema.rs:170,175`) stay exactly as they are: ingest builds owned `StringArray`s from JSON, the compactor's streaming merge reads-and-rewrites with the same schema, `ukiel_expr`'s *materialized* columns evaluate at ingest — all write-side, all `Utf8`, all untouched (on-disk parquet is byte-array either way; view-vs-owned is an in-memory representation, not a format). What changes is only what the **query path** asks the reader to produce: the provider view-types its copy of both schemas (`Utf8 → Utf8View`; nothing else — names, order, nullability, Int64/Float64/Boolean identical), so parquet decode yields views, and views flow through repartition/aggregation instead of copies. `query_schema` must flip **together with** the scan schema: if SQL still saw `Utf8`, DataFusion would insert a cast that re-materializes every string and hands the win back.

Why the rest of the read path tolerates it, verified against the tree:
- The provider's expression building on utf8 columns is type-agnostic or Int64-only: `extract_int64_ranges` (`provider.rs:242`) and the isolation predicate (`provider.rs:336–339`) touch Int64; `ukiel_expr::column_refs(sql, &schema)` (`provider.rs:304`) resolves names, not types. User predicates on strings are planned by DataFusion against the (now view-typed) table schema — string predicates on views are the engine's default configuration, exercised by every stock DataFusion deployment.
- *Alias* columns are the one read-side `ukiel_expr` surface (compiled via DataFusion's `create_physical_expr` over a `DFSchema`, `ukiel-expr/src/lib.rs:10–14`): compiled against the view-typed schema, DF's own type inference does the work. The residual risk — a string kernel returning `Utf8` where the declared alias type says view (or vice versa) — is handled at the alias projection with a cast **on the alias output only** (a final, narrow column; not the hot path), decided by comparing the inferred type to the declared one at build time.
- HTTP results serialization (`results.rs:117–118`) matches on `DataType` and downcasts `StringArray` — it needs a `Utf8View`/`StringViewArray` arm, and so do the test helpers (`query_test.rs::all_strings` downcasts `StringArray` and would panic on view output — it must become view-aware **in the same commit** as the provider flip, or half the suite fails; called out in Task 4).
- Plan-35 interplay (may execute concurrently): `parts_statistics` marks utf8 columns `Absent` regardless of view typing, and column order is unchanged — semantic no-op; both plans edit `provider.rs`, so coordinate textually, not logically.

**Tech Stack:** no new dependencies — `DataType::Utf8View`/`StringViewArray` are arrow 58.3. Measurement per the plan-37 protocol (Global Constraints).

**Prerequisites:** Plan 37 executed (F3 landed — this plan's before-numbers are the post-F3 ones: 35.3 s / 1.27× / median 1.09×). Plan 35 in flight is fine (see interplay above). Open question named rather than settled: whether q34-class *peak memory* fully recovers depends on DF's view-aware group-by internals — the exit is measured, not assumed.

## Global Constraints

- Rust edition 2024, toolchain ≥ 1.96; datafusion 54 / arrow+parquet 58.3 / object_store 0.13 pinned; docs.rs adapt-minimally rule.
- **Measurement protocol (plan 37's, binding):** same-binary A/B where a toggle is possible — here the before/after is a code change, so: interleave nothing, rerun the recorded post-F3 labels' exact commands, ≥ 2 runs, claim beyond the re-measured noise floor, state the profile. **Product guard:** `bench hits queries` before/after; strings flow through every product scenario's `payload`, so this suite is the leak-detector, and a regression beyond noise reverts the flip.
- **Every existing test stays green after every task** — Tasks 1–3 are additive; Task 4 (the flip) carries the test-helper migration in the same commit and re-runs the full ukiel-query suite plus `make e2e` (S-suites read strings over HTTP end to end).
- Isolation is untouched: the `FilterExec` boundary and the slice predicate are Int64 machinery.
- Conventional commits, no Claude/AI attribution; `cargo fmt && cargo clippy --all-targets -- -D warnings` per task.

---

### Task 1: Pure view-typing helper

**Files:**
- Create: `crates/ukiel-query/src/view_types.rs`
- Modify: `crates/ukiel-query/src/lib.rs` (module)

**Interfaces (Produces):**
- `pub fn view_typed(schema: &arrow::datatypes::Schema) -> arrow::datatypes::Schema` — maps every `DataType::Utf8` field to `DataType::Utf8View`, preserving name, nullability, metadata, and order; every other type passes through untouched. Pure; the doc comment states the boundary rule (query-side representation only; writers and core schemas never see it) and cites `schema_force_view_types` as the engine default this restores parity with.

- [x] **Step 1: failing tests** (`cargo test -p ukiel-query --lib`): utf8→utf8view; int64/float64/bool/timestamp-backed-int64 unchanged; names/nullability/order preserved; empty schema.
- [x] **Step 2: verify fail (module missing). Step 3: implement. Step 4: test. Step 5: lint and commit**

```bash
git commit -m "feat: view-typed query schema helper - Utf8View parity with the engine default"
```

---

### Task 2: HTTP results serialize Utf8View

**Files:**
- Modify: `crates/ukiel-query/src/results.rs`
- Test: its existing test module (adapt to local fixture style)

**Interfaces:**
- The `DataType` match (`results.rs:117`) gains a `DataType::Utf8View` arm downcasting `StringViewArray`, emitting JSON identical to the `Utf8` arm (nulls included). Check what the current fallthrough arm does with an unknown type and keep that contract for genuinely-unknown types.

- [x] **Step 1: failing test** — a `RecordBatch` with a `StringViewArray` column serializes to the same JSON as its `StringArray` twin (build both from the same values; assert equality).
- [x] **Step 2–4: fail → implement → `cargo test -p ukiel-query`. Step 5: lint and commit**

```bash
git commit -m "feat: query results serialize Utf8View columns"
```

---

### Task 3: Alias expressions over view-typed inputs

**Files:**
- Modify: `crates/ukiel-query/src/provider.rs` (alias projection site — the `col(field.name(), &scanned_schema)`/`ukiel_expr` region around `provider.rs:304–330`; adapt to current lines)
- Possibly modify: `crates/ukiel-expr/src/lib.rs` (only if compilation against a view-typed `DFSchema` surfaces a gap — expected not: it delegates to DataFusion's `create_physical_expr`)
- Test: `crates/ukiel-query/tests/query_test.rs` (the existing alias tests are the fixture pattern — e.g. `narrow_projection_and_alias_on_unprojected_column`)

**Interfaces:**
- Alias physical exprs compile against the **view-typed** scanned schema. At the alias projection build site: infer the expr's output type; if it differs from the declared query-schema field type, wrap in a cast **on the alias output only**, with the comment: *string kernels may return either string representation; casting one final projected column is cheap, casting the scan is the bug this plan removes.*
- This task lands **before** the flip and must be a no-op under the current `Utf8` schemas (the inferred-vs-declared comparison already matches today) — that is what keeps it independently committable.

- [x] **Step 1: failing-or-green test** — a unit-style test compiling and evaluating an alias expr over a batch whose string column is a `StringViewArray` against a view-typed schema, asserting value correctness and output-type consistency with the declared field. (Under today's schemas this test constructs the view world explicitly — it fails until the cast-guard exists.)
- [x] **Step 2–4: fail → implement → `cargo test -p ukiel-query -- --test-threads=2`. Step 5: lint and commit**

```bash
git commit -m "feat: alias projection tolerates view-typed string inputs"
```

---

### Task 4: The flip — provider schemas go view-typed

**Files:**
- Modify: `crates/ukiel-query/src/provider.rs` (construction site `provider.rs:199–204`)
- Modify: `crates/ukiel-query/tests/query_test.rs` (helpers + new type-parity tests)

**Interfaces:**
- `UkielTableProvider::try_new`: `physical_schema = Arc::new(view_typed(&columns.physical_schema()))`, `query_schema = Arc::new(view_typed(&columns.query_schema()))`. One comment at the site: *query-side representation only — core schemas, ingest, and the compactor keep `Utf8`; parquet bytes are identical; flipping only one of the two schemas would insert a materializing cast and hand the win back (gap note §H6).* Everything downstream (`ParquetSource::new` at `provider.rs:345`, scanned-schema projection at `:317`, pushdown, statistics if plan 35 landed) picks the change up through these two fields — no other provider edits.
- **Test-helper migration in the same commit:** `all_strings` (and any `StringArray` downcast in test helpers) becomes representation-agnostic (match on both array types, or go through `arrow::compute::cast` to `Utf8` inside the helper).
- New tests: `SELECT arrow_typeof("payload")`-style assertion returns `Utf8View` through **both** the operator session and a scoped session; the full existing suite re-run is the correctness net (isolation, alias, pushdown, ts-stats pruning, plan-16 bitmap tests — none assert `Utf8` plan types, verified by grep; the helpers were the only trap).

- [x] **Step 1: failing test** (the `arrow_typeof` parity test). **Step 2: verify fail** (`Utf8` today). **Step 3: implement flip + helper migration. Step 4:** `cargo test -p ukiel-query -- --test-threads=2` **and** `make e2e` (strings over HTTP end to end; S1/S4/S7 read `payload`). **Step 5: lint and commit**

```bash
git commit -m "perf: query path reads strings as Utf8View - stop defeating the engine default"
```

---

### Task 5: Measure, record, close out — including plan 37's exit arm

**Files:**
- Modify: `docs/notes/2026-07-12-official-suite-gap.md` (§H6 → fixed, with numbers; residual table gains the row)
- Modify: `docs/superpowers/plans/2026-07-13-ukiel-stack-residual.md` (executed banner: note the ≤ 1.15× arm's post-39 status)
- Modify: `bench/README.md` (results addendum, labels + profile)
- Modify: `docs/superpowers/plans/2026-07-05-ukiel-v1-roadmap.md` (row 39 → **Executed**)

- [x] **Step 1: measure** — on the post-F3 fixture and commands: q34/q35 wall + `peak_mem_used` + `repartition_time` via `clickbench analyze` (before-numbers recorded in the gap note: q34 3,864 ms / 14.9 GB / 3,449 ms), the 41-query totals (before: 35.3 s, 1.27×, median 1.09×), and the **product guard** `bench hits queries`. Expected: most of the 7.5 s string residual recovered; check whether the total now meets plan 37's ≤ 1.15× — record either way, flipping 37's exit note if met.
- [x] **Step 2: full-workspace check** — `cargo fmt --check && cargo clippy --all-targets -- -D warnings && make test` (e2e already ran in Task 4).
- [x] **Step 3: docs + flips, commit**

```bash
git commit -m "docs: Utf8View parity measured and recorded - row 39 executed"
```

---

## Self-review notes

**Coverage mapping.** Row 39 / gap-note §H6 → Tasks 1 (representation mapping) + 4 (the flip at the decision point `ParquetSource::new(physical_schema)`, via the two provider fields); the row's "design pass on where the boundary sits" → the Architecture section's crate-line rule with each shared consumer (ingest, compactor, materialized columns) explicitly left on `Utf8`; the two read-side compatibility surfaces the row names (`ukiel_expr` alias compilation, plus results serialization found by this plan's own grep) → Tasks 3 and 2; plan 37's unmet exit arm → Task 5.

**Type consistency.** `view_typed(&Schema) -> Schema` feeds the two `Arc::new(...)` construction sites (`provider.rs:199–204`) whose fields type every downstream use (`SchemaRef` throughout — no signature changes anywhere); `StringViewArray` is the arrow 58.3 counterpart the results arm and test helpers downcast; alias cast-guard compares `ExprSchemable`-inferred type against the declared query-schema field — both `DataType`s from the same schema object.

**Sequencing.** 1 (pure) → 2, 3 (independent, additive, no-ops under current schemas) → 4 (the flip; helpers migrate in the same commit — the one place "every test stays green" needs care) → 5 (measure/flip docs). Tasks 2 and 3 before 4 is load-bearing: after the flip, their absence is a runtime panic (results) and a potential type error (alias), not a perf note.

**Safety argument.** No correctness surface changes representation across a trust boundary: isolation predicate and slice pruning are Int64; parquet bytes and all writers are untouched; `Utf8View` vs `Utf8` carry identical values (pinned by Task 2's equal-JSON test and Task 4's full-suite + e2e run, where every product scenario reads strings end to end). The single new failure mode — a kernel/type mismatch at the alias projection — is closed by construction in Task 3 with a narrow output-side cast. Worst remaining case is a perf disappointment, and Task 5's protocol records it against the pre-registered expectation instead of rounding it away.

**Sizing.** 5 tasks, ~5 commits — inside the envelope.

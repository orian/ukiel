# Ukiel Plan 27: Ingest Sorts by Declared sort_key (Shared Sorting Module) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the latent soundness gap from `docs/notes/2026-07-07-ingest-sort-by-sort-key.md`: the query provider declares per-file ordering from `hypertable.sort_key` (`provider.rs:265–277`, `with_output_ordering`) and DataFusion **elides sorts** on that declaration — but ingest physically sorts L0 by the hardcoded pair `(packing_key, ts_column)` (`writer.rs:77`) and never reads `sort_key`. Any config where `sort_key != [packing_key, ts_column]` silently returns wrong query results until compaction rewrites the files. Deliverables: (a) **registration-time validation** of the invariants the packing machinery actually needs (the guarantee lands *before* code trusts it); (b) a **shared `ukiel-core::sorting` module** — one lexsort, one `SortingColumn` stamp, one writer-props builder for ingest and compactor, so ordering semantics cannot drift again; (c) a **unified nulls convention** (asc, nulls-first — today three layers disagree: `lexsort_to_indices(None)` sorts nulls first, both parquet stamps say `nulls_first: false` (`rewrite.rs:169`, `writer.rs:123,128`), the provider declares nulls-first); (d) **ingest sorts the Arrow batch by `sort_key`** after defaults/materialized are applied.

**Architecture:** Validation is a pure fn in the new core module, enforced at three choke points (catalog `create_hypertable`, ukield `bootstrap::apply`, ingest route startup — the last two give clear errors for catalogs that predate validation). The sort/stamp/props trio moves from `ukiel-compactor::rewrite` into `ukiel-core::sorting`; `rewrite` keeps thin delegating re-exports so external callers (the bench binary, `hits.rs:254/263`) compile unchanged. Ingest's pipeline becomes decode (unsorted, keeps the packing/ts i64 poison validation) → defaults/materialized → `sort_batch(sort_key)` → encode with `sorted_writer_props` — which also means `sort_key` may include defaulted/materialized columns (the old pre-decode JSON sort could never see those values, and its `as_i64` comparators only ever handled two i64 columns). Everything is behavior-preserving for every existing config: all current tables have `sort_key == [packing_key, ts_column]` with non-nullable i64 columns, so files come out byte-identical in row order; the only observable delta is the parquet `SortingColumn.nulls_first` metadata flip (false→true), which no existing file with nulls exists to be affected by and which the read path does not consume for pruning.

**Tech Stack:** Rust edition 2024; workspace pins (arrow/parquet 58.3). One dep move: `ukiel-core` gains the workspace `parquet` dep (for `SortingColumn`/`WriterProperties`).

**Prerequisites:** Plan 17 executed (this ports 17's rewritten compactor write path onto the shared module rather than re-authoring it). Must execute **immediately before plan 14**, which extends the shared writer-props module this plan births (one module, not two — the roadmap-order rationale). The correctness fix is latent — no current config trips it — so there is no backfill/migration step (see Compat).

## Global Constraints

- Rust edition 2024, toolchain ≥ 1.96; workspace dep pins per the roadmap (arrow/parquet 58.3 — never bump independently).
- **Every existing test stays green after every task.** Validation passes every existing config by construction; the sort refactor is behavior-preserving for `[packing_key, ts_column]` i64 keys (the plan-11 declared-ordering test `order_by_sort_key_results_stay_correct_and_ordered`, `query_test.rs:693`, is the end-to-end regression net).
- Unit tests (`cargo test -p <crate> --lib`) pass without Docker; component tests via testcontainers.
- Commit messages: conventional style, no Claude/AI attribution.
- Every task ends with `cargo fmt && cargo clippy --all-targets -- -D warnings` clean.

---

### Task 1: `ukiel-core::sorting` module — validation half

**Files:**
- Create: `crates/ukiel-core/src/sorting.rs` (module declared in `lib.rs`)
- Modify: `crates/ukiel-catalog/src/tables.rs` (`create_hypertable`), `crates/ukield/src/bootstrap.rs` (`apply`), `crates/ukiel-ingest/src/consumer.rs` (route startup, where the hypertable is resolved)
- Test: core unit tests; `crates/ukiel-catalog/tests/catalog_test.rs`; `crates/ukield/tests/bootstrap_test.rs`

**Interfaces:**

```rust
/// The invariants the packing machinery needs from sort_key (note §invariants):
/// 1. non-empty; 2. sort_key[0] == packing_key (split_by_key, key_range
/// pruning, and the isolation predicate assume the packing key leads);
/// 3. every column exists in the schema (kills the provider's silent
/// truncation on unknown names); 4. ts_column ∈ sort_key when given
/// (preserves within-key time order the read path assumes) — Option because
/// only bootstrap/routes know ts_column; the catalog checks 1–3.
pub fn validate_sort_key(
    sort_key: &[String],
    packing_key: &str,
    ts_column: Option<&str>,
    schema: &arrow::datatypes::Schema,
) -> Result<(), String>;
```

- Enforcement: `create_hypertable` runs rules 1–3 (it has `table_schema`; parse via the existing `TableColumns`/`arrow_schema_from_json` path) and returns a new `CatalogError::InvalidTable(String)` variant; `bootstrap::apply` and ingest route startup run all four with the table name in the error — fail fast, clear message, because existing catalogs predate the validation.
- Ordering matters: this lands **first** so `sort_key[0] == packing_key` is a *guarantee* by the time Task 3's writer change relies on it — otherwise a bad `sort_key` could produce files that `split_by_key` and the isolation predicate silently mis-slice.

- [ ] **Step 1: Write the failing tests** — core unit tests for each rejection (empty, wrong leader, unknown column, missing ts) and the pass case; a catalog test asserting `create_hypertable` rejects `sort_key = ["ts", "tenant_id"]` (wrong leader) with `InvalidTable`; a bootstrap test with a bad `[[tables]]` entry naming the table in the error.
- [ ] **Step 2: Verify they fail** — `cargo test -p ukiel-core --lib sorting` (fn doesn't exist).
- [ ] **Step 3: Implement** (pure fn + the three enforcement points; compile errors surface every `create_hypertable` caller — none need changes, all pass valid keys).
- [ ] **Step 4: Verify** — `cargo test -p ukiel-core --lib && cargo test -p ukiel-catalog -p ukield -- --test-threads=2`; all existing tests pass untouched (every current config satisfies the rules).
- [ ] **Step 5: Lint and commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add crates/ukiel-core crates/ukiel-catalog crates/ukield crates/ukiel-ingest
git commit -m "feat: sort_key invariants validated at registration, bootstrap, and route startup"
```

---

### Task 2: `ukiel-core::sorting` — shared sort/stamp/props (behavior-preserving port)

**Files:**
- Modify: `crates/ukiel-core/src/sorting.rs`, `crates/ukiel-core/Cargo.toml` (add workspace `parquet`)
- Modify: `crates/ukiel-compactor/src/rewrite.rs` (delegate), `src/compactor.rs:330,364` and `src/deletion.rs:64` (unchanged call shapes via delegation)
- Test: core unit tests; compactor tests (existing = regression net; one new metadata assertion)

**Interfaces:**

```rust
/// Canonical sort semantics: ascending, nulls FIRST (arrow/DataFusion
/// default — lexsort_to_indices(None) and the provider's
/// PhysicalSortExpr::new_default already agree; the parquet stamps were the
/// odd ones out at nulls_first: false).
pub fn sort_batch(batch: &RecordBatch, sort_key: &[String]) -> Result<RecordBatch, ArrowError>;
/// SortingColumn metadata matching sort_batch exactly (asc, nulls_first: true),
/// for whichever sort_key names exist in the schema, in order.
pub fn sorting_columns(schema: &Schema, sort_key: &[String]) -> Vec<SortingColumn>;
/// ZSTD + sorting-columns writer props — THE writer props both ingest and
/// compaction use, so compression and ordering metadata cannot drift.
/// (Plan 14 extends this fn with row-group sizing/encodings — one module.)
pub fn sorted_writer_props(schema: &Schema, sort_key: &[String]) -> WriterProperties;
```

- `rewrite::sort_batch` / `rewrite::batch_to_parquet` keep their exact signatures (including `CompactorError`) and delegate — **verified external caller:** the bench binary (`bench/hits.rs:254,263`) must compile unchanged. `batch_to_parquet`'s hand-rolled `SortingColumn` block (`rewrite.rs:163–169`, `nulls_first: false`) is replaced by `sorting_columns`/`sorted_writer_props`.
- Error mapping: core returns arrow-level errors; `rewrite` wraps into `CompactorError` as today.

- [ ] **Step 1: Write the failing tests** — core: `sort_batch` on a 3-column key with a nullable utf8 middle column asserts nulls sort **first**; `sorting_columns` asserts `nulls_first: true` and skips names absent from the schema (in order). Compactor: read back a `batch_to_parquet` file's `SortingColumn`s and assert `nulls_first: true`.
- [ ] **Step 2: Verify they fail**, implement the move + delegation.
- [ ] **Step 3: Verify** — `cargo test -p ukiel-core --lib && cargo test -p ukiel-compactor -- --test-threads=2` (every existing compactor test passes unmodified) and `cargo build --release -p ukiel-e2e --bin bench` (delegation kept the bench compiling).
- [ ] **Step 4: Lint and commit**

```bash
git commit -m "refactor: shared ukiel-core sorting module - one lexsort, one SortingColumn stamp, nulls-first unified"
```

---

### Task 3: Ingest writer sorts the batch by `sort_key`

**Files:**
- Modify: `crates/ukiel-ingest/src/writer.rs`
- Test: writer unit tests (no Docker)

**Interfaces:**
- `rows_to_parquet(schema, packing_key, ts_column, sort_key: &[String], rows)` and `encode_rows(cols, packing_key, ts_column, sort_key: &[String], rows)` — packing key stays (it computes `key_min`/`key_max`), ts stays (the i64 poison validation in `decode_rows` is unchanged; beyond those two, sort columns need **no** JSON-level checks — a missing value decodes to null and sorts deterministically under the canonical options).
- `decode_rows` **stops sorting** (delete `writer.rs:77`); the pipeline becomes decode → `apply_defaults_and_materialized` → `ukiel_core::sorting::sort_batch(sort_key)` → `finish_batch`, so `sort_key` can include defaulted/materialized columns.
- `finish_batch(batch, packing_key, sort_key)` drops the hand-rolled two-column `SortingColumn` block (`writer.rs:119–130`) for `sorted_writer_props`. One batch → one row group, unchanged.
- Safe *now* because Task 1 guarantees the packing key leads.

- [ ] **Step 1: Write the failing tests** (extend the existing writer test module; existing tests pass `&["tenant_id".into(), "ts".into()]`):
  - 3-column `sort_key` (`tenant_id, region, ts`, utf8 middle) with unsorted input → assert physical row order **and** read-back `SortingColumn` metadata (3 columns, `nulls_first: true`);
  - `sort_key` containing a **materialized** column → sorted by computed values (impossible under the old pre-decode sort);
  - nullable sort column → nulls first, metadata agrees;
  - existing `sorts_rows_and_reports_key_range` unchanged except the new arg — same expected order (behavior-preserving for the 2-key case).
- [ ] **Step 2: Verify they fail** — `cargo test -p ukiel-ingest --lib` (signature mismatch).
- [ ] **Step 3: Implement** (compile errors point at every caller; the flusher/consumer call is Task 4's — pass `&[packing_key, ts_column]` temporarily there is NOT allowed; do Task 4's one-line change in the same commit if the crate won't compile otherwise, keeping tasks reviewable but the tree green).
- [ ] **Step 4: Verify** — `cargo test -p ukiel-ingest --lib`.
- [ ] **Step 5: Lint and commit**

```bash
git commit -m "feat: ingest sorts L0 batches by the declared sort_key via the shared sorting module"
```

---

### Task 4: Consumer wires `hypertable.sort_key`; component + e2e verification

**Files:**
- Modify: `crates/ukiel-ingest/src/consumer.rs` (`encode_rows` call, `consumer.rs:333`)
- Test: `crates/ukiel-ingest/tests/consumer_test.rs`; `crates/ukiel-query/tests/query_test.rs`; compactor deletion test

**Interfaces:**
- The flush path passes `&hypertable.sort_key` (already resolved next to `packing_key` at the call site). All existing component tests create tables with `sort_key == [packing, ts]` → identical files, tests green untouched.
- New tests:
  - component (ingest→query): a table with 3-column `sort_key` including a utf8 column — produce unsorted, flush, `ORDER BY` the full sort key through the session and assert order **with the sort physically elided** semantics guarded by correct results (extends the `order_by_sort_key_results_stay_correct_and_ordered` pattern at `query_test.rs:693` to L0 files written by the *new* writer — this is the end-to-end guard on DataFusion's sort-elision path, the exact bug scenario);
  - deletion-path sortedness (gap noted in the 07-06 audit): after `delete_key` rewrites a packed file, read the output back and assert it is still physically sorted by `sort_key`.

- [ ] **Step 1: Write the new tests, verify the component suites** — `cargo test -p ukiel-ingest -p ukiel-query -p ukiel-compactor -- --test-threads=2`.
- [ ] **Step 2: Full workspace** — `cargo fmt --check && cargo clippy --all-targets -- -D warnings && make test`; `make e2e` (S0–S8 green — ingest output for every scenario config is byte-order-identical to before).
- [ ] **Step 3: Lint and commit**

```bash
git commit -m "feat: consumer passes declared sort_key; e2e guards on sort-elision and deletion-path order"
```

---

### Task 5: Perf check + docs & status flips

**Files:**
- Modify: `docs/notes/2026-07-06-parquet-read-performance.md` (perf-smoke medians)
- Modify: `docs/notes/2026-07-07-ingest-sort-by-sort-key.md` (status → executed, point at this plan)
- Modify: `docs/superpowers/plans/2026-07-05-ukiel-v1-roadmap.md` (row 27 → Executed)
- Modify: `docs/superpowers/specs/2026-07-05-ukiel-design.md` ("Not yet" list; the sort-key gap leaves "Known gaps")

- [ ] **Step 1: Perf check** — run the perf smoke (`cargo test -p ukiel-query --test perf_smoke -- --ignored --nocapture`) and record medians in the perf note: columnar lexsort should be ≤ the old serde_json comparator sort. The bench fixtures are unaffected (both use `[key, ts]` sort keys — no re-run needed; note it).
- [ ] **Step 2: Docs flips** — note status, roadmap row 27 **Executed** (list what landed: validation choke points, `ukiel-core::sorting`, nulls-first unification, batch-level sort), design doc.
- [ ] **Step 3: Commit**

```bash
git add docs/
git commit -m "docs: mark plan 27 executed - ingest sorts by declared sort_key, shared sorting module"
```

---

## Compat / rollout (from the note, re-verified)

- Every existing table has `sort_key == [packing_key, ts_column]` with non-nullable i64 columns → already-written L0 files satisfy the new ordering; **no rewrite, no migration**. The `nulls_first` metadata flip affects only future files with nullable sort columns (none exist).
- Changing an existing table's `sort_key` remains unsupported (would require full recompaction of live parts); registration-time validation only. A `sort_key` migration story is explicitly out of scope.

## Self-review notes

- **Coverage vs the note:** all six note steps land (validation → shared module → writer → consumer → tests incl. the elision e2e and deletion-path sortedness → perf check + docs). Nothing deferred except the explicitly out-of-scope `sort_key` migration.
- **Interfaces re-verified 2026-07-09** (the note's citations had drifted): the hardcoded sort is `writer.rs:77` (`decode_rows`) with the stamp at `writer.rs:119–130`; the consumer call is `consumer.rs:333`; `sort_batch`/`batch_to_parquet` live at `rewrite.rs:58/159` with call sites `compactor.rs:330,364`, `deletion.rs:64`, **plus the bench binary (`bench/hits.rs:254,263`) — new since the note**, which is why `rewrite` delegates instead of moving; the catalog fn is `create_hypertable` (`tables.rs:19`), not the note's `insert_hypertable`; the e2e regression net is `order_by_sort_key_results_stay_correct_and_ordered` (`query_test.rs:693`).
- **Sequencing argument (from the note, preserved):** validation lands before the writer trusts the invariant; the shared-module port is independently green (no ingest change in Task 2); the writer change lands on a guaranteed invariant. Task 3/4 may share a commit if the crate cannot compile split — noted inline rather than discovered mid-execution.
- **Nulls decision:** asc/nulls-first because two of three layers (arrow lexsort default, provider `new_default`) already say so — the parquet stamps flip, not the sort. No custom `SortOptions` anywhere.
- **Safety argument:** the gap is *latent* (no current config trips it), so every task is behavior-preserving for existing data; the one semantic change (batch-level sort after materialization) is strictly more correct and covered by a test that was impossible before (materialized column in `sort_key`). Plan 14 gate honored: `sorted_writer_props` is the single extension point it will build on.

# Ukiel Plan 14: Parquet File Layout & Encoding (Tier-3 write-side) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

> **Re-authored 2026-07-09** against post-17/27/12/13/28 interfaces (the 2026-07-06 original predated all of them). What changed: the layout policy now **extends `ukiel_core::sorting`** (the module plan 27 birthed — "one module, not two", per the roadmap-order rationale) instead of creating a parallel `parquet_props` module; the nulls convention in stamps is `nulls_first: true` (plan 27 unified it — the original's tests asserted `false`); the compactor call site is `merge_parts` over `plan_chunks` output with 3-variant placement (plan 17), so key alignment is decided **per chunk** (`key_min != key_max`) rather than per placement enum; `EncodedPart` carries `column_stats` (plan 12 — must be preserved); the row-group builder method in parquet 58.3 is `set_max_row_group_row_count(Some(n))`; and `rewrite::batch_to_parquet(batch, sort_key)` keeps its exact signature because the bench binary (`bench/hits.rs:263`) calls it.

**Goal:** Tier 3 of `docs/notes/2026-07-06-parquet-read-performance.md` (items #7–#10): (7) rewrite-output row groups aligned to packing-key runs so row-group key stats become perfectly selective for the isolation predicate, (8) row-group size discipline (128k-row cap on every writer), (9) `DELTA_BINARY_PACKED` for the sorted ts column plus per-level compression (L0 = LZ4_RAW for fast decode of short-lived files, L1+ = ZSTD), (10) opt-in per-column bloom filters declared in the table schema. The 10 GB-tier baseline (`bench/README.md` §6) is the before-measurement; heavy-tenant scans (`top_urls` 363 ms over 8.5M rows) are the workload #7–#9 target.

**Architecture:** All layout policy lands in `ukiel-core::sorting` as `WriteOpts` + `writer_props(schema, sort_key, &WriteOpts)`; the existing `sorted_writer_props(schema, sort_key)` (plan 27) becomes a thin delegate (`WriteOpts::for_level(1)`), so every current caller — including the bench — compiles unchanged and silently gains the row-group cap. `WriteOpts::from_columns(cols, sort_key, level)` derives the ts column (first `sort_key` entry with `ukiel_type == "timestamp_ms"`) and bloom columns from the schema. Key alignment is `rewrite::batch_to_parquet_key_aligned`: per-key slices from the existing `split_by_key` stream into ONE `ArrowWriter`, `flush()`ing (= ending the row group) before any key run that would overflow the cap — tiny keys share row groups, a key bigger than the cap spans several. It applies to any multi-key output chunk: packed placement and `SizeTargeted` shared chunks alike (single-key chunks — separated placement, whale chunks — need no alignment by construction). Two invariants survive every task: **readers are layout-agnostic** (every existing test stays green — the behavioral safety net), and **layout claims are verified by reading footers back** (row-group counts, per-group key stats, encodings, compression, bloom offsets).

**Tech Stack:** arrow + parquet 58.3 (pinned — never bump independently). Verified against the workspace: `set_max_row_group_row_count(Some(n))` (`perf_smoke.rs:148` uses it), `parquet::file::metadata::SortingColumn` (the path `ukiel_core::sorting` already imports), `ColumnPath`, `set_column_encoding` / `set_column_dictionary_enabled` / `set_column_bloom_filter_enabled`, `ArrowWriter::{flush, in_progress_rows}`, and the metadata readers (`ColumnChunkMetaData::{compression, encodings, dictionary_page_offset, bloom_filter_offset, statistics}`, `RowGroupMetaData::{num_rows, sorting_columns}`). On any name drift, check docs.rs for the pinned version and adapt minimally. `Compression::LZ4_RAW` and bloom writing are default parquet features; if a feature error appears, add the named feature to the workspace `parquet` dep — do not bump.

**Prerequisites:** Plan 27 executed (this extends its `sorting` module and inherits its nulls-first stamps and validation guarantee); plan 17 executed (`plan_chunks` shapes the outputs being aligned). Plan 29 (streaming merge) restructures `merge_parts` next — this plan's writer entry points (`writer_props`, `batch_to_parquet_key_aligned`) are exactly what 29's streaming writer consumes chunk-by-chunk, so land them here once.

## Global Constraints

- Rust edition 2024, toolchain ≥ 1.96; workspace dep pins per the roadmap.
- **Every existing ingest, compactor, query, e2e, and bench-binary build stays green after every task.** Readers are layout-agnostic; a red reader test means broken data, not layout.
- Unit tests (`cargo test -p <crate> --lib`) pass without Docker; component tests via testcontainers.
- Preserve plan-27 semantics exactly: stamps stay `nulls_first: true` via the existing `sorting_columns` helper — this plan adds properties around it, never re-implements it.
- Commit messages: conventional style, no Claude/AI attribution.
- Every task ends with `cargo fmt && cargo clippy --all-targets -- -D warnings` clean.

---

### Task 1: `WriteOpts` layout policy in `ukiel-core::sorting` + `bloom_filter` schema attribute

**Files:**
- Modify: `crates/ukiel-core/src/sorting.rs`, `src/schema.rs`, `src/lib.rs` (re-exports)
- Test: core unit tests (no Docker)

**Interfaces:**

```rust
// schema.rs
pub struct ColumnSpec { /* existing fields */ pub bloom_filter: bool }
// JSON attr `"bloom_filter": true`, default false; rejected on alias columns
// (never stored) with SchemaError::Invalid.
impl TableColumns {
    /// Stored columns flagged for bloom filters, in schema order.
    pub fn bloom_columns(&self) -> Vec<String>;
}

// sorting.rs — the layout half (plan 14), alongside plan 27's ordering half.
pub const DEFAULT_MAX_ROW_GROUP_ROWS: usize = 128 * 1024; // Tier-3 #8

#[derive(Debug, Clone)]
pub struct WriteOpts {
    /// 0 → LZ4_RAW (short-lived, decode-fast); >= 1 → ZSTD (read-many).
    pub level: i16,
    /// DELTA_BINARY_PACKED with dictionary off — sorted epoch-ms delta-packs
    /// spectacularly; a dictionary on a near-unique column only adds
    /// indirection.
    pub ts_column: Option<String>,
    pub bloom_columns: Vec<String>,
    pub max_row_group_rows: usize,
}
impl WriteOpts {
    pub fn for_level(level: i16) -> Self; // no hints, default cap
    /// Derives ts (first sort_key entry with ukiel_type == "timestamp_ms")
    /// and blooms from the table schema.
    pub fn from_columns(cols: &TableColumns, sort_key: &[String], level: i16) -> Self;
}

/// THE writer properties: compression by level, sort stamps via the
/// plan-27 `sorting_columns` helper (nulls_first: true — do not re-implement),
/// delta-packed ts, opt-in blooms, row-group cap.
pub fn writer_props(schema: &Schema, sort_key: &[String], opts: &WriteOpts) -> WriterProperties;

/// Plan-27 signature, now a delegate: writer_props(schema, sort_key,
/// &WriteOpts::for_level(1)). Existing callers (rewrite, bench) compile
/// unchanged and gain the row-group cap.
pub fn sorted_writer_props(schema: &Schema, sort_key: &[String]) -> WriterProperties;
```

- [ ] **Step 1: Write the failing tests**
  - schema: `bloom_filter` parses, defaults false, orthogonal to `default`/`materialized`, **rejected on alias**; `bloom_columns()` order.
  - sorting (footer read-back over a small 3-column file, `SerializedFileReader`): level 0 → every chunk `LZ4_RAW`, level 1 → `ZSTD`; ts column has `DELTA_BINARY_PACKED` and no `dictionary_page_offset`, other columns keep dictionaries; `max_row_group_rows: 4` over 10 rows → groups `[4, 4, 2]`; stamps present with **`nulls_first: true`** (the plan-27 convention — assert it explicitly so a regression here is loud); blooms present exactly where asked.
- [ ] **Step 2: Verify they fail** — `cargo test -p ukiel-core --lib` (fields/fns don't exist).
- [ ] **Step 3: Implement** (builder: `set_max_row_group_row_count(Some(opts.max_row_group_rows))`; compression by level; `set_sorting_columns(Some(sorting_columns(schema, sort_key)))` when non-empty; `ColumnPath`-keyed encoding/dictionary/bloom overrides).
- [ ] **Step 4: Verify** — `cargo test -p ukiel-core --lib && cargo build --workspace` (existing `sorted_writer_props` callers compile; `ColumnSpec` is constructed only inside `schema.rs`).
- [ ] **Step 5: Lint and commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add crates/ukiel-core
git commit -m "feat: write-side layout policy in ukiel-core sorting - WriteOpts, per-level compression, blooms"
```

---

### Task 2: Ingest L0 uses the layout (LZ4_RAW, delta ts, blooms)

**Files:**
- Modify: `crates/ukiel-ingest/src/writer.rs`
- Test: writer unit tests (no Docker)

**Interfaces:**
- `finish_batch(batch, packing_key, sort_key, opts: WriteOpts)`; `rows_to_parquet` builds `WriteOpts { level: 0, ts_column: Some(ts_column), bloom_columns: vec![], .. }`, `encode_rows` builds `WriteOpts::from_columns(cols, sort_key, 0)`. Public signatures of `rows_to_parquet`/`encode_rows` unchanged (they already take `ts_column` and `sort_key` post-27).
- **`EncodedPart.column_stats` is preserved** (plan 12) — the props swap touches only the `WriterProperties` construction inside `finish_batch`, nothing else.

- [ ] **Step 1: Write the failing footer test** — `encode_rows` with a `bloom_filter: true` utf8 column: every chunk `LZ4_RAW`; ts delta-packed, dictionary off; stamps = full `sort_key` with `nulls_first: true`; bloom offset only on the flagged column; and `column_stats` still populated (the plan-12 regression the original plan would have missed).
- [ ] **Step 2: Verify it fails** — `cargo test -p ukiel-ingest --lib` (compression assertion: current L0 is ZSTD).
- [ ] **Step 3: Implement** — replace `finish_batch`'s props construction with `ukiel_core::sorting::writer_props(&batch.schema(), sort_key, &opts)`; delete the local ZSTD/stamp code.
- [ ] **Step 4: Verify** — `cargo test -p ukiel-ingest --lib && cargo test -p ukiel-compactor --lib` (compactor unit tests read L0 fixtures written by this writer — LZ4_RAW decodes transparently).
- [ ] **Step 5: Lint and commit**

```bash
git commit -m "feat: ingest L0 layout - lz4_raw, delta-packed ts, opt-in blooms via shared WriteOpts"
```

---

### Task 3: Compactor — key-aligned row groups on multi-key chunks

**Files:**
- Modify: `crates/ukiel-compactor/src/rewrite.rs`, `src/compactor.rs` (`merge_parts`, `compactor.rs:364`), `src/deletion.rs` (`deletion.rs:64`)
- Test: rewrite unit tests; existing component suites as the net

**Interfaces:**
- `rewrite::batch_to_parquet(batch, sort_key)` — **signature unchanged** (verified external caller: `bench/hits.rs:263`); body now delegates through `sorted_writer_props` as today, which already gained the cap in Task 1. Rewrites are always L1+ → ZSTD, so its behavior is exactly today's + row-group discipline.
- New: `rewrite::batch_to_parquet_opts(batch, sort_key, opts: &WriteOpts)` — plain write with explicit opts (level, blooms, ts).
- New: `rewrite::batch_to_parquet_key_aligned(batch, packing_key, sort_key, opts) -> Result<Vec<u8>, CompactorError>` — one file, row groups ending on key boundaries: stream `split_by_key` slices into one `ArrowWriter`, and when `in_progress_rows() > 0 && in_progress + slice_rows > opts.max_row_group_rows`, `flush()` first. A key bigger than the cap spans several groups (ArrowWriter splits at the cap on its own); tiny keys share groups; consecutive groups' key ranges never interleave (input is key-sorted — plan 27's guarantee).
- `merge_parts` (`compactor.rs:364`): `opts = WriteOpts::from_columns(&cols, &hypertable.sort_key, output_level)`; per chunk from `plan_chunks`: **`key_min != key_max` → key-aligned, else plain** — one rule covering packed whole-partition chunks and `SizeTargeted` shared chunks, while separated/whale single-key chunks skip alignment by construction (this is the roadmap row's "chunks with `key_min != key_max`" contract).
- `delete_key` (`deletion.rs:64`): rewritten packed file → key-aligned at `part.meta.level` (filtering preserves the sort; an L0 rewrite becomes LZ4_RAW per policy — consistent, short-lived either way).

- [ ] **Step 1: Write the failing layout test** — key runs `(1×3, 2×3, 3×6, 4×1)`, cap 4: expect row groups `[3, 3, 4, 3]` with per-group key stats `[(1,1), (2,2), (3,3), (3,4)]` (k1/k2 flush on their boundaries, k3 exceeds the cap alone and spans, k4 rides the tail), no interleaving between consecutive groups; plus ZSTD + delta-ts + `nulls_first: true` stamps from the shared opts.
- [ ] **Step 2: Verify it fails** — `cargo test -p ukiel-compactor --lib` (fns don't exist).
- [ ] **Step 3: Implement** rewrite fns + the two call sites.
- [ ] **Step 4: Verify** — `cargo test -p ukiel-compactor --lib`; then component: `cargo test -p ukiel-compactor -p ukiel-query -- --test-threads=2` (merges, races, deletion, isolation — readers layout-agnostic); `cargo build --release -p ukiel-e2e --bin bench` (signature kept).
- [ ] **Step 5: Lint and commit**

```bash
git commit -m "feat: key-aligned row groups on multi-key rewrite chunks; shared WriteOpts in compactor"
```

---

### Task 4: Full verification, perf check, docs & status flips

**Files:**
- Modify: `docs/notes/2026-07-06-parquet-read-performance.md`, `docs/superpowers/plans/2026-07-05-ukiel-v1-roadmap.md` (row 14 → Executed), `docs/superpowers/specs/2026-07-05-ukiel-design.md` ("Not yet" list)

- [ ] **Step 1: Full verification** — `cargo fmt --check && cargo clippy --all-targets -- -D warnings && make test`; `make e2e` (write-path changes touch every scenario's data — S6 compaction-equivalence and S8 GC-hygiene are the nets).
- [ ] **Step 2: Perf check** — run the perf smoke (`cargo test -p ukiel-query --test perf_smoke -- --ignored --nocapture`), record medians in the perf note. Note in the bench README that the 10 GB-tier read baseline predates plan 14's layout (its fixture files are pre-14; a reloaded fixture measures the new layout — a follow-up data-collection run, not part of this plan; row 16 revalidates the population anyway).
- [ ] **Step 3: Docs flips** — perf note Tier-3 items #7–10 marked done with where each lives (`ukiel_core::sorting::{WriteOpts, writer_props}`, `rewrite::batch_to_parquet_key_aligned`, `"bloom_filter"` schema attr); roadmap row 14 → **Executed**; design-doc "Not yet" list updated.
- [ ] **Step 4: Commit**

```bash
git add docs/
git commit -m "docs: record tier-3 parquet layout status, roadmap row 14 executed"
```

---

## Self-review notes

- **Coverage:** #7 → Task 3 (per-chunk `key_min != key_max` rule — covers plan-17's `SizeTargeted`, which the original plan predated); #8 → Task 1 (`DEFAULT_MAX_ROW_GROUP_ROWS` flows through `sorted_writer_props`, so every writer gets it, bench loader included); #9 → Tasks 1–3 (level in `WriteOpts`; only ingest writes L0, so `batch_to_parquet`'s unchanged signature loses nothing); #10 → Tasks 1–2 (attr + `from_columns`; compactor picks blooms up via `from_columns` in Task 3).
- **Interfaces re-verified 2026-07-09:** `sorting_columns`/`sorted_writer_props` exist with `nulls_first: true` (`sorting.rs:66–80`); the merge call site is `merge_parts` at `compactor.rs:364` over `plan_chunks` output; deletion at `deletion.rs:64` keeps `part.meta.level`; `EncodedPart.column_stats` exists (plan 12) and Task 2 asserts it survives; the row-group method is `set_max_row_group_row_count` (verified at `perf_smoke.rs:148`); the bench calls `rewrite::{sort_batch, batch_to_parquet}` (`hits.rs:254,263`) — both signatures preserved, and Task 3 builds the bench to prove it.
- **One module, not two:** everything extends `ukiel_core::sorting`; the stamps come from plan 27's `sorting_columns` helper — never re-implemented, so the nulls convention cannot fork again. Plan 29's streaming writer takes `writer_props` + the key-aligned flush rule as its building blocks — declared in Prerequisites so 29's author knows what to consume.
- **Semantics reviewed:** the alignment flush rule only ever *ends* row groups early (never mid-key unless the key alone exceeds the cap — then ArrowWriter's own split applies), so files stay sorted and stats stay honest; `sorted_writer_props` silently gaining the cap is deliberate (#8 "everywhere") and behavior-preserving for tests (all fixtures ≪ 128k rows); deletion rewrites at L0 flipping to LZ4_RAW is policy-consistent, called out rather than hidden.
- **Measurement honesty:** the recorded 10 GB baseline is pre-14 layout; the plan does not claim a re-measured win — it notes the follow-up run explicitly.

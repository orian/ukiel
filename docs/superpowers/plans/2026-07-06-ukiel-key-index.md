# Ukiel Plan 16: Key Index (Tier-2 #5 bitmap + #6 catalog access plans) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

> Re-authored 2026-07-13 against the current tree. The original (2026-07-06) predated plans 13/14/15/17/27/29 and spliced into a write path that no longer exists — its "change `batch_to_parquet_key_aligned` to return `(bytes, spans)`" assumed the buffered writer plan 29 replaced with `StreamingChunkWriter`, and its provider sketch used a `packing_key_value` field that is now `slice: Option<i64>` (plan 32's operator mode). This version splices into the streaming writer via row-group *metadata* (no manual flush tracking), scopes every index to the scoped-slice path, and re-prices deliverable (b) honestly against the plan-13 footer cache.

**Goal:** Tier-2 items #5 and #6 of `docs/notes/2026-07-06-parquet-read-performance.md`:

- **(a) Roaring bitmap of distinct packing keys** per multi-key part → catalog part pruning becomes *exact*. Min/max ranges false-positive on sparse keys (range 100–4500 "contains" a tenant with zero rows); the gap-attribution note measured what that costs at fan-out: raw DataFusion pays a flat ~70 ms floor opening 1,102 footers on tiny queries while ukiel's catalog pruning runs the same queries 3–4.5× faster — the bitmap extends that advantage to the sparse-tenant case, where the headline is **zero object-store requests** for a tenant whose key falls inside ranges but owns no rows.
- **(b) Catalog-stored row-group key spans** → the provider hands DataFusion a per-file `ParquetAccessPlan` that pre-skips row groups. Honest post-13 framing: the footer cache already makes *repeat* footer reads free, so (b)'s remaining value is the cold path (first touch per file per process), skipped pruning work per query, and row-group selection that composes with pushdown (DataFusion reduces a provided plan further, never widens it). It is kept because the write side gets the spans for free from row-group metadata, but (a) is the headline and (b) must not complicate it.

**Architecture:** Both indices live inside the existing `parts.column_stats` JSONB — **no migration** (`ls crates/ukiel-catalog/migrations/` stays at 0007). Keys:

- `"packing_keys"`: base64(RoaringTreemap bytes) of the distinct packing-key values. **Treemap, not Bitmap** — packing keys are `i64` and production tenant ids will not fit u32; `RoaringTreemap` covers u64, and any negative key skips the bitmap entirely (absence degrades to "keep"). Written only when distinct-key count ≤ `MAX_BITMAP_KEYS = 10_000` (beyond that, min/max is dense enough that the bitmap buys little and the blob gets big) and only for **multi-key parts** (`key_min != key_max`) — dedicated files and plan-29 whale slices are already exact under range pruning.
- `"key_row_groups"`: `[[min, max], …]` per row group in file order, derived from the writer's own `RowGroupMetaData` (the plan-14 key-aligned flush rule makes consecutive spans non-interleaving). Same multi-key-only rule.

Write side touches all three writers where they already build `column_stats`: ingest (`writer.rs:122`, sync `ArrowWriter`), compactor streaming merge (`rewrite.rs` `OpenFile`, `AsyncArrowWriter` — plan 29), deletion rewrite (`deletion.rs:60–95`, via `batch_to_parquet_key_aligned`). Read side is one splice in `provider.rs::scan` after `live_parts_pruned` (`provider.rs:179–187`): bitmap filter first (drop parts that provably exclude the slice key), then `PartitionedFile::with_extension(access_plan)` for survivors that carry spans. **Both apply only when `self.slice = Some(key)`** — the unscoped operator path has no key to test.

Degradation rules (the invariant every task's tests re-assert): **absence or garbage is never an error and never drops data** — missing bitmap → keep the part; undecodable bitmap → keep; missing spans → no access plan → footer path as today; a `Skip` decision requires *proven* non-overlap. The `FilterExec` isolation backstop stays above the scan regardless, so an over-broad plan costs performance only; an over-narrow one is prevented by construction.

**Tech Stack:** `roaring` + `base64` (workspace-pinned to the versions `cargo search <crate> --limit 1` reports at execution time). DataFusion 54's external-index mechanism, verified in the vendored source: `ParquetAccessPlan`/`RowGroupAccess` re-exported at `datafusion-datasource-parquet-54.0.0/src/mod.rs:47`, attached via `PartitionedFile::new(path, size).with_extension(access_plan)` (worked example at `source.rs:195–241`; the opener reads `partitioned_file.extensions` and *reduces* the provided plan with predicate/statistics pruning — it never widens it). Row-group spans come from `parquet 58.3`'s `ArrowWriter::flushed_row_groups() -> &[RowGroupMetaData]` (sync; `async_writer/mod.rs:187` delegates to it) — call before `close()`; column-chunk statistics of the packing-key column give each group's `(min, max)`.

**Prerequisites:** Plans 11, 12, 13, 14, 15, 17, 27, 29, 32 executed (all are — this re-author verified the splice points against that tree). Notable interactions verified: plan 15's cache sits below the reader factory, so access plans compose with it untouched; plan 32's `slice: Option<i64>` gates all new pruning to scoped mode; the provider's predicate-gated ordering declaration (gap-note fix) is orthogonal — this plan does not touch `FileScanConfigBuilder` ordering.

## Global Constraints

- Rust edition 2024, toolchain ≥ 1.96; datafusion 54 / arrow+parquet 58.3 / object_store 0.13 pinned — adapt minimally on docs.rs API drift (plan-11 convention), and if `with_extension` differs in the pinned patch release, keep the semantic: per-file row-group preselection supplied by the provider.
- Component tests via `make test` (`--test-threads=2`). Bitmap/span encoding tests are pure (`cargo test -p ukiel-core --lib`, no Docker).
- **Every existing test stays green after every task.** The isolation tests (`crates/ukiel-query/tests/query_test.rs`) are the regression net — existing fixtures write parts with `column_stats: None` or stats lacking the new keys, and the degradation rules guarantee those parts keep being read.
- Conventional commits, no Claude/AI attribution; `cargo fmt && cargo clippy --all-targets -- -D warnings` per task.

---

### Task 1: Key-index encoding in ukiel-core

**Files:**
- Modify: `Cargo.toml` (workspace deps: `roaring`, `base64`, pinned per `cargo search`)
- Modify: `crates/ukiel-core/Cargo.toml`
- Modify: `crates/ukiel-core/src/stats.rs`

**Interfaces (Produces, in `ukiel_core::stats`):**
- `pub const PACKING_KEYS_STAT: &str = "packing_keys";`, `pub const KEY_ROW_GROUPS_STAT: &str = "key_row_groups";`, `pub const MAX_BITMAP_KEYS: usize = 10_000;`
- `pub struct KeyBitmapAccumulator` — fold-in style matching `Int64StatsAccumulator` (`stats.rs:38`): `update(&mut self, batch: &RecordBatch, packing_key: &str)` inserts distinct keys (a negative key or a missing/non-Int64 column poisons the accumulator → `finish` returns `None`); `finish(self) -> Option<String>` returns base64(treemap bytes), `None` when poisoned or distinct count > `MAX_BITMAP_KEYS`. Whole-batch-vs-slices equivalence holds by construction (set semantics).
- `pub fn key_bitmap(batch: &RecordBatch, packing_key: &str) -> Option<String>` — one-shot wrapper (accumulate one batch, finish); the ingest/deletion call shape.
- `pub fn bitmap_contains(encoded: &str, key: i64) -> Option<bool>` — `None` on decode failure or negative key (caller keeps the part).
- `pub fn key_row_groups(row_groups: &[parquet::file::metadata::RowGroupMetaData], packing_key: &str) -> Option<serde_json::Value>` — per-group `[min, max]` from the packing-key column chunk's Int64 statistics; `None` if any group lacks them (all-or-nothing: a partial span list would mis-index groups). This is the one helper both the sync and streaming writers share. (`parquet` is already a `ukiel-core` dependency via the sorting module.)

- [ ] **Step 1: Write the failing tests** in `stats.rs`'s test module — round trip (`bitmap_contains` true for present keys, **false for an in-range absent key** — the false-positive killer), cap (> `MAX_BITMAP_KEYS` distinct → `None`), poison (negative key, missing column), garbage tolerance (`bitmap_contains("!!!", 100) == None`), accumulator≡one-shot equivalence, and `key_row_groups` against a real two-row-group file written with `sorted_writer_props` (write via `ArrowWriter`, read back `flushed_row_groups()` pre-close).
- [ ] **Step 2: Verify they fail** — `cargo test -p ukiel-core --lib key_` (compile error: names don't exist).
- [ ] **Step 3: Implement** — `RoaringTreemap::insert(key as u64)`, `serialize_into` a `Vec`, base64 STANDARD; decode failures all map to `None`. Match the file's comment style: one sentence on why Treemap (i64 domain) and why the cap exists.
- [ ] **Step 4: Verify pass**, **Step 5: lint and commit**

```bash
git commit -m "feat: roaring key bitmap + row-group key spans in ukiel-core stats"
```

---

### Task 2: Writers populate the key indices

**Files:**
- Modify: `crates/ukiel-ingest/src/writer.rs` (bitmap only — L0 files are small, short-lived, and single-row-group in practice; spans buy nothing there)
- Modify: `crates/ukiel-compactor/src/rewrite.rs` (streaming writer: bitmap + spans; sync key-aligned writer: return spans)
- Modify: `crates/ukiel-compactor/src/deletion.rs` (bitmap + spans on rewritten parts)
- Test: the crates' existing test modules/files

**Interfaces:**
- Ingest (`writer.rs:122`): after `int64_column_stats(&batch)`, merge `PACKING_KEYS_STAT: key_bitmap(&batch, packing_key)` into the JSON when `Some` **and** `key_min != key_max` (single-key L0 flushes stay lean). Merge = insert into the `Object`; when `column_stats` was `None` but a bitmap exists, build a one-key object.
- Streaming writer (`rewrite.rs`): `OpenFile` gains `keys: KeyBitmapAccumulator` fed in `write_slice` (next to `stats.update`, `rewrite.rs:298`); `OpenFile::close` (`rewrite.rs:308`) calls `self.writer.flushed_row_groups()` **before** `writer.close().await?` to derive spans via `key_row_groups(...)`, then merges both keys into `ChunkOutput.column_stats` — multi-key files only (`key_min != key_max`); dedicated/whale files (`min == max`, including every `Separated` output and plan-29 ts-slices) get neither. Note at the decision site: range pruning is already exact for them, and whale slices would blow the cap anyway.
- Sync key-aligned writer: `batch_to_parquet_key_aligned` (`rewrite.rs:240`) returns `(Vec<u8>, Option<serde_json::Value>)` — bytes plus spans from `writer.flushed_row_groups()` pre-close. Its two callers adapt: deletion (`deletion.rs:68`) merges spans + `key_bitmap(&kept, …)` into the `PartMeta.column_stats` it already builds (`deletion.rs:93`), same multi-key gate; the `rewrite.rs` unit tests destructure the tuple.
- Tests assert, per writer: a multi-key output's stats carry a bitmap that `bitmap_contains` confirms (present key true / absent in-range key false) and spans that **match the file's own footer** (read the bytes back with `ParquetRecordBatchReaderBuilder` and compare per-row-group min/max — the existing footer read-back tests in `rewrite.rs` are the pattern to extend); a deletion rewrite's bitmap no longer contains the deleted key; a single-key output carries neither stat.

- [ ] **Step 1: failing tests** (one per writer, extending each file's existing fixtures — adapt to local helper names).
- [ ] **Step 2: verify fail. Step 3: implement. Step 4: `cargo test -p ukiel-ingest -p ukiel-compactor -- --test-threads=2`. Step 5: lint and commit**

```bash
git commit -m "feat: writers record packing-key bitmaps and row-group key spans"
```

---

### Task 3: Provider — exact part pruning via bitmap

**Files:**
- Modify: `crates/ukiel-query/src/provider.rs`
- Test: `crates/ukiel-query/tests/query_test.rs`

**Interfaces:**
- In `scan()`, immediately after `live_parts_pruned` (`provider.rs:179–187`) and only when `let Some(key) = self.slice`: retain a part unless its stats carry a `PACKING_KEYS_STAT` string for which `bitmap_contains(encoded, key) == Some(false)` — every other outcome (no stats, no key, `None` from decode) keeps the part. One comment at the site: *exactness is the bitmap's job; safety is the keep-by-default rule plus the FilterExec backstop.*
- Test (`query_test.rs`, reusing the `setup()` harness at its current shape): commit a packed part whose `column_stats` carries a real bitmap for keys {1, 2} (build with `ukiel_core::stats::key_bitmap`), plus `packing_key_min: 1, packing_key_max: 4500`; a session for namespace 300 — inside the range, absent from the bitmap — plans **zero parquet files** (EXPLAIN contains no `.parquet`) and returns zero rows; namespaces 1 and 2 still see exactly their rows (isolation re-asserted next to the new path). A part with `column_stats: None` under the same query still scans (degradation pin).

- [ ] **Steps: failing test → verify fail → implement → `cargo test -p ukiel-query --test query_test` → lint and commit**

```bash
git commit -m "feat: exact part pruning via packing-key bitmaps"
```

---

### Task 4: Provider — catalog-driven ParquetAccessPlan

**Files:**
- Modify: `crates/ukiel-query/src/provider.rs`
- Test: `crates/ukiel-query/tests/query_test.rs`

**Interfaces:**
- Still under `Some(key)`: for each surviving part whose stats carry `KEY_ROW_GROUPS_STAT`, parse `[[min,max],…]`; build `ParquetAccessPlan::new_all(n)` and `skip(i)` every group whose span provably excludes `key`; attach via `.with_extension(plan)` on the `PartitionedFile` (`provider.rs:184–187` construction site; mechanism verified — see Tech Stack). Malformed spans (wrong arity, non-i64) → no plan, footer path. **Skip-all edge:** if every group is skipped, drop the file from the scan entirely rather than attaching an empty plan (cheaper, and `ParquetAccessPlan` semantics stay trivially sound).
- Test: write a two-key file through `batch_to_parquet_key_aligned` (keys in separate row groups — the Task-2 fixture), commit it with the produced spans; query one key with `EXPLAIN ANALYZE` and assert the other key's row group was never considered — expected observable: `row_groups_pruned_statistics=1 total → …` *changes to a smaller "total"* (the access plan removes the group before statistics pruning runs; the existing `pushdown_prunes_row_groups_in_packed_files` test shows the metric-string idiom). If DF 54's metrics cannot distinguish plan-skipped from stats-pruned groups, fall back to asserting correctness plus zero-file planning for a spans-say-absent key, and record the observability gap in the perf note (the honest-fallback convention from the original plan stands).

- [ ] **Steps: failing test → verify fail → implement → `cargo test -p ukiel-query --test query_test` → lint and commit**

```bash
git commit -m "feat: catalog-driven parquet access plans pre-skip row groups"
```

---

### Task 5: Measure, record, status flips

**Files:**
- Modify: `docs/notes/2026-07-06-parquet-read-performance.md` (Tier-2 #5/#6 → done, with the post-13 framing from this plan's Goal)
- Modify: `docs/notes/2026-07-12-official-suite-gap.md` (one line under the fix list: bitmap exactness shipped — the q38–q43-class advantage now covers sparse tenants)
- Modify: `docs/superpowers/plans/2026-07-05-ukiel-v1-roadmap.md` (row 16 → **Executed**, list what landed)

No ukield config: `MAX_BITMAP_KEYS` is a documented constant, not a knob (three writers would need it threaded; no deployment has asked; the constant's doc says where to promote it if one does). No monitoring-spec rows: no new metrics (part pruning is already visible via `query_parts_scanned/pruned`).

- [ ] **Step 1: Full-workspace check** — `cargo fmt --check && cargo clippy --all-targets -- -D warnings && make test`; run `make e2e` (S6/S7 exercise compaction + key deletion whose outputs now carry the new stats; S8 is untouched — no storage-lifecycle change, stats ride existing commits).
- [ ] **Step 2: Measure** — `bench hits queries` against the loaded fixture vs the latest recorded label, plus the Task-3 scenario headline: a sparse in-range namespace goes from opening files to **zero object-store requests**. Record both in the perf note's results section (profile stated, per the bench README rule).
- [ ] **Step 3: Docs + roadmap flips, commit**

```bash
git commit -m "docs: key index results - exact sparse-tenant pruning, Tier-2 5/6 closed"
```

---

## Self-review notes

**Coverage mapping.** Perf-note Tier-2 #5 (exact pruning) → Tasks 1–3; #6 (catalog access plans) → Tasks 1 (spans encoding), 2 (spans written), 4 (plans attached); measurement/close-out → Task 5. The roadmap row's "revalidate splice points" instruction → this re-author itself: every splice point below was re-verified 2026-07-13.

**Type consistency.** `KeyBitmapAccumulator::finish -> Option<String>` matches all three write-side merge sites (JSON string values); `key_row_groups(&[RowGroupMetaData], &str) -> Option<Value>` is produced by `flushed_row_groups()` at both writer types (sync `ArrowWriter` in `batch_to_parquet_key_aligned`/ingest, `AsyncArrowWriter` delegating at `async_writer/mod.rs:187`) — both expose it **before** `close()`, which is why every call site reads spans pre-close; `batch_to_parquet_key_aligned`'s new tuple return is consumed at its two verified call sites (`deletion.rs:68`, `rewrite.rs` tests). `bitmap_contains(&str, i64) -> Option<bool>` matches the provider filter, whose key comes from `self.slice: Option<i64>` (`provider.rs:109`) — the current field, not the pre-32 `packing_key_value` the old plan named. `with_extension` takes the plan by value per the DF 54 doc example (`source.rs:234–236`).

**Sequencing.** Task 1 (pure encoding) before 2 (writers) before 3/4 (reader) — each independently committable because the degradation rules make reader and writer separately shippable: stats written before the reader exists are inert JSON; the reader over stats-less parts is a no-op filter. Task 4 after 3 only because both edit the same `scan()` region. Task 5 last (flips + measurement).

**Safety argument.** Isolation never depends on the new indices: the injected `packing_key = key` FilterExec (scoped mode) remains above every scan, so the worst failure of a wrong bitmap or span is *reading more than needed* — except a false `Some(false)`/wrongful `Skip`, which is prevented by construction: the bitmap is built from the exact rows written into the part (set membership, no approximation), spans come from the file's own row-group statistics, and every ambiguous state (missing, garbage, negative key, partial spans, unscoped mode) resolves to "keep"/"scan". Existing parts (all pre-16 files, `column_stats` without the new keys) are the permanent degradation case and are pinned by tests in Tasks 3 and 4.

**Sizing.** 5 tasks, ~6 commits — within the one-row/one-day envelope.

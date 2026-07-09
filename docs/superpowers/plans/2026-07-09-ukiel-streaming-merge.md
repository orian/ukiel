# Ukiel Plan 29: Streaming Merge (Bounded-Memory Compaction, Whale-Key Splitting) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Issue 0005's real fix, per `docs/notes/2026-07-08-streaming-merge.md`: replace the compactor's read-all → `concat` → in-memory-sort → chunk pipeline with a **streaming k-way merge** whose memory is O(K·batch + row group) instead of O(decoded partition) — at target scale (PB+, 100–500 GB partitions/keys common) the plan-28 cap otherwise inverts into a policy failure where the partitions that matter stay permanently multi-run. Deliverables: (a) async per-part Parquet streams with the existing schema adaptation; (b) a k-way merge over already-sorted inputs (never re-sorts — plan 27's validated ordering is the trust anchor) with a **loud ordering-drift guard**; (c) an incremental chunk writer implementing all three placements on **actual encoded size** (retiring the `bytes_per_row` estimate) plus **whale-key splitting** (a single key's run larger than the target cuts *within* the key into ts-sliced dedicated files — a 500 GB key must not be one file); (d) streaming multipart uploads with per-file upload-intent registration; (e) **retiring the plan-28 cap** (`max_merge_input_bytes`, `merge_capped`, `compactor_capped_merges_total`). **Phase 2 (slice sealing) is deliberately split out to roadmap row 31** — its catalog-model question (`sealed` flag vs metadata carry; the note leans flag) deserves its own just-in-time plan once this lands (per the write-plan sizing rule).

**Architecture:** Three new seams in `ukiel-compactor`, composed in `merge_parts`: `rewrite::part_streams` (async `ParquetRecordBatchStream` per input via `ParquetObjectReader` — range reads, O(row-group) memory per stream — applying the existing `adapt_to_schema` and `apply_defaults_and_materialized` **per batch**; row-wise deterministic, so per-batch ≡ the old post-merge application), `merge::merge_streams` (loser-tree over `arrow::row::RowConverter` rows on the sort key — pure, generic over batch streams, no DataFusion dependency), and `rewrite::StreamingChunkWriter` (placement cuts + whale splitting + plan-14 `writer_props`/key-aligned row groups, writing each output through `AsyncArrowWriter` + `ParquetObjectWriter` multipart uploads, registering upload intent per file before its first byte). The commit stays one REPLACE at the end: a crash leaves intent rows for GC's sweep, a lost race replans — unchanged lifecycle. `plan_chunks`/`batch_to_parquet`/`read_parts_to_batch` **remain** for their surviving callers (deletion's bounded single-part rewrite; the bench loader) — this plan restructures the merge path, not every writer.

**Tech Stack:** arrow/parquet 58.3 + object_store 0.13 (pinned). Parquet needs two **feature additions** (not version bumps): `async` + `object_store` on the workspace `parquet` dep — this enables `parquet::arrow::async_reader::{ParquetRecordBatchStreamBuilder, ParquetObjectReader}` and `parquet::arrow::async_writer::{AsyncArrowWriter, ParquetObjectWriter}`. API names drift between parquet majors; on mismatch check docs.rs for 58.3 and adapt minimally (the fallbacks: hand-roll an `AsyncFileWriter` over `object_store::WriteMultipart`; `bytes_written()`/`in_progress_size()` for encoded-size feedback). `arrow::row::{RowConverter, SortField}` is in the pinned arrow.

**Prerequisites:** Plan 27 executed (streaming merge trusts declared ordering — the shared sort definition, nulls-first convention, and registration validation are what make "never re-sort" sound); plan 14 executed (its `writer_props` + key-aligned flush rule are this plan's writer building blocks — declared there, consumed here). Plan 15's prewarm hooks into this plan's output path next: `StreamingChunkWriter` is the extension point (note for 15's author: prewarm under multipart streaming means teeing to the local cache file during upload, not copying after).

## Global Constraints

- Rust edition 2024, toolchain ≥ 1.96; workspace dep pins per the roadmap — parquet gains **features only**.
- **Every existing test stays green after every task.** Tasks 1–4 are additive (new fns/modules beside the old pipeline); the old path is deleted only in Task 5, in the same commit that moves `merge_parts` over — the compactor component suite and S6 are the equivalence nets.
- Merge/chunker logic is pure or memory-store-testable without Docker where possible; component tests via testcontainers; `make e2e` before the docs flip.
- **No silent fallback:** an ordering-drift detection fails the merge with a named part path — it must never fall back to an in-memory sort (that would reintroduce the O(partition) path this plan exists to remove, silently).
- Commit messages: conventional style, no Claude/AI attribution.
- Every task ends with `cargo fmt && cargo clippy --all-targets -- -D warnings` clean.

---

### Task 1: Async parquet foundations — per-part batch streams

**Files:**
- Modify: root `Cargo.toml` (parquet features), `crates/ukiel-compactor/src/rewrite.rs`

**Interfaces:**

```rust
/// Batch size for merge input streams: small enough that K streams stay
/// cheap, large enough to amortize decode (K x MERGE_BATCH_ROWS rows resident).
pub const MERGE_BATCH_ROWS: usize = 8 * 1024;

/// One async, schema-adapted, materialized-recomputed batch stream per part.
/// Range-reads via ParquetObjectReader — O(row-group) memory per stream, the
/// file is never resident. Each stream carries its part path for error
/// attribution (the merge guard names the offending file).
pub fn part_streams(
    store: &Arc<dyn ObjectStore>,
    schema: SchemaRef,
    cols: &TableColumns,
    parts: &[Part],
) -> Vec<PartBatchStream>; // Stream<Item = Result<RecordBatch, CompactorError>> + path
```

- Per batch: the existing `adapt_to_schema` (additive evolution — old files contribute NULLs, `rewrite.rs:42`) then `ukiel_expr::apply_defaults_and_materialized` (organic backfill, exactly what `merge_parts` does post-concat today — row-wise deterministic, so per-batch application is equivalent).
- `read_parts_to_batch` is untouched (deletion still uses it — one part at a time, bounded by a single file).

- [ ] **Step 1: Add the parquet features** — `parquet = { version = "58.3", features = ["async", "object_store"] }` in the workspace table (verify the exact feature names against docs.rs for 58.3).
- [ ] **Step 2: Write the failing component test** (memory object store, no Docker): upload two small parquet parts (one written *without* a later-added column), `part_streams` yields adapted batches — added column NULL-filled, materialized column recomputed, batch sizes ≤ `MERGE_BATCH_ROWS`, part path attribution on a corrupt-file error.
- [ ] **Step 3: Implement; verify** — `cargo test -p ukiel-compactor --lib` (new) and the full compactor suite untouched.
- [ ] **Step 4: Lint and commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add Cargo.toml Cargo.lock crates/ukiel-compactor
git commit -m "feat: async per-part parquet streams with schema adaptation (streaming merge groundwork)"
```

---

### Task 2: K-way merge over sorted streams

**Files:**
- Create: `crates/ukiel-compactor/src/merge.rs`
- Modify: `crates/ukiel-compactor/src/lib.rs` (module), `src/error.rs` (variant)

**Interfaces:**

```rust
/// Merges K already-sorted batch streams into one sorted batch stream
/// (batches of ~MERGE_BATCH_ROWS). Comparison via arrow::row::RowConverter
/// over `sort_key` under the canonical plan-27 options (asc, nulls first) —
/// converter rows are memcmp-ordered, so the loser tree is type-agnostic.
/// NEVER sorts: an input batch that violates its stream's order (or a stream
/// head that regresses) is a loud error naming the part path —
/// CompactorError::MergeOrderViolation { path, .. }. The full re-sort the old
/// pipeline did is exactly what masked writer-ordering bugs; this guard makes
/// that class of bug fail the merge instead of corrupting output order.
pub fn merge_streams(
    streams: Vec<PartBatchStream>,
    schema: SchemaRef,
    sort_key: &[String],
) -> impl Stream<Item = Result<RecordBatch, CompactorError>>;
```

- Implementation shape: per-stream cursor `(current batch, Rows, row_idx)`; a `BinaryHeap`/loser tree keyed by the current row; output assembled with `arrow::compute::interleave` over `(stream_idx, row_idx)` runs. Memory: K × (one batch + its `Rows`) + one output batch.
- Ordering-drift semantics (document at the guard): the one legitimate trigger is a **changed materialized expression on a `sort_key` column** — recomputation (Task 1) can then break the on-disk order plan 27 guarantees. The error message says so and names the remediation (re-author the column, or drop it from `sort_key`; a recompute-only rewrite path is future work). Never fall back to sorting.

- [ ] **Step 1: Write the failing pure tests** (streams = `futures::stream::iter` over in-memory batches; no Docker):
  - equivalence: for several shapes (uneven stream lengths, duplicate keys across streams, empty streams, single stream, batch boundaries mid-key), merged output rows == the reference `concat + sort_batch` of the same inputs, and arrives in ≤ `MERGE_BATCH_ROWS` batches;
  - multi-column key with nullable utf8 middle column: nulls-first order preserved (plan-27 canonical options end-to-end);
  - guard: an out-of-order input batch → `MergeOrderViolation` naming the stream's path; a stream whose next batch starts before its previous batch ended → same.
- [ ] **Step 2: Verify they fail; implement; verify** — `cargo test -p ukiel-compactor --lib merge`.
- [ ] **Step 3: Lint and commit**

```bash
git commit -m "feat: k-way streaming merge over sorted part streams with ordering-drift guard"
```

---

### Task 3: Incremental column stats

**Files:**
- Modify: `crates/ukiel-core/src/stats.rs`

**Interfaces:**

```rust
/// Fold-in variant of int64_column_stats for streaming writers: accumulate
/// per-batch, finish to the identical {"col": {"min", "max"}} JSON.
#[derive(Default)]
pub struct Int64StatsAccumulator { /* per-column running min/max */ }
impl Int64StatsAccumulator {
    pub fn update(&mut self, batch: &RecordBatch);
    pub fn finish(self) -> Option<serde_json::Value>;
}
```

- [ ] **Step 1: Failing unit test** — folding N slices of a batch yields exactly `int64_column_stats(&whole)` (including the all-null-column and no-int64-columns `None` cases).
- [ ] **Step 2: Implement; verify** — `cargo test -p ukiel-core --lib`.
- [ ] **Step 3: Lint and commit**

```bash
git commit -m "feat: incremental int64 column stats accumulator for streaming writers"
```

---

### Task 4: Streaming chunk writer — placements, whale splitting, multipart uploads

**Files:**
- Modify: `crates/ukiel-compactor/src/rewrite.rs` (`StreamingChunkWriter`)

**Interfaces:**

```rust
/// Consumes a sorted batch stream and writes placement-shaped output files
/// via multipart uploads. Replaces plan_chunks' bytes_per_row estimate with
/// the writer's ACTUAL encoded size (bytes_written + in_progress_size).
pub struct StreamingChunkWriter { /* store, opts: WriteOpts, placement, packing_key, paths_fn, on_open */ }
pub struct ChunkOutput { pub path: String, pub key_min: i64, pub key_max: i64,
    pub row_count: i64, pub size_bytes: i64, pub column_stats: Option<Value> }
impl StreamingChunkWriter {
    /// `on_open(path)` fires BEFORE a file's first byte — the caller
    /// registers upload intent there (plan-9 flow, now per file).
    pub async fn write(self, stream: impl Stream<Item = Result<RecordBatch, CompactorError>>)
        -> Result<Vec<ChunkOutput>, CompactorError>;
}
```

Cut rules (the incremental `plan_chunks`, evaluated at key boundaries within each arriving batch — the stream is key-sorted, so key runs are contiguous):

- **Packed:** one output file for the whole stream (the placement's contract; memory is bounded regardless — the *file* is as big as the policy asks).
- **Separated:** cut at every key boundary (one key per file, as today).
- **SizeTargeted(N):** cut at the *next key boundary* once encoded size ≥ N — small keys share files, exactly `plan_chunks`' semantics but compression-aware. **Whale splitting (new):** if the *current key alone* pushes encoded size past N, cut **mid-key** at ≥ N: the slice closes as a dedicated file (`key_min == key_max == k`) and k continues in a fresh dedicated file — consecutive slices tile k's ts range (the stream is `(key, ts)`-sorted), so plan-12 ts stats prune them and retention can drop aged slices metadata-only. The run invariant is restated, not broken: files within a run are pairwise disjoint on *(key, ts-slice)*; catalog key-range pruning is unchanged (slices match only queries for k), and `delete_key` already drops every `min == max == k` file (verified: `deletion.rs` loops all overlapping dedicated parts).
- Within every output file: plan-14 `writer_props` (level compression, delta ts, blooms, stamps) and the key-aligned row-group flush rule (end the row group at a key boundary when the next run would overflow the cap).
- Failure path: on any error, best-effort abort of the in-flight multipart upload, then propagate — completed-but-uncommitted files are ordinary intent-covered orphans. **Ops note (recorded in docs, Task 7):** a *crashed* process can leave incomplete multipart uploads, which are not objects and are invisible to GC — the bucket needs an abort-incomplete-multipart lifecycle rule (standard S3/MinIO hygiene).

- [ ] **Step 1: Write the failing component tests** (memory object store — it supports multipart; tiny targets so every rule fires):
  - SizeTargeted with keys `(1×small, 2×small, 3×huge, 4×small)`: small keys share files cut at key boundaries; key 3 splits into ≥ 2 dedicated slices whose `(key_min, key_max)` are `(3,3)` and whose ts ranges tile without overlap; key 4 lands after, never mixed into a slice;
  - Separated / Packed shapes match today's semantics;
  - per-output `ChunkOutput` row counts/stats equal a reference computation; read one file back: stamps (`nulls_first: true`), compression, key-aligned row groups;
  - `on_open` fires once per file, before that file exists in the store;
  - error mid-stream → no panic, error propagates, previously-completed files exist (orphan story), no file appears without its `on_open`.
- [ ] **Step 2: Verify they fail; implement; verify** — `cargo test -p ukiel-compactor --lib`.
- [ ] **Step 3: Lint and commit**

```bash
git commit -m "feat: streaming chunk writer - placement cuts on actual encoded size, whale-key ts-sliced splitting, multipart uploads"
```

---

### Task 5: Pipeline integration — `merge_parts` goes streaming; the plan-28 cap retires

**Files:**
- Modify: `crates/ukiel-compactor/src/compactor.rs` (`merge_parts`, config), `crates/ukield/src/config.rs` + `src/run.rs`, `ukield.example.toml`
- Test: compactor component tests

**Interfaces:**
- `merge_parts` becomes: `part_streams` → `merge_streams` → `StreamingChunkWriter` (paths `ht/{id}/L{level}/{uuid}.parquet` as today; `on_open` = `register_pending_objects` for that one path) → the same REPLACE commit over the collected `ChunkOutput`s. `read_parts_to_batch`/`sort_batch`/`plan_chunks` disappear **from this path only**; the conflict-replan and finalize-recheck logic around it is untouched.
- **Cap retirement (issue 0005 closes):** delete `CompactorConfig.max_merge_input_bytes`, `merge_capped`, both call-site checks, and the `compactor_capped_merges_total` emission; drop `max_merge_input_mb` from `CompactorSection`, `run.rs`, and `ukield.example.toml` (serde ignores unknown keys by default, so existing TOML files with the knob keep parsing — verify no `deny_unknown_fields` on the config structs and say so in the commit). The K-bound repurposing the note floated is **not** taken: K is structurally bounded by the ladder (`fanout` runs per level) and post-ladder finalization (≈ `l0_fanout + fanout × depth` files); if a pathological catalog state produces huge K, file-handle exhaustion fails loudly — acceptable, revisit with evidence.
- The plan-28 component test `oversized_merges_are_capped_not_attempted` is **replaced** by its inverse: the same oversized fixture now merges successfully (that test was the cap's spec; its successor is the feature's spec).

- [ ] **Step 1: Write/adapt the failing tests**
  - inverse cap test: the plan-28 oversized fixture (cap would have skipped it) merges to completion — terminal invariant reached;
  - whale end-to-end: one key's rows ≫ `target_file_bytes` in a `SizeTargeted` hypertable → after compaction + finalization, k occupies multiple dedicated slice files; a `count(*)` query for k's namespace returns every row (multiple files per key per run — the restated invariant, exercised through the real provider); `delete_key(k)` drops **all** slices metadata-only (`dropped_parts == slice count`, no rewrite);
  - equivalence net: the existing merge/race/finalization component tests pass unmodified — same observable catalog state, streaming internals.
- [ ] **Step 2: Verify; implement; verify** — `cargo test -p ukiel-compactor -- --test-threads=2 && cargo test -p ukield -- --test-threads=2`.
- [ ] **Step 3: Lint and commit**

```bash
git commit -m "feat: merge_parts streams end to end - O(K*batch + row-group) memory; plan-28 cap retired"
```

---

### Task 6: e2e + bench validation (the caveat lifts)

**Files:**
- Modify: `bench/README.md` (§5 caveat, §6 results)

- [ ] **Step 1: Full verification** — `cargo fmt --check && cargo clippy --all-targets -- -D warnings && make test && make e2e` (S6 compaction-equivalence and S8 GC-hygiene are the invariants' nets; S8 additionally proves the per-file intent flow leaves no orphans).
- [ ] **Step 2: Bluesky 10M re-run** — `bench bluesky run --files 10 --label post-29`: exactly-once still asserted; compaction/finalization timings recorded next to the `4d5f93a` baseline (capped-merge row gone from the report).
- [ ] **Step 3: Compact the hits fixture — the bench §5 caveat exists for this moment.** The 100M-row fixture (1,102 overlapping L1 parts) could never be compacted (the cap, by design); now: reload if needed, drive compaction + finalization to quiescence over the `hits` hypertable, re-run `bench hits queries --label post-29-compacted`. Expected story: fan-out collapses from 67–108 toward ~day-count, and the ~45–55 ms tiny-tenant floor drops with it (plan 13 attacked per-file cost; this attacks file count — record both against the baseline table). Update §5 (caveat resolved) and §6 (new subsection, machine + SHA).
- [ ] **Step 4: Commit**

```bash
git add bench/README.md
git commit -m "bench: post-29 baselines - streaming merge compacts the 100M-row fixture, fan-out collapse measured"
```

---

### Task 7: Docs & status flips

**Files:**
- Modify: `docs/issues/0005-finalization-whole-partition-memory.md` (→ **Resolved (plan 29)**)
- Modify: `docs/notes/2026-07-08-streaming-merge.md` (phase 1 executed; sealing question → row 31)
- Modify: `docs/notes/2026-07-06-lsm-hierarchy.md` (non-goals: streaming merge done; run invariant restated for whale slices)
- Modify: `docs/superpowers/specs/2026-07-05-ukiel-design.md` (deferred list; "Limits" merge-RAM sentence; placement section gains the whale-slice sentence; storage-lifecycle section gains the multipart-lifecycle ops note)
- Modify: `docs/superpowers/specs/2026-07-06-ukiel-monitoring.md` (`compactor_capped_merges_total` retired — plan 29 removed the cap)
- Modify: `docs/superpowers/plans/2026-07-05-ukiel-v1-roadmap.md` (row 29 → **Executed**; row 31 slice-sealing is already sketched there)

- [ ] **Step 1: Flip everything above** (issue 0005's status line names what shipped and points sealing to row 31).
- [ ] **Step 2: Commit**

```bash
git add docs/
git commit -m "docs: mark plan 29 executed - streaming merge ships, issue 0005 resolved, cap retired"
```

---

## Self-review notes

- **Coverage vs the note:** §1 sorted-stream trust + drift guard → Tasks 1–2; §2 actual-size cutting → Task 4; §3 whale splitting with all four checked consequences (pruning, restated run invariant, metadata-only deletion — deletion loop verified in code, ts-sliced retention) → Tasks 4–5; §4 streaming uploads + per-file intent + unchanged commit → Tasks 4–5; §5 sealing → **split to row 31 by design** (sizing rule; the catalog-model question deserves its own gate). Cap retirement → Task 5. The note's "resumable merges" and "intra-partition parallel merge" stay deferred as written.
- **Interfaces verified 2026-07-09:** `adapt_to_schema` + `apply_defaults_and_materialized` in today's `merge_parts` (per-batch move is row-wise-equivalent); plan-14 `writer_props`/key-aligned flush as the writer base; `deletion.rs` loops all `min == max == key` parts (whale deletion works unchanged); parquet 58.3 lacks `async`/`object_store` features in the workspace (Task 1 adds features, not versions); `plan_chunks`/`batch_to_parquet`/`read_parts_to_batch` keep their callers (deletion, bench `hits.rs:254,263`) — nothing external breaks.
- **Semantics reviewed:** no-silent-fallback is a global constraint (a fallback sort would silently reintroduce the O(partition) path); the one legitimate drift trigger (changed materialized expr on a sort-key column) is documented at the guard with remediation; packed placement still yields one arbitrarily large *file* (the placement's contract — memory is bounded either way; size-targeted remains the recommended setting); deletion's single-part sync rewrite stays (bounded by one file) — noted, not hidden.
- **Sequencing:** 1 → 2 → 4 → 5 is the dependency chain; 3 is independent (needed by 4); 6 → 7 close. Task 5 is the only cut-over commit — everything before it is additive, everything after is measurement and paperwork.
- **Safety argument:** the commit protocol is untouched (REPLACE at the end; conflicts replan; intent rows cover uncommitted uploads — now registered per file *before* first byte, strictly earlier than today's batch registration), so exactly-once, GC bijection (S8), and DR invariants hold by construction. The new failure surface is upload aborts and incomplete multiparts: aborts are best-effort + intent-swept; incomplete multiparts are invisible to GC and get an explicit lifecycle-rule ops note rather than silence.

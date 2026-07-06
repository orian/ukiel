# Ukiel Plan 16: Key Index (Tier-2 #5 bitmap + #6 catalog access plans) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.
>
> **REVALIDATE BEFORE EXECUTING:** this plan assumes plans 11 (scan pushdown), 12 (stats pruning), and 14 (key-aligned row groups) have executed. Read `crates/ukiel-query/src/provider.rs`, `crates/ukiel-ingest/src/writer.rs`, and `crates/ukiel-compactor/src/rewrite.rs` first and adapt the modification points to their current shape — the *designs* below are binding, the exact splice points are not.

**Goal:** Tier-2 items #5 and #6 of `docs/notes/2026-07-06-parquet-read-performance.md`: (a) a **roaring bitmap of distinct packing keys** per packed part, making catalog part pruning *exact* (min/max ranges false-positive on sparse keys: range 100–4500 "contains" a tenant with zero rows); (b) **catalog-stored row-group key ranges**, letting the provider hand DataFusion a `ParquetAccessPlan` that skips row groups **without reading the file's footer** — the catalog becomes a global page index.

**Architecture:** Both indices live inside the existing `parts.column_stats` JSONB (no migration): `"packing_keys"` = base64 serialized roaring bitmap (written only when distinct-key count ≤ a cap, default 10k — beyond that min/max is dense enough that the bitmap buys little), and `"key_row_groups"` = `[[min, max], ...]` per row group in order (written by the compactor, whose plan-14 key-aligned writer knows the boundaries; ingest L0 files are small and short-lived — footer reads there are cheap, so L0 skips `key_row_groups`). Read side: the bitmap check happens **in Rust after `live_parts_pruned`** (Postgres can't cheaply intersect roaring bitmaps; candidate lists post range-pruning are small); the access plan is attached per `PartitionedFile`. Everything degrades safely: missing bitmap → keep the part; missing `key_row_groups` → no access plan → DataFusion reads the footer as today.

**Tech Stack:** `roaring` (pure-Rust RoaringBitmap — pin the version `cargo search roaring --limit 1` reports), `base64`, datafusion 54's `ParquetAccessPlan`.

**Prerequisites:** Plans 1–7, 11, 12, 14 executed and green.

## Global Constraints

- Rust edition 2024; datafusion 54 / arrow+parquet 58.3 / object_store 0.13 pinned; docs.rs adapt-minimally rule for API drift (plan 11's convention), especially `ParquetAccessPlan` attachment — verify on docs.rs for the workspace's DataFusion version how a per-file access plan is supplied (`PartitionedFile::extensions` with `Arc<ParquetAccessPlan>` is the expected mechanism; the type lives under `datafusion::datasource::physical_plan::parquet`).
- Component tests via `make test` (`--test-threads=2`); pure bitmap/encoding tests must pass without Docker.
- **Every existing test stays green after every task**; isolation tests are the regression net.
- Conventional commits, no Claude/AI attribution; fmt + `clippy -D warnings` per task.

---

### Task 1: Key-index encoding in ukiel-core

**Files:**
- Modify: `Cargo.toml` (workspace: add `roaring` and `base64` pinned to current versions per `cargo search`)
- Modify: `crates/ukiel-core/Cargo.toml` (both deps)
- Modify: `crates/ukiel-core/src/stats.rs`

**Interfaces:**
- Produces (in `ukiel_core::stats`):
  - `pub const PACKING_KEYS_STAT: &str = "packing_keys";` and `pub const KEY_ROW_GROUPS_STAT: &str = "key_row_groups";`
  - `key_bitmap(batch: &RecordBatch, packing_key: &str, max_distinct: usize) -> Result<Option<String>, SchemaError>` — base64(roaring bytes) of the distinct packing-key values, `None` when distinct count exceeds `max_distinct` or the column is absent. Keys are `i64` but ≥ 0 in practice; values are stored as `u32` when they fit, else the bitmap is skipped (`RoaringBitmap` is u32 — document this bound; a `RoaringTreemap` upgrade is noted as follow-up if key space outgrows u32).
  - `bitmap_contains(encoded: &str, key: i64) -> Option<bool>` — `None` on decode failure or out-of-range key (caller treats as "unknown, keep the part").
- Design rule: **absence or garbage is never an error** — every decode failure degrades to "keep".

- [ ] **Step 1: Write the failing test** — round trip, cap behavior, garbage tolerance:

```rust
    #[test]
    fn key_bitmap_round_trips_and_caps() {
        let schema = Arc::new(Schema::new(vec![Field::new("tenant_id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![100, 4500, 100, 300]))],
        )
        .unwrap();

        let encoded = key_bitmap(&batch, "tenant_id", 10).unwrap().expect("under cap");
        assert_eq!(bitmap_contains(&encoded, 100), Some(true));
        assert_eq!(bitmap_contains(&encoded, 300), Some(true));
        assert_eq!(bitmap_contains(&encoded, 200), Some(false), "the false-positive killer");

        // Cap: more distinct keys than max_distinct -> no bitmap.
        assert!(key_bitmap(&batch, "tenant_id", 2).unwrap().is_none());
        // Missing column -> None, not error.
        assert!(key_bitmap(&batch, "nope", 10).unwrap().is_none());
        // Garbage decodes to "unknown".
        assert_eq!(bitmap_contains("!!!not-base64!!!", 100), None);
    }
```

- [ ] **Step 2: verify fail** (`cargo test -p ukiel-core key_bitmap`), **Step 3: implement** (RoaringBitmap::new + insert u32-converted keys, skip when any key < 0 or > u32::MAX, `serialize_into` a Vec, base64 STANDARD encode; `bitmap_contains` = decode → `RoaringBitmap::deserialize_from` → contains, all failures → None), **Step 4: run/lint/commit** (`feat: roaring key bitmap encoding for part stats`).

---

### Task 2: Writers populate the key indices

**Files:**
- Modify: `crates/ukiel-ingest/src/writer.rs` (bitmap only — merge into the `column_stats` object built in `finish_batch`)
- Modify: `crates/ukiel-compactor/src/compactor.rs` + `crates/ukiel-compactor/src/rewrite.rs` (bitmap + `key_row_groups`; the plan-14 key-aligned writer knows each row group's key span — capture `Vec<(i64, i64)>` alongside the bytes, e.g. by changing `batch_to_parquet_key_aligned` to return `(Vec<u8>, Vec<(i64, i64)>)`)
- Modify: `crates/ukiel-compactor/src/deletion.rs` (bitmap on rewritten parts)
- Modify: respective tests

**Interfaces:**
- `column_stats` gains, for packed parts: `"packing_keys": "<base64>"` (ingest + compactor + deletion outputs; cap 10_000 distinct) and, compactor packed outputs only: `"key_row_groups": [[min,max], ...]` (row-group order). Separated-placement outputs get neither (min==max already exact, one key per file).
- Tests assert: an ingest flush's part carries a bitmap that `bitmap_contains` confirms; a compacted packed part carries `key_row_groups` whose spans match the footer's row-group stats (read the footer back in the test and compare); deletion rewrite refreshes the bitmap (deleted key absent).

(Splice points depend on plans 12/14 exact shapes — REVALIDATE note applies. Commit per sub-area: `feat: writers record packing-key bitmaps and row-group key spans`.)

---

### Task 3: Provider — exact part pruning via bitmap

**Files:**
- Modify: `crates/ukiel-query/src/provider.rs`
- Modify: `crates/ukiel-query/tests/query_test.rs`

**Interfaces:**
- In `scan()`, after `live_parts_pruned(...)` returns candidates, drop every part whose stats contain a `packing_keys` bitmap that **provably excludes** this namespace's key:

```rust
        let parts: Vec<_> = parts
            .into_iter()
            .filter(|p| {
                let Some(stats) = &p.meta.column_stats else { return true };
                let Some(encoded) = stats.get(ukiel_core::stats::PACKING_KEYS_STAT)
                    .and_then(|v| v.as_str()) else { return true };
                ukiel_core::stats::bitmap_contains(encoded, self.packing_key_value)
                    .unwrap_or(true) // unknown -> keep
            })
            .collect();
```

- Test: one packed part covering keys {100, 4500} with a real bitmap; a session for namespace 300 (inside the min/max range, absent from the bitmap) plans **zero files** (`EXPLAIN` output contains no `.parquet` path) and returns zero rows; namespace 100 still sees its rows. This is the sparse-tenant false-positive killer, verified end to end.

(Commit: `feat: exact part pruning via packing-key bitmaps`.)

---

### Task 4: Provider — catalog-driven ParquetAccessPlan

**Files:**
- Modify: `crates/ukiel-query/src/provider.rs`
- Modify: `crates/ukiel-query/tests/query_test.rs`

**Interfaces:**
- For each surviving part whose stats carry `key_row_groups`, build a `ParquetAccessPlan`: `RowGroupAccess::Scan` for row groups whose `[min,max]` contains the namespace key, `Skip` otherwise; attach it to the `PartitionedFile` (expected mechanism: `partitioned_file.extensions = Some(Arc::new(access_plan))` — verify on docs.rs; if this DataFusion version attaches access plans differently, adapt while keeping the semantic: per-file row-group preselection supplied by the provider). Parts without the stat get no plan (footer-based pruning as today).
- Skip-all edge: if every row group is skipped, drop the file entirely instead of attaching an empty plan.
- Test: a two-key packed file written by the key-aligned compactor path (keys in separate row groups, `key_row_groups` recorded); `EXPLAIN ANALYZE` for one key shows the other's row group not scanned. Because plan 11's pushdown would *also* prune it via footer stats, make the access-plan effect observable: assert on the row-group matched/pruned metrics AND (stronger) temporarily corrupt nothing — instead assert `predicate` metrics show `row_groups_pruned_statistics=0` while output is still one row group (i.e. the skip happened *before* statistics evaluation). If the metric split is not distinguishable in this DataFusion version, fall back to asserting correctness plus the plan-string presence of the access-plan row-group selection, and note the observability gap in the perf note.
- **The FilterExec isolation backstop remains above the scan regardless** — an over-broad access plan can only cost performance, never leak rows; an over-narrow one is prevented by construction (`Scan` whenever range contains the key, `Skip` only on proven non-overlap).

(Commit: `feat: catalog-driven parquet access plans skip row groups without footer reads`.)

---

### Task 5: Measure, record, close out

- Full suite green (`make test`), e2e optionally (`make e2e`).
- Re-run the perf smoke; append a "After key index" column to the perf note's results table. The headline metric for this plan: the sparse-namespace query (Task 3's scenario) goes from opening files to **zero object-store requests**.
- Perf note: mark Tier-2 #5/#6 done. Roadmap: plan 16 → **Executed**.
- `git commit -m "docs: record key-index results"`.

# Streaming merge — bounded-memory compaction at PB+ scale

> **Status (2026-07-09):** Phase 1 (§§1–4) **executed as plan 29** —
> `part_streams` + `merge_streams` + `StreamingChunkWriter` ship in
> `ukiel-compactor`; the plan-28 cap is retired; issue 0005 is Resolved.
> Phase 2 (§5 slice sealing) is deliberately **split to roadmap row 31**:
> its catalog-model question (`sealed` flag vs metadata carry) deserves its
> own just-in-time plan. The rest of this note is the as-shipped design.

Issue 0005's real fix. The plan-28 `max_merge_input_bytes` cap keeps the
compactor alive, but at target scale (PB+ lakes, 100–500 GB partitions and
single keys being *common*, not outliers) it inverts into a policy failure:
every partition that matters ends up permanently multi-run, the terminal
invariant (cold partition = one run) stops holding exactly where read amp
hurts most, and metadata-only deletion/retention never kicks in for the
heavy keys that motivated separated placement. The cap is a stopgap; this
is the design that retires it.

## Problem shape

`merge_parts` today: read all input parts → `concat` into one
`RecordBatch` → in-memory sort → `plan_chunks` → write outputs. RAM is
O(decoded partition) — several × the ZSTD Parquet bytes. Two distinct
ceilings hide in there:

1. **Merge memory** — the concat+sort. Removed by streaming (below).
2. **Whale keys** — `plan_chunks` never splits mid-key, so a 500 GB key
   would become one 500 GB file: unwritable in memory, uncacheable,
   unprunable. Needs a rule change, not just streaming.

## Design

### 1. Inputs are already sorted — merge, never sort

Every part is written sorted by the declared `sort_key` (plan 27's shared
`ukiel-core` sorting module + registration-time validation is the trust
anchor). A k-way merge (loser tree / `BinaryHeap` over per-file batch
streams, comparing via arrow `RowConverter` rows on the sort key) yields
the globally sorted stream in O(K × batch) memory. No full sort ever
happens again in the compactor.

**Ordering-drift guard:** the merge asserts output-key monotonicity per
batch (one row comparison per batch boundary — ~free). Today's full re-sort
*masks* any writer-ordering bug; streaming merge would silently interleave
wrongly instead, so the assertion turns that class of bug loud. This is
why plan 27 is a hard prerequisite: it unifies the sort definition and
nulls convention the merge trusts.

### 2. Incremental chunk cutting with real sizes

The chunk cutter consumes the merged stream and applies the same placement
semantics as `plan_chunks`, but decides cuts on the fly — and uses the
in-progress `ArrowWriter`'s actual encoded size instead of the
`bytes_per_row` estimate (strictly better: compression-aware, no
estimation drift on skewed data).

### 3. Whale keys split into ts-sliced dedicated files

New placement rule at scale: a single key's run larger than the target is
cut *within the key* at target size. Each slice is a dedicated file
(`key_min == key_max == k`), consecutive slices tiling k's `sort_key`
suffix (ts) range. Consequences, all checked:

- **Point query for k** reads all of k's slices — that is the tenant's
  data volume, unavoidable; ts predicates prune slices via plan-12
  `column_stats`.
- **Run invariant restated:** files within a run are pairwise disjoint on
  `(key, ts-slice)` rather than key alone; catalog key-range pruning is
  unchanged (slices match only queries for k).
- **Deletion of k** stays metadata-only (drop all slices); **retention**
  improves — aged slices drop metadata-only, which a single monolithic
  whale file could never do.
- "Never split mid-key" becomes "never *mix* keys in a split file".

### 4. Streaming uploads, unchanged lifecycle

Each output file streams through `ArrowWriter` into an object-store
multipart upload — RAM is O(row group) per open file, one file open at a
time. Upload intent (`pending_objects`) is registered per file before its
first byte, same flow as today just incremental. The commit stays one
REPLACE at the end: a crash mid-merge leaves orphaned uploads for GC's
intent sweep, a lost race replans — a multi-hour merge holds no locks and
cold partitions rarely race.

**Memory bound, end to end:** K × batch (K ≈ l0_fanout + fanout × depth,
well under a few hundred streams even for a deep partition) + one row
group + merge state ≈ low hundreds of MB, independent of partition size.
A 500 GB finalization becomes a single network-bound pass.

### 5. Write amplification: slice sealing (phase 2)

Streaming fixes memory; it does not fix rewriting a 500 GB key on every
ladder climb. The companion (the lsm note's deferred "stable split
points", concretized): a dedicated slice that is at target size is
**sealed** — merge planning passes it through untouched and merges only
unsealed inputs; new data for k lands in new slices; a late batch
overlapping a sealed slice's ts range unseals (rewrites) just that slice.
Once a heavy key's history is sealed, it is never rewritten again unless
deleted — write amp for whales drops from O(depth) to O(1) per byte.

**Open catalog-model question (resolve at plan time):** a passed-through
slice keeps its old `created_by_commit`, so run counting
(`finalize_candidates`, fanout triggers) would see it as a perpetual extra
run. Two options:
- (a) a `sealed` flag on parts; run counting and finalization consider
  unsealed parts only. Simple; GC untouched.
- (b) metadata-only carry: the REPLACE emits a *new part row pointing at
  the same object path*. Cleaner run identity, but it breaks GC's
  part ↔ object bijection (the reaper deletes a tombstoned part's object —
  which a live row still references). Requires a path-liveness check in
  the reaper and an S8 rework. **Lean (a)** unless something else needs
  multi-reference paths.

## Interactions

- **Plan 27 (prerequisite):** trusted declared ordering; see §1.
- **Plan 14 (prerequisite):** the shared writer-props / key-aligned-row-group
  writer is the same writer this restructures — build it once, streaming.
- **Materialized columns:** recomputed per batch during the merge; the
  expr engine is already batch-at-a-time. Organic backfill semantics
  unchanged.
- **Plan-28 cap:** retired as a byte cap once streaming lands; optionally
  repurposed as a max-inputs (K) bound.
- **Backpressure / finalization / DR / time travel:** untouched — same
  commit protocol, same immutable objects, same intent rows.

## Deferred beyond this

- **Resumable merges** (checkpointed multipart uploads + a manifest in
  `pending_objects`) — only if restart cost of multi-hour merges hurts in
  practice; replan-from-scratch is correct today.
- **Intra-partition parallel merge** (workers split the key space, commit
  disjoint REPLACEs) — the commit protocol already permits it; a
  throughput lever for later, not a correctness need.

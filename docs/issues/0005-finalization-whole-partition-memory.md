# 0005 — Whole-partition merges are in-memory; finalization makes them the peak-memory event

- **Severity:** Medium (scaling ceiling, not a correctness bug — degrades to OOM crash loop under partition growth)
- **Status:** Open — interim size cap planned (roadmap row 28); streaming merge deferred post-v1
- **Components:** `crates/ukiel-compactor`
- **Found by:** post-landing review of plans 17/18, 2026-07-08 (`docs/notes/2026-07-08-plan17-18-review.md`)

## Summary

`Compactor::merge_parts` reads every input part, concatenates into a single
`RecordBatch`, sorts it in memory, then cuts chunks. That was a declared
non-goal when plan 17 landed (lsm-hierarchy note: "whole-partition merges
stream through RAM; streaming merge is deferred"), and ladder merges are
naturally bounded (`fanout` runs at one level). But **cold-partition
finalization is not**: it feeds a partition's *entire live part set* (all
levels) into one merge, and size-targeted placement explicitly anticipates
partitions large enough to cut into several multi-hundred-MB files. Peak
compactor RSS is now a small multiple of the *decoded* size of the largest
cold partition (decoded Arrow is typically several × its ZSTD Parquet).

## Impact

A partition that grew large while hot will, once quiet for
`finalize_after_secs`, be pulled wholesale into memory by the finalization
sweep. If that OOMs, the failure repeats: the sweep is stateless and replans
the same merge on restart — a crash loop that also takes down the ladder
merges sharing the process. No data is ever corrupted (the commit either
happens or doesn't), but compaction for the whole deployment stalls.

## Fix

**Interim (roadmap row 28):** cap merge input size — skip any merge whose
inputs' `sum(size_bytes)` exceeds `max_merge_input_mb` (config; generous
default, e.g. 1024). Emit a warning and a counter metric so capped
partitions are visible. A too-big cold partition stays multi-run
(correct, queryable, just not terminal); the cap applies to ladder merges
too, since a single level's runs can also grow arbitrarily large. The
`size_bytes` stats needed are already on `PartMeta` (the same numbers drive
`plan_chunks`' `bytes_per_row` estimate).

**Real fix (post-v1):** streaming merge. Inputs are already sorted files, so
a k-way merge over Parquet readers writing output chunk-by-chunk bounds
memory at O(row-group) instead of O(partition) and removes the cap. Slots
naturally after plan 14 (shared writer-props module restructures the write
path this would build on).

## Notes

- The read side is unaffected — this is purely a compactor-process ceiling.
- The cap interacts with backpressure (plan 18): a capped partition's L0
  keeps merging (L0 groups are small), so ingest is not backpressured by a
  cap that only blocks the big fold.

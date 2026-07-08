# LSM level hierarchy, runs, and size-targeted placement

Status: implemented (plan 17, `docs/superpowers/plans/2026-07-06-ukiel-lsm-hierarchy.md`).

## The model

- **Run** — the key-disjoint output set of one merge commit. Derived, not
  stored: distinct `created_by_commit` among a partition's live parts at a
  level. Each L0 file is its own run.
- **Level ladder** — when a partition holds enough runs at level *n*, they
  merge into one run at level *n+1*. The trigger is **two-tier**: a low
  `l0_fanout` for level 0 and a higher `fanout` for L1+, because the levels
  are asymmetric — L0 files span ~all tenants (no key pruning: read amp is
  linear in their count, and they're tiny, so merging is cheap), while L1+
  runs are key-disjoint (a query touches <= 1 file per run) and their
  merges move real bytes. Same shape as RocksDB's L0 trigger vs level
  multiplier, and as lazy leveling (tier the young levels, level the last —
  finalization is the "level the last one" half). `level` is an `i16`;
  depth is emergent (~log_fanout(flushes per partition)), not hardcoded.
  One merge per partition per pass; the ladder cascades across passes.
- **Finalization** — a partition with no L0 arrivals for
  `finalize_after_secs` and >1 run folds into a single run one level above
  its max. Terminal invariant: **a cold partition is one run**, i.e. all its
  files are pairwise key-disjoint.
- **Placement = one knob** (`hypertables.target_file_bytes`): NULL = packed
  (one file per merge), 0 = separated (every key its own files), N =
  size-targeted (cut merge output at key boundaries at ~N bytes; a key run
  bigger than N gets a dedicated `min == max` file — never split mid-key).

## Why per-file tenant coverage inverts with depth

L0 files are wide in key-space and thin in time. Under a size target, per-file
key coverage grows with level only until accumulated bytes cross the target;
above the crossover, files cap at N bytes and each covers an ever narrower
key slice. Heavy keys precipitate into dedicated files at exactly the level
where their data justifies it; per-key GDPR deletion of such keys is
metadata-only (`delete_key` already branches on part shape).

## Read amplification

Files within a run are key-disjoint, so a point query for key k touches at
most one file per run (catalog range pruning) — plus every live L0 file,
which no key pruning can skip. Hot partition: <= l0_fanout unpruned L0
files + fanout x log_fanout(N) pruned files. Cold partition: exactly one
file chain (<= 1 file for small keys). The sparse-range false positives that remain are
plan 16's target (roaring key bitmaps).

## Non-goals / deferred

- Whole-partition merges stream through RAM (same as v1 packed merges);
  streaming merge is deferred. Issue 0005: finalization makes this the
  compactor's peak-memory event — interim merge-input size cap shipped
  (plan 28); streaming merge still deferred post-v1.
- Split points are recomputed per merge (no remembered boundaries); stable
  cut keys would tighten pre-finalization overlap and are deferred.
- Retention-aware compaction and L2+ scheduling niceties ride the same
  machinery later.

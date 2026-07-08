# 0007 — Partition probes are unindexed; `partition_l0_quiet_since` scans all-time history

- **Severity:** Low today (performance; grows unboundedly with part history)
- **Status:** Open — fix planned in roadmap row 28 (migration 0007)
- **Components:** `crates/ukiel-catalog`
- **Found by:** post-landing review of plans 17/18, 2026-07-08 (`docs/notes/2026-07-08-plan17-18-review.md`)

## Summary

The plan-17/18 probes filter parts by
`(hypertable_id, partition_values[, level])`, but no index serves that
shape: `parts_live_idx` is keyed on packing-key range and is partial on
live rows only. Worst case is `partition_l0_quiet_since`, which takes
`max(created_at)` over **every L0 part ever committed** to the partition —
tombstoned/purged included, which is deliberate (it measures arrivals, not
liveness; part rows are never deleted) — so it cannot use the partial index
even in principle. Each finalize candidate costs a per-hypertable scan,
every sweep, with cost growing with all-time part history.

Affected probes: `partition_l0_quiet_since`, `live_l0_parts`,
`live_partition_parts`, and `finalize_candidates`' `GROUP BY
partition_values`.

## Impact

Finalization-sweep and backpressure-probe latency degrade as the parts
table grows (it only grows — GC purges objects, not rows). Catalog CPU
burns on repeated scans of cold history. No correctness impact.

## Fix

Migration `0007`: composite index
`parts (hypertable_id, partition_values, level)` (non-partial, so
quiet-since's dead rows are covered). One index serves all four probes.
Revisit when issue-tracking row counts (roadmap row 23, feed horizon)
changes what "all-time history" means.

## Notes

- `commits` lookups in the quiet-since join are by primary key — fine.
- Pairs with issue 0006 (grouped probe): same migration/plan (row 28).

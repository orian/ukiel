# 0006 — Backpressure probe issues one catalog query per buffered day, per flush tick

- **Severity:** Low (performance/load, no correctness impact)
- **Status:** Resolved (plan 28) — the flush probe calls `max_live_l0_parts(&[partitions])` once per tick (one grouped catalog query) instead of one `live_l0_parts` per buffered day
- **Components:** `crates/ukiel-ingest`, `crates/ukiel-catalog`
- **Found by:** post-landing review of plans 17/18, 2026-07-08 (`docs/notes/2026-07-08-plan17-18-review.md`)

## Summary

`RouteIngest::flush` probes live-L0 pressure by calling
`catalog.live_l0_parts` once for every day currently buffered, on every
flush tick. Backfills are exactly when many days are buffered — that is the
very condition `warn_partitions_per_flush` (default 64) detects — so the
probe is most expensive precisely when the system is already stressed: a
wide backfill can put 100+ sequential catalog round-trips on every tick,
per route.

## Impact

Catalog load and flush-path latency scale with buffered-partition spread.
No correctness impact: the probe only feeds `flush_decision`, and deferral
is exactly-once-safe regardless.

## Fix

One grouped probe — refined by plan 28 to
`max_live_l0_parts(hypertable_id, partitions: &[Value]) -> i64`: a single
round-trip (`WHERE partition_values = ANY($2)` + `GROUP BY` + `max`),
returning only the max since `flush_decision` never consumed per-partition
counts. Replaces `live_l0_parts` (no other production caller). Pairs with
the index from issue 0007.

## Notes

- Sequencing: plan 8 (pipelines) ports the backpressure code; landing this
  first avoids re-porting (roadmap row 28 rationale).

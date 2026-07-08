# 0006 — Backpressure probe issues one catalog query per buffered day, per flush tick

- **Severity:** Low (performance/load, no correctness impact)
- **Status:** Open — fix planned in roadmap row 28 (must land before plan 8 ports the backpressure code into `PipelineIngest`)
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

One grouped probe: `live_l0_counts(hypertable_id, partitions: &[Value]) ->
Vec<(Value, i64)>` — a single round-trip with `WHERE partition_values =
ANY($2)` + `GROUP BY partition_values`. `flush_decision` keeps taking the
max, as today. Pairs with the index from issue 0007.

## Notes

- Sequencing: plan 8 (pipelines) ports the backpressure code; landing this
  first avoids re-porting (roadmap row 28 rationale).

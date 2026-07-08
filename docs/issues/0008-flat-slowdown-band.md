# 0008 — Backpressure slowdown band is a flat 2× regardless of pressure depth

- **Severity:** Low (graceful-degradation quality, bounded by the memory valve)
- **Status:** Accepted — no fix planned; revisit trigger defined below
- **Components:** `crates/ukiel-ingest`
- **Found by:** post-landing review of plans 17/18, 2026-07-08 (`docs/notes/2026-07-08-plan17-18-review.md`)

## Summary

`flush_decision`'s slowdown band defers every other tick — a flat 2×
flush-rate reduction across the whole `[l0_slowdown_parts, l0_stop_parts)`
range (defaults `[30, 200)`). If compaction falls behind for real, ingest
spends a long time hovering near the stop threshold instead of backing off
proportionally to depth (e.g. defer count scaling with `max_live_l0`).

## Impact

Under sustained compaction lag, the system oscillates around
`l0_stop_parts` rather than converging earlier in the band. L0 read
amplification sits near its worst allowed value for longer than a
proportional controller would allow. Correctness is unaffected: the stop
band plus the 2× memory valve bound both L0 count and ingest memory.

## Decision

Accepted as-is. The flat rule is easy to reason about and test (three
bands, one counter); a proportional controller adds tuning surface without
evidence it is needed. **Revisit trigger:** metrics P2 (roadmap row 21)
backlog gauges showing partitions in sustained hover at `l0_stop_parts` —
that is both the signal the band is too blunt and the data needed to tune a
replacement.

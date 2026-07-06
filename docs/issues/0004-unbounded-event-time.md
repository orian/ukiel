# 0004 — Event time is unbounded: junk partitions and the "unknown" day

- **Severity:** Low–Medium (data hygiene / partition pollution, no corruption)
- **Status:** Open — fix planned: plan 19 (`docs/superpowers/plans/2026-07-06-ukiel-safety-rails.md`)
- **Components:** `crates/ukiel-ingest`
- **Found by:** design review, 2026-07-06

## Summary

The `day` partition value is derived from the event's `ts` with no sanity
bounds (`consumer.rs::buffer_message`):

- A `ts` in year 3000 (producer bug, wrong unit — seconds vs millis) mints a
  real partition `{"day": "3000-01-27"}`. It's never queried, never matches
  retention, and sits in the partition set forever.
- A `ts` that `chrono` can't represent at all falls back to
  `day = "unknown"` — a permanent garbage partition that accumulates every
  malformed timestamp ever ingested and can never go "cold" in any
  meaningful sense.

ClickHouse guards exactly this (too-far-in-future insert checks); ukiel
currently accepts anything an `i64` can hold.

## Impact

Partition-set pollution (each junk partition costs a file floor and
background-sweep attention), misleading freshness metrics (a future `ts`
dominates `max event ts`), and the `"unknown"` partition as a slow leak.
Correctness of real data is unaffected.

## Fix

Plan 19: bound event time at ingest — config `max_event_age_days` /
`max_event_future_secs`; rows outside the window are skipped exactly like
poison messages (offsets still advance, counter incremented), and the
`"unknown"` fallback is removed (an unrepresentable `ts` is by definition
out of bounds). Out-of-bounds rows are dropped visibly, by design — same
philosophy as poison handling (S4).

## Notes

- Legitimate historical backfills need `max_event_age_days` raised for the
  duration — a config decision, not a code change; the default should admit
  normal late data (days), not archaeology (decades).
- Interacts with issue-free late-data handling at the compaction layer: a
  bounded window also bounds how far back finalized partitions can be
  reopened.

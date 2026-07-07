# 0004 — Event time is unbounded: junk partitions and the "unknown" day

- **Severity:** Low–Medium (data hygiene / partition pollution, no corruption)
- **Status:** Resolved (plan 19) — ingest applies an asymmetric event-time bound in `buffer_message` (`event_time_in_bounds`): out-of-window rows are skipped like poison (offsets still advance), the `"unknown"` day fallback is removed. Defaults: `max_event_age_days = 3650` (per-table overridable; `0` = unbounded for backfills), `max_event_future_secs = 3600`.
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

Plan 19, with **asymmetric bounds** — the two directions have different
failure semantics:

- **Future bound (`max_event_future_secs`, server-wide, hard):** nothing
  legitimate arrives from the future beyond clock skew; a far-future `ts`
  is a producer bug (sec-vs-ms confusion, corruption) by definition. Skip
  like poison (offsets advance, counter incremented, warn logged).
- **Past bound (`max_event_age_days`, server default overridable
  per table, `0` = unbounded):** old timestamps are *ambiguous* — a unit
  bug maps to 1970, but **customer migrations from other systems are
  legitimate decades-old history** and must never be silently dropped. The
  server default catches garbage on normal streaming tables; a
  migration/backfill table sets `max_event_age_days = 0` (or a wide value)
  for the duration. Skipping is never silent either way
  (`ingest_out_of_bounds_total` + warn).
- **Unrepresentable `ts`** (chrono cannot express it) is skipped regardless
  of configuration — no partition value can be derived; the `"unknown"`
  fallback is removed.

## Notes

- **Migration is a first-class flow, not an exception**: pre-pipelines, the
  supported shape is a per-table bound override for the backfill window
  (design doc "Backfills & migrations"); once plan 8 lands, a backfill is
  its own kafka→parquet pipeline with its own bounds — per-stream, not
  per-server.
- **The bounds are boundary policy, not an engine concept** (design doc:
  time-agnostic core): the config knobs are pre-pipelines scaffolding, and
  plan 8 deletes them in favor of ordinary per-pipeline predicates — they
  must not be ported or grown into API.
- Backfills writing into cold partitions is by design: finalized partitions
  reopen (the quiet-check fails, the ladder resumes) and re-finalize when
  the backfill completes; the plan-18 spread warning firing during a
  migration is expected, not a fault.
- A bounded past window on normal tables also bounds how far back finalized
  partitions can be reopened by stray late data.

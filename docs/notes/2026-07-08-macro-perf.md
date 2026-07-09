# Macro perf harness — design & provenance (plan 30)

> **Runbook, full command reference, and all recorded results live in
> [`bench/README.md`](../../bench/README.md).** This note is the design
> rationale and dataset provenance; it deliberately does not duplicate the
> operational instructions or the results tables (single source of truth).

Volume-scale benchmarks bridging the plan-11 micro harness (~100k synthetic
rows, `crates/ukiel-query/tests/perf_smoke.rs`) to the README's PB+ intent. The
`bench` binary in `ukiel-e2e` reuses the e2e `Stack` (real Kafka/Postgres/MinIO
via docker-compose, in-process ingest/compactor, HTTP query server), so the
benchmark measures the same components the e2e suite proves correct.
Manual-only, never CI; always `--release`; datasets and results gitignored.

## Why these two datasets

- **ClickBench `hits`** (~100M rows, 14.8 GB Parquet) is the industry-standard
  analytical read benchmark. We cast a 14-column subset to the supported ukiel
  types and treat each `CounterID` as a tenant (packing key) — turning a
  single-table benchmark into the multi-tenant read shape ukiel is built for.
- **Bluesky / JSONBench** (real Jetstream ndjson, 1M events/file) is a genuine
  high-cardinality *write* stream: millions of `did`s hashed to packing keys,
  driven through Kafka → ingest → backpressure → ladder → finalization. It is
  the first time plans 17/18 run at the volume they were designed for, and the
  run *asserts* exactly-once at volume rather than only measuring throughput.

## Design decisions (and deviations from the plan-30 sketch)

- **hits: no compaction over the fixture.** ClickBench files are row-range
  slices with overlapping `counter_id` ranges; merging them would rewrite the
  whole ~15 GB and trip the plan-28 cap. So the loader commits one run per file
  at L1 and leaves them. Consequence: per-tenant fan-out ≈ day-count, so
  pruning is under-measured vs a compacted layout — revisit after row 29
  (streaming merge) makes compacting the fixture feasible.
- **hits `day` from `ts`, not `EventDate`** — the same
  `chrono::from_timestamp_millis` derivation ingest uses, avoiding a dependency
  on `EventDate`'s source type.
- **Bluesky exactly-once asserted at the storage layer + per-tenant.** Rows
  span millions of hashed-`did` namespaces, so there is no single global count
  query; the run asserts `sum(live_parts.row_count) == mapped`,
  `ingest_offset_sum == produced_lines`, and per-census-tenant queryable count.
- **`--wave-files` applies to `run`, not standalone `produce`** (only `run` has
  an in-process consumer to drain against).
- **Row-group sizing** uses the shared house writer (`rewrite::batch_to_parquet`)
  — the bench measures what the system writes today; row-group tuning is
  plan 14's territory.
- **Historical timestamps**: the run widens `max_event_age_days` (like the e2e
  Stack) so the event-time rail (issue 0004) keeps 2023-era events.

## Status

Harness executed and smoke-verified end to end (2026-07-09), then the **10 GB
tier recorded the same day** (hits: all 100 files, 100M rows, 1,102 parts,
per-tenant suite; Bluesky: 10M events with both guardrails observed engaging
and exactly-once asserted) — results in `bench/README.md` §6. Remaining:
Bluesky 100M (long run) and 1B (stretch, `--wave-files`).

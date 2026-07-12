# Macro perf harness — design & provenance (plans 30 & 32)

> **Runbook, full command reference, and all recorded results live in
> [`bench/README.md`](../../bench/README.md).** This note is the design
> rationale and dataset provenance; it deliberately does not duplicate the
> operational instructions or the results tables (single source of truth).
>
> Plan 32 (2026-07-12) added the **official** upstream suites on top of this
> chassis — see "The two suite families" below.

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

## The two suite families (plan 32)

The harness now runs two kinds of benchmark over these same two datasets, and
conflating them would be the easiest way to lie with the numbers:

1. **The house suites** (plan 30): per-tenant scenarios, namespace-scoped,
   through the **HTTP query endpoint**. This is the product's real query path —
   an isolated tenant reading its own slice — and it is what the SaaS story
   stands or falls on. No upstream benchmark measures this shape, because no
   upstream benchmark is multitenant.
2. **The official suites** (plan 32): **ClickBench's 43 queries** and
   **JSONBench's 5**, vendored and run whole-table. This is the number the
   outside world can compare, and refusing to produce it would look like hiding.

The tension plan 32 had to resolve: the official suites are *whole-table*, and
ukiel's product path is *namespace-scoped by construction* — every provider
injects an isolation predicate. Rather than bend the benchmark (adding a fake
tenant column, or a `WHERE CounterID = …` to all 43 queries, both of which
would measure something other than what ClickBench measures), we added one piece
of product code: an **operator (unscoped) session** in `ukiel-query`. It builds
the identical provider with the slice set to `None` — catalog pruning, stats
pruning, footer cache and declared output ordering all unchanged, *only* the
isolation predicate absent. So the official suites measure the real read stack
minus scoping, and nothing about the measurement is a special case.

Its security posture is the load-bearing part: **it is unreachable from the HTTP
server**, where `namespace_id` is still caller-supplied (issue 0001) and an
unscoped route would hand any caller every tenant's data. That is enforced by
construction (nothing in `server.rs` references it) and asserted by a test that
greps the server source — the one invariant a runtime test cannot express, since
you cannot assert the absence of a route nobody wrote.

**The raw-DataFusion reference runner** is the other half of the design. A
benchmark number in isolation says nothing: 47 ms is good or bad only relative
to something. So `clickbench raw` runs the *same SQL over the same parquet* on
bare DataFusion — no catalog, no provider, no cache. Upstream's own ClickBench
harness pins DataFusion 54, which is ukiel's pin, so the reference is both a
ceiling (the engine's own best) and a cross-check on our harness against
published numbers. Every query then reports a `ukiel / raw` ratio: **the price
of the stack, per query shape**. It is the most falsifiable number this project
produces, which is exactly why it is worth producing.

**Vendoring discipline.** Each suite keeps the pristine upstream file
(`queries-upstream.sql`) next to the executed one, so `diff` *is* the adaptation
set — no adaptation can hide in a description. `ADAPTATIONS.md` explains each one
and, more importantly, the type-level deltas that do *not* change the SQL but do
change what the ratio means (ukiel has no integer narrower than `int64`, so it
reads wider columns than the raw engine; that cost is stack-adjacent, not stack
overhead, and saying so is the difference between a benchmark and a sales
pitch).

## Design decisions (and deviations from the plan-30 sketch)

- **hits: the loader leaves the fixture uncompacted; compaction is a separate,
  measured step.** ClickBench files are row-range slices with overlapping
  `counter_id` ranges, so the loader commits one run per file at L1 — giving a
  realistic per-tenant *fan-out* to measure pruning against. Originally the fold
  was not merely skipped but *impossible*: rewriting the whole ~15 GB tripped
  the plan-28 merge cap. Plan 29's streaming merge retired that cap, so
  `{hits,clickbench} compact` now folds the fixture in bounded memory, and the
  before/after is the fan-out story (§6 of `bench/README.md`) rather than a
  standing caveat.
- **hits `day` from the event-time column, not `EventDate`** — the same
  `chrono::from_timestamp_millis` derivation ingest uses, avoiding a dependency
  on `EventDate`'s source type. (`hits_cb` *stores* `EventDate` as well, since
  seven of the 43 official queries filter on it.)
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
and exactly-once asserted) — results in `bench/README.md` §6.

**Plan 32 (2026-07-12)**: the official suites landed on this chassis —
ClickBench's 43 queries over a verbatim-named `hits_cb` fixture, JSONBench's 5
over `bsky` (which gained a `did` column), and the raw-DataFusion reference
runner with per-query `ukiel / raw` ratios. First official numbers in
`bench/README.md` §6.

Remaining: Bluesky 100M (long run) and 1B (stretch, `--wave-files`); TSBS as the
tenant-cardinality benchmark is roadmap row 33.

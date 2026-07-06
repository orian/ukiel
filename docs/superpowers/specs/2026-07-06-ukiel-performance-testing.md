# Ukiel Performance Testing

Companion to `2026-07-05-ukiel-testing-design.md` (correctness) and
`docs/notes/2026-07-06-parquet-read-performance.md` (the optimization map).
That map's rule — *every optimization ships with a number* — needs a
methodology behind it; this document is that methodology.

## Philosophy

1. **Measure what users feel and what ops pays for.** User-felt: query
   latency percentiles by query shape, freshness (produce → queryable).
   Ops-paid: object-store requests per query, bytes decoded per row returned,
   write amplification, catalog load. A benchmark that measures neither is
   decoration.
2. **Regressions matter more than absolutes.** Absolute numbers vary by
   machine; direction and ratio don't. Every recorded result carries its
   environment (machine, dataset seed, config) and is compared against the
   previous recording of the *same* scenario.
3. **Medians and tails, never means.** p50 tells the story, p95/p99 tell the
   truth. Cold (first-touch) and warm (cached) runs are separate scenarios,
   not noise to average away.
4. **Fixed seeds, pinned datasets.** Every generator takes a seed; a scenario
   is reproducible from its config alone. No benchmark depends on wall-clock
   or prior runs' leftovers (per-run nonces, same as the e2e suite).
5. **The catalog claim gets its own tier.** "1M tenants" is a *metadata*
   claim — it must be tested without generating petabytes: catalog-only
   benchmarks with synthetic part rows exercise planning, commit, and feed
   paths at full scale for pennies.

## The four tiers

| Tier | What | Harness | Cadence |
|---|---|---|---|
| P1 micro | single query shapes over controlled files | `ukiel-query/tests/perf_smoke.rs` (plan 11) | per optimization plan, manual |
| P2 macro | realistic workload: concurrent ingest + queries + compaction | `ukiel-bench` binary against the compose stack | before/after major changes; nightly later |
| P3 scale | catalog at 1M namespaces / millions of part rows | catalog-only, testcontainers Postgres | when touching catalog queries or schema |
| P4 soak | hours-long S6-style run watching equilibrium | e2e harness, extended | nightly/weekly |

### P1 — micro (exists)

`perf_smoke` prints medians for pinned scenarios (selective count in a packed
file, narrow projection, ORDER BY + LIMIT). Results land in the perf map's
table. Scenarios are append-only: new optimizations add scenarios; existing
ones never change semantics (that would orphan history).

### P2 — macro workload

A `ukiel-bench` binary (future small plan) driving the compose stack the way
production would: N namespaces with a skewed size distribution (Zipf — a few
big tenants, a long tail of tiny ones), ingest at a configured rate, a query
mix replayed concurrently (dashboard-style selective scans, aggregations,
recent-window queries), compactor and GC running. Reports:

- **freshness p50/p95** (produce timestamp → first visible in a query)
- **query latency p50/p95/p99 per query class**, cold and warm
- **compaction equilibrium**: live L0 count over time (must plateau, not grow)
- **write amplification**: compactor bytes written / ingest bytes written
- **commit conflict rate** per writer kind
- **object-store request count** per query class (see the counting wrapper below)

Workload profiles are named and versioned (`packed-tail`, `big-tenant`,
`mixed`) so a result is meaningful without its config attached.

### P3 — scale (the 1M-tenant proof)

Pure-catalog benchmarks: fill Postgres with 1M `logical_tables`, thousands of
hypertables, millions of `parts` rows (synthetic paths, no objects), then
measure with realistic concurrency:

- `live_parts_pruned` latency at various part counts and pruning selectivities
- session build (`list_logical_tables` + table resolution)
- commit throughput with K concurrent committers on one hypertable
  (conflict-rate curve — where does optimistic concurrency start thrashing?)
- `changes_since` scan cost at deep cursors; GC reapability query cost

These pin the indexes: any catalog schema change reruns P3 before merging.

### P4 — soak

The e2e S6 scenario stretched to hours with GC enabled: asserts the system
reaches steady state (L0 plateau, cache dir bounded by eviction policy once
one exists, `pending_objects` near-empty, memory flat) and that no invariant
drifts. Failures here are usually leaks and unbounded queues — things no
short test catches.

## The counting object store

A first-class test utility (lives with the cache code): an `ObjectStore`
wrapper counting `get`/`get_range`/`head`/`put`/`list` calls and bytes.
Wrapping any scenario's store answers "how many object-store requests did
this query make?" — the single most cost-predictive metric we have, and the
direct measure of Tier-2/Tier-4 optimizations (stats pruning should drive
requests to zero for pruned parts; the metadata cache should remove the
2-per-file footer tax). Micro and macro tiers both report it.

## Results discipline

- P1 results: the perf map's table (append columns per plan).
- P2/P3/P4 results: dated files under `docs/perf/` (`YYYY-MM-DD-<tier>-<profile>.md`)
  containing config, environment, and the metric table — never overwritten.
- A result without environment + seed + git SHA is not a result.
- Regression gate (manual v1): a plan that moves any recorded scenario >10%
  in the wrong direction blocks until explained. Automation later.

## Out of scope for now

Cross-node distributed benchmarks (single-process ukield is the current
deployment shape), cost-based benchmark CI gating (needs stable runners), and
comparative benchmarks vs other engines (fun, not load-bearing).

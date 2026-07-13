# Catalog scalability under surge — what to measure, and the options

Ukiel's horizontal story rests on stateless workers over shared storage — but
every query and every flush serializes through **one Postgres catalog**. It is
the component whose ceiling nobody has measured. This note dimensions the
problem, designs the catalog-only benchmark (roadmap row 40), and lays out the
scaling options with their correctness constraints, so the measurement can
hand each one a verdict instead of a vibe.

## What one unit of load actually costs the catalog

Verified against `crates/ukiel-catalog` (2026-07-13):

| load unit | catalog work | shape |
|---|---|---|
| one product query | `live_parts_pruned` (1 SELECT over `parts` with key/range clauses) | read; plus session build (`get_hypertable`, `list_logical_tables`) which is per-session and trivially cacheable |
| one ingest flush | `commit_with_offsets` (1 transaction: `commits` insert, `parts` inserts ×K, `ingest_offsets` CAS, `pending_objects` clear) + `max_live_l0_parts` probe per flush tick | write txn + 1 read |
| compactor pass | `changes_since` poll, `live_parts`, REPLACE `commit` (conflict → rollback + replan) | read + contended write |
| finalization sweep | `finalize_candidates` + `partition_l0_quiet_since` + `live_partition_parts` per candidate | reads (0007 index) |
| GC / collector | backlog aggregates, cursor upserts, gauges every tick | background reads |

Two structural facts before any measurement:

- **The pool is hardcoded**: `PgPoolOptions::max_connections(16)`
  (`lib.rs:27–29`) — per process. A query surge beyond 16 in-flight catalog
  calls queues in sqlx, invisible except via `catalog_pool_connections`
  (plan 21). Pool size is a bench axis, then a config knob (it is neither
  today).
- **QPS ceiling ≠ query ceiling.** Queries touch the catalog once at plan
  time; the scan itself is object-store work. So catalog QPS bounds *query
  admission rate*, not query concurrency — and cheap plan-time reads are
  exactly what Postgres is good at. The realistic risk order is: pool
  starvation first, then Postgres CPU on `parts` scans at high cardinality,
  then commit contention under ingest+compaction storms. The bench exists to
  confirm or reorder that list.

## The benchmark (row 40): catalog-only, cores-limited

No Kafka, no object store, no DataFusion — drive `PostgresCatalog` directly
against the compose `postgres` service (postgres:17, stock config), with CPU
as the controlled variable: `docker update --cpus=N <project>-postgres-1`
changes the limit **live**, so one seeded catalog serves the whole
cores-sweep. Seeding is synthetic `commit()`s with fake paths — parts rows
need no objects behind them, which is what makes catalog-only benching cheap
at high cardinality (millions of `parts` rows in minutes).

Workloads (each a `bench catalog` subcommand; N async workers over one pool,
measured as achieved ops/s + latency percentiles + saturation signature):

1. **read-surge** — `live_parts_pruned` storms with realistic key
   distributions over T tables × P parts; the query-admission ceiling.
2. **write-surge** — `commit_with_offsets` storms across W distinct
   topic-partitions (no CAS conflicts) + the flush-tick probe; the ingest
   ceiling.
3. **conflict-storm** — concurrent REPLACE `commit`s targeting the same
   hypertable (the compactor-vs-compactor and plan-8 MV shape); measures
   useful-work fraction under optimistic-concurrency churn.
4. **mixed surge** — the product ratio (reads ≫ writes, background probes on
   ticks) to find which side degrades first.

Axes: PG cores (1/2/4/8+), pool size (16/32/64), `parts` cardinality (10⁴ →
10⁷ rows), tables/namespaces count. Saturation attribution: sample
`pg_stat_activity` wait events + `pg_stat_database` from a side connection
during each run — pool-wait vs PG-CPU vs lock-wait vs IO-wait is the whole
diagnosis, and it must be recorded per run, not inferred afterward.

## The options (to be verdicted by measurement, not adopted a priori)

**Parallelize the catalog?** Postgres already parallelizes across
connections; the real questions are sharper:

- **Read replicas for plan-time reads.** `live_parts_pruned` on a replica
  serves a *stale snapshot* — which ukiel already tolerates by design: a
  query planned at T sees parts live at T, and objects stay readable for
  `gc.tombstone_grace` after tombstoning. The correctness rule is the same
  invariant family as issue 0002: **replica lag + statement timeout <
  tombstone grace**, enforceable at startup. Writes (commits, CAS) stay on
  the primary. This is the cleanest true-parallelization path and it needs
  zero schema change — but it only pays if the bench shows reads saturating
  the primary's CPU rather than the pool.
- **Sharding by hypertable** (multiple catalogs) is mechanically plausible —
  every hot query is single-hypertable — but it breaks the global change
  feed and cross-table GC/collector sweeps into per-shard loops, and nothing
  yet suggests the write path needs it. File under "if commit TPS on one
  primary is really the wall."

**What can be cached?** In risk order of staleness:

- **Hypertable/logical-table metadata** — changes only at DDL; per-process
  cache with change-feed (or TTL) invalidation is nearly free and removes
  the session-build reads entirely.
- **Live-parts snapshots** — the big one (plan 37's F2 explicitly deferred
  it here): a per-process `(hypertable → parts)` cache invalidated by
  `changes_since` polling. Staleness is bounded by the poll interval and
  guarded by the same tombstone-grace arithmetic as replicas. Turns the
  per-query catalog cost from one SELECT into zero for hot tables. The bench
  quantifies what it buys (pool pressure and PG CPU at read-surge) before
  anyone builds it.
- **Not cacheable**: anything the ingest transaction reads for correctness
  (the offsets CAS *is* the exactly-once mechanism) and the backpressure
  probe (its freshness is the guardrail's point — though plan 28 already
  grouped it to one query per tick).

**Bottleneck priors to test** (each falsifiable by the matrix): sqlx pool
exhaustion at surge (fix: knob + right-size); `parts` scan CPU at 10⁶⁺ rows
per hypertable (fix: index tuning — 0007 covers partition probes but
`live_parts_pruned`'s key/stat clauses deserve `EXPLAIN ANALYZE` under load);
`commits` insert serialization on one hypertable's optimistic concurrency
(fix: nothing cheap — this is the sharding trigger if real); JSONB
`column_stats` extraction cost in pruning clauses (fix: promote hot bounds to
real columns).

## Relation to the roadmap

Row 26 (horizontal ingest) multiplies write-side catalog load by the worker
count; row 8 (pipelines/MVs) adds feed consumers; both make this measurement
prerequisite knowledge rather than curiosity. Follow-up rows come out of the
verdict table (likely candidates: pool knob + snapshot cache; possibly
replica reads), not out of this note.

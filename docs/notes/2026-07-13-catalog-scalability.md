# Catalog scalability under surge — demand, ceilings, and options

Ukiel's horizontal story rests on stateless workers over shared storage, but
every query admission and durable write currently crosses one PostgreSQL
primary. Now that cached-MinIO query execution is within 3% of bare DataFusion,
the catalog is the largest unmeasured horizontal dependency. Roadmap row 40
must measure when one primary stops being sufficient for Ukiel's
million-tenant/PB+ target, and make each scaling option earn its complexity.

## Start with a product demand envelope

A ceiling without a target cannot decide whether to cache, replicate, or shard.
Every report carries configurable demand scenarios. Suggested defaults are:

| scenario | registered tenants | active fraction | query rate | ingest lanes / flush |
|---|---:|---:|---:|---:|
| steady | 1,000,000 | 1% | 1 query/min active tenant | 256 / 10 s |
| dashboard surge | 1,000,000 | 10% | 1 query/min active tenant | 256 / 10 s |
| backfill | 1,000,000 | 1% | 1 query/min active tenant | 2,048 / 10 s |

These are benchmark defaults, not product promises. Derive required read QPS,
write TPS, and background QPS from the recorded inputs. Verdicts use measured
headroom over these targets. The provisional capacity gate is p99 within the
declared admission SLO, zero timeouts, and at least 2x steady / 1.25x surge
throughput headroom. The run must state the SLO rather than silently invent it.

## What one unit of load costs

| load unit | catalog work | shape |
|---|---|---|
| product query | live_parts_pruned plus session metadata reads | read admission, then JSONB/bitmap decode |
| ingest flush | commit_with_offsets: commit, K part inserts, offset CAS, intent clear; grouped L0 probe | write transaction + guardrail read |
| compactor | feed poll, live-part replan, optimistic REPLACE | reads + contended write |
| finalization | candidates, quiet check, partition parts | background reads |
| GC/collector/pipeline | aggregates, pending/tombstone queries, cursors and feed polls | low-rate broad work |

The SQLx maximum of 16 connections is per process. Horizontal deployment
multiplies pools and connections; one 64-connection pool is not equivalent to
eight processes with eight connections each. Catalog QPS bounds query admission,
not scan concurrency.

## State model: history, liveness, and hot-table fan-out

One parts count is insufficient. Vary independently:

- total historical rows, including tombstoned feed history;
- currently live rows covered by the partial live index;
- live rows in one hot hypertable;
- candidate and returned rows for one packing key;
- tables, partitions, packing keys, and L0/L1+ mix.

Use four named states:

| state | history/live shape | purpose |
|---|---|---|
| fresh | mostly live, modest history | new deployment |
| mature-compacted | large history, small live fraction, bounded runs | intended steady state |
| backlogged | large history and elevated live L0/run counts | backfill or compactor lag |
| pathological | mostly live and concentrated in one hypertable | failure envelope only |

Fixtures include realistic plan-16 column_stats: Int64 bounds, roaring key
bitmaps and row-group spans where applicable, the 10k-key omission case, and
dedicated parts without redundant bitmaps. This preserves row width, TOAST
pressure, transfer, JSON decode, and provider filtering costs.

Large fixtures use set-based SQL or COPY-style bulk seeding while preserving
commits, foreign keys, live/tombstoned ratios, and indexes. Product write
throughput is measured separately through commit_with_offsets. Seed time and
product TPS must never be conflated.

## Load topology and modes

Vary PostgreSQL cores, process-equivalent pool count, connections per pool,
global connection budget, workers, offered rate, cardinality, and uniform versus
Zipf keys. Include topologies such as 1x16, 4x16, 32x8, and 64x4.

Every workload has two modes:

1. Closed loop: N workers issue the next request after completion. This finds
   the throughput knee and service-time curve.
2. Open loop: a rate scheduler submits at fixed QPS/TPS. This exposes queue
   growth, overload latency, timeouts, shedding, and recovery without
   coordinated omission.

Report offered, started, completed, timed-out, and failed operations. Every
operation has a deadline. Label hot-cache and cold/evicted-cache runs separately.

## Workloads

1. Read surge: live_parts_pruned with key and timestamp mixes, optionally
   followed by actual key-bitmap decode/filter. Report SQL, deserialize, and
   filter time plus returned rows and bytes.
2. Write surge: conflict-free commit_with_offsets over distinct lanes with
   realistic parts per commit and the real flush-tick probe ratio. Record
   cardinality at start and end.
3. Conflict storm: same partition, same hypertable/different partitions, and
   Zipf-hot partitions. Model the real reread/replan/backoff policy. Report
   useful swaps, attempts, rolled-back work, and latency per success.
4. Mixed surge: demand-envelope reads and writes plus compactor replanning,
   finalization, feed polls and cursor advances, GC queries, and monitoring
   aggregates at their real tick rates.
5. Connection surge: start process-equivalent pools concurrently and measure
   establishment latency, exhaustion, and recovery.

## Saturation attribution

Use a side connection for pg_stat_activity, pg_stat_database, locks, transaction
and block statistics. Measure PostgreSQL and driver CPU from cgroup/process
CPU-time deltas or continuous samples, not a single docker-stats snapshot.
Record WAL bytes/fsyncs, I/O, memory, shared_buffers, storage driver,
dataset-to-RAM ratio, max_connections, and non-default PostgreSQL settings.

Sample size, idle count, and waiters where available for every client pool. A
separate acquire probe is only a canary and must not be called the workload's
acquire-latency distribution. Record EXPLAIN (ANALYZE, BUFFERS, WAL) at each
cardinality knee and include result rows/bytes with every latency claim.

## Options receive measured verdicts

- Per-process pool knob and global connection budget: only if pool queueing
  dominates before PostgreSQL saturates.
- PgBouncer: if many pools or connection surges are the wall; compare
  transaction pooling with direct SQLx pools.
- Batched part inserts: if multi-part commits spend material time in the current
  per-part INSERT loop, while preserving transaction and offset-CAS semantics.
- Metadata cache: justified by measured session-build share.
- Live-parts snapshot cache: only if reads saturate the primary and memory plus
  invalidation beats replicas. It requires atomic snapshot/feed-high-watermark
  bootstrap and bounded memory.
- Read replicas: only if primary read CPU is the wall.
- Typed-column/index promotion: if JSONB/range pruning is the measured wall.
- Feed horizon or physical table partitioning: if history, vacuum, or index
  bloat damages the live path; this directly informs row 23.
- Hypertable sharding: last resort, only if useful commit TPS misses the demand
  envelope after cheaper fixes.

A pool knob without a fleet-wide connection budget can make the system worse.

## Correctness boundary for stale reads

Cache or replica lag has two independent consequences:

1. A stale deletion can reference a tombstoned object. The condition
   lag + query lifetime < tombstone grace preserves object availability.
2. A stale addition is omitted and returns incomplete recent data. Only the
   declared query-freshness SLO can authorize that lag.

Tombstone grace does not solve freshness. Any cache or replica follow-up must
specify freshness, race-free snapshot/feed bootstrap, read-your-writes,
failover, memory bounds, and behavior when lag exceeds the limit.

## Roadmap relation

Row 40 supplies prerequisite evidence for row 26, which multiplies pools and
write lanes; row 8, which adds feed consumers; and row 23, which bounds catalog
history. The final answer is not merely QPS at N cores. It is: at what tenant
activity, ingest rate, catalog age, and fleet size does one primary miss the
SLO, and which least-complex option restores the required headroom?

---

# MEASURED (plan 40, 2026-07-13)

**Catalog only.** No Parquet was written, no object store touched, no DataFusion
query run. Rows in Postgres, and the catalog API over them — the question is where
the single primary stops meeting the SLO, not how fast the scan layer is.

Fixture: 25k parts / 4 hypertables / 30 day-partitions / 1M packing keys, with
real plan-16 `column_stats` (roaring bitmaps + row-group spans, so JSONB and TOAST
width are honest). **This fixture contains no `logical_tables` at all**, and the
read workload resolves one hypertable once before the run — so nothing below is
evidence about *tenant metadata* cardinality. That gap was filed as issue 0011 and
is now closed by the measurement at the end of this note, which re-runs the claim
against a million real logical tables. Machine: 12 physical cores (24 threads), Postgres 17 in compose,
`max_connections = 100`. Driver and Postgres are co-located.
SLO: query-admission p99 ≤ 50 ms, commit p99 ≤ 100 ms, zero timeouts,
2× headroom (steady) / 1.25× (surge).

## The headline

**One primary is nowhere near the wall at the million-tenant envelope, and the
first thing to break is not throughput — it is `max_connections`.**

| demand scenario | derived load | measured (mixed, all workloads together) | headroom |
|---|---|---|---|
| steady | 167 read QPS + 25.6 write TPS | 6,739 ops/s | **17×** |
| dashboard-surge | 1,667 read QPS | 17,748 ops/s | **8.4×** |
| backfill | 205 write TPS | 6,997 ops/s | **9.3×** |

All three **PASS** the capacity gate. Reads alone sustain 20,000 QPS open-loop at
6 cores with p99 6.2 ms and zero timeouts; commits sustain ~8,000/s against a
demand of 25.6.

## Reads: liveness costs, history is nearly free

The single most useful result, and it is counter-intuitive:

| state | live / tombstoned (of 25k) | read QPS (closed, 32 workers) |
|---|---|---|
| fresh (90% live) | 22,500 / 2,500 | **2,674** (uniform) / 3,498 (zipf) |
| **mature (10% live)** | 2,500 / 22,500 | **17,469** / **21,298** |
| backlogged (35% live) | 8,750 / 16,250 | 4,015 / 5,201 |

**A catalog with 22,500 tombstoned rows is 6.5× FASTER than one with 22,500 live
rows.** The partial live index does its job: history is close to free, and what
costs is the number of rows that are still *live*. So the compacted steady state
is the fast state, and a backlog — not age — is what hurts. Zipf keys are ~20%
faster than uniform (a hot key's parts stay in cache).

This directly re-prices roadmap row 23 (feed horizon): **not needed for read
latency.** Its motivation is disk and vacuum, not query admission, and it should
be argued on those terms.

## Cores: ~3.5k read QPS per core, linear

| Postgres cores | closed-loop knee | open-loop sustained (p99 ≤ SLO, 0 timeouts) |
|---|---|---|
| 2 | 7,570 op/s (p99 48.8 ms — at the SLO edge) | **5,000/s** |
| 4 | 15,806 op/s | **10,000/s** |
| 6 | 21,820 op/s | **20,000/s** (p99 6.2 ms) |

Saturation at 2 cores classifies as **PostgresCpu** (1.94 of 2.0 cores, 97%).
Above that, nothing dominates — the primary is simply not being pushed.

Even **2 cores clears every scenario**: 15× headroom on steady, 2× on the surge.

## The first wall: `max_connections`, and it is not a throughput problem

Topology, at a fixed 64 total connections:

| topology | read QPS |
|---|---|
| 1 pool × 16 | 22,561 |
| 4 × 16 (64 conns) | 25,435 |
| 8 × 8 (64 conns) | 25,649 |
| 16 × 4 (64 conns) | 25,147 |

Four times the connections buys **+13%**. The pool is never the wall.

But the connection *surge* — P process-equivalent pools all demanding their
connections at once, as a deploy does — finds a hard global limit:

| fleet | connections wanted | admitted | refused |
|---|---|---|---|
| 4 processes × 16 | 64 | 64 | 0 |
| 6 × 16 | 96 | 96 | 0 |
| **7 × 16** | **112** | **100** | **12** |
| 8 × 16 | 128 | 100 | 28 |
| 12 × 16 | 192 | 100 | 92 |

**A 7-process fleet exhausts the default `max_connections`.** No per-process pool
setting moves this; a bigger pool just walks a smaller fleet into the same wall
sooner. (This is also why sqlx's laziness nearly hid it: constructing 8 pools
opens almost no sockets, and the test cheerfully reported "128 connections, 0
failures" until every pool was forced to hold its connections simultaneously.)

## Writes: the per-part INSERT loop is not the problem

| parts per commit | commits/s | parts/s |
|---|---|---|
| 1 | 4,920 | 4,920 |
| **4** | **8,026** | **32,104** |
| 16 | 4,537 | 72,592 |

Per-**commit** cost (transaction + offset CAS) dominates; the per-part INSERT is
cheap up to ~4. Against a demand of 25.6 TPS steady / 205 backfill, that is
**157× / 20× headroom**. Batched part inserts would optimize something that is not
the constraint.

## Conflicts: optimistic REPLACE does not scale with contenders on one partition

Backlogged fixture, one hypertable. *Useful* swaps — the commit that actually
landed — not attempts:

| shape | contenders | useful swaps | conflicts | rollback tax |
|---|---|---|---|---|
| same-partition | 2 | 58 | 54 | 10.6% |
| same-partition | 8 | **58** | 293 | 21.6% |
| same-partition | 32 | **58** | 1,134 | **55.7%** |
| zipf (hot partition) | 2 | 393 | 18 | 3.5% |
| zipf | 8 | **971** | 259 | 14.7% |
| zipf | 32 | **678** | 1,468 | **61.9%** |
| spread (own partition each) | 2 | 540 | 5 | 0.9% |
| spread | 32 | 1,740 | 794 | 1.4% |

**Piling contenders onto one partition adds no useful work and only waste**: 32
contenders do exactly the same 58 swaps as 2, while throwing away 56% of their
attempts. On a hot partition, throughput *declines* past 8 contenders (971 → 678).

Product consequence: the compactor should not run many workers against the same
partition. Partition-disjoint work scales; contended work does not. This is a
scheduling rule, not a database limit.

> Honesty note: the same-partition and spread points at high contender counts
> merged their partitions *out* mid-run (no-ops dominated), so those rows are
> bounded by the fixture as well as by contention. The bench says so on the run
> rather than reporting the no-ops as swaps — which an earlier version of it did,
> claiming 62,000 useful merges/s from a partition that only ever held 25 parts.

## Partition leases: the scheduling rule, enforced (plan 41)

The rule above ("do not run many workers against the same partition") is now
mechanism rather than advice. A compactor claims a renewable lease on
`(hypertable, partition)` **before it opens a Parquet file**, and that lease
generation is fenced inside the REPLACE transaction. Re-run of the same A/B on a
fixture with real merge headroom (`--tables 1 --parts 600000 --state backlogged`
→ 3,500 live parts in the hot partition, so **no** point is fixture-bounded: every
run below reports zero no-ops). Two reps per point; the spread is within ±7%.

| shape | contenders | useful swaps/s (optimistic → leased) | conflicts | rollback tax | p99 |
|---|---:|---|---:|---:|---|
| same-partition | 2 | 37.7 → **41.3** | 349 → **0** | 48% → **0%** | 202 → **30** ms |
| same-partition | 8 | 28.4 → **33.4** | 1,794 → **0** | 86% → **0%** | 302 → **30** ms |
| same-partition | 32 | 9.4 → **18.3** | 2,778 → **0** | 97% → **0%** | 824 → **26** ms |
| zipf (hot partition) | 2 | 48.1 → **48.8** | 29 → **0** | 6% → **0%** | 107 → **65** ms |
| zipf | 8 | 70.6 → **121.3** | 701 → **0** | 50% → **0%** | 395 → **119** ms |
| zipf | 32 | 47.4 → **146.2** | 1,607 → **0** | 77% → **0%** | 1,232 → **236** ms |
| spread (own partition each) | 2 | 51.0 → 49.2 | 1 → **0** | 0.1% → 0% | 63 → 65 ms |
| spread | 8 | 123.1 → 122.4 | 33 → **0** | 2.6% → **0%** | 168 → 119 ms |
| spread | 32 | 111.4 → **148.2** | 465 → **0** | 29% → **0%** | 1,214 → **279** ms |

**Compactor/compactor REPLACE conflicts are zero in every leased point** — not
reduced, gone: the contender that does not own the partition never plans a merge.
Optimistic REPLACE stays in the commit path and stays mandatory; it is simply no
longer what resolves compactor-vs-compactor. It still catches the writers that
take no lease, which is exactly its job (a catalog test proves a deletion racing a
lease holder still makes the leased REPLACE conflict).

Three results worth stating plainly:

- **Optimistic REPLACE does not merely waste work at high contention — it loses
  ground.** Same-partition useful throughput *falls* 37.7 → 9.4 swaps/s going from
  2 to 32 contenders, because the retry storm crowds out the winners. Under a
  lease the same 32 contenders do **18.3/s**, roughly 2×, and on the realistic
  Zipf-hot shape **3.1×** (47.4 → 146.2).
- **Partition-disjoint work does not regress.** Spread at 2 and 8 contenders is
  flat within the repeatability band (49.2 vs 51.0; 122.4 vs 123.1) — the lease
  serializes *inside* a partition, not across the fleet. At 32 it actually *wins*
  (111.4 → 148.2), because "spread" still collides sometimes (30 partitions, 32
  contenders) and those collisions were paying the tax.
- **Latency is the quiet win.** p99 falls 824 → 26 ms at same-partition 32. A
  worker that skips a partition it does not own returns in one round-trip instead
  of retrying a doomed transaction five times.

**The coordination is cheap, and priced.** The lease costs one upsert per
candidate — at the worst point (same-partition, 32 contenders) 2,764 lease ops/s,
essentially all of them *skips*: a contender is told "not yours" for the price of a
single primary-key upsert. Against a measured 8,026 commits/s and 20k read QPS
ceiling, that is inside the noise of the demand model, and it uses the existing
pool: **no lease adds a connection**, so plan 40's first wall (`max_connections`)
is exactly where it was — a 7-process fleet at 16 conns still wants 112 against a
default 100, re-confirmed after this change.

**Understated by construction.** This is the catalog-only bench: the work a losing
contender throws away here is one `live_partition_parts` read. In the product it is
a Parquet read, a k-way merge, and an upload — all paid *before* REPLACE says it
lost. The component test asserts the product-path version directly: two compactors
on one partition, and exactly one of them touches object storage at all.

## The driver is expensive, and that is a fleet-sizing fact

At 21k read QPS the **bench driver burns ~6 cores** deserializing ~500k part rows/s
(JSONB → serde) while Postgres burns ~5.7. Neither is the wall on a 24-thread box,
so the point is valid — but the ratio is a real number for capacity planning:
**a ukield process needs roughly one core per 3.5k catalog reads/s**, and that
cost is client-side, not database-side. It is not fixed by any option below.


## `max_connections` 100 vs 200 — measured, not argued

Same fixture, same binary, only `max_connections` changed
(`UKIEL_PG_MAX_CONNECTIONS`, compose knob).

**Admission — the wall moves exactly where the arithmetic says:**

| fleet (× 16 conns) | wanted | admitted @100 | admitted @200 |
|---|---|---|---|
| 4 processes | 64 | 64 | 64 |
| 6 | 96 | 96 | 96 |
| **8** | **128** | **100** (28 refused) | **128** (0 refused) |
| 12 | 192 | 100 (92 refused) | **192** (0 refused) |
| 16 | 256 | 100 (156 refused) | 200 (56 refused) |

The fleet ceiling goes from **6 processes to 12**.

**Cost of raising it — none measurable:**

| topology | @100 | @200 |
|---|---|---|
| 1×16 (16 conns) | 22,122 op/s | 22,029 op/s |
| 4×16 (64) | 24,782 op/s | 24,726 op/s |
| 8×16 (128) | 22,729 op/s | 23,529 op/s |
| postgres RSS | 94.3 MiB | 81.7 MiB |

Throughput is flat within noise and memory does not grow — Postgres allocates a
backend on connect, it does not reserve one per configured slot. At this scale the
"raising `max_connections` is expensive" folklore simply does not bite.

**And the finding that matters most — the wall does not announce itself in p99:**

| max_connections | fleet | achieved | **p99** | **max** | **timed out** |
|---|---|---|---|---|---|
| **100** | 8×16 = 128 conns | 22,729 op/s | **2.1 ms** | **2,001.6 ms** | **46** |
| **200** | 8×16 = 128 conns | 23,529 op/s | 2.6 ms | 21.0 ms | **0** |

A fleet that overshoots `max_connections` does not merely get connections refused.
Its **queries miss deadlines** — 46 of them hit the 2 s timeout — while **p99 still
reads 2.1 ms** and throughput looks healthy. Only `max` and the timeout counter
show it. A dashboard watching p99 would have seen nothing wrong.

**Verdict #1 sharpens: set `max_connections` to at least `2 × fleet_size ×
pool_size`, and treat it as a fleet-wide budget.** It is free at this scale, and
the failure it prevents is invisible to the metric most people watch.

---

# OPTION VERDICTS

Each with the deciding number. **No product change ships from plan 40 itself** —
its one product consequence, partition leases, became plan 41 and is measured
above (option 11).

| # | option | verdict | the deciding number |
|---|---|---|---|
| 1 | **Fleet-wide connection budget** | **PURSUE** | A 7-process fleet (112 conns) exceeds `max_connections = 100` — the first wall the system actually hits, long before any throughput limit. **Measured 100 vs 200: raising it costs nothing** (throughput flat within noise, RSS unchanged) and doubles the fleet ceiling to 12 processes. **And it is not merely an admission problem: at 100 with a 128-conn fleet, 46 queries missed their 2 s deadline while p99 still read 2.1 ms.** The wall is invisible to the metric most people watch. Set it to ≥ 2 × fleet × pool_size. |
| 2 | Per-process pool knob | **DECLINE** (alone) | 1×16 → 16×4 at constant connections moves throughput +13%. The pool is never the wall. A bigger pool without a fleet budget makes #1 arrive *sooner*. |
| 3 | **PgBouncer** | **NOT YET** — trigger named | Needed once the fleet exceeds `max_connections / pool_size`. But raising `max_connections` is the cheaper first move and was **measured free** (100 → 200: no throughput or memory cost, ceiling 6 → 12 processes). PgBouncer earns its hop only when the fleet outgrows what a single primary will accept in backends — a much larger fleet than anything on the roadmap. |
| 4 | Batched part inserts | **DECLINE** | 4 parts/commit already gives 8,026 commits/s = 32k parts/s against a 25.6 TPS demand (157× headroom). Per-commit cost dominates the per-part INSERT. Optimizing the loop optimizes a non-constraint. |
| 5 | DDL / metadata cache | **NOT MEASURABLE HERE** | Session build was out of this plan's catalog-only scope. It remains a *latency* question (per-query), not a capacity one. File separately if per-query latency is ever the complaint. |
| 6 | Live-parts snapshot cache | **DECLINE** | Reads sustain 20k QPS against a 167 QPS demand (120×) and 1,667 QPS surge (12×). It would add a cache-coherency and stale-read problem to solve a problem that does not exist. |
| 7 | Read replicas | **DECLINE** | Same as #6: primary read CPU is not the wall. And a replica inherits the two invariants below, for no measured gain. |
| 8 | Typed stats columns / index changes | **DECLINE** | Buffer hit ratio 1.000; zero I/O waits; no lock waits. JSONB decode cost is **client-side** (the driver's 6 cores), which no Postgres schema change touches. |
| 9 | Feed horizon / physical partitioning (row 23) | **RE-PRICED** | History is nearly free for reads — a 90%-tombstoned catalog is **6.5× faster** than a 90%-live one. Row 23's motivation is **disk and vacuum, not query latency**, and it should be argued on those terms. |
| 10 | Hypertable sharding | **DECLINE** | Useful commit TPS is 8,026 against a 205 TPS backfill demand. Sharding is a last resort for a constraint that is 20–150× away. |
| 11 | **Compaction partition leases** (plan 41) | **SHIPPED** | The one product change plan 40's numbers *did* earn. Compactor/compactor conflicts 2,778 → **0** at 32 same-partition contenders; useful throughput 2× there and 3.1× on the Zipf-hot shape; p99 824 → 26 ms; partition-disjoint work unchanged. Coordination costs one upsert per candidate on the existing pool, so `max_connections` remains the first wall. |

## The two invariants any future cache or replica must satisfy

Recorded because they were nearly forgotten, and because tombstone grace is the
attractive wrong answer:

1. **Deletion safety:** `lag + query lifetime < tombstone grace`. A stale read may
   reference an object GC has already reaped.
2. **Freshness:** `lag ≤ the declared query-freshness SLO`. A stale read *omits*
   recently added parts and silently returns incomplete data.

**Tombstone grace does not solve freshness.** They are independent, and only #2 can
authorize a lag. Any cache or replica follow-up must specify both, plus a race-free
snapshot/feed-high-watermark bootstrap, read-your-writes, memory bounds, failover,
and what happens when lag exceeds the limit.

## What would change these verdicts

- A fleet larger than ~6 processes → #1 and #3 become live immediately.
- Read demand above ~10k QPS sustained (60× today's steady) → #6/#7 return.
- A backlogged catalog as the *steady* state (not a transient) → reads drop to
  ~4k QPS, and the compactor's ability to keep up becomes the question, not the
  primary's throughput.


---

# MEASURED (issue 0011, 2026-07-13): the million-logical-table proof

Plan 40's headline was measured on a catalog with **four hypertables and zero
logical tables**. A million packing-key *values* is not a million tenants'
metadata: index depth, namespace-local listing and session construction all scale
with the second and not the first. So the claim was re-run against the shape it is
actually about.

Fixture: **1,000,000 logical tables over 1,000,000 namespaces, 2,000 hypertables**,
parts at 10k / 1M / 10M, `mature`. The namespace id *is* the packing key
(`slice = namespace.0`). Two reps per point, agreeing within ~2%.
Reproduce: `bench/run-issue-0011.sh`; hashes in `bench/manifests/issue-0011.json`.

## The headline stands — and now it is about tenants

**One primary is nowhere near the wall at a million tenants.** Every point passes
the capacity gate with zero timeouts; the worst configuration measured (full
admission at 10M parts) still clears **26×** the steady demand of 167 read QPS.

`op/s` / `p99 ms` / `catalog rows per op`:

| parts | parts-only (plan 40's workload) | catalog admission (typical tenant) | catalog admission (zipf) | full admission (+ session & plan) | cold buffers |
|---|---|---|---|---|---|
| 10k | 44,422 / 0.6 / 6 | 23,432 / 1.0 / 0 | 22,486 / 1.2 / 1 | 9,712 / 2.4 | 23,245 / 1.0 |
| 1M | 7,142 / 4.4 / 93 | 18,429 / 1.3 / 6 | 15,331 / 2.3 / 14 | 7,595 / 3.9 | 16,089 / 2.2 |
| 10M | 782 / 39.4 / **928** | 17,224 / 1.3 / 8 | 7,081 / 16.6 / **88** | 4,387 / 20.2 | 7,114 / 16.0 |

**The tenant metadata is free.** `list_logical_tables` and `get_logical_table` are
index probes on `logical_tables_namespace_id_name_key`, and catalog-only admission
holds 17–23k op/s at p99 1.0–1.3 ms for the typical tenant at *every* part count.
Adding the three metadata round trips to the parts-only call roughly halves
throughput at the small point (44k → 23k) and costs nothing at the large ones — the
metadata is not where the money goes at any scale measured.

**A cold cache is a non-event**: 7,114 op/s cold vs 7,081 warm at 10M (buffer-hit
0.92 vs 0.89). The partial live index keeps the working set small, as plan 40
argued — now checked rather than assumed.

## What actually costs, at a million tenants

**1. The parts fan-out, not the tenant count.** The slow columns are slow because
they return more rows: 88 and 928 catalog rows per op at 10M, against 8 for the
typical tenant. And those rows are then *discarded* by the provider's key bitmap —
the catalog ships the bitmap so the client can use it to throw away the row that
carried it. Filed as **issue 0014**; this fixture is the first with enough parts in
one hypertable to make it visible.

**2. Query admission is CPU-bound in the process, not in the catalog.** Full
admission (SessionContext + physical plan) burns **8.6–8.9 of 12 cores in the
driver** while Postgres sits at 2.5–3.6, capping one ukield at ~4.4–9.7k
admissions/s. This sharpens plan 40's "~1 core per 3.5k catalog reads/s" note: the
per-request wall is DataFusion planning, and no amount of Postgres scaling moves
it. Fleet sizing must budget for it.

## Two bench bugs found on the way — both would have invalidated the result

**The saturation classifier was blind outside the main checkout.** It located
Postgres by the hard-coded container name `ukiel-postgres-1`. Anywhere else — a git
worktree, a renamed directory, `COMPOSE_PROJECT_NAME` — `docker inspect` failed,
Postgres CPU read as **0.00 cores**, and the classifier then attributed *every* run
to the driver, confidently and with a number. It now resolves the container by its
compose service label. Plan 40's attributions above are unaffected (it ran where the
guess was right), but a classifier that says "0.00 cores" is worse than one that
says it cannot tell.

**The fixture was silently empty.** Every hypertable was given parts spanning the
whole key space. With a million tenants over 2,000 hypertables, a table owning 500
tenants held parts whose ranges covered all million — so 99.95% of tenants resolved
to **zero parts**, and the catalog would have looked magnificent while being asked
nothing at all. Key bands are now solved per hypertable from a target fan-out; the
seeder *measures* the fan-out it achieved (median tenant 7–8 parts, against plan
16's measured 7 for the real product) and **fails the run** if the median tenant
resolves to nothing while there are enough live parts to go round.

A fixture that returns nothing is indistinguishable from a fast one, unless you
check. That is the whole lesson of issue 0011, and it applies to plan 40's original
fixture too: its numbers were real, but they were not answering the question the
project was asking them.

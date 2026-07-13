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

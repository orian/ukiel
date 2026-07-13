# Ukiel Plan 40: Catalog Scalability Bench

> Investigation first. Tasks 1–4 build permanent benchmark capabilities, Task
> 5 runs the matrix, and Task 6 gives every pre-registered option a measured
> verdict. No performance fix ships from this plan; a confirmed fix earns its
> own roadmap row.

> **EXECUTED 2026-07-13** (commits `d26994e`…). Catalog only — rows in Postgres; no Parquet, no object store, no DataFusion query. 25k-part fixtures on a 12-physical-core box.
>
> **Result: one primary is nowhere near the wall. All three demand scenarios PASS with 8–17× headroom, and the first thing to break is `max_connections` — a 7-process fleet at 16 conns/process exceeds the default 100.** That is a budget problem, not a sharding problem.
>
> - Reads sustain **20,000 QPS** open-loop at 6 cores (p99 6.2 ms, zero timeouts) against a **167 QPS** steady demand. Commits sustain **8,026/s** against **25.6 TPS**.
> - **Liveness costs; history is nearly free.** A 90%-tombstoned catalog reads **6.5× faster** than a 90%-live one — so **row 23 (feed horizon) is re-priced: its motivation is disk and vacuum, not query latency.**
> - **Optimistic REPLACE does not scale with contenders on one partition**: 32 contenders do the same **58** useful swaps as 2, while throwing away **56%** of attempts. A compactor scheduling rule, not a database limit.
> - Ten options carry numbered verdicts. **No product change ships from this plan** — one row (fleet-wide connection budget) is PURSUE, one (row 23) is re-priced, the rest DECLINE or NOT-YET with the trigger named.
>
> Four counting bugs were found and fixed *in the bench itself*, each of which produced a plausible wrong number — they are the reason the results are trustworthy, and they are documented rather than quietly patched:
> - **Throughput** divided warmup completions by post-warmup duration (reported 1,250 op/s for a 1,000/s offered rate).
> - **Conflict** counted no-ops as useful swaps (claimed 62,000 merges/s from a partition holding 25 parts).
> - **The CPU limit could not be restored** — `docker update --cpus 0` silently does nothing, so sections after the cores sweep ran at 8 cores while labelled unlimited. The *reports* were honest throughout (each records its own `pg_cpu_limit`), which is how it was caught.
> - **The connection surge measured nothing**: sqlx pools connect lazily, so 8×16 opened almost no sockets and cheerfully reported "128 connections, 0 failures" against `max_connections = 100`. Forcing every pool to hold its connections simultaneously is what exposed the real global wall.
>
> One deviation: the driver-invalidation rule became a **share**, not the plan's flat ">2 cores". The intent is right; the flat test is wrong on a big box, where the driver legitimately burns ~6 cores deserializing part rows next to a Postgres burning ~5.7, with 24 threads spare. Driver CPU is reported on every run regardless — "a ukield process needs ~1 core per 3.5k catalog reads/s" is a fleet-sizing fact, not a bench artifact.

## Goal

Measure when the single PostgreSQL primary stops meeting Ukiel's query-admission
and durable-write SLO under the million-tenant/PB+ target. Report sustainable
open- and closed-loop read QPS, write TPS, useful conflict TPS, latency,
timeouts, recovery, and saturation attribution across:

- product demand scenario;
- PostgreSQL cores and memory/working-set posture;
- process-equivalent pool topology and global connections;
- historical, live, and hot-hypertable part cardinality;
- fresh, mature-compacted, backlogged, and pathological states.

The output must answer: at what tenant activity, ingest rate, catalog age, and
fleet size does one primary miss the declared SLO, and which least-complex
option restores the required headroom?

Driver: roadmap row 40 and
docs/notes/2026-07-13-catalog-scalability.md. Read that note first.

## Architecture

Everything lives in the manual bench binary under
crates/ukiel-e2e/src/bin/bench/catalog.rs, plus main.rs wiring. It drives
PostgresCatalog directly: no Kafka, object store, or DataFusion.

One small library addition is allowed:
PostgresCatalog::connect_with_pool_size(url, n); existing connect delegates to
it with 16. Multi-pool tests create several PostgresCatalog instances to model
separate ukield processes. A single large pool must never stand in for a fleet.

Large fixture creation uses bench-only set-based SQL or COPY-style insertion,
not millions of product API calls. It preserves commits, foreign keys,
live/tombstoned ratios, indexes, realistic partition/key distributions, and
plan-16 column_stats. The write benchmark itself always uses the real
commit_with_offsets path, including its current per-part INSERT loop.

Every workload supports:

- closed-loop workers for the service-capacity knee;
- open-loop fixed arrival rate for queueing, timeout, overload, and recovery;
- per-operation deadlines;
- multiple process-equivalent pools;
- warmup excluded from results;
- hot and cold/evicted PostgreSQL-cache labels.

A separate observer connection samples PostgreSQL. Driver and database resource
use are measured continuously or from CPU/cgroup counter deltas, never from one
point-in-time docker-stats sample.

## Demand scenarios and capacity rule

Reports contain the raw inputs and derived load:

- registered tenants;
- active fraction;
- queries per active tenant per second;
- ingest lanes and effective flush interval;
- compactor, pipeline, GC, and collector tick rates;
- declared query-admission and commit p99 SLOs.

Default scenarios, explicitly overrideable:

| scenario | tenants | active | query rate | ingest lanes / flush |
|---|---:|---:|---:|---:|
| steady | 1,000,000 | 1% | 1/min | 256 / 10 s |
| dashboard-surge | 1,000,000 | 10% | 1/min | 256 / 10 s |
| backfill | 1,000,000 | 1% | 1/min | 2,048 / 10 s |

The provisional capacity verdict requires zero timeouts, p99 within the declared
SLO, and at least 2x headroom over steady demand / 1.25x over surge demand.
Results without a stated target and SLO are measurements, not capacity verdicts.

## Global constraints

- Rust edition 2024 and pinned workspace dependencies.
- Existing behavior and tests stay green after every task.
- Manual-only runs; JSON reports append under bench/results/catalog-*.json.
- At least two runs per matrix point; interleave comparisons.
- Every result records offered/started/completed/failed/timed-out ops, p50/p95/
  p99/max, queue depth, conflicts, returned rows/bytes, pool topology, catalog
  state, PostgreSQL resources/settings, driver resources, and cache posture.
- Detect coordinated omission: open-loop latency starts at scheduled arrival,
  not when a worker finally begins the database call.
- If driver CPU exceeds about two cores or its queue drops work before the
  database knee, the point is invalid and the driver must be fixed.
- The benchmark writes only bench_cat_* objects in disposable compose
  PostgreSQL. Restore any live CPU limit after a run session.
- No cache, replica, index, batching, pooling, or sharding product change lands
  here. Task 6 files follow-up rows with measured motivation.

## Task 1: Demand model, fixture states, and feasible seeding

Files:

- Create crates/ukiel-e2e/src/bin/bench/catalog.rs
- Modify crates/ukiel-e2e/src/bin/bench/main.rs
- Modify crates/ukiel-catalog/src/lib.rs
- Add pure unit tests in the bench module and one ignored compose smoke test

Interfaces:

- bench catalog seed --label L --tables T --history-parts H --live-parts V
  --hot-table-live X --state fresh|mature|backlogged|pathological
  --packing-keys K --dedicated-frac F
- bench catalog demand with scenario inputs prints and serializes derived read
  QPS, write TPS, and background QPS.
- connect_with_pool_size(url, n), with connect delegating to n=16.

The seed plan must create:

- independent historical, live, and hot-table populations;
- realistic live/tombstoned rows and L0/L1+ levels;
- about 30 day partitions and configurable table/key skew;
- Int64 min/max plus realistic roaring bitmap and row-group-span payloads;
- dense bitmap-omission and dedicated-part cases;
- deterministic labels and loud mismatch refusal.

Use set-based SQL/COPY for million-row fixtures. Include a small
--via-product-api validation mode proving the synthetic rows match the schema
and query behavior; do not use it for 10M seeding.

Tests:

- demand arithmetic and capacity-gate boundaries;
- deterministic state generator and exact live/history counts;
- dedicated fraction, L0/L1+, bitmap presence/omission, and hot-table skew;
- connect delegation;
- ignored seed/read smoke.

Commit:

    bench: catalog demand model and realistic bulk-seeded states

## Task 2: Closed- and open-loop workload engine

Interfaces shared by read, write, conflict, mixed, and connection workloads:

- --mode closed|open
- --workers N or --rate R
- --duration S --warmup S --timeout-ms N
- --pools N --connections-per-pool N
- --key-dist uniform|zipf
- --label L

Open-loop scheduling records latency from scheduled arrival, bounded queue depth,
late starts, dropped submissions, completed work after the offered phase, and
recovery time. It must not allocate one Tokio task per arrival without a bound.

Workloads:

1. read: live_parts_pruned with key-only, key+time, and unscoped/operator mix.
   Optionally run actual plan-16 bitmap decode/filter. Split SQL+deserialize from
   client filtering and report result rows/bytes.
2. write: commit_with_offsets on distinct lanes, configurable parts/commit, and
   max_live_l0_parts at the real flush-tick ratio. Record start/end cardinality.
3. conflict: same partition, same hypertable/different partitions, and Zipf-hot
   partitions. Re-read/replan/backoff matches the current compactor behavior.
   Report attempts, successful swaps, conflicts, rollback work, and latency per
   useful success.
4. mixed: demand-scenario read/write load plus changes_since, cursor advance,
   live-part replanning, finalization, pending/tombstone GC queries, and
   monitoring aggregates at declared tick rates.
5. connection: concurrently create process-equivalent pools and measure
   establishment, exhaustion, steady admission, and recovery.

Tests:

- open-loop scheduler does not coordinate-omit queued latency;
- bounded queue and timeout accounting conservation;
- Zipf sanity;
- mixed-rate derivation;
- conflict useful-work accounting.

Commit:

    bench: catalog open-loop surges and process-equivalent pool topology

## Task 3: Saturation and resource attribution

Run a side observer connection outside every measured pool. Sample approximately
every 250 ms:

- pg_stat_activity state and wait events;
- pg_stat_database transaction, block, temp, and conflict deltas;
- locks and long transactions;
- WAL bytes/fsync/checkpoint counters available in PostgreSQL 17;
- every pool's size, idle count, and waiters where exposed;
- PostgreSQL cgroup CPU/memory/I/O counters;
- driver process CPU and RSS.

Record SHOW ALL once per report header plus PostgreSQL version, max_connections,
shared_buffers, storage driver/filesystem, container CPU limit, host RAM, and
dataset-to-RAM ratio.

A separate pool-acquire probe is a canary only. Do not label it as the workload's
acquire distribution. The derived saturation class may be pool-queue,
connection-limit, PostgreSQL CPU, lock, WAL/fsync, data I/O, or driver; preserve
raw samples so the classification can be challenged.

Record EXPLAIN (ANALYZE, BUFFERS, WAL) for representative live_parts_pruned and
multi-part commit shapes at every observed cardinality knee.

Tests:

- pure folding over synthetic samples;
- CPU counter delta conversion;
- dominant-class classification with ambiguous/mixed fallback;
- JSON backward compatibility.

Commit:

    bench: catalog saturation attribution across pools, CPU, WAL and IO

## Task 4: Runbook, CPU limits, and cache posture

The benchmark records but does not mutate Docker CPU limits. Document:

- cores sweep and mandatory restore to unlimited;
- PostgreSQL hot-cache run;
- cold/evicted-cache procedure, labeled destructive to cache state;
- multi-pool topology sweep;
- open-loop arrival sweep around the closed-loop knee;
- fixture disk estimates and bulk-seed time;
- how to stop safely and preserve append-only reports.

A run is invalid if the recorded CPU limit/settings differ from its label.

Commit:

    bench: catalog scalability runbook and self-describing environment

## Task 5: Measurement matrix

Run release builds, at least twice per point.

### 5.1 Cardinality and state

At representative 10K, 1M, and 10M historical rows, compare fresh,
mature-compacted, and backlogged states. Hold total history while varying live
fraction; hold live total while varying hot-hypertable rows. Measure read surge
with uniform/Zipf keys and key-only/key+time shapes. Record returned rows/bytes
and EXPLAIN at each knee.

Deliverable: distinguish history/vacuum/index-bloat cost from live-index,
hot-table, JSONB, transfer, and client-decode cost.

### 5.2 Fleet topology and connection surge

Compare 1x16, 4x16, 32x8, and 64x4 (bounded by PostgreSQL max_connections),
then a same-total-connections topology comparison. Run connection-surge and
steady read admission.

Deliverable: local pool queue versus global connection wall; evidence for a
fleet budget or PgBouncer, not merely a larger per-process pool.

### 5.3 Cores and offered load

At the middle mature state, sweep 1/2/4/8 PostgreSQL cores. First find closed-
loop knees for reads and writes; then drive open-loop rates below, at, and above
each knee. Measure queue growth, p99, timeouts, shedding, and post-surge recovery.

Deliverable: sustainable QPS/TPS per core under the SLO, not only maximum ops/s.

### 5.4 Write shape and conflicts

Sweep write workers/rates and parts-per-commit. Attribute current per-part INSERT
cost. Run all three conflict shapes at 2/8/32 contenders with product backoff.

Deliverable: conflict-free commit TPS, useful replacement TPS, rollback tax, and
whether batching or sharding earns a follow-up.

### 5.5 Mixed product envelopes

Run steady, dashboard-surge, and backfill scenarios with all background ticks.
Measure which foreground SLO fails first and the sampler's attribution. Include
hot and cold PostgreSQL cache postures where the dataset/working-set ratio makes
both meaningful.

Commit measured tables and raw-report labels to the scalability note:

    bench: catalog matrix recorded - demand headroom and saturation knees

## Task 6: Verdicts and close-out

Each option receives pursue/decline/not-yet-measurable with the deciding number:

- per-process pool knob plus fleet-wide connection budget;
- PgBouncer;
- batched part inserts;
- DDL metadata cache;
- live-parts snapshot cache;
- read replicas;
- typed stats columns/index changes;
- feed horizon and/or physical partitioning;
- hypertable sharding.

Cache and replica verdicts must address two separate invariants:

1. deletion safety: lag + query lifetime < tombstone grace;
2. freshness: replica/cache lag <= declared freshness SLO.

A live-parts cache additionally requires atomic snapshot plus feed high-watermark
bootstrap, bounded memory, read-your-writes behavior, lag breach behavior, and
failover semantics. Tombstone grace alone never authorizes missing new parts.

Sharding is pursued only when useful primary commit TPS misses a named demand
scenario after cheaper fixes. A pool knob is pursued only with a global
connection budget.

Update:

- docs/notes/2026-07-13-catalog-scalability.md with measured tables/verdicts;
- bench/README.md with commands and headline results;
- roadmap row 40 to Executed;
- rows 8, 23, and 26 with any gate the measurements establish;
- new follow-up rows only for options that earned them.

Run fmt, clippy, make test, restore PostgreSQL CPU limits, and commit:

    docs: catalog scalability measured - demand ceilings and option verdicts

## Exit criteria

Plan 40 is complete only when:

- the three demand scenarios have explicit pass/fail headroom;
- open-loop and closed-loop knees are both recorded;
- process-equivalent multi-pool topology is measured;
- mature history/live separation and backlogged state are measured;
- read, write, conflict, mixed, and connection workloads have saturation causes;
- every option has a numbered verdict;
- no stale-read option is recommended using tombstone grace as a substitute for
  freshness semantics;
- all raw reports are labeled and reproducible, and the CPU limit is restored.

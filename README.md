Ukiel is a lake. Here, a data lake. In real world it's a lake in Olsztyn, Poland.

Experimental. Not production ready. Testing ideas from my head.

# Genesis

I want an _infinitely_ horizontally scalable, distributed, S3 based, compute-storage separated system.

The experiences at the moment:
1. My 1.5 year of running PB+ scale ClickHouse at PostHog
2. My 3 years of building Oxla - a distributed, S3 based, compute-storage separated database.

ClickHouse OSS at some scale is causing lot of troubles that are not easily fixable,
the company iself is going in a direction that could be hard for us.

Why not Iceberg? It's overcomplicated and there are problems I'm not gonna solve
(e.g. S3 Iceberg table does not support planScan).
DeltaLake is IMHO more prod ready than Iceberg, but I perceive both as an interop solution.
It seems to me that the way those work is: each query engine uses the catalog to exchange the data 
but builds all the mechanics above it.
Both could be added as external sync later.

But all in all, i just want something to experiment quickly without a fuss. I don't really expect this to go production
soon, but i'd like to run it in prod to test query performance.

# The idea:

1. Using Apache DataFusion as query engine.
2. Use Parquet as on disk data format.
3. When inserting data, create files using defined table schema but optimize the Parquet files.
4. Create a custom catalog to manage files and support efficient paritioning
5. what we want to achieve is either multitenancy at file managment level or scalable table-tenancy
   (meaning having table customized per tenant)
6. Catalog must support MV -> events on new data parts
7. Replace on catalog files
8. Q: how to handle deletes? as the database would be multitenant, we could have a worker rewriting the data, that's all.
9. conflicts between: mutations and merges

## Development

Rust workspace. Design spec: `docs/superpowers/specs/2026-07-05-ukiel-design.md`,
plans: `docs/superpowers/plans/`, concept notes: `docs/notes/`
(e.g. [schema management](docs/notes/schema-management.md)).
Plans follow the house format — the `write-plan` skill
(`.claude/skills/write-plan/`) holds the process and template for writing
one from a roadmap row.

### Benchmarks

Volume-scale benchmarks run against the compose stack via the `bench` binary —
runbook, tooling, and all recorded results in
[`bench/README.md`](bench/README.md). Two families: ukiel's own per-tenant SaaS
scenarios (namespace-scoped, through the HTTP endpoint — the product's real query
shape), and the **official upstream suites** (ClickBench's 43 queries, JSONBench's
5), vendored with a per-query adaptation log.

**Official headline (2026-07-13):** on ClickBench at 100M rows, over identical
local files, ukiel's full stack — catalog planning, provider, tenant-isolation
machinery, caches — runs the 41 stack-comparable queries at **0.99× bare
DataFusion 54** (27.4 s vs 27.7 s, median per-query 0.98×, every answer
identical, ukiel ahead on 24 of 41). The plan-32 first measurement said 2.16×
and "beats it on nothing"; the gap decomposed into transport (fixed by the
plan-15 cache tier, 94% recovered), a doubly-evaluated predicate (plan 37),
and a `Utf8`-vs-`Utf8View` opt-out (plan 39) — full decomposition in
`docs/notes/2026-07-12-official-suite-gap.md`. Calibration (plan 34): on the
same parquet, **DataFusion outruns ClickHouse** (24.7 s vs 31.5 s); ClickHouse's
benchmark lead is MergeTree's storage format (~1.4×) — the named, deliberate
cost of the open-format lakehouse shape. (All reference runners are in the
repo: `bench clickbench raw|compare`, `bench clickhouse …` — check us.)

**House headline (the product's query shape):** catalog pruning makes
per-tenant queries *cheaper than bare DataFusion* on the same files —
key-filtered queries run 3–4.5× faster than raw, and the plan-16 key bitmaps
make pruning exact: the median 100M-row tenant plans **7 files instead of
108** (−93%), sparse-tenant scan scenarios drop 53–73%, and an in-range tenant
with zero rows touches zero objects. The plan-13 footer cache had already cut
per-tenant read latency ~30–40% by removing the footer-fan-out floor.

## Requirements

This repo needs Rust. I run it on the newest stable version. Beware, the dependiecies for this project are crazy (DataFusion, Parquet, etc.)
and they can easily 80-100GB of disk space in ./target directory (sic).

I develop on Apple M3 36GB ram and AMD Ryzen AI 9 HX 370, 96GB ram.

### Quickstart

```sh
make play           # compose stack (Kafka/Postgres/MinIO) + the ukield server
```

Then, in another terminal, produce an event and query it back:

```sh
echo '{"tenant_id": 1, "ts": 1751800000000, "payload": "hello"}' | \
  docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server localhost:9092 --topic events

curl -s -X POST http://127.0.0.1:8080/api/query \
  -H 'content-type: application/json' \
  -d '{"namespace_id": 1, "sql": "SELECT payload FROM events"}'
```

Tables, namespaces, and server roles are declared in `ukield.example.toml`.

Results come back **columnar JSON by default** (`{"schema": [...], "columns":
{col: [...]}, "row_count": N}` — cheaper and chart/dataframe-ready); add
`"format": "rows"` for row-oriented JSON or `"format": "arrow"` (or `Accept:
application/vnd.apache.arrow.stream`) for a lossless Arrow IPC stream — see
`docs/notes/2026-07-06-columnar-results.md`. Browse the schema over the same
endpoint: `SELECT table_name FROM information_schema.tables`.

- `crates/ukiel-core` — shared types (ids, parts, commit ops).
- `crates/ukiel-catalog` — Postgres-backed catalog: transactional part commits
  (ADD / REPLACE / DELETE with optimistic concurrency), packing-key-pruned
  part listing, ordered change feed. The catalog is tenancy-agnostic:
  logical tables live in namespaces, parts cover packing-key ranges;
  multitenancy is a deployment mapping (namespace = tenant).
- `crates/ukiel-ingest` — Kafka → sorted ZSTD Parquet L0 files → catalog
  commits. Exactly-once: Kafka offsets are stored in the catalog in the same
  transaction as the part commit; workers resume from catalog offsets and
  never commit Kafka group offsets.
- `crates/ukiel-query` — DataFusion query serving: catalog-planned Parquet
  scans (no object-store listings; range stats + exact packing-key bitmaps +
  row-group key spans prune before any I/O), namespace isolation injected
  into the physical plan, `Utf8View` string scans (engine-default parity),
  HTTP SQL endpoint (`POST /api/query`), process-wide Parquet footer cache,
  and a local-disk cache tier with write-through prewarm and chunked
  large-object caching in front of the object store.
- `crates/ukiel-compactor` — background rewrite workers on the change feed:
  fanout-driven level-ladder compaction (a run = one commit's key-disjoint
  output; runs merge a level up at a two-tier trigger — eager `l0_fanout`
  for the unpruned L0 files, lazier `fanout` for L1+), placement policies
  `packed` / `separated` / size-targeted (`target_file_mb`: small tenants
  share files, heavy tenants separate organically), cold-partition
  finalization into a single key-disjoint run, and atomic key deletion
  (metadata-only for dedicated files, rewrite for packed ones). All swaps
  are optimistic REPLACE commits; losers replan from fresh state.
  See `docs/notes/2026-07-06-lsm-hierarchy.md`.
- `crates/ukiel-expr` — deterministic SQL expression engine for column
  specs: `default` / `materialized` (computed at write + recomputed on
  rewrites = organic backfill) / `alias` (computed at query time). See
  `docs/notes/2026-07-06-materialized-columns.md`.
- `crates/ukiel-gc` — object garbage collection: reaps tombstoned parts'
  objects after a grace period (fenced by worker cursors) and sweeps orphaned
  uploads via a write-ahead intent table (`pending_objects`) — no object-store
  listing on the hot path. A rare `reconcile` pass lists the bucket as
  defense-in-depth. Purged parts keep catalog rows for feed replay.
- `crates/ukiel-e2e` — end-to-end suite against the docker-compose stack
  (Kafka + Postgres + MinIO); tests are `#[ignore]`d by default. Philosophy
  and scenario catalog: `docs/superpowers/specs/2026-07-05-ukiel-testing-design.md`.
- `crates/ukield` — the all-in-one server binary: ingest + query + compactor
  + GC in one process (per-role deployment via `roles = [...]`), idempotent
  table bootstrap from TOML config, graceful shutdown. Exposes `GET /metrics`
  (Prometheus) and `GET /readyz` (catalog + object-store reachability) on the
  query listener, plus a standalone `metrics_listen` port for processes
  running without the query role; a periodic collector adds consumer/feed
  lag, backlog, GC, pool, and cache-dir gauges (monitoring P2, plan 21).

Tests: `cargo test` (integration tests spin up Postgres/Kafka via
testcontainers — Docker must be running). End-to-end: `make e2e`.

Known issues: `docs/issues/` (0001: query endpoint trusts caller-supplied
namespace — do not expose to untrusted callers until auth lands).


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

Results come back row-oriented JSON by default (with an additive `schema`
block); add `"format": "columns"` for columnar JSON or `"format": "arrow"`
(or `Accept: application/vnd.apache.arrow.stream`) for a lossless Arrow IPC
stream — see `docs/notes/2026-07-06-columnar-results.md`. Browse the schema
over the same endpoint: `SELECT table_name FROM information_schema.tables`.

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
  scans (no object-store listings), namespace isolation injected into the
  physical plan, HTTP SQL endpoint (`POST /api/query`), local-disk
  read-through cache in front of the object store.
- `crates/ukiel-compactor` — background rewrite workers on the change feed:
  L0→L1 compaction honoring each hypertable's placement policy (`packed` or
  `separated` per key), and atomic key deletion (metadata-only for dedicated
  files, rewrite for packed ones). All swaps are optimistic REPLACE commits;
  losers replan from fresh state.
- `crates/ukiel-expr` — deterministic SQL expression engine for column
  specs: `default` / `materialized` (computed at write + recomputed on
  rewrites = organic backfill) / `alias` (computed at query time). See
  `docs/notes/2026-07-06-materialized-columns.md`.
- `crates/ukiel-gc` — object garbage collection: reaps tombstoned parts'
  objects after a grace period (fenced by worker cursors so lagging feed
  consumers are safe) and sweeps never-committed orphans left by crashed or
  conflicted writers. Purged parts keep catalog rows for feed replay.
- `crates/ukiel-e2e` — end-to-end suite against the docker-compose stack
  (Kafka + Postgres + MinIO); tests are `#[ignore]`d by default. Philosophy
  and scenario catalog: `docs/superpowers/specs/2026-07-05-ukiel-testing-design.md`.
- `crates/ukield` — the all-in-one server binary: ingest + query + compactor
  + GC in one process (per-role deployment via `roles = [...]`), idempotent
  table bootstrap from TOML config, graceful shutdown.

Tests: `cargo test` (integration tests spin up Postgres/Kafka via
testcontainers — Docker must be running). End-to-end: `make e2e`.

Known issues: `docs/issues/` (0001: query endpoint trusts caller-supplied
namespace — do not expose to untrusted callers until auth lands).


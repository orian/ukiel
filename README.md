Ukiel is a lake. Here, a data lake. In real world it's a lake in Olsztyn, Poland.

The idea:

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
- `crates/ukiel-gc` — object garbage collection: reaps tombstoned parts'
  objects after a grace period (fenced by worker cursors so lagging feed
  consumers are safe) and sweeps never-committed orphans left by crashed or
  conflicted writers. Purged parts keep catalog rows for feed replay.
- `crates/ukiel-e2e` — end-to-end suite against the docker-compose stack
  (Kafka + Postgres + MinIO); tests are `#[ignore]`d by default. Philosophy
  and scenario catalog: `docs/superpowers/specs/2026-07-05-ukiel-testing-design.md`.

Tests: `cargo test` (integration tests spin up Postgres/Kafka via
testcontainers — Docker must be running). End-to-end: `make e2e`.

Known issues: `docs/issues/` (0001: query endpoint trusts caller-supplied
namespace — do not expose to untrusted callers until auth lands).


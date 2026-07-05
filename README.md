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
plans: `docs/superpowers/plans/`.

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

Tests: `cargo test` (integration tests spin up Postgres via testcontainers —
Docker must be running).


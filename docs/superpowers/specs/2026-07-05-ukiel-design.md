# Ukiel — Multitenant Data Lake: Design & V1 Plan

## Context

Ukiel (README.md) is a data lake backend for a SaaS product: DataFusion as the query engine, Parquet on object storage, and a **custom catalog** that makes multitenancy a file-management concern. Requirements settled during brainstorming:

- Tenant-facing SQL analytics; **1M tenants**, horizontal scaling is the defining constraint
- Mixed workload: append-heavy events + mutable dimension tables
- Source of truth today is **Kafka**; freshness adjustable, target **<10–20s** optimistic path
- Object store + 2-layer cache (local NVMe + distributed)
- Hybrid mutation handling (fast tombstones + background rewrites, resource-driven)
- Generic **MV mechanism** (tenant- and system-defined) driven by catalog change events
- **V1 proves the core: write → catalog → query** (MVs/mutations/caches designed-for, stubbed)

Chosen approach (over Iceberg-style file manifests or reusing Iceberg/Delta): **shared hypertables + database-backed catalog**. File-based catalogs can't survive 1M tiny tenants × 10s commits; a DB catalog makes packing, replace semantics, and change feeds one transactional problem.

## Architecture

```
Kafka ──► Ingest workers ──► Object store (Parquet parts)
                │ commit            ▲ read via cache (NVMe → distributed → S3)
                ▼                   │
           Catalog service ◄── Query nodes (DataFusion) ◄── tenant SQL (HTTP / Arrow Flight)
                │ change feed
                ▼
    Background workers: compaction · mutation-rewrite · MV maintenance
```

All components horizontal: ingest scales with Kafka partitions, query nodes stateless, workers queue-driven, catalog DB shards by hypertable.

### Catalog data model (the heart)

- **Hypertable** — physical table family: Arrow schema, partition spec (e.g. `(tenant_bucket, day)`), sort order (`tenant_id, ts`). Thousands exist, not millions.
- **Logical table** — tenant-visible: `(tenant_id, name) → hypertable + tenant key + column mapping`. Per-tenant schema customization = column mapping over a wider hypertable schema, or a dedicated hypertable for heavy tenants. 1M tenants = cheap DB rows.
- **Part** — one Parquet file: path, partition values, tenant_id min/max (packed files span many tenants, dedicated files one), row count, size, column stats, **level** (L0 fresh → L2 compacted).
- **Commit** — atomic `ADD` / `REPLACE(old→new)` / `DELETE` of parts with optimistic concurrency: REPLACE fails if any input part is not live; loser retries on fresh state. This one rule resolves mutation-vs-compaction conflicts (README pt 9).
- **Change feed** — per-hypertable ordered log of commit events (`parts_added`, `parts_replaced`, seq no). MV/compaction workers are durable cursors. (README pt 6.)

Query planning = one indexed catalog query (*live parts where partition ∈ P and tenant range ∋ T*), never an object-store listing.

### Ingest (Kafka → Parquet)

Ingest workers own Kafka partition subsets; buffer rows per (hypertable, partition); flush every ~10s (per-table knob) as sorted, ZSTD Parquet L0 files; then commit to catalog and advance Kafka offsets. Exactly-once via commit idempotency keys (`kafka partition + offset range`) — a re-flush after crash is deduped by the catalog. Read-your-writes = data visible at next commit (~flush interval).

### Query path

Custom DataFusion `CatalogProvider`/`TableProvider`: resolve tenant's logical table → prune parts via catalog → scan Parquet with `tenant_id` predicate pushed down (files sorted by tenant → row-group pruning is effective). Tenant isolation enforced by injecting the tenant filter at plan time (not SQL rewriting); quotas/limits per tenant at the session level. V1 cache: local NVMe read-through cache; distributed cache tier is an interface with a no-op impl.

### Mutations (hybrid) & compaction

- Deletes/updates land immediately as **tombstone parts** (delete vectors keyed by row position or key); readers merge them (dimension tables are small — cheap).
- Background **rewrite workers** fold tombstones into plain Parquet when resources allow (priority: tables with high tombstone ratio; GDPR deletes get deadlines).
- **Compaction workers** merge L0→L1→L2, re-sorting/packing by tenant; big tenants naturally separate into dedicated files.
- Both are just change-feed consumers issuing REPLACE commits; optimistic concurrency arbitrates.
- V1: copy-on-write REPLACE + compaction only; tombstone read-merge stubbed behind the TableProvider interface.

### MVs

MV = durable change-feed cursor + SQL transform + target logical table. MV worker reads new parts, runs the transform through DataFusion scoped to those parts, appends results. Insert-only incremental in v1 semantics; REPLACE events trigger re-derivation of affected MV partitions (post-v1). Tenant and system MVs share the machinery. V1: change feed fully implemented + one demo system MV; MV service productization later.

## V1 implementation plan (Rust workspace)

Crates: `ukiel-catalog` (model + Postgres impl + commit protocol + change feed), `ukiel-ingest` (Kafka consumer, buffering, Parquet writer), `ukiel-query` (DataFusion providers, tenant scoping, server: HTTP + Arrow Flight SQL), `ukiel-compactor` (change-feed-driven L0 merging), `ukiel-core` (shared types).

Build order:
1. `ukiel-core` + `ukiel-catalog`: schema (hypertables, logical_tables, parts, commits, change_feed), commit protocol with optimistic REPLACE, Postgres via sqlx; property tests for commit conflicts.
2. `ukiel-ingest`: Kafka → sorted Parquet L0 → commit, idempotent offsets; integration test with testcontainers (Kafka + Postgres + MinIO).
3. `ukiel-query`: TableProvider with catalog pruning + tenant filter injection; SQL endpoint; NVMe read-through cache.
4. `ukiel-compactor`: change-feed cursor, L0→L1 merge, REPLACE commits racing ingest under test.
5. End-to-end: docker-compose (Kafka, Postgres, MinIO); demo: N simulated tenants stream events → query each tenant's slice → verify isolation, freshness <20s, compaction shrinks part count.

## Verification

- Unit/property tests per crate (commit-conflict properties especially).
- Integration: testcontainers e2e — produce to Kafka, assert rows queryable <20s, assert tenant A can't see tenant B, run compactor concurrently with ingest and assert no lost/duplicated rows.
- `cargo clippy` + `cargo test` green.

## Out of scope for v1 (designed-for, stubbed)

Distributed cache tier, tombstone merge-on-read, MV service, per-tenant quotas/billing, catalog DB sharding (schema is shard-ready), schema evolution tooling.

## First actions on approval

1. Save this design as `docs/superpowers/specs/2026-07-05-ukiel-design.md`; commit it with README + CLAUDE.md as the initial commit.
2. Scaffold the Cargo workspace and start step 1 (catalog crate).

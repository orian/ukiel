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

The catalog core is **tenancy-agnostic**. It knows two generic concepts: **namespaces** (own logical tables) and a per-hypertable **packing key** (an i64 column whose min/max every part records, used for range pruning and file packing). Multitenancy is a deployment mapping, not part of the engine spec: namespace = tenant, packing key = the `tenant_id` column. Non-multitenant embedders map namespaces to datasets/teams and pack by any i64 key (string keys are hashed by the embedder).

- **Hypertable** — physical table family: Arrow schema, partition spec (e.g. `(key_bucket, day)`), sort order (`tenant_id, ts`), declared packing key. Thousands exist, not millions.
- **Logical table** — what a client sees: `(namespace_id, name) → hypertable + column mapping`. Per-table schema customization = column mapping over a wider hypertable schema, or a dedicated hypertable for heavy namespaces. 1M namespaces = cheap DB rows.

#### Hypertable vs logical table

One physical storage pool shared by many virtual tables.

A **hypertable is physical storage**: it owns the Parquet files and defines the on-disk schema, sort order, partition spec, and packing key. There is roughly one per *kind of data*, not per customer. Ingest writes files into hypertables; compaction, deletion, and retention workers operate on hypertable files.

A **logical table is what a client names**: a catalog row saying "namespace 42's table `events` is a slice of hypertable `events_v1`". It owns no files — it is a pointer plus access scope. Millions exist; each is one cheap DB row.

```
Hypertable "events_v1"                      (physical, 1 of ~thousands)
  schema: tenant_id, ts, payload · packing key: tenant_id
  parts: ht/7/L0/a.parquet   [key range 100–4500]   ← packed, many tenants
         ht/7/L1/b.parquet   [key range 4200–4200]  ← one big tenant

Logical table (namespace=100, "events") ──┐
Logical table (namespace=101, "events") ──┼──► all point at hypertable "events_v1"
Logical table (namespace=4200, "events") ─┘
```

Query path for tenant 101's `SELECT * FROM events`:

1. Resolve logical table `(namespace 101, "events")` → hypertable `events_v1` — *which physical pool?*
2. Catalog lookup: live parts of `events_v1` whose packing-key range contains 101 — *which files could hold their rows?* (`a.parquet` yes, `b.parquet` no)
3. Scan those files with `tenant_id = 101` injected as a mandatory plan-time filter — *which rows inside the packed file are theirs?*

The logical table answers "what does this client's name refer to, and what slice may they see"; the hypertable answers "how are bytes laid out, sorted, packed, and compacted". This split is what makes the 1M-tenant math work: physically-per-tenant tables would mean millions of tiny files and metadata trees (rejected Approach B), while packing lets tiny tenants share files and big tenants graduate to dedicated ones — invisibly, because the client-facing logical table never changes; only which parts hold its rows does.

`column_mapping` (currently always `None`) is the reserved hook for per-tenant schema customization: a logical `events` could expose a renamed/subset view over a wider hypertable schema, or a heavy tenant can get a dedicated hypertable entirely.
- **Part** — one Parquet file: path, partition values, packing-key min/max (packed files span a key range, separated files exactly one key), row count, size, column stats, **level** (L0 fresh → L2 compacted).
- **Commit** — atomic `ADD` / `REPLACE(old→new)` / `DELETE` of parts with optimistic concurrency: REPLACE fails if any input part is not live; loser retries on fresh state. This one rule resolves mutation-vs-compaction conflicts (README pt 9).
- **Change feed** — per-hypertable ordered log of commit events (`parts_added`, `parts_replaced`, seq no). MV/compaction workers are durable cursors. (README pt 6.)

Query planning = one indexed catalog query (*live parts where partition ∈ P and packing-key range ∋ k*), never an object-store listing.

### Ingest (Kafka → Parquet)

Ingest workers own Kafka partition subsets; buffer rows per (hypertable, partition); flush every ~10s (per-table knob) as sorted, ZSTD Parquet L0 files; then commit to catalog and advance Kafka offsets. Exactly-once via commit idempotency keys (`kafka partition + offset range`) — a re-flush after crash is deduped by the catalog. Read-your-writes = data visible at next commit (~flush interval).

### Query path

Custom DataFusion `CatalogProvider`/`TableProvider`: resolve tenant's logical table → prune parts via catalog → scan Parquet with `tenant_id` predicate pushed down (files sorted by tenant → row-group pruning is effective). Tenant isolation enforced by injecting the tenant filter at plan time (not SQL rewriting); quotas/limits per tenant at the session level. V1 cache: local NVMe read-through cache; distributed cache tier is an interface with a no-op impl.

### Mutations (hybrid) & compaction

- Deletes/updates land immediately as **tombstone parts** (delete vectors keyed by row position or key); readers merge them (dimension tables are small — cheap).
- Background **rewrite workers** fold tombstones into plain Parquet when resources allow (priority: tables with high tombstone ratio; GDPR deletes get deadlines).
- **Compaction workers** merge L0→L1→L2, re-sorting by packing key; heavy keys naturally separate into dedicated files.
- Both are just change-feed consumers issuing REPLACE commits; optimistic concurrency arbitrates.
- V1: copy-on-write REPLACE + compaction only; tombstone read-merge stubbed behind the TableProvider interface.

### Placement policy: per-key deletion & retention

Per-customer data deletion (GDPR) and per-customer retention favor physical separation; the small-file problem at 1M tiny tenants favors packing. Resolution: **placement is a per-hypertable policy, not an architecture choice**, and the catalog supports both shapes natively (a separated file is just a part with `packing_key_min == packing_key_max`).

- **L0 is always packed** — ingest at a 10–20s flush cadence cannot maintain a million open writers. L0 files are short-lived (minutes–hours until compaction).
- **At rest (L1+), policy per hypertable**: `separated` — compaction splits every packing key into its own files (SaaS default; the right choice when deletion/retention SLAs dominate); or `packed` — small keys share files, sorted by packing key so each key's rows occupy contiguous row groups.
- **Deletion of key k**: separated files → metadata-only DELETE commit (drop k's files); packed L0/L1 files overlapping k → REPLACE-rewrite without k's rows. Sorted packing makes rewrites cheap: untouched row groups are copied byte-wise, no decode. Worst-case rewrite scope = the hot window, not the lake.
- **Retention**: policy per logical table (namespace). Separated + time-partitioned data → expiry is metadata-only drops of aged parts. Packed data → retention-aware compaction filters expired rows on every pass, plus a sweep worker that rewrites any file whose expired fraction crosses a threshold.
- Cost of `separated` at rest: file count scales with active namespaces × partitions. Mitigation: size-based rolling (a small namespace's file spans many days until it reaches target size), so tiny namespaces produce few files, not one per day.
- All of this runs on the same change-feed + REPLACE machinery as compaction; deletion and retention workers are just more commit authors, arbitrated by optimistic concurrency.

### MVs

MV = durable change-feed cursor + SQL transform + target logical table. MV worker reads new parts, runs the transform through DataFusion scoped to those parts, appends results. Insert-only incremental in v1 semantics; REPLACE events trigger re-derivation of affected MV partitions (post-v1). Tenant and system MVs share the machinery. V1: change feed fully implemented + one demo system MV; MV service productization later.

## V1 implementation plan (Rust workspace)

Crates: `ukiel-catalog` (model + Postgres impl + commit protocol + change feed), `ukiel-ingest` (Kafka consumer, buffering, Parquet writer), `ukiel-query` (DataFusion providers, tenant scoping, server: HTTP; Arrow Flight SQL post-v1), `ukiel-compactor` (change-feed-driven L0 merging), `ukiel-core` (shared types).

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

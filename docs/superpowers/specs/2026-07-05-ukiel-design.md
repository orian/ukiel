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

- **Hypertable** — physical table family: Arrow schema, partition spec (e.g. `(key_bucket, day)`), sort order (`tenant_id, ts`), declared packing key, placement policy (see below). Thousands exist, not millions.
- **Logical table** — what a client sees: `(namespace_id, name) → hypertable + column mapping`. Per-table schema customization = column mapping over a wider hypertable schema, or a dedicated hypertable for heavy namespaces. 1M namespaces = cheap DB rows.
- **Part** — one Parquet file: path, partition values, packing-key min/max (packed files span a key range, separated files exactly one key), row count, size, column stats, **level** (L0 fresh → L2 compacted), and GC state (`purged_at`).
- **Commit** — atomic `ADD` / `REPLACE(old→new)` / `DELETE` of parts with optimistic concurrency: REPLACE fails if any input part is not live; loser retries on fresh state. This one rule resolves mutation-vs-compaction conflicts (README pt 9).
- **Change feed** — per-hypertable ordered log of commit events. Background workers (compaction, MVs) are **durable cursors** (`worker_cursors`); the feed replays from any point because part rows are never deleted, only stamped purged. (README pt 6.)

Query planning = one indexed catalog query (*live parts where partition ∈ P and packing-key range ∋ k*), never an object-store listing.

Schema is owned by the hypertable row (JSON; see `docs/notes/schema-management.md`). Columns can be plain, `default`, `materialized`, or `alias` expressions (`docs/notes/materialized-columns.md`) — write-time expressions are recomputed on every rewrite, which doubles as organic backfill after additive schema changes.

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

### Ingest (Kafka → Parquet)

Ingest workers own Kafka partition subsets; buffer rows per (hypertable, day); flush every ~10s (per-table knob) as sorted, ZSTD Parquet L0 files; then commit to the catalog. **Exactly-once via catalog-anchored offsets**: Kafka offsets are stored in `ingest_offsets` in the *same transaction* as the part commit, workers seek Kafka from catalog offsets on startup, and Kafka consumer-group offsets are never committed — so a crash anywhere replays from exactly the right position. (Commit idempotency keys exist as a secondary mechanism for other writers.) Poison messages are skipped while their offsets still advance. Read-your-writes = data visible at next commit (~flush interval).

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
- **At rest (L1+), policy per hypertable** (`hypertables.placement`, engine default `packed`): `separated` — compaction splits every packing key into its own files (recommended for the SaaS deployment, where deletion/retention SLAs dominate); or `packed` — small keys share files, sorted by packing key so each key's rows occupy contiguous row groups.
- **Deletion of key k**: separated files → metadata-only DELETE commit (drop k's files); packed L0/L1 files overlapping k → REPLACE-rewrite without k's rows. Sorted packing makes rewrites cheap: untouched row groups are copied byte-wise, no decode. Worst-case rewrite scope = the hot window, not the lake.
- **Retention**: policy per logical table (namespace). Separated + time-partitioned data → expiry is metadata-only drops of aged parts. Packed data → retention-aware compaction filters expired rows on every pass, plus a sweep worker that rewrites any file whose expired fraction crosses a threshold.
- Cost of `separated` at rest: file count scales with active namespaces × partitions. Mitigation: size-based rolling (a small namespace's file spans many days until it reaches target size), so tiny namespaces produce few files, not one per day.
- All of this runs on the same change-feed + REPLACE machinery as compaction; deletion and retention workers are just more commit authors, arbitrated by optimistic concurrency.

### Storage lifecycle & GC

Writers upload objects **before** committing, and REPLACE tombstones catalog rows without touching objects — so unreferenced objects accumulate by design, and GC is the other half of the storage lifecycle:

- **Reaper**: deletes objects of tombstoned parts once safe — after a `tombstone_grace` (protects queries planned just before the REPLACE; see issue 0002 for the `query_timeout < grace` invariant) and only when every registered worker cursor has passed the deleting commit (protects lagging feed consumers, precisely). Purged parts keep their catalog rows (`purged_at`) so the change feed replays forever.
- **Orphan sweep**: objects whose commit never landed (crashed writers, lost REPLACE races). Discovery is a catalog query via **write-ahead upload intent** (`pending_objects`: writers register a path before uploading; the referencing commit clears it in the same transaction) — zero object-store `LIST` on the hot path. A rare `reconcile` listing pass is the backstop for objects uploaded without registration.
- Grace periods make both safe against in-flight work; they are time-window guarantees (same trade-off as Iceberg snapshot expiration / Delta VACUUM), not reference counting.

### MVs

MV = durable change-feed cursor + SQL transform + target logical table. MV worker reads new parts, runs the transform through DataFusion scoped to those parts, appends results. Insert-only incremental in v1 semantics; REPLACE events trigger re-derivation of affected MV partitions (post-v1). Tenant and system MVs share the machinery. The cursor infrastructure (`worker_cursors`) is shared with the compactor and GC fencing; the demo MV worker is roadmap plan 8. Note: many per-row "MV" use cases are better served by materialized columns (`docs/notes/materialized-columns.md`), leaving MVs for genuine aggregations.

## Implementation status

Executed plan-by-plan; the authoritative status table is
`docs/superpowers/plans/2026-07-05-ukiel-v1-roadmap.md`. Crates
(`crates/*`): `ukiel-core` (shared types, column specs), `ukiel-catalog`
(Postgres catalog: commit protocol, change feed, offsets, cursors, GC
bookkeeping), `ukiel-ingest` (Kafka → sorted ZSTD Parquet L0, exactly-once),
`ukiel-query` (DataFusion providers, namespace isolation, HTTP SQL, NVMe
read-through cache), `ukiel-compactor` (L0→L1 with placement policy, key
deletion), `ukiel-gc` (reaper + orphan sweep), `ukiel-e2e` (scenario suite).
Planned: `ukiel-expr` + column kinds (plan 7), demo MV (plan 8), upload
intent (plan 9).

Verification philosophy and the scenario catalog (S0–S8, invariants I1–I8)
live in `docs/superpowers/specs/2026-07-05-ukiel-testing-design.md`; the e2e
suite proves isolation under packing, ≤20s freshness, crash/resume
exactly-once, compaction equivalence under racing ingest, and key deletion.

## Known gaps & deferred work

- **Auth on the query endpoint** — `namespace_id` is caller-supplied
  (issue 0001); do not expose to untrusted callers until an authenticated
  principal supplies it.
- **Statement timeout** — the GC tombstone grace assumes bounded query
  duration (issue 0002).
- Deferred, designed-for: distributed cache tier, tombstone merge-on-read,
  MV service productization, per-tenant quotas/billing, retention sweep,
  L1→L2 / size-based rolling, Arrow Flight SQL, catalog DB sharding (schema
  is shard-ready), `alter_hypertable_add_column` API (storage layer already
  handles additive evolution via schema-adapting rewrites).

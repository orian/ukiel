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
- **Part** — one Parquet file: path, partition values, packing-key min/max (packed files span a key range, separated files exactly one key), row count, size, column stats, **level** (0 = fresh ingest; ladder depth is compaction policy, not schema — plan 17), and GC state (`purged_at`).
- **Commit** — atomic `ADD` / `REPLACE(old→new)` / `DELETE` of parts with optimistic concurrency: REPLACE fails if any input part is not live; loser retries on fresh state. This one rule resolves mutation-vs-compaction conflicts (README pt 9).
- **Change feed** — per-hypertable ordered log of commit events. Background workers (compaction, MVs) are **durable cursors** (`worker_cursors`); the feed replays from any point because part rows are never deleted, only stamped purged. (README pt 6.)

Query planning = one indexed catalog query (*live parts where partition ∈ P and packing-key range ∋ k*), never an object-store listing.

Schema is owned by the hypertable row (JSON; see `docs/notes/schema-management.md`). Columns can be plain, `default`, `materialized`, or `alias` expressions (`docs/notes/2026-07-06-materialized-columns.md`) — write-time expressions are recomputed on every rewrite, which doubles as organic backfill after additive schema changes.

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

**Result formats.** `POST /api/query` returns results in one of three encodings (`docs/notes/2026-07-06-columnar-results.md`), selected by an optional `format` field (or the `Accept` header): `columns` (default; columnar JSON — one value array per column plus a `schema` block, ~half the size on wide results and chart/dataframe-ready), `rows` (row-oriented JSON with the same `schema` block), or `arrow` (Arrow IPC stream, lossless, zero re-parse for Arrow-native clients). Both JSON formats' `schema` block resolves each column's Ukiel type via an inert `ukiel:type` Arrow field-metadata hint (recovers `timestamp_ms`, which stores as `Int64`, and survives plain column references); computed columns fall back to the Arrow type. Errors are always JSON regardless of requested format. Formatting is strictly downstream of `df.collect()`, so isolation and `SQLOptions` are unaffected.

**Schema introspection.** The per-namespace session enables DataFusion's `information_schema`, so clients discover their own tables and columns over the same SQL endpoint — `SELECT table_name FROM information_schema.tables`, `SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '…'` — scoped to the namespace (a namespace only sees the logical tables it owns, with the query-time schema including `materialized`/`alias` columns). There is no separate introspection API; the SQL endpoint is the client-facing view. The operator's cross-namespace view is the catalog itself — plain Postgres tables (`hypertables`, `logical_tables`, `parts`, …) queried directly (schema JSON, placement, part counts/sizes/levels per hypertable).

### Mutations (hybrid) & compaction

- Deletes/updates land immediately as **tombstone parts** (delete vectors keyed by row position or key); readers merge them (dimension tables are small — cheap).
- Background **rewrite workers** fold tombstones into plain Parquet when resources allow (priority: tables with high tombstone ratio; GDPR deletes get deadlines).
- **Compaction workers** climb a fanout-driven level ladder (plan 17): a *run* = one commit's key-disjoint output set (derived from `created_by_commit`, no extra schema); `fanout` runs at a level merge into one run a level up, and cold partitions finalize into a single key-disjoint run. Under size-targeted placement, heavy keys separate into dedicated files organically.
- Both are just change-feed consumers issuing REPLACE commits; optimistic concurrency arbitrates.
- V1: copy-on-write REPLACE + compaction only; tombstone read-merge stubbed behind the TableProvider interface.

### Placement policy: per-key deletion & retention

Per-customer data deletion (GDPR) and per-customer retention favor physical separation; the small-file problem at 1M tiny tenants favors packing. Resolution: **placement is a per-hypertable policy, not an architecture choice**, and the catalog supports both shapes natively (a separated file is just a part with `packing_key_min == packing_key_max`).

- **L0 is always packed** — ingest at a 10–20s flush cadence cannot maintain a million open writers. L0 files are short-lived (minutes–hours until compaction).
- **At rest (L1+), policy per hypertable** — one knob, `hypertables.target_file_bytes` (plan 17; engine default packed): `packed` (NULL) — each merge output is one file, sorted by packing key so each key's rows occupy contiguous row groups; `separated` (0) — compaction splits every packing key into its own files; **size-targeted** (N bytes) — merge outputs are cut at key boundaries into ~N-byte files, so small keys share files and a key bigger than N gets dedicated `min == max` files (never split mid-key). Size-targeted subsumes both extremes (packed = ∞, separated = 0) and is the recommended SaaS setting: heavy tenants separate *organically, in proportion to their size*, and their deletion/retention becomes metadata-only without paying the small-file cost for the long tail.
- **Levels & runs** (plan 17): a *run* = one merge commit's key-disjoint output set (derived from `created_by_commit` — each L0 file is its own run). When a partition holds enough runs at a level they merge into one run a level up (~log_fanout(flushes) depth, emergent). The trigger is **two-tier** because the levels are asymmetric: a low `l0_fanout` (~4) for level 0 — L0 files span all tenants, so no key pruning applies and read amp is linear in their count, while the files are tiny and cheap to merge — and a higher `fanout` (~10) for L1+, where runs are internally key-disjoint (a query touches ≤ 1 file per run) but merges move real bytes (write amp ≈ ladder depth per byte). Same shape as RocksDB's L0-trigger vs level-multiplier split and lazy leveling's tier-young/level-last. A partition with no L0 arrivals for `finalize_after_secs` and >1 run is *finalized* into a single run one level above its max — the "level the last one" half. Terminal invariant: **a cold partition is one run** — all files pairwise key-disjoint. Read amp for one key: ≤ l0_fanout unpruned L0 files + fanout × depth pruned files hot, ~1 file per cold partition. Per-file key coverage *inverts* with depth: it widens only until accumulated bytes cross the size target, then narrows as capped files tile an ever-thinner key slice.
- **Deletion of key k**: separated files → metadata-only DELETE commit (drop k's files); packed L0/L1 files overlapping k → REPLACE-rewrite without k's rows. Sorted packing makes rewrites cheap: untouched row groups are copied byte-wise, no decode. Worst-case rewrite scope = the hot window, not the lake.
- **Retention**: policy per logical table (namespace). Separated + time-partitioned data → expiry is metadata-only drops of aged parts. Packed data → retention-aware compaction filters expired rows on every pass, plus a sweep worker that rewrites any file whose expired fraction crosses a threshold.
- Cost of `separated` at rest: file count scales with active namespaces × partitions — which is why size-targeted placement (above) is preferred: only keys that fill a target-size file get dedicated ones.
- All of this runs on the same change-feed + REPLACE machinery as compaction; deletion and retention workers are just more commit authors, arbitrated by optimistic concurrency.

### Storage lifecycle & GC

Writers upload objects **before** committing, and REPLACE tombstones catalog rows without touching objects — so unreferenced objects accumulate by design, and GC is the other half of the storage lifecycle:

- **Reaper**: deletes objects of tombstoned parts once safe — after a `tombstone_grace` (protects queries planned just before the REPLACE; see issue 0002 for the `query_timeout < grace` invariant) and only when every registered worker cursor has passed the deleting commit (protects lagging feed consumers, precisely). Purged parts keep their catalog rows (`purged_at`) so the change feed replays forever.
- **Orphan sweep**: objects whose commit never landed (crashed writers, lost REPLACE races). Discovery is a catalog query via **write-ahead upload intent** (`pending_objects`: writers register a path before uploading; the referencing commit clears it in the same transaction) — zero object-store `LIST` on the hot path. A rare `reconcile` listing pass is the backstop for objects uploaded without registration.
- Grace periods make both safe against in-flight work; they are time-window guarantees (same trade-off as Iceberg snapshot expiration / Delta VACUUM), not reference counting.

### Pipelines (ingest routing, MVs, egress)

Data movement is unified ClickHouse-style (`docs/notes/2026-07-06-pipelines.md`): tables have an **engine** — `parquet` (hypertable) or `kafka` (topic endpoint, no direct SELECT) — and a **pipeline** is a SQL query between two tables, executed batch-at-a-time through DataFusion. The four combinations: kafka→parquet = ingest (replaces route config; the transform, filtering, and partition derivation live in the SELECT), parquet→parquet = aggregation MV (change-feed cursor scoped to new parts), parquet→kafka = egress/CDC, kafka→kafka = out of scope. Delivery is a per-pipeline property: inbound stays exactly-once (catalog-anchored offsets, unchanged); outbound offers `at_most_once` / `at_least_once` (default) / `exactly_once` (Kafka transactional producer with a cursor record in the same transaction). Tenant and system pipelines share the machinery; per-row "MV" use cases belong to materialized columns (`docs/notes/2026-07-06-materialized-columns.md`), pipelines to transforms and genuine aggregations. Plan 8 implements the catalog entities plus the kafka→parquet and parquet→parquet variants; egress follows.

## Limits & guardrails (capacity model)

ClickHouse's parts/partition limits exist because parts are *physical residents* (directories, in-memory part objects on every replica, Keeper entries). Ukiel's parts are catalog rows plus S3 objects — query nodes hold no part registry and nothing lists directories — so the analogous limits surface later, as **catalog query cost and background sweep cost**:

- **Live parts per hypertable** — the parts-per-table analog. Every query plans via `live_parts` (B-tree on `(hypertable, key_min, key_max)`); range *containment* examines every live row with `min ≤ k`, so planning is O(live parts per hypertable) worst case. Comfortable to ~10⁴–10⁵ live parts. Held down structurally by the level ladder + finalization (plan 17); extended by catalog stats pruning (plan 12 makes the scan time-selective), retention, partition coalescing (below), a GiST/`int8range` index if containment scans bite first, and finally catalog sharding.
- **Partitions per hypertable** — no structural limit: a partition is a JSONB tag, not a directory. The real costs are (a) the ≥1-file-per-partition floor, (b) the finalization sweep (plan 18 moves its aggregation into SQL instead of fetching every live part), and (c) ingest fan-out: a backfill spanning D days emits D L0 files per flush. A hard per-flush partition cap is deliberately impossible — a flush's Kafka offset range covers *all* rows consumed, so exactly-once forbids flushing some days and deferring others; plan 18 adds a spread warning instead.
- **Total catalog rows** — the genuinely unbounded quantity: part and commit rows are never deleted (the change feed replays forever). **Acknowledged gap:** the feed eventually needs a horizon so purged rows can be pruned; until then the ceiling is Postgres capacity, then catalog DB sharding by hypertable (schema is shard-ready).
- **Commit rate** — one transaction per flush/merge; a single Postgres sustains low thousands of commits/s; sharding beyond.
- **Hypertables** — thousands by design; background iteration is event-gated, logical tables are millions of cheap rows.
- **Object store** — no LIST on any hot path (GC `reconcile` is the rare O(objects) backstop); object *count* is unbounded but tiny files cost per-request money and per-file scan overhead.

**Guardrails (plan 18, `2026-07-06-ukiel-guardrails.md`).** ClickHouse *enforces* its limits (delays, then rejects inserts under parts pressure); ukiel v1 trusted compaction to keep up. Plan 18 adds: **ingest backpressure** — the flusher checks the live L0 count of its target partitions and defers flushes past a slowdown threshold (halving flush rate ⇒ fewer, bigger L0 files — exactly the corrective), stopping past a stop threshold with a memory safety valve; safe because Kafka offsets only advance with flushes. Plus the **partition-spread warning** and the **SQL-side finalization candidates** query. The alarms standing in for hard limits live in the monitoring spec: live parts per level, backlog runs, backpressure deferrals, oldest unfinalized partition.

Partition granularity itself is a maintenance knob, not a query commitment (planning prunes by key range + part stats, never partition equality). Day is right for the events workload — finalization latency, merge RAM (merges hold a group in memory until streaming merge lands), retention granularity. The per-partition file floor is answered by **partition coalescing** (future plan): rewrite aged day-partitions into month-partitions on the same REPLACE machinery — the time-axis analog of the plan-17 key-axis ladder. Fully dynamic layout (parts as key × time rectangles, no calendar boxes — possible precisely because partitions are tags) is deliberately deferred: it complicates ingest buffering and merge planning for marginal benefit over coalescing.

## Backup & disaster recovery

The catalog is the single source of truth — losing Postgres means losing the lake (the bucket becomes unreferenced object soup). The architecture makes DR unusually cheap, because parts are immutable, catalog rows are never deleted, and ingest offsets live *in* the catalog. **Any Postgres PITR snapshot at time T is a consistent lake**, provided two time-window invariants (same shape as the GC grace / issue-0002 invariant):

1. **`gc.tombstone_grace > max_backup_lag`** — every object the snapshot considers live was, at worst, tombstoned after T and therefore not yet reaped when the snapshot is restored. Nothing the restored catalog references is missing.
2. **`kafka_retention > max_backup_lag`** — the restored `ingest_offsets` also rewind to T, so ingest *replays exactly the lost window* from Kafka: data ingested after T self-heals. This is the payoff of catalog-anchored offsets — the backup story for ingested data is Kafka retention, not Postgres backup frequency.

What about work committed after T? REPLACE outputs (compaction, deletion rewrites) lose their catalog rows, but their *input* parts are live again in the restored catalog — and REPLACE is equivalence-preserving, so no data is lost; the ladder simply redoes the work. The now-unreferenced post-T objects are cleaned by the orphan machinery (`reconcile` backstop; upload-intent rows also rewind to T). One operational rule: **pause GC after a restore** until one full grace has elapsed, so the reaper never acts on rewound bookkeeping. Deletions (GDPR) executed after T must be re-issued — deletion requests should be durable outside the catalog (queue/ticket), which is worth stating in the deletion-worker's eventual productization.

Not yet done (deferred): a documented restore runbook, automated `tombstone_grace >= backup_lag` validation at deploy time, and periodic restore drills. Time-travel reads (below) would give restores named, testable consistency points.

## Implementation status

Executed plan-by-plan; the authoritative status table is
`docs/superpowers/plans/2026-07-05-ukiel-v1-roadmap.md`. Crates
(`crates/*`): `ukiel-core` (shared types, column specs), `ukiel-catalog`
(Postgres catalog: commit protocol, change feed, offsets, cursors, GC
bookkeeping), `ukiel-ingest` (Kafka → sorted ZSTD Parquet L0, exactly-once),
`ukiel-query` (DataFusion providers, namespace isolation, HTTP SQL, NVMe
read-through cache), `ukiel-compactor` (L0→L1 with placement policy, key
deletion), `ukiel-gc` (reaper + orphan sweep), `ukiel-expr` (column kinds:
`default`/`materialized`/`alias`, plan 7), `ukiel-e2e` (scenario suite).
Written, not yet executed (roadmap's recommended order:
9 → 11 → 17 → 18 → 12 → 14 → 13 → 15 → 16 → 8): upload intent (plan 9),
read-performance tiers (plans 11–16), level hierarchy + size-targeted
placement (plan 17), capacity guardrails (plan 18). Engines + pipelines
(plan 8, per `docs/notes/2026-07-06-pipelines.md`) is not written — write it
after plan 18, since pipelines replace the ingest routing code 18's
backpressure lands in.

Verification philosophy and the scenario catalog (S0–S8, invariants I1–I8)
live in `docs/superpowers/specs/2026-07-05-ukiel-testing-design.md`; the e2e
suite proves isolation under packing, ≤20s freshness, crash/resume
exactly-once, compaction equivalence under racing ingest, and key deletion.

## Known gaps & deferred work

- **Auth on the query endpoint** — `namespace_id` is caller-supplied
  (issue 0001); do not expose to untrusted callers until an authenticated
  principal supplies it.
- **Statement timeout** — the GC tombstone grace assumes bounded query
  duration (issue 0002). Fix planned: plan 19
  (`docs/superpowers/plans/2026-07-06-ukiel-safety-rails.md`).
- **Concurrent ingest writers double-commit** (issue 0003) — the
  `ingest_offsets` upsert is last-writer-wins and every worker assigns all
  Kafka partitions, so two ingest processes on one topic silently duplicate
  data. Plan 19 adds an offset CAS turning this into a loud error; true
  horizontal ingest (partition-subset leases) remains future work.
- **Unbounded event time** (issue 0004) — junk partitions from bad
  timestamps plus the permanent `"unknown"` day. Plan 19 bounds event time
  at ingest (out-of-window rows skipped like poison).
- **Level ladder & size-targeted placement** — designed, plan written and
  ready to execute (plan 17,
  `docs/superpowers/plans/2026-07-06-ukiel-lsm-hierarchy.md`): fanout ladder
  over runs, cold-partition finalization, `target_file_bytes` placement.
  Supersedes the old "L1→L2 / size-based rolling" deferred item.
- **Capacity guardrails** — designed (see "Limits & guardrails"), plan
  written and ready to execute (plan 18,
  `docs/superpowers/plans/2026-07-06-ukiel-guardrails.md`): ingest
  backpressure, partition-spread warning, SQL-side finalization candidates.
- Deferred, designed-for: distributed cache tier, tombstone merge-on-read,
  MV service productization, per-tenant quotas/billing, retention sweep,
  Arrow Flight SQL, catalog DB sharding (schema is shard-ready),
  `alter_hypertable_add_column` API (storage layer already handles additive
  evolution via schema-adapting rewrites), streaming merges (compaction
  currently holds a whole merge group in RAM), stable split points across
  merges, **partition coalescing** (rewrite aged day-partitions into
  month-partitions — the answer to the per-partition file floor), **change
  feed horizon + purged-row pruning** (part/commit rows currently grow
  forever; the feed's replay-forever guarantee needs a bound before catalog
  sharding is the only lever).
- **Time travel (`AS OF` reads)** — nearly free: the catalog already
  retains full history, so a snapshot read at commit N is one predicate
  (`created_by_commit <= N AND (deleted_by_commit IS NULL OR
  deleted_by_commit > N)`), bounded by the GC grace for object
  availability. Would also fix a subtle gap: a multi-table query resolves
  each table's live parts independently, so a REPLACE landing between
  resolutions yields cross-table snapshot skew today; pinning one commit id
  per query session closes it. Also gives backup/restore named consistency
  points. Schema supports it now; deliberately unbuilt.
- **Late-data re-finalization write amp** — a finalized partition that
  receives late events correctly reopens (the quiet-check fails, the ladder
  resumes), but a slow trickle re-finalizes the whole partition per late
  batch. Acceptable while late data is rare and event time is bounded
  (issue 0004); if it becomes hot, the mitigation is routing very-late rows
  to a sibling partition folded in at coalescing time.

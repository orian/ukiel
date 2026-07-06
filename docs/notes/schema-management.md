# Schema management in Ukiel

How schema is defined, who consumes it, and how evolution will work. Reflects
the state after V1 plans 1–3; see `docs/superpowers/specs/2026-07-05-ukiel-design.md`
for the surrounding architecture.

## One source of truth

The **hypertable row in the catalog** owns the schema, stored as JSON in
`hypertables.table_schema`:

```json
{"fields": [
  {"name": "tenant_id", "type": "int64"},
  {"name": "ts",        "type": "timestamp_ms"},
  {"name": "payload",   "type": "utf8", "nullable": true}
]}
```

Supported types: `int64`, `float64`, `utf8`, `bool`, `timestamp_ms` (stored as
Int64 epoch-milliseconds in v1). `nullable` defaults to `true`. Defined once at
creation:

```rust
catalog.create_hypertable("events", &schema_json, &partition_spec, &sort_key, "tenant_id").await?
```

Three consumers derive from that row via `ukiel_core::arrow_schema_from_json`:

- **Ingest** (at flush time): JSON rows from Kafka are decoded against the
  Arrow schema, sorted, written to Parquet. Extra JSON fields in a message are
  ignored; missing nullable fields become NULL; rows missing the packing key
  or ts column as i64 are rejected.
- **Query** (at planning time): the `TableProvider` presents the Arrow schema
  to DataFusion.
- **Parquet files** carry an embedded copy of the schema they were written
  with — load-bearing for evolution (below).

## What tenants see

Tenants never touch hypertable schema. A tenant's table is a **logical
table**: `(namespace, name) → hypertable + column_mapping`. Tenant-issued DDL
is blocked at the SQL endpoint (`SQLOptions` rejects DDL/DML/statements), so
the only schema operations are catalog API calls made by the operator:

- `create_hypertable(...)` — define a new physical schema (one per *kind of
  data*, not per tenant).
- `create_logical_table(namespace, name, hypertable_id)` — give a tenant a
  table on it.

`column_mapping` (currently always `None`) is the reserved hook for per-tenant
variation: renamed columns, a subset view over a wider hypertable schema, or —
for heavy tenants — a dedicated hypertable with a genuinely custom schema.

## Evolution: not built yet, but designed

There is **no ALTER today** (v1 "designed-for, stubbed"). The constraints that
shape it:

1. **Parquet files are immutable and never rewritten for schema changes.** A
   hypertable's files will permanently span multiple schema versions.
2. **Additive, nullable changes are the safe path.** DataFusion's Parquet
   reader adapts old files to the current table schema — a column added later
   reads as NULL from files written before it existed. So
   `add_column(name, type, nullable=true)` is a pure catalog metadata update
   plus a compatibility check; no data movement.
3. **Renames and drops go through `column_mapping`, not the files.** A rename
   is a mapping change on the logical table; a "drop" hides the column while
   old files keep the bytes (which age out with retention).
4. **Type changes are the hard case**: a background rewrite worker consuming
   the change feed, REPLACE-committing converted files, arbitrated by
   optimistic concurrency — same machinery as compaction. Deliberately last.

Column kinds (`default` / `materialized` / `alias` expressions) are documented
in [materialized-columns.md](materialized-columns.md). Note that compactor
reads are schema-adapting (old files contribute NULLs for later-added
columns) and rewrites recompute write-time expressions — additive evolution
plus organic backfill already works at the storage layer; only the
`alter_hypertable_add_column` API with validation remains future work.

When needed, the plan-sized first slice is: `alter_hypertable_add_column` with
compatibility validation (nullable-or-default enforced, name collisions
rejected), a schema-version stamp on parts so files predating a column are
identifiable, and a test proving old-file reads return NULL for new columns.
It slots in any time after plan 4 (compactor) without structural changes to
ingest or query.

## Practical guidance today

- No ALTER exists: while iterating on a schema in development, create a new
  hypertable (`events_v2`) and repoint logical tables — cheap, they're catalog
  rows.
- Choose hypertable schemas wide-ish: per-tenant needs are meant to be served
  by `column_mapping` over a wider shared schema, not by per-tenant
  hypertables (reserve those for genuinely heavy tenants).
- The packing key and sort key are part of the schema contract: they must
  exist as `int64` columns in every row, and changing them later means
  rewriting files — pick them carefully at creation.

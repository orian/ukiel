# Ingest must sort by the declared sort_key (plan)

Status: **executed** (plan 27, 2026-07-09). Shipped as designed: shared
`ukiel_core::sorting` module (`sort_batch`, `sorting_columns`,
`sorted_writer_props`, `validate_sort_key`) under one canonical convention
(ascending, nulls-first); ingest sorts the Arrow batch by `hypertable.sort_key`
after defaults/materialized (so `sort_key` may include computed columns);
compactor's `rewrite.rs` delegates to the same module; invariants validated at
`create_hypertable`, bootstrap, and consumer startup. The nulls-first metadata
flip only affects files with nullable sort columns (none exist today). One
deliberate simplification vs the sketch: `validate_sort_key` takes the parsed
physical `Schema` rather than re-deriving column names, so the same call serves
the catalog, bootstrap, and consumer paths.

Follow-up to the declared-sort-order work
(43de799). That commit made compaction sort by `hypertable.sort_key` and
made the query provider *declare* per-file ordering from
`hypertable.sort_key` — but ingest still sorts L0 files by the hardcoded
pair `(packing_key, ts_column)` (`ukiel-ingest/src/writer.rs:75`,
`consumer.rs:256-261`) and never reads `sort_key`. Nothing enforces
`sort_key == [packing_key, ts_column]`, so any other configuration makes
the provider assert an ordering L0 files don't physically have, and
DataFusion — which elides sorts based on `with_output_ordering`
(`ukiel-query/src/provider.rs:199-211`) — silently returns wrong results
until compaction rewrites the files.

Fix: ingest sorts by `sort_key`, the invariants that remain (packing key
leads) get validated at registration, and the sort semantics (including
nulls) become one shared implementation.

## Design decisions

**Sort the Arrow batch, not the JSON rows.** Today `decode_rows` sorts
`Vec<serde_json::Value>` with `as_i64` extractors — that only works for
exactly two i64 columns. Instead: decode unsorted → apply
defaults/materialized → lexsort the batch by `sort_key` → encode. This

- handles any Arrow type (utf8, float, timestamps) with real columnar
  comparators instead of JSON-level i64 hacks;
- uses the exact same code path as compaction, so L0 and L1 ordering
  semantics cannot drift;
- lets `sort_key` include defaulted or materialized columns, because the
  sort runs *after* `apply_defaults_and_materialized` (the current
  pre-decode sort could never see those values).

**Share one sort implementation.** `sort_batch` (lexsort + take) and the
`SortingColumn`-stamping writer props currently live in
`ukiel-compactor/src/rewrite.rs:58-132`. Move to `ukiel-core` as a new
`sorting` module (core already has arrow; add the workspace `parquet` dep
for the metadata half):

- `sort_batch(&RecordBatch, &[String]) -> RecordBatch` — lexsort with the
  canonical `SortOptions` (below);
- `sorting_columns(&Schema, &[String]) -> Vec<SortingColumn>` — metadata
  matching those options;
- `sorted_writer_props(&Schema, &[String]) -> WriterProperties` — ZSTD +
  sorting columns, used by both writers so compression/props also stay
  identical.

Compactor's `rewrite.rs` re-exports/delegates; ingest's `finish_batch`
loses its hand-rolled two-column `SortingColumn` block.

**One nulls convention, everywhere.** Today the three layers disagree:
`lexsort_to_indices(options: None)` sorts nulls *first* (arrow default);
both parquet writers stamp `nulls_first: false`; the provider declares
`PhysicalSortExpr::new_default` = nulls *first*. Harmless while sort
columns are non-nullable i64, wrong the moment a nullable column enters
`sort_key`. Canonical choice: **asc, nulls first** (arrow/DataFusion
default — no custom options anywhere). Fix the parquet metadata stamps to
`nulls_first: true` in the shared helper; provider stays as-is.

## Invariants to enforce (registration-time validation)

The packing machinery still needs the leading column fixed:
`split_by_key`, per-key L1 files, `key_range`/`packing_key_min/max`
pruning, and the provider's isolation predicate all assume `packing_key`
is the primary sort column. So validate at table registration
(`ukiel-catalog::tables::insert_hypertable`) and re-check at bootstrap +
consumer startup (fail fast, clear error — existing catalogs predate the
validation):

1. `sort_key` is non-empty;
2. `sort_key[0] == packing_key`;
3. every `sort_key` column exists in the table schema (kills the
   provider's silent `Err(_) => break` truncation path);
4. `ts_column` appears in `sort_key` (keeps the "within a key, rows are
   time-ordered" property the read path was built around; drop this rule
   only if a table genuinely wants otherwise — not today).

Beyond `sort_key[0]` (already validated as i64 per row for
`key_range`/day-partitioning), sort columns need **no** JSON-level poison
checks: a missing value decodes to null and sorts deterministically under
the canonical options.

## Implementation steps

Ordered so the invariant is enforced *before* any code starts trusting
it: validation first (behavior-preserving — every existing config already
satisfies it), then the behavior-preserving refactor, then the actual
write-path change on top of a now-guaranteed invariant.

1. **Registration validation** — implement rules 1–4 (below) in the
   catalog insert path + bootstrap + consumer/route startup; unit tests
   for each rejection. Lands first because it's a no-op for current
   configs yet makes `sort_key[0] == packing_key` a guarantee the writer
   change in step 3 relies on (otherwise a bad `sort_key` could produce
   files `split_by_key` and the isolation predicate silently mis-slice).
2. **ukiel-core `sorting` module** (behavior-preserving refactor) — move
   `sort_batch`, add `sorting_columns` + `sorted_writer_props` with the
   canonical `SortOptions`; add `parquet` dep to core. Port compactor
   (`rewrite.rs`, `compactor.rs:216`, `deletion.rs:64`) onto it. Existing
   compactor tests keep passing; add a metadata assertion (read back
   `SortingColumn`s, including `nulls_first: true`). No ingest change yet,
   so this stays green independently.
3. **Ingest writer** (the actual fix) — `rows_to_parquet`/`encode_rows`
   take `sort_key: &[String]` alongside `packing_key`/`ts_column` (packing
   key still needed for `key_min/key_max`). `decode_rows` stops sorting
   (keeps the packing/ts i64 validation); pipeline becomes decode →
   defaults/materialized → `sort_batch(sort_key)` →
   `finish_batch(sorted_writer_props)`. One batch → one row group,
   unchanged. Safe now because step 1 guarantees packing key leads.
4. **Consumer** — pass `&hypertable.sort_key` at `consumer.rs:256`. (Route
   startup validation already added in step 1.)
5. **Tests**
   - writer: 3-column `sort_key` (`tenant_id, region, ts`) with a utf8
     middle column, unsorted input → assert physical row order AND
     read-back `SortingColumn` metadata;
   - writer: `sort_key` containing a materialized column sorts by the
     computed values;
   - writer: nullable sort column → nulls first, metadata agrees;
   - query e2e: `ORDER BY sort_key` over fresh L0 files returns correct
     order (guards the DataFusion sort-elision path end to end);
   - deletion-path: rewritten file still physically sorted (gap noted in
     the 07-06 audit).
6. **Perf check** — run the perf smoke harness (7d93601). Lexsort on
   columnar data should be ≤ the current serde_json comparator sort;
   record the numbers in the perf note.

## Compat / rollout

- Existing tables all have `sort_key == [packing_key, ts_column]`, so
  already-written L0 files satisfy the new ordering; no rewrite needed.
  (The nulls_first metadata flip only affects files with nullable sort
  columns — none exist today.)
- Changing an existing table's `sort_key` remains unsupported (would
  require full recompaction of live parts) — registration-time
  validation only; a `sort_key` migration story is out of scope here.

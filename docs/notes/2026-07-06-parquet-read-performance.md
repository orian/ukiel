# Parquet read performance: the map

Living document for the read-path optimization effort. Audit of the executed
code (2026-07-06) found the scan path functional but naive: `ParquetSource`
gets **no predicate** (the isolation filter is a `FilterExec` above the scan,
so every row of every catalog-pruned file is decoded), the scan reads the
**full physical schema** (projection applied after the filter), writers set
only ZSTD (no row-group sizing, no bloom filters, no declared sort), and
`parts.column_stats` is `None` on every part ever written.

Ordered by expected impact. Status updated as plans land.

## Tier 1 — make DataFusion do what it already can *(plan 11 — executed)*

Status: #0 (harness), #1 (predicate pushdown), #2 (projection pushdown) shipped
and measured. #3 partial: writer `sorting_columns` + declared scan ordering
shipped, but sort elimination is blocked on DataFusion 54 (see the follow-up
below). The reusable perf harness lives at `crates/ukiel-query/tests/perf_smoke.rs`.

| # | Change | Why it wins |
|---|---|---|
| 1 | Push predicates into `ParquetSource` (`with_predicate`, `pushdown_filters`, `reorder_filters`, page index on) | Row-group pruning + page pruning + late materialization. A tenant owning 1% of a packed file decodes ~1% instead of 100%. The `FilterExec` stays above as the isolation backstop — pushdown is a performance layer, never the security boundary. |
| 2 | Project at the scan (`projection ∪ {packing key} ∪ alias-referenced columns`), drop the key in the final projection | Today `SELECT payload` decodes every column. Win ∝ schema width. |
| 3 | Declare output ordering: one `FileGroup` per file + `with_output_ordering((packing_key, ts))`; write `sorting_columns` into Parquet metadata | `ORDER BY ts` stops sorting (with `key = const`, `(key, ts)` collapses to `(ts)` via equivalence); streaming operators kick in; per-file parallelism improves. |

> **Sort-elimination follow-up (plan 11, partial).** The writers now stamp
> `sorting_columns` and the provider declares the file's `(packing_key, ts)`
> output ordering over one-file-per-group, but on DataFusion 54 `ORDER BY ts`
> still plans a `SortExec`: change #1's predicate pushdown rebuilds the scan and
> clears the declared ordering, and DataFusion won't use the isolation
> predicate's `packing_key = const` guarantee to collapse `(packing_key, ts)` to
> `(ts)` after projection drops the constant column. Declaring `(ts)` alone is
> unsound (a packed file is not globally ts-sorted) and gets stripped by stats
> validation. The metadata is correct and forward-looking; eliminating the sort
> needs pushdown and declared ordering to coexist (a DataFusion-side fix or a
> version bump). Pruning (#1) is the larger win and is kept. Tracked in
> `order_by_sort_key_results_stay_correct_and_ordered`.
| 0 | Perf smoke harness first (`tests/perf_smoke.rs`, `#[ignore]`, prints medians) | Every subsequent claim gets a number. Baseline before any change. |

## Tier 2 — catalog-side indices (our unfair advantage)

The catalog is a queryable database in front of every file open — most
lakehouses don't have one. Prune before I/O exists:

4. ~~**Populate `column_stats`**~~ **(plan 12, done)**: ingest + compactor +
   deletion rewrites write per-column Int64 min/max (`ts`, packing key, any
   Int64 column) into `parts.column_stats`; `scan()` extracts time bounds from
   query predicates and `live_parts_pruned` eliminates whole parts in the
   catalog query before any I/O. Day-partition pruning was **subsumed, not
   implemented**: part-level `ts` min/max gives equal granularity (a `day`
   partition and its parts share the same time bound), so a separate
   `partition_values`-based clause would be redundant. Parts without stats
   (all pre-plan-12 files) always survive — pruning is strictly an optimization.
5. **Per-part key bitmap for packed files**: min/max ranges false-positive on
   sparse tenants (range 100–4500 "contains" keyless tenant 300). A roaring
   bitmap of distinct packing keys (~100s of bytes in `column_stats`) makes
   part pruning exact.
6. **Catalog-driven `ParquetAccessPlan`** (endgame): files are key-sorted, so
   store per-part `(key → row-group range)` boundaries at write time; the
   provider hands DataFusion exact row-group selections **without reading
   footers** — the catalog becomes a global page index.

## Tier 3 — write-side layout & encoding (rides the compactor)

7. **Row groups aligned to packing-key runs** (a row group never spans two
   keys, up to a size cap): row-group stats become perfectly selective for
   the isolation predicate.
8. **Row-group size discipline** for compaction output (~64–128k rows):
   large enough to amortize metadata, small enough to prune.
9. **Encodings**: `DELTA_BINARY_PACKED` for sorted `ts` (delta-packs
   spectacularly → fewer pages → less I/O); dictionary stays for `utf8`.
   Compression per level: L0 = LZ4 (3–5× faster decompress, short-lived),
   L1+ = ZSTD (read-many, space wins) — compaction re-encodes anyway, free.
10. **Bloom filters** on chosen high-cardinality non-sort columns
    (write-side per-column enable; DataFusion prunes with them).

## Tier 4 — metadata & cache

11. **Footer + page-index cache** (custom `ParquetFileReaderFactory`):
    every query currently re-fetches every file's footer (~2 object-store
    roundtrips per file). Files are **immutable** — the cache never
    invalidates. Biggest *latency* win for interactive queries.
12. **Write-through cache prewarm**: compactor L1 outputs are the hottest
    files in the system; push them into the NVMe cache at write time.
13. **Range-granular caching** for large dedicated files (whole-file caching
    is right for small packed files, wasteful at 1GB).

## Sequencing

All planned (roadmap rows in parentheses): Tier 1 = plan 11
(`ukiel-scan-pushdown`); Tier-2 #4 = plan 12 (`catalog-stats-pruning`);
Tier-4 #11 = plan 13 (`parquet-metadata-cache`); Tier-3 #7–10 = plan 14
(`file-layout`); Tier-4 #12–13 = plan 15 (`cache-tiering`); Tier-2 #5–6 =
plan 16 (`key-index`, execute after 11/12/14). Recommended execution order:
11 → 12 → 13 → 14 → 15 → 16, though 13/14/15 are mutually independent.
Perf smoke numbers get appended here after each plan.

## Baseline & results

This note holds the **micro tier** (`tests/perf_smoke.rs`, ~100k synthetic
rows). **Volume-scale baselines** (10–100 GB on ClickBench `hits` and Bluesky
ndjson) live in `docs/notes/2026-07-08-macro-perf.md` (plan 30's `bench`
binary).

(Recorded by plan executors; medians from `tests/perf_smoke.rs`. Methodology,
tiers beyond this micro harness, and results discipline:
`docs/superpowers/specs/2026-07-06-ukiel-performance-testing.md`.)

Dataset: `wide` (100 tenants × 1000 rows, one packed L1 file, one row group
per tenant, 8 utf8 columns); median of 10, warm, single process. The harness
(`tests/perf_smoke.rs`) is dataset-parametric — add a `Dataset` to `datasets()`
to record another fixture in its own rows.

| Scenario | Baseline | After plan 11 | After stats pruning |
|---|---|---|---|
| `wide/selective_count`: 1 tenant of 100 in packed file, `count(*)` | 16.0 ms | 13.4 ms | 13.6 ms |
| `wide/narrow_projection`: 1 of 8 columns | 13.6 ms | 8.9 ms | 8.9 ms |
| `wide/order_by_limit`: `ORDER BY ts LIMIT 100` | 16.1 ms | 11.0 ms | 12.1 ms |

Stats pruning is flat on this fixture by design: the `wide` dataset is a
single packed L1 file, and part pruning eliminates whole *files* — with one
file there is nothing to drop. The win shows when a table spans many parts in
disjoint time ranges; the `catalog_prunes_parts_by_ts_stats` query test asserts
it directly (the pruned part never appears in the physical plan). Numbers
recorded here for regression visibility only.

Plan 11 wins on the micro fixture: row-group pruning + predicate pushdown
(`selective_count`), column pruning (`narrow_projection`, the largest relative
drop), and pushdown/limit (`order_by_limit`). `order_by_limit` still plans a
`SortExec` — see the sort-elimination follow-up above; the gain here is from
pruning + projection, not sort elimination.

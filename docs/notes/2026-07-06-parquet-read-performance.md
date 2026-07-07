# Parquet read performance: the map

Living document for the read-path optimization effort. Audit of the executed
code (2026-07-06) found the scan path functional but naive: `ParquetSource`
gets **no predicate** (the isolation filter is a `FilterExec` above the scan,
so every row of every catalog-pruned file is decoded), the scan reads the
**full physical schema** (projection applied after the filter), writers set
only ZSTD (no row-group sizing, no bloom filters, no declared sort), and
`parts.column_stats` is `None` on every part ever written.

Ordered by expected impact. Status updated as plans land.

## Tier 1 — make DataFusion do what it already can *(plan 11)*

| # | Change | Why it wins |
|---|---|---|
| 1 | Push predicates into `ParquetSource` (`with_predicate`, `pushdown_filters`, `reorder_filters`, page index on) | Row-group pruning + page pruning + late materialization. A tenant owning 1% of a packed file decodes ~1% instead of 100%. The `FilterExec` stays above as the isolation backstop — pushdown is a performance layer, never the security boundary. |
| 2 | Project at the scan (`projection ∪ {packing key} ∪ alias-referenced columns`), drop the key in the final projection | Today `SELECT payload` decodes every column. Win ∝ schema width. |
| 3 | Declare output ordering: one `FileGroup` per file + `with_output_ordering((packing_key, ts))`; write `sorting_columns` into Parquet metadata | `ORDER BY ts` stops sorting (with `key = const`, `(key, ts)` collapses to `(ts)` via equivalence); streaming operators kick in; per-file parallelism improves. |
| 0 | Perf smoke harness first (`tests/perf_smoke.rs`, `#[ignore]`, prints medians) | Every subsequent claim gets a number. Baseline before any change. |

## Tier 2 — catalog-side indices (our unfair advantage)

The catalog is a queryable database in front of every file open — most
lakehouses don't have one. Prune before I/O exists:

4. **Populate `column_stats`** (field exists since plan 1, never filled): at
   minimum `ts` min/max per part, written by ingest + compactor. Extract time
   bounds from query predicates in `scan()` and prune **parts in the catalog
   query**. Same for `day` partition pruning (also currently unused).
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

(Recorded by plan executors; medians from `tests/perf_smoke.rs`. Methodology,
tiers beyond this micro harness, and results discipline:
`docs/superpowers/specs/2026-07-06-ukiel-performance-testing.md`.)

Dataset: `wide` (100 tenants × 1000 rows, one packed L1 file, one row group
per tenant, 8 utf8 columns); median of 10, warm, single process. The harness
(`tests/perf_smoke.rs`) is dataset-parametric — add a `Dataset` to `datasets()`
to record another fixture in its own rows.

| Scenario | Baseline | After plan 11 | After stats pruning |
|---|---|---|---|
| `wide/selective_count`: 1 tenant of 100 in packed file, `count(*)` | 16.0 ms | | |
| `wide/narrow_projection`: 1 of 8 columns | 13.6 ms | | |
| `wide/order_by_limit`: `ORDER BY ts LIMIT 100` | 16.1 ms | | |

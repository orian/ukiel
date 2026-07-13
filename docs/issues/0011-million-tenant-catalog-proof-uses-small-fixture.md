# 0011 — The million-tenant catalog conclusion is based on a 25k-part fixture

- **Severity:** High (evidence gap in a defining scalability claim, not a demonstrated runtime failure)
- **Status:** Open
- **Components:** `crates/ukiel-e2e` catalog benchmark, performance documentation
- **Found by:** post-implementation review of plans 40/41, 2026-07-13

## Summary

Plan 40 concludes that one PostgreSQL primary is nowhere near the catalog wall
for Ukiel's million-tenant demand envelope. The throughput measurements are real,
but the fixture does not exercise the catalog cardinality that the project calls
its million-tenant proof.

The recorded state matrix contains 25k parts, four hypertables, thirty day
partitions, and one million *packing-key values*. The seeder creates hypertables
and parts, but no `logical_tables`. The measured read operation resolves the
hypertable once before the run and then repeatedly calls `live_parts_pruned`; it
does not include namespace table listing, logical-table resolution, or session
construction.

This differs from both specifications:

- the performance-testing spec's P3 tier calls for 1M `logical_tables`,
  thousands of hypertables, and millions of parts, including session build;
- Plan 40 Task 5.1 calls for representative 10k, 1M, and 10M historical-row
  states, with key-only and key+time reads and EXPLAIN at each knee.

The local raw reports confirm that the fresh, mature, and backlogged state runs
all use the 25k-part/four-table fixture.

## Impact

The measured 20k read QPS, 8k commit/s, connection wall, and live-versus-history
comparison remain useful for the fixture that was run. What is unsupported is
the broader conclusion that the catalog has been proven at one million tenants.

Several costs can change independently of packing-key cardinality:

- `logical_tables` index depth and namespace-local listing;
- thousands of hypertable rows and schemas;
- session construction and table-provider creation;
- million- and ten-million-row `parts` index/vacuum/working-set behavior;
- cold-cache behavior when the catalog no longer fits the measured hot working
  set.

This matters because one million tenants is a defining product constraint, not a
stretch goal. A demand model with one million tenants is not a substitute for a
catalog populated with their metadata.

## Proposed resolution

Run a measurement-only Plan 40 follow-up; no product optimization should be
preselected.

1. Extend the set-based seeder with independent knobs for logical tables,
   hypertables, historical parts, live parts, and hot-hypertable live parts.
2. Seed the P3 shape: 1M `logical_tables`, thousands of hypertables, and at least
   the prescribed 10k/1M/10M parts points. Keep realistic plan-16 statistics.
3. Add a full admission workload that performs the real namespace path:
   `list_logical_tables`/`get_logical_table`, provider or session construction,
   and `live_parts_pruned`. Retain the existing parts-only workload so the
   metadata cost can be separated rather than hidden.
4. Run key-only and key+time shapes, uniform and Zipf distributions, hot and
   cold PostgreSQL-cache postures, and at least two repetitions per point.
5. Record EXPLAIN at each cardinality knee and preserve a checked-in manifest
   containing every raw-report label, configuration, SHA, and content hash.
6. Until that matrix exists, describe Plan 40 as demand modeled for one million
   tenants on a 25k-part catalog rather than as the million-tenant proof.

## Acceptance criteria

- The seed report proves exact logical-table, hypertable, live-part, and
  historical-part counts for every point.
- The 1M-logical-table and 1M/10M-part fixtures are queried through both the
  parts-only and full session/admission paths.
- Every headline has two reproducible runs and names its cache posture, returned
  rows/bytes, database resources, and driver resources.
- The capacity verdict is updated from measured cardinality, or explicitly
  narrowed if the larger matrix cannot meet its SLO.

## Notes

- Roadmap row 33 (TSBS multitenant) measures end-to-end tenant query shapes. It
  does not replace this catalog-only proof; the two tiers answer different
  questions.
- Raw reports currently live in gitignored `bench/results`. They are valuable
  locally, but a small checked-in manifest is needed for an independent reviewer
  to establish which files support the published tables.

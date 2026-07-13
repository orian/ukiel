# 0011 — The million-tenant catalog conclusion is based on a 25k-part fixture

- **Severity:** High (evidence gap in a defining scalability claim, not a demonstrated runtime failure)
- **Status:** Resolved (2026-07-13) — the P3 fixture now exists (1M `logical_tables`, 1M namespaces, 2,000 hypertables, 10k/1M/10M parts) and the whole admission path is measured through it. **Plan 40's conclusion survives**: one primary is nowhere near the catalog wall at a million tenants. What the bigger fixture *did* change is where the cost sits — see the Result section and issue 0014
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

## Result (2026-07-13)

Fixture: **1,000,000 logical tables over 1,000,000 namespaces, 2,000 hypertables**,
parts at 10k / 1M / 10M, `mature` state (10% live), real plan-16 `column_stats`.
The namespace id *is* the packing key (`slice = namespace.0`, the provider's v1
convention), so tenants and the key space are one axis. Every point ran **twice**;
the reps agree within ~2%. Reproduce with `bench/run-issue-0011.sh`; the reports
that back these tables are hashed in `bench/manifests/issue-0011.json`.

`achieved op/s` / `p99 ms` / `catalog rows per op`, 16 workers, closed loop:

| parts | parts-only (plan 40's workload) | catalog admission (uniform) | catalog admission (zipf) | full admission (+ session & plan) | cold buffers |
|---|---|---|---|---|---|
| 10k | 44,422 / 0.6 / 6 | 23,432 / 1.0 / 0 | 22,486 / 1.2 / 1 | 9,712 / 2.4 | 23,245 / 1.0 |
| 1M | 7,142 / 4.4 / 93 | 18,429 / 1.3 / 6 | 15,331 / 2.3 / 14 | 7,595 / 3.9 | 16,089 / 2.2 |
| 10M | 782 / 39.4 / **928** | 17,224 / 1.3 / 8 | 7,081 / 16.6 / **88** | 4,387 / 20.2 | 7,114 / 16.0 |

**Every point PASSES the capacity gate with zero timeouts.** Against the steady
demand of 167 read QPS, the *worst* measured configuration (full admission at 10M
parts) still clears **26×** headroom.

**A million tenants' metadata is free.** `list_logical_tables` and
`get_logical_table` are index probes on `logical_tables_namespace_id_name_key`
(EXPLAIN recorded per point) and do not move with tenant count: catalog-only
admission holds 17–23k op/s at p99 1.0–1.3 ms for the *typical* tenant at every
part count, 10k through 10M. The index depth, the namespace-local listing and the
session build — the costs the issue predicted a key-space knob could not reach —
are simply not where the money goes.

**A cold cache is a non-event.** 7,114 op/s cold vs 7,081 warm at 10M (buffer-hit
0.92 vs 0.89). The mature state is 90% tombstoned history and the partial live
index keeps the working set small, exactly as plan 40 argued.

**What *does* cost, and neither is the catalog's cardinality:**

1. **The parts fan-out.** The zipf and parts-only columns are slow purely because
   they return more rows: 88 and 928 catalog rows per op at 10M, against 8 for the
   typical tenant. Those rows are then discarded by the provider's key bitmap —
   filed as **issue 0014**, which this fixture is the first to have made visible.
2. **DataFusion planning, in the process.** Full admission is **driver-bound**: it
   burns 8.6–8.9 of 12 cores in the client while Postgres sits at 2.5–3.6, capping
   a single ukield at ~4.4–9.7k admissions/s. The catalog is not the wall for a
   *request*; the process is. That is a fleet-sizing fact, and no amount of
   Postgres scaling moves it.

Two bench bugs were found and fixed on the way, both of which would have
invalidated the result:

- The saturation classifier located Postgres by a **hard-coded container name**. In
  any checkout not named `ukiel` it found nothing, read Postgres CPU as `0.00
  cores`, and then confidently attributed *every* run to the driver. It now
  resolves the container by its compose service label. (Plan 40's own attributions
  are unaffected — it ran where the guess happened to be right.)
- The fixture gave every hypertable parts spanning the **whole** key space. With
  1M tenants over 2,000 hypertables, a table holding 500 tenants was given parts
  whose ranges covered all million, so 99.95% of tenants resolved to **zero parts**
  — the catalog would have looked extremely fast while being asked nothing. Key
  bands are now solved per hypertable from a target fan-out, the seed *measures* the
  fan-out it achieved (median tenant: 7–8 parts, matching plan 16's measured 7), and
  it **fails the run** if the median tenant resolves to nothing while there are
  enough live parts to cover the tables.

## Notes

- Roadmap row 33 (TSBS multitenant) measures end-to-end tenant query shapes. It
  does not replace this catalog-only proof; the two tiers answer different
  questions.
- Raw reports live in gitignored `bench/results`. The checked-in manifest
  (`bench/manifests/issue-0011.json`) records every report's label, content hash,
  git SHA and headline numbers, so a reviewer can establish which files support the
  published tables without the files themselves.
- An index-shape hypothesis (cost growing with a tenant's key *rank*, because
  `kmin <= k AND kmax >= k` is range containment) was raised and **measured, then
  refuted**: `parts_live_idx` serves both bounds, and the tenant matching nothing is
  5× *cheaper* than the one matching 1,078 parts. Recorded in issue 0014 so it is
  not re-raised.

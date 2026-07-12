# Decomposing the 2.1× — where the official-suite gap actually comes from

Plan 32's headline was blunt: ukiel costs ~2.1× bare DataFusion on ClickBench's
43 queries and beats it on nothing (`bench/README.md` §"OFFICIAL SUITES"). This
note decomposes that ratio into its components with controlled runs, because
"2.1× slower" is not one fact — it is at least four, with different owners and
different fixes:

| component | what it is | the control that isolates it |
|---|---|---|
| **Transport** | ukiel read MinIO over HTTP (the e2e `Stack` wires no data cache — plan 15 unexecuted); raw read local NVMe | the same suite over a **local-filesystem object store** (`UKIEL_E2E_STORE_DIR`, added for this note) |
| **File layout** | ukiel's files are int64-widened (25 cols, no narrow types), ZSTD (L1+ posture), re-sorted by `(CounterID, EventTime)`; the source files keep upstream's narrow types and encodings | **raw DataFusion over ukiel's own files** (`bench clickbench raw --dir`) — zero ukiel stack, ukiel bytes |
| **Stack** | catalog planning, provider, per-part `FileGroup`s, footer-cache factory, declared ordering | local-store ukiel run vs raw-over-ukiel-files: same bytes, same disk, only the stack differs |
| **Metadata-answerable aggregates** | q01/q07-class queries DataFusion answers from parquet metadata while ukiel scans; the catalog holds the same answer in Postgres, unused | issue 0010 (filed by plan 32); fix is a plan, not a measurement |

Method notes: same machine as the plan-32 runs (Ryzen AI 9 HX 370, 24 threads,
91 GB, NVMe), same fixture content (99,997,497 rows, 1,102 L1 parts, loaded
state), ClickBench methodology (1 cold + 3 hot, headline = hot minimum). The
recorded plan-32 MinIO and raw-source numbers are reused as the two anchors;
the two new runs sit between them. Raw-over-ukiel-files runs against the
freshly-loaded store only (post-compaction the store dir holds superseded
objects until GC — a directory listing would double-count).

## Results — loaded state (1,102 parts), sum of 43 hot minima

Four anchors, one variable changed at each step:

| run | total | step multiplier | what the step prices |
|---|---|---|---|
| raw DataFusion, source files, local NVMe | 24.7 s | — | the engine ceiling |
| raw DataFusion, **ukiel's files**, local NVMe | 27.7 s | **1.12×** | layout: int64 widening + ZSTD + re-sort + 1,102-file fan-out with no catalog |
| ukiel stack, **local store** | 38.6 s | **1.39×** | the stack: catalog planning, provider, per-part groups, footer cache |
| ukiel stack, MinIO (the plan-32 headline) | 53.4 s | **1.38×** | transport: HTTP object store with no data cache (plan 15 unexecuted) |

Reading, by share of the 28.7 s gap: **transport 52%** (14.8 s), **stack 38%**
(10.9 s), **layout 10%** (3.0 s). The plan-32 "2.16×" was half transport — an
artifact of benching the e2e stack with no local cache tier, not of ukiel's
read path. **With local bytes, ukiel is 1.56× raw** (median per-query 1.65×).

The stack's 10.9 s is not uniform — it is a few mechanisms:

- **Metadata-answerable aggregates (issue 0010)** dominate the worst ratios:
  q07 12.9× / q01 9.7× over raw-on-the-same-files. Raw answers them from
  parquet metadata; ukiel scans. The catalog holds the same answer in Postgres.
- **Filterless scans on declared-ordering scans** (q24 4.3×, q28 2.2×, q26
  2.3×): the declared `(packing_key, ts…)` output ordering forced
  order-preserving file repartitioning — see the next section. Fixed.
- **Where the product actually lives — key-filtered queries — the stack is a
  large win, not a cost**: q38–q43 run 0.22–0.34× of raw over the same files
  (3–4.5× faster). Raw pays 1,102 footer opens per query; ukiel's catalog
  prunes to a handful of parts before any I/O. This is the fan-out that the
  plan-13 cache amortizes and the catalog eliminates.
- Layout, second-order: ukiel's sorted, smaller files are *faster* than the
  source for TopK (q24: 77 ms vs 199 ms raw-on-source) and cost ~10–25% extra
  decode on full scans (widening + ZSTD, e.g. q05 428 vs 357 ms, q17 1127 vs
  991 ms). On tiny key-filtered queries the "layout" anchor is really
  catalog-less fan-out (a flat ~70 ms floor over 1,102 footers), not encoding.

Per-query table: compare `bench/results/clickbench-{raw-loaded,raw-ukiel-files,ukiel-local-loaded,ukiel-loaded}.json`
(the `clickbench compare` subcommand diffs any pair).

## Stance: no ClickBench hyperoptimization

Production is orders of magnitude bigger than this fixture and every product
query is tenant-scoped. ClickBench is a *ceiling instrument* — we take from it
only optimizations that are generic (or that fix how the benchmark deploys
ukiel, see the layout section), and we explicitly decline provider heuristics
whose only beneficiary is a whole-table, unscoped query shape that cannot
occur on the product path. Two decisions below follow from this stance.

## The declared-ordering split (packed layouts) — and the fix that shipped

The provider used to declare the file's `(packing_key, ts…)` output ordering on
every scan. Same-binary A/B on the packed local fixture (env-toggled provider,
interleaved suites, 2 runs per config, identical-config noise median 1.5% / p90
7.9%) found the declaration cuts **both ways**, on disjoint query populations:

- **Scans where the ordering survives projection, hurt badly.** When the scan
  projection keeps the packing key (the ordering's leading column), the
  declared ordering reaches file repartitioning and forces order-preserving
  mode (`FileGroupPartitioner::with_preserve_order_within_groups`,
  DataFusion 54): whole-file groups instead of balanced byte-range splits.
  EXPLAIN on packed hits_cb showed 24 groups gated on one **732 MB single
  file**. Cost: q28 (`GROUP BY CounterID`, projects the key) 2979→1940 ms
  without the declaration (**1.54×**), q27 1.20×. A WHERE clause does *not*
  prevent this — q28 has one; what clears the ordering is the projection
  dropping the key.
- **Predicated scans that project the key away, helped broadly.** Dropping
  the declaration cost 15–25% wall on a band of filtered GROUP BYs (q11–q15,
  q26, q29–q32) — with **byte-identical plans (EXPLAIN VERBOSE), identical
  scan metrics, identical total CPU**, and lower achieved concurrency (~1130%
  vs ~1450% CPU on a 24-thread box; isolated single-query runs reproduce
  0.72 s vs 0.92 s exactly, 3/3). The declaration never even survives to
  these plans, yet its presence at scan-build time still changes execution
  scheduling. The precise DataFusion 54 mechanism is not pinned; the effect
  is robust to codegen (one binary), suite order (interleaved), and neighbors
  (isolated runs).

**Shipped fix — declare iff a pushdown predicate exists** (WHERE or the
isolation slice), `provider.rs` scan. Validated packed totals (sum of 43 hot
minima, local store): declare-always **39.4 s** / never **41.4 s** /
predicate-gated **40.0 s** — the gate recovers the whole predicated-scan band
(q12 623, q15 888, q26 632 ms ≈ declare-always) and q28 stays slow (3039 ms:
it is predicated, so it declares, and its projection keeps the key). We
deliberately do **not** add a projection-based gate to also catch q28: its
query shape (whole-table, unscoped, key-projecting, filterless-or-nearly) is
benchmark-only — a product query is always key-scoped and prunes to a handful
of files where group balance is irrelevant — and a projection gate would flip
behavior for every scoped scan (they all project the key for isolation), an
unmeasured product-path change. q28's honest fix is the layout below. Every
namespaced product query is predicated by construction, so the product path
keeps pre-existing behavior exactly. Both sides pinned in
`order_by_sort_key_results_stay_correct_and_ordered`. Sort elimination never
fired on DataFusion 54 in any configuration, so nothing is given up there.

## The fixture was deployed wrong — time-led layout (`--layout ts`)

`hits_cb` packed and sorted by `CounterID` — modeling ClickBench as if it
were the multitenant product. That is the right shape for *our* workload and
the wrong one for ClickBench's: a whole-table analytics deployment wants
files in **time order**. `bench clickbench load --layout ts` loads the same
25 columns as `(packing_key = EventDate, sort_key = [EventDate, EventTime])`:
part stats prune date-range queries, the packing key is constant within a day
partition so SizeTargeted compaction ts-slices each day into balanced files
(plan-29 whale-split), and the pathological "one 732 MB file per day, key
spans every CounterID" packed shape cannot arise.

Results (local store, sum of 43 hot minima): ts-loaded 39.2 s, ts-sized-256MB
**42.4 s** — vs counter-loaded 38.6 s and raw 24.7 s. **Not a net win, and
that is the finding.** The trade is clean:

| query | raw | counter-loaded | ts-sized | what moved |
|---|---|---|---|---|
| q01 `COUNT(*)` | 1 | 43 | **5** | few files, few footers |
| q07 `MIN/MAX(EventDate)` | 1 | 69 | **18** | date stats align with files |
| q25 `ORDER BY EventTime LIMIT 10` | 65 | 83 | **42** | ts-ordered files; ukiel beats raw 0.65× |
| q24 `SELECT * ORDER BY EventTime` | 199 | 328 | **271** | same |
| q37 `CounterID=62 AND date range` | 55 | **52** | 300 | key clustering lost |
| q43 (one counter, minute buckets) | 15 | **15** | 70 | same |
| q28 `GROUP BY CounterID HAVING` | 1181 | 2169 | 2245 | decode-bound either way |

Time-led layout buys exactly the queries the catalog will answer for free
once row 35 (aggregate pushdown) lands (q01/q07), plus the ts-TopK pair —
and it destroys the point-lookup class where ukiel's packing architecture is
the whole point (q37–q43 went from beating raw to 5–13× behind it; per-column
bloom filters on `CounterID` — plan 14's opt-in attr — are the generic tool
if anyone ever deploys this shape). The scan-heavy GROUP-BY majority barely
moves: it is decode-bound (widening + ZSTD, rows 34/36 territory), not
layout-bound. Conclusion: keep the multitenant-shaped fixture as the primary
recorded configuration — it is the shape we actually sell — and keep
`--layout ts` loadable as the whole-table deployment reference. Neither
layout should be tuned further against this suite (see Stance).

## Fix list (ranked by measured impact)

1. **Deployment/bench posture: get the data local** (52% of the plan-32 gap).
   The e2e/bench `Stack` had no data-cache tier, so every read paid MinIO
   HTTP. `UKIEL_E2E_STORE_DIR` (this note) is the bench-side answer;
   **plan 15 (cache tiering — prewarm + chunked local cache) is the product
   answer** and just got re-authored against the post-29 tree. After 15, the
   hot path serves from local NVMe like the local-store runs here.
2. **Predicate-gated ordering declaration** (shipped with this note): q28-class
   filterless aggregates 1.5× on low-fan-out layouts, no product-path change.
3. **Catalog aggregate pushdown — roadmap row 35 / issue 0010** (the worst
   per-query ratios): q07 `MIN/MAX` 12.9× and q01 `COUNT(*)` 9.7× over raw on
   identical local bytes. The answer sits in `parts.column_stats`/`row_count`.
4. **Narrow column types — roadmap row 36** (structural, priced): the whole
   layout anchor is 1.12× of the suite total (~10–25% extra decode on full
   scans). Needs a type-system design sketch; dictionary/codec knobs belong in
   the same sketch. Row 34's ClickHouse substrate ladder will sharpen this
   before anyone commits to it.

What is *not* on the list: the catalog/provider stack itself. On key-filtered
product-shaped queries it already beats bare DataFusion by 3–4.5× on identical
files (q38–q43, 0.22–0.34×) — the pruning does exactly what it was built for.

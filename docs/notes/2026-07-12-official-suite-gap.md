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
   **Executed and measured (2026-07-12, same day):** the suite over MinIO with
   the plan-15 cache on tmpfs (`UKIEL_E2E_CACHE_DIR`, wired for this run) totals
   **39.5 s** — 13.9 s of the 14.8 s transport share recovered (94%), within
   2.3% of the 38.6 s local-store anchor. This item is done; the ranked list
   below it is what remains. (`bench/README.md` §"OFFICIAL SUITES — plan 15";
   label `cache-shm`.)
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

## Residual attribution — plan 37 (2026-07-13)

The 1.39× same-bytes residual (ukiel-local 38.6 s vs raw-over-ukiel's-files 27.7 s)
was attacked with three new instruments (`clickbench plan`, `clickbench analyze`,
and a plan/execute split in the suite harness), because the earlier tools could
not see the thing that mattered: **`EXPLAIN` truncates its file-group list at five
groups and never prints equivalence properties at all**, and **`EXPLAIN ANALYZE`
sums metrics across partitions**. "Textually identical plans" was never evidence.

### The hypothesis table, filled

| # | Hypothesis | Verdict | Evidence |
|---|---|---|---|
| H1 | Partition composition/skew differs invisibly | **Refuted** | At full resolution both sides produce **24 groups over the same 1,102 files, 7,575.9 MiB, max/mean = 1.00** — identical balance. Only the *byte-split boundaries* differ (catalog order vs directory order), with no balance consequence. |
| H2 | Per-query planning cost is material | **Refuted as a suite-level lever** | Planning is now recorded per query (`plan_ms`); it is a small constant, nowhere near the 10.9 s residual. Retained as an instrument because it is product-latency-relevant even when it is not suite-relevant. |
| H3 | 1,102 one-file partitions cost per-partition setup | **Refuted** | DataFusion repartitions to 24 balanced groups on *both* sides before execution. The 1,102 one-file groups never reach the executor. |
| H4 | Per-file reader-factory / schema-adaptation overhead | **Partially confirmed → see H6** | The reader factory is a clean passthrough (`get_metadata` only; the lock is never held across I/O). But the *schema* it presents is not — see H6. |
| H5 | The effect is DataFusion-internal sort-pushdown machinery | **Refuted** | `enable_sort_pushdown = false` on both sessions (same binary) changed **nothing** on either side: ukiel 1,142–1,245 ms, raw 845–872 ms, unmoved. The named call chain is live but is not this effect. |

### What it actually was: two named mechanisms

**F3 — the scan predicate was evaluated twice. (Fixed, shipped.)** The provider
hand-built every user filter into the `ParquetSource` predicate *and* reported
`Inexact` pushdown, so DataFusion pushed the same filters into the same source
again. The reader decoded every predicate column twice. The instrument made it
unmissable: **`predicate_cache_records` was exactly 2×** ukiel-vs-raw
(181,821,762 vs 90,949,793). Removing the hand-built user filters halved it to
exactly raw's number, with **byte-identical `bytes_scanned`, `pushdown_rows_pruned`
and answers** — DataFusion achieves the same pruning on its own. The isolation
predicate stays: DataFusion has no way to know about it.

Effect: 41 in-scope queries **38.5 s → 35.3 s**; residual **1.39× → 1.27×**;
**median per-query ratio 1.19× → 1.09×**. Concentrated exactly where the double
decode was (q28 −22%, q31 −27%, q32 −21%, q21 −18%). Product-suite guard
(same-binary A/B, scoped path): predicated scenarios improve, nothing regresses
beyond the noise floor.

**H6 — the provider defeats `Utf8View`. (Named, measured, filed as roadmap row
39 — NOT fixed here.)** DataFusion 54 defaults `schema_force_view_types = true`,
so the raw reference reads string columns as **`Utf8View`**. Ukiel's provider
builds its scan schema from `TableColumns::physical_schema()`, which forces plain
**`Utf8`** — `SELECT arrow_typeof("URL")` returns `Utf8` on ukiel and `Utf8View`
on raw over the *same files*. The signature is string copies instead of string
views:

| q34 (`GROUP BY "URL"`, no predicate) | ukiel | raw | Δ |
|---|---|---|---|
| `bytes_scanned` | 1,730,466,741 | 1,730,466,405 | **±0%** |
| `peak_mem_used` | 14.9 GB | 9.2 GB | **+62%** |
| `repartition_time` | 3,449 ms | 1,086 ms | **+218%** |
| `time_elapsed_processing` | 17,493 ms | 11,687 ms | +50% |
| wall | 3,864 ms | 2,533 ms | 1.53× |

This accounts for the bulk of what remains: the residual after F3 is **7.5 s over
41 queries, 91% of it in the top ten, and those ten are the string-heavy scans**
(q34/q35 `GROUP BY URL`, q22/q23/q26/q28 string predicates). It is **not** fixed
here because it is not a micro-fix: `physical_schema()` is shared with ingest,
the compactor, alias compilation (`ukiel_expr`) and materialized columns, so
switching the *scan* schema to view types is a design change with a blast radius.
Plan-37's own sizing rule says such a thing files its own row rather than growing
this one. **Roadmap row 39.**

### Where the residual stands

| | 41 in-scope queries | vs raw (same bytes) | median per-query |
|---|---|---|---|
| raw DataFusion over ukiel's files | 27.7 s | 1.00× | 1.00× |
| ukiel, local store (before plan 37) | 38.5 s | 1.39× | 1.19× |
| **ukiel, local store + F3** | **35.3 s** | **1.27×** | **1.09×** |

**Ukiel now beats raw DataFusion outright on 15 of the 41 queries.** The exit
criterion of ≤ 1.15× on the *total* is not met (1.27×), and the plan's alternative
exit applies: every remaining component is attributed to a named mechanism with a
disposition — H1/H2/H3/H5 refuted with evidence, F3 fixed, **H6 named, measured,
and filed as row 39**.

**Shipped since this note: the key index (roadmap row 16, 2026-07-13).** The
q38–q43-class advantage below now extends to the *sparse* tenant — the case
min/max pruning was blindest to. A roaring bitmap of each part's distinct
packing keys makes part pruning exact: on the 100M-row fixture the median
tenant's planned files fall 108 → 7 (−93%) and the light tenant's 82 → 8 (−90%),
and its scan-heavy scenarios drop 53–73%. Those were files whose key *range*
contained the tenant but which held none of its rows. The heavy tenant is
unchanged (it has rows nearly everywhere), which is the correct outcome.

What is *not* on the list: the catalog/provider stack itself. On key-filtered
product-shaped queries it already beats bare DataFusion by 3–4.5× on identical
files (q38–q43, 0.22–0.34×) — the pruning does exactly what it was built for.

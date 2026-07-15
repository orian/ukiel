# Ukiel's actual compacted part shape (plan 46)

**Date:** 2026-07-14 · **Plan:** 46 · **Status:** executed (geometry decision), timing exploratory

Plan 45 measured a fixture *as loaded*. This plan asks the next question: when the **real
leased/fenced compactor** turns that fixture into final parts, what shape are those parts,
and are they in the regime the issue-0014 key filter is built for? The answer decides
whether the filter is *active*, *dormant*, or *insufficient* for the files Ukiel actually
writes — and it turns out to depend, sharply and legibly, on tenant scale and placement.

Every number here is from a **synthetic, profile-derived** fixture. Nothing in it is a
production measurement. The pipeline is the plan-46 tools: generate → stage L0 →
compaction-input → real compactor → scan. Row censuses and order-independent row
fingerprints match end to end at every arm (no row was lost, duplicated, or changed by
compaction), and every compacted scoped query equalled raw DataFusion.

## What was run, and what was reduced

| tier | tenants | rows | placements measured | note |
|---|---:|---:|---|---|
| smoke | 500 | 40k | packed | first-light, in the task-4 test |
| baseline | 10,000 | 3M | packed, 64 MiB, 256 MiB, separated | geometry + one admission A/B |
| shape | 142,066 | 6M | packed (separated attempted) | the absolute-cardinality decision |

**The row counts are reduced from the plan's 30M/100M, deliberately and disclosed.** The
decision this plan owns — *how many distinct keys does a compacted part hold, and does it
cross the filter's tiers* — is driven by the **membership graph and the tenants active per
UTC day**, not by row count. A part's key cardinality is the number of distinct tenants
whose rows landed in it, which is set by the graph; more rows per tenant make each part
bigger in bytes but not in *keys*. So a reduced-row run answers the cardinality question
faithfully. It does **not** exercise size-targeting's byte cuts (see Q1), which is the one
place the reduction bites, and that is called out where it matters rather than hidden.

Geometry is deterministic in the seed — same profile, seed, and flush size produce a
byte-identical staged artifact and therefore an identical compacted shape — so the two
interleaved repetitions the plan asks for are, for geometry, definitionally identical. The
repetition requirement is about *timing* noise, and the timings here are labelled
exploratory.

## The seven questions

### Q1 — distinct keys per final part, by placement

Baseline (10,000 tenants), per final part:

| placement | final parts | dk p50 | dk p90 | dk max | key band | filter |
|---|---:|---:|---:|---:|---|---|
| packed | 14 | 5,783 | 6,048 | 6,459 | `1281..8000` | all 14, 2048-byte tier |
| 64 MiB | 14 | 5,783 | 6,048 | 6,459 | `1281..8000` | identical to packed |
| 256 MiB | 14 | 5,783 | 6,048 | 6,459 | `1281..8000` | identical to packed |
| separated | 74,864 | 1 | 1 | 1 | `dedicated` | none (min == max is exact) |

**Packed makes one part per UTC-day partition** — 14 days, 14 parts — each holding the
~6,000 tenants active that day. **Separated makes one part per key** — 74,864 dedicated,
single-key files, each exact under range pruning with no filter needed. These are the two
controls, and both behave exactly as designed: separated's `min == max` needs no bitmap or
Bloom, and the experiment would be *wrong* if it did.

**The size-targeted arms are identical to packed, and that is the reduction showing.** At
3M rows a day-partition compacts to ~10 MB, far below the 64/256 MiB targets, so the size
cut never triggers and the merge output for a day is one file regardless. To exercise
size-targeting's cuts a day-part must exceed the target — ~1.4M rows/day for 64 MiB, ~5.5M
for 256 MiB — i.e. the full 20–80M-row baseline. **What size-targeting *would* do is not in
doubt from the design** (it cuts merge outputs at key boundaries into ~N-byte files, so
heavy keys separate and light keys pack), but this run did not measure its output shape,
and no conclusion about it is drawn from these numbers.

### Q1 at production cardinality — the shape tier

Shape (142,066 tenants — the profile's own active-tenant estimate), packed:

| placement | final parts | dk p50 | dk p90 | dk max | key band | filter |
|---|---:|---:|---:|---:|---|---|
| packed | 14 | 80,767 | 85,285 | 90,703 | `>8000` | **none** — all 14 `key_filter` NULL |

**This is the decision.** At production tenant cardinality, a packed day-part holds ~80,000
distinct keys — an order of magnitude past `MAX_KEYS_WORTH_FILTERING` (8,000). The filter is
**correctly not stored**: at that density every tier is 100% saturated, so a 2 KB blob per
part would keep every part it was asked about and prune nothing. This is exactly the regime
`ukiel_core::keyfilter`'s own comment predicted from the source's 49,300-keys/part median —
now confirmed on Ukiel's *own* compacted output, not just the source's ClickHouse parts.

The progression across scale is the whole story: **500 tenants → ~285 keys/part (filtered,
comfortably); 10,000 → ~6,000 (filtered, but past the 1% design point); 142,000 → ~80,000
(no filter).** The packed compacted part crosses every filter tier as the tenant universe
grows, and at production scale it is off the top.

### Q2 — filter-tier coverage and dedicated fraction

- **Packed, baseline:** 0% dedicated, 100% filtered, every filter in the largest (2048-byte)
  tier. Filter bytes were 14,350 of a 278 KB parts table (5%) and 14 KB of a 98 KB live index
  (15%) — cheap.
- **Packed, shape:** 0% dedicated, 0% filtered (all NULL), because every part is `>8000`.
  The filter costs *nothing* here — it is simply not written.
- **Separated, baseline:** 100% dedicated, 0% filtered — and correctly so.

### Q3 — range vs filter vs exact, and Q4 — admission

Baseline packed, closed-loop admission A/B (16 workers, unvacuumed), across 19 tenants:

| path | ops/s | p50 | p99 | mean parts | mean tuple bytes | provider residue |
|---|---:|---:|---:|---:|---:|---:|
| range-only (pre-0014) | 12,283 | 1.06 ms | 1.85 ms | 14.0 | 153,238 | 6.4 |
| filtered (real) | 14,515 | 0.94 ms | 1.75 ms | 9.1 | 100,824 | 1.5 |

The filtered path ships **1.5× fewer tuple bytes** and ~18% more throughput. But note the
**filtered residue of 1.5**: at ~6,000 keys the 2048-byte filter is well past its 1,280-key
1%-false-positive design point, so it keeps ~9 parts where ~7 hold the tenant — it is
*active but degraded*. Its false-positive rate at 6,000 keys is high by construction, which
is exactly why the design stores nothing past 8,000: a filter that keeps everything is not
worth its bytes.

Crucially, the *magnitude* of the win is modest here (1.5×, ~2,000 ops/s) because **packed
day-merges produce few parts with low range over-fetch**. With only 14 parts, each spanning
most of the key universe, a tenant is a range candidate in ~14 and an exact member of ~9 —
an over-fetch near 1.5×, not the 2,144× the filter was built to kill. The catastrophic
fan-out issue 0014 targets comes from *many sparse parts*, which packed day-compaction does
not create.

### Q5 — unvacuumed vs vacuumed

Not measured in this run: the vacuumed phase requires an operator `VACUUM (ANALYZE) parts`,
which the benchmark deliberately never issues, and this execution ran only the unvacuumed
phase. The tooling for both phases exists (`admission --phase`), the orchestration script
pauses for the operator VACUUM, and this remains an open, cheap follow-up. No conclusion
about visibility-map/autovacuum posture is drawn here.

### Q6 — compacted query equivalence

**Yes.** Every compacted scoped Ukiel query equalled raw DataFusion — 48 query/tenant pairs
at baseline packed, 0 failures, after compaction had changed every source path and grouping.
Scoped Ukiel beat raw on the sparse tenants (down to 0.19×) and paid a small overhead on the
dense ones, exactly as in plan 45.

### Q7 — which regime is the normal case for the docs

**For packed placement at production cardinality, the filter is normally dormant.** A
packed day-part holds tens of thousands of keys, is correctly stored with no filter, and —
because it is one of only a handful of dense parts — has small range over-fetch anyway, so
the filter is neither present nor needed. The filter *earns its keep* on **sparse, modest-key
parts**, which are produced by **separated** (every part exact, no filter needed at all) and
by **size-targeted** placement (heavy keys cut to dedicated files, light keys packed into
filterable sets) — and, transiently, on L0 and low-level runs before a partition finalizes.

## Decision (do not optimize)

Applying the plan's decision table to the measured arms:

- **Packed, production cardinality** → *"most parts are dense/dedicated or range fan-out is
  already small → filter is normally dormant but cheap → keep the conservative design; do not
  enlarge it without a failing SLO."* The filter is dormant here and costs nothing (not
  stored). Its cost where it *is* stored (baseline, 5% of the parts table) is small.
- **Separated** → the exact-range control behaved exactly: `min == max`, no filter, 100%
  dedicated. The experiment is wired correctly.
- **No correctness failure** anywhere: no row lost or duplicated (fingerprints matched), no
  exact member filtered (zero false negatives across every arm), every compacted query
  agreed. The provider's exact bitmap backstop remains in place and its residue is reported.

**No product change is warranted, and none is made.** The three-tier, 8,000-key-ceiling
design is confirmed appropriate: it is active and cheap where parts are modest, and it
correctly stores nothing where they are dense — which at production cardinality under packed
placement is the common case, and is *also* the case where range over-fetch is already small.

Two things are explicitly **left open**, as measurement gaps rather than defects, each cheap
to close later:

1. **Size-targeted output shape at a byte-triggering row count.** This run's reduced rows
   never hit the size cut, so the placement most likely to *keep the filter active at scale*
   (heavy keys dedicated, light keys packed into ≤8,000-key sets) was not observed. A focused
   arm at ~20M+ rows would settle it. This is the one arm worth running before any statement
   that the filter is dormant *in general* rather than *for packed placement*.
2. **The unvacuumed/vacuumed delta**, per Q5.

Neither is a residual issue-0014 regime (no arm showed harmful fan-out on parts too dense to
filter), so no new issue is filed. If the size-targeted arm at full row count were to show
filterable-but-numerous parts with high over-fetch, *that* would be the case to file — but
it has not been observed.

## Reproducing

```bash
prod-synth      generate --tier baseline --profile docs/prod-info --output <src> --no-gate
prod-synth-l0   stage --manifest <src>/manifest.json --output <l0> --flush-rows 100000
cp bench/config/prod-synth-compactor.toml.example arm.toml   # point at a disposable stack
bench/prod-synth-part-shape.sh --l0-manifest <l0>/l0-manifest.json \
  --placement packed --label baseline-packed --config arm.toml --out <out> --ephemeral
```

Raw reports for the arms above are in `bench/results/prod-synth-part-shape/`.

## What this may not be used to claim

- Nothing about production performance or timing — synthetic fixture, reduced rows, timings
  exploratory.
- No catalog-capacity claim — 14–75k parts is a geometry measurement; plan 40 owns capacity.
- No compaction-ladder conclusion drawn from ClickHouse levels — the source's merge levels
  are provenance and were never mapped onto Ukiel's ladder; every generated part entered at
  level 0 and climbed the real ladder.
- No statement about size-targeted placement's output shape — it was not exercised at a
  byte-triggering row count.

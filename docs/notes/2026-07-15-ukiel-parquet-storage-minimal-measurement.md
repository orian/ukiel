# Minimal Parquet Storage Measurement — result (Plan 48)

**Status (2026-07-15): executed.** The 30M-row minimal screen ran end-to-end over an
immutable snapshot of **converged, packed, L1+ ZSTD Ukiel parts** — the real product bytes,
not the L0/LZ4 the earlier smoke used. The corrected run-order tooling (Plan 48 Tasks 1/2/5)
drove it: the recorded seeded schedule was the executed schedule, with the product control
measured independently at the start and end of each of two repetitions.

Raw evidence (planned/complete run sets, all 26 bench reports, per-column censuses, analysis
JSON/Markdown, receipt, and a SHA-256 inventory) is under
`bench/results/parquet-lab/plan48/`. Parquet variants and datasets are not committed.

## Headline

**Exactly one directional candidate: `compression-zstd-6`** — it compresses the 30M control
**~12% smaller with no read-latency penalty** (both repetitions slightly *faster*, well
inside the noise band). Everything else is dominated, workload-neutral, or unchanged. In
particular, **the two "already-proven lossless" narrow-type projections do not help on ZSTD'd
event data** — a genuine negative result that directly informs roadmap row 36.

## The control

| property | value |
|---|---|
| source | converged, packed, L1+ Ukiel parts frozen with `from-ukiel` |
| files / rows | 14 / 30,000,000 |
| codec | ZSTD (level 1 — the product L1+ policy) |
| compressed bytes | 1,288,132,743 (≈1.29 GB) |
| warm suite median | 292.4 ms |
| noise bands | size 5.0% (decision threshold), time 6.9% (`3·MAD/median`) |
| control drift | rep0 0.21%, rep1 1.55% — both inside noise, no machine drift |

Two repetitions, seeded interleaved order (seed 47), one cold + five warm per query, `--mode
local` (real filesystem). No repetition classified `unstable`.

## All eleven arms

| axis | variant | size Δ% | time Δ% (rep0 / rep1) | classification |
|---|---|---|---|---|
| compression | **zstd-6** | **−12.08** | −1.10 / −2.51 | **pareto-candidate (balanced)** |
| compression | zstd-1 | −0.04 | ≈−2.2 | no-demonstrated-change (this *is* the control codec) |
| compression | lz4-raw | +71.19 | −4.86 | dominated (much bigger for a small decode win) |
| encoding | strings-no-dict | +14.56 | +55.38 | dominated (dictionaries strongly help, size and time) |
| encoding | ts-plain | +1.67 | +1.34 | no-demonstrated-change |
| geometry | rowgroup-32k | +4.32 | +0.99 | no-demonstrated-change |
| geometry | rowgroup-512k | +0.15 | +1.93 | no-demonstrated-change |
| geometry | page-64k | +1.42 | +1.33 / −3.42 | no-demonstrated-change |
| types | team-int32 | +0.74 | −0.88 | no-demonstrated-change |
| types | ts-timestamp | +6.53 | +2.34 | dominated (Arrow `Timestamp` is bigger) |
| bloom | distinct-id-01 | +0.74 | +1.59 / −1.59 | no-demonstrated-change |

Every arm passed the eligibility gates before timing: logical fingerprint preserved, schema
and file membership unchanged, exact query/probe answers equal to the control, and census
confirming the requested property was actually written (LZ4_RAW codec; `team_id` physical
`INT32`; a Bloom filter present on `distinct_id`; the dictionary page absent on the promoted
URL column). **Caveat on the ZSTD level:** Parquet footers record the *codec* (ZSTD) but not
the *level*, so census cannot read "level 6" back; the −12% byte reduction versus the ZSTD-1
control (1.13 GB vs 1.29 GB) is the evidence the stronger level took effect, and the variant
manifest records the requested `zstd(6)`.

## What this refutes and confirms (directional, prod-synth only)

- **Narrow types do not pay on ZSTD'd event data.** `team_id Int64→Int32` moved size by
  +0.74% (i.e. nothing) and `timestamp→Timestamp(ms)` made it 6.5% *bigger*. ZSTD already
  compresses the int64 columns effectively, so narrowing the physical type — proven lossless
  and answer-preserving — buys no bytes here. This is the key input for **roadmap row 36**:
  the narrow-type ceiling on this workload is ≈0, not the guessed 10–25%.
- **Dictionaries and the timestamp delta earn their place.** Disabling string dictionaries
  cost +15% bytes and +55% time (dominated); plain timestamps were neutral-to-slightly-worse.
- **Row-group/page geometry, and a `distinct_id` Bloom, show no material effect** at this
  selectivity mix — the catalog + native page statistics already prune what these would.
- **Stronger whole-file ZSTD is the one lever worth confirming**: ~12% smaller at no read
  cost, consistent across both repetitions.

## Task 6 — earned attribution checks: none earned

- **Page-index A/B:** gated on `page-64k` moving a *selective* query outside noise. It did
  not (no-demonstrated-change; selective probes within noise). `not_run_gate_not_met`.
- **Bloom A/B:** gated on `distinct-id-01` moving `eq_distinct_id` outside noise. It did not
  (~5.5 ms, within noise). `not_run_gate_not_met`.
- **Custom skip index:** no equality/prefix probe remained materially expensive after the
  native result (`eq_distinct_id` at 0.1% selectivity is ~5 ms), so **no custom skip-index
  experiment is earned** — a complete result, not an omission.

## The single next earned action

One candidate survived: **`compression-zstd-6`, best-balanced.** Per Plan 48 Task 7 Step 4,
the next (and only) earned confirmation is:

1. Confirm `zstd-6` against the **64 MiB size-targeted control** *only if* that placement
   produces materially different part geometry than packed (Plan 46 measured the trigger);
   otherwise the packed result stands.
2. Confirm the survivor on the **10M ClickBench** slice to see whether the ~12% ZSTD-6 size
   win is event-specific or general.
3. A combined candidate + leave-one-out is unnecessary here — there is only one axis to keep.
4. Wire real MinIO/S3 + returned-byte/provenance accounting **only** before making any
   remote-I/O claim; this screen is `--mode local` and quotes no object-store metrics.

No object-store, cross-workload, writer-default, or production claim is made from this
screen. Plan 47 is **not** marked complete: a `zstd-6` production default requires the
ClickBench confirmation and a focused evidence-backed issue naming the codec change, its
write-amplification cost, and a rollback gate.

## Reproduction

```bash
# Stack: docker compose up -d postgres minio ; disposable db `plan48`.
prod-synth generate --tier baseline --profile docs/prod-info --output b/src --seed 0
prod-synth-l0 stage --manifest b/src/manifest.json --output b/l0
ukiel-prod-load compaction-input --l0-manifest b/l0/l0-manifest.json --label b30m-packed \
  --config config.toml --receipt b/receipt.json --placement packed --ephemeral
ukield --config config.toml &                       # compactor
ukiel-prod-bench wait-compacted --receipt b/receipt.json --config config.toml --timeout-secs 3600
# (place l0-manifest.json + source manifest.json next to the receipt for from-ukiel)
parquet-lab-snapshot from-ukiel --receipt b/receipt.json --config config.toml --output b/SNAP
parquet-lab-bench compile-suite --manifest b/SNAP/manifest.json --kind prod-synth \
  --queries bench/queries/prod-synth/queries.sql \
  --probes bench/config/parquet-lab/probes/prod-synth.toml --output b/suite.json
python3 bench/parquet-lab-run-set.py plan --specs-from bench/config/parquet-lab/minimal-screen.txt \
  --backend local --repetitions 2 --seed 47 --out b/planned.json
for rep in 0 1; do
  PARQUET_LAB_BIN=target/release bench/parquet-lab.sh --snapshot b/SNAP --suite b/suite.json \
    --run-set b/planned.json --rep $rep --out b/rep$rep --mode local --cold 1 --warm 5
done
python3 bench/parquet-lab-run-set.py close --run-set b/planned.json --reports b/rep0 b/rep1 --out b/complete.json
python3 bench/parquet-lab-analyze.py --run-set b/complete.json --reports b/rep0 b/rep1 \
  --json-out b/analysis.json --md-out b/analysis.md
```

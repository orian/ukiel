# Minimal Parquet Storage Measurement — note (Plan 48)

**Status (2026-07-15):** The measurement *tooling* is corrected, hardened, and validated
end-to-end; the **30M-row live screen itself is the operator-run compute step** and has not
been executed here. This note records what the corrected pipeline now guarantees, the
exact ready-to-run commands, and the single next action — it does not present a storage
result, because per Plan 48 no result may be concluded from anything smaller than the 30M
converged-L1+ baseline.

## What was fixed and validated (Tasks 1, 2, 5)

The audit that motivated this plan found that **the recorded schedule was not the executed
schedule**: `parquet-lab.sh` ignored the run set, ran specs in lexical order, and wrote a
single `product-control.json`, so the closer bound *both* scheduled control brackets to one
file — the start/end control was a single measurement wearing two hats. That is now fixed:

- **Task 1** — `parquet-lab.sh --run-set PLANNED.json --rep N` executes the *exact* recorded
  schedule for the repetition, in the seeded interleaved order, writing each measurement to
  its scheduled `report_id` (`order-000-product-control.json`, `order-003-page-64k.json`, …).
  The product control is measured **independently at the start and end** of each repetition
  (two distinct reports). The closer resolves each entry's exact recorded path, verifies its
  digest, requires the two control brackets to be distinct reports over the same snapshot,
  requires the same variant label to resolve to the same variant digest across repetitions,
  and validates backend/reader-config/run-order. Safe publication (fail-fast, atomic rename,
  marker-guarded `--replace`, symlink/traversal refusal) is preserved. A fake-tool
  execution-order test asserts the planned order runs (not lexical) with two distinct control
  reports; the analyzer now consumes both control measurements and reports start-to-end
  drift per repetition. **Validated end-to-end on the real prod-synth smoke snapshot**
  (plan → run rep0 & rep1 → close → analyze), confirming seed-shuffled order, two distinct
  control brackets, and per-rep drift.
- **Task 2** — the 11-variant screen is registered as an ordered *list*
  (`bench/config/parquet-lab/minimal-screen.txt`) consumed by `--specs-from`, never a copied
  spec directory. The list's SHA-256 and every referenced spec digest are recorded; missing,
  duplicate, absolute, traversing, non-TOML, or outside-`bench/config/parquet-lab` paths and
  duplicate labels are refused; a golden test pins the list to exactly the 11 registered
  specs.
- **Task 5** — the analyzer requires **repetition agreement** (opposing per-repetition signs
  beyond noise → `unstable`, never a candidate), keeps **per-query** median/MAD and result
  rows (so a suite-total win that is actually workload-specific is visible), classifies only
  size (exact compressed bytes, 5% threshold) and local reads (the registered `3·MAD/median`
  band), and emits immutable JSON + Markdown with formulas, exclusions, raw identities,
  seed/order, and control drift. Golden tests cover opposing repetitions, excessive control
  drift, one dominating query, answer rejection, missing census, tampered/incomplete run
  sets, and a stable Pareto candidate.

## The 30M live screen (Tasks 3, 4, 6) — the operator step

Generation is cheap (1M rows in ~1.5 s → 30M in ~45 s), but the screen is a multi-hour
compute job: staging ~1.3 GB as L0, loading it into a disposable Ukiel stack, compacting
**packed placement to convergence** with the real ladder (product L1+ ZSTD bytes — *not* the
L0/LZ4 bytes the historical smoke used, which made every ZSTD rewrite a false win), freezing
one immutable `from-ukiel` snapshot, then 11 rewrites + census + **24 timed benchmark runs**
(2 controls + 11 variants, twice) over 1.3 GB, each query one cold + five warm.

Ready-to-run, with the corrected tooling:

```bash
# Task 3: freeze the single converged-L1+ product control (packed placement).
prod-synth generate --tier baseline --profile docs/prod-info --output SRC --seed 0
prod-synth-l0 stage --manifest SRC/manifest.json --output L0
# ... compact packed to convergence via bench/prod-synth-part-shape.sh (ephemeral stack),
# ... then snapshot the converged catalog:
parquet-lab-snapshot from-ukiel --receipt PLAN46_RECEIPT.json --config UKIEL_CONFIG.toml --output SNAP
parquet-lab-snapshot verify --manifest SNAP/manifest.json
parquet-census  --manifest SNAP/manifest.json --report runs/product-control-census.json
parquet-lab-bench compile-suite --manifest SNAP/manifest.json \
  --queries bench/queries/prod-synth/queries.sql \
  --probes  bench/config/parquet-lab/probes/prod-synth.toml --output runs/suite.json

# Task 4: plan both repetitions, then run the exact registered schedule in release mode.
cargo build --release -p parquet-census -p parquet-rewrite -p parquet-lab-bench
python3 bench/parquet-lab-run-set.py plan \
  --specs-from bench/config/parquet-lab/minimal-screen.txt \
  --backend local --repetitions 2 --seed 47 --out runs/plan48-planned.json
for rep in 0 1; do
  PARQUET_LAB_BIN=target/release bench/parquet-lab.sh --snapshot SNAP --suite runs/suite.json \
    --run-set runs/plan48-planned.json --rep $rep --out runs/rep$rep --mode local --cold 1 --warm 5
done
python3 bench/parquet-lab-run-set.py close --run-set runs/plan48-planned.json \
  --reports runs/rep0 runs/rep1 --out runs/plan48-complete.json

# Task 5/analysis:
python3 bench/parquet-lab-analyze.py --run-set runs/plan48-complete.json \
  --reports runs/rep0 runs/rep1 --json-out runs/plan48-analysis.json --md-out runs/plan48-analysis.md
```

Task 6's page-index / Bloom reader A/Bs and any sidecar remain **gated** on a variant moving
a named query outside noise; each unearned A/B is recorded `not_run_gate_not_met`, and "no
custom skip-index experiment earned" is a valid complete result.

## Result placeholders (fill from the operator run)

| axis | variants | size Δ% (rep0/rep1) | local time Δ% (rep0/rep1) | classification |
|---|---|---|---|---|
| geometry | rowgroup-32k, rowgroup-512k, page-64k | _pending_ | _pending_ | _pending_ |
| encoding | strings-no-dict, ts-plain | _pending_ | _pending_ | _pending_ |
| compression | lz4-raw, zstd-1, zstd-6 | _pending_ | _pending_ | _pending_ |
| types | team-int32, ts-timestamp | _pending_ | _pending_ | _pending_ |
| bloom | distinct-id-01 | _pending_ | _pending_ | _pending_ |

## The single next earned action

Run the 30M screen above once against a disposable stack, close and analyze it, then follow
Plan 48 Task 7 Step 4: if no candidate survives repetition agreement, keep the product policy
and stop Plan 47 storage work; if a candidate survives, confirm only it against the 64 MiB
control (if that placement produced materially different geometry) and then 10M ClickBench,
build one combined + leave-one-out arm only after both confirmations, and wire real
MinIO/S3 + returned-byte/provenance accounting only before any remote-I/O claim. No
object-store, cross-workload, writer-default, or production claim may be made from this
screen, and Plan 47 is **not** marked complete.

# Ukiel Plan 48: Minimal Parquet Storage Measurement

> **For agentic workers:** Execute tasks in order. Keep this file's checkboxes
> truthful, run each focused gate before its commit, and stop on any correctness
> or binding failure. This plan intentionally produces a directional local
> storage/read result; it does not satisfy Plan 47's remote object-store or full
> cross-workload publication gate.

**Status:** Ready to execute.

**Goal:** Find the small set of Parquet choices worth confirming by running one
correct 30M-row prod-synth screen over actual converged Ukiel L1+ bytes. Measure
the product control plus 11 bracketing variants, twice in genuinely interleaved
order, with exact answer/fingerprint checks and local-file read timings. Name at
most three candidates and defer every intermediate or cross-workload experiment
until a measured result earns it.

## Why this plan exists

Plan 47 built the laboratory and most of its remediation. Running its entire
matrix now would still spend time on redundant intermediate points and would
mix two different goals:

1. finding whether any storage axis has a material effect; and
2. producing a publishable, remote-object-store, cross-workload recommendation.

We need the first answer before paying for the second. The 30M prod-synth
baseline is only about 1.3 GB, but it is the smallest registered tier that
preserves the production-derived tenant activity and part-membership geometry.
The earlier 8k L0 smoke is unsuitable: its LZ4 control made every ZSTD rewrite
look like an artificial storage win.

The minimal screen therefore keeps the things that make a result believable —
actual L1+ product bytes, exact equivalence, two repetitions, five warm samples,
bracketing values, immutable evidence, and a recorded noise band — while
removing intermediate variants and unearned confirmations.

## Result scope

This plan may conclude:

- a variant changes compressed bytes on prod-synth;
- a variant changes local-file DataFusion read latency for a named query class;
- a variant is dominated, a directional candidate, workload-specific, or shows
  no demonstrated change; and
- a native feature deserves one focused reader A/B or a residual predicate
  deserves a later sidecar confirmation.

This plan may **not** conclude:

- remote S3/MinIO request or returned-byte savings;
- a production default codec, encoding, type, page, Bloom, or sidecar;
- a write-path win unless separately measured writer evidence exists;
- that a prod-synth result generalizes to ClickBench; or
- that Plan 47 Task 8 is complete.

The current benchmark `object-store` mode wraps a local filesystem and the CLI
still refuses `--store-receipt`. Returned-byte accounting, I/O phase
classification, additive repetition I/O, and the publishable provenance gate
also remain open. This plan uses `--mode local` and does not quote object-store
metrics.

## Global constraints

- No production crate, schema, compactor policy, catalog migration, or public
  configuration change.
- Reuse one immutable snapshot and compiled suite for every arm.
- Original compacted bytes are the product control; never replace them with a
  rewrite using supposedly equivalent settings.
- Every variant must preserve the logical fingerprint, schema, file membership,
  and every query answer before its timing is eligible.
- Two repetitions are mandatory. Within each repetition variants follow a
  seeded order and the control is independently measured at both start and end.
- Every query gets one fresh-session cold run followed by five warm runs.
- Results inside `max(5%, 3 × MAD(control suite totals) / median)` are “no
  demonstrated change.” Both repetitions must move in the same direction before
  an arm can become a candidate.
- Raw reports are immutable and digest-bound. Derived Markdown is regenerated
  from them.
- Missing values are `null`, not zero. No inferred remote bytes or invented host
  metadata.

## Exact minimal matrix

The product control supplies current 128k row groups, current page target,
timestamp delta encoding, current L1+ ZSTD policy, wide physical types, and no
experimental Bloom.

| axis | registered variants | why these are sufficient for screening |
|---|---|---|
| geometry | `pages/rowgroup-32k.toml`, `pages/rowgroup-512k.toml`, `pages/page-64k.toml` | brackets current 128k row groups and tests one materially smaller page target |
| encoding | `encodings/strings-no-dict.toml`, `encodings/ts-plain.toml` | tests the two likely policy questions; current timestamp delta is already represented by the product bytes |
| compression | `compression/lz4-raw.toml`, `compression/zstd-1.toml`, `compression/zstd-6.toml` | brackets decode-fast and stronger-compression behavior around the current ZSTD policy |
| physical types | `types/team-int32.toml`, `types/ts-timestamp.toml` | measures the two already-proven lossless projections |
| native Bloom | `blooms/distinct-id-01.toml` | tests one strong equality-filter point; FPP 0.05 is an intermediate follow-up only if 0.01 helps |

Exactly 11 rewrites plus the original product control enter the screen. Do not
add `page-256k`, `stats-chunk`, `ts-delta`, `snappy`, `zstd-3`, or
`distinct-id-05` before analyzing it. Those are interpolation/attribution points,
not discovery points.

The compiled workload contains the six prod-synth query classes and all seven
registered probes from `bench/config/parquet-lab/probes/prod-synth.toml`:
non-packing-key equality, high-selectivity `distinct_id` equality, timestamp
range, URL prefix, URL null, narrow projection, and wide projection.

## Task 1: Make the recorded schedule the executed schedule

The current run-set planner records a shuffled sequence with product control at
both ends, but `parquet-lab.sh` ignores it, sorts specs lexically, and writes only
one `product-control.json`. The closer then binds both scheduled control entries
to that same file. Unit tests do not currently exercise this mismatch. No timing
run is valid until this task closes it.

**Files:**

- Modify: `bench/parquet-lab-run-set.py`
- Modify: `bench/parquet-lab.sh`
- Modify: `bench/tests/parquet-lab.sh`
- Modify: `bench/tests/test_parquet_lab_analyze.py`
- Modify: `bench/README.md`

**Interface:**

```text
bench/parquet-lab-run-set.py plan \
  --block bench/config/parquet-lab/pages --backend local \
  --repetitions 2 --seed 47 --out planned.json

bench/parquet-lab.sh \
  --snapshot SNAP --suite SUITE --run-set planned.json --rep 0 \
  --out runs/rep0 --mode local --cold 1 --warm 5
```

- [x] **Step 1: Write a failing execution-order test.** Fake tools append every
  invocation to a log. Assert that the exact planned sequence is executed, not
  lexical order, with separately named start/end control reports and the
  scheduled `run_order` in every report.
- [x] **Step 2: Make report identities collision-free.** Store reports by
  scheduled entry, for example `bench/order-000-product-control.json` and
  `bench/order-012-product-control.json`. The closer resolves the exact path
  recorded in each schedule entry; it never derives a non-unique path from the
  label alone.
- [x] **Step 3: Validate execution against the plan.** Refuse a missing, extra,
  duplicated, reordered, wrong-repetition, wrong-reader-config, or wrong-backend
  report. Verify suite/control/variant digests and require start/end controls to
  be distinct reports over the same snapshot. The same variant label in both
  repetitions must resolve to the same deterministic variant digest.
- [x] **Step 4: Preserve safe publication.** Child failure, interruption, or
  schedule mismatch leaves the run failed and unpublished. Existing traversal,
  symlink, marker, and atomic-rename tests remain green.
- [x] **Step 5: Correct the analyzer fixture.** It must consume both real control
  measurements rather than loading the same report twice. Report start-to-end
  control drift per repetition.
- [x] **Step 6: Verify and commit.**

```bash
bash -n bench/parquet-lab.sh
bash bench/tests/parquet-lab.sh
python3 -m unittest bench/tests/test_parquet_lab_analyze.py
git diff --check
git commit -m "bench: execute registered parquet run order"
```

## Task 2: Register the reduced matrix without copying specs

**Files:**

- Create: `bench/config/parquet-lab/minimal-screen.txt`
- Modify: `bench/parquet-lab-run-set.py`
- Modify: `bench/tests/parquet-lab.sh`
- Modify: `bench/README.md`

`minimal-screen.txt` is an ordered, comment-capable list of the 11 existing TOML
paths above. It is not another writer-spec directory and must not duplicate TOML
contents. The planned seeded order records the SHA-256 of the list plus every
referenced spec digest.

- [x] **Step 1: Write list validation tests.** Refuse missing, duplicate,
  absolute, traversing, non-TOML, outside-`bench/config/parquet-lab` paths and a
  spec whose embedded label is duplicated.
- [x] **Step 2: Implement `--specs-from`.** Preserve the explicit list identity,
  then seed-shuffle its entries independently for each repetition. Reusing the
  seed produces a byte-identical schedule.
- [x] **Step 3: Pin the intended set.** A golden test asserts the list contains
  exactly the 11 registered paths in this plan. Changing the screen is a plan
  change, not a runtime convenience.
- [x] **Step 4: Verify and commit.**

```bash
bash bench/tests/parquet-lab.sh
python3 bench/parquet-lab-run-set.py plan \
  --specs-from bench/config/parquet-lab/minimal-screen.txt \
  --backend local --repetitions 2 --seed 47 --out /tmp/plan48-runset.json
git diff --check
git commit -m "bench: register minimal parquet storage screen"
```

## Task 3: Prepare and freeze the single product control

**Inputs:**

- Plan 45's verified 30M prod-synth manifest;
- one staged L0 artifact digest reused from Plan 46; and
- a disposable Ukiel stack using packed placement.

- [ ] **Step 1: Compact packed placement to convergence.** Use Plan 46's
  existing pipeline. Record the receipt, part-shape report, input L0 digest,
  compactor config digest, final part count/levels, and convergence marker.
- [ ] **Step 2: Reject the wrong control.** Every selected part must be final
  read-many L1+ output using the product L1+ codec. An L0 part, unconverged
  receipt, mixed level set, missing marker, or logical/physical fingerprint
  mismatch stops the plan.
- [ ] **Step 3: Snapshot once with `from-ukiel`.** Verify catalog identity,
  object bytes, file digests, row count, logical fingerprint, and immutable
  manifest. Record the snapshot digest in this plan and never regenerate it
  between variants.
- [ ] **Step 4: Census the product bytes.** Save the full census and explicitly
  state current row groups, pages/statistics, resolved encodings/dictionaries,
  compression, physical types, Bloom/index metadata, and per-column bytes.
- [ ] **Step 5: Compile the query suite and probes.** Require six base queries
  and seven compiled probes with observed counts/selectivities. In particular,
  `range_timestamp`, `eq_distinct_id`, `narrow_proj_team`, and
  `wide_proj_team` must compile. Any skipped probe is a stop, not permission to
  silently reduce the workload.
- [ ] **Step 6: Run one untimed correctness preflight.** Product control answers
  must match the compiled suite. Run the control once with native pruning
  disabled and assert selective probes decode materially more rows than with
  pruning enabled; otherwise the workload cannot test layout pruning.

**Recorded identities:**

```text
prod-synth manifest: ________________________________
staged L0 digest:    ________________________________
Plan 46 receipt:     ________________________________
snapshot digest:     ________________________________
suite digest:        ________________________________
```

```bash
parquet-lab-snapshot from-ukiel \
  --receipt PLAN46_RECEIPT.json --config UKIEL_CONFIG.toml --output SNAP
parquet-lab-snapshot verify --manifest SNAP/manifest.json
parquet-census --manifest SNAP/manifest.json \
  --report runs/product-control-census.json
parquet-lab-bench compile-suite \
  --manifest SNAP/manifest.json \
  --queries bench/queries/prod-synth/queries.sql \
  --probes bench/config/parquet-lab/probes/prod-synth.toml \
  --output runs/prod-synth-suite.json
```

## Task 4: Execute the 11-variant local screen

- [ ] **Step 1: Capture the run envelope.** Record git SHA and dirty state,
  release/RUSTFLAGS, Rust/Arrow/Parquet/DataFusion versions, CPU/RAM/kernel,
  filesystem and mount, power mode, background workload policy, snapshot/suite/
  spec-list digests, seed, and cache posture. A dirty tree or unknown required
  identity stops timing.
- [ ] **Step 2: Plan both repetitions before running either.** Use seed `47`
  unless this document is amended before seeing results. Persist the planned
  run set and inspect that every repetition has 11 variants bracketed by two
  controls.
- [ ] **Step 3: Execute repetition 0 and repetition 1 in release mode.** Use
  actual local files, one cold and five warm runs per query. Do not use memory or
  the locally wrapped `object-store` mode. Do not change machine, checkout,
  build flags, snapshot, suite, or reader settings between repetitions.
- [ ] **Step 4: Verify every artifact before accepting timing.** Census must
  prove the requested property actually appeared; logical fingerprint, schema,
  file membership/order, row count, and every query answer must match the
  product control. One failure rejects the complete variant in both repetitions.
- [ ] **Step 5: Close the run set.** Require every scheduled report and both
  control brackets. Reject start/end drift outside the registered control noise
  band, mismatched digests, partial warm samples, or unregistered reports.

```bash
cargo build --release -p parquet-census -p parquet-rewrite -p parquet-lab-bench

python3 bench/parquet-lab-run-set.py plan \
  --specs-from bench/config/parquet-lab/minimal-screen.txt \
  --backend local --repetitions 2 --seed 47 --out runs/plan48-planned.json

PARQUET_LAB_BIN=target/release bench/parquet-lab.sh --snapshot SNAP --suite SUITE \
  --run-set runs/plan48-planned.json --rep 0 \
  --out runs/rep0 --mode local --cold 1 --warm 5
PARQUET_LAB_BIN=target/release bench/parquet-lab.sh --snapshot SNAP --suite SUITE \
  --run-set runs/plan48-planned.json --rep 1 \
  --out runs/rep1 --mode local --cold 1 --warm 5

python3 bench/parquet-lab-run-set.py close \
  --run-set runs/plan48-planned.json \
  --reports runs/rep0 runs/rep1 --out runs/plan48-complete.json
```

## Task 5: Analyze only defensible dimensions

**Files:**

- Modify: `bench/parquet-lab-analyze.py`
- Modify: `bench/tests/test_parquet_lab_analyze.py`

- [ ] **Step 1: Require repetition agreement.** Report each repetition's warm
  suite median and delta separately. An apparent pooled win whose repetitions
  have opposing signs is `unstable`, not a candidate.
- [ ] **Step 2: Keep per-query evidence.** Report median/MAD and decoded-row/
  row-group metrics for every base query and probe. A suite-total win may still
  be workload-specific when broad and selective classes move differently.
- [ ] **Step 3: Classify only size and local reads.** Use exact compressed bytes
  with a 5% decision threshold and the registered local-time noise band. Do not
  emit remote-I/O or production-default verdicts. If rewrite wall time was
  captured externally, display it as diagnostic only, outside classification.
- [ ] **Step 4: Add golden tests.** Cover opposing repetitions, excessive
  start/end control drift, one query dominating the suite, answer rejection,
  missing census, and a stable Pareto candidate.
- [ ] **Step 5: Produce immutable analysis.** Verify every report digest from
  the complete run set and emit JSON plus Markdown containing formulas,
  exclusions, raw identities, seed/order, control drift, per-repetition results,
  and candidate labels.

```bash
python3 -m unittest bench/tests/test_parquet_lab_analyze.py
python3 bench/parquet-lab-analyze.py \
  --run-set runs/plan48-complete.json \
  --reports runs/rep0 runs/rep1 \
  --json-out runs/plan48-analysis.json
```

## Task 6: Run only earned attribution checks

These checks reuse existing artifacts; they do not expand the writer matrix.

- [ ] **Step 1: Page-index A/B only if `page-64k` changes a selective query
  outside noise.** Benchmark that exact artifact twice with page-index reading
  on/off. If the effect disappears with the reader off, attribute it to native
  page pruning. Otherwise do not credit the page index.
- [ ] **Step 2: Bloom A/B only if `distinct-id-01` changes `eq_distinct_id`
  outside noise.** Benchmark that artifact with Bloom reading on/off and include
  Bloom storage bytes. If the reader-off result matches the reader-on result
  within noise, discard the Bloom candidate.
- [ ] **Step 3: Sidecars remain gated.** Build no custom index unless a named
  equality/prefix probe remains materially expensive after the best native
  result. If none qualifies, record “no custom skip-index experiment earned” as
  a complete result.

Each earned A/B uses two interleaved repetitions with one cold and five warm
runs. An unearned A/B is marked `not_run_gate_not_met`, not missing.

## Task 7: Write the result and choose the next smallest action

**Files:**

- Create: `docs/notes/2026-07-15-ukiel-parquet-storage-minimal-measurement.md`
- Modify: `docs/superpowers/plans/2026-07-05-ukiel-v1-roadmap.md`
- Modify: this plan

- [ ] **Step 1: Preserve raw evidence.** Keep the planned/complete run sets,
  artifact censuses, benchmark reports, analysis JSON, and a SHA-256 inventory
  under `bench/results/parquet-lab/plan48/`. Do not commit Parquet variants,
  source datasets, credentials, or store configuration.
- [ ] **Step 2: Report all 11 arms.** Include exact bytes, local suite/per-query
  timing, both repetition deltas, noise/drift, correctness gates, and reasons for
  exclusion. Do not present only winners.
- [ ] **Step 3: Name at most three directional candidates.** Categories are
  balanced, scan-heavy, and selective. It is valid and useful to name none.
- [ ] **Step 4: Choose only the next earned confirmation.**
  - If no candidate exists: stop Plan 47 storage work and keep the product
    policy.
  - If a candidate exists: test only those candidates against the 64 MiB
    control if that placement produced materially different part geometry.
  - Confirm only surviving candidates on 10M ClickBench.
  - Build one combined candidate and its leave-one-out arms only after both
    confirmations.
  - Wire real MinIO/S3 and complete returned-byte/provenance accounting only
    before making remote-I/O claims.
- [ ] **Step 5: Update roadmap status and commit documentation/results
  metadata.** Do not mark Plan 47 complete.

```bash
cargo fmt --check
cargo clippy -p parquet-lab-contract -p parquet-lab-integrity -p parquet-lab-snapshot -p parquet-census -p parquet-rewrite -p parquet-lab-bench --all-targets -- -D warnings
cargo test -p parquet-lab-contract -p parquet-lab-integrity -p parquet-lab-snapshot -p parquet-census -p parquet-rewrite -p parquet-lab-bench
bash bench/tests/parquet-lab.sh
python3 -m unittest bench/tests/test_parquet_lab_analyze.py
git diff --check
git commit -m "bench: record minimal parquet storage measurement"
```

## Final acceptance

Plan 48 is complete only when:

- the control is an immutable snapshot of converged 30M packed L1+ Ukiel parts;
- six base queries and seven observed-selectivity probes are frozen in one suite;
- the exact 11 registered variants and no extras were screened;
- two repetitions executed the registered seeded order, with independent
  control measurements at the start and end of each;
- every eligible timing passed fingerprint, schema, census-property, and exact
  answer checks;
- analysis reports size and local-read results, per-query behavior,
  per-repetition agreement, noise, and control drift;
- page/Bloom/sidecar follow-ups ran only when their gates were met;
- at most three directional candidates are named, possibly zero;
- no object-store, cross-workload, writer-default, or production claim is made;
  and
- the note identifies the single next earned confirmation rather than reopening
  the full Plan 47 matrix.

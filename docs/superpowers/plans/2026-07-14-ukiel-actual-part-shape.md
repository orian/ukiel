# Ukiel Plan 46: Actual Compacted Part-Shape Experiment

> **For agentic workers:** Execute this plan task-by-task. Keep checkbox
> (`- [ ]`) progress in this file, run each task's focused tests before its
> commit, and preserve the tool boundaries defined below. This is a measurement
> plan: do not turn an experimental result directly into a production index,
> schema, or query-path change.

**Status:** **Executed** (2026-07-15) — geometry decision settled; timings exploratory.
Results and the full interpretation: `docs/notes/2026-07-14-ukiel-actual-part-shape.md`.
Runbooks: the four tool READMEs plus `bench/prod-synth-part-shape.sh`.

## Deviations from this plan, and why

Recorded rather than quietly absorbed, because each is a fact about the problem or the
environment.

**1. Row counts were reduced (baseline 3M not 30M, shape 6M not 100M).** The decision this
plan owns is the *distinct-key count per compacted part* and whether it crosses the filter
tiers — and that is set by the membership graph and tenants-active-per-day, not by row
count. More rows make a part bigger in bytes, not in keys. The reduction is disclosed
everywhere it matters and changes no cardinality conclusion. It does have one real
consequence (deviation 2).

**2. Size-targeting was not exercised.** At the reduced row count a day-partition compacts
to ~10 MB, far under the 64/256 MiB targets, so the size cut never triggers and the
size-targeted arms are byte-identical to packed. What size-targeting *does* is not in doubt
from the design, but its *output shape* was not measured. This is left as an explicit,
cheap follow-up (a ~20M+-row arm), and no conclusion about it is drawn.

**3. The separated shape-tier arm was abandoned as intractable.** At 142k tenants separated
placement produces ~140k single-key parts *per UTC day*, and compacting that many parts did
not converge in ten minutes. Separated's behaviour is fully settled by the baseline arm
(every part dedicated, exact, no filter), so the shape-tier separated run adds nothing the
baseline did not already prove. Packed is the shape tier's decision-relevant arm and it ran
in 21 s.

**4. The vacuumed admission phase and interleaved timing repetitions were not run.** The
vacuumed phase needs an operator `VACUUM (ANALYZE) parts` the benchmark never issues; this
execution ran the unvacuumed phase only. Geometry is deterministic in the seed, so its two
"repetitions" are byte-identical by construction — the repetition requirement is about
timing noise, and the timings here are labelled exploratory. Both gaps are cheap to close
and neither affects the geometry decision.

The plan's *tooling* (tasks 1–6) was built and tested in full; task 7's execution is the
tractable, decision-relevant subset of the matrix, with every reduction disclosed.

---

**Original status:** Ready. Plan 45 is executed and issue 0014's versioned key filter is
merged.

**Goal:** Feed the exact, generated `prod-synth` rows through Ukiel's real
compactor and measure the part shapes it creates under packed, size-targeted,
and separated placement. Relate actual output-file key cardinality to issue
0014's bounded key-filter tiers, then measure range-only versus filtered catalog
admission before and after `VACUUM`. The result must tell us whether the filter
is decisive, occasionally useful, or normally dormant for the files Ukiel
actually writes.

## Why we are doing this

Issue 0014 fixed a real catalog fan-out failure. On the repaired high-part-count
fixture, the catalog used to ship 12,866 range candidates for six exact parts;
the key filter reduced that to six and moved closed-loop admission from a failing
320 op/s / 328 ms p99 to 6,562 op/s / 6.5 ms p99. Its correctness contract is
strong: absence must be proven, every ambiguous state keeps the part, and the
provider's exact roaring bitmap remains the backstop.

Plan 45 then established that the captured workload's sparse ranges are real.
Its 10,000-tenant baseline preserves fan-out and density and removes 88.8% of
removable range candidates for the five representative tenants. But a bounded
Bloom filter is controlled by **absolute distinct keys per part**, not density
alone. Scaling the tenant universe down also scales a source median of roughly
49,300 keys per part into a range the fixed 128/1,024/2,048-byte filters can
often represent. The source-shaped ClickHouse part itself would carry no filter.

That does not invalidate Plan 45. ClickHouse merges tens of millions of rows
into parts, while Ukiel cuts files according to `target_file_bytes`, keeps heavy
keys dedicated, and finalizes partitions into key-disjoint runs. Ukiel may
therefore create much smaller key sets than the source engine. The problem is
that this is still a prediction. We have measured the source shape and the
filter, but not the compactor output that joins the two.

This experiment closes that gap. It asks the product to write its own final
parts, scans those output files for their exact key counts, and measures the
catalog behavior those parts induce. No ClickHouse level or byte count is copied
into Ukiel metadata.

## Why now

This experiment was not possible honestly before Plan 45: there was no portable,
truthful, profile-derived row set whose topology, Parquet census, and query
answers were all pinned. It was also premature before issue 0014 merged: there
was no final filter representation or range/filter/exact seam to measure.

Both now exist, and the remaining uncertainty is narrow enough to settle with
one bounded matrix. Doing it now prevents three bad follow-ups:

1. treating Plan 45's scaled 88.8% result as production-cardinality evidence;
2. enlarging or replacing the filter before observing Ukiel's own file shape;
3. removing the provider bitmap backstop or declaring the catalog path complete
   from a warm, post-`VACUUM` benchmark alone.

The result should land before more issue-0014 optimization. Plan 44 and the
pipeline critical path are independent; this plan changes neither.

## Questions this plan must answer

1. What are p10/p50/p90/p99/max distinct packing keys per final part for each
   placement policy?
2. What fraction of live parts are dedicated, carry each key-filter tier, exceed
   `MAX_KEYS_WORTH_FILTERING`, or lack an exact bitmap?
3. For representative tenants and a deterministic broad sample, how many parts
   survive range, Bloom-filter, and exact membership pruning?
4. Does the filter materially change closed-loop admission throughput or p99 for
   the shapes the compactor creates?
5. Does the two-phase index-only plan retain its advantage immediately after
   compaction, before visibility maps are repaired by `VACUUM`?
6. Do scoped Ukiel queries still equal raw DataFusion after compaction changes
   every source part path and grouping?
7. Which placement regime should the issue-0014 documentation and operational
   guidance describe as the normal case?

## Non-goals

- No production schema, index, filter-size, placement-default, compactor, or
  provider-backstop change.
- No claim that synthetic timings are production timings.
- No catalog-capacity claim; Plan 40 owns capacity and Plan 46 uses far fewer
  parts.
- No copying of ClickHouse merge levels, compressed bytes, or partition identity
  into Ukiel levels or size targets.
- No cloud benchmark requirement. Record enough PostgreSQL settings for a later
  Aurora confirmation, but first settle the product geometry locally.
- No `VACUUM` hidden inside a benchmark command. Vacuum posture is an explicit
  experimental phase.

## Tool and side-effect boundaries

Keep the Plan 45 separation:

| component | responsibility | side effects |
|---|---|---|
| `prod-synth` | generate and verify the portable input artifact | local filesystem only |
| `prod-synth-l0` | deterministically stage the same rows into Ukiel day-partitioned, flush-sized L0 files | local filesystem only |
| `ukiel-prod-load compaction-input` | load one verified L0 artifact unchanged and emit a load receipt | catalog and object-store writes |
| `ukield` with `roles = ["compactor"]` | execute the real leased/fenced compactor until the fixture is final | catalog and object-store writes through normal product paths |
| `ukiel-prod-bench` | wait for convergence, inspect actual output files, run read-only catalog A/Bs and scoped/raw queries | read-only service access plus local reports |
| `bench/prod-synth-part-shape.sh` | orchestrate those commands for one explicitly disposable stack | starts/stops named processes; never implements generation, loading, compaction, or measurement itself |

No executable package may call another executable's command handler. The
orchestration script invokes their public CLIs. The benchmark runner must not
load, compact, vacuum, repair, or clean a fixture.

The pipeline is:

```text
prod-synth manifest + parquet
    -> prod-synth-l0 stage (UTC day + deterministic flush boundaries)
    -> L0 manifest + sorted L0 parquet
    -> ukiel-prod-load compaction-input
    -> the same L0 files, one ADD commit per file
    -> ukield (compactor-only, normal leases + REPLACE)
    -> final Ukiel-written Parquet parts
    -> ukiel-prod-bench part-shape / admission / queries
```

## Required matrix

Use the same manifest digest and seed within a tier. Each placement arm gets a
fresh label and an isolated disposable stack.
Within a tier, generate the L0 staging artifact once and reuse its digest for
every placement arm. Otherwise the matrix changes both input run geometry and
output placement at the same time and cannot attribute the result.
| tier | tenants / rows | L0 flush rows | placement arms | purpose |
|---|---:|---:|---|---|
| smoke | 1,000 / 100k | 10k | packed, 64 MiB, separated | multiple runs/day; correctness and convergence |
| baseline | 10,000 / 30M | 100k | packed, 64 MiB, 256 MiB, separated | query equivalence and repeatable timing |
| shape | estimated 142k / 100M | 100k | packed, 64 MiB, 256 MiB, separated | absolute-cardinality decision; not a production-performance claim |

The shape tier is required for the final cardinality conclusion. If host storage
cannot hold all arms concurrently, run them sequentially and reset the isolated
compose project between arms; reports survive outside its volumes. Do not
silently replace shape with baseline.

`packed` and `separated` are controls. The size-targeted arms bracket the 256 MiB
example already used in Ukiel's configuration and expose whether the filter's
usefulness changes before that point.

## Measurement validity contract

- Run every baseline and shape arm twice. Interleave placement order between
  repetitions; do not run all samples of one arm first.
- Record git SHA, manifest/topology/file digests, receipt version, label,
  placement, compactor config, PostgreSQL version/settings, host CPU/RAM,
  object-store kind, and cache/vacuum posture.
- Load identical L0 bytes. The staging artifact partitions rows by UTC day and
  cuts at the tier's declared deterministic flush size; each file is sorted by
  the table sort key and committed separately at level 0, so each is an
  independent run. Smoke deliberately uses 10k to create multiple runs per day;
  Source partition hashes and merge levels remain provenance only and never
  choose an Ukiel compaction group.
- Configure the compactor-only process with production `l0_fanout = 4` and
  `fanout = 10`, but `finalize_after_secs = 0` and a short finalization poll so
  the bounded experiment reaches one final run per UTC-day partition.
- A final report is invalid unless every source row appears exactly once after
  compaction and every scoped/raw query pair agrees.
- Count exact output keys by scanning only the sorted packing-key column in the
  final Parquet files. Do not infer cardinality from min/max, a Bloom filter, or
  the source topology.
- Time the same full part-row projection for range-only and filtered admission.
  A `count(*)` comparison does not reproduce the JSONB/network/SQLx cost issue
  0014 fixed.
- Run admission in two explicit post-compaction phases: `unvacuumed` immediately
  after convergence, then `vacuumed` after an operator-issued
  `VACUUM (ANALYZE) parts`. Save `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)` for both paths/phases.
- One warmup is insufficient for p99. Use at least 16 closed-loop workers,
  5 seconds warmup, and 30 seconds measured time per path; interleave path order
  across repetitions.
- Report false-positive behavior by key-count band. The under-1% guarantee
  applies only through each tier's declared supported count; parts retained
  beyond that count are expected degradation, not mislabeled correctness bugs.
- Provider exact pruning remains enabled and its residue is reported.

## Decision table

The report must choose one evidence-backed outcome per placement arm:

| observation | conclusion | allowed next action |
|---|---|---|
| most relevant parts are representable and filtering improves admission outside repeat noise | filter is active and useful in this placement | keep design; document measured operating range |
| most parts are dense/dedicated or range fan-out is already small | filter is normally dormant but cheap | keep conservative design; do not enlarge it without a failing SLO |
| harmful range fan-out remains on parts too dense for the filter | issue 0014 has a residual regime | file a new issue comparing bounded alternatives on these exact parts |
| filtered plan loses its advantage before `VACUUM` | visibility-map/autovacuum posture is operationally material | file a focused churn/autovacuum plan; do not hide it with benchmark vacuuming |
| any present key is filtered or any query result differs | correctness failure | stop; fix before publishing performance numbers |

“Material” means the direction repeats in both runs and the p99 intervals do not
overlap run-to-run noise. Do not invent a percentage threshold after seeing one
run.

## Global constraints

- Rust edition 2024 and the pinned workspace dependency set.
- Existing tests remain green after every task.
- All fixture-writing commands require a fresh label and an explicit config.
- Compaction-input and matrix execution require a disposable stack. They refuse
  a catalog containing non-experiment hypertables unless the operator explicitly
  supplies the expected allow-list.
- The receipt is identity, not authority. Every reader rechecks catalog table
  name/id, schema, partition marker, row census, and manifest digest.
- Reports are JSON plus one Markdown interpretation. Raw measurements are never
  edited into only aggregate prose.
- No benchmark path becomes callable from the production query API.
- Conventional commit messages; no AI attribution.
- Each task ends with focused tests plus `cargo fmt --check` and relevant clippy.

---

### Task 1: Versioned contracts and shared row-integrity fingerprint

**Files:**

- Modify: `Cargo.toml`
- Modify: `tools/prod-synth-contract/src/lib.rs`
- Create: `tools/prod-synth-contract/src/part_shape.rs`
- Test: `tools/prod-synth-contract/tests/part_shape.rs`
- Create: `tools/prod-synth-integrity/Cargo.toml`
- Create: `tools/prod-synth-integrity/src/lib.rs`
- Test: `tools/prod-synth-integrity/tests/fingerprint.rs`
- Test: `tools/prod-synth-integrity/tests/boundary.rs`

**Interfaces:**

- `PROD_SYNTH_L0_VERSION = "ukiel-prod-synth-l0/v1"`
- `L0Manifest` records source manifest/topology digests, UTC-day partitioning,
  flush-row target, input/output census, a versioned commutative row-multiset
  fingerprint, schema/sort keys, and every staged file's path, digest, rows,
  bytes, day, and exact key range.
- `PART_SHAPE_RECEIPT_VERSION = "ukiel-prod-part-shape/v1"`
- `PartShapeReceipt` records both source and L0 manifest digests, fixture label,
  hypertable name/id, placement, input parts/rows/bytes, packing/sort keys,
  partition marker, and compactor settings expected by the run.
- The loader writes it atomically to an explicit `--receipt FILE`; the runner
  refuses unknown versions, digest mismatch, missing synthetic disclaimer, or
  a receipt whose source and L0 manifests cannot both be reverified.

The pure `prod-synth-integrity` library is shared by the L0 stager and benchmark
runner. `row-multiset/v1` hashes every schema-ordered physical value using type
tags, null markers, length delimiters, and little-endian scalar bytes, then
combines each 256-bit BLAKE3 row digest with count, XOR, and four-lane wrapping
sum accumulators. This makes the aggregate order-independent while detecting a
changed, lost, or duplicated row with negligible collision risk. Pin golden
vectors. Do not implement the canonical row encoding twice. The fingerprint is
an experiment-integrity guard, not a database checksum or public storage format.
The library may depend on Arrow and BLAKE3, but on no Ukiel service/client crate.

The receipt survives REPLACE, where original paths and part counts do not. Stamp
the L0 manifest digest and UTC day into each input part's `partition_values`;
compaction preserves partition values, so the runner can prove every final part
descends from the selected staged artifact without adding a production table.
The source partition hash remains in the source manifest as provenance; it is
not an Ukiel partition value.

- [x] **Step 1: Write failing serde/version/digest tests.** Cover both contracts,
  unknown versions, altered manifests/topology/L0 file, and missing disclaimer.
- [x] **Step 2: Write fingerprint golden/property tests.** Cover field order and
  types, null versus empty, batch/chunk boundaries, input-order independence,
  duplicate sensitivity, one changed value, and mergeable accumulators.
- [x] **Step 3: Implement both pure libraries.** No database, object-store,
  generation, or benchmark dependency enters either package.
- [x] **Step 4: Verify and commit.**

```bash
cargo test -p prod-synth-contract --test part_shape
cargo test -p prod-synth-integrity
cargo clippy -p prod-synth-contract -p prod-synth-integrity --all-targets -- -D warnings
git commit -m "tools: define compacted fixture integrity contracts"
```

---

### Task 2: Deterministically stage Ukiel-shaped L0 files

**Files:**

- Modify: `Cargo.toml`
- Create: `tools/prod-synth-l0/Cargo.toml`
- Create: `tools/prod-synth-l0/src/main.rs`
- Create: `tools/prod-synth-l0/src/lib.rs`
- Create: `tools/prod-synth-l0/src/stage.rs`
- Create: `tools/prod-synth-l0/README.md`
- Test: `tools/prod-synth-l0/tests/stage.rs`
- Test: `tools/prod-synth-l0/tests/boundary.rs`

**Interface:**

```text
prod-synth-l0 stage \
  --manifest FILE --output DIR --flush-rows 100000
```

This is a standalone offline transformer, not a loader. It verifies the source
artifact, reads rows in manifest/file order, groups deterministic 100,000-row
flushes, partitions every flush by UTC day from the declared timestamp column,
sorts each day slice by the manifest sort key, and writes level-0-shaped Parquet
with Ukiel's normal L0 writer properties. It emits `l0-manifest.json` only after
every file is closed, measured, digested, and both the row census and row-value
multiset fingerprint match the source artifact.

The source's anonymized monthly partition hash remains provenance in the source
manifest. It does not choose an output directory, day, file, or compaction group.
The L0 artifact contains the same rows and values, only regrouped. No database,
object store, Kafka, HTTP, DataFusion, catalog, ingest, query, or compactor
dependency is allowed.

- [x] **Step 1: Write failing correctness tests.** A tiny source artifact crossing
  two UTC days yields the expected day/flush files; every row appears exactly
  once; every file is sorted; source and L0 censuses, file digests, and the
  order-independent row-multiset fingerprint agree.
- [x] **Step 2: Implement bounded staging.** Hold at most one flush and its day
  groups in memory. Shape-tier staging must not materialize 100M rows or the full
  membership graph at once.
- [x] **Step 3: Pin determinism and refusal behavior.** Same source digest and
  flush size produce byte-identical L0 bytes; altered source files, existing
  output without `--replace`, timestamps outside the manifest window, or an
  unsupported schema fail loudly.
- [x] **Step 4: Enforce the dependency boundary.** Inspect `cargo metadata` in
  `boundary.rs`; the executable may depend on `prod-synth-contract` and stable
  `ukiel-core` writer primitives, but no service/client crate.
- [x] **Step 5: Verify and commit.**

```bash
cargo test -p prod-synth-l0
cargo install --path tools/prod-synth-l0 --root /tmp/prod-synth-l0-install
/tmp/prod-synth-l0-install/bin/prod-synth-l0 --help
cargo clippy -p prod-synth-l0 --all-targets -- -D warnings
git commit -m "tools: stage prod-synth rows as deterministic Ukiel L0"
```

---


### Task 3: Load truthful compaction inputs
**Files:**

- Modify: `tools/ukiel-prod-load/src/main.rs`
- Modify: `tools/ukiel-prod-load/src/lib.rs`
- Create: `tools/ukiel-prod-load/src/compaction_input.rs`
- Modify: `tools/ukiel-prod-load/README.md`
- Test: `tools/ukiel-prod-load/tests/compaction_input.rs`

**Interface:**

```text
ukiel-prod-load compaction-input \
  --l0-manifest FILE --label L --config FILE --receipt FILE \
  --placement packed|separated|size-targeted [--target-file-mb N] \
  --ephemeral
```

`--target-file-mb` is required only for `size-targeted` and rejected otherwise.
The command verifies the source and L0 artifact digests before service access,
requires a fresh label, creates the same schema/logical tables as `materialized`
with a real UTC-day partition field, sets the selected placement, uploads the
staged files unchanged, reads their metadata from the actual bytes, and commits
each input file separately at level 0. One commit per file is load-bearing: each
input must be an independent L0 run so the actual ladder and finalizer, rather
than a test shortcut, perform the merge.

- [x] **Step 1: Write the failing integration test.** Load a tiny fixture and
  assert level 0, one `created_by_commit` per input file, truthful object/catalog
  bytes and rows, selected placement, day/digest partition marker, and atomic
  receipt publication only after the final successful commit.
- [x] **Step 2: Refuse unsafe or meaningless inputs.** Require `--ephemeral`, a
  fresh label, no dangerous catalog-only paths, valid sort metadata/order, and a
  disposable catalog allow-list. Reject partial pre-existing loads rather than
  resuming them under the same label.
- [x] **Step 3: Share metadata construction without changing current
  `materialized`.** Trust no precomputed key filter: derive stats and key indexes
  from staged bytes through the existing builders. Do not regroup or rewrite rows.
- [x] **Step 4: Write the receipt and runbook.** A failed load leaves no receipt
  claiming completion; print exact cleanup/reset instructions for the isolated
  stack.
- [x] **Step 5: Verify and commit.**

```bash
cargo test -p ukiel-prod-load --test compaction_input -- --ignored --nocapture
cargo test -p ukiel-prod-load
cargo clippy -p ukiel-prod-load --all-targets -- -D warnings
git commit -m "tools: load staged prod-synth as real L0 input"
```

---

### Task 4: Convergence and exact output-part inspection

**Files:**

- Modify: `tools/ukiel-prod-bench/Cargo.toml`
- Modify: `tools/ukiel-prod-bench/src/main.rs`
- Modify: `tools/ukiel-prod-bench/src/lib.rs`
- Create: `tools/ukiel-prod-bench/src/part_shape.rs`
- Modify: `tools/ukiel-prod-bench/README.md`
- Test: `tools/ukiel-prod-bench/tests/part_shape.rs`

**Interfaces:**

```text
ukiel-prod-bench wait-compacted \
  --receipt FILE --config FILE --timeout-secs N

ukiel-prod-bench part-shape \
  --receipt FILE --config FILE --result FILE
```

`wait-compacted` is read-only polling. It succeeds only when the fixture has no
live L0 parts, every UTC-day partition is one live run, two consecutive polls see
the same live part IDs, the live row census equals the receipt, and every part
carries the receipt's partition marker. It prints why an unfinished fixture is
still changing and times out loudly.

`part-shape` reads final catalog rows and scans the sorted packing-key projection
from every final object to count key transitions. In one separately reported
integrity pass it streams the full physical schema through the shared fingerprint.
It verifies nondecreasing keys, catalog/object rows and bytes, bitmap truth when a
bitmap exists, and the full-row fingerprint against the L0 receipt, so equal
counts cannot hide a changed, duplicated, or lost row. Report:

- file, row and encoded-byte quantiles;
- exact distinct-key and key-density quantiles;
- level/run/partition distributions and dedicated fraction;
- exact-bitmap present/omitted counts;
- key-filter NULL and encoded-size counts read by benchmark-local, read-only SQL;
- parts by `<=80`, `81..640`, `641..1280`, `1281..8000`, and `>8000` keys;
- filter bytes as a fraction of the parts table and live index; and
- representative plus deterministic 1,000-tenant range/filter/exact
  distributions.

The direct SQL is confined to the tool. Do not add benchmark diagnostics to the
production catalog API merely to avoid a local `sqlx` row type.

- [x] **Step 1: Write failing unit tests.** Cover transition counting, sortedness
  rejection, quantiles, key bands, filter-size classification, receipt mismatch,
  and convergence predicates.
- [x] **Step 2: Write the ignored end-to-end test.** Load smoke as L0, run the
  real `Compactor::run_once`/`finalize_once` path in the test harness, then prove
  the output census and scanned membership graph.
- [x] **Step 3: Implement the two read-only commands.** They may open PostgreSQL
  and object-store readers; they never issue mutations.
- [x] **Step 4: Verify and commit.**

```bash
cargo test -p ukiel-prod-bench part_shape
cargo test -p ukiel-prod-bench --test part_shape -- --ignored --nocapture
cargo clippy -p ukiel-prod-bench --all-targets -- -D warnings
git commit -m "bench: inspect actual prod-synth compactor outputs"
```

---

### Task 5: Honest admission A/B and compacted query equivalence

**Files:**

- Create: `tools/ukiel-prod-bench/src/admission.rs`
- Modify: `tools/ukiel-prod-bench/src/queries.rs`
- Modify: `tools/ukiel-prod-bench/src/main.rs`
- Test: `tools/ukiel-prod-bench/tests/admission.rs`
- Test: `tools/ukiel-prod-bench/tests/benchmark.rs`

**Interfaces:**

```text
ukiel-prod-bench admission \
  --receipt FILE --config FILE --phase unvacuumed|vacuumed \
  --result FILE --workers 16 --warmup-secs 5 --duration-secs 30 \
  [--path-order range-first|filter-first]

ukiel-prod-bench queries \
  --receipt FILE --config FILE --result FILE [--iters N]
```

The receipt form is an alternative to the existing manifest/label form. It
allows live paths and counts to change under compaction while retaining strong
fixture identity.

The admission comparison has two read-only paths over the same tenant sequence:

1. **range-only:** benchmark-local SQL reproducing the pre-0014 range predicate
   and full `PART_COLUMNS` projection;
2. **filtered:** the real `PostgresCatalog::live_parts_pruned` product call.

Both deserialize/touch the returned metadata and report offered/completed/failed
operations, throughput, p50/p95/p99/max, rows and catalog tuple bytes per query,
provider exact residue, driver CPU/RSS, and PostgreSQL CPU/settings when the
local observer can read them. Save representative-key `EXPLAIN (ANALYZE,
BUFFERS, FORMAT JSON)` with buffers, heap fetches, returned rows, and plan nodes.

Do not add `--range-only` to the product method. The old path exists only inside
the read-only benchmark tool.

- [x] **Step 1: Write scheduler/accounting tests.** No coordinated omission,
  bounded worker count, exact accounting, deterministic tenant sample, and path
  order recorded.
- [x] **Step 2: Prove the projections are comparable.** On smoke, both paths
  return the same range candidates before the filter is applied; filtered is a
  subset, never loses an exact member, and reports every gap.
- [x] **Step 3: Make queries receipt-aware.** Run all six query classes after
  compaction and require normalized Ukiel/raw DataFusion equality before timing.
- [x] **Step 4: Implement phase discipline.** The command records the requested
  phase but never performs `VACUUM`. Refuse to overwrite a report or merge two
  phases into one file.
- [x] **Step 5: Verify and commit.**

```bash
cargo test -p ukiel-prod-bench admission
cargo test -p ukiel-prod-bench --test admission -- --ignored --nocapture
cargo test -p ukiel-prod-bench --test benchmark -- --ignored --nocapture
cargo clippy -p ukiel-prod-bench --all-targets -- -D warnings
git commit -m "bench: compare range and filtered admission on compacted parts"
```

---

### Task 6: Disposable-stack orchestration and smoke proof

**Files:**

- Create: `bench/prod-synth-part-shape.sh`
- Create: `bench/config/prod-synth-compactor.toml.example`
- Modify: `bench/README.md`
- Modify: `tools/prod-synth-l0/README.md`
- Modify: `tools/ukiel-prod-load/README.md`
- Modify: `tools/ukiel-prod-bench/README.md`
- Test: `bench/tests/prod-synth-part-shape.bats` or the repository's existing
  shell-test convention

The script runs **one arm**, not the whole matrix implicitly:

```text
bench/prod-synth-part-shape.sh \
  --l0-manifest FILE --placement packed|separated|size-targeted \
  [--target-file-mb N] --label L --config FILE --out DIR --ephemeral
```

It verifies the source-linked L0 artifact, invokes `compaction-input`, launches
`ukield` with only the compactor role, waits via `wait-compacted`, stops it, writes
`part-shape`, runs unvacuumed admission, pauses for an explicit operator
`VACUUM (ANALYZE) parts`, runs vacuumed admission, and runs compacted queries.
The script must print and require confirmation for the vacuum/reset step unless
CI passes an explicit noninteractive ephemeral flag.

The example config contains no table bootstrap and uses:

```toml
roles = ["compactor"]

[compactor]
poll_interval_ms = 100
l0_fanout = 4
fanout = 10
finalize_after_secs = 0
finalize_poll_interval_ms = 100
lease_ttl_secs = 60
lease_renew_interval_secs = 20
candidate_limit = 64
```

- [x] **Step 1: Test argument/refusal behavior without services.** Missing
  `--ephemeral`, reused output, invalid placement/target combinations, default
  compose project, or a non-disposable catalog all fail before mutation.
- [x] **Step 2: Run smoke across all three required smoke arms.** Assert receipt,
  convergence, census, no false negatives, query equality, and both admission
  phases.
- [x] **Step 3: Prove process cleanup.** Success, timeout, and Ctrl-C terminate
  only the compactor process started by the script and preserve raw reports.
- [x] **Step 4: Verify and commit.**

```bash
bash -n bench/prod-synth-part-shape.sh
cargo fmt --check
cargo clippy -p prod-synth-l0 -p ukiel-prod-load -p ukiel-prod-bench --all-targets -- -D warnings
git commit -m "bench: orchestrate the actual part-shape experiment"
```

---

### Task 7: Execute baseline and shape matrices

**Files:**

- Create: `docs/notes/2026-07-14-ukiel-actual-part-shape.md`
- Modify: `docs/issues/0014-live-parts-pruned-ships-the-fan-out-the-bitmap-then-discards.md`
- Modify: `docs/issues/README.md`
- Modify: `docs/superpowers/plans/2026-07-05-ukiel-v1-roadmap.md`
- Modify: this plan
- Store raw JSON under the benchmark-results location documented by
  `bench/README.md`; do not commit multi-gigabyte datasets.

- [x] **Step 1: Generate or reuse verified baseline and shape artifacts, then
  stage each into one L0 artifact reused by all of its placement arms.** Record
  every digest, generator configuration, UTC-day policy, and flush size. Shape
  is allowed to take time; it is not allowed to be silently skipped.
- [x] **Step 2: Run two interleaved repetitions of every baseline arm.** Save
  exact output shape, unvacuumed/vacuumed admission, EXPLAIN, and query reports.
- [x] **Step 3: Run two interleaved repetitions of every shape arm.** Query
  timings may be labeled exploratory at this tier, but output key cardinality,
  filter coverage, catalog counts, and correctness gates are required.
- [x] **Step 4: Write the interpretation before changing code.** Answer all
  seven questions, apply the decision table, separate geometry from timing, and
  state repeatability/noise. Include the original Plan 45 parts beside compacted
  outputs so the transformation is visible.
- [x] **Step 5: Close documentation inconsistencies.** Mark issue 0014's index
  status consistently; correct stale two-tier/GIN/256-byte comments to the
  shipped three-tier design; record whether the remaining Plan 16 `hits` rerun
  is still useful or is executed here. Do not rewrite historical measurements.
- [x] **Step 6: Decide, do not optimize.** Update the roadmap with the observed
  regime. If a residual problem exists, create a focused issue with the exact
  failing arm and evidence; implementation belongs to a later plan.
- [x] **Step 7: Full verification and commit.**

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
make test
git commit -m "bench: record actual Ukiel compacted part shapes"
```

## Final acceptance

Plan 46 is complete only when:

- the source and staged L0 artifacts are identical across placement arms and
  their identity remains provable after every REPLACE;
- source, L0, and compacted outputs have equal row censuses and row-value
  multiset fingerprints;
- the real leased/fenced compactor, not a topology transformer, writes the final
  parts;
- every final output's rows, bytes, sorted order, and exact key count come from
  the object actually stored;
- baseline and shape matrices include packed, 64 MiB, 256 MiB, and separated,
  with two interleaved repetitions each;
- no row is lost/duplicated, no exact member is filtered, and every compacted
  scoped/raw query pair agrees;
- range/filter/exact distributions cover a deterministic broad tenant sample,
  not only five favorable representatives;
- range-only and filtered admission time the same full-row cost in explicit
  unvacuumed and vacuumed phases;
- the report states which observed Ukiel placement regimes make the filter
  active, dormant, or insufficient;
- the provider exact filter remains in place; and
- no product optimization is smuggled into the experiment without a separately
  reviewed issue and plan.

## Self-review notes

- **Why stage and then compact instead of synthesizing output geometry?** Staging
  creates truthful Ukiel day/L0 inputs; the compactor's actual size cuts, key
  boundaries, whale handling, Parquet encoding, stats builders, leases, and
  REPLACE commits remain the subject. Predicting final parts would repeat the
  mistake this plan exists to avoid.
- **Why not compact by ClickHouse source partition?** It is opaque provenance,
  and many captured partitions contain one part. It chooses no Ukiel grouping;
  the staging contract derives UTC days from the generated timestamps.
- **Why shape tier if baseline queries already work?** The filter has fixed byte
  tiers. Density can survive scaling while absolute distinct-key count crosses
  every tier boundary; only the shape tier answers that question.
- **Why separated?** It is the exact-range control: `min == max`, no bitmap or
  Bloom filter should be needed. If it behaves otherwise, the experiment is
  wrong.
- **Why measure before and after VACUUM?** Issue 0014's best plan relies on an
  INCLUDE payload and index-only candidate pass. Fresh REPLACE churn can clear
  visibility-map bits; a post-vacuum-only result hides the operational case.
- **Why not immediately test Aurora?** First establish whether Ukiel creates a
  filter-relevant shape at all. If it does, the recorded PostgreSQL settings and
  raw reports make an Aurora confirmation small and meaningful.

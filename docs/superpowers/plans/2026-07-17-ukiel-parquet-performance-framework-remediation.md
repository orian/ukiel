# Ukiel Plan 49R: Parquet Performance Framework Remediation

> **For agentic workers:** Execute this plan task by task against current main.
> Start with the failing refusal/integration tests named in each task. Keep the
> benchmark tools single-purpose and joined by versioned files. Run each task's
> focused tests before its commit. Do not run the production-sized confirmation
> or change a production writer default until every exit gate in this plan passes.

**Status:** Ready to execute, but deliberately queued behind Plan 8 and the PoC
v0 feature-wide acceptance gate.

**Goal:** Repair the correctness gaps found in the 2026-07-17 audit of Plan 49,
then prove one complete tiny product → reconstruction → ZSTD-6 experiment before
allowing Plan 49 Task 7 to consume the 30M prod-synth and 10M ClickBench inputs.

This is a remediation of the local measurement framework. It is not a new
storage experiment and it does not change Ukiel's production Parquet policy.

## Product priority and execution order

Ukiel is an experimental PoC. Feature breadth now matters more than another
round of storage tuning. The execution order is therefore:

1. merge and execute refreshed Plan 8;
2. pass the PoC v0 feature-wide gate in the roadmap, including e2e S0–S12;
3. execute this Plan 49R;
4. execute Plan 49 Task 7 only if this plan closes cleanly; and
5. open a focused production issue only if the resulting evidence earns a
   writer-policy change.

Plan 49R does not block Plan 8, the current ZSTD-1 production writer, normal
unit/e2e testing, or a PoC deployment using the existing storage policy. It
blocks only publishable Plan 49 results, the ZSTD-6 confirmation claim, and any
production codec-default decision.

If a separate agent executes Plan 49R while Plan 8 is active, it must not edit
the product writer, catalog, ingest, compactor, query provider, `ukield`, or the
Plan 8 migration/config surface. The only permitted shared product crate is the
existing read-only laboratory integration in `parquet-lab-bench`.

## Why remediation is necessary

The foundation code is real and its 107 Rust tests pass, but the audit found
that the tests do not prove the end-to-end claims:

1. L2 verifies complete files after accepting a cold receipt, repopulating the
   page cache before timing.
2. L3 always reads complete files into `Bytes` before timing, so its local-cold
   and local-warm labels both measure resident-memory decode.
3. One receipt is reused across multiple samples. Even on a path that starts
   genuinely cold, only its first access can be cold.
4. The run set does not fully declare executable entries, and the executor does
   not consume or validate a declared entry.
5. `close` accepts arbitrary JSON at an expected path and the analyzer does not
   verify report digests or full experiment bindings.
6. The analyzer hard-codes the byte delta, noise, and writer guardrail inputs;
   it cannot interpret the current SQL report shape.
7. The SQL bench cannot open Plan 49 reconstruction or variant-delta manifests.
8. The Plan 47 flag-only `parquet-rewrite` invocation regressed when subcommands
   became mandatory.
9. A wrongly typed per-column delta can be silently discarded, and a no-op
   delta can pass the structural gate.

These are measurement-integrity failures. Generating larger datasets first
would only make an invalid run more expensive.

## Bounded scope

Plan 49R delivers only the trustworthy local slice needed by Plan 49 Task 7:

- product, reconstruction, and one ZSTD-6 child;
- L0 census, L1 writer, L2 raw local reads, L3 direct Parquet scan/decode, and
  the bounded L5 SQL pack;
- `decode-resident`, `local-os-warm`, `local-os-cold`, and
  `local-reader-warm`, with honest semantics;
- two repetitions, independently prepared samples, paired
  reconstruction-versus-variant ratios, and separately reported rewrite bias;
- exact total/per-column bytes plus timing and writer guardrails; and
- one tiny offline end-to-end fixture followed by the existing production-sized
  Task 7 run.

The following remain outside this remediation:

- S3/MinIO/network performance and remote cache profiles;
- distributed cache behavior;
- page-level byte accounting not exposed by the pinned reader;
- new skip-index, encoding, type, compression, or query candidates;
- automatic production configuration changes;
- redesigning the product writer or compactor; and
- generating the 30M/10M Task 7 datasets.

## Terminal definition

Plan 49R is complete only when all of the following are true:

- the old Plan 47 command line and the Plan 49 subcommands both work;
- a variant's resolved diff is non-empty and exactly equals its declared delta;
- every timed entry comes from a versioned run-set entry and produces a report
  bound to that entry, artifact, scenario, cache preparation, host, and build;
- every local-cold timed sample has its own successful preparation immediately
  before the one timed access;
- L3 reports distinguish resident-memory decode from file-backed local reads;
- reconstruction and variant-delta artifacts run through L5 SQL;
- `close` rejects missing, malformed, stale, mismatched, duplicate, or modified
  reports;
- the analyzer derives bytes, noise, drift, writer vetoes, paired timings, and
  classifications from evidence rather than defaults;
- a tiny actual end-to-end experiment closes and analyzes successfully; and
- all refusal tests plus the old Plan 47/48 compatibility tests pass.

Passing unit tests without the tiny end-to-end experiment is not completion.

## Contracts fixed by this plan

### One scheduled entry is one independently prepared timed sample

Do not put `samples = 7` inside one cold benchmark process. The run-set planner
expands the frozen sample policy into individual entries. Each entry names:

```text
entry_id
repetition + order_index + sample_ordinal
artifact_role + artifact_manifest_digest
scenario_digest + layer + concrete tool arguments
cache_profile
expected cache-receipt path (when applicable)
expected report path
host digest + build digest
```

The executor receives `--run-set`, `--entry-id`, and an output root. It obtains
all benchmark arguments from the entry. Extra layer/scenario/artifact arguments
on the command line are refused, because they would create an undeclared run.

Warm and cold sample counts remain frozen from reconstruction-control pilots.
The planner expands exactly that count for both paired arms before any variant
result is accepted.

### Verification, preparation, and measurement are separate phases

For file-backed profiles the lifecycle is:

```text
artifact/scenario verification and work compilation
  -> cache preparation and residency receipt for this entry
  -> exactly one timed measurement with no full-file pre-read
  -> report validation and atomic publication
```

Verification is allowed to warm files because the cache controller runs after
it. Measurement validates manifest/receipt/preflight digests but must not read
complete data merely to validate it before starting the timer.

For `decode-resident`, the scan tool verifies and preloads immutable bytes
before the timer and records an embedded preparation proof. It is explicitly a
codec/decode CPU floor and never an I/O result.

For `local-reader-warm`, one run entry creates a reader/session, performs one
declared untimed warm-up, then records exactly one timed reuse. Every sample is
therefore independently reproducible; it is not a later element in an opaque
multi-sample loop.

### One canonical report envelope

Every L0–L5 report gains or is wrapped in a common
`ukiel-parquet-perf-report/v1` envelope containing:

- run-set digest and entry id;
- experiment, artifact, scenario, workload, cache-receipt, host, and build
  digests;
- artifact role, layer, cache profile, repetition, order, and sample ordinal;
- tool/report schema version;
- raw measurement body; and
- correctness/preflight digest.

The envelope does not normalize away layer-specific data. It gives `close` and
the analyzer one strict identity boundary while preserving each tool's raw
report.

Raw reports are immutable. Aggregation and classification happen only in the
analyzer.

## Global constraints

- Rust edition 2024, toolchain ≥ 1.96.
- Keep DataFusion 54, Arrow/Parquet 58.3, and object_store 0.13 pinned together.
- No executable package depends on another executable package or calls another
  command handler. Pure contract, cache, reader, or writer libraries may be
  shared.
- The shell executor may invoke installed single-purpose CLIs; it contains no
  writer, reader, SQL, cache, or analysis implementation.
- Product bytes and reconstruction controls are immutable. Never rewrite in
  place.
- Correctness, schema, fingerprint, expected-result, and physical-work gates
  run before cache preparation and timing.
- Output publication is temporary-path plus atomic rename; existing or partial
  destinations are refused.
- Unknown/missing metrics are `null`, never zero or fabricated.
- Size uses exact bytes. Five percent is a materiality threshold, not a claim of
  statistical noise.
- Conventional commits; no AI attribution.

---

### Task 1: Restore compatibility and make causal deltas exact

**Files:**

- Modify: `tools/parquet-rewrite/src/{main,config,reconstruct}.rs`
- Modify: `tools/parquet-lab-contract/src/experiment.rs`
- Modify: `tools/parquet-rewrite/tests/{boundary,properties,rewrite}.rs`
- Modify: `tools/parquet-lab-contract/tests/contracts.rs`
- Test: `bench/parquet-lab.sh`

Restore both supported Plan 47 forms:

```text
parquet-rewrite --manifest SNAPSHOT --spec SPEC --output DIR [--replace]
parquet-rewrite variant --manifest SNAPSHOT --spec SPEC --output DIR [--replace]
```

Keep `reconstruct` and `vary` unchanged. The legacy form must dispatch to the
same `variant` implementation, not a copied code path.

Harden delta resolution:

- `allowed_changes` and `changes` are non-empty, unique canonical paths;
- every changed path is known and every declared allowed path is present in
  `changes` for this v1 contract;
- per-column values have the exact expected type instead of becoming `None`;
- applying the delta and converting through `VariantSpec` preserves every
  changed path and value;
- the resolved reconstruction-to-child diff is non-empty and equals the delta
  key set exactly; and
- the parent digest and logical fingerprint still match.

A future conceptual variable that needs multiple physical fields may declare
multiple paths, but every declared path must actually change. Plan 49's ZSTD-6
delta remains one path.

- [ ] Failing test: the current flag-only Plan 47 command is rejected.
- [ ] Failing tests: wrongly typed `columns.x.dictionary`, encoding,
  compression, Bloom and physical-type values are refused.
- [ ] Failing tests: empty, no-op, broader-than-change, narrower-than-change,
  unknown, and second-axis deltas are refused.
- [ ] Positive tests: Plan 47 legacy and explicit subcommand outputs agree;
  ZSTD-1 → ZSTD-6 has exactly one resolved diff.
- [ ] Verify:

```bash
cargo test -p parquet-lab-contract -p parquet-rewrite
bash bench/tests/parquet-lab.sh
cargo clippy -p parquet-lab-contract -p parquet-rewrite --all-targets -- -D warnings
```

- [ ] Commit: `bench: harden parquet causal contracts`

### Task 2: Add per-entry preflight and cache-preparation contracts

**Files:**

- Modify: `tools/parquet-lab-contract/src/{cache,report,run_set,scenario}.rs`
- Create: `tools/parquet-lab-contract/src/preflight.rs`
- Modify: `tools/parquet-lab-contract/src/lib.rs`
- Modify: `tools/parquet-cachectl/src/{main,lib,linux}.rs`
- Modify: `tools/parquet-cachectl/tests/{profiles,boundary}.rs`
- Modify: `tools/file-read-bench/src/{main,lib}.rs`
- Modify: `tools/file-read-bench/tests/{read,boundary}.rs`

Add a versioned preflight contract binding the verified artifact files, concrete
scenario work, expected correctness digest, and run-set entry. Preflights are
immutable and are consumed by exactly one measurement entry.

Extend cache receipts with the run-set/entry/scenario identity and a preparation
nonce. A receipt proves the state immediately after preparation for one entry;
it is not reusable by a second sample or another artifact arm. Duplicate receipt
use is rejected by `close`.

Split `file-read-bench` into explicit CLI phases while keeping its pure range
builder and reader:

```text
file-read-bench verify  --entry ENTRY.json --preflight OUT.json
file-read-bench measure --entry ENTRY.json --preflight P.json \
  --receipt CACHE.json --report OUT.json
```

`verify` checks complete file digests and compiles the range plan. The executor
then runs `parquet-cachectl prepare`. `measure` checks only identities and cheap
immutable metadata before the timer, opens the declared files inside the timed
operation, performs one range plan, and emits one sample.

The cache controller may read files while verifying them, but preparation must
be the final data-touching step before measurement. Cold eviction closes all
descriptors before probing. Warm preparation reads every registered target
range. Unsupported or ineffective states remain unavailable and non-zero.

- [ ] Failing test: full-file validation after a cold receipt changes residency
  and is refused as a stale lifecycle.
- [ ] Failing test: one receipt cannot bind two sample entries.
- [ ] Failing tests: receipt artifact, scenario, entry, target-file, nonce, and
  profile mismatches are refused.
- [ ] Linux integration test: three cold entries each independently reach the
  cold ceiling before their timed read; no test treats sample 2/3 as cold merely
  because sample 1 was cold.
- [ ] Warm/cold tests explicitly skip only with an unsupported/unavailable
  receipt; they never silently pass as another profile.
- [ ] Verify:

```bash
cargo test -p parquet-lab-contract -p parquet-cachectl -p file-read-bench
cargo clippy -p parquet-lab-contract -p parquet-cachectl -p file-read-bench --all-targets -- -D warnings
```

- [ ] Commit: `bench: prepare every parquet io sample independently`

### Task 3: Separate resident decode from file-backed Parquet scan

**Files:**

- Modify: `tools/parquet-scan-bench/src/{main,lib,measure,selection,sink}.rs`
- Create: `tools/parquet-scan-bench/src/preflight.rs`
- Modify: `tools/parquet-scan-bench/tests/{projection,selection,equivalence,boundary}.rs`

Add explicit verify/measure phases analogous to L2. Verification reads and
hashes the files, validates logical equivalence, resolves projection roles,
compiles global row-group ordinals into per-file ordinals, and writes a bound
preflight.

Implement two honest backends:

- `decode-resident`: load verified immutable file bytes before timing and decode
  from `Bytes`; record zero filesystem-read claims and an embedded preload proof;
- `local-file`: open the Parquet files through a file-backed reader in the timed
  operation, so local-cold includes first-touch metadata + selected data + decode
  and local-warm measures the same path with resident file pages.

Do not accept a local cache receipt while silently selecting the resident-memory
backend. Report requested compressed bytes, returned bytes when observable,
rows/values/batches/files/row groups, and `null` for unavailable page metrics.

Each entry measures exactly one selection once. The checksum and expected row
count from preflight must match before the report is published.

- [ ] Failing test: local-cold cannot use `Bytes` loaded before cache
  preparation.
- [ ] Failing test: resident and file-backed reports cannot share the same
  backend/profile identity.
- [ ] Golden tests cover every projection/selection, nulls, strings, cross-file
  ordinals, and zero-row-group metadata control on both applicable backends.
- [ ] Equivalence test: reconstruction and ZSTD-6 decode the same rows/checksum
  for the same ordinal plan.
- [ ] Linux test proves the cold file-backed sample starts below the ceiling and
  the warm sample above the floor; the resident sample makes no filesystem-I/O
  claim.
- [ ] Verify:

```bash
cargo test -p parquet-scan-bench -p parquet-cachectl -p parquet-lab-contract
cargo clippy -p parquet-scan-bench -p parquet-cachectl -p parquet-lab-contract --all-targets -- -D warnings
```

- [ ] Commit: `bench: separate resident and file backed parquet scans`

### Task 4: Make the SQL guard consume causal artifacts one scenario at a time

**Files:**

- Modify: `tools/parquet-lab-bench/src/{main,lib,runner,compare,plan_guard}.rs`
- Modify: `tools/parquet-lab-bench/tests/{runner,equivalence,result_semantics}.rs`
- Modify: `tools/parquet-lab-bench/README.md`
- Modify: `tools/parquet-lab-contract/src/{report,suite}.rs`

Teach the read-only artifact loader to accept:

- product snapshots;
- old Plan 47 variants for reproduction;
- Plan 49 reconstruction controls; and
- Plan 49 variant-delta artifacts.

Every kind resolves to immutable file paths plus an explicit artifact role and
digest. A delta must be checked against its reconstruction parent before SQL
timing is allowed.

Add a one-query execution mode selected from the compiled seven-query suite.
The run-set scenario names the query, required columns, result sink, predicate
shape, reader reuse mode, and cache profile. Do not execute and pool all seven
queries under one schedule entry.

The SQL lifecycle is:

1. verify artifact/suite/scenario and expected result digest;
2. build and assert the physical plan before cache preparation;
3. prepare the declared OS profile for this entry;
4. create either a fresh session, or create + untimed-warm a session for the
   `local-reader-warm` scenario; and
5. time exactly one selected query and publish one bound sample.

The physical plan and exact result digest are retained in the report. Plan
capture for metrics after timing may execute only if it is clearly recorded as
untimed and cannot mutate the evidence attributed to the timed sample.

- [ ] Failing tests: reconstruction and delta manifests currently fail artifact
  loading.
- [ ] Failing test: one cold receipt cannot label multiple queries or
  iterations.
- [ ] Tests prove every query class scans its required columns and cannot use a
  metadata-only `count(*)` substitute.
- [ ] Product/reconstruction/variant results agree under the declared semantics;
  a changed answer or dropped projection fails before publication.
- [ ] Fresh-session and reader-warm samples have distinct, accurate identities.
- [ ] Old Plan 47/48 CLI and report readers remain reproducible.
- [ ] Verify:

```bash
cargo test -p parquet-lab-bench -p parquet-lab-contract
cargo clippy -p parquet-lab-bench -p parquet-lab-contract --all-targets -- -D warnings
```

- [ ] Commit: `bench: bind causal parquet artifacts in sql`

### Task 5: Make the run set executable and the analyzer evidence-driven

**Files:**

- Modify: `bench/parquet-perf-run-set.py`
- Modify: `bench/parquet-perf.sh`
- Modify: `bench/parquet-perf-analyze.py`
- Modify: `bench/tests/{test_parquet_perf_run_set.py,test_parquet_perf_analyze.py,parquet-perf.sh}`
- Modify: `bench/README.md`

The planner consumes the experiment and versioned scenario manifests, freezes
sample counts from reconstruction pilots, and expands them into fully executable
entries. Every product/reconstruction bracket is a concrete scenario entry;
there are no generic controls with `scenario_id = null`.

The executor accepts exactly one declared entry:

```text
bench/parquet-perf.sh --run-set PLANNED.json --entry-id ID --out-root DIR
```

It verifies the run-set digest and every declared input, runs verification,
prepares the entry's cache state, invokes exactly one measurement, validates the
report envelope, and atomically publishes the expected output. It refuses
unknown entries, alternate artifacts/options, dirty or unknown builds in
publishable mode, existing/partial outputs, and PATH tools whose version/build
identity does not match the run set.

`close` reparses every report and verifies:

- report digest equals the actual bytes and the planned binding;
- all envelope digests and entry coordinates match;
- every planned entry appears once and no unplanned report is present;
- every required receipt is valid, unique, and bound to that entry;
- host/build identities are complete and consistent; and
- artifact, scenario, correctness, and result digests agree across paired arms.

Modifying a report after `close` must make analysis fail because the analyzer
rechecks the recorded digest.

The analyzer reads actual evidence:

- product/reconstruction/variant censuses for exact total and per-column bytes;
- reconstruction/variant writer reports for the registered writer guardrail;
- opening/closing product and reconstruction controls for drift/noise;
- paired one-sample entries grouped only within the same scenario and
  repetition;
- raw L2/L3 samples and the selected L5 query sample shape; and
- cache profile, workload, backend, host, and build identities.

It reports product→reconstruction rewrite bias separately. For the causal
reconstruction→variant comparison it reports exact byte deltas, median/MAD,
p95 only at ≥20 samples, per-repetition paired ratios, drift, and the larger of
5% or registered control noise. It then emits only the fixed classifications:
`dominated`, `no-demonstrated-change`, `unstable`, `pareto-candidate`,
`workload-specific`, `cache-specific`, or `storage-only`.

No classifier input may default to a value that can improve a candidate's
verdict. Missing required evidence makes the scenario invalid.

- [ ] Refusal tests cover generic/null controls, missing layer/options, duplicate
  sample ordinal, unlike pairing, variant-first sizing, stale/reused receipt,
  report-content mismatch, modified-after-close report, wrong artifact/scenario,
  unknown tool/build/host, incomplete run, unplanned report, and partial output.
- [ ] End-to-end analyzer fixtures use the real L1/L2/L3/L5 report shapes—not
  hand-written `{"samples": ...}` stubs.
- [ ] Fixtures derive rewrite bias, per-column storage-only wins, control noise,
  opposite repetition direction, writer veto, cache-only/workload-only wins,
  and a true Pareto result.
- [ ] SQL timings are parsed from their actual selected-query report body.
- [ ] Verify:

```bash
bash -n bench/parquet-perf.sh
python3 -m unittest bench/tests/test_parquet_perf_run_set.py bench/tests/test_parquet_perf_analyze.py
bash bench/tests/parquet-perf.sh
```

- [ ] Commit: `bench: execute and validate declared parquet run sets`

### Task 6: Prove the repaired framework with a tiny actual experiment

**Files:**

- Create: `bench/fixtures/parquet-perf/README.md`
- Create: `bench/tests/parquet-perf-e2e.sh`
- Modify: `bench/README.md`
- Modify: `docs/notes/2026-07-16-ukiel-parquet-framework-baseline.md`
- Modify: `docs/superpowers/plans/2026-07-16-ukiel-parquet-performance-framework-foundation.md`
- Modify: `docs/superpowers/plans/2026-07-05-ukiel-v1-roadmap.md`

Build the fixture at test time from deterministic source rows; do not commit
generated Parquet or benchmark outputs. It needs at least two files, multiple row
groups, nulls, fixed-width values, high-cardinality strings, wide text, and a
predicate that selects no rows.

The test must execute the real installed/debug binaries through the real shell
executor:

1. freeze a product snapshot;
2. reconstruct it under ZSTD-1;
3. vary exactly compression to ZSTD-6;
4. census all three artifacts;
5. compile the workload/scenario/SQL evidence;
6. create reconstruction-only pilots and freeze the run set;
7. execute all declared L1/L2/L3/L5 entries across two repetitions, including
   independently prepared warm/cold samples;
8. close the run set;
9. analyze it; and
10. assert logical/result equivalence, separate rewrite bias, exact byte inputs,
    populated timing inputs, and a classification derived from those inputs.

The same test then tampers one report and proves re-analysis fails. A second
compatibility leg executes one old Plan 47 variant through the flag-only CLI and
the historical runner.

This fixture proves plumbing and integrity, not that ZSTD-6 wins; tiny timings
must not be promoted as performance evidence.

- [ ] `bench/tests/parquet-perf-e2e.sh` uses real tools and does not skip when
  they are available.
- [ ] Linux cache tests either prove the thresholds or return a named unavailable
  result; the e2e test never relabels unavailable as cold/warm.
- [ ] Run the complete focused verification:

```bash
cargo test -p parquet-lab-contract -p parquet-rewrite -p parquet-lab-write-core \
  -p parquet-write-bench -p parquet-cachectl -p file-read-bench \
  -p parquet-scan-bench -p parquet-lab-bench
python3 -m unittest bench/tests/test_parquet_perf_run_set.py bench/tests/test_parquet_perf_analyze.py
bash bench/tests/parquet-lab.sh
bash bench/tests/parquet-perf.sh
bash bench/tests/parquet-perf-e2e.sh
cargo clippy -p parquet-lab-contract -p parquet-rewrite -p parquet-lab-write-core \
  -p parquet-write-bench -p parquet-cachectl -p file-read-bench \
  -p parquet-scan-bench -p parquet-lab-bench --all-targets -- -D warnings
```

- [ ] Update Plan 49 and the baseline note with exact test counts and the
  repaired tiny-e2e evidence. Do not restore “validated” language before this
  task passes.
- [ ] Mark Plan 49 Task 7 unblocked, not executed.
- [ ] Commit: `bench: prove parquet framework end to end`

### Task 7: Resume the original Plan 49 confirmation

After Tasks 1–6 and only after the PoC v0 feature-wide gate, return to Plan 49
Task 7. Do not duplicate its dataset-generation or evidence instructions here.

Before starting the large run:

- [ ] both product datasets are frozen and verified;
- [ ] every production scenario resolves into concrete run-set entries;
- [ ] the publishable host/build manifests are clean and complete;
- [ ] one cold and one warm smoke entry for each layer closes successfully; and
- [ ] the expected runtime and disk footprint are recorded.

If either dataset is unavailable, stop with Task 7 pending. Do not substitute a
different scale or workload and call it the registered confirmation.

The production decision remains evidence-driven: ZSTD-6 may be rejected,
workload-specific, storage-only, or Pareto. No outcome changes
`ukiel_core::writer_props` inside Plan 49/49R.

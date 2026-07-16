# Ukiel Plan 49: Parquet Performance Framework Foundation

> **For agentic workers:** Execute this plan task by task. Keep every executable
> single-purpose, run each task's focused tests before its commit, and do not
> turn a benchmark result into a product writer change inside this plan.

**Status:** Framework built and validated 2026-07-16 (Tasks 1–6, 107 passing tests). Task 7
production confirmation (30M prod-synth + 10M ClickBench) pending dataset generation; see
`docs/notes/2026-07-16-ukiel-parquet-framework-baseline.md`.

**Goal:** Implement the smallest trustworthy local vertical slice of
`docs/superpowers/specs/2026-07-16-ukiel-parquet-performance-framework.md`, then
use it to confirm or reject Plan 48's only survivor: ZSTD level 6. The bounded
experiment compares exact Ukiel product bytes, a reconstruction control, and a
one-variable ZSTD-6 child on the existing 30M prod-synth event shape and the
bounded 10M ClickBench OLAP shape.

This plan builds a reusable measurement kernel. It does **not** implement every
cache/backend/index layer in the framework and it does **not** change Ukiel's
production writer defaults.

## Why this is next

Plan 48 found one directional candidate: ZSTD-6 was about 12% smaller than the
packed prod-synth control without a measured read regression. That result is
useful but not yet a production decision:

- the product control was compared directly with rewritten variants, so rewrite
  bias was not measured independently;
- writer CPU/throughput was not measured;
- raw filesystem I/O, Parquet decode, and SQL execution were blended;
- “cold” meant a fresh DataFusion session, not verified OS-cache eviction; and
- the result has not crossed from the event shape to ClickBench.

Running another ClickBench suite with those same ambiguities would produce one
more number without explaining it. ZSTD-6 is instead the acceptance workload
for the new framework: it is a real candidate, it changes exactly one physical
axis, and compression should visibly separate bytes, I/O, decode CPU, SQL, and
writer cost.

Plan 49 is independent of refreshed Plan 8. It may run alongside the V1 product
track and must not become a gate for pipelines.

## Exact experiment boundary

There are exactly two dataset artifacts:

1. the Plan-48-shaped 30M, packed, converged L1+ prod-synth artifact; and
2. the deterministic 10M ClickBench slice already specified by Plan 47.

Each dataset has exactly three measured artifact roles:

| role | bytes | purpose |
|---|---|---|
| product control | exact frozen Ukiel output; never rewritten | ecological reference: what ships today |
| reconstruction control | same logical rows rewritten with the resolved product L1+ policy, including ZSTD-1 | quantifies rewrite/library/default/row-group bias |
| `compression-zstd-6` | child of the reconstruction control changing only compression level | causal candidate |

That is six artifacts total. Do not add LZ4, type, page, dictionary, Bloom, or
sidecar arms. The old Plan 47/48 artifacts and reports remain readable; this
plan introduces new contracts instead of reinterpreting old evidence.

## Primary scorecard and scenario pack

Record these dimensions separately; never combine them into one score:

1. exact total and per-column bytes;
2. writer wall/CPU time, rows/s, logical MiB/s, output bytes, and peak RSS;
3. raw byte-read wall/CPU time and MiB/s;
4. direct Parquet scan/decode time; and
5. SQL time for the bounded query pack.

The direct scan pack binds generic roles in each dataset manifest:

| projection | row-group access plans |
|---|---|
| one fixed-width key | all, one, 10% contiguous, 10% sparse |
| one high-cardinality string | all, one, 10% contiguous, 10% sparse |
| one wide text/string column | all, one, 10% contiguous, 10% sparse |
| registered hot set | all, one, 10% contiguous, 10% sparse |
| all columns | all, one, 10% contiguous, 10% sparse |

The SQL guard pack is deliberately small: full numeric scan, full string scan,
hot-set scan, all-column scan, aligned selective/narrow, scattered selective,
and absent predicate. Every query consumes its projection through an aggregate
or benchmark checksum sink and carries a physical-plan assertion. `count(*)`
may be retained only as a labelled metadata-path negative control.

Run discovery under `decode-resident`, `local-os-warm`, and
`local-reader-warm`. Because ZSTD-6 already survived Plan 48, also run the final
three-arm comparison under verified `local-os-cold`. Remote/object-cache modes
are outside this plan; no remote-I/O claim may appear in the result.

## Contract and tool boundaries

All tools exchange versioned files from `parquet-lab-contract`. No executable
package depends on another executable package or calls another command handler.
Pure libraries may be shared.

| component | one responsibility |
|---|---|
| `parquet-lab-contract` | artifact roles, reconstruction/variant deltas, workload/scenario/cache/run-set/report contracts |
| `parquet-lab-write-core` | pure Arrow-to-Parquet implementation shared by rewrite and writer timing |
| `parquet-rewrite` | materialize one verified reconstruction or one resolved child variant |
| `parquet-census` | L0 physical/per-column accounting |
| `parquet-write-bench` | L1 writer timing only; no census or publication in the timed interval |
| `file-read-bench` | L2 byte/range reads to a checksum sink; never parses Parquet |
| `parquet-scan-bench` | L3 direct Arrow/Parquet decode and explicit row-group selection; no DataFusion |
| `parquet-cachectl` | prepare and verify one local cache profile; performs no benchmark timing |
| `parquet-lab-bench` | bounded L5 SQL guard; no longer calls a fresh session “OS cold” |
| `bench/parquet-perf-run-set.py` | plan/close the paired seeded schedule; starts no benchmark |
| `bench/parquet-perf.sh` | execute one declared schedule entry; contains no writer/reader/query logic |
| `bench/parquet-perf-analyze.py` | validate reports and calculate paired ratios/Pareto classifications |

## Global constraints

- Rust edition 2024 and the workspace toolchain; keep Arrow/Parquet 58.3 and
  DataFusion 54 pinned.
- Release binaries for timing. Record target CPU/RUSTFLAGS and dirty state.
- Setup, input validation, cache preparation, fingerprinting, census, output
  verification, and report writing stay outside timed intervals.
- Exact logical fingerprint, schema, file membership, file row counts, result
  digests, and requested physical property gates run before timing.
- Product bytes are immutable. A command that would write into their directory
  must fail before opening an output file.
- A variant is a delta against the reconstruction control. A full replacement
  writer configuration is rejected at this boundary.
- Sample counts are determined from controls before variant results are read.
  Warm counts are `max(7, ceil(3 seconds / control median))`, capped by a
  pre-registered maximum; cold uses at least three independently prepared
  samples. Every paired arm uses the same count.
- Two independently shuffled seeded repetitions; product and reconstruction
  controls bracket each repetition.
- Conventional commits; no AI attribution.

---

### Task 1: Dual-control, delta, scenario, and cache contracts

**Files:**

- Modify: `tools/parquet-lab-contract/src/{lib,variant,run_set,report}.rs`
- Create: `tools/parquet-lab-contract/src/{experiment,scenario,cache}.rs`
- Modify: `tools/parquet-lab-contract/tests/{contracts,boundary}.rs`
- Modify: `tools/parquet-rewrite/src/{main,lib,spec}.rs`
- Test: `tools/parquet-rewrite/tests/{boundary,properties,rewrite}.rs`

Add versioned contracts for:

- `ukiel-parquet-experiment/v1`: dataset/workload bindings plus the digests of
  product, reconstruction, variant delta, scenarios, and planned run set;
- `ukiel-parquet-reconstruction/v1`: parent product snapshot, full requested
  and resolved writer properties, logical/file fingerprints, and rewrite-bias
  identity;
- `ukiel-parquet-variant-delta/v1`: reconstruction parent digest,
  `allowed_changes`, and only the changed values;
- `ukiel-parquet-scenario/v1`: layer, projection roles, row-group selection,
  predicate/query, result sink, backend, cache profile, and sample policy; and
- `ukiel-parquet-cache-receipt/v1`: target manifest/files, requested profile,
  preparation method, residency before/after, thresholds, and validity.

Keep the existing v1 snapshot/variant/suite/report readers for historical Plan
47/48 evidence. New run sets must reject mixing old untyped reports with the
new experiment.

Add explicit rewrite modes:

```text
parquet-rewrite reconstruct \
  --manifest PRODUCT.json --baseline BASELINE.toml --output DIR

parquet-rewrite vary \
  --manifest RECONSTRUCTION.json --delta DELTA.toml --output DIR
```

The resolver writes a complete resolved configuration, then structurally diffs
the reconstruction and child manifests. It refuses any changed physical field
outside `allowed_changes`; the census later proves the requested observable
change. ZSTD level is not footer-observable, so the requested/resolved manifest
is authoritative for the level while the footer proves codec family.

- [ ] Failing tests: product cannot be rewritten in place; reconstruction binds
  exact parent; a ZSTD-6 delta changes only compression level; a delta that also
  changes row-group size is refused; unknown allowlist paths fail closed.
- [ ] Implement contracts, parsing, canonical digests, resolver, and both CLI
  modes atomically.
- [ ] Prove old Plan-48 manifests remain parseable by their old reader and are
  never silently promoted to the new causal contract.
- [ ] Verify and commit:

```bash
cargo test -p parquet-lab-contract -p parquet-rewrite
cargo clippy -p parquet-lab-contract -p parquet-rewrite --all-targets -- -D warnings
git commit -m "bench: add causal parquet experiment contracts"
```

### Task 2: Shared writer core and isolated writer benchmark

**Files:**

- Create: `tools/parquet-lab-write-core/{Cargo.toml,src/lib.rs}`
- Move/refactor: pure write/property-resolution code from
  `tools/parquet-rewrite/src/{rewrite,types}.rs`
- Create: `tools/parquet-write-bench/{Cargo.toml,src/{main,lib,measure}.rs}`
- Create: `tools/parquet-write-bench/tests/{timing,boundary}.rs`
- Modify: root `Cargo.toml`

`parquet-lab-write-core` accepts logical input batches plus resolved properties
and writes a supplied sink. It knows nothing about CLI paths, timing, reports,
or experiment selection. `parquet-rewrite` retains ownership of validation,
fingerprinting, manifest publication, and atomic directories.

`parquet-write-bench` reads one frozen logical artifact and resolved writer
configuration. For each sample it prepares input before the clock, writes to a
fresh disposable output during the clock, closes the writer, stops the clock,
then validates fingerprint/footer/output and removes the sample outside the
clock. Record wall/user/system CPU, rows, logical bytes, output bytes, rows/s,
MiB/s, and peak RSS where supported.

The product control has no invented writer time: L1 compares reconstruction
ZSTD-1 with its ZSTD-6 child. Product bytes participate in L0/L2/L3/L5 only.

- [ ] Characterization tests prove moving writer code changes no bytes or
  resolved manifests for existing fixtures.
- [ ] A delay in validation/report writing does not change the measured writer
  interval; a delay in the writer does.
- [ ] Boundary tests keep DataFusion, catalog, object-store, and Ukiel service
  crates out of both offline packages.
- [ ] Verify standalone installation and commit:

```bash
cargo test -p parquet-lab-write-core -p parquet-rewrite -p parquet-write-bench
cargo install --path tools/parquet-write-bench --root /tmp/parquet-write-bench-install
cargo clippy -p parquet-lab-write-core -p parquet-rewrite -p parquet-write-bench --all-targets -- -D warnings
git commit -m "bench: isolate parquet writer throughput"
```

### Task 3: Verified local cache controller and raw byte reader

**Files:**

- Create: `tools/parquet-cachectl/{Cargo.toml,src/{main,lib,linux}.rs}`
- Create: `tools/parquet-cachectl/tests/{profiles,boundary}.rs`
- Create: `tools/file-read-bench/{Cargo.toml,src/{main,lib,ranges}.rs}`
- Create: `tools/file-read-bench/tests/{read,ranges,boundary}.rs`
- Modify: root `Cargo.toml`

`parquet-cachectl` supports only explicit benchmark files and these local
profiles:

- `decode-resident`: materialize immutable bytes into a declared memory
  artifact;
- `local-os-warm`: read every registered target range and verify at least the
  configured residency floor (90% default);
- `local-os-cold`: close descriptors, call per-file
  `posix_fadvise(POSIX_FADV_DONTNEED)`, then verify at most the configured
  residency ceiling (10% default); and
- `local-reader-warm`: verify OS residency; session reuse remains the query or
  scan tool's responsibility.

Use `mincore` or an equivalent page-residency probe. Unsupported kernels,
filesystems, ineffective eviction, or failed thresholds produce an unavailable
receipt and a non-zero exit; they never relabel the sample. Do not use global
`drop_caches`.

`file-read-bench` reads immutable bytes into a BLAKE3/checksum sink without a
Parquet dependency. It supports all files, one complete file, registered
contiguous ranges, and registered sparse ranges with equal returned-byte
targets. Record requested/returned bytes, ranges, reads/syscalls, wall/CPU time,
MiB/s, and the bound cache-receipt digest.

- [ ] Cache tests cover warm, effective cold, ineffective-cold refusal, receipt
  binding, and “other process files are untouched.” Platform-specific tests may
  skip only with an explicit unsupported result.
- [ ] Reader tests prove range coverage, equal-byte contiguous/sparse plans,
  checksum stability, and that setup/reporting are outside timing.
- [ ] Boundary tests prove neither tool parses Parquet or invokes another
  executable.
- [ ] Verify and commit:

```bash
cargo test -p parquet-cachectl -p file-read-bench
cargo clippy -p parquet-cachectl -p file-read-bench --all-targets -- -D warnings
git commit -m "bench: verify cache state and raw parquet io"
```

### Task 4: Direct Parquet scan/decode benchmark

**Files:**

- Create: `tools/parquet-scan-bench/{Cargo.toml,src/{main,lib,projection,selection,sink}.rs}`
- Create: `tools/parquet-scan-bench/tests/{projection,selection,equivalence,boundary}.rs`
- Modify: root `Cargo.toml`

Read through Arrow/Parquet 58.3 directly. Do not create a DataFusion session or
SQL/logical plan. Bind projection roles from the workload manifest and compile
global row-group ordinals deterministically into all/one/10%-contiguous/10%-
sparse access plans. The causal reconstruction/variant pair must select the
same files, row-group ordinals, and expected rows; product-vs-reconstruction is
reported as rewrite bias and is not forced to share physical boundaries.

Decoded arrays feed a typed deterministic checksum/count sink. Record rows and
values decoded, compressed/requested/returned bytes where observable, files,
row groups and pages opened where observable, batches, wall/CPU time, rows/s,
values/s, MiB/s, peak RSS, artifact/scenario/cache digests, and missing metrics
as `null`.

- [ ] Golden tests cover every core projection, all four selection modes, zero
  selected groups as an untimed metadata negative control, nulls, strings, and
  cross-file ordinals.
- [ ] Equivalence tests prove control/variant checksums and row counts agree and
  that an intentionally changed row is rejected before timing.
- [ ] Boundary test proves no DataFusion or Ukiel service dependency.
- [ ] Verify and commit:

```bash
cargo test -p parquet-scan-bench
cargo install --path tools/parquet-scan-bench --root /tmp/parquet-scan-bench-install
cargo clippy -p parquet-scan-bench --all-targets -- -D warnings
git commit -m "bench: measure direct parquet scan and decode"
```

### Task 5: Make the SQL guard explicit about work and cache state

**Files:**

- Modify: `tools/parquet-lab-contract/src/suite.rs`
- Modify: `tools/parquet-lab-bench/src/{main,lib,runner,compare}.rs`
- Modify: `tools/parquet-lab-bench/src/suites/{prod_synth,clickbench}.rs`
- Modify: `tools/parquet-lab-bench/tests/{runner,equivalence,result_semantics}.rs`
- Modify: `tools/parquet-lab-bench/README.md`

Add the seven registered query classes from this plan. Each declares required
columns, result sink/digest, predicate shape, and physical-plan assertions.
Reject a full-scan scenario if the optimized plan answers from metadata, drops a
required projection, or returns the full result set when the scenario declares
an aggregate/checksum sink.

Replace the ambiguous `cold_iters`/`warm_iters` report vocabulary:

- fresh DataFusion session + verified OS profile;
- reused DataFusion session + verified OS profile; and
- explicit cache-receipt digest and residency.

Keep old CLI/report parsing for Plan 47/48 reproduction, but new Plan-49 reports
must use scenario/cache contracts. This task does not rename the binary and does
not add remote storage.

- [ ] Tests prove `count(*)` cannot satisfy full numeric/string/all-column
  scans; physical-plan assertion failure rejects timing.
- [ ] Fresh-session and reused-session tests name reader reuse honestly and do
  not claim OS cold without a valid receipt.
- [ ] Exact result digest remains a pre-timing gate for every artifact.
- [ ] Verify and commit:

```bash
cargo test -p parquet-lab-bench -p parquet-lab-contract
cargo clippy -p parquet-lab-bench -p parquet-lab-contract --all-targets -- -D warnings
git commit -m "bench: bind parquet sql work and cache profiles"
```

### Task 6: Paired run-set orchestration and analysis

**Files:**

- Create: `bench/parquet-perf-run-set.py`
- Create: `bench/parquet-perf.sh`
- Create: `bench/parquet-perf-analyze.py`
- Create: `bench/tests/{test_parquet_perf_run_set.py,test_parquet_perf_analyze.py,parquet-perf.sh}`
- Modify: `bench/README.md`

The planner emits two seeded repetitions. Each repetition brackets variants
with product and reconstruction controls and pairs every ZSTD-6 scenario with
its reconstruction scenario. Sample counts come only from a control pilot and
are frozen into the plan before variant reports are accepted.

The executor runs one declared entry and verifies every input/output digest. It
may invoke the single-purpose tools but contains no benchmark implementation.
It publishes through a temporary directory plus atomic rename and refuses an
existing or partially complete destination.

The analyzer:

- validates complete artifact/scenario/cache/host/build bindings;
- reports product-vs-reconstruction rewrite bias separately;
- computes reconstruction-vs-ZSTD-6 paired ratios per repetition;
- reports exact total/per-column byte deltas, median/MAD, meaningful p95, drift,
  and the larger of 5% or registered control noise;
- classifies each scenario without pooling different queries/projections; and
- emits `dominated`, `no-demonstrated-change`, `unstable`, `pareto-candidate`,
  `workload-specific`, `cache-specific`, or `storage-only`.

- [ ] Refusal tests cover missing brackets, duplicate entries, unlike scenario
  pairing, variant-first sample sizing, incomplete cache receipt, unknown host,
  dirty/unidentified build in publishable mode, and partial output.
- [ ] Synthetic analysis fixtures cover rewrite bias, opposite repetition
  directions, a small exact column win, writer regression, cache-only win, and
  a true Pareto result.
- [ ] `bash -n`, Python unit tests, and a tiny end-to-end offline fixture pass.
- [ ] Commit:

```bash
bash -n bench/parquet-perf.sh
python3 -m unittest bench/tests/test_parquet_perf_run_set.py bench/tests/test_parquet_perf_analyze.py
bash bench/tests/parquet-perf.sh
git commit -m "bench: orchestrate paired parquet performance runs"
```

### Task 7: Execute the six-artifact confirmation and close the decision

**Files:**

- Create: `bench/config/parquet-perf/{prod-synth,clickbench}.toml`
- Create: `bench/config/parquet-perf/compression-zstd-6.toml`
- Create: `docs/notes/2026-07-XX-ukiel-parquet-framework-baseline.md`
- Modify: `docs/superpowers/plans/2026-07-05-ukiel-v1-roadmap.md`

Prepare/freeze both product controls, materialize their reconstruction controls,
then materialize only the ZSTD-6 delta. Run L0–L3 and the SQL guard under the
registered profiles and two repetitions. The evidence directory is:

```text
bench/results/parquet-perf/plan49/
  experiment.json
  datasets/{prod-synth,clickbench}/
  artifacts/{product,reconstruction,zstd-6}/
  run-set-planned.json
  run-set-complete.json
  samples/
  censuses/
  sha256sums.txt
  analysis.json
  analysis.md
```

The result note must answer, per workload and layer:

1. How large is rewrite bias?
2. Are total and material per-column savings outside 5%?
3. What writer CPU/throughput/RSS cost does ZSTD-6 impose?
4. Does raw-byte time change in proportion to bytes under verified cold/warm?
5. Does decode CPU regress for one/hot/all projections?
6. Do SQL full-scan and selective classes agree with the lower layers?
7. Is ZSTD-6 a general, workload-specific, cache-specific, or rejected result?

If it survives both workloads and the registered writer guardrail, create a
focused issue for a production L1+ codec-level change with rollout, compatibility,
and rollback gates. If it fails, record the negative result and stop. Do not
change `ukiel_core::writer_props` in this plan.

- [ ] All six artifact manifests and every raw report are digest-bound.
- [ ] Both repetitions agree or the result is `unstable`; no third repetition
  is added after seeing the answer unless the pre-registered policy requires it.
- [ ] Result note and roadmap state exactly what is and is not earned.
- [ ] Verify the framework's focused tests and commit evidence/docs separately
  from any future product change.

## Final acceptance

Plan 49 is complete when:

- the product/reconstruction distinction is enforced by contracts and visible
  in reports;
- writer, raw I/O, direct decode, SQL, and cache preparation are distinct timed
  boundaries;
- cache claims are backed by verified residency receipts;
- one/hot/all projections and all/one/contiguous/sparse row-group access work on
  standalone local Parquet artifacts;
- the exact six-artifact experiment is reproducible and fully bound;
- ZSTD-6 receives a cross-workload, per-layer verdict; and
- no production storage setting changed as a side effect.

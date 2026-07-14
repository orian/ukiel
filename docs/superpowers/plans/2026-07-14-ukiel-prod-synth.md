# Ukiel Plan 45: Production-Shaped Synthetic Event Workload

> **For agentic workers:** Execute this plan task-by-task. Use checkbox
> (`- [ ]`) progress, run each task's focused tests before its commit, and do not
> cross tool boundaries: the generator, loader, and benchmark runner are separate
> executables joined only by a versioned manifest.

**Status:** **Executed** (2026-07-14). All six tasks landed. Results and measured
numbers: `docs/notes/2026-07-14-prod-synth-baseline.md`. Runbooks:
`tools/prod-synth/README.md`, `tools/ukiel-prod-load/README.md`,
`tools/ukiel-prod-bench/README.md`.

## Deviations from this plan, and why

Four things in the plan below turned out to be wrong or unbuildable. They are recorded
here rather than quietly fixed, because each one is a fact about the source data or the
problem, not a matter of taste.

**1. The topology algorithm (step 5) cannot run as specified.** It bounds the tenant
degree repair to `1..part_count` and to matching the margins. That is necessary and not
sufficient: 7 source parts hold exactly one key, so they scale to capacity 1, while the
resample yields ~37 tenants needing two or more of those 7 slots. The degree sequence is
**not bigraphic** and Havel-Hakimi fails outright, whatever the sums say. The repair now
bounds on **Gale-Ryser feasibility**. It costs 1.1% of tenants at baseline — inside the
plan's own 2% ceiling — all in the extreme tail, and it moves neither gated quantile.

**2. The `range_overfetch p90` saturation gate is unsatisfiable as written.** It asks for
"p90 in the same saturation bucket (at least 90% of source part count)" — 61.2 of 68. The
source's own two capture files disagree: `tenant-fanout.jsonl` reports up to 63 range
candidates, but `part-geometry.jsonl` holds only **61** parts with a key range wider than a
point, and a single-key part is a range candidate for exactly one tenant. No graph over 61
multi-key parts can put a tenant in 63 ranges. Saturation is asserted against the
**achievable maximum** instead — a strictly stronger statement than 90% — and every manifest
carries a note explaining the discrepancy. Documented in `docs/prod-info/README.md`.

**3. Baseline is 30M rows per shard, not 5M.** The tier table and the activity-skew gate
contradict each other. A baseline topology has ~168k memberships and every membership must
carry at least one row (the plan's own rule, and a necessary one — a part must contain the
tenants the catalog says it contains). At 5M rows that floor, not the activity weights,
decides the row count of **70% of tenants**, and the measured p90/p50 skew collapses to 18
against the source's 98. 26.4M is where the median tenant's weight-share clears its floor;
30M clears it, and the skew gates pass. Cost: ~1.3GB instead of ~220MB.

**4. The loader/bench integration tests are not `--ignored`.** They run against a throwaway
PostgreSQL (testcontainers) and an in-memory object store, so they run in `make test`. A
check that only runs when someone remembers to run it is a check that rots.

One packaging fix was also needed: the workspace enabled `parquet`'s `async` +
`object_store` features globally, so `ukiel-core` — and any offline tool built on it —
dragged in tokio, `object_store` and reqwest. Moved to the two crates that genuinely read
asynchronously from an object store, which is what makes `cargo install --path
tools/prod-synth` work with only a Rust toolchain.

**Goal:** Turn the anonymized ClickHouse observations in `docs/prod-info/` into
a deterministic synthetic event fixture named **prod-synth**. Ship the offline
generator as a standalone, single-purpose CLI that anyone with a Rust toolchain
can run without Ukiel services. Separate executables load that artifact into
Ukiel and benchmark it. Use the fixture to test Ukiel's real catalog, Parquet
scan, namespace isolation, and representative event queries with the production
tenant/part shape instead of independent random numbers.

**Why now:** The issue-0014 investigation showed that a plausible-looking random
fixture can answer the wrong question. Production has a median 11 exact parts per
sampled tenant but 63 range candidates, median 5.727x range overfetch, a
three-orders-of-magnitude tenant activity skew, and sparse part key ranges. Those
relationships are precisely what the catalog key filter, packing, and scoped
query path must handle. We now have enough anonymized evidence to reproduce that
shape, but not enough to claim a byte-for-byte or column-value replica.

## Execution position and boundaries

Issue 0014 is merged. This plan consumes its final `key_filter`, truthful-bitmap
write path, and range-only benchmark semantics; it must not copy their
implementation into the generator. Product-specific metadata is derived only by
the loader through Ukiel's normal write path.
The four source files under `docs/prod-info/` must be tracked in the same
revision as the parser tests; no tool fetches production metadata
from the network.

Plan 44 is **not** a prerequisite. Until native logical JSON lands, `properties`
is stored as valid JSON text in an `utf8` field, matching the physical reality of
the captured ClickHouse `String` column. A later JSON plan may add extraction
queries over the same generated values.

The generated data, artifact paths, manifest format, table names, CLI names, and
query-suite names use `prod-synth`, not a source-product name. The source workload
is acknowledged only as provenance in documentation and the input profile. The
artifact must identify itself as synthetic and profile-derived; it must never
present itself as a production copy.

This is a new house benchmark. It does not replace ClickBench, JSONBench, the
catalog saturation suite, or their recorded baselines:

- the catalog-only arm measures realistic membership geometry cheaply;
- the materialized arm measures real Ukiel planning and scans over generated
  Parquet;
- ClickBench and JSONBench remain the standard comparisons; and
- Plan 40 remains the high-cardinality catalog capacity proof.

No production code or migration should be necessary. If implementation appears
to require either, stop and document the missing product capability before
expanding this benchmark plan.

## Tool boundaries

Plan 45 produces three executables, not another command family inside the existing
8k-line `ukiel-e2e` `bench` binary:

| executable | package | one responsibility | allowed side effects |
|---|---|---|---|
| `prod-synth` | `tools/prod-synth` | validate a profile and deterministically produce or verify a portable fixture | local filesystem only |
| `ukiel-prod-load` | `tools/ukiel-prod-load` | load one existing manifest into an explicitly selected Ukiel test deployment, or create its ephemeral catalog-only form | catalog and object-store writes declared by the command |
| `ukiel-prod-bench` | `tools/ukiel-prod-bench` | measure an existing load and compare scoped Ukiel results with raw DataFusion | read-only service access plus local result files |

The artifact pipeline is:

```text
docs/prod-info/
    -> prod-synth
    -> manifest.json + topology.json + parquet/
    -> ukiel-prod-load
    -> explicitly named Ukiel fixture
    -> ukiel-prod-bench
```

The tools communicate through `ukiel-prod-synth/v1`; they do not call each
other's command handlers or reach into another binary's modules. The manifest is
the public contract. Its serde types and version checks live in the tiny
`tools/prod-synth-contract` library, which performs no profile parsing,
generation, service access, or benchmarking. All three executables may depend on
that contract library; no executable package may depend on another executable
package.

`prod-synth` remains usable without compiling PostgreSQL, Kafka, object-store,
HTTP, or DataFusion clients. It may depend on `ukiel-core` only for stable
schema/Parquet writer primitives; it must not depend on `ukiel-e2e`, `ukield`,
`ukiel-catalog`, `ukiel-query`, `ukiel-ingest`, or `ukiel-compactor`.

The separation is enforced by tests and dependency inspection:

- `cargo install --path tools/prod-synth` succeeds from a fresh checkout;
- `prod-synth --help`, `profile`, `generate`, and `verify` need no running service;
- the loader accepts a manifest path and cannot invoke topology generation;
- the benchmark runner accepts a manifest/load identity and cannot generate or
  mutate a fixture; and
- no command assumes the repository working directory or a fixed `bench/` path.

## What the source can and cannot tell us

The captured one-shard, 14-day observations contain:

- 68 part records and 534 sampled tenant records;
- 2,389,316 total part-to-tenant memberships
  (`sum(part.distinct_keys)`);
- tenant exact-part p50/p90 of 11/43;
- tenant range-part p50/p90 of 63/63;
- range-overfetch p50/p90 of 5.727/63;
- tenant-row p50/p90/p99/max of 127/12,493/439,457/5,684,918;
- part key-density p10/p50/p90 of
  0.00625175/0.10264053/0.13749292; and
- a key span around 511k for ordinary large parts.

If the tenant sample is uniform over active tenant IDs and covers the same part
scope, the active tenant count is estimable from the two independent margins:

```text
estimated active tenants
  = sum(part.distinct_keys) / mean(tenant.exact_parts)
  = 2,389,316 / 16.818352...
  = 142,066
```

That is an **estimate**, not a discovered production total. The profile summary
and every generated manifest must state the formula, assumptions, and override.

The capture does not contain real tenant IDs, the original part-to-tenant
matrix, per-event distributions, property cardinalities, per-column compression,
query frequency, host identity, or replica identity. The generator therefore:

- statistically reconstructs one consistent membership graph;
- preserves joint tenant activity/exact-fanout samples and part degree margins;
- uses declared deterministic synthetic distributions for events and
  properties; and
- records source bytes, ClickHouse part type, and ClickHouse merge level only as
  provenance. It never writes them as a generated Ukiel part's size or level.

## Generator and artifact design

### Two representations from one topology

`prod-synth generate` compiles a `SyntheticTopology` once and writes:

1. **Portable topology:** exact memberships, truthful ranges, row weights, time
   statistics, and representative tenants. It contains no PostgreSQL rows and no
   precomputed Ukiel `key_filter`. The loader can consume it for a fast
   catalog-only measurement.
2. **Materialized data:** actual sorted Parquet files described by the same
   manifest. Here `row_count` and `size_bytes` come from the generated files,
   never from scaled ClickHouse counters.

The manifest fingerprints the input profile, generator version, seed, topology,
and every output file. A report from one seed or tier must be impossible to
mistake for another.

Generation requires an explicit `--output`; examples in this plan use
`bench/datasets/prod-synth/<label>/`, but the executable itself has no repository
default. Output is a temporary sibling directory renamed atomically only after
verification. `prod-synth verify <manifest>` performs offline structural,
digest, footer, and census verification and never contacts Ukiel.

### Deterministic topology compilation

Implement this algorithm explicitly; do not substitute independent random bands:

1. Parse and validate complete records from `table.json`,
   `part-geometry.jsonl`, and `tenant-fanout.jsonl`.
2. Compute the source active-tenant estimate above. `--tenants` controls
   generated scale but does not rewrite the source estimate.
3. Scale every part's `distinct_keys` and `key_span` by
   `generated_tenants / estimated_source_tenants`. Clamp so
   `1 <= distinct_keys <= key_span` and
   `distinct_keys <= generated_tenants`. Use largest-remainder allocation so
   rounding does not drift the total degree.
4. Deterministically stratified-resample complete tenant records. Keep
   `tenant_rows`, `exact_parts`, `range_parts`, and `range_overfetch` together.
5. Repair sampled tenant degrees by the smallest deterministic changes, bounded
   to `1..part_count`, until their sum equals the scaled part-degree sum. Record
   every repair and fail if more than 2% of tenants change.
6. Realize the exact bipartite degree sequence with bipartite Havel-Hakimi, then
   run deterministic degree-preserving edge swaps to remove construction-order
   bias. Duplicate tenant/part edges and unfilled margins are errors.
7. Draw unique synthetic numeric tenant IDs uniformly from a scaled key universe
   derived from the source maximum `key_span`. Sparse min/max ranges then emerge
   from actual membership rather than being assigned independently.
8. Recompute each part's min, max, density and every tenant's exact/range fanout
   from the finished graph. These generated values drive metadata and checks.
9. Use sampled `tenant_rows` as joint activity weights. Give every membership at
   least one row, allocate each part's remaining budget across members by those
   weights, and retain source `observed_rows` shares across parts. Reject a row
   tier smaller than its membership count.
10. For `--shards N`, build independent topologies with domain-separated seeds
    and common tenant IDs. Candidate counts add; densities and ratios do not.
    One shard is the default because that is what was captured.

Use a local stable PRNG with an explicit algorithm/version and golden sequence
test. Avoid `thread_rng`, randomized hash-map iteration, wall-clock data, and
library sampling helpers whose algorithm is absent from the manifest contract.

### Tiers

| tier | tenants | rows per shard | purpose |
|---|---:|---:|---|
| `smoke` | 1,000 | 100,000 | parser/generator/load correctness |
| `baseline` | 10,000 | 5,000,000 | normal local query and catalog baseline |
| `shape` | estimated 142,066 | 100,000,000 | optional cardinality confirmation |

`--tenants`, `--rows-per-shard`, `--shards`, and `--seed` may override these
versioned defaults. Every override is recorded. `shape` is not an acceptance
gate and does not pretend to reproduce the source's 4.47B observed rows.

### Production-derived event schema

Generate a supported subset of the captured event table:

```text
team_id          int64          packing key
timestamp        timestamp_ms   event time
event            utf8
distinct_id      utf8
uuid             utf8
properties       utf8           valid compact JSON text
elements_chain   utf8
mat_$current_url utf8           promoted from properties
mat_$host        utf8           promoted from properties
mat_$lib         utf8           promoted from properties
```

Use sort key `[team_id, timestamp, event, distinct_id, uuid]`. ClickHouse hashes
the last two sort expressions; Ukiel declares columns rather than arbitrary sort
expressions, so direct string ordering is an explicit adaptation. The packing key
remains first.

The field selection and event vocabulary are derived from the captured event
workload, but generated artifacts use only the neutral `prod-synth` identity.
The non-profile dimensions are synthetic and labelled as such:

- a fixed skewed vocabulary containing `$pageview`, `$autocapture`,
  `$pageleave`, `$identify`, and custom events;
- deterministic persons with repeated events, so `distinct_id` is neither
  unique per row nor constant per tenant;
- a bounded Zipf-like URL/host/library vocabulary; and
- valid `properties` JSON whose promoted values exactly match the three
  `mat_*` columns.

Keep these in a versioned `ValueModel` stored verbatim in the manifest. They are
useful query data, not claims about production.

### Part and partition semantics

Each generated source part becomes one generated Parquet part. Rows are sorted
and written in bounded batches with Ukiel's normal writer properties. Use the
anonymized source `partition_id` as a provenance partition tag; do not call it an
Ukiel day partition. Clamp sampled time spans to the 14-day fixture window and
record the clamp count.

Load at a fixed non-L0 level only to keep an idle benchmark stable. Do not map
ClickHouse `level` to Ukiel's compaction ladder and do not draw a compaction
conclusion from this layout. A later ingest/compaction workload may reuse the row
generator but must define Ukiel day partitioning as a separate experiment.

## Fidelity gates

Offline generation fails before reporting if any invariant is false:

- exact part count equals the selected profile count per shard;
- every part and tenant degree equals its compiled graph margin;
- part ranges are derived from and bracket every exact member;
- actual Parquet rows, footer key/timestamp bounds, manifest counts, and file
  sizes agree;
- generated count queries agree with the generator census; and
- the same seed/config/profile digest produces the same topology/file digests.

Loading fails before reporting success if any service-side invariant is false:

- decoding each product-derived packing-key bitmap returns the manifest key set;
- every exact member is accepted by the product-derived key filter;
- object HEAD, manifest file size, and catalog `size_bytes` agree;
- actual rows, manifest rows, and catalog `row_count` agree; and
- the loaded manifest digest and fixture label are recorded together.

For `baseline`, compare generated and source one-shard distributions:

- exact-parts p50/p90 within 1 part;
- range-parts p50 within 3 parts;
- range-overfetch p50 within 15% and p90 in the same saturation bucket
  (at least 90% of source part count);
- key-density p10/p50/p90 within 20% relative error;
- normalized tenant activity p50/p90/p99 and the
  `log1p(tenant_rows)` to `exact_parts` rank correlation within documented
  tolerances; and
- no more than 2% tenant-degree repairs.

Print every source/generated pair. Do not silently loosen a failed tolerance.

---

### Task 1: Profile parser, validator, and summary

**Files:**

- Modify: `Cargo.toml` (add the standalone tool package to the workspace)
- Create: `tools/prod-synth-contract/Cargo.toml`
- Create: `tools/prod-synth-contract/src/lib.rs`
- Create: `tools/prod-synth/Cargo.toml`
- Create: `tools/prod-synth/README.md`
- Create: `tools/prod-synth/src/lib.rs`
- Create: `tools/prod-synth/src/main.rs`
- Create: `tools/prod-synth/src/profile.rs`
- Track: all four files under `docs/prod-info/`
- Test: `tools/prod-synth/tests/profile.rs`

**Interfaces:**

- package and binary name `prod-synth`
- `ProductionProfile::load(path) -> Result<ProductionProfile>`
- typed `TableObservation`, `PartObservation`, and `TenantObservation`
- `ProfileSummary` with digests, counts, quantiles, summed part degree, mean
  tenant degree, and the active-tenant estimate
- `prod-synth profile --profile DIR --report FILE`

- [x] **Step 1: Write failing parser/validation tests.** Pin 68 parts, 534
  tenant samples, 2,389,316 memberships, exact p50/p90 11/43, range p50 63,
  and estimate 142,066. Invalid JSONL, negative/non-finite values,
  `observed_rows > physical_rows`, inconsistent density, and inconsistent
  overfetch must name the file and record.
- [x] **Step 2: Implement streaming loading and stable quantiles.** Hash raw
  bytes, not reserialized structs. `show-create.sql` is hashed provenance, not a
  schema language to parse.
- [x] **Step 3: Add the CLI/report.** Require an explicit report path, print the
  sampling/replica caveats, and make `--help` work without a repository cwd.
- [x] **Step 4: Prove standalone installation.** Install into a temporary root
  and run `--help` and `profile` without any Ukiel service.
- [x] **Step 5: Verify and commit.**

```bash
cargo test -p prod-synth profile
cargo run -p prod-synth -- profile --profile docs/prod-info --report /tmp/prod-synth-profile.json
cargo install --path tools/prod-synth --root /tmp/prod-synth-install
cargo fmt --check
cargo clippy -p prod-synth --all-targets -- -D warnings
git commit -m "tools: parse and validate the production event shape"
```

---

### Task 2: Deterministic topology compiler

**Files:**

- Create: `tools/prod-synth/src/topology.rs`
- Create: `tools/prod-synth/src/rng.rs`
- Create: `tools/prod-synth-contract/src/manifest.rs`
- Test: `tools/prod-synth/tests/topology.rs`

**Interfaces:**

- `TopologyConfig { tier, tenants, rows_per_shard, shards, seed }`
- `SyntheticTopology { tenants, parts, memberships, summary }`
- `compile(profile, config) -> Result<SyntheticTopology>`
- manifest format `ukiel-prod-synth/v1`

- [x] **Step 1: Pin PRNG/scaling math.** Golden sequence; exact rounded totals;
  tier expansion; too-few rows reports the required minimum.
- [x] **Step 2: Write failing graph tests.** Hand-built graphical/impossible
  sequences; exact margins; no duplicates; deterministic digest; seed change.
- [x] **Step 3: Implement the specified compiler.** Keep tenant fields joint.
  Derive ranges/fanout only after graph realization.
- [x] **Step 4: Run smoke/baseline in memory.** All fidelity gates pass; write
  reports only to the explicit path supplied by the caller.
- [x] **Step 5: Verify and commit.**

```bash
cargo test -p prod-synth topology
cargo fmt --check
cargo clippy -p prod-synth --all-targets -- -D warnings
git commit -m "tools: compile production-shaped tenant and part topology"
```

---

### Task 3: Streaming Parquet generator and manifest

**Files:**

- Create: `tools/prod-synth/src/generate.rs`
- Create: `tools/prod-synth/src/value_model.rs`
- Modify: `tools/prod-synth-contract/src/manifest.rs`
- Modify: `tools/prod-synth/Cargo.toml` only for generator dependencies
- Test: `tools/prod-synth/tests/generate.rs`

**Interfaces:**

- `prod-synth generate --tier smoke|baseline|shape --profile DIR --output DIR`
  `[--seed N] [--tenants N] [--rows-per-shard N] [--shards N]`
- `prod-synth verify MANIFEST`
- example output `bench/datasets/prod-synth/<label>/`; never an implicit default
- `manifest.json` with all digests/configuration, source/generated summaries,
  `ValueModel`, exact membership topology, actual rows/bytes, and representative
  tenants. Ukiel bitmaps and key filters are not generated here.

- [x] **Step 1: Define schema/value model/manifest round-trip tests.** Unknown
  manifest versions fail closed.
- [x] **Step 2: Write a failing tiny generation test.** Assert sorted rows,
  valid JSON, promoted equality, repeated distinct IDs, deterministic UUIDs,
  time/footer/membership truth, and identical file digests on rerun.
- [x] **Step 3: Implement bounded generation.** One part at a time, bounded
  Arrow batches, actual closed-file size. Never materialize all rows in memory.
- [x] **Step 4: Make output atomic.** Temporary sibling then rename after all
  gates; refuse overwrite without explicit `--replace` scoped to that label.
- [x] **Step 5: Implement offline verification.** Recompute profile, topology,
  file, footer, schema, census, and manifest digests without network access.
- [x] **Step 6: Run smoke twice and prove byte determinism.**
- [x] **Step 7: Verify and commit.**

```bash
cargo test -p prod-synth generate
cargo run --release -p prod-synth -- generate --tier smoke --profile docs/prod-info \
  --output bench/datasets/prod-synth/plan45-smoke
cargo run --release -p prod-synth -- verify \
  bench/datasets/prod-synth/plan45-smoke/manifest.json
cargo fmt --check
cargo clippy -p prod-synth --all-targets -- -D warnings
git commit -m "tools: generate deterministic production-shaped parquet"
```

---

### Task 4: Truthful Ukiel loader and catalog geometry checks

**Files:**

- Modify: `Cargo.toml` (add the loader package to the workspace)
- Create: `tools/ukiel-prod-load/Cargo.toml`
- Create: `tools/ukiel-prod-load/src/main.rs`
- Create: `tools/ukiel-prod-load/src/load.rs`
- Create: `tools/ukiel-prod-load/src/catalog_only.rs`
- Test: `tools/ukiel-prod-load/tests/load.rs`

**Interfaces:**

- `ukiel-prod-load materialized --manifest FILE --label L --config FILE`
- `ukiel-prod-load catalog-only --manifest FILE --label L --ephemeral --catalog-url URL`
- hypertable `prod_synth_events_<label>`, packing key `team_id`
- source partition provenance in `partition_values`
- logical table `events` for heavy, median, light, high-overfetch,
  low-overfetch, and a deterministic tenant sample
- no profile parsing, topology compilation, row generation, or benchmark commands

- [x] **Step 1: Write the failing integration test.** Generate a tiny artifact
  with `prod-synth`, load it, then assert object HEAD = manifest = catalog bytes;
  actual rows = manifest = catalog rows; every member is returned; exact-absent
  members are removed by the provider bitmap.
- [x] **Step 2: Validate the artifact boundary.** Load and verify
  `ukiel-prod-synth/v1`, reject unknown versions or digest mismatches before
  connecting to a service, and never regenerate missing data.
- [x] **Step 3: Implement product-path create/upload/ADD.** Use normal metadata
  builders so the product derives roaring bitmaps and issue-0014 key filters.
  Set-based fake part rows are forbidden for the materialized fixture. The
  separate ephemeral arm may bulk-seed the same truthful topology with
  `size_bytes = 0` and `catalog-only://` paths because it measures no object
  behavior.
- [x] **Step 4: Verify catalog shape.** Per representative tenant report range,
  shipped-filter, and exact counts. Assert zero false negatives.
- [x] **Step 5: Make lifecycle loud.** Require a fresh label and distinguish
  catalog-only seeds from materialized loads. The ephemeral command requires a
  disposable stack, refuses to run alongside configured compactor/GC roles,
  cleans its hypertables/commits/parts on success, and attempts the same cleanup
  on error. If cleanup fails, exit with the exact manual reset command. Never
  leave fake paths silently for a background compactor.
- [x] **Step 6: Verify and commit.**

```bash
cargo test -p ukiel-prod-load --test load -- --ignored --nocapture
cargo fmt --check
cargo clippy -p ukiel-prod-load --all-targets -- -D warnings
git commit -m "tools: load a prod-synth artifact into Ukiel"
```

---

### Task 5: Scoped prod-synth queries and raw-file reference

**Files:**

- Modify: `Cargo.toml` (add the benchmark package to the workspace)
- Create: `bench/queries/prod-synth/queries.sql`
- Create: `bench/queries/prod-synth/README.md`
- Create: `tools/ukiel-prod-bench/Cargo.toml`
- Create: `tools/ukiel-prod-bench/src/main.rs`
- Create: `tools/ukiel-prod-bench/src/catalog.rs`
- Create: `tools/ukiel-prod-bench/src/queries.rs`
- Test: `tools/ukiel-prod-bench/tests/benchmark.rs`

**Query set:**

1. tenant event count;
2. count in a 24-hour window;
3. top events;
4. distinct persons;
5. top promoted current URLs; and
6. event counts by promoted library.

The Ukiel arm uses the real namespace-scoped HTTP path and omits `team_id` from
user SQL. Raw DataFusion reads the same files and adds the equivalent explicit
`team_id = ?`. Normalize result batches and require equality before timing.

**Interfaces:**

- `ukiel-prod-bench queries --manifest FILE --label L --result FILE [--iters N]`
- `ukiel-prod-bench catalog --manifest FILE --label L --result FILE [--range-only]`
- classes: heavy, median, light, high-overfetch, low-overfetch
- one warmup then median of five by default
- report source/generated geometry, range/filter/exact candidates, planned
  files, returned rows, Ukiel/raw timings and all fixture digests
- no generation, upload, catalog mutation, or cleanup commands

- [x] **Step 1: Test query rendering.** Quoted `"mat_$current_url"` and
  `"mat_$lib"` survive; comparison queries have deterministic ordering.
- [x] **Step 2: Write failing end-to-end equivalence.** All queries/classes agree
  between scoped Ukiel and raw DataFusion; count equals census; range-only
  false candidates are not read.
- [x] **Step 3: Implement runner/report.** Measure query time only; generation
  and loading are facts read from the manifest/load record, not actions this
  process may perform. Record failed queries and fail after attempting the rest.
- [x] **Step 4: Implement the catalog command for an existing load.** Use the
  same tenants/topology and issue-0014 before/after arms. Report rows/bytes and
  latency, but do not call 68 parts a saturation test.
- [x] **Step 5: Enforce read-only behavior.** Fail startup if the requested
  command would require creating, loading, repairing, or cleaning a fixture.
- [x] **Step 6: Verify and commit.**

```bash
cargo test -p ukiel-prod-bench
cargo test -p ukiel-prod-bench --test benchmark -- --ignored --nocapture
cargo fmt --check
cargo clippy -p ukiel-prod-bench --all-targets -- -D warnings
git commit -m "tools: benchmark a loaded prod-synth artifact"
```

---

### Task 6: Baseline, runbook, and roadmap close-out

**Files:**

- Modify: `bench/README.md`
- Modify: `docs/prod-info/README.md`
- Modify: `tools/prod-synth/README.md`
- Create: `docs/notes/2026-07-14-prod-synth-baseline.md`
- Modify: `docs/superpowers/plans/2026-07-05-ukiel-v1-roadmap.md`
- Modify: this plan

- [x] **Step 1: Full verification.**

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
make test
cargo install --path tools/prod-synth --root /tmp/prod-synth-install
/tmp/prod-synth-install/bin/prod-synth --help
```

- [x] **Step 2: From a clean checkout, generate and verify the one-shard
  baseline with no services running.** Record profile, topology, and file
  digests plus generation bytes/time. Copy or move the completed portable
  artifact to the benchmark host; do not regenerate it inside the loader.
- [x] **Step 3: On a clean disposable stack, load that exact manifest and run the
  baseline.** Record machine/SHA, load time, fidelity pairs, catalog A/B,
  scoped/raw results, and whether issue 0014 removes overfetch before SQLx.
- [x] **Step 4: Generate smoke with `--shards 10`, load its topology in
  catalog-only mode, and measure it.** Candidate counts add while
  density/overfetch distributions stay stable. Materializing 10x data is not an
  acceptance gate.
- [x] **Step 5: Write three runbooks.** The generator runbook covers installation,
  explicit inputs/outputs, determinism, portability, and offline verification.
  The loader runbook covers mutation scope and cleanup. The benchmark runbook
  covers read-only execution and result interpretation. Include
  properties-as-utf8, replica caveats, and why ClickHouse bytes/levels are not
  copied.
- [x] **Step 6: Mark row 45 executed with measured conclusions and commit.**

```bash
git add Cargo.toml tools/prod-synth-contract tools/prod-synth \
  tools/ukiel-prod-load tools/ukiel-prod-bench \
  bench/README.md bench/queries/prod-synth docs/prod-info \
  docs/notes/2026-07-14-prod-synth-baseline.md \
  docs/superpowers/plans/2026-07-05-ukiel-v1-roadmap.md \
  docs/superpowers/plans/2026-07-14-ukiel-prod-synth.md
git commit -m "bench: record the production-shaped prod-synth baseline"
```

## Final acceptance

Plan 45 is complete only when:

- a fresh checkout installs `prod-synth`, and its offline CLI compiles the
  committed profile into the same manifest without any service running;
- the generator has none of the forbidden service/client dependencies, and no
  executable package depends on another executable package;
- baseline contains real Parquet objects and truthful catalog metadata;
- generated distributions pass the pinned fidelity gates;
- scoped Ukiel results equal raw DataFusion for every query/class;
- catalog range/filter/exact counts are recorded together;
- generator, loader, and benchmark remain separate executables joined by the
  versioned manifest, with their declared side-effect boundaries intact;
- generated artifacts, commands, paths, tables, and query suites use the neutral
  `prod-synth` identity;
- no source byte total, ClickHouse level, or invented value distribution is
  presented as measured Ukiel production behavior; and
- another operator can generate the portable artifact and reproduce the loaded
  baseline from the three runbooks alone.

## Self-review notes

- **Why not copy 4.47B rows?** Correlated shape is the useful finding. Baseline
  scales cardinality while retaining graph degrees, skew, and sparsity.
- **Why not generate independent ranges?** That is how the old fixture lied.
  Ranges, bitmaps, and filters derive from one exact membership graph.
- **Why catalog-only and materialized?** Catalog A/B should take seconds; query
  and object correctness require real files. Fake paths cannot answer both.
- **Why a DDL subset?** Ukiel does not support every captured ClickHouse type,
  Map, Array, Enum, UUID, materialized expression, or DateTime variant.
- **Why no JSON extraction?** Plan 44 owns it. Valid raw JSON keeps that future
  query possible without changing generation semantics.
- **Why no compaction conclusion?** ClickHouse parts are not Ukiel day
  partitions or ladder levels. Reusing their levels would give a real number a
  false meaning.

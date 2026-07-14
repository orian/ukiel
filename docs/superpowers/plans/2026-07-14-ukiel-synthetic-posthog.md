# Ukiel Plan 45: Production-Shaped Synthetic PostHog

> **For agentic workers:** Execute this plan task-by-task. Use checkbox
> (`- [ ]`) progress, run each task's focused tests before its commit, and do not
> start until issue 0014's catalog key-filter implementation has merged.

**Status:** Ready after issue 0014 merges.

**Goal:** Turn the anonymized ClickHouse observations in `docs/prod-info/` into
a deterministic synthetic PostHog fixture. Use it to test Ukiel's real catalog,
Parquet scan, namespace isolation, and representative event queries with the
production tenant/part shape instead of independent random numbers.

**Why now:** The issue-0014 investigation showed that a plausible-looking random
fixture can answer the wrong question. Production has a median 11 exact parts per
sampled tenant but 63 range candidates, median 5.727x range overfetch, a
three-orders-of-magnitude tenant activity skew, and sparse part key ranges. Those
relationships are precisely what the catalog key filter, packing, and scoped
query path must handle. We now have enough anonymized evidence to reproduce that
shape, but not enough to claim a byte-for-byte or column-value replica.

## Execution position and boundaries

Issue 0014 is a hard prerequisite. This plan consumes the final `key_filter` and
truthful-bitmap write path plus its range-only benchmark arm; it must not guess
those APIs while another agent is changing them. Rebase after the issue lands.
The four source files under `docs/prod-info/` must be tracked in the same
revision as the parser tests; the benchmark never fetches production metadata
from the network.

Plan 44 is **not** a prerequisite. Until native logical JSON lands, `properties`
is stored as valid JSON text in an `utf8` field, matching the physical reality of
the captured ClickHouse `String` column. A later JSON plan may add extraction
queries over the same generated values.

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

## Generator design

### Two outputs from one topology

`bench posthog synth` compiles a `SyntheticTopology` once and feeds:

1. **Catalog topology:** truthful ranges, exact packing-key bitmaps, bounded key
   filters, row weights, and time stats. It can be seeded without objects for a
   fast range-vs-filter/admission measurement.
2. **Materialized data:** actual sorted Parquet files and matching catalog rows.
   Here `row_count` and `size_bytes` come from the generated file, never from
   scaled ClickHouse counters.

The manifest fingerprints the input profile, generator version, seed, topology,
and every output file. A report from one seed or tier must be impossible to
mistake for another.

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

### PostHog-shaped schema

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

Generation fails before reporting if any invariant is false:

- exact part count equals the selected profile count per shard;
- every part and tenant degree equals its compiled graph margin;
- part ranges are derived from and bracket every exact member;
- every exact member is accepted by the stored key filter;
- decoding the stored packing-key bitmap returns the manifest key set exactly;
- actual Parquet rows, footer key/timestamp bounds, catalog `row_count`, and
  object `size_bytes` agree;
- generated count queries agree with the generator census; and
- the same seed/config/profile digest produces the same topology/file digests.

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

- Create: `crates/ukiel-e2e/src/bin/bench/posthog/mod.rs`
- Create: `crates/ukiel-e2e/src/bin/bench/posthog/profile.rs`
- Modify: `crates/ukiel-e2e/src/bin/bench/main.rs`
- Test: profile module unit tests

**Interfaces:**

- `PosthogProfile::load(path) -> Result<PosthogProfile>`
- typed `TableObservation`, `PartObservation`, and `TenantObservation`
- `ProfileSummary` with digests, counts, quantiles, summed part degree, mean
  tenant degree, and the active-tenant estimate
- `bench posthog profile [--profile docs/prod-info]`

- [ ] **Step 1: Write failing parser/validation tests.** Pin 68 parts, 534
  tenant samples, 2,389,316 memberships, exact p50/p90 11/43, range p50 63,
  and estimate 142,066. Invalid JSONL, negative/non-finite values,
  `observed_rows > physical_rows`, inconsistent density, and inconsistent
  overfetch must name the file and record.
- [ ] **Step 2: Implement streaming loading and stable quantiles.** Hash raw
  bytes, not reserialized structs. `show-create.sql` is hashed provenance, not a
  schema language to parse.
- [ ] **Step 3: Add the CLI/report.** Write
  `bench/results/posthog-profile-<digest>.json` and print the sampling/replica
  caveats.
- [ ] **Step 4: Verify and commit.**

```bash
cargo test -p ukiel-e2e --bin bench posthog::profile
cargo fmt --check
cargo clippy -p ukiel-e2e --bin bench -- -D warnings
git commit -m "bench: parse and validate production PostHog shape"
```

---

### Task 2: Deterministic topology compiler

**Files:**

- Create: `crates/ukiel-e2e/src/bin/bench/posthog/topology.rs`
- Create: `crates/ukiel-e2e/src/bin/bench/posthog/rng.rs`
- Test: topology/rng unit tests

**Interfaces:**

- `TopologyConfig { tier, tenants, rows_per_shard, shards, seed }`
- `SyntheticTopology { tenants, parts, memberships, summary }`
- `compile(profile, config) -> Result<SyntheticTopology>`
- manifest format `ukiel-posthog-synth/v1`

- [ ] **Step 1: Pin PRNG/scaling math.** Golden sequence; exact rounded totals;
  tier expansion; too-few rows reports the required minimum.
- [ ] **Step 2: Write failing graph tests.** Hand-built graphical/impossible
  sequences; exact margins; no duplicates; deterministic digest; seed change.
- [ ] **Step 3: Implement the specified compiler.** Keep tenant fields joint.
  Derive ranges/fanout only after graph realization.
- [ ] **Step 4: Run smoke/baseline in memory.** All fidelity gates pass; store
  reports only under gitignored `bench/results/`.
- [ ] **Step 5: Verify and commit.**

```bash
cargo test -p ukiel-e2e --bin bench posthog::topology
cargo fmt --check
cargo clippy -p ukiel-e2e --bin bench -- -D warnings
git commit -m "bench: compile production-shaped tenant and part topology"
```

---

### Task 3: Streaming Parquet generator and manifest

**Files:**

- Create: `crates/ukiel-e2e/src/bin/bench/posthog/generate.rs`
- Modify: `crates/ukiel-e2e/Cargo.toml` only for workspace-pinned dependencies
- Modify: `crates/ukiel-e2e/src/bin/bench/main.rs`
- Test: generator unit tests with a tiny profile

**Interfaces:**

- `bench posthog synth --tier smoke|baseline|shape [--profile DIR]`
  `[--label L] [--seed N] [--tenants N] [--rows-per-shard N] [--shards N]`
- output `bench/datasets/posthog-synth/<label>/`
- `manifest.json` with all digests/configuration, source/generated summaries,
  `ValueModel`, per-part bitmap/stats, actual rows/bytes, and representative
  tenants

- [ ] **Step 1: Define schema/value model/manifest round-trip tests.** Unknown
  manifest versions fail closed.
- [ ] **Step 2: Write a failing tiny generation test.** Assert sorted rows,
  valid JSON, promoted equality, repeated distinct IDs, deterministic UUIDs,
  time/footer/bitmap truth, and identical file digests on rerun.
- [ ] **Step 3: Implement bounded generation.** One part at a time, bounded
  Arrow batches, actual closed-file size. Never materialize all rows in memory.
- [ ] **Step 4: Make output atomic.** Temporary sibling then rename after all
  gates; refuse overwrite without explicit `--replace` scoped to that label.
- [ ] **Step 5: Run smoke twice and prove byte determinism.**
- [ ] **Step 6: Verify and commit.**

```bash
cargo test -p ukiel-e2e --bin bench posthog::generate
cargo run --release -p ukiel-e2e --bin bench -- posthog synth --tier smoke --label plan45-smoke
cargo fmt --check
cargo clippy -p ukiel-e2e --bin bench -- -D warnings
git commit -m "bench: generate deterministic synthetic PostHog parquet"
```

---

### Task 4: Truthful Ukiel loader and catalog geometry checks

**Files:**

- Create: `crates/ukiel-e2e/src/bin/bench/posthog/load.rs`
- Modify: `crates/ukiel-e2e/src/bin/bench/main.rs`
- Test: ignored integration test in `crates/ukiel-e2e/tests/`

**Interfaces:**

- `bench posthog load --label L`
- `bench posthog catalog-seed --label L --ephemeral` for the topology-only arm
- hypertable `posthog_events_synth_<label>`, packing key `team_id`
- source partition provenance in `partition_values`
- logical table `events` for heavy, median, light, high-overfetch,
  low-overfetch, and a deterministic tenant sample

- [ ] **Step 1: Write the failing integration test.** Generate tiny data,
  upload and commit it, then assert object HEAD = manifest = catalog bytes;
  actual rows = manifest = catalog rows; every member is returned; exact-absent
  members are removed by provider bitmap.
- [ ] **Step 2: Implement product-path create/upload/ADD.** Use normal metadata
  builders and issue-0014 key-filter derivation. Set-based fake part rows are
  forbidden for this 68-part materialized fixture. The separate ephemeral arm
  may bulk-seed the same truthful topology with `size_bytes = 0` and
  `catalog-only://` paths because it measures no object behavior.
- [ ] **Step 3: Verify catalog shape.** Per representative tenant report range,
  shipped-filter, and exact counts. Assert zero false negatives.
- [ ] **Step 4: Make lifecycle loud.** Require a fresh label and distinguish
  catalog-only seeds from materialized loads. The ephemeral command requires a
  disposable stack, refuses to run alongside configured compactor/GC roles,
  cleans its hypertables/commits/parts on success, and attempts the same cleanup
  on error. If cleanup fails, exit with the exact manual reset command. Never
  leave fake paths silently for a background compactor.
- [ ] **Step 5: Verify and commit.**

```bash
cargo test -p ukiel-e2e --test posthog_synth_test -- --ignored --nocapture
cargo fmt --check
cargo clippy -p ukiel-e2e --bin bench -- -D warnings
git commit -m "bench: load truthful synthetic PostHog fixture into Ukiel"
```

---

### Task 5: Scoped PostHog queries and raw-file reference

**Files:**

- Create: `bench/queries/posthog-synth/queries.sql`
- Create: `bench/queries/posthog-synth/README.md`
- Create: `crates/ukiel-e2e/src/bin/bench/posthog/run.rs`
- Modify: `crates/ukiel-e2e/src/bin/bench/main.rs`
- Test: rendering/result unit tests and ignored integration test

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

- `bench posthog run --label L [--iters N]`
- classes: heavy, median, light, high-overfetch, low-overfetch
- one warmup then median of five by default
- report source/generated geometry, range/filter/exact candidates, planned
  files, returned rows, Ukiel/raw timings and all fixture digests

- [ ] **Step 1: Test query rendering.** Quoted `"mat_$current_url"` and
  `"mat_$lib"` survive; comparison queries have deterministic ordering.
- [ ] **Step 2: Write failing end-to-end equivalence.** All queries/classes agree
  between scoped Ukiel and raw DataFusion; count equals census; range-only
  false candidates are not read.
- [ ] **Step 3: Implement runner/report.** Separate generation/load/query time.
  Record failed queries and fail after attempting the rest.
- [ ] **Step 4: Add `bench posthog catalog --label L [--range-only]` for an
  existing materialized load, and share its measurement code with
  `catalog-seed --ephemeral`. Use the same tenants/topology and issue-0014
  before/after arms. Report rows/bytes and latency, but do not call 68 parts a
  saturation test.
- [ ] **Step 5: Verify and commit.**

```bash
cargo test -p ukiel-e2e --bin bench posthog::run
cargo test -p ukiel-e2e --test posthog_synth_test -- --ignored --nocapture
cargo fmt --check
cargo clippy -p ukiel-e2e --bin bench -- -D warnings
git commit -m "bench: run scoped PostHog queries over production-shaped data"
```

---

### Task 6: Baseline, runbook, and roadmap close-out

**Files:**

- Modify: `bench/README.md`
- Modify: `docs/prod-info/README.md`
- Create: `docs/notes/2026-07-14-synthetic-posthog-baseline.md`
- Modify: `docs/superpowers/plans/2026-07-05-ukiel-v1-roadmap.md`
- Modify: this plan

- [ ] **Step 1: Full verification.**

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
make test
```

- [ ] **Step 2: On a clean stack run baseline, one shard.** Record machine/SHA,
  all digests, generation bytes/time, load time, fidelity pairs, catalog A/B,
  scoped/raw results, and whether issue 0014 removes overfetch before SQLx.
- [ ] **Step 3: Run smoke with `--shards 10` in catalog-only mode.** Candidate
  counts add while density/overfetch distributions stay stable. Materializing
  10x data is not an acceptance gate.
- [ ] **Step 4: Write runbook/limitations.** Include lifecycle, gitignored paths,
  exact commands, properties-as-utf8, replica caveat, and why ClickHouse
  bytes/levels are not copied.
- [ ] **Step 5: Mark row 45 executed with measured conclusions and commit.**

```bash
git add bench/README.md docs/prod-info/README.md docs/notes/2026-07-14-synthetic-posthog-baseline.md \
  docs/superpowers/plans/2026-07-05-ukiel-v1-roadmap.md \
  docs/superpowers/plans/2026-07-14-ukiel-synthetic-posthog.md
git commit -m "bench: record production-shaped synthetic PostHog baseline"
```

## Final acceptance

Plan 45 is complete only when:

- a fresh checkout compiles the committed profile into the same manifest;
- baseline contains real Parquet objects and truthful catalog metadata;
- generated distributions pass the pinned fidelity gates;
- scoped Ukiel results equal raw DataFusion for every query/class;
- catalog range/filter/exact counts are recorded together;
- no source byte total, ClickHouse level, or invented value distribution is
  presented as measured Ukiel production behavior; and
- another operator can reproduce the baseline from the runbook alone.

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

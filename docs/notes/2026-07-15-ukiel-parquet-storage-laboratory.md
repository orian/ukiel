# Parquet Storage Laboratory — interpretation note (Plan 47)

**Status correction (audit 2026-07-15):** This is a historical interpretation of the first
implementation and smoke run, not proof that the laboratory is measurement-ready. A later
code audit found that probes are not compiled, skip sidecars are not applied to scan plans,
the named local/object-store modes both preload data into memory, I/O/provenance are
incomplete, the noise calculation pools unlike queries, and orchestration needs fail-fast
and safe-publication fixes. Plan 47 now makes Tasks 47A–47F and a converged-L1+ smoke-v2
mandatory before the 30M event or 10M ClickBench runs. Statements below about what the
tooling “guarantees” or can “answer” describe the original implementation intent and are
superseded where the audit disagrees.

This note is deliberately honest about that boundary: per the plan, smoke output carries
**no performance conclusion**, and the event baseline is *required* for any published claim.

## What was built and proven

Seven crates and an orchestration layer, each with focused tests (and the offline tools
installable standalone):

| component | guarantee proven by test |
|---|---|
| `parquet-lab-contract` | every versioned manifest round-trips, fails closed on an unknown version, binds by digest, and refuses absolute/duplicate paths, incomplete resolved properties, and unknown index kinds |
| `parquet-lab-integrity` | `logical-row-multiset/v1` fingerprints `Int32`/`Int64`/timestamp/`Utf8`/`Utf8View` equal under the declared logical type, and fails closed on a lossy or unsupported cast (golden-pinned) |
| `parquet-lab-snapshot` | `from-ukiel` freezes a **converged** load only, cross-checks the physical `row-multiset/v1` against the plan-46 receipt, and publishes atomically — validated against a real Postgres + the real in-harness compactor |
| `parquet-census` | reads codec, encodings actually written, dictionary/page/Bloom presence, and truncation **from footers**, with `scanned` NDV where the footer cannot; reads both snapshot and variant manifests |
| `parquet-rewrite` | publishes a variant only after the logical fingerprint is unchanged; reads resolved properties back from footers; refuses overflowing/float/date-guess projections |
| `parquet-lab-bench` | proves exact, **encoding-independent** result equivalence before timing; cold/warm timing; DataFusion plan+metrics; truthful per-query `ObjectStore` I/O accounting; reader A/B flags recorded |
| `parquet-skip-index` | three conservative prototypes with a property-tested **no-false-negative** guarantee (`NoMatch` ⇒ full scan finds zero) over thousands of random groups; digest-bound sidecars |

The orchestrator `bench/parquet-lab.sh` runs one block (census + bench of the product
control, then rewrite → census → bench per variant), and `bench/parquet-lab-analyze.py`
derives the noise band and dominated/Pareto/workload-specific labels from the immutable raw
reports.

### Two real bugs the smoke run caught (now fixed + regression-tested)

1. A high-cardinality string group-by came back **dictionary-encoded** from one artifact and
   plain `Utf8` from another — identical logical answer, different Arrow IPC bytes — which
   falsely failed the equivalence gate. The result digest now canonicalizes encoding
   (dictionaries decoded, views widened, metadata/nullability stripped). Locked by
   `compare::tests::encoding_does_not_change_the_digest`.
2. `parquet-census` could not read a variant manifest (only a snapshot). It now reads both.

## Smoke validation (NOT a storage conclusion)

A real prod-synth smoke fixture (8000 rows, 200 tenants, staged as 14 L0 files with the
actual production schema — `team_id`, `timestamp`, `event`, `distinct_id`, `mat_$current_url`,
`mat_$lib`, …) was snapshotted with `from-files`, a suite compiled from the six real
prod-synth query classes, and all five blocks (17 variants) run through the full pipeline.
Every variant preserved the logical fingerprint, every query answer matched the control, and
census confirmed the requested properties.

The analyzer reported every ZSTD/page/type variant as a "Pareto candidate" at roughly −37%
size. **This is an artifact, not a result.** The smoke control is the *L0* bytes, which are
`LZ4_RAW` (the L0 lifetime codec), so every ZSTD variant "wins" ~37% purely by re-encoding
L0 as L1. This is exactly why the plan mandates the control be the **final compacted
read-many parts** (`from-ukiel` over converged L1+ objects), never L0 or a fresh re-encode.
The smoke run proves the machinery; it cannot and does not rank storage choices.

## The ten questions

The tooling can now *answer* each question once the event baseline is snapshotted; the smoke
run does not answer any of them. Marked ✅ = tooling ready + method validated, ⏳ = needs the
real 30M event baseline / 10M ClickBench run.

1. **Where do bytes live?** ✅ `parquet-census` reports per-column-chunk compressed/uncompressed
   bytes, per-column fractions, and metadata overhead. ⏳ needs the real parts.
2. **Dictionaries that do/don't help?** ✅ census reads resolved dictionary use + `--scan-columns`
   NDV; Block B toggles it. ⏳.
3. **Smaller pages/page indices vs their overhead?** ✅ Block A + object-store I/O accounting
   (footer/page/Bloom reads counted separately). ⏳.
4. **Is 128k key-aligned row-group policy still Pareto?** ✅ Block A (32k/128k/512k) + the
   selective/broad probes. ⏳.
5. **Which narrow int/date/timestamp types save bytes losslessly?** ✅ Block D — validated on
   smoke that `team_id→Int32` and `timestamp→Timestamp(ms)` preserve the logical fingerprint
   **and** every query answer. ⏳ for the byte-savings-after-compression magnitude.
6. **Per-column encoding/compression wins?** ✅ Blocks B + C. ⏳.
7. **Do native Bloom filters help a measured equality predicate?** ✅ Block E + reader
   `bloom_filter_on_read` A/B + separate Bloom-byte accounting. ⏳ (needs an equality probe at
   a realized selectivity band — see below).
8. **Which residual predicate justifies a custom skip index?** ✅ `parquet-skip-index` +
   `--skip-manifest` price a bound sidecar against native pruning. ⏳ — **no custom index is
   justified yet**: the residual only exists after Blocks A–E on the real data, which have not
   run. This is a successful "not yet demonstrated," not an incomplete task.
9. **Does a combined candidate keep the individual wins?** ✅ the runner + leave-one-out method.
   ⏳.
10. **Event-specific vs ClickBench-reproducing?** ✅ `from-files` + a ClickBench suite compile.
    ⏳ needs the 10M slice.

### Probes and selectivity bands

The suite contract carries declarative probes (equality/range/prefix/substring/`IS NULL`/
narrow+wide projection) with an `observed_count` the compiler fills before timing. The smoke
run used the six named query classes only; the event baseline should compile probes at
measured selectivity bands (the compiler reports the observed count so a probe is dropped
when the column cannot realize its band).

## What remains: the measurement phase (operator-run)

Per the dependency-and-execution order, before the publishable matrix:

1. Stage one verified **30M-row** prod-synth L0 artifact; compact it through the existing
   plan-46 pipeline into **two** controls — `packed` and `64 MiB` size-targeted — recording
   receipts and part-shape reports. Confirm whether the 64 MiB arm actually cut (≈1.4M
   rows/day crosses 64 MiB); if it did not trigger, **report that fact and use the packed
   output** rather than inventing a smaller target after seeing the data.
2. `parquet-lab-snapshot from-ukiel` each converged control into an immutable snapshot.
3. Compile the prod-synth suite; run Blocks A–E **twice, interleaved**, in `object-store`
   mode, rerunning the product control in every block.
4. Only if Blocks A–E leave a residual predicate expensive after native features, build the
   Block F sidecar (`bench/config/parquet-lab/skip/*.toml`) and price it; otherwise record
   "no custom index justified."
5. Run the 10M ClickBench confirmation over the adapted SQL; classify disagreements as
   event-specific.
6. Run the combined candidate + leave-one-out checks; cap the published candidates at
   balanced / scan-heavy / selective.

Illustrative commands (the smoke run above is the same sequence at 8000 rows):

```bash
parquet-lab-snapshot from-ukiel --receipt R.json --config C.toml --output SNAP
parquet-lab-bench compile --manifest SNAP/manifest.json --kind prod-synth \
  --sql bench/queries/prod-synth/queries.sql --suite-out suite.json
for blk in pages encodings compression types blooms; do
  bench/parquet-lab.sh --snapshot SNAP --suite suite.json \
    --block bench/config/parquet-lab/$blk --out runs/rep1/$blk --mode object-store --warm 5 --replace
done
# ... a second interleaved repetition into runs/rep2, then:
bench/parquet-lab-analyze.py --run runs/rep1/compression
```

Raw JSON must be stored under the benchmark-results location and is immutable; this note is
the derived interpretation and must be regenerated from those raw reports once they exist.

## A storage ceiling is not an end-to-end claim

Block D measures the *ceiling* for roadmap row 36 (narrow production types). The tooling
proves the conversions are lossless and answer-preserving; it does **not** modify
`ukiel-core::schema` or claim the end-to-end migration/evolution cost is free. Any candidate
worth that cost becomes a separate implementation plan, fed by this laboratory's evidence.

## Remediation status (Tasks 47A–47F) and smoke-v2

The audit-driven remediation is largely implemented and tested:

- **47A** run contracts (`ukiel-parquet-probes/v1`, `ukiel-parquet-store/v1`,
  `ukiel-parquet-run-set/v1`), extended report identity, credentials-never-serialized golden
  tests, and CLI flags that fail closed until their feature lands.
- **47B** real selectivity-probe compilation (typed literals + observed selectivity frozen
  against the control; unsupported probes recorded with a reason; `ordered`/`multiset`
  answer semantics) — validated compiling seven probes against real prod-synth data.
- **47C** sidecars applied to actual scans: a shared pure `parquet-skip-index-core`, a
  per-file `ParquetAccessPlan` that omits only `NoMatch` groups, and a spy-store proof that
  a skipped row group's data range is never fetched.
- **47D** the `parquet-lab-store` publisher/verifier (local + S3/MinIO) and three real bench
  modes (`memory`/`local`/`object-store`) that no longer preload whole files.
- **47E** a fail-fast, atomic, marker-guarded `parquet-lab.sh` and a seeded run-set planner.
- **47F** an analyzer that consumes a *complete run set* and computes noise on like-for-like
  suite totals (never pooling unlike queries), with synthetic unit tests.

**Smoke-v2 (converged L1+ + MinIO + applied sidecar, all blocks twice) remains the
operator-run completion gate**, as does the 30M event baseline and 10M ClickBench. The
historical smoke run in this note used L0 `from-files` bytes as its control and therefore
carries **no** storage conclusion; smoke-v2 must snapshot converged L1+ parts via
`from-ukiel`. See the plan's "Deviation (partial)" notes on 47D for the remaining
returned-byte accounting, range classification, and the `--publishable` provenance gate.

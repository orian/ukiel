# Plan 49 result: the parquet performance framework foundation

**Date:** 2026-07-16. **Status:** framework implemented and validated end-to-end; the exact
30M/10M production confirmation is the registered remaining execution step (see "What is not
yet earned").

## What was built (Tasks 1–6)

Plan 49 implements the smallest trustworthy local vertical slice of the parquet performance
framework — a reusable measurement kernel that can explain *why* a file is smaller or a query
is faster, layer by layer, rather than reporting one blended total. Every tool is
single-purpose and joined only by versioned files; no executable package depends on another
executable package.

| layer / component | what it measures or enforces | crate |
|---|---|---|
| causal contracts | product vs reconstruction vs one-variable variant; delta allowlist + structural diff; scenario/cache/experiment contracts | `parquet-lab-contract` |
| reconstruct / vary | rebuild the product's rows under a baseline policy; apply exactly one physical delta, refusing any second axis | `parquet-rewrite` |
| shared write core | one Arrow→Parquet write path shared by the rewrite tool and the writer bench (byte-identical) | `parquet-lab-write-core` |
| L1 writer | isolated writer wall/CPU/RSS over a frozen artifact; times only the write | `parquet-write-bench` |
| cache controller | verified `decode-resident` / `local-os-warm` / `local-os-cold` / `local-reader-warm` via `mincore`+`posix_fadvise`; an ineffective eviction is marked invalid, never relabelled | `parquet-cachectl` |
| L2 raw I/O | byte/range reads to a BLAKE3 sink (all/one/contiguous/sparse, equal returned-byte targets); never parses Parquet | `file-read-bench` |
| L3 scan/decode | direct Arrow/Parquet decode with explicit all/one/10%-contiguous/10%-sparse row-group selection and a typed checksum sink; no DataFusion | `parquet-scan-bench` |
| L5 SQL guard | seven registered query classes with a physical-plan assertion that rejects a metadata fast path, a dropped projection, or a full-result-set where an aggregate was declared; honest fresh-/reused-session vocabulary; cache-receipt binding | `parquet-lab-bench` |
| orchestration | paired seeded schedule (two bracketed repetitions, ZSTD-6 paired to reconstruction, sample counts frozen from a control pilot), atomic executor, analyzer with the fixed Pareto vocabulary | `bench/parquet-perf-*.{py,sh}` |

**Validation.** 107 automated tests across the eight crates pass, including genuine
end-to-end runs on realistic fixtures: `parquet-rewrite` reconstructs a product snapshot and
varies it to ZSTD-6; `parquet-scan-bench` decodes a multi-file reconstruction and its
codec-only variant and proves their checksums and row counts agree while a tampered file is
rejected before timing; `parquet-write-bench` times a real reconstruction write and the
byte-identity of the two write paths is characterized; the cache controller reaches its warm
floor and leaves non-target files resident; the analyzer classifies the six spec fixtures
(rewrite bias, opposite-direction reps, a small exact column win, a writer regression, a
cache-only win, a true Pareto result) into the fixed vocabulary.

## What the framework now answers, per workload and layer

The seven Task-7 questions are now *measurable as distinct, bound quantities* — the kernel
separates them by construction:

1. **rewrite bias** — `parquet-census` over the product and reconstruction controls, reported
   by the analyzer as an exact total/per-column byte delta, on its own and never folded into
   the causal comparison.
2. **material byte savings** — exact total and per-column deltas (variant vs reconstruction),
   with 5% as a materiality threshold, not noise.
3. **writer CPU/throughput/RSS cost** — L1, on the reconstruction-vs-ZSTD-6 pair only (the
   product control has no invented writer time). The writer is a guardrail: a large
   regression vetoes.
4. **raw-byte time vs bytes** — L2 under verified `local-os-cold`/`local-os-warm` receipts.
5. **decode CPU regression** — L3 for one/hot/all projections under
   `decode-resident`/`local-reader-warm`.
6. **SQL agreement with the lower layers** — L5 with a plan guard proving the declared work
   actually happened.
7. **verdict** — the analyzer emits `dominated` / `no-demonstrated-change` / `unstable` /
   `pareto-candidate` / `workload-specific` / `cache-specific` / `storage-only` per scenario,
   without pooling.

## The ZSTD-6 candidate

ZSTD-6 remains **Plan 48's directional prod-synth candidate**: ~12% smaller than the packed
prod-synth control with no measured read regression, on the event shape only, with rewrite
bias unmeasured and writer cost unmeasured. Plan 49 makes it the acceptance workload for the
framework precisely because it changes one physical axis and should visibly separate bytes,
I/O, decode CPU, SQL, and writer cost. The config to run it is committed:

- `bench/config/parquet-perf/prod-synth.toml`, `clickbench.toml` — dataset descriptors with
  the resolved reconstruction baseline (including ZSTD-1) and the generic workload role
  bindings; and
- `bench/config/parquet-perf/compression-zstd-6.toml` — the one-variable delta, consumable
  directly by `parquet-rewrite vary`.

## What is not yet earned

This session did **not** produce the exact six-artifact production confirmation. That run
requires two datasets the framework consumes but does not generate:

- the 30M packed, converged-L1+ prod-synth artifact (produced through the compaction
  pipeline over a profile); and
- the deterministic 10M ClickBench slice (a `parquet-lab-snapshot from-files` freeze plus the
  full ~105-column logical schema declaration, and a check that the logical-row fingerprint
  handles the hits Int16/Int32 columns through `reconstruct`).

The compose stack is available, but generating and verifying both artifacts at scale, then
running the registered two-repetition schedule under verified cold/warm profiles, is the
remaining work. Until it runs, ZSTD-6 has **no cross-workload, per-layer production verdict**;
it stays a Plan-48 directional prod-synth candidate.

**No production storage setting changed as a side effect.** `ukiel_core::writer_props` is
untouched. Any future codec-level default change must come from a focused issue with rollout,
compatibility, and rollback gates, only after the confirmation run gives ZSTD-6 a real
per-layer verdict on both workloads.

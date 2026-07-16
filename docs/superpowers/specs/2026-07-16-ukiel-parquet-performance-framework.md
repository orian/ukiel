# Ukiel Parquet Performance Improvement Framework

**Status:** Design draft, 2026-07-16.

This document defines how Ukiel should investigate Parquet internals: physical
types, encodings, dictionaries, compression, row groups, pages, native indices,
and experimental skip structures. It replaces “run a query suite and compare
one total” with a layered framework that can explain *why* a file is smaller or
a query is faster.

The framework is reusable. A storage experiment supplies an immutable dataset,
a control, one or more one-variable variants, and a workload binding. The
framework supplies the measurements, cache profiles, correctness gates,
interleaving, analysis, and evidence contracts.

## Objective and priorities

Ukiel can scale write work horizontally, so raw writer throughput is a guardrail
rather than the principal optimization target. The primary scorecard is:

1. total file bytes;
2. per-column bytes, including dictionaries and index metadata;
3. Parquet read/decode speed for one column, useful column sets, and all columns;
4. SQL query speed over full scans, selected row groups, and filtered rows; and
5. the same read/query shapes under explicit cache states.

Writer wall time, CPU, rows/s, bytes/s, and peak memory are always recorded. A
large writer regression can veto a candidate, but a small writer win cannot
compensate for larger files or slower reads.

There is no single weighted “Parquet score.” Results form a Pareto frontier:
bytes, read throughput, query latency, and the writer guardrail remain visible
as separate dimensions.

## Core principle: measure layers, not one blended latency

```text
logical Arrow rows
    -> Parquet writer
    -> physical file and metadata
    -> raw byte I/O
    -> Parquet page decode
    -> projection / row-group selection / predicate filtering
    -> SQL operators
    -> result materialization
```

A variant is measured at every relevant boundary. If SQL improves but decode
does not, the win is probably pruning or planning. If decode improves but SQL
does not, later operators or result materialization dominate. If warm reads
improve but cold reads do not, the choice may be CPU-friendly but not
I/O-friendly. The framework must retain those distinctions.

## Terminology and immutable identities

- **Dataset artifact:** immutable logical rows plus schema, sort/packing keys,
  column profile, file membership, and a logical fingerprint.
- **Product control:** exact bytes written by Ukiel. It answers “what ships?”
  and is never rewritten.
- **Reconstruction control:** the same logical rows rewritten through the
  laboratory writer with properties resolved to match the experiment baseline.
  It reveals rewrite-tool, row-group-boundary, and library-default bias.
- **Variant:** a reconstruction-control child declaring an exact allowlist of
  changed properties.
- **Scenario:** one layer, projection, selection/predicate, backend, cache
  profile, result sink, and sample policy.
- **Workload binding:** maps generic roles such as `low_cardinality_key` or
  `wide_text` onto actual dataset columns.
- **Run set:** the complete seeded schedule and the digests of every artifact,
  scenario, sample, census, and report.

Every report binds dataset, product control, reconstruction control, variant
delta, workload, backend, cache profile, host, build, and schedule identities.
Raw samples are immutable.

## Two controls are mandatory

Storage experiments need two different comparisons:

| comparison | question |
|---|---|
| product control vs reconstruction control | did rewriting itself change page/row-group boundaries, defaults, bytes, or timing? |
| reconstruction control vs one-variable variant | what is the causal effect of the requested physical change? |

The original product bytes remain the ecological reference, but causal claims
use the reconstruction control. A variant must not repeat global settings such
as `compression = "zstd(3)"` merely to be explicit; doing so can accidentally
change two axes when the product uses another level.

Variant specifications therefore contain a delta, not a second complete writer
configuration:

```toml
version = "ukiel-parquet-variant-delta/v1"
label = "team-int32"
parent_manifest_digest = "...reconstruction control..."
allowed_changes = ["columns.team_id.physical_type"]

[changes.columns.team_id]
physical_type = "int32"
```

The resolver writes the full resolved configuration into the manifest. Before
timing, a validator compares control and variant manifests and refuses any
physical difference outside `allowed_changes`. Footer census then proves the
requested difference actually exists.

If the reconstruction control differs materially from product bytes, report
that as rewrite bias. Do not silently compare variants to the product control.

## Measurement layers

### L0: physical census — no timing

For the product control, reconstruction control, and every variant, record:

- total file bytes and bytes/row;
- compressed and uncompressed data bytes;
- footer and other metadata bytes;
- dictionary, page-index, offset-index, Bloom, and custom-index bytes;
- file, row-group, page, and column-chunk counts;
- per-column physical/logical type, codec and level requested, encodings actually
  used, dictionary fallback, null count, NDV provenance, min/max availability,
  value-width percentiles, compressed bytes, and uncompressed bytes; and
- per-row-group equivalents needed to explain selection and pruning.

Codec levels are not encoded in ordinary Parquet footers, so the resolved writer
manifest is the authority for the requested level; footer census proves the
codec family and other observable properties.

Primary L0 outputs are total bytes and per-column bytes. A whole-file percentage
must never be attributed to one column without also showing that column's own
byte delta.

### L1: raw writer

Measure only Arrow-to-Parquet writing. Dataset loading, input validation,
logical fingerprinting, footer census, output hashing, and publication occur
outside the timed interval.

Record per sample:

- rows and logical input bytes;
- wall, user CPU, and system CPU time;
- rows/s and logical MiB/s;
- bytes written and output/input ratio;
- peak RSS and allocation/spill counters when observable; and
- resolved writer properties.

The writer is a guardrail. A candidate that harms compaction throughput enough
to violate the registered compaction/freshness budget is rejected even if reads
improve. Otherwise writer speed does not participate in candidate ranking.

### L2: raw byte-read baseline

Read immutable file bytes without parsing Parquet and feed them to a checksum
sink. This establishes the backend/cache ceiling and prevents a codec result
from being mistaken for a disk or network fluctuation.

Scenarios:

- sequential read of all files;
- one complete file;
- registered contiguous ranges; and
- registered sparse ranges with the same returned-byte total.

Record requested and returned bytes, requests/syscalls, ranges, wall/CPU time,
MiB/s, and cache residency. Local and remote backends use the same logical
ranges.

### L3: Parquet scan and decode without SQL

Use the pinned Arrow/Parquet reader directly. No DataFusion logical plan,
catalog, SQL optimizer, aggregation, or result serialization is present.
Decoded values feed a deterministic checksum/count sink so the compiler and
reader cannot discard work and large result arrays do not dominate timing.

Core projections use column roles rather than hard-coded names:

| projection | purpose |
|---|---|
| one fixed-width key | physical width, dictionary/RLE, filter-key decode |
| one monotonic numeric/time column | delta/plain encoding and range pruning |
| one high-cardinality string | dictionary fallback, byte-array encoding, decompression |
| one wide text/JSON-like column | compression and memory pressure |
| hot column set | realistic narrow product query |
| all columns | full-row scan ceiling |

Core row-group selections:

| selection | meaning |
|---|---|
| all | full scan |
| one | exact single-row-group access |
| 10% contiguous | range-like access |
| 10% sparse | dashboard/key-like scattered access |
| zero | metadata/footer-only negative control |

Selection is explicit through row-group access plans; it does not depend on a
predicate. This measures the value of reading fewer row groups separately from
the ability of statistics or an index to discover them.

Record rows and values decoded, compressed/requested/returned bytes, row groups
and pages opened, batches, wall/CPU time, rows/s, values/s, MiB/s, and peak
memory.

### L4: pruning and filtering

Now add predicates while keeping SQL operators out. Every predicate has a
realized match count and one of these shapes:

- **aligned:** matches are concentrated in prunable row groups;
- **scattered:** the same match count is spread across every row group;
- **absent:** zero matches, testing metadata/index rejection;
- **all:** every row matches, a negative control; and
- **unsupported:** the native/index layer must conservatively keep rows.

Registered selectivity bands are 0%, approximately 0.1%, 1%, 10%, and 100%
where the dataset can realize them. Compare:

1. explicit access-plan selection;
2. native row-group statistics;
3. page statistics/index;
4. native Bloom where applicable; and
5. a custom sidecar only after a residual remains.

This layer reports discovery/pruning cost separately from data read, decode,
predicate evaluation, and output materialization. `NoMatch` is the only custom
decision allowed to remove a row group.

### L5: SQL execution over the file

SQL scenarios exist to reveal interactions with DataFusion, not to replace L2–L4.
Each query declares required columns, expected selection/predicate shape,
expected result digest, and a physical-plan assertion proving the optimizer did
not remove the work the scenario intended to measure.

The core query pack contains:

| query class | required behavior |
|---|---|
| metadata aggregate | `count(*)`/metadata fast path; explicitly labelled, never called a scan |
| full numeric scan | force decode of one numeric/time column into an aggregate checksum |
| full string scan | force decode of one high-cardinality string column |
| hot-set scan | consume every hot-set column without returning millions of rows |
| all-column scan | checksum/aggregate every column; plan must project all columns |
| aligned selective, narrow output | prunable predicate plus one output/aggregate column |
| aligned selective, wide output | same predicate and matches, all relevant columns |
| scattered selective | same match count as aligned but touches every row group |
| absent equality/range | zero-result pruning cost |
| low-cardinality group | dictionary/decode plus aggregation |
| high-cardinality group | string decode, hash and memory pressure |
| ordered TopK | decode plus partial sort/early-stop interaction |

Large projections use checksum aggregates or a benchmark-native sink. Returning
1.6 million rows and hashing them is a result-materialization benchmark, not a
clean scan benchmark. A separate materialization scenario may measure that cost
when relevant.

Record total latency plus planning, scan/decode, filter, operator, and result-
sink time where observable; physical plan; input/output rows; row groups/pages
pruned by reason; bytes; spill; peak memory; and exact result digest.

## Cache is an explicit scenario dimension

“Cold” and “warm” are invalid labels without naming which cache is cold.
The framework registers these profiles:

| profile | process/session | file/object data | purpose |
|---|---|---|---|
| `decode-resident` | fresh reader | bytes preloaded in memory | codec/decode CPU floor; not an I/O result |
| `local-os-cold` | fresh process and reader | target files evicted from OS page cache | device + metadata + decode first touch |
| `local-os-warm` | fresh process and reader | target file ranges resident | warm filesystem, cold reader metadata |
| `local-reader-warm` | reused process/session | target ranges resident | repeated-query steady state |
| `remote-direct` | fresh client/session | application data cache disabled | real request/returned-byte/network cost |
| `remote-cache-cold` | fresh client/session | Ukiel cache empty | miss/fill cost |
| `remote-cache-warm` | reused or fresh session as declared | Ukiel cache primed and verified | cache-hit path |

For local cold runs, evict only the benchmark files with
`posix_fadvise(POSIX_FADV_DONTNEED)` after closing them; do not require global
`drop_caches`. Use `mincore` or an equivalent residency probe before and after:
a cold sample is valid only below the registered residency ceiling, and a warm
sample only above the registered floor. Defaults are ≤10% and ≥90% resident.
Unsupported or ineffective eviction makes the cold profile unavailable; it
must not silently become warm.

For reader warmness, distinguish a fresh process, fresh DataFusion session, and
reused session. For object stores, distinguish client/TLS connection reuse from
Ukiel's object cache. Each report records both.

Do not run the full Cartesian product. Discovery uses `decode-resident`,
`local-os-warm`, and `local-reader-warm`. Only surviving candidates add
`local-os-cold`; remote/cache profiles are confirmation gates before remote or
product-cache claims.

## Dataset and workload contracts

Every dataset manifest records the distributions that influence Parquet:

- null fraction;
- min/max and numeric width;
- NDV and top-frequency mass;
- string/binary length percentiles;
- run-length/repetition profile;
- sort correlation and monotonicity;
- per-row-group distribution and selectivity realizability; and
- entropy/compressibility provenance where measured.

The first reusable workload pack should bind these generic roles:

```text
low_cardinality_key
monotonic_time
low_cardinality_string
high_cardinality_string
wide_text
nullable_column
hot_columns[]
all_columns[]
```

Use three evidence stages:

1. **controlled shape fixture:** deterministic columns deliberately exercising
   each role and aligned/scattered predicates;
2. **prod-synth:** production-derived tenant/part geometry with explicitly
   synthetic values; and
3. **bounded public OLAP confirmation:** ClickBench or another versioned public
   dataset.

Controlled data explains mechanisms. Prod-synth tests Ukiel-shaped geometry.
Public data tests whether a candidate generalizes. None substitutes for the
others.

## Sample and execution discipline

- Run release binaries with build flags recorded.
- Pin dataset, artifacts, scenarios, seed, host, filesystem/backend, cache
  procedure, and reader flags before timing.
- Use at least two seeded, independently shuffled repetitions. Bracket each
  repetition with product and reconstruction controls.
- Pair each variant with its reconstruction control inside the repetition;
  analyze paired ratios, not unrelated pooled samples.
- The control determines the sample count before variant results are visible.
  For warm scenarios choose the larger of seven samples and the count needed
  to exceed three seconds of measured control time, capped by a pre-registered
  maximum. Use the same count for every paired variant. Use at least three
  separately prepared cold samples.
- Record every raw sample. Report median, MAD, p95 where the sample count makes
  it meaningful, each repetition's delta, and start/end drift. Never use means
  as the headline.
- Time only the declared layer. Setup, cache preparation, validation, census,
  result verification, and report writing stay outside the interval.
- Interleave variants; never run every control first and every candidate later.
- One changed answer, logical fingerprint, schema contract, or false-negative
  prune rejects the variant regardless of speed.

## Analysis and decision rules

Size is exact; 5% is a materiality threshold, not statistical noise. Timing uses
the larger of 5% and the registered control noise band.

Per-column size changes report both percentage and absolute contribution. A
50% saving on a column occupying 0.04% of the file is useful evidence about the
column but not a whole-file candidate.

A candidate must:

- preserve correctness absolutely;
- change a primary dimension outside its threshold;
- move in the same direction in both repetitions;
- survive the appropriate cache profile rather than only an accidental state;
- show no unexplained primary regression outside threshold; and
- stay within the registered writer/compaction guardrail.

Classifications are:

- `dominated`;
- `no-demonstrated-change`;
- `unstable`;
- `pareto-candidate`;
- `workload-specific`;
- `cache-specific`; or
- `storage-only` when a physical saving does not survive decode/query layers.

The report may name at most three survivors: balanced, full-scan, and selective.
A result with no survivor is complete.

## Framework components and boundaries

Keep tools single-purpose and join them with versioned files:

| component | responsibility |
|---|---|
| shared contracts | dataset, artifact, variant delta, workload, scenario, cache profile, run set, and reports |
| `parquet-census` | L0 physical accounting only |
| writer core + `parquet-write-bench` | L1 timed Arrow-to-Parquet writing; validation after timing |
| `file-read-bench` | L2 raw byte/range I/O with a checksum sink; never parses Parquet |
| `parquet-scan-bench` | L3/L4 Parquet decode, row-group selection, and pruning without SQL |
| `parquet-query-bench` | L5 SQL and physical-plan assertions; evolve or replace the current laboratory bench |
| cache controller | prepare and verify local/object cache profiles; performs no benchmark logic |
| run-set orchestrator | execute the registered paired schedule; performs no writer/reader/query logic |
| analyzer | validate bindings and derive paired ratios/Pareto classifications |

No executable depends on another executable or calls another command handler.
Pure writer/reader libraries may be shared. The query tool remains read-only;
object publication and cache mutation are separate tools.

## Evidence layout

```text
bench/results/parquet-perf/<experiment-id>/
  experiment.json
  dataset.json
  product-control/
  reconstruction-control/
  variants/<label>/
  run-set-planned.json
  run-set-complete.json
  samples/<scenario-id>/*.json
  censuses/*.json
  sha256sums.txt
  analysis.json
  analysis.md
```

The complete run set binds censuses and raw sample reports, not only aggregate
query reports. Required provenance includes git SHA/dirty state, target CPU and
RUSTFLAGS, dependency versions, CPU/RAM/kernel, filesystem/device or object-store
identity, cache preparation/residency, and scenario digests. A result with an
unknown git SHA or empty host identity is exploratory, never publishable.

## Experiment funnel

1. Freeze dataset and workload identities.
2. Census exact product bytes.
3. Build and measure the reconstruction control; quantify rewrite bias.
4. Resolve one-variable deltas and refuse confounded variants.
5. Run L0 and the L1 writer guardrail.
6. Discover mechanisms with L2–L4 under resident/warm-local profiles.
7. Run the small L5 SQL edge pack.
8. Discard dominated/no-change variants.
9. Confirm only survivors under verified local-cold and relevant cache profiles.
10. Confirm only survivors on prod-synth/public OLAP or real object storage when
    the intended claim requires it.
11. File a production issue only after the candidate's benefit, regression,
    operational cost, compatibility, and rollback gate are known.

## Anti-patterns this framework rejects

- Comparing a ZSTD-3 type/page arm directly to ZSTD-1 product bytes.
- Calling “physical Int32 widened to Int64 by the view” a native Int32 execution
  benchmark.
- Calling `count(*)` a full-column scan when metadata answers it.
- Returning millions of rows when the intended measurement is decode speed.
- Pooling latencies from queries with different scales into one noise sample.
- Treating fresh DataFusion metadata, OS page cache, object cache, and result
  cache as one “cold/warm” switch.
- Calling a counted local-filesystem wrapper an object-store measurement.
- Attributing whole-file movement to a tiny column without its byte census.
- Running a full combinatorial matrix before bracketing experiments find a
  survivor.
- Publishing an aggregate suite win that hides a major query-class regression.

## Initial implementation boundary

The first implementation should stop at a trustworthy local framework:

- dual controls and one-variable delta validation;
- complete physical/per-column census;
- raw writer timing;
- raw byte plus Parquet scan/decode scenarios;
- explicit all/one/contiguous/sparse row-group access;
- the core SQL edge pack;
- verified resident/local-cold/local-warm/reader-warm profiles; and
- paired run-set analysis with full provenance.

Real S3/MinIO, Ukiel object-cache cold/warm confirmation, and custom sidecars
are second-stage consumers. They must use the same scenario/result contracts,
but they should not delay fixing the local causal framework.

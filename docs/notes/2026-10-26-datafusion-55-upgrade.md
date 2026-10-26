# Dependency refresh and DataFusion 55.2.0

The runnable MV demo was committed separately as `5ff872d`. This change updates
the workspace dependencies, migrates the affected APIs, and compares the old
and new dependency sets on the same benchmark files.

## Dependency set

| Dependency | Before | After |
|---|---|---|
| DataFusion and datasource-parquet | 54.0.0 | 55.2.0, exact workspace pins |
| Arrow / Parquet | 58.3.0 | 59.3.2 |
| object_store | 0.13.2 | 0.13.2, compatible with DataFusion 55 |
| Tokio | 1.52.3 | 1.54.1 |
| reqwest, direct dependency | 0.12.28 | 0.13.5 |
| metrics-exporter-prometheus | 0.16.2 | 0.18.3 |
| metrics-util | 0.19.1 | 0.20.4 |
| lru | 0.18.0 | 0.19.0 |
| base64, direct dependency | 0.22.1 | 0.23.1 |
| parquet-lab-store's sha2 | 0.10.9 | 0.11.1 |

The lockfile also refreshes compatible transitive dependencies. Older reqwest,
base64, and sha2 versions remain where other dependencies require them.
Arrow/Parquet 60 and object_store 0.14 are outside DataFusion 55's dependency
family; one Arrow/Parquet 59.3.2 set is shared across the workspace. The build
and measurements use Rust 1.98.1.

Sources: [DataFusion 55.2.0 dependencies](https://docs.rs/crate/datafusion/55.2.0),
[Apache release record](https://release-catalog.apache.org/datafusion/index.html).

## Compatibility changes

- Standalone default/materialized/alias expressions now supply DataFusion's
  `PhysicalPlanningContext`, as required by `create_physical_expr` in 55.
  Execution-time coercion and clock simplification still run for each attempt.
- The small `ukiel-io` adapter implements Parquet range and suffix reads over
  `ObjectStore`. Compaction and MV workers use it in place of the deprecated
  `ParquetObjectReader`. It preserves concrete storage errors through the
  Parquet error chain, which MV completion reconciliation relies on.
- Streaming compaction passes `object_store::buffered::BufWriter` directly to
  `AsyncArrowWriter`, replacing the deprecated `ParquetObjectWriter` wrapper.
- The laboratory writer uses `set_column_bloom_filter_max_ndv` in place of
  its deprecated alias. Tool reports record the new engine/writer versions.
- reqwest's TLS feature is now `rustls`; URL query serialization is explicitly
  enabled with its new `query` feature.

The provider's conditional ordering declaration is retained. The existing
ordered-result and physical-plan regression tests pass on 55.2.0; removing the
ordering workaround remains a separate controlled optimization.

## Correctness checks

`cargo test --workspace -- --test-threads=2` passed: **696 passed, 18 ignored**.
This includes query isolation, row-group pruning, footer caching, write/read
round trips, native clock expressions, MV publication/recovery/GC races,
compaction/deletion, and the offline tooling boundaries. The new I/O adapter has
explicit page-index/round-trip and typed-NotFound tests. Workspace Clippy with
all targets and fault injection enabled passes with warnings denied.

The ignored S12 test was then run explicitly against the isolated PostgreSQL,
Kafka, and **MinIO** stack, with the local-store override removed: **1 passed**
in 3.78 s. It covers active-active MV publication, default `now()` storage,
and preservation of results and stored clock values through compaction.

## Benchmark protocol

Raw evidence is under
`bench/results/deps-20261026-1793015882938/` (gitignored). It includes both release
binaries, before/after manifests and lockfiles, the registry audit, environment,
fixture hashes, execution schedule, and per-command logs. Suite JSON reports
retain unique labels under `bench/results/`; historical timings are preserved.

- Baseline source: `5ff872ddc2df9a32fdfcc2f1e72855caaa112566`, original lockfile.
- Both binaries: `--release`, workspace ThinLTO, empty `RUSTFLAGS`, Rust 1.98.1,
  24 logical CPUs, Linux x86-64. Compilation finishes before measurements.
- Dedicated Docker project `ukiel-dep-bench`, using the demo compose services;
  catalog and Kafka are isolated from the root development stack. A benchmark
  override enables Kafka topic auto-creation, which the existing harness requires.
  The first write attempt failed at topic readiness before producing data;
  its log is retained, and all measured write attempts use fresh retry databases.
- ClickBench: **99,997,497 rows**, the full cached 100-file source dataset loaded
  once by the baseline writer. The 25-column Ukiel fixture has **1,102 live
  Parquet files / 7,943,862,055 bytes**. Both Ukiel and bare DataFusion read those
  exact files. There is no compaction between arms.
- Tenant suite: the same row population in its 14-column fixture, **1,102 files /
  6,693,775,066 bytes**, with the same heavy/median/light census for both binaries.
- The local-filesystem object store isolates the compute/provider effects from
  HTTP object transport. The optional object cache is disabled; default sort
  pushdown remains enabled.
- Schedule: 54/r1 -> 55/r1 -> 55/r2 -> 54/r2. Each arm runs Ukiel ClickBench,
  bare-DataFusion ClickBench, and the tenant suite sequentially. Each ClickBench
  query gets one fresh-session sample and three reused-session samples per
  repetition. Tenant scenarios use five measured iterations per repetition.
- The first Ukiel/54 sample showed substantial timing drift. An additional
  55/r3 -> 54/r3 Ukiel-only pair checks the later timing range. Original samples
  remain in the evidence and the aggregate; the follow-up does not replace them.
- “Cold” means a new engine/session/footer cache. The operating-system page
  cache is not flushed; these are not physical cold-I/O claims.
- These compare the **complete dependency refresh plus necessary API adapters**;
  the changes cannot be attributed to DataFusion alone.

The suite rejects execution errors and records result row counts. Those counts
are compared across arms; they are not a proof of every returned value. The
correctness tests above cover the query and storage contracts independently.

The write comparison uses the first cached Bluesky file, **1,000,000 events**
per repetition, with fresh databases and local stores. It follows the same
54/55/55/54 schedule, `--wave-files 1 --flush-ms 1000`, and drains compaction and
finalization to quiescence. The existing harness checks consumed offsets,
stored/mapped counts, and tenant census counts, and records the terminal layout.
Total process wall time includes setup and the post-write tenant queries; production overlaps
ingestion and background compaction, so component times are not isolated writer
microbenchmarks.

The five-query JSONBench comparison uses the first baseline Bluesky fixture
unchanged for both engines, again 54/55/55/54 with three hot samples per query.
These are the existing promoted-column adaptations, not native JSON parsing;
Plan 44's native JSON work remains separate.

## Measurements

| Workload | Before (DF 54) | After (DF 55.2.0) | Change |
|---|---:|---:|---:|
| Ukiel ClickBench, 100M rows / 43 queries | 29.551 s | 27.790 s | −6.0% |
| Bare DataFusion, identical files / 43 queries | 30.651 s | 27.576 s | −10.0% |
| JSONBench, 1M rows / 5 promoted-column queries | 73.15 ms | 71.28 ms | −2.6%, roughly flat |
| Bluesky write workload, 1M events | 8.881 s | 9.138 s | +2.9%, roughly flat |

Read totals sum each query's median across all measured hot samples: nine
samples per version for Ukiel ClickBench, six for raw ClickBench and JSONBench.
The write row is the median total process wall time over two fresh runs per
version. The traditional ClickBench hot-minimum sums are **28.236 -> 26.407 s**
for Ukiel and **29.608 -> 26.583 s** for raw DataFusion.

All 43 ClickBench and five JSONBench queries executed successfully in every
repetition, with matching result row counts across dependency sets. Ukiel
improved on 38/43 pooled query medians; raw DataFusion improved on 39/43.
Every write run consumed and stored exactly 1,000,000 events, with zero poison
events, no invariant violations, matching tenant census counts, and one final
L2 part. No backpressure was observed at this bounded write tier; it is not a
sustained-load or capacity claim.

### Timing drift and the slow query

Ukiel's first baseline was unusually slow. The additional pair confirmed that
the later timing range was stable; the initial apparent ~25% win does not
survive that check. The totals below sum each run's three-sample query medians:

| Version | Repetition 1 | Repetition 2 | Confirmation 3 |
|---|---:|---:|---:|
| DF 54 / Ukiel | 40.827 s | 29.405 s | 29.353 s |
| DF 55 / Ukiel | 30.186 s | 27.715 s | 27.479 s |
| DF 54 / raw | 31.120 s | 30.380 s | — |
| DF 55 / raw | 27.616 s | 27.597 s | — |

There is one repeatable material regression: **q25**, a filtered
`SearchPhrase` projection ordered by `EventTime` with `LIMIT 10`, moves from
**79.64 to 94.91 ms (+19.2%, about 15 ms)** through Ukiel. Bare DataFusion's
same query improves from 67.77 to 58.86 ms. Planning stays around 17–18 ms,
pointing to execution rather than catalog-planning overhead.

The existing `clickbench plan` instrument shows an additional projection and
sort-preserving merge in the 55 Ukiel plan. A seven-iteration `clickbench analyze`
probe retains both physical plans and operator metrics. Disabling sort pushdown
does not remove the execution regression (warm execution medians: 38.7 ->
57.1 ms with it disabled; 40.3 -> 45.1 ms with it enabled). These are leads for
a provider/optimizer follow-up; the cause has not been isolated. The ordering
workaround and default query settings are unchanged.

### Tenant queries

Across the 21 tenant/scenario pairs, the median new/old ratio is **0.976**;
15 improve and six move slightly upward. Each cell below is 54 -> 55, using the
median of the two reported five-iteration medians. Census counts match in both
repetitions: heavy 8,527,070, median 123, light 1,003.

| Scenario | Heavy, ms | Median, ms | Light, ms |
|---|---:|---:|---:|
| count | 10.35 -> 9.93 | 5.81 -> 5.78 | 5.56 -> 5.07 |
| adv_filter | 14.08 -> 12.01 | 5.88 -> 5.95 | 5.89 -> 5.44 |
| distinct_users | 27.02 -> 23.95 | 6.09 -> 6.06 | 6.25 -> 6.10 |
| top_urls | 258.18 -> 218.02 | 7.61 -> 7.70 | 8.81 -> 8.80 |
| search_phrases | 49.86 -> 49.43 | 8.55 -> 8.58 | 8.79 -> 9.06 |
| ts_window | 3.23 -> 3.29 | 2.48 -> 2.39 | 2.47 -> 2.55 |
| minutely | 4.27 -> 4.16 | 3.73 -> 3.59 | 3.66 -> 3.54 |

The small JSON and write comparisons have only two repetitions per version;
their few-percent differences are not evidence of a meaningful speed change.
The complete refresh improves this read workload overall, with q25 retained
as a measured follow-up. These results use a freshly loaded, uncompacted L1
fixture and all 43 queries; they do not replace the older 41-query headline on
a different layout.

### Evidence

`comparison.json` contains every query/scenario ratio and the write measurements.
`read-completed.json`, `write-completed.json`, and `json-completed.json` retain
commands, labels, and elapsed times; the runner and analysis scripts are copied
beside them. `q25-diagnostics.json` and `q25-*-{plan,analyze}.log` retain the
follow-up. The before/after manifests, lockfiles, and `binary-hashes.json` identify
the compared dependency sets and executables.

All **2,204** original read-fixture files (**14,637,637,121 bytes**) retain their
size, modification time, and SHA-256 after the complete schedule. The JSON
fixture also passes a before/after SHA-256 check. Datasets, catalogs, and raw
results are retained locally for inspection; timing evidence is gitignored.

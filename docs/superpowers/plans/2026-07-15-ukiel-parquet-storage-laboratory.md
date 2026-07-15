# Ukiel Plan 47: Parquet Storage Laboratory

> **For agentic workers:** Execute this plan task-by-task. Keep checkbox
> (`- [ ]`) progress in this file, run each task's focused tests before its
> commit, and preserve the executable boundaries below. This is an experimental
> plan: it may produce evidence and follow-up issues, but it must not silently
> turn laboratory controls into production table settings.

**Status:** Written. Plan 46 is executed and its tooling, receipts, compacted
objects, and geometry interpretation are available. Its reduced 3M-row baseline
did not trigger the 64/256 MiB size cuts, so Plan 47 uses those outputs for tool
development but prepares a full 30M-row packed/64 MiB pair before publishing
storage conclusions.

**Goal:** Build a reproducible laboratory around Parquet files written by
Ukiel's real compactor. Freeze those exact files into a verified snapshot,
measure their physical structure, rewrite the same logical rows into controlled
variants, and compare native Parquet layout, encodings, physical types,
compression, Bloom filters, and experimental skip indices. Produce evidence
that tells us which choices deserve focused production issues and which are
dominated or workload-specific.

## Why we are doing this

Ukiel has closed most of the query-stack overhead on identical Parquet bytes.
The official-suite ladder showed DataFusion reading the same Parquet faster than
ClickHouse reading it, while ClickHouse recovered an advantage when it read its
own MergeTree representation. That leaves a real and interesting frontier: not
another generic query-engine optimization, but how Ukiel represents actual
event data on disk.

Plan 14 made sensible first choices: 128k-row caps, packing-key-aligned row
groups, L0 `LZ4_RAW`, L1+ ZSTD, `DELTA_BINARY_PACKED` timestamps, and opt-in
Parquet Bloom filters. Those choices are covered by footer tests, but they were
not selected by a controlled experiment over production-shaped compacted data.
The current `WriteOpts` is a small fixed policy, not an evidence-producing
format laboratory.

Roadmap row 36 names the likely remaining format costs: every integer is
currently `Int64`, every float `Float64`, dates/timestamps are not represented
with narrow native types, dictionary policy is mostly the writer default, and
compression is chosen only by compaction level. It correctly asks for a design
sketch before changing the type system. This plan supplies the missing evidence
without first expanding the production type system.

Custom skip indices belong in the same investigation, but not in the same
conceptual bucket as an encoding. Ukiel already prunes at four layers:

1. catalog part ranges and membership filters before object I/O;
2. catalog-stored row-group key spans through `ParquetAccessPlan`;
3. native Parquet row-group/page statistics and Bloom filters; and
4. pushed-down row filtering during decode.

A custom index is useful only when those layers still read expensive data for a
real predicate. The laboratory therefore measures native controls first and
allows a custom sidecar prototype only for a named residual query. It never
starts by inventing a general index API and then searching for a workload.

## Why now

This experiment would have been misleading before Plans 37 and 39 closed the
same-bytes Ukiel/DataFusion residual: format wins would have been mixed with
provider overhead and `Utf8` materialization. It also needed Plan 45's verified
production-shaped rows and Plan 46's real compactor path. Those pieces now
exist.

Plan 46 is deliberately about part geometry and issue 0014. Adding dozens of
writer variants to it would obscure its answer and break its identical-input
placement matrix. Plan 47 starts from a *finished Plan 46 output*, after the real
ladder, placement policy, key alignment, and finalization have made the files we
actually want to understand.

Doing this before implementing roadmap row 36 prevents an end-to-end type-system
rewrite based on a guessed 10–25% ceiling. It also prevents `WriteOpts` from
becoming a public bag of codec switches whose combinations nobody has measured.

## Questions this plan must answer

1. Where do bytes live in actual compacted event parts, by column, row group,
   page, encoding, and metadata/index family?
2. Which columns are paying for dictionaries that do not help, or missing
   dictionaries that would help?
3. Do smaller pages and page indices reduce object-store bytes for selective
   predicates enough to pay for their metadata and request overhead?
4. Is the current 128k key-aligned row-group policy still on the Pareto frontier
   for sparse tenant queries and broad scans?
5. Which integer/date/timestamp columns can be stored narrowly without changing
   logical values or query answers, and how much do they save after compression?
6. Which per-column encoding and compression combinations improve size, decode
   CPU, or both?
7. Do native Parquet Bloom filters help any measured non-packing-key equality
   predicate once their size and read cost are included?
8. Which residual predicates, if any, justify a custom skip index rather than a
   materialized/promoted column, sorting change, or native Parquet feature?
9. Does a combined candidate retain the individual wins, or were they caused by
   interactions such as dictionary × compression or page size × page index?
10. Which results are specific to prod-synth event data, and which reproduce on
    a bounded ClickBench OLAP slice?

## Non-goals

- No production `WriteOpts`, table-schema, catalog migration, compactor, query
  provider, or configuration change.
- No public table-level codec or encoding knobs. A laboratory spec is not a
  product contract.
- No full Cartesian product. Stages are sequential, rerun the product control,
  and carry forward only non-dominated candidates.
- No new general-purpose index framework in `ukiel-core` or `ukiel-query`.
- No claim that local filesystem latency is object-store latency. Object-store
  bytes and request counts are measured in a separate confirmation mode.
- No claim that prod-synth reproduces production column values or compression;
  its profile explicitly cannot tell us those distributions.
- No lossy type conversion. `Float64 -> Float32`, timestamp unit changes, string
  normalization, and decimal scale changes are excluded unless every value is
  exactly round-trippable and query equality remains exact.
- No JSON storage conclusion before Plan 44 establishes the logical JSON
  contract. JSON text remains a string workload here; JSON physical layouts are
  a later row using this laboratory.
- No replacement for Plan 46's catalog-admission experiment, ClickBench,
  JSONBench, or the macro performance suites.

## Dependency and execution order

Plan 47 consumes, but does not extend, these contracts:

- Plan 45 `ukiel-prod-synth/v1` for the source provenance and query classes;
- Plan 46 `ukiel-prod-part-shape/v1` receipt and converged compacted objects;
- Plan 46 `row-multiset/v1` as the physical-row integrity guard where the Arrow
  schema is unchanged; and
- Plan 14's current writer output as the immutable product control.

Implementation may start against the converged Plan 46 smoke and reduced
baseline receipts. Before Task 8 runs the publishable prod-synth matrix, prepare
two full 30M-row controls from one staged artifact through the existing Plan 46
pipeline: packed and size-targeted 64 MiB. Plan 46 measured that a day needs
roughly 1.4M rows to cross 64 MiB and roughly 5.5M to cross 256 MiB; 30M total
rows should exercise 64 MiB without turning input preparation into an 80M-row
placement study. If the cut still does not trigger, report that fact and use the
packed output rather than inventing a smaller target after seeing the data.

Plan 44 is independent. When it lands, a later plan may add a JSON suite to the
same snapshot/variant contracts. Plan 47 does not wait for it.

Roadmap row 36 becomes an evidence consumer: narrow production types remain a
separate implementation plan if this laboratory finds a candidate worth the
end-to-end schema cost.

## Tool boundaries

Plan 47 adds small executables joined by versioned files. No executable package
depends on another executable package or calls another command handler.

| component | one responsibility | allowed side effects |
|---|---|---|
| `parquet-lab-contract` | serde contracts for snapshots, variants, index sidecars, suites, and reports | none |
| `parquet-lab-integrity` | canonical logical-row fingerprint shared by snapshot and rewrite tools | none |
| `parquet-lab-snapshot` | freeze an explicit Parquet file set, including a Plan 46 receipt adapter, into a verified local artifact | read catalog/object store or local files; write only the selected output directory |
| `parquet-census` | inspect one snapshot/variant and report physical Parquet structure | local reads; one explicit report file |
| `parquet-rewrite` | rewrite one snapshot under one explicit variant spec | local reads; one fresh output directory |
| `parquet-skip-index` | build one versioned experimental sidecar-index artifact | local reads; one fresh output directory |
| `parquet-lab-bench` | run declared queries read-only over one existing artifact, optionally consuming a sidecar | local or read-only object-store access; one explicit result file |
| `bench/parquet-lab.sh` | orchestrate commands for one declared matrix block | starts child commands and writes a run directory; implements no snapshot, rewrite, index, or query logic |

The offline tools must install and run without PostgreSQL, Kafka, Ukiel services,
or repository-relative paths. Only `parquet-lab-snapshot from-ukiel` may depend
on catalog/object-store clients. `from-files`, census, rewrite, sidecar build,
and the local benchmark must remain usable on any explicit Parquet dataset.

The pipeline is:

```text
Plan 46 receipt + converged final objects
    -> parquet-lab-snapshot from-ukiel
    -> immutable snapshot + exact original bytes + snapshot manifest
    -> parquet-census
    -> product-control census
    -> parquet-rewrite (one explicit spec)
    -> variant manifest + rewritten parquet
    -> parquet-census + parquet-lab-bench
    -> optional parquet-skip-index for one residual predicate family
    -> parquet-lab-bench with/without the sidecar
    -> raw JSON reports + one interpretation note
```

## Versioned artifact contracts

### Snapshot: `ukiel-parquet-snapshot/v1`

The snapshot manifest records:

- source kind (`plan46_receipt` or `explicit_files`), source contract/digests,
  git SHA, Arrow/Parquet/DataFusion versions, and creation command;
- declared logical schema, physical Arrow schema, packing/sort keys, and any
  logical projection needed to compare physical-type variants;
- every local file's relative path, original object key, BLAKE3 digest, rows,
  bytes, row groups, and source part identity;
- total row/byte census, physical `row-multiset/v1` when applicable, and the
  logical fingerprint below; and
- the synthetic disclaimer when the source carries one.

`from-ukiel` revalidates receipt, catalog identity, convergence, partition
marker, object HEAD, and full-row fingerprint. It downloads each final object
unchanged and publishes the manifest atomically only after local digests equal
the downloaded bytes. It does not compact, vacuum, query, or mutate the source.

`from-files` requires an explicit schema and sorted file list; it never lists a
directory implicitly. This adapter exists for the bounded ClickBench slice and
for anyone using the tools outside Ukiel.

### Logical fingerprint: `logical-row-multiset/v1`

The existing physical fingerprint must disagree when `Int64` becomes `Int32`;
that is correct for compaction integrity but wrong for a type experiment. The
laboratory therefore adds a second, narrowly scoped canonical fingerprint:

- every column is encoded under the manifest's declared *logical* type and
  name/order, not its physical Arrow representation;
- signed integers normalize losslessly to signed 128-bit canonical bytes;
- timestamps normalize to epoch milliseconds only when the declared unit is
  exactly convertible without rounding;
- `Utf8`/`Utf8View` normalize to the same length-delimited UTF-8 bytes;
- booleans are one byte; floats preserve canonical `Float64` bits, with
  `-0.0 == 0.0` and no lossy `Float32` promotion accepted; and
- null remains distinct from every value.

Like `row-multiset/v1`, it carries count, XOR, and four wrapping-sum lanes over
BLAKE3 row hashes. Golden vectors pin the format. Unsupported or lossy casts
fail closed; a column can never be silently omitted.

### Variant: `ukiel-parquet-variant/v1`

Every variant manifest records the parent snapshot digest, exact variant-spec
digest, a human label, complete resolved writer properties globally and by
column, input/output censuses, logical fingerprint, per-file input/output
mapping, and every output digest/footer summary.

Initial variants preserve file membership, row order, sort semantics, and file
count. They may change row-group/page boundaries and physical schema. A later
experiment that changes file partitioning is a different plan because it mixes
placement with format.

### Sidecar: `ukiel-parquet-skip/v1`

An experimental sidecar is bound to the exact variant-manifest digest and every
file digest. For each indexed column and row group it records index kind,
version, parameters, payload offset/length, and payload digest. Any unknown
version, schema mismatch, file mismatch, missing row group, corrupt payload, or
unsupported predicate produces `Unknown`/keep, never skip.

The sidecar is not a proposed production format. It exists to price build time,
bytes, metadata requests, pruning, and latency before choosing whether a real
implementation belongs in Parquet metadata, object sidecars, or the catalog.

## Workloads and bounded data tiers

| tier | artifact | purpose | acceptance role |
|---|---|---|---|
| unit | generated files under 10k rows | footer, fingerprint, refusal, and query-equality tests | correctness only |
| smoke | Plan 46 prod-synth smoke compacted output | end-to-end tool and script proof | no performance conclusion |
| event baseline | one 30M-row prod-synth L0 artifact compacted as packed and 64 MiB size-targeted | primary sparse multi-tenant event/layout result; the 64 MiB arm must actually cut or be reported as non-triggering | required |
| OLAP confirmation | deterministic 10M-row ClickBench slice, explicit files and existing adapted query schema | wide scans, narrow types, dictionaries, codecs | required for any general claim; may classify event-only |
| JSON | Plan 44 fixture | raw/promoted/native JSON layout | deferred; not Plan 47 acceptance |

The event suite reuses the six Plan 45 query classes and adds declarative probes
for equality, bounded range, prefix, substring, `IS NULL`, narrow projection,
and wide projection at measured selectivity bands. A probe is included only if
the column actually contains enough values to realize its declared selectivity;
the suite compiler reports the observed count before timing.

The OLAP suite uses the existing adapted ClickBench SQL over the exact 10M-row
slice. Queries that cannot preserve answer semantics under a physical-type view
are excluded with a named reason, never silently rewritten into easier SQL.

## Measurement validity contract

- The product control is the exact original snapshot, not a fresh rewrite using
  supposedly equivalent defaults.
- Every matrix block reruns the product control. Machine drift must be visible.
- One variable changes at a time until the confirmation candidate. Carry
  forward only candidates not dominated on the measured workload.
- Run at least two interleaved repetitions of every publishable block. Within a
  repetition, randomize variant order from a recorded seed.
- For each query: one process/session-cold run, then at least five warm runs.
  “Cold” means fresh DataFusion session and empty laboratory metadata cache; do
  not claim the host page cache was dropped unless it actually was and the
  mechanism is recorded.
- Local mode isolates decode/layout. Object-store confirmation uses an
  instrumented `ObjectStore` wrapper and records requests, requested bytes,
  returned bytes, footer/page-index/Bloom reads, and retries. Do not infer I/O
  from output file size.
- Reader A/Bs explicitly record `enable_page_index`, `pruning`,
  `pushdown_filters`, `reorder_filters`, and `bloom_filter_on_read`. A writer
  feature is not credited when its reader is disabled.
- Save DataFusion physical plan and metrics for every query/variant, including
  row groups/pages pruned, rows decoded, output rows, elapsed compute, spill,
  and peak memory where exposed.
- Writer reports include wall time, rows/s, input/output bytes, per-column
  compressed/uncompressed bytes, footer/index/Bloom bytes, and peak RSS/CPU when
  the host observer supports it. Missing host metrics are `null`, never zero.
- Query answers and schemas are normalized and compared before timing. Every
  artifact must match the parent logical fingerprint. A mismatch invalidates
  the entire variant.
- Record git SHA, build flags, target CPU, host CPU/RAM, filesystem/object-store
  kind, kernel, Arrow/Parquet/DataFusion versions, suite digest, snapshot digest,
  variant digest, cache posture, and run order.
- Raw JSON is immutable. Markdown interpretation is derived from raw reports;
  do not retain only charts or aggregate prose.

## Sequential experiment matrix

The implementation encodes these as versioned TOML specs. Values that the
pinned Parquet 58.3 writer does not support fail validation; they are not
approximated with another setting.

### Block A: row groups, pages, and native pruning metadata

- product control: original compacted bytes;
- row-group cap: 32k, current 128k, 512k rows;
- packing-key boundary flush: current aligned versus unaligned fixed caps;
- data-page target: 64 KiB, 256 KiB, current 1 MiB;
- page statistics/index: `EnabledStatistics::Chunk` versus `Page`, with offset
  index enabled/disabled as paired reader A/Bs; and
- one targeted interaction confirmation: winning page size × winning row-group
  size, because page selectivity depends on row-group geometry.

The laboratory runner has no catalog and uses an explicit packing-key predicate,
so this block measures file-native row-group/page pruning only. Keep Plan 46's
catalog/provider numbers beside it as separate context; never add the two wins
or attribute catalog membership pruning to a physical-layout variant.

### Block B: encodings and dictionary policy

Use census-derived column classes, not column-name folklore:

- sorted integral/time columns: `PLAIN` versus `DELTA_BINARY_PACKED`, dictionary
  disabled;
- low/mid/high-NDV strings: dictionary on/off, dictionary-page limits, and
  `PLAIN`, `DELTA_LENGTH_BYTE_ARRAY`, or `DELTA_BYTE_ARRAY` fallback where valid;
- floating columns: `PLAIN` versus `BYTE_STREAM_SPLIT` only if the pinned reader
  round-trips and the focused compatibility test passes; and
- booleans remain their required encoding and serve as a negative control.

Record the encodings that actually appear in every column chunk. Requested
properties are not evidence that the writer used them; dictionary fallback in
particular must be read from footers.

### Block C: compression

- whole-file controls: ZSTD levels 1, 3, 6; `LZ4_RAW`; `SNAPPY`;
- `UNCOMPRESSED` only as a diagnostic ceiling on smoke;
- per-column candidate: the census-selected combination, typically stronger
  ZSTD for cold/wide strings and a decode-fast codec for hot narrow columns; and
- dictionary × codec confirmation for any dictionary candidate, because a
  codec win can disappear after dictionary encoding removes repetition.

Compare write amplification and decode CPU as well as bytes. L0 lifetime policy
is not part of this experiment; the source snapshot contains final read-many
parts.

### Block D: physical/logical data types

- signed integer columns may try `Int8`/`Int16`/`Int32` only when census min/max
  proves every value fits;
- semantically millisecond timestamps compare physical `Int64` with Arrow
  `Timestamp(Millisecond, None)` while the logical view preserves Ukiel's
  declared timestamp contract;
- `Date32` is tested only for a column semantically declared as a date, never by
  guessing from an integer's values;
- `Utf8View` is not a storage type candidate: Plan 39 is an in-memory reader
  representation over unchanged Parquet bytes;
- `Float64 -> Float32` is rejected unless every value round-trips bit-exactly,
  which is expected to exclude ordinary metric columns; and
- query views cast physical columns back to the snapshot's logical schema before
  running unchanged SQL.

This block measures the ceiling for roadmap row 36. It does not modify
`ukiel-core::schema` or claim that the migration/evolution cost is free.

### Block E: native Bloom filters

For equality-filtered, non-packing-key columns selected by the query suite:

- off, default FPP 0.05, and FPP 0.01;
- NDV set from the census versus writer default;
- reader `bloom_filter_on_read` off/on; and
- report Bloom bytes and extra reads separately from data bytes avoided.

Never enable Blooms on every column. Range, prefix, substring, and high-hit-rate
predicates are negative controls unless the native implementation explicitly
supports the predicate.

### Block F: custom skip-index prototypes

Only predicates that remain expensive after Blocks A–E qualify. The first
candidate set is deliberately small:

1. **external zone map** for supported scalar/string columns, to price skipping
   before a Parquet footer versus native footer/page statistics;
2. **small exact value set** for equality/`IN` on low-NDV row groups, capped by
   an explicit payload-byte budget; and
3. **prefix set** at declared byte-prefix lengths for `LIKE 'literal%'`, only if
   a real prefix query remains. A substring n-gram Bloom may be added only after
   a named substring query passes a baseline-cost gate and its no-false-negative
   property test is specified.

The laboratory reader translates `NoMatch` row groups into a
`ParquetAccessPlan`; `Maybe` and `Unknown` stay selected. It reports sidecar
fetches separately. The product's packing-key exact bitmap remains outside this
comparison—it already solves a different, catalog-admission problem.

## Decision rules

Correctness is absolute. One changed answer, schema, logical fingerprint, or
false-negative skip rejects the variant regardless of speed.

Performance classification uses the control repetitions to derive a noise band:

```text
noise = max(5%, 3 * MAD(control repetitions) / median(control repetitions))
```

Apply it separately to wall time, bytes, and write throughput. A result inside
the band is “no demonstrated change,” not a win or loss.

| observation | classification | allowed next action |
|---|---|---|
| worse or equal size and worse or equal read/write performance outside noise | dominated | discard |
| improves one dimension outside noise with no measured regression outside noise | Pareto candidate | carry to confirmation block |
| improves targeted queries but regresses broad scans or writes | workload-specific | document; require a table/column policy issue, never make global default |
| native feature matches custom sidecar pruning within noise | native wins on simplicity | discard custom index |
| custom index avoids material bytes/latency after including its fetch/build/storage cost | residual index candidate | file focused issue naming predicate, placement, format options, and failure behavior |
| narrow type saves bytes but needs casts or changes query semantics | storage ceiling only | feed roadmap row 36 design; no direct implementation |
| combined candidate loses an individual win | interaction/path dependence | run leave-one-out confirmation; do not publish additive claims |

The final report must name the product control and at most three candidates:
best balanced, best scan-heavy, and best selective. A table of every number is
welcome; a production configuration with dozens of per-column switches is not.

## Global constraints

- Rust edition 2024 and workspace dependency pins: DataFusion 54,
  Arrow/Parquet 58.3, object_store 0.13. Never bump them independently inside
  an experiment.
- Offline packages do not depend on `ukield`, `ukiel-catalog`, Kafka, HTTP, or
  testcontainers. The snapshot's Ukiel adapter is the only service-aware tool.
- No executable package depends on another executable package.
- Explicit inputs and outputs only; no current-directory dataset discovery.
- Fresh output directories/files by default; overwrite requires `--replace`
  and an exact expected parent digest.
- All manifests/specs/reports are version-gated and published atomically.
- Unit tests run without Docker. Optional object-store confirmation tests use
  testcontainers and are clearly marked.
- Existing production tests stay green. Production crates should not change;
  if a task appears to need one, stop and write the missing capability into the
  report.
- Conventional commit messages; no AI attribution.
- Each task ends with focused tests, `cargo fmt --check`, and relevant clippy.

---

### Task 1: Versioned contracts and logical-row integrity

**Files:**

- Modify: `Cargo.toml`
- Create: `tools/parquet-lab-contract/Cargo.toml`
- Create: `tools/parquet-lab-contract/src/lib.rs`
- Create: `tools/parquet-lab-contract/src/{snapshot,variant,index,suite,report}.rs`
- Create: `tools/parquet-lab-integrity/Cargo.toml`
- Create: `tools/parquet-lab-integrity/src/lib.rs`
- Test: `tools/parquet-lab-contract/tests/contracts.rs`
- Test: `tools/parquet-lab-integrity/tests/{golden,types,boundary}.rs`

- [x] **Step 1: Write failing contract tests.** Cover every version, digest
  binding, relative-path rule, duplicate file/variant labels, incomplete
  resolved properties, unknown index kinds, and atomic-report identity fields.
- [x] **Step 2: Write logical fingerprint golden/property tests.** Cover
  `Int64`/lossless `Int32` equivalence under declared `int64`, timestamp
  normalization, null versus zero/empty, `Utf8` versus `Utf8View`, batch/file
  order independence, duplicate sensitivity, lossy narrowing refusal, and an
  unsupported type failing closed.
- [x] **Step 3: Implement the two pure libraries.** Contract has serde/BLAKE3
  only; integrity adds Arrow but no Parquet reader, DataFusion, Ukiel, or
  service client. Reuse a canonical encoder once—snapshot and rewrite must not
  implement it independently.
- [x] **Step 4: Pin dependency boundaries via `cargo metadata`.** An executable
  is not added in this task.
- [x] **Step 5: Verify and commit.**

```bash
cargo test -p parquet-lab-contract -p parquet-lab-integrity
cargo clippy -p parquet-lab-contract -p parquet-lab-integrity --all-targets -- -D warnings
git commit -m "tools: define parquet laboratory contracts and logical integrity"
```

### Task 2: Freeze actual compacted parts into an immutable snapshot

**Files:**

- Create: `tools/parquet-lab-snapshot/Cargo.toml`
- Create: `tools/parquet-lab-snapshot/src/{main,lib,from_ukiel,from_files}.rs`
- Create: `tools/parquet-lab-snapshot/README.md`
- Test: `tools/parquet-lab-snapshot/tests/{snapshot,ukiel_adapter,boundary}.rs`

**Interfaces:**

```text
parquet-lab-snapshot from-ukiel \
  --receipt FILE --config FILE --output DIR

parquet-lab-snapshot from-files \
  --schema FILE --files-from FILE --logical-schema FILE --output DIR

parquet-lab-snapshot verify --manifest FILE
```

- [x] **Step 1: Write failing local snapshot tests.** Explicit sorted inputs
  produce byte-identical copied files and a deterministic manifest; altered,
  added, missing, duplicated, absolute/traversing, or implicitly discovered
  files fail verification.
- [x] **Step 2: Write the Plan 46 adapter integration test.** A smoke receipt
  must be converged and fully marked; catalog rows, object HEAD, downloaded
  bytes, row census, physical fingerprint, and logical fingerprint all agree.
  The command performs no SQL mutation and refuses an unconverged fixture.
- [x] **Step 3: Implement bounded streaming download/copy.** Never hold a full
  part in memory; write to a temporary sibling and rename only after all
  digests/fingerprints close.
- [x] **Step 4: Enforce boundaries.** `from-files` and `verify` must not need a
  service. Keep Plan 46 parsing in the adapter; do not move snapshot behavior
  into `ukiel-prod-bench`.
- [x] **Step 5: Verify installability, help, and commit.**

```bash
cargo test -p parquet-lab-snapshot
cargo install --path tools/parquet-lab-snapshot --root /tmp/parquet-lab-snapshot-install
/tmp/parquet-lab-snapshot-install/bin/parquet-lab-snapshot --help
cargo clippy -p parquet-lab-snapshot --all-targets -- -D warnings
git commit -m "tools: snapshot verified compacted parquet inputs"
```

### Task 3: Standalone physical Parquet census

**Files:**

- Create: `tools/parquet-census/Cargo.toml`
- Create: `tools/parquet-census/src/{main,lib,census}.rs`
- Create: `tools/parquet-census/README.md`
- Test: `tools/parquet-census/tests/{census,boundary}.rs`

**Interface:**

```text
parquet-census --manifest FILE --report FILE
```

The JSON report contains raw per-file/row-group/column-chunk records plus
aggregates. It reads physical/logical types, rows, values/nulls where statistics
are truthful, compressed/uncompressed sizes, encodings actually used,
dictionary pages, compression, min/max truncation, sorting columns, page/offset
indices, Bloom offsets/lengths, footer bytes, and per-column fractions. Where a
footer cannot provide NDV or value-width/cardinality, stream only that column
and label the measurement `scanned`; never invent it from min/max.

- [x] **Step 1: Build golden files covering every relevant encoding, codec,
  dictionary fallback, page/statistics mode, Bloom, null pattern, and multiple
  row groups.** Assert the census reads what was actually written.
- [x] **Step 2: Implement footer-first inspection plus bounded optional column
  scans.** Report provenance (`footer`, `page_index`, or `scanned`) per metric.
- [x] **Step 3: Add determinism/refusal tests.** Stable sort order, atomic
  report, parent-manifest verification, unknown physical type reported without
  dropping the column, corrupt footer fails loudly.
- [x] **Step 4: Enforce standalone installation and dependency boundary.** No
  DataFusion or Ukiel dependency; Arrow/Parquet only.
- [x] **Step 5: Verify and commit.**

```bash
cargo test -p parquet-census
cargo install --path tools/parquet-census --root /tmp/parquet-census-install
cargo clippy -p parquet-census --all-targets -- -D warnings
git commit -m "tools: inspect physical parquet storage by column and row group"
```

### Task 4: Deterministic one-spec Parquet variant writer

**Files:**

- Create: `tools/parquet-rewrite/Cargo.toml`
- Create: `tools/parquet-rewrite/src/{main,lib,spec,rewrite,types}.rs`
- Create: `tools/parquet-rewrite/README.md`
- Create: `bench/config/parquet-lab/product-control.toml`
- Test: `tools/parquet-rewrite/tests/{rewrite,properties,types,boundary}.rs`

**Interface:**

```text
parquet-rewrite \
  --manifest SNAPSHOT.json --spec VARIANT.toml --output DIR
```

The spec exposes only pinned Parquet 58.3 controls used by the matrix:
row-group rows, key-boundary flush, write batch, data/dictionary page sizes,
statistics/index mode, per-column dictionary, encoding, compression/level,
Bloom FPP/NDV, and declared lossless physical-type projection. It records both
requested and resolved properties. Invalid type/encoding/codec combinations are
validation errors before any output is written.

- [x] **Step 1: Write failing property round-trip tests.** Read every output
  footer and assert resolved compression, actual encodings, dictionary use,
  row-group/page metadata, Blooms, sorting columns, and row counts. Cover
  dictionary fallback explicitly.
- [x] **Step 2: Write type safety tests.** Lossless integer/timestamp variants
  preserve logical fingerprint and normalized query schema; overflow, rounding,
  timezone/unit ambiguity, float loss, and a semantic date guess are refused.
- [x] **Step 3: Implement bounded rewrite.** Preserve file membership and sort
  order; stream batches, flush row groups deterministically, and publish the
  variant manifest only after census and logical equality pass.
- [x] **Step 4: Make the product control unrewritable.** The original snapshot
  is the control. `product-control.toml` documents current properties for
  comparison but the runner must never substitute freshly encoded bytes for
  the control arm.
- [x] **Step 5: Verify standalone installation and commit.**

```bash
cargo test -p parquet-rewrite
cargo install --path tools/parquet-rewrite --root /tmp/parquet-rewrite-install
cargo clippy -p parquet-rewrite --all-targets -- -D warnings
git commit -m "tools: rewrite parquet into verified storage variants"
```

### Task 5: Read-only query benchmark with truthful I/O accounting

**Files:**

- Create: `tools/parquet-lab-bench/Cargo.toml`
- Create: `tools/parquet-lab-bench/src/{main,lib,runner,counting_store,compare,metrics}.rs`
- Create: `tools/parquet-lab-bench/src/suites/{mod,prod_synth,clickbench,probes}.rs`
- Create: `tools/parquet-lab-bench/README.md`
- Test: `tools/parquet-lab-bench/tests/{runner,counting_store,equivalence,boundary}.rs`

**Interface:**

```text
parquet-lab-bench run \
  --manifest FILE --suite FILE --result FILE \
  --mode local|object-store --cold-iters 1 --warm-iters 5 \
  [--skip-manifest FILE]
```

- [x] **Step 1: Define versioned suites.** Import the six prod-synth query
  classes and existing adapted ClickBench SQL without calling their executable
  handlers. Add parameterized probes with expected selectivity/result digests.
- [x] **Step 2: Write accounting tests.** `CountingObjectStore` attributes
  HEAD/get/range requests and requested/returned bytes by metadata, index, and
  data phase. No coordinated omission; every scheduled query is completed or
  failed and counted.
- [x] **Step 3: Implement schema/result equivalence before timing.** Physical
  variants are exposed through the declared logical projection; normalized
  batches must equal the snapshot control exactly.
- [x] **Step 4: Capture plans and DataFusion metrics.** Pin reader flags, cache
  posture, cold/warm phases, run order, host metadata, and missing metrics as
  `null`. Refuse report overwrite.
- [x] **Step 5: Prove read-only boundaries and commit.** The package has no
  catalog mutator, writer, generator, or compactor dependency.

```bash
cargo test -p parquet-lab-bench
cargo clippy -p parquet-lab-bench --all-targets -- -D warnings
git commit -m "bench: compare parquet variants with exact query and io accounting"
```

### Task 6: Native layout, encoding, compression, type, and Bloom matrix

**Files:**

- Create: `bench/parquet-lab.sh`
- Create: `bench/config/parquet-lab/{pages,encodings,compression,types,blooms}/`
- Create: `bench/tests/parquet-lab.bats` or the repository's shell-test convention
- Modify: `bench/README.md`

- [x] **Step 1: Encode Blocks A–E as explicit specs.** Every spec changes one
  axis from its block control and has a stable label. The script runs one block,
  never an implicit all-night matrix.
- [x] **Step 2: Test orchestration/refusal without large data.** Missing
  manifests, reused output, spec/parent mismatch, invalid stage order, absent
  control, and partial reports fail before a benchmark claim is emitted.
- [x] **Step 3: Run the complete smoke matrix.** Every variant fingerprints
  equal, every query answer matches, every requested property is confirmed by
  census, and local/object-store accounting balances.
- [x] **Step 4: Add selection/confirmation logic to reports, not the writer.**
  Compute the registered noise band, dominated/Pareto/workload-specific labels,
  and carry-forward candidates. Do not mutate later TOML specs automatically;
  the chosen digest is explicit.
- [x] **Step 5: Verify shell, workspace, and commit.**

```bash
bash -n bench/parquet-lab.sh
cargo test -p parquet-lab-contract -p parquet-lab-integrity -p parquet-lab-snapshot -p parquet-census -p parquet-rewrite -p parquet-lab-bench
cargo fmt --check
cargo clippy -p parquet-lab-contract -p parquet-lab-integrity -p parquet-lab-snapshot -p parquet-census -p parquet-rewrite -p parquet-lab-bench --all-targets -- -D warnings
git commit -m "bench: define the native parquet storage matrix"
```

### Task 7: Experimental custom skip-index sidecars

**Files:**

- Create: `tools/parquet-skip-index/Cargo.toml`
- Create: `tools/parquet-skip-index/src/{main,lib,builder,zone_map,value_set,prefix_set}.rs`
- Create: `tools/parquet-skip-index/README.md`
- Modify: `tools/parquet-lab-bench/src/{runner,metrics}.rs`
- Create: `tools/parquet-lab-bench/src/skip.rs`
- Test: `tools/parquet-skip-index/tests/{correctness,corruption,boundary}.rs`
- Test: `tools/parquet-lab-bench/tests/skip_index.rs`

**Interfaces:**

```text
parquet-skip-index build \
  --manifest VARIANT.json --spec SKIP.toml --output DIR

parquet-lab-bench run \
  --manifest VARIANT.json --suite FILE --skip-manifest DIR/skip.json \
  --result FILE --mode local|object-store
```

- [x] **Step 1: Write no-false-negative property tests.** For every supported
  predicate and random row group, `NoMatch` implies a full scan finds zero
  matches. Nulls, empty values, Unicode byte prefixes, truncated values, `IN`,
  and unsupported expressions degrade to `Unknown`.
- [x] **Step 2: Pin binding/corruption behavior.** Wrong file/schema/variant
  digest, unknown version, missing payload, corrupt checksum, or row-group
  mismatch keeps every affected group and increments a reasoned fallback metric.
- [x] **Step 3: Implement the three bounded prototypes.** Payload budgets and
  prefix lengths are explicit in the spec. No index is built for a column/query
  family absent from the compiled suite.
- [x] **Step 4: Translate only `NoMatch` into `ParquetAccessPlan`.** Preserve
  native DataFusion pruning underneath. Report custom-skipped, native-skipped,
  decoded, sidecar request/byte, build-time, and storage costs separately.
- [x] **Step 5: Compare against native controls on smoke and commit.** A native
  feature matching the result makes the custom candidate fail the simplicity
  gate, as intended.

```bash
cargo test -p parquet-skip-index -p parquet-lab-bench
cargo clippy -p parquet-skip-index -p parquet-lab-bench --all-targets -- -D warnings
git commit -m "bench: prototype conservative parquet skip-index sidecars"
```

### Task 8: Execute bounded event and OLAP matrices

**Files:**

- Store raw JSON under the benchmark-results location documented by
  `bench/README.md`; do not commit datasets or multi-gigabyte rewritten variants
- Create: `docs/notes/2026-07-15-ukiel-parquet-storage-laboratory.md`

- [ ] **Step 1: Prepare and snapshot the full event controls with Plan 46's
  existing pipeline.** Generate/stage one verified 30M-row baseline artifact,
  reuse its exact L0 digest for packed and 64 MiB compaction arms, and record
  receipts/part-shape reports. Confirm whether 64 MiB actually cut output. Once
  snapshotted, never regenerate or recompact between Parquet variants.
- [ ] **Step 2: Run Blocks A–E twice in interleaved order.** Save every census,
  rewrite, query, I/O, plan, and host report. Rerun the original product bytes in
  every block.
- [ ] **Step 3: Build only qualified Block F indices.** If no predicate passes
  the residual-cost gate, record “no custom index justified” and treat that as a
  successful answer, not an incomplete task.
- [ ] **Step 4: Run the 10M ClickBench confirmation.** Reproduce balanced
  candidates and classify disagreements as event-specific. Do not expand to
  100M merely to make a small delta look significant.
- [ ] **Step 5: Run the combined candidate and leave-one-out checks.** Confirm
  interactions and cap the published candidates at balanced, scan-heavy, and
  selective.
- [ ] **Step 6: Write the interpretation.** Answer all ten questions; separate
  size, write, local read, and object-store results; report noise and raw digests;
  distinguish a storage ceiling from an end-to-end Ukiel claim.
- [ ] **Step 7: File focused follow-up issues only for demonstrated residuals.**
  Each issue names exact artifact/query/spec digests, benefit, regressions,
  production placement options, backward compatibility, fallback semantics,
  and a rollback gate.

### Task 9: Close roadmap and verification

**Files:**

- Modify: `docs/superpowers/plans/2026-07-05-ukiel-v1-roadmap.md`
- Modify: this plan
- Modify: `docs/issues/README.md` only if Task 8 created issues

- [ ] **Step 1: Update roadmap row 47 with measured outcomes.** Update row 36
  with the narrow-type verdict; do not mark row 36 executed because an
  experiment is not a product type-system implementation.
- [ ] **Step 2: Mark every task truthfully.** Record deviations and failed
  hypotheses in this plan; do not rewrite the original matrix after seeing data.
- [ ] **Step 3: Full verification.**

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
make test
git diff --check
```

- [ ] **Step 4: Commit documentation/results metadata.**

```bash
git commit -m "bench: record parquet storage laboratory results"
```

## Final acceptance

Plan 47 is complete only when:

- the input is a verified immutable snapshot of actual compacted Ukiel parts;
- the product control uses original bytes and appears in every matrix block;
- every variant preserves file membership/order and the logical-row fingerprint;
- every timed query first passes exact schema/result equivalence;
- census proves properties actually written rather than trusting requested
  settings;
- writer cost, storage bytes, local read behavior, and object-store requests/
  bytes are reported separately;
- native page/statistics/Bloom features are measured with corresponding reader
  switches on and off;
- row groups/pages, encodings/dictionaries, compression, physical types, and
  native Blooms each have a bounded one-factor result;
- any custom skip index is tied to a named residual predicate, has zero observed
  false negatives plus property tests, and includes build/storage/fetch cost;
- the event result is confirmed or classified event-specific on the bounded
  ClickBench slice;
- combined candidates survive interaction/leave-one-out confirmation;
- no production crate, migration, or public configuration knob changes; and
- follow-up product work exists only as focused evidence-backed issues/plans.

## Self-review notes

- **Why not extend Plan 14?** Plan 14 is executed production history. Its policy
  is the control; retrofitting an experimental matrix into it would erase the
  distinction between what shipped and what is being explored.
- **Why not extend Plan 46?** Plan 46 answers whether real compactor part shape
  makes issue 0014's catalog filter useful. Plan 47 freezes one of those outputs
  and asks a different question about bytes inside each part.
- **Why original bytes as control?** Re-encoding with “the same” writer settings
  can change defaults, page boundaries, metadata, or library behavior. The file
  the product actually wrote is the only honest control.
- **Why a new logical fingerprint?** Physical row integrity must notice a type
  change; a type experiment must instead prove equal declared values across
  different physical representations. Conflating those contracts would weaken
  Plan 46 or make Plan 47 impossible.
- **Why separate tools?** Snapshotting, inspecting, rewriting, indexing, and
  benchmarking have different side effects and dependency needs. Keeping them
  separate makes each reusable and prevents another monolithic benchmark binary.
- **Why sidecars for custom indices?** A sidecar is a reversible laboratory
  carrier with measurable request cost. It is not a production decision. The
  winning evidence decides later whether metadata belongs in Parquet, object
  sidecars, or PostgreSQL.
- **Why native controls first?** A custom index that reproduces page statistics
  or Parquet Bloom pruning adds format/version/availability complexity for no
  new capability. The simplicity gate should eliminate it early.
- **Why not expose every writer property to users?** The matrix needs controls
  to identify good policy. Product configuration should expose stable intent,
  not a serialization library's entire builder API.

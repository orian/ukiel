# Ukiel Plan 44: Native JSON Baseline and JSONBench Implementation Plan

> **For agentic workers:** Execute this plan task-by-task. Use checkbox (`- [ ]`)
> progress, run each task's focused tests before its commit, and keep Plan 43's
> ambiguous-mutation reconciliation out of this work.

**Goal:** Add an honest first `json` type to Ukiel, retain a complete Bluesky
document beside its promoted hot fields, and run JSONBench's five queries over
both representations. The default baseline is one source file (about one
million rows); ten files are an optional confirmation, not an acceptance gate.

**Why now:** Ukiel already runs JSONBench-shaped queries, but it discards the raw
document and queries five fields flattened by benchmark-specific Rust code.
`bench/queries/jsonbench/ADAPTATIONS.md` correctly says those timings are not a
JSON extraction benchmark. Arrow 58.3 has the canonical `arrow.json` extension,
while the pinned DataFusion 54 has JSON file input but no built-in SQL JSON
extraction functions. That makes a small, deliberately simple baseline useful:
it prices parse-at-read JSON, compares it with Ukiel's practical promoted-column
posture on the same rows, and tells us whether columnar shredding deserves a
later design.

## Decision: lossless raw JSON plus promoted typed columns

The first Ukiel JSON type is a **logical type**, not a claim that Parquet or
DataFusion now has ClickHouse-style native JSON subcolumns.

- A schema field declared as `json` is stored physically as compact RFC 8259
  text in Arrow/Parquet `Utf8` and tagged with both `ukiel:type=json` and the
  canonical Arrow `arrow.json` extension metadata.
- Ingest accepts a native JSON value in the row. It serializes the value once;
  callers do not double-encode it. A present JSON `null` is stored as the text
  `null`, while an absent field remains SQL `NULL`. Objects, arrays, strings,
  numbers, booleans, and JSON null are all valid.
- This is semantically lossless, not byte-for-byte lossless: insignificant
  whitespace and object-key order may change during `serde_json` parsing and
  serialization.
- SQL extracts typed scalars with deterministic functions using RFC 6901 JSON
  Pointer paths: `json_get_string(data, '/commit/collection')` and
  `json_get_i64(data, '/time_us')`. A missing path, JSON null, or a value of the
  wrong requested type returns SQL `NULL`. Malformed stored JSON is corruption
  and fails the query rather than silently becoming null.
- A JSON column selected directly remains compact JSON **text** on Ukiel's JSON
  result encodings. The schema reports `json`, and Arrow IPC carries the
  canonical extension metadata. Returning text is deliberate: it preserves the
  distinction between SQL `NULL` and the JSON value `null` and does not embed an
  unbounded document recursively inside the result envelope.
- The recommended production shape is hybrid: retain the raw document as the
  source of truth and promote stable, frequently filtered/grouped paths into
  ordinary typed columns. Unknown and newly added fields remain queryable; hot
  paths do not pay a parse per row forever.

For this baseline the scalar functions use the workspace's existing
`serde_json`; no parser dependency is added. Each function invocation parses its
input document. JSONBench therefore measures the intentionally simple floor,
including repeated parses when a query references several paths. Do not add a
batch cache, SIMD parser, path index, hidden generated columns, or columnar
shredding before recording this baseline—the result decides which of those is
worth a follow-up.

## Benchmark shape

One `bsky` hypertable carries:

```json
{
  "fields": [
    {"name": "tenant_id", "type": "int64"},
    {"name": "ts", "type": "timestamp_ms"},
    {"name": "data", "type": "json"},
    {"name": "did", "type": "utf8"},
    {"name": "kind", "type": "utf8"},
    {"name": "operation", "type": "utf8"},
    {"name": "collection", "type": "utf8"},
    {"name": "time_us", "type": "int64"},
    {"name": "payload", "type": "utf8"}
  ]
}
```

`data` is the complete Jetstream event. The existing fields are promoted by the
fixture mapper in the same single parse; they are not materialized by five JSON
UDF calls during loading. `time_us` is newly retained so the promoted arm has
the upstream query's microsecond semantics rather than the current
millisecond-truncated approximation. `tenant_id` and `ts` remain Ukiel's packing
and event-time columns.

The same table is intentional. Parquet projection means the raw arm reads
`data`, while the promoted arm reads the typed columns, and both see exactly the
same accepted rows, files, catalog fan-out, and compaction state. Total table
size includes both representations and is therefore reported as a **hybrid
fixture size**, not compared with a raw-only system's storage number. An
isolated storage-format comparison belongs in a later native-JSON design if the
latency baseline earns it.

The runner has two named representations:

- `json`: the five adapted queries use `json_get_string` and `json_get_i64` on
  `data`;
- `promoted`: the same logical queries use `kind`, `operation`, `collection`,
  `did`, and `time_us`.

`bench bluesky jsonbench --representation both` is the default. It first proves
the two query files return identical ordered values, then times each through the
existing unscoped operator session. Reports keep the established cold run plus
three hot runs and write separate engine labels/files so the existing report
tool can ingest them.

## Scope boundary

This plan does **not** implement a nested struct type, schema inference, JSON
mutation, arrays-as-relations, wildcard/recursive JSONPath, predicate pushdown
through an extraction function, per-path statistics, a parsed-document cache,
or native Parquet subcolumn shredding. It also does not claim that Arrow
extension metadata accelerates anything; the metadata is an interoperable type
contract over string storage.

The unfinished ClickHouse-JSON arm remains owned by Plan 34. Plan 44 vendors no
second copy of ClickHouse's DDL and does not make a cross-engine speed claim.
Because the dataset, five logical queries, cold/hot methodology, and 1M tier are
the same, Plan 34 can later add the external anchor without changing this
fixture or reinterpreting its results.

## Prerequisites and constraints

- Plans 30, 32, 34's shared suite harness, and 39 are executed.
- Plan 43 remains reserved for ambiguous catalog mutation reconciliation and is
  independent of this benchmark/type work.
- Rust edition 2024, toolchain >= 1.96; keep DataFusion 54 and Arrow/Parquet
  58.3 pinned in lockstep.
- No new JSON/path dependency in this plan. Use `serde_json::Value::pointer` for
  RFC 6901 lookup.
- JSON extraction functions must accept both `Utf8` and query-side `Utf8View`.
  Plan 39's `view_typed` boundary and all field metadata must survive unchanged.
- Compaction rewrites JSON as an opaque string column. It must neither parse nor
  reserialize documents.
- Unit tests run without Docker. The 1M baseline is manual and Docker-backed;
  it is required before plan close-out but not added to routine CI.
- Every task ends with focused tests. Before each commit run
  `cargo fmt --check` and the relevant Clippy target with warnings denied.
- Conventional commits; no AI attribution.

---

### Task 1: Add the logical JSON type and native-value ingest contract

**Files:**
- Modify: `crates/ukiel-core/src/schema.rs`
- Modify: `crates/ukiel-ingest/src/writer.rs`
- Modify: `crates/ukiel-query/src/view_types.rs`
- Modify: `crates/ukiel-query/src/results.rs` only if a schema/result regression
  test exposes a missing metadata propagation

**Required interfaces and behavior:**

`TableColumns::parse` maps `json` to physical `DataType::Utf8`. Its field carries
`UKIEL_TYPE_META = "json"` and Arrow's canonical `Json::default()` extension.
Unlike ordinary `utf8`, JSON always needs the Ukiel hint because its physical
type is ambiguous. `ukiel_type_of` must return `json` for both the owned schema
and Plan 39's `Utf8View` query schema.

Before Arrow's JSON decoder sees a row, normalize each present JSON-typed field:

```rust
fn normalize_json_fields(schema: &Schema, rows: &mut [serde_json::Value])
    -> Result<(), IngestError>;
```

For an object row, replace a present field value `v` with
`Value::String(serde_json::to_string(v)?)`. Do not touch a missing field. This
must run in the shared `decode_rows` path so both `rows_to_parquet` and
column-spec-aware `encode_rows` behave consistently.

- [ ] **Step 1: Failing schema tests.** Parse a `json` field; assert physical
  `Utf8`, `ukiel_type_of == "json"`, valid `arrow.json` extension metadata, and
  metadata preservation after `view_typed` changes it to `Utf8View`.
- [ ] **Step 2: Failing ingest round-trip tests.** Cover object, nested array,
  number, boolean, JSON string, present JSON null, missing/SQL null, and extra
  row fields. Read the Parquet back and assert compact text exactly. In
  particular, JSON null must read as the non-null string `"null"` and a missing
  nullable field as Arrow null.
- [ ] **Step 3: Implement the mapping and normalization.** Do not special-case
  the Bluesky fixture; schema metadata drives normalization for every table.
- [ ] **Step 4: Result-boundary tests.** A direct JSON projection reports
  `{"type":"json"}` and returns compact text in rows/columns; Arrow IPC retains
  the extension. Ordinary `utf8` output is byte-for-byte unchanged.
- [ ] **Step 5: Verify:**

```bash
cargo test -p ukiel-core
cargo test -p ukiel-ingest writer
cargo test -p ukiel-query results::
cargo test -p ukiel-query view_types::
cargo clippy -p ukiel-core -p ukiel-ingest -p ukiel-query --all-targets -- -D warnings
```

- [ ] **Step 6: Commit:**

```bash
git commit -m "feat: add lossless JSON logical type over Arrow string storage"
```

---

### Task 2: Add deterministic typed JSON extraction functions

**Files:**
- Create: `crates/ukiel-expr/src/json.rs`
- Modify: `crates/ukiel-expr/src/lib.rs`
- Modify: `crates/ukiel-query/src/context.rs`
- Test: `crates/ukiel-expr/src/lib.rs` and/or `crates/ukiel-expr/tests/json_test.rs`
- Test: `crates/ukiel-query/tests/query_test.rs`

**SQL contract:**

```sql
json_get_string(json_value, '/commit/collection') -> utf8 nullable
json_get_i64(json_value, '/time_us')               -> int64 nullable
```

Implement these as immutable DataFusion scalar UDFs and expose one registration
helper from `ukiel-expr`:

```rust
pub fn register_json_functions(ctx: &SessionContext);
```

Call it in every context that parses Ukiel expressions: `compile`,
`column_refs`, schema validation through those functions, namespace query
sessions, and operator sessions. Registration must happen before parsing SQL;
the parsed scalar-UDF object then flows into the physical expression normally.

The first argument accepts `Utf8` or `Utf8View`. The pointer is a non-null
scalar string literal; reject a per-row/dynamic pointer with a clear planning or
execution error. `json_get_string` returns only JSON strings.
`json_get_i64` returns only integers representable as i64. Missing pointer,
wrong type, or JSON null returns SQL null. Invalid RFC 6901 syntax follows
`serde_json::Value::pointer`'s no-match behavior; malformed document text is an
execution error containing the row index but not the document contents.

- [ ] **Step 1: Failing pure/vector tests.** Both string representations,
  nested paths, escaped pointer tokens (`~0`, `~1`), null input, JSON null,
  missing path, wrong type, negative/maximum i64, overflow, malformed document,
  and non-scalar path.
- [ ] **Step 2: Implement UDFs and registration.** Preserve array nulls and
  return one output value per input row. Mark both functions immutable so they
  remain legal in default/materialized/alias expressions.
- [ ] **Step 3: Expression tests.** Validate and evaluate a materialized
  `json_get_string(data, '/kind')` and an alias using `json_get_i64`; this proves
  write-time and read-time expression contexts use the same registry.
- [ ] **Step 4: Query integration test.** Store JSON, compact/rewrite it, query
  both functions through a namespace session and the operator session, and
  assert the same answer. Include a query-side `Utf8View` assertion so an
  accidental owned-string cast cannot silently undo Plan 39.
- [ ] **Step 5: Verify:**

```bash
cargo test -p ukiel-expr
cargo test -p ukiel-query json
cargo clippy -p ukiel-expr -p ukiel-query --all-targets -- -D warnings
```

- [ ] **Step 6: Commit:**

```bash
git commit -m "feat: query JSON scalars with typed RFC 6901 functions"
```

---

### Task 3: Retain the raw Bluesky event and make both query arms exact

**Files:**
- Modify: `crates/ukiel-e2e/src/bin/bench/bluesky.rs`
- Create: `bench/queries/jsonbench/queries-json.sql`
- Rename or copy current executed file to:
  `bench/queries/jsonbench/queries-promoted.sql`
- Modify: `bench/queries/jsonbench/queries.sql` only as a compatibility shim if
  existing scripts require the path
- Modify: `bench/queries/jsonbench/ADAPTATIONS.md`

Change `map_bluesky_event` to keep the parsed event itself in `data` and retain
the original `time_us` beside `ts = time_us / 1000`. The mapper still parses
each source line exactly once. Poison policy remains unchanged: invalid JSON or
missing `did`/`time_us` cannot produce Ukiel's packing/event-time keys and is
counted and skipped exactly as today.

Adapt all five JSON queries with the two functions. Use `time_us` and
`to_timestamp_micros` in both query arms so q3/q4/q5 have identical precision.
The existing promoted mapper represents an absent optional string path as the
empty string. Preserve that fixture contract explicitly with
`coalesce(json_get_string(...), '')` wherever the raw path is projected or
grouped; the UDF itself must retain the generally useful missing-path-is-NULL
semantics. Filters comparing a missing path with a non-empty literal need no
coalesce. This distinction belongs in `ADAPTATIONS.md` and in an equivalence
test containing an event without `commit.collection`.
Keep one query per line and upstream order. Do not silently modify
`queries-upstream.sql`.

Add a pre-timing equivalence check that executes paired queries, serializes the
small ordered result sets to a stable logical JSON form, and compares complete
values and nulls—not only output row counts. A mismatch names q01..q05 and
prints a bounded first difference. Every official query already has an
`ORDER BY`, so the check must not sort away ordering bugs.

- [ ] **Step 1: Failing mapper/schema tests.** Assert `data` is the full native
  event, `time_us` is exact, `ts` is milliseconds, promoted fields match their
  raw paths, and poison counts do not move.
- [ ] **Step 2: Add the raw and promoted query files.** Update the adaptation
  log path-by-path, including the new removal of the q4/q5 millisecond caveat.
- [ ] **Step 3: Add equivalence helper/tests.** Pin equal outputs for all five
  queries on a small in-memory fixture containing missing optional commit
  fields, JSON nulls, Unicode, and escaped strings. Pin a deliberate mismatch
  failure so the gate cannot degrade into row-count-only checking.
- [ ] **Step 4: Reject stale fixtures.** `bluesky jsonbench` must require both
  `data: json` and `time_us: int64` in the catalog schema and print the exact
  `e2e-down`/reload instruction for an older `bsky` table.
- [ ] **Step 5: Verify:**

```bash
cargo test -p ukiel-e2e --bin bench bluesky
cargo clippy -p ukiel-e2e --bin bench -- -D warnings
```

- [ ] **Step 6: Commit:**

```bash
git commit -m "bench: retain Bluesky JSON and add exact promoted query arm"
```

---

### Task 4: Make the small dual-representation baseline reproducible

**Files:**
- Modify: `crates/ukiel-e2e/src/bin/bench/main.rs`
- Modify: `crates/ukiel-e2e/src/bin/bench/bluesky.rs`
- Modify: `crates/ukiel-e2e/src/bin/bench/suite.rs`
- Modify: `crates/ukiel-e2e/src/bin/bench/report.rs` only if comparison output
  belongs in the shared report layer
- Modify: `bench/report.py`
- Modify: `bench/README.md`

**CLI:**

```text
bench bluesky jsonbench \
  [--representation json|promoted|both] \
  [--iters N] [--label LABEL]
```

Default `--representation both`; reject unknown values. Each arm uses the
existing `suite::run_suite` methodology and a fresh operator session for its
cold runs. Use engine names `ukiel-json` and `ukiel-promoted`, and output:

```text
bench/results/jsonbench-ukiel-json-<label>.json
bench/results/jsonbench-ukiel-promoted-<label>.json
```

Extend `SuiteReport` compatibly with optional fixture facts:

```rust
#[serde(default)]
pub table_bytes: Option<i64>;
#[serde(default)]
pub live_parts: Option<usize>;
```

Older reports must still deserialize. For both arms set rows, sum of live-part
bytes, and live-part count from one catalog snapshot. Label bytes clearly as
the hybrid table size. Add a comparison table showing, per query and in total,
`json / promoted` for cold, hot minimum, and hot median; fail comparison on
different fixture rows, parts/bytes, query failures, or answer-equivalence
failure. Do not turn a ratio into a pass/fail performance gate in this first
run.

- [ ] **Step 1: Failing CLI/report compatibility tests.** Default/each mode,
  bad mode, old report deserialization, optional fixture fields, and mismatched
  fixture refusal.
- [ ] **Step 2: Implement the two arms and comparison.** Snapshot fixture facts
  once before either run. Keep `--iters 3` as the default.
- [ ] **Step 3: Update report rendering.** The HTML/text report must distinguish
  `ukiel-json` from the historical flattened `ukiel` result rather than merging
  the series by label.
- [ ] **Step 4: Document the honest claims.** State prominently that `json`
  parses raw text at read time, `promoted` is the recommended hybrid hot path,
  total bytes contain both, the OS cache is not dropped, and this is not yet a
  ClickHouse comparison. Include the 1M default runbook and optional 10M rerun.
- [ ] **Step 5: Verify:**

```bash
cargo test -p ukiel-e2e --bin bench
python3 -m unittest discover -s bench -p 'test_*.py'
cargo clippy -p ukiel-e2e --bin bench -- -D warnings
```

- [ ] **Step 6: Commit:**

```bash
git commit -m "bench: compare raw JSON with promoted Bluesky paths"
```

---

### Task 5: Run and record the 1M baseline

**Files:**
- Modify: `bench/README.md`
- Modify: this plan's status/checklist after successful close-out
- Modify: `docs/superpowers/plans/2026-07-05-ukiel-v1-roadmap.md`

The result JSON files remain gitignored. Record the environment, commands,
fixture facts, per-query raw/promoted hot minima, totals, ratios, ingest/load
time, and any query failure in `bench/README.md`. Do not copy only the fastest
number: retain cold, hot minimum, and hot median in the generated local reports,
and summarize both total and the worst per-query ratios in the README.

- [ ] **Step 1: Full static verification:**

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test --workspace
```

- [ ] **Step 2: Fresh 1M fixture and baseline:**

```bash
make e2e-down
make e2e-up
make bench-fetch-bluesky FILES=1
cargo run --release -p ukiel-e2e --bin bench -- \
  bluesky run --files 1 --label json44-1m
cargo run --release -p ukiel-e2e --bin bench -- \
  bluesky jsonbench --representation both --iters 3 --label json44-1m
```

- [ ] **Step 3: Inspect the result, not just the exit code.** Assert zero suite
  failures, raw/promoted value equivalence for q01..q05, stored rows equal
  mapped rows, the two timing reports describe identical fixture rows/bytes/
  parts, and `json / promoted` ratios are finite. Inspect at least one physical
  plan to confirm the JSON arm projects `data` and the promoted arm does not.
- [ ] **Step 4: Optional 10M confirmation.** Run only if the 1M timings are too
  noisy or unexpectedly dominated by fixed planning overhead. It is not a
  close-out gate:

```bash
make bench-fetch-bluesky FILES=10
cargo run --release -p ukiel-e2e --bin bench -- \
  bluesky run --files 10 --label json44-10m
cargo run --release -p ukiel-e2e --bin bench -- \
  bluesky jsonbench --representation both --iters 3 --label json44-10m
```

- [ ] **Step 5: Write the verdict.** Choose exactly one evidence-backed next
  action without implementing it here:
  1. keep raw-text extraction as the cold-path feature and recommend promotions;
  2. optimize parse-on-read (multi-path projection/shared parse/SIMD parser);
  3. design native per-path columnar shredding and statistics.

  The verdict must separate parse CPU from catalog/provider overhead: Plan 39
  already closed the same-bytes read-stack residual, so a large raw/promoted
  gap is JSON representation/extraction work, not permission to reopen the
  catalog optimization hunt without new evidence.
- [ ] **Step 6: Close out roadmap and plan, then commit:**

```bash
git commit -m "docs: record the first Ukiel JSONBench baseline"
```

## Acceptance criteria

Plan 44 is complete only when all of the following are true:

1. `json` is a declared Ukiel type with canonical Arrow metadata and semantic
   round-trip tests for every JSON value category.
2. Native JSON input, JSON null, and SQL null have explicit, tested semantics.
3. Both extraction functions work in `Utf8`/`Utf8View`, query sessions, operator
   sessions, and deterministic schema expressions.
4. The Bluesky fixture retains the complete raw event and exact microsecond
   timestamp while preserving Ukiel's packing/event-time columns.
5. All five raw and promoted queries return identical ordered values before
   timing.
6. A fresh ~1M-row run records cold/hot timings and hybrid fixture facts for
   both arms; no 10M/100M/1B run is required.
7. The README makes no ClickHouse, raw-only storage, native-columnar JSON, or
   JSONPath claim the implementation did not measure.
8. The close-out names one next representation decision from the measured
   result instead of silently growing this baseline into a second JSON engine.

# parquet-lab-bench

Run declared queries **read-only** over one laboratory artifact (a snapshot or a variant),
with exact result equivalence and truthful I/O accounting. It measures an artifact that
already exists — it cannot generate, rewrite, index, or compact one.

```text
# Compile a suite from a snapshot control: bake the logical view and expected digests.
parquet-lab-bench compile --manifest SNAPSHOT.json --kind prod-synth|click-bench \
  --sql QUERIES.sql --suite-out SUITE.json

# Run a suite over an artifact and write a result report.
parquet-lab-bench run --manifest FILE --suite SUITE.json --result RESULT.json \
  --mode memory|local|object-store --cold-iters 1 --warm-iters 5 [--skip-manifest DIR/skip.json] \
  [--no-page-index] [--no-pruning] [--no-pushdown-filters] [--no-reorder-filters] \
  [--no-bloom-filter-on-read]
```

## How answers stay comparable

The suite bakes a `CREATE OR REPLACE VIEW events` that casts every physical column back to
its declared logical type (Int32/Int64/Timestamp all present as `BIGINT`, `Utf8View` as
`VARCHAR`, …). Every variant therefore presents the identical logical schema, so the
control and a variant produce byte-identical Arrow — and a BLAKE3 digest over the result's
Arrow IPC bytes is an **exact** equivalence test. Each query's answer must equal the
control's before any warm timing; a changed answer rejects the variant.

## What a report records

- per query: one cold run (fresh session, empty metadata cache) and several warm runs, the
  warm median, the result digest and row count, the physical plan with metrics, and — in
  object-store mode — the reads attributed to it (HEADs, `get_opts` reads, coalesced
  ranges, requested bytes) via an instrumented `ObjectStore`;
- the reader A/B switches in force (`enable_page_index`, `pruning`, `pushdown_filters`,
  `reorder_filters`, `bloom_filter_on_read`) — a writer feature is not credited when its
  reader is off;
- the identity envelope binding the report to the exact snapshot, variant, and suite
  digests, plus tool versions and run order.

Object-store I/O is measured, never inferred from output file size. Reports are written
atomically and refuse to overwrite without `--replace`.

## Selectivity probes (Task 47B)

```text
parquet-lab-bench compile-suite \
  --manifest CONTROL.json --queries QUERIES.sql --probes PROBES.toml --output SUITE.json
```

Probes are declared by family + column + target band and *compiled* against the immutable
control: the compiler materializes a typed literal that realizes a usable band, renders the
predicate SQL, and freezes the exact answer digest, control row count, match count, and the
**observed** selectivity. A requested probe that cannot be compiled is recorded with one
stable reason (`column_missing`, `no_literal_in_band`, `unsupported_type`, `empty_control`)
— never silently dropped — and a non-empty request that compiles nothing is refused (Blocks
A/D/E/F require a probe set; Block E requires a non-packing-key equality probe like
`eq_event`). Probe/query answers are compared under declared **result semantics**:
`ordered` (scalar aggregates, deterministic `ORDER BY`) or `multiset` (a multi-row answer
whose row order is not part of the result, compared order-independently but preserving
duplicate counts). Probe request files live under `bench/config/parquet-lab/probes/`.

# parquet-lab-snapshot

Freeze an explicit set of Parquet files into a verified, immutable **snapshot** — the
product control for the plan-47 parquet storage laboratory. A snapshot is the *exact
bytes* a source produced, copied unchanged, with per-file digests, a physical
`row-multiset/v1` fingerprint (when frozen from a Ukiel load), and a
`logical-row-multiset/v1` fingerprint every downstream variant must reproduce.

## Commands

```text
# Freeze a converged plan-46 compacted load (service-aware: catalog + object store).
parquet-lab-snapshot from-ukiel --receipt FILE --config FILE --output DIR [--replace]

# Freeze an explicit, sorted local Parquet file list (fully offline).
parquet-lab-snapshot from-files \
  --schema FILE --files-from FILE --logical-schema FILE --output DIR [--replace]

# Re-check a published snapshot against its manifest (offline).
parquet-lab-snapshot verify --manifest FILE
```

- `--schema` is the declared logical/table schema JSON, stored verbatim in the manifest.
- `--files-from` is a text file of source Parquet paths, one per line, in sort order.
  A directory is never listed implicitly — a snapshot is a *chosen* set of bytes.
- `--logical-schema` is an ordered `{ "columns": [{ "name", "logical" }] }` declaration
  matching the physical column order, where each `logical` is a name
  `parquet-lab-integrity` accepts (`int64`, `timestamp_ms`, `date`, `utf8`, `bool`,
  `float64`).

## Guarantees

- `from-ukiel` refuses a load that has not converged (any L0, any multi-run partition, a
  wrong row census, or a part missing the artifact marker) and refuses to publish unless
  the downloaded objects fingerprint to the staged input the receipt records. It
  compacts, vacuums, queries, and mutates nothing.
- Output is published atomically: files are written to a sibling temp tree, re-read and
  proven byte-identical, and the tree is renamed onto the output only after every digest
  and fingerprint closes.
- `from-files` and `verify` need no PostgreSQL, object store, Kafka, or Ukiel service.
- `verify` recomputes the logical fingerprint over the frozen rows, so an alteration that
  also rewrote a recorded digest still fails.

The offline paths are usable on any explicit Parquet dataset — the bounded ClickBench
slice, or anyone's files outside Ukiel.

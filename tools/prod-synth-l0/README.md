# prod-synth-l0 — the L0 staging runbook

Restages a plan-45 `prod-synth` fixture as Ukiel-shaped **level-0** Parquet: the same
rows, regrouped into UTC-day partitions and flush-sized files, so the real compactor
(fed by `ukiel-prod-load compaction-input`) can turn them into final parts whose shape
plan 46 measures.

It is **offline** — no database, object store, Kafka, HTTP, or DataFusion — and
installs with only a Rust toolchain. It is a transformer, not a loader and not a
generator.

## What it does, exactly

1. Verifies the source manifest, topology, and every source Parquet part by digest.
2. Reads the source rows in manifest/file order.
3. Cuts them into deterministic flushes of `--flush-rows` rows.
4. Splits each flush by the **UTC day** of its `timestamp` column.
5. Sorts each day slice by the table sort key.
6. Writes each slice as one level-0 Parquet file with Ukiel's own L0 writer properties
   (LZ4_RAW, the sort-key stamp, the delta-packed timestamp, opt-in blooms).
7. Emits `l0-manifest.json` only after every file is closed, measured, digested, and
   both the **row census** and the **order-independent row fingerprint** match the
   source.

Output: `parquet/<YYYY-MM-DD>/flush-<NNNNN>.parquet` plus `l0-manifest.json`. Each file
is one independent L0 run — the loader commits it separately, so the real ladder and
finalizer, not a test shortcut, do the merging.

## Use

```bash
prod-synth-l0 stage \
  --manifest bench/datasets/prod-synth/baseline/manifest.json \
  --output   bench/datasets/prod-synth-l0/baseline \
  --flush-rows 100000
```

Stage **once per tier** and reuse the artifact for every placement arm — otherwise the
experiment changes both the input run geometry and the output placement at once and
cannot attribute the result. Smoke uses `--flush-rows 10000` deliberately, to create
several runs per UTC day.

## Determinism and refusal

Same source digest + same `--flush-rows` → byte-identical L0 files. A smaller flush cuts
more files over the same rows; the fingerprint is unchanged because the rows are.

It fails loudly on: an existing output without `--replace`, a source part whose digest
does not match the manifest, an unknown source version, a timestamp outside the fixture
window, or an internal census/fingerprint mismatch (which would mean the regrouping lost
or duplicated a row — a bug, never tolerated).

## What the source's ClickHouse partition hash does here

Nothing. The UTC day is derived from the generated `timestamp` column. The source's
anonymized monthly partition hash stays in the source manifest as provenance and chooses
no output directory, day, file, or compaction group.

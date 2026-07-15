# ukiel-prod-load — the loader runbook

Loads one `prod-synth` manifest into an explicitly selected Ukiel deployment.

**This is the only mutating tool in the pipeline.** It writes to a catalog and an
object store, and it does nothing else: it cannot parse a profile, compile a topology,
generate a row, or benchmark anything. If the fixture is missing or its digests do not
match, it fails — it never makes one.

## Mutation scope

Everything it touches is declared by the command you ran.

| command | catalog | object store |
|---|---|---|
| `materialized` | creates `prod_synth_events_<label>`, one `events` logical table per queryable tenant, one `Add` commit, N part rows | uploads N Parquet objects under `prod-synth/<label>/` |
| `catalog-only` | the same catalog rows, with `size_bytes = 0` and `catalog-only://` paths | **nothing** |

It never deletes a part, never runs a compaction, and never touches a hypertable it did
not create. Labels are not reusable: a fixture loaded over another produces a catalog
that is a mixture of two seeds, and no report drawn from it means anything.

## Materialized

```bash
ukiel-prod-load materialized \
  --manifest bench/datasets/prod-synth/baseline/manifest.json \
  --label baseline \
  --config ukield.example.toml
```

Real objects, real catalog metadata, through the product's ordinary write path. The
`column_stats` are built by `ukiel_core::stats` — the same builders ingest and compaction
use — from the **actual Arrow batches read back out of the uploaded files**, not from the
topology and not from the manifest.

That indirection is the point. If the loader took the key set from the graph, it would be
asserting that the file contains what the graph says it contains, which is exactly the
claim under test. Reading it back means the roaring bitmap, and therefore the catalog's
issue-0014 Bloom filter, is derived from the bytes that are really in the object store.

Before it reports success it checks, independently:

- **object HEAD = manifest = catalog `size_bytes`** — three measurements of the same bytes;
- **actual rows = manifest = catalog `row_count`**;
- **zero false negatives** — every part that holds a tenant is returned for that tenant,
  across the deterministic sample. This is the only invariant that, if broken, makes rows
  silently disappear from query results with nothing in any log.

## Catalog-only

```bash
ukiel-prod-load catalog-only \
  --manifest bench/datasets/prod-synth/smoke/manifest.json \
  --label geo --config disposable.toml --ephemeral --keep
```

The same truthful topology, no objects. It measures catalog membership geometry — range
candidates versus filter candidates versus exact members — which needs the graph and
nothing else. Seconds instead of minutes.

What stays truthful: the parts, their key ranges, their exact member sets, and therefore
the roaring bitmaps and key filters the catalog derives from them. **Those are the thing
under measurement**, and it reproduces the materialized arm's geometry exactly (there is a
test).

What is deliberately fake, and cannot be mistaken for real: `size_bytes = 0`, and
`catalog-only://` paths that no object store can open. Nothing here says anything about
scans or bytes, and the tool says so on every run.

### Why it insists on a disposable stack

These part rows point at objects that do not exist. A compactor that picks one up will try
to merge a file that was never written; a GC sweeper will try to reap it. Neither failure
is dangerous, but both are confusing, and the confusion lands on whoever is next to look at
that deployment.

So: `--ephemeral` is mandatory, the command refuses to run against a config that gives
anything the `compactor` or `gc` role, it cleans up its own hypertable, commits and parts on
success, and attempts the same cleanup on failure. **If cleanup itself fails, it prints the
exact SQL to finish the job by hand.** A failed cleanup is never a shrug.

Pass `--keep` when a benchmark run intends to measure the seeded catalog —
`ukiel-prod-bench catalog` needs it to still be there. You then own the cleanup, and the
tool prints the command.

## Compaction-input (plan 46)

```bash
ukiel-prod-load compaction-input \
  --l0-manifest bench/datasets/prod-synth-l0/baseline/l0-manifest.json \
  --label baseline-packed --config bench/config/prod-synth-compactor.toml \
  --receipt out/baseline-packed/receipt.json \
  --placement packed|separated|size-targeted [--target-file-mb 256] \
  --ephemeral [--allow-hypertable NAME ...]
```

Loads a **staged L0 artifact** (`prod-synth-l0`) as real compaction input, so a live
compactor can turn it into final parts. It differs from `materialized` in three ways
that all matter:

- **Level 0, one commit per file.** Each staged file becomes its own `created_by_commit`
  — an independent L0 run. A single bulk commit would make the whole fixture one run and
  the compactor would have nothing to merge. This is the load-bearing difference:
  measuring the *compactor's* output requires feeding it the ladder's actual input.
- **A real UTC-day partition.** Every part's `partition_values` carries
  `{l0_manifest, utc_day}`. Compaction preserves partition values, so a final part still
  carries the marker and the runner can prove it descends from this exact staged
  artifact — even though REPLACE has destroyed every original path and count. (The
  source's ClickHouse partition hash still chooses nothing.)
- **The selected placement** is set on the hypertable, so the compactor's size cuts and
  key handling are the ones under test.

Metadata is derived from the uploaded bytes through the product's own accumulators — no
precomputed key filter is trusted. Object HEAD, catalog, and manifest rows/bytes are
each measured independently and must agree.

### Safety

`--ephemeral` is mandatory. The load also refuses a catalog holding any hypertable not
named `prod_synth_events_*` unless you pass `--allow-hypertable NAME` for each — a
stranger's table means this is probably not the disposable stack you meant. A **fresh
label** is required; a partial pre-existing load is dropped deliberately, never resumed.

The receipt is published **atomically, only after the final successful commit**. A
failed load leaves no receipt and prints the exact reset SQL.

One fixture per catalog: logical tables are keyed by `(namespace_id, name)` globally, so
a tenant's `events` table can belong to only one fixture per catalog. Each placement arm
gets its own disposable stack — which is exactly what the plan-46 matrix prescribes.

## Note on the schema

The catalog schema is a single initial migration, applied to an empty database. A
PostgreSQL instance that was migrated by an older Ukiel will refuse to start with
`migration 2 was previously applied but is missing in the resolved migrations` — that is
correct, and the fix is a fresh database, not a downgrade.

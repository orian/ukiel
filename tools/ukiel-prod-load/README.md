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

## Note on the schema

The catalog schema is a single initial migration, applied to an empty database. A
PostgreSQL instance that was migrated by an older Ukiel will refuse to start with
`migration 2 was previously applied but is missing in the resolved migrations` — that is
correct, and the fix is a fresh database, not a downgrade.

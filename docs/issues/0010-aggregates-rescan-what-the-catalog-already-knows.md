# 0010 — Metadata-answerable aggregates rescan data the catalog already holds

- **Severity:** Low (performance, not correctness — but it is the single worst ratio in the official benchmark, by a wide margin)
- **Status:** Open. No roadmap row claims it yet
- **Components:** `crates/ukiel-query` (provider / plan rewriting), `crates/ukiel-catalog`
- **Found by:** plan 32's ClickBench run against the raw-DataFusion reference, 2026-07-12 (`bench/README.md` §6)

## Summary

Some aggregates need no data at all — their answer is fully determined by
statistics ukiel **already stores in Postgres**. Ukiel scans the Parquet anyway.

The official benchmark makes this loud. On the 100M-row `hits_cb` fixture, hot
minimum, versus bare DataFusion over the same files:

| query | ukiel | raw DataFusion | ratio |
|---|---|---|---|
| q07 `SELECT MIN("EventDate"), MAX("EventDate") FROM hits_cb` | 78.7 ms | **1.0 ms** | **78×** |
| q01 `SELECT COUNT(*) FROM hits_cb` | 4.8–60 ms | **1.0 ms** | 5–60× |

DataFusion is not doing anything clever: it reads the min/max out of the Parquet
column statistics and the row count out of the row-group metadata, and never
opens a page. It is faster than ukiel *because it looks at less*.

The irony is that ukiel is the one holding the answer. `parts.column_stats`
(plan 12) carries per-part Int64 min/max for exactly these columns, and
`parts.row_count` carries the count — both in Postgres, both already read during
planning to prune parts. A `MIN`/`MAX` over a stats-carrying Int64 column, or a
`COUNT(*)` with no filter, is one SQL aggregate over rows the planner has in hand.

## Why it exists

`UkielTableProvider::scan` returns a `DataSourceExec` over the pruned file list
and lets DataFusion do the rest. It never inspects the *shape of the query* — it
sees a projection, filters, and a limit, and there is no seam for "this
aggregate is answerable from the catalog." The provider's job today ends at
producing rows.

## Sketch of a fix (not a plan)

DataFusion's `TableProvider` has no aggregate-pushdown hook, so this wants either:

- an **optimizer rule** in the session (`SessionState::add_optimizer_rule`) that
  rewrites `Aggregate(MIN/MAX/COUNT) → Values(...)` when every input part carries
  the needed stat and the scan has no filter (or only filters already fully
  resolved by part pruning), or
- a provider-level check in `scan` that recognizes the degenerate plan shape and
  returns a one-row `MemoryExec`.

Both are only sound when **every** candidate part has the stat (a part with
`column_stats = NULL` — written before plan 12, or by a path that skips stats —
must force the scan; the existing pruning code already treats missing stats as
"survives", and the same conservatism applies here). `COUNT(*)` additionally
requires that no rows are filtered *and* that deletion tombstones cannot make
`row_count` stale — check that against the delete path before trusting it.

Note the namespace-scoped case is the harder and more valuable one: a tenant's
`COUNT(*)` is `SELECT sum(row_count) FROM parts WHERE …` only if the parts are
**dedicated** to that key (`packing_key_min = packing_key_max = key`); packed
parts hold other tenants' rows and their `row_count` says nothing about this
tenant. So the win is exact for dedicated/whale-split layouts and unavailable for
packed ones — which is itself an argument for SizeTargeted placement.

## Why it is not urgent

No customer query shape in the product suite (`bench hits queries`) is dominated
by a bare `MIN`/`MAX`/`COUNT(*)` over a whole hypertable; the multitenant path
filters by packing key and reads real rows. This is a benchmark-visible gap and a
genuine one, but it is a *ceiling* issue, not a hot path — recorded so the number
is not mistaken for something inherent about the architecture. It is not: the
data is already in Postgres, unused.

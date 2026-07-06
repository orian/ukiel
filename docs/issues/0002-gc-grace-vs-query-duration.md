# 0002 — GC tombstone grace is not enforced against query duration

- **Severity:** Medium (correct failure, not corruption — but unbounded queries can break)
- **Status:** Open — fix planned: plan 19 (`docs/superpowers/plans/2026-07-06-ukiel-safety-rails.md`), statement timeout + startup invariant check
- **Components:** `crates/ukiel-gc`, `crates/ukiel-query`
- **Found by:** design review, 2026-07-06

## Summary

GC protects running queries from object deletion **probabilistically**: a
tombstoned part's object is deleted no earlier than `tombstone_grace`
(default 900s) after the deleting commit, so any query that planned against
the old file layout has the grace window to finish reading. The safety
invariant is therefore:

```
max_query_duration < tombstone_grace
```

Nothing enforces the left-hand side. `ukiel-query` has no statement timeout,
so a query running longer than the grace can hit a deleted object mid-scan.

## Impact

The failure mode is clean — object-store `NotFound` → query error; a retry
replans against the current live parts and succeeds. Results are never wrong
and never partial. But long analytical queries become unreliable, and the
invariant lives only in reviewers' heads.

(Feed consumers are not affected: the reaper fences on `worker_cursors`, which
is precise. Orphan sweeping cannot affect queries at all — orphans were never
referenced by any plan.)

## Fix

Recommended (cheap, sufficient):

1. Add a statement timeout to `ukiel-query` (config, e.g. 300s): wrap
   `df.collect()` in `tokio::time::timeout` and surface a clear "query
   exceeded time limit" error. Needed regardless for runaway-query/quota
   protection in a tenant-facing SQL product.
2. Validate the invariant at deployment wiring: refuse (or loudly warn) if
   `query_timeout >= gc.tombstone_grace`.

Later, if long-running queries become a product feature: query-node leases in
the catalog (each query node heartbeats its oldest running query's plan time;
the reaper's effective grace = `max(configured_grace, now - min(lease))`),
so hour-long scans don't force an hour-long grace globally.

## Notes

- Same trade-off as Iceberg snapshot expiration / Delta `VACUUM` retention:
  time-window safety, not reference counting.
- The local-disk read-through cache can mask the failure (file still cached
  after deletion) — do not rely on it; it is per-node and evictable.

# 0012 — The deterministic compaction candidate window can starve later partitions

- **Severity:** Medium (fleet fairness and backlog convergence; no data-correctness impact)
- **Status:** Open
- **Components:** `crates/ukiel-catalog`, `crates/ukiel-compactor`
- **Found by:** post-implementation review of plan 41, 2026-07-13

## Summary

Plan 41 discovers compaction work from current live state, which correctly keeps
abandoned work visible after a shared cursor advances. Candidate selection is
nevertheless a deterministic prefix:

```sql
ORDER BY partition_values::text
LIMIT $candidate_limit
```

The default limit is 64. Every replica asks for the same prefix and only then
tries to lease each partition. The candidate query neither excludes unexpired
leases nor rotates fairly across the eligible set.

This has two related failure modes:

1. A deeply backlogged or continuously hot partition can remain eligible after
   the one-merge-per-pass rule processes it. If the first 64 ordered partitions
   remain eligible, later partitions never enter a subsequent window.
2. Partitions already leased by peers still consume window slots. Replicas pay
   acquisition round trips to skip them instead of discovering claimable work
   farther into the backlog.

Live-state discovery prevents work from becoming permanently invisible, but a
bounded deterministic prefix does not guarantee that every visible item is
eventually scheduled.

## Impact

Backlog convergence and fleet utilization can depend on the lexical order of a
partition's JSON representation. Later partitions may wait indefinitely behind
the same hot prefix even though they are independent and safe to compact in
parallel.

The current deployment is unlikely to hit the 64-way concurrency ceiling
immediately, but 100–500GB partitions make individual leases long-lived, and a
PB-scale backlog can keep early partitions eligible across many passes. This is
precisely when fair work distribution matters.

The Plan 41 contention result remains valid: leases eliminate duplicate work
inside a partition. This issue concerns selection *between* partitions.

## Proposed resolution

Make discovery lease-aware and fair while keeping current live parts as the work
ledger.

1. Exclude partitions with an unexpired lease from the candidate query, using
   PostgreSQL time. Acquisition must remain the atomic authority because the
   query and claim can race.
2. Replace the permanent lexical prefix with a continuation cursor or stable
   rotating order per hypertable. A sweep should wrap after the last partition;
   a crash must not make the tail unreachable.
3. Overscan is acceptable as a short-term mitigation, but not as the fairness
   invariant: any fixed overscan can still be occupied by a sufficiently deep
   hot prefix.
4. Keep deterministic behavior available in tests, and expose enough metrics to
   see candidate count, claimable count, held skips, and sweep wrap/progress.

An atomic `claim_next_compaction_candidate` API is another valid design if it
selects from current live state, never holds a transaction during object I/O,
and retries a raced claim without repeatedly choosing the same loser.

## Acceptance criteria

- With more than `candidate_limit` eligible partitions and the first window
  still eligible after every pass, all tail partitions are eventually returned
  or claimed.
- With the first window covered by unexpired peer leases, another worker can
  discover an unleased partition beyond it in the same sweep/tick.
- Concurrent replicas still produce one owner per partition and preserve the
  plan-41 zero duplicate-object-work property.
- Candidate selection remains bounded and uses an index-supported query plan at
  representative live-part cardinality.
- Cursor progress remains observability/wake progress only; no fairness fix may
  make the change feed the compaction work ledger again.

## Notes

- Random ordering on every query is not sufficient by itself: it is expensive,
  hard to reproduce, and gives probability rather than bounded eventual
  progress.
- Finalization uses the same partition lease but currently has a different,
  unbounded candidate query. A unified fair selector may be worthwhile, but the
  finalization quietness predicate must remain intact.

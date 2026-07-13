# 0012 — The deterministic compaction candidate window can starve later partitions

- **Severity:** Medium (fleet fairness and backlog convergence; no data-correctness impact)
- **Status:** Resolved — the candidate query is lease-aware and resumable, and both sweeps rotate a per-hypertable cursor through the whole eligible set. Migration `0009_compaction_candidate_index.sql` makes the selection index-only.
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

## Fix

One selector (`window_sql` in `crates/ukiel-catalog/src/parts.rs`), shared by
both sweeps so neither can quietly regain an unbounded or lease-blind query. It
returns a `CandidateWindow`: the claimable partitions after a cursor, the cursor
to resume from, and the counts (`eligible`, `leased`) that make the sweep's
health visible.

1. **Lease-aware.** Partitions a *peer* holds under an unexpired lease are
   excluded, using PostgreSQL time. The predicate is the exact complement of
   what `try_acquire_compaction_lease` accepts — unexpired *and* someone else's
   — so a worker still sees its own leases. Hiding them locked a process rebuilt
   after a blip out of the work it already owned until its own TTL ran down; two
   existing recovery tests caught it. Acquisition remains the atomic authority:
   the query only stops a window slot being spent on work this owner provably
   cannot take.
2. **Resumable.** `after` is an exclusive lower bound in the sweep's own order,
   and the window carries Postgres's `partition_values::text` for its last row
   as the next cursor (never rebuilt in Rust — `jsonb` text rendering is
   Postgres's, and it is what the ordering compares).
3. **Rotating.** The compactor keeps an in-memory cursor per hypertable per
   sweep and advances it past everything the window offered, claimed or not, so
   a permanently-eligible head cannot pin the window. A short window means the
   set is spent: the cursor resets to the head. Running off the tail is retried
   from the head in the same tick, so a wrap costs no idle pass. In memory on
   purpose — this is scheduling fairness, not durable state; a restart resumes at
   the head, which is exactly what a fresh process has no reason to skip.
4. **Finalization** shares all of it, and its quietness predicate moved *into*
   the eligibility query. This is load-bearing, not tidying: ">= 2 live runs"
   alone describes nearly every partition that has been written to and not yet
   finalized, hot ones included, so a window bounded on that set would be filled
   with partitions it can only claim, probe and discard — and a genuinely cold
   partition would wait for the cursor to rotate past all of them. The old sweep
   got away with it by being unbounded; a bounded one cannot. The post-claim
   recheck stays as the race guard.
5. **Metrics.** `compactor_candidates_{eligible,claimable,leased}{kind}` (the
   backlog, what this fleet can take, what peers hold),
   `compactor_candidates_offered_total{kind}` (sweep progress) and
   `compactor_sweep_wraps_total{kind}` (rotations completed — a large backlog
   whose wrap count stays flat is a rotation that is not finishing).

Selection stays deterministic: lexical order plus a cursor, no randomness.

Migration `0009` was necessary to keep acceptance criterion 4 honest. The
candidate queries count *distinct runs*, so they need `created_by_commit`
alongside the grouping columns; the 0007 index (which the old docstring credited)
covers neither that nor the liveness predicate, so the sweep was in fact
sequentially scanning every live part and spilling an external merge sort to
disk. Measured on 400k live parts / 5k partitions: the ladder candidate query
1005ms → 165ms (index-only, no spill); finalization 978ms → 405ms with no spill,
and it now returns only the partitions that can actually be finalized instead of
every partition with 2+ runs — which also removes a claim and a
`partition_l0_quiet_since` round trip per hot partition, per sweep. The
collector's `compactor_backlog_groups` gauge, same run counting every 30s across
all hypertables, drops to 72ms. Index: 26MB against a 247MB `parts` table.

## Notes

- Random ordering on every query is not sufficient by itself: it is expensive,
  hard to reproduce, and gives probability rather than bounded eventual
  progress.
- The fleet cursor in `worker_cursors` is untouched. The sweep cursor is a
  different thing entirely: process-local scheduling state, never consulted to
  decide whether work *exists*. Discovery is still current live state.

# 0015 — Change-feed cursors may skip transactions that commit out of ID order

- **Severity:** High (confirmed: a feed consumer could permanently omit committed input)
- **Status:** Resolved — shared-writer/exclusive-reader publication barriers prevent incomplete feed prefixes
- **Components:** `crates/ukiel-catalog/src/commit.rs`, `crates/ukiel-catalog/src/feed.rs`, `crates/ukiel-catalog/src/cursors.rs`
- **Found by:** source review during the Clicklake comparison, 2026-10-21; checkout `6932d8b`

## Summary

The catalog allocates a commit ID inside the write transaction, before that
transaction commits. The change feed reads visible rows with `id > cursor`,
ordered by ID. Sequence allocation order does not establish transaction commit
order, so a consumer may advance past an ID whose transaction is still running.

Evidence in the reviewed checkout:

- [The schema](../../crates/ukiel-catalog/migrations/0001_init.sql) gives
  `commits.id` a `GENERATED ALWAYS AS IDENTITY` value.
- [`commit_txn`](../../crates/ukiel-catalog/src/commit.rs) inserts the commit
  row before applying parts and offsets, then commits the transaction. There
  is no common per-hypertable serialization fence across all write paths.
- [`changes_since` and `feed_head`](../../crates/ukiel-catalog/src/feed.rs)
  use `id > after` and `max(id)` respectively.
- [`set_cursor`](../../crates/ukiel-catalog/src/cursors.rs) only advances;
  it cannot revisit a lower ID that becomes visible later.

This was initially a source-level hypothesis. The follow-up below reproduces
the loss deterministically and implements a publication barrier.

## Impact

A consumer treating its cursor as a complete observed prefix could permanently
miss an ADD, REPLACE or DELETE. This matters for planned feed-driven materialized
views, bootstrap-at-head, and any use of a cursor as permission to reclaim input.
Audit those uses rather than assuming that monotonic cursor updates prove that
all preceding commits were observed.

The current [compactor](../../crates/ukiel-compactor/src/compactor.rs) discovers
work from current live state and explicitly does not use its feed cursor as a
completion ledger. This finding does not demonstrate lost compaction work,
incorrect direct query results, or a current GC data-loss incident. Part and
offset atomicity remain separate properties.

## Proposed resolution

First reproduce this schedule with two independent transactions on the same
hypertable and disjoint inputs/offset lanes:

1. A allocates commit ID N and pauses before commit.
2. B allocates N+1 and commits.
3. A consumer reads B and persists cursor N+1.
4. A commits; the consumer polls again from its persisted cursor.

Use explicit barriers, not timing sleeps. Check whether A is delivered. Repeat
with A rolling back: identity gaps are legal and cannot simply block the feed
forever.

Then choose a feed contract that prevents a cursor from passing an unresolved
transaction. Options include serializing ID allocation through transaction
completion per hypertable, or introducing a proven publication watermark/log.
If serialization is selected, acquire the fence before allocating the ID and
hold it through commit; measure the effect on concurrent catalog writes.

`ORDER BY id`, a repeatable-read snapshot, monotonic cursor updates, and an
arbitrary polling delay alone do not fix a transaction that commits behind an
already persisted cursor. Every mutation path must follow the selected contract.

## Acceptance criteria

- Deterministic coverage demonstrates the original schedule and verifies that
  every committed event is eventually delivered after consumer restart.
- A rolled-back lower-ID transaction does not stall later events indefinitely.
- Ingest, compaction, deletion and operator commits obey the same feed contract.
- `feed_head`, bootstrap-at-head and GC cursor fences use a documented safe
  prefix; any intentionally weaker observational metric is identified as such.
- Concurrent write throughput and latency are measured if publication is serialized.

## Fix and verification (2026-10-21)

The original implementation loses a committed event on PostgreSQL 17. A test
trigger pauses a real ingest transaction immediately after ID allocation,
before even the commit row's foreign-key check. A second ingest transaction on
a disjoint Kafka partition commits ID 2; the consumer persists cursor 2; the
first transaction commits ID 1. A new catalog pool reloads the cursor and
returns an empty tail. Synchronization uses database locks and
`pg_blocking_pids`, without timing sleeps.

The fix keeps concurrent writers and makes feed reads establish a complete
prefix:

1. Every `commit_txn` takes `pg_advisory_xact_lock_shared(~hypertable_id)`
   **before** inserting the commit row. It holds the lock through commit or
   rollback. Ingest, ordinary ADD/REPLACE/DELETE, and leased compaction all use
   this path. Negative one-bigint advisory keys are reserved for this protocol.
2. `feed_head` takes the exclusive counterpart in an explicit READ COMMITTED
   transaction. After that lock is granted, a **separate** statement reads
   `max(id)` and the transaction releases the lock.
3. `changes_since` captures that safe head and only selects `after < id <= head`.
   It releases the barrier before loading parts. The upper bound matters:
   later writers may already be allocating and committing new IDs while the
   page is constructed.
4. The identity sequence explicitly uses `CACHE 1`. Sequence gaps remain legal;
   no consumer waits for absent integer IDs.

Why this is safe: when the reader obtains the exclusive lock, every writer
that could have allocated an earlier ID has committed or rolled back. The next
statement's snapshot sees the committed rows. After the reader releases its
lock, all new allocations are greater than the captured head. The head is
therefore a complete prefix for this hypertable, even though transaction
completion itself can still occur out of ID order.

The protocol relies on PostgreSQL's
[transaction-scoped advisory locks](https://www.postgresql.org/docs/17/explicit-locking.html#ADVISORY-LOCKS),
[fresh READ COMMITTED statement snapshots](https://www.postgresql.org/docs/17/transaction-iso.html#XACT-READ-COMMITTED),
and [cross-session sequence ordering with CACHE 1](https://www.postgresql.org/docs/17/sql-createsequence.html).
An implicit FK lock is too late, a snapshot taken before the wait is unsafe,
and caching several sequence values per connection breaks the proof.

The tests in
[`feed_ordering_test.rs`](../../crates/ukiel-catalog/tests/feed_ordering_test.rs)
cover both feed APIs, commit and rollback, pagination and consumer restart,
repeatable-read database defaults, independent hypertables, offset atomicity,
and pre-allocation locking for all five mutation paths. They also require the
second writer to finish while the first is paused, preserving concurrent writes.

Validation: `rtk cargo test -p ukiel-catalog -p ukiel-compactor -p ukiel-gc -- --test-threads=2`
passed **154 tests**, with the manual benchmark ignored in that run. The
benchmark was run separately for each implementation. The regression fails on
the original checkout `6932d8b` and passes with the fix.
`rtk proxy cargo clippy -p ukiel-catalog --all-targets -- -D warnings` also
passed.

### Measured tradeoff

This initial comparison predates the explicit catalog lock timeout added below.

The manual `feed_publication_benchmark` uses disposable PostgreSQL 17 containers
on a local Ryzen AI 9 HX 370 (24 logical CPUs), a 64-connection pool, one part
per unkeyed ADD, and three 2-second trials per scenario. A feed poller scans
the heads and pauses 20 ms between scans. These are closed-loop local metadata
measurements, without object-store I/O, large batches, or production network
latency; they establish a contention tradeoff, not deployment capacity.

For 16 writers on one hypertable, with polling, the medians of the three
per-run measurements were:

| Implementation | Commits/s | Commit p99 (ms) | Feed-head p99 (ms) |
|---|---:|---:|---:|
| Original (unsafe) | 17,641 | 1.403 | 0.506 |
| Exclusive writer serialization | 2,396 | 9.209 | 9.315 |
| Shared writers, exclusive feed barrier | 14,818 | 2.051 | 2.062 |

The selected barrier cost about 16% throughput against the unsafe baseline and
retained roughly 6.2 times the serialized alternative's throughput in this
scenario. With eight hypertables and polling, the corresponding throughputs
were 17,325, 11,086, and 14,940 commits/s. The final candidate without polling
reached 15,353 commits/s for 16 writers on one hypertable.

Run the final implementation's experiment with:

```bash
rtk proxy cargo test -p ukiel-catalog --test feed_ordering_test feed_publication_benchmark -- --ignored --nocapture
```

### Timeout follow-up benchmark (2026-10-21)

A matched follow-up on PostgreSQL **17.6** retained the safe shared-writer
barrier in both arms and changed only the server lock timeout: disabled in the
disposable control sessions versus the configured **2 seconds**. Application
configuration still refuses zero. Both arms prewarmed 64 sessions and ran each
scenario for five seconds, with three repetitions and alternating arm order.
These were local debug-build, closed-loop measurements on the same Ryzen machine;
the earlier two-second, initially cold-pool numbers are a separate experiment.

Median completed commits/s across the three repetitions:

| Writers | Hypertables | Head polling | Timeout disabled | Timeout 2 s | Change |
|---:|---:|---|---:|---:|---:|
| 1 | 1 | No | 2,359 | 2,283 | −3.2% |
| 16 | 1 | No | 15,588 | 15,424 | −1.1% |
| 16 | 1 | Yes | 14,936 | 14,767 | −1.1% |
| 16 | 8 | Yes | 15,446 | 15,044 | −2.6% |

For the hot hypertable with polling, the medians of per-run commit p99 were
2.078 ms without the timeout and 2.099 ms with it; feed-head p99 was 2.614 ms
and 2.210 ms respectively. The throughput ranges overlapped: 14,623–15,056
versus 14,643–14,997 commits/s. All normal-load calls succeeded. Three short
repetitions do not establish statistical significance for these small changes
or a production capacity limit; they show no large additional throughput cost
from enabling the timeout in this workload.

The second experiment paused a real writer A immediately after ID allocation,
queued a feed reader behind it, then queued writer B behind that reader.
Database barriers acknowledged the schedule before the controlled three-second
hold. A used a fixed ten-second budget; the reader and B used either the
default two-second budget or a ten-second control. Each arm ran three times,
with alternating execution order. Medians:

| Reader/B lock budget | Feed outcome | Feed latency | B outcome | B latency | B completed before releasing A |
|---|---|---:|---|---:|---:|
| 2 s | Timeout, 3/3 | 2,001.645 ms | Committed, 3/3 | 2,003.852 ms | 3/3 |
| 10 s control | Success, 3/3 | 3,008.520 ms | Committed, 3/3 | 3,011.803 ms | 0/3 |

The two-second reader timeout removed the exclusive waiter and allowed B to
commit while A was still paused. A remained alive for the full stall. The
consumer cursor stayed unchanged during every stall; both committed events
were delivered after release in all six cases. Unrelated-hypertable writes
took 2.557–3.131 ms and ordinary live-part catalog reads 0.532–1.045 ms across
these isolated runs. Subsequent healthy feed-head calls took 0.690–2.604 ms;
that last measurement is a fresh call after the queued operations finished,
not an end-to-end worker-recovery or retry-backoff SLO.

This quantifies the bounded queueing benefit, while preserving the limitation:
the timeout does not terminate the stalled transaction or make the feed
available before that transaction resolves. The experiment used one queued
reader/writer and separate available connections. Larger fleets, repeated
polling/recovery, pool exhaustion, large transactions, and production network
latency still require broader measurements before making capacity claims.

Commands and controls are in the
[benchmark runbook](../../bench/README.md#feed-publication-and-lock-timeouts-issue-0015).
Local raw logs, the source/environment receipt, and the per-trial JSON summary
are under the ignored directory
`bench/results/issue-0015/2026-10-21-timeout/`. The receipt records base checkout
`6932d8b`, the dirty source hashes, PostgreSQL image digest, compiler, commands,
and environment overrides. All seven benchmark invocations passed; Clippy
passed for the benchmark test target with warnings denied.

### Contract and remaining limits

- `catalog.lock_timeout_ms` defaults to **2,000 ms** and bounds each PostgreSQL
  lock acquisition on catalog connections, including shared writer and exclusive
  feed barriers. It is applied at connection startup for eager/lazy pools and
  replacement connections, without another per-commit query. Zero and values
  above PostgreSQL's signed-32-bit millisecond range are rejected.
- PostgreSQL cancels an over-budget lock wait with SQLSTATE `55P03`, classified
  as `RetryableDatabase`. Feed calls fail without returning an unsafe prefix or
  advancing cursors. A mutation that times out before COMMIT is rolled back;
  parts, offsets and operation identity remain atomic. Existing worker recovery
  backs off and rebuilds from authority. Commit-boundary transport failures
  retain their separate ambiguous-outcome semantics.
- An unfinished writer can delay a feed read; later writers can queue behind
  that reader. A timed-out reader leaves the queue so compatible writers can
  proceed. The setting also bounds waits for other catalog locks, including
  row locks and migration locks. It is **not** a whole-transaction, execution,
  network, or connection-acquisition deadline, and does not kill the transaction
  holding the contested lock. It cannot guarantee a healthy feed while a writer
  remains stuck. PostgreSQL documents the per-acquisition behavior under
  [`lock_timeout`](https://www.postgresql.org/docs/17/runtime-config-client.html#GUC-LOCK-TIMEOUT).
- All live writers must follow the protocol, including any future direct-SQL
  mutation tooling. Existing raw inserts are test fixtures or offline benchmark
  seeding, not alternative production mutation paths.
- `feed_lags` remains an observational ID-distance metric; its raw `max(id)`
  cannot authorize a cursor advance. Consumers use `changes_since` or
  `feed_head`. GC still fences on durable consumer cursors.
- A safe head makes starting at the current head well-defined. An atomic
  snapshot-plus-tail bootstrap and cursor registration/GC coordination still
  need their own contract when those features are implemented.
- This prevents future skips; it cannot repair a cursor that already skipped
  historical input. No deployed installations need an upgrade migration; the
  repository's single initial schema is updated in place.

Timeout regressions additionally verify eager/lazy/replacement connection
settings, override precedence over URL options, both feed APIs, connection reuse,
cursor preservation, writer retry after a barrier timeout, and atomic rollback
after a later row-lock timeout has already inserted parts inside the transaction.
The timeout follow-up passed **204 tests** with
`rtk cargo test -p ukiel-catalog -p ukiel-compactor -p ukiel-gc -p ukield -- --test-threads=2`
(one manual benchmark ignored). Clippy passed for both `ukiel-catalog` and
`ukield`, with all targets and warnings denied.

## Potential improvement: a durable publication watermark

The current barrier is the implemented correctness contract. A future design
could expose a durable **published prefix** without requiring a feed reader to
stop catalog writers. A poll could return the last safe prefix immediately
while newer work remains unpublished. Investigate this if measured polling
contention, lock timeouts, or catalog write cost justify the extra machinery;
it is not a prerequisite for Plan 8 or a promise of a throughput improvement.

One candidate is a transactional outbox: source mutations atomically record
their event, then a fenced publisher assigns feed positions to committed,
unpublished events and advances a durable watermark in batches. A transaction
that commits late can receive a later **publication position**, even if its
catalog commit ID was allocated earlier. Readers stop at the published
watermark, independently of transactions still writing source metadata.

This would require a design and proof before replacing the current barrier:

- Separate feed position from commit identity, or specify an equally rigorous
  representation; adapt pagination, durable cursors, bootstrap-at-head, feed lag,
  and GC fences consistently.
- Discover committed **unpublished** events. Merely polling an outbox identity
  column with `id > cursor` recreates issue 0015. An alternative allocation
  registry must prevent unregistered allocations and distinguish rollback from
  transactions that are still running; a registry hidden in the same uncommitted
  transaction cannot supply that information to readers.
- Publish event positions and the watermark atomically as a complete prefix.
  Fence concurrent publishers across failover; allocating another sequence
  before publication commit can reproduce the same ordering bug.
- Recover after crashes at source commit, publication, and watermark advancement
  without losing or duplicating logical events. Preserve ADD/REPLACE/DELETE
  dependencies and keep required objects GC-protected while publication lags.
- Bound and observe the unpublished backlog and specify publisher-outage
  behavior. The additional stage adds publication latency, catalog writes, and
  operational work even if it removes reader/writer barrier contention.
- Repeat deterministic concurrency, rollback, restart, bootstrap and GC tests,
  then compare throughput, tail latency and recovery under realistic transaction
  sizes, polling rates and network latency against the current bounded barrier.

## Notes

- Consider before implementing the feed-driven MV path in
  [Plan 8](../superpowers/plans/2026-07-07-ukiel-pipelines.md). That plan already
  has local edits; this review does not modify them.
- Operation idempotency and transactional Kafka offsets do not establish feed
  commit ordering. Their existing guarantees must survive any resolution.

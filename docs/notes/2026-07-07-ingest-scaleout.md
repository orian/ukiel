# Horizontal ingest: partition-subset leases (sketch, roadmap row 26)

Status: sketch. Plan written after plan 8 — leases belong to the pipeline
ingest worker (`PipelineIngest`), not the `RouteIngest` code it replaces.
Completes issue 0003's future-work half: the plan-19 offset CAS made the
single-writer assumption *safe* (duplicate writers die loudly); leases make
multiple writers *real* — ingest throughput scales with Kafka partitions
instead of being pinned to one process per topic.

The ownership seam this plugs into already exists in code:
`ukiel-ingest`'s assignment module (`desired_assignment(existing, ownership,
stored_offsets)`, `Ownership::All | Subset`). Today every worker passes
`Ownership::All`; this plan is "compute a `Subset` from catalog leases and
re-run the assignment each flush tick." Nothing in the offset/positioning
logic changes.

## Why not Kafka's own consumer-group rebalance

Kafka already solves membership, failure detection, and partition
distribution — `subscribe()` instead of explicit `assign()` would be *less*
code. We deliberately don't, but the honest reasons are narrower than "keep
offsets catalog-anchored" (you *can* `subscribe()` and still seek each newly
assigned partition to its catalog offset — the anchor survives either way):

1. **Async seek in a sync callback.** Both designs must seek every newly
   owned partition to its catalog offset — group offsets are never committed
   (`enable.auto.commit=false`), so `auto.offset.reset=earliest` would
   otherwise re-read the whole partition from 0 (safe under the CAS, but
   catastrophically wasteful). Under `subscribe()` that seek must happen in
   rdkafka's *synchronous* `post_rebalance` callback, where awaiting Postgres
   is awkward. Under explicit `assign()` the seek is already ordinary
   `await`-able code in our own control flow. This is the real ergonomic win.
2. **One ownership model, not two.** Leases put ownership and offsets in the
   same store (the catalog), fenced by the same transactions. Kafka rebalance
   would split coordination across two systems with two failure models.
3. **Fits the worker refactor.** Leases live in `PipelineIngest` (plan 8),
   which we control; rebalance semantics would be inherited baggage.

Tradeoff acknowledged: we build membership + failure detection + fair-share
ourselves (below) instead of getting them from Kafka. The offset CAS is what
makes that safe to hand-roll.

## Design

```sql
CREATE TABLE ingest_leases (
    topic           TEXT NOT NULL,
    kafka_partition INT  NOT NULL,
    worker_id       TEXT NOT NULL,        -- stable per process (host + uuid)
    expires_at      TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (topic, kafka_partition)
);
CREATE INDEX ingest_leases_expiry ON ingest_leases (expires_at);
```

- **Acquire/renew** — one CAS-shaped upsert per partition: insert, or update
  `WHERE expires_at < now() OR worker_id = $me`. TTL ≈ 3× flush interval.
- **Renewal is coupled to the *tick*, not to a successful flush.** This is
  the correction over the first draft. Renewal is a catalog write; it must
  fire whenever the catalog is reachable, *independent of whether parts were
  committed*. The ingest flush path already *defers* flushes under L0
  backpressure (`flush_decision` → `Defer`, plan 18) — a healthy but
  backpressured worker commits no parts for many ticks. Coupling renewal to
  commits would make that worker drop its leases; the partitions would bounce
  to peers under the *same* L0 pressure, churning without relief. Only a
  worker that cannot reach Postgres at all should lose its leases. So: renew
  on every tick when the catalog responds; a deferred flush still renews.
- **Fair share via rendezvous (highest-random-weight) hashing** — not
  greedy first-come CAS. Each worker independently computes, for every
  partition, `HRW(worker_id, partition)` over the set of currently-active
  workers (distinct unexpired `worker_id`s for the topic), and prefers the
  partitions where it ranks highest, up to `ceil(partitions / workers)`.
  Because every worker derives the same preference order from the same
  inputs, acquisitions and releases converge to a stable, balanced
  assignment that reshuffles *only* the partitions whose owner actually
  changed when the worker set changes — minimal churn, no coordinator, no
  partition tug-of-war. The CAS still arbitrates the transient overlap; the
  hash just stops workers from fighting over arbitrary partitions.
- **Consume** — the worker builds `Ownership::Subset(leased)` and calls the
  existing `desired_assignment(existing, ownership, stored_offsets)` →
  explicit `assign()` with each new partition seeked to its catalog offset.
  It re-checks its lease set each flush tick and re-assigns on change; flush
  commits only leased partitions' offset ranges (already per-partition).
- **Release on shutdown** — graceful stop deletes this worker's lease rows so
  peers pick up immediately instead of waiting out the TTL.
- **Failure** — worker dies → leases expire (≤ TTL) → peers' next rendezvous
  pass acquires them → resume from catalog offsets. Exactly-once untouched:
  offsets still move only inside part commits.

## Why safety does not depend on leases

The plan-19 offset CAS remains the correctness layer. Any lease anomaly —
clock skew, a zombie holding a stale lease through a GC pause, the brief
overlap while a partition changes hands — degrades to an `OffsetRace` on the
losing commit, never to duplicate data: the new owner seeks to the last
committed catalog offset, and whichever worker commits a range first wins the
`WHERE next_offset <= range.first` guard; the other rolls back. Leases are an
availability/efficiency mechanism; the CAS is the invariant. The plan's
adversarial test is exactly that: two workers, one forced-stale lease, assert
the zombie's commit errors and the data is single-copy.

## Interactions checked

- **Backpressure (plan 18)**: renewal decoupled from flush (above) is the
  one hard coupling. Deferral keeps the raw buffer and does not advance
  offsets, so exactly-once holds across a lease handoff mid-deferral: the new
  owner just re-reads the un-committed tail.
- **Exactly-once / offset CAS**: unchanged. Per-partition offset ranges mean
  a worker only ever commits partitions it owns; CAS covers the handoff race.
- **Rebalance-on-topic-growth**: partition count comes from Kafka metadata,
  refreshed on the tick. New partitions appear in `existing`, enter the
  rendezvous pass, and get leased like any other — no special case.
- **Multiple routes/topics**: leases key on `(topic, kafka_partition)`; a
  worker running N pipelines leases per topic independently. "Active workers"
  is counted per topic, so a heterogeneous fleet (not every worker runs every
  topic) still balances correctly as long as the worker writes a lease row
  only for topics it actually consumes.
- **GC feed fence**: untouched — leases are current-state rows, never part of
  the change feed or the cursor horizon.
- **Cold start**: all workers begin with zero leases and the same rendezvous
  order, so they partition the topic on the first pass rather than dogpiling;
  CAS + small jittered retry absorbs the initial contention.

## Scope notes

Per-topic parallelism ceiling = Kafka partition count (as everywhere). Failure
detection latency = up to the TTL. MV/egress workers get the same treatment
later only if single-worker throughput becomes a bottleneck — their
cursor-per-pipeline model is already race-safe via commit idempotency, so
leases there are optimization, not correctness.

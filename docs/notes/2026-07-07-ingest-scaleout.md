# Horizontal ingest: partition-subset leases (sketch, roadmap row 26)

Status: sketch. Plan written after plan 8 — leases belong to pipeline
workers, not the `RouteIngest` code they replace. Completes issue 0003's
future-work half: the offset CAS made the single-writer assumption *safe*
(duplicate writers die loudly); leases make multiple writers *real*.

## Design

Kafka's own group-rebalance protocol is deliberately not used (ukiel does
explicit assignment so offsets stay catalog-anchored). The lease table is
the catalog-native replacement:

```sql
CREATE TABLE ingest_leases (
    topic TEXT NOT NULL,
    kafka_partition INT NOT NULL,
    worker_id TEXT NOT NULL,        -- stable per process (host + uuid)
    expires_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (topic, kafka_partition)
);
```

- **Acquire/renew** — one CAS-shaped upsert: insert, or update `WHERE
  expires_at < now() OR worker_id = $me`. TTL ≈ 3× flush interval; renewal
  piggybacks on the flush tick (a worker that can't flush also loses its
  leases — the right coupling).
- **Fair share** — each worker counts active workers (distinct unexpired
  `worker_id`s) and acquires up to `ceil(partitions / workers)`; over-share
  leases are released on renewal. Greedy and eventually balanced; churn is
  bounded by the TTL. No coordinator.
- **Consume** — a worker assigns exactly its leased partitions (same
  explicit-assignment path as today) and re-checks the lease set each
  flush tick, reassigning on change. Flush commits only leased partitions'
  offset ranges (already per-partition).
- **Failure** — worker dies → leases expire → peers acquire → resume from
  catalog offsets. Exactly-once is untouched: offsets still move only
  inside part commits.

## Why safety does not depend on leases

The plan-19 offset CAS remains the correctness layer: even a lease bug
(clock skew, a zombie holding a stale lease through a GC pause) degrades to
an `OffsetRace` error on the zombie's commit — never to duplicate data.
Leases are an availability/efficiency mechanism; the CAS is the invariant.
The plan's adversarial test is exactly that: two workers, one forced-stale
lease, assert the zombie errors and the data is single-copy.

## Scope notes

Per-topic parallelism ceiling = Kafka partition count (as everywhere).
MV/egress workers get the same treatment later only if single-worker MV
throughput becomes a bottleneck — their cursor-per-pipeline model is
already race-safe via commit idempotency, so leases there are optimization,
not correctness.

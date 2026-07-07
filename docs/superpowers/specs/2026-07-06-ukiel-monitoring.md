# Ukiel Monitoring & Metrics

What to measure, per critical component, and which signals page a human.
Guiding rule: **instrument the invariants** — the testing design's I1–I8
(`2026-07-05-ukiel-testing-design.md`) are exactly the properties production
must keep proving, so every invariant gets a production signal, and every
alert traces back to an invariant or an SLO.

## Stack

- **Metrics:** the `metrics` crate facade in every worker crate
  (`counter!`/`gauge!`/`histogram!`), exported via
  `metrics-exporter-prometheus` on the existing HTTP server (`GET /metrics`
  on the query listener; `ukield` installs the recorder once per process).
  Library crates emit; only the binary wires an exporter — tests and embedders
  pay nothing. P1 limitation: a process running without the query role has no
  listener and exposes no `/metrics` (recorder installs, nothing serves it);
  a standalone metrics listener is P2.
- **Logs:** `tracing` (already in use — worker loops log stats structs);
  structured fields, no prose parsing. Every commit logs its `commit_id`,
  hypertable, kind, and part counts — the catalog is the system of record,
  logs are the narrative.
- **Traces:** OpenTelemetry spans are phase 3 (per-query spans across
  session build → catalog → scan); not load-bearing yet.
- Label discipline: `hypertable` is a fine label (thousands); **namespace is
  not** (1M cardinality — namespace-level questions are catalog queries, not
  metric labels).

## Component metrics

### Ingest (`ukiel-ingest`)

| Metric | Type | Why |
|---|---|---|
| `ingest_consumer_lag{topic,partition}` | gauge | Kafka high-watermark minus catalog `next_offset` — the true backlog, computed against *our* offsets, not group offsets |
| `ingest_flush_duration_seconds` | histogram | flush = encode + upload + commit; the freshness budget |
| `ingest_flush_rows` / `ingest_flush_bytes` | histogram | small-flush syndrome shows here before it shows in L0 counts |
| `ingest_rows_total{hypertable}` | counter | throughput |
| `ingest_poison_messages_total{topic}` | counter | I6; a step change means a producer broke |
| `ingest_out_of_bounds_total{hypertable}` | counter | event-time bound skips (plan 19 / issue 0004) — dropping is by design but never silent; a spike on a *migration* table means its per-table bound override is missing |
| `ingest_commit_failures_total` | counter | commit errors (not conflicts — ADD doesn't conflict) |
| `ingest_last_flush_event_ts_seconds{hypertable}` | gauge | max event ts of the last non-empty flush (unix secs); freshness = `time() −` this. Timestamp form on purpose: it keeps aging when ingest hangs, where a computed freshness gauge would freeze at a healthy value — the exact failure the alert exists for (I3) |
| `ingest_backpressure_deferrals_total{hypertable}` | counter | plan-18 guardrail: flushes deferred because live L0 count crossed the slowdown/stop thresholds — nonzero means compaction is behind and ingest is self-throttling (by design) |
| `ingest_flush_partitions` | histogram | distinct partitions per flush; a backfill spanning many days shows here (the spread warning's metric twin — no hard cap exists because a flush's offset range covers all its rows) |

### Catalog (Postgres)

| Metric | Type | Why |
|---|---|---|
| `catalog_commit_duration_seconds{kind}` | histogram | the transactional heart's latency |
| `catalog_commit_conflicts_total{worker}` | counter | expected under concurrency; a *rate spike* = livelock risk (I5) |
| `catalog_live_parts{hypertable,level}` | gauge (periodic collector) | L0 growth = compaction starvation; total growth = planning cost growth |
| `catalog_feed_lag{worker,hypertable}` | gauge | head commit id − worker cursor. **Load-bearing twice**: consumer health AND the GC reaper fence — a stalled cursor silently blocks object reaping |
| `catalog_pool_connections{state}` | gauge | sqlx pool saturation |
| `catalog_pending_objects` / `catalog_unpurged_tombstones` | gauge | GC backlog inputs |

### Query (`ukiel-query`)

| Metric | Type | Why |
|---|---|---|
| `query_requests_total{format,status}` | counter | rate + error ratio |
| `query_duration_seconds{class}` | histogram | p50/p95/p99; `class` = coarse shape (select/aggregate/introspection), not SQL text |
| `query_rows_returned` | histogram | runaway result sets (ties to the 0002 quota work) |
| `query_timeouts_total` | counter | statement-timeout expiries (plan 19 / issue 0002) — a rate here means tenants are hitting the rail; sustained growth is a capacity or pruning problem wearing a timeout costume |
| `query_parts_scanned` / `query_parts_pruned` | histogram | pruning effectiveness in production, not just benchmarks |
| `query_objectstore_requests_total{op}` | counter | the cost metric (same counting wrapper the perf spec defines) |
| `cache_hits_total` / `cache_misses_total{tier}` | counter | NVMe file cache and Parquet metadata cache (plan 13's counters) — hit-ratio drop predicts latency regression and disk pressure |
| `query_sessions_built_total` | counter | per-request session cost visibility |

### Compactor

| Metric | Type | Why |
|---|---|---|
| `compactor_merges_total` / `compactor_parts_in_total` / `parts_out_total` | counter | throughput and effectiveness (in/out ratio) |
| `compactor_conflicts_total` | counter | losing every race = starvation with a healthy-looking loop |
| `compactor_pass_duration_seconds` | histogram | pass time approaching the poll interval = falling behind |
| `compactor_backlog_groups` | gauge | partitions with a level at/over its merge trigger (`l0_fanout` / `fanout` runs, plan 17) — the direct "is compaction keeping up" signal |
| `compactor_unfinalized_partitions` / `compactor_oldest_unfinalized_age_seconds` | gauge | partitions past `finalize_after_secs` still holding >1 run — the plan-17 terminal invariant ("cold partition = one run") as a production signal; age growth = finalization starving |
| `compactor_last_success_timestamp` | gauge | liveness that survives error-looping |

### GC

| Metric | Type | Why |
|---|---|---|
| `gc_reaped_parts_total` / `gc_swept_orphans_total` | counter | normal operation |
| `gc_reconcile_orphans_total` | counter | **should be ~zero forever** — nonzero means a writer uploaded without registering intent (plan 9's invariant violated); this is a bug detector, not a load metric |
| `gc_pending_object_age_seconds` (max) | gauge | orphan sweep health |
| `gc_reap_fence_age_seconds` | gauge | how long the oldest reapable-but-fenced part has waited — the cursor-stall early warning |

### Process (`ukield`)

`ukield_worker_up{role}` gauge (set 1 on spawn, 0 on exit), `ukield_worker_restarts_total{role}`, standard process metrics (RSS, FDs, tokio task counts if cheap). Cache directory disk usage gauge — we have already lived one disk-full incident; the cache dir currently has **no eviction** (plan 15 notes it), so this gauge is the guardrail.

## Alerts (invariant-derived)

| Alert | Condition (tune thresholds in deployment) | Invariant / risk |
|---|---|---|
| FreshnessBreach | `time() - ingest_last_flush_event_ts_seconds` > 60 for 5m, or consumer lag growing 15m | I3 / SLO |
| FeedConsumerStalled | `catalog_feed_lag` growing for 15m | consumer health **and** GC fence — precedes disk growth |
| CompactionStarved | `compactor_backlog_groups` growing 30m, or `last_success` > 3× poll interval | I4/I5 operationally; query latency follows L0 count |
| IngestBackpressured | `ingest_backpressure_deferrals_total` increasing for 15m | the plan-18 guardrail is holding the line — freshness is being traded for part count; find out why compaction lags before the stop threshold turns it into staleness |
| FinalizationStarved | `compactor_oldest_unfinalized_age_seconds` > 6× `finalize_after_secs` | cold partitions stuck pre-terminal: whale deletes need rewrites, per-key read amp stays at hot-partition levels |
| GcStarved | `catalog_unpurged_tombstones` or `gc_pending_object_age` growing steadily | storage-cost leak |
| ReconcileFoundOrphans | `gc_reconcile_orphans_total` increased at all | plan-9 write-protocol violation — a bug, page during business hours |
| ConflictStorm | conflict rate > 20% of commits for 10m | optimistic-concurrency livelock |
| QueryErrors | error ratio > 2% or p99 > SLO for 5m | user-facing |
| CacheDegraded | hit ratio drop > 20 points, or cache-dir disk > 80% | latency regression / disk-full incident repeat |
| WorkerDown | `ukield_worker_up == 0` for 1m | process health |
| IngestPoisonSpike | poison rate step change | producer broke; data is being skipped (visibly, by design) |

Deliberately absent: an "isolation violated" alert — I2 has no runtime
observer (a leak wouldn't announce itself). Isolation assurance stays with
the test suite and the plan-time filter design; the security posture items
are issues 0001/0002, tracked as work, not alerts.

## Health endpoints

`GET /healthz` (exists) stays liveness-only. Add `GET /readyz`: catalog
`SELECT 1` + object-store `head` on a sentinel — for load-balancer rotation
of query nodes; workers don't need readiness (they self-heal by replanning).

## Dashboards (per-hypertable golden signals)

1. **Flow**: ingest rows/s → flush duration → freshness | L0 count →
   compactor backlog → merges | reap/sweep counts.
2. **Query**: rate, error ratio, p50/p95/p99 by class, parts scanned vs
   pruned, cache hit ratios, object-store requests/query.
3. **Catalog**: commit latency by kind, conflicts by worker, feed lag by
   worker, pool saturation, table row counts (parts/pending/tombstoned).
4. **Capacity**: cache-dir disk, object count/bytes per hypertable (from
   catalog sums — no bucket listing), Postgres size.

## Implementation phases

- **P1 (roadmap row 20, `ukiel-metrics-p1`):** `metrics` facade in the five worker crates —
  the stats structs already returned by `run_once`/flush paths become
  counters/histograms at their call sites; Prometheus exporter + `/metrics`
  + `/readyz` in `ukield`; cache/metadata-cache counters exposed.
- **P2:** the periodic collector gauges that need queries (live-part counts,
  feed lag, pending/tombstone backlogs, Kafka high-watermark lag) — a small
  ticker task in `ukield` reusing existing catalog APIs.
- **P3:** OTel spans across the query path; trace-id in query error
  responses.

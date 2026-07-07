# Change-feed horizon & purged-row pruning (sketch, roadmap row 23)

Status: sketch. Plan written after plan 8 — pipeline/MV cursors are the
consumers the horizon must fence, and their semantics must be real first.
This bounds the design's one acknowledged unbounded quantity: **total
catalog rows** (part and commit rows are never deleted today because the
feed promises replay-from-genesis).

## The semantic change (the actual decision)

"Replay forever" becomes **"snapshot + tail"**: a new consumer no longer
starts its cursor at 0 and replays history; it bootstraps from current
state (`live_parts` — which parquet-sourced pipelines can already consume
as an initial batch) and subscribes from the current head
(`register_cursor_at_head`). This is not a loss of capability — it is what
every mature log system does (Kafka retention, Postgres WAL horizon), and
MV workers are structurally ready for it.

## Mechanics

- Config `feed_horizon_secs` (default e.g. 30 days, processing time). The
  **prune point** for a hypertable = the newest commit that is BOTH older
  than the horizon AND `<= min(worker_cursors.last_commit_id)` over every
  registered cursor. Cursors always win over the clock — the horizon never
  prunes unconsumed history; it just stops promising it to consumers that
  don't exist yet.
- **Prune pass** (a new sweep in `ukiel-gc`, batched deletes):
  1. Delete part rows that are tombstoned AND purged AND created before the
     prune point (their objects are long gone; only replay needed them).
  2. Delete commit rows before the prune point that no remaining part row
     references via `created_by_commit` OR `deleted_by_commit` — live
     parts keep their creating commits alive indefinitely (FK integrity
     and provenance both want that).
  3. `ingest_offsets`, `pending_objects`, cursor rows are untouched —
     they're current-state, not history.
- **Safety alarm before enforcement**: `feed_horizon_margin_seconds`
  (metrics P2) — distance between the laggiest cursor and the horizon. A
  worker down longer than the horizon minus its lag would stall pruning
  (by design, loudly) rather than lose its position silently.

## Interactions checked

- **GC reaper fence**: unchanged and stricter than the horizon (cursor-
  based, exact). Pruning only touches rows the reaper is already done with.
- **Backup/DR**: PITR restore rewinds the prune bookkeeping consistently
  with everything else; the restore-then-replay story (design doc) is
  unaffected because it relies on `ingest_offsets` + Kafka retention, not
  feed history.
- **Time travel (deferred)**: a feed horizon is also the natural `AS OF`
  retention bound — the two features should share the horizon config when
  time travel lands.

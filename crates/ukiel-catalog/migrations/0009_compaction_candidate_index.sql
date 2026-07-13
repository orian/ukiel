-- Issue 0012: the compaction candidate query counts *distinct runs* per
-- (partition, level), so it needs created_by_commit alongside the grouping
-- columns. parts_partition_idx (0007) has neither that nor the liveness
-- predicate, so the sweep fell back to a sequential scan of every live part with
-- an external merge sort behind it — ~1s and a 19MB disk spill per pass per
-- hypertable at 400k live parts, on a query that runs on every tick.
--
-- Live rows only (the sweeps never look at tombstones) and covering, so the run
-- counts stream straight out of the index in group order: index-only scan, no
-- sort, no spill. Measured on 400k live parts / 5k partitions: the ladder
-- candidate query 1005ms -> 165ms, and the collector's compactor_backlog_groups
-- gauge — same run counting, every 30s, across all hypertables — 72ms. Index:
-- 26MB against a 247MB parts table.
CREATE INDEX parts_compaction_idx
    ON parts (hypertable_id, partition_values, level, created_by_commit)
    WHERE deleted_by_commit IS NULL;

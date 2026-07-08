-- Issue 0007: the plan-17/18 partition probes filter parts by
-- (hypertable_id, partition_values[, level]), but parts_live_idx is keyed
-- on packing-key range and partial on live rows — partition_l0_quiet_since
-- (which counts tombstoned rows too: it measures arrivals, not liveness)
-- degrades to a per-hypertable scan growing with all-time history.
-- Deliberately non-partial so dead rows are covered.
CREATE INDEX parts_partition_idx
    ON parts (hypertable_id, partition_values, level);

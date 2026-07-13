-- Plan 41: one renewable compaction lease per (hypertable, partition).
--
-- The partition is the exclusion domain compaction actually has: merge
-- planning and finalization both consume a partition's live parts, and plan 40
-- measured that concurrency *inside* one partition buys no useful throughput
-- (32 same-partition contenders land the same 58 swaps as 2, wasting 55.7% of
-- attempts) while the production loser pays a full Parquet read/merge/upload
-- before REPLACE tells it that it lost.
--
-- The lease is a scheduling claim, not a correctness mechanism: optimistic
-- REPLACE conflict checking stays mandatory, because ingest, deletion and
-- operator actions never take one.
--
-- `expires_at` is always derived from PostgreSQL's clock, never a worker's:
-- a skewed worker must not be able to extend or shorten its own tenancy.
CREATE TABLE compaction_leases (
    hypertable_id BIGINT NOT NULL REFERENCES hypertables(id) ON DELETE CASCADE,
    partition_values JSONB NOT NULL,
    -- Process instance, not pod name: names are reused after a restart, and a
    -- reused identity would let a fresh process inherit a zombie's tenancy.
    owner_id UUID NOT NULL,
    -- Bumped on every acquisition and reclamation, preserved by renewal. The
    -- REPLACE transaction fences on it, so an expired owner that comes back to
    -- life cannot commit over its successor.
    generation BIGINT NOT NULL CHECK (generation > 0),
    expires_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (hypertable_id, partition_values)
);

-- Serves the "oldest expired lease" observability probe; the table holds at
-- most one row per partition under active compaction.
CREATE INDEX compaction_leases_expiry_idx ON compaction_leases (expires_at);

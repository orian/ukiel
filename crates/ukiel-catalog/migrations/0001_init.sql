-- The ukiel catalog schema.
--
-- Ukiel has no deployed installations, so this is the whole schema: one file
-- applied to an empty database, not a chain of upgrades. Change it in place.

CREATE TABLE hypertables (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    table_schema JSONB NOT NULL,
    partition_spec JSONB NOT NULL,
    sort_key TEXT[] NOT NULL,
    -- The i64 column whose min/max each part records for range pruning
    -- (e.g. 'tenant_id' in a multitenant deployment).
    packing_key TEXT NOT NULL,
    -- Size-targeted placement, one knob:
    --   NULL  = packed (one file per merge output),
    --   0     = separated (every packing key gets its own files, so per-key
    --           deletion and retention are metadata-only at rest),
    --   N > 0 = merge outputs cut at key boundaries into ~N-byte files, so keys
    --           bigger than N get dedicated files -- heavy keys separate
    --           organically.
    target_file_bytes BIGINT CHECK (target_file_bytes IS NULL OR target_file_bytes >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE logical_tables (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    namespace_id BIGINT NOT NULL,
    name TEXT NOT NULL,
    hypertable_id BIGINT NOT NULL REFERENCES hypertables(id),
    column_mapping JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (namespace_id, name)
);

CREATE TABLE commits (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    hypertable_id BIGINT NOT NULL REFERENCES hypertables(id),
    kind TEXT NOT NULL CHECK (kind IN ('add', 'replace', 'delete')),
    idempotency_key TEXT,
    -- The fingerprint of the logical operation this commit applied.
    --
    -- `idempotency_key` carries the operation key, but a key alone cannot *prove*
    -- that it means what the caller thinks it means: a buggy caller or direct SQL
    -- could pair one key with different intent. The stored digest turns that from
    -- a silent "already applied" -- the worst possible answer, since it would tell
    -- a worker that someone else's commit was its own -- into a loud permanent
    -- error.
    --
    -- Nullable, because a commit may legitimately carry no key at all. A NULL
    -- fingerprint under a looked-up key fails loudly rather than being trusted.
    operation_fingerprint BYTEA
        CHECK (operation_fingerprint IS NULL OR octet_length(operation_fingerprint) = 32),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Idempotency: at most one commit per (hypertable, key). NULL keys are exempt.
CREATE UNIQUE INDEX commits_idempotency_idx
    ON commits (hypertable_id, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

-- Change feed ordering.
CREATE INDEX commits_feed_idx ON commits (hypertable_id, id);

CREATE TABLE parts (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    hypertable_id BIGINT NOT NULL REFERENCES hypertables(id),
    path TEXT NOT NULL,
    partition_values JSONB NOT NULL,
    packing_key_min BIGINT NOT NULL,
    packing_key_max BIGINT NOT NULL,
    row_count BIGINT NOT NULL,
    size_bytes BIGINT NOT NULL,
    level SMALLINT NOT NULL DEFAULT 0,
    column_stats JSONB,
    -- A fixed-size Bloom filter over the part's packing keys, so the catalog can
    -- reject a part it can *prove* does not hold the key, instead of shipping the
    -- row so the client can prove it.
    --
    -- Range pruning alone matches a superset: a packed file whose keys span
    -- 100..4500 "contains" tenant 3000 even when it holds none of tenant 3000's
    -- rows -- the ordinary case, because a file holds the tenants active in the
    -- window it covers, not the tenants its endpoints bracket. On a product-shaped
    -- fixture (100k tenants, heavily packed) the median tenant matched 20,000 parts
    -- by range and 7 by key set.
    --
    -- Two things this deliberately is NOT, both rejected on measurements:
    --
    --   * NOT an inverted index over the keys. A GIN index on a BIGINT[] of the key
    --     set reads beautifully and writes catastrophically -- one entry per key per
    --     part took commits from 7,815/s to 2,254/s and PostgreSQL from 4.4 to 11.7
    --     of 12 cores, because every insert and every compaction REPLACE churns an
    --     entry for each key. That is the (part, key) expansion, hiding inside an
    --     index.
    --   * NOT the exact key set in the index payload. Index tuples cap at ~2704
    --     bytes; a thousand keys is 8 KB and the insert would simply fail.
    --
    -- The error is one-directional: the filter never says "absent" for a key that is
    -- present. It may say "maybe" for one that is absent, which keeps a part that
    -- could have been skipped -- costing a little pruning, never a row. NULL, an
    -- unknown size, a malformed blob: all keep. Pruning may only remove what it can
    -- prove is absent.
    --
    -- Note what is *not* here: no hash function, and no membership function. The size
    -- is fixed, so the probe positions depend on the key alone: `ukiel_core::keyfilter`
    -- computes them once per query and binds them, and SQL is left with four
    -- `get_byte(...) & mask <> 0` tests. Sizing the blob per part would make the bit a
    -- key lands on depend on the part, so only the database could compute it -- and a
    -- plpgsql call per candidate row made the query *slower* than the scan it replaced
    -- (16.5 ms against 12.8 ms) despite reading a sixth of the buffers.
    --
    -- The exact key set is NOT stored here; storing it is what made writes collapse.
    -- `column_stats.packing_keys` remains the exact record, read by the query provider;
    -- the filter is the part of it the *catalog* can act on.
    --
    -- A part with a single key gets no filter: its `packing_key_min` and
    -- `packing_key_max` already *are* its key set, so the range predicate is exact for
    -- it. Dedicated (unpacked) parts are all of that shape.
    key_filter BYTEA,
    created_by_commit BIGINT NOT NULL REFERENCES commits(id),
    deleted_by_commit BIGINT REFERENCES commits(id),
    -- GC bookkeeping: a tombstoned part whose object has been deleted keeps its
    -- catalog row (change-feed replay needs it) but is stamped purged.
    purged_at TIMESTAMPTZ
);

-- Query planning: live parts of a hypertable, pruned by packing-key range, with the
-- key filter as an INCLUDE payload so the scan rejects a part from the index alone
-- (`Index Only Scan`, `Heap Fetches: 0`) and touches the heap only for survivors.
-- `id` rides along so the candidate pass stays index-only. One entry per part, so the
-- write path keeps its shape.
CREATE INDEX parts_live_idx
    ON parts (hypertable_id, packing_key_min, packing_key_max)
    INCLUDE (key_filter, id)
    WHERE deleted_by_commit IS NULL;

-- Change feed: parts added / removed by a commit.
CREATE INDEX parts_created_by_idx ON parts (created_by_commit);
CREATE INDEX parts_deleted_by_idx ON parts (deleted_by_commit) WHERE deleted_by_commit IS NOT NULL;

-- GC: tombstoned parts whose objects are still out there.
CREATE INDEX parts_reapable_idx ON parts (hypertable_id)
    WHERE deleted_by_commit IS NOT NULL AND purged_at IS NULL;

-- Partition probes filter parts by (hypertable_id, partition_values[, level]), which
-- parts_live_idx cannot serve: it is keyed on packing-key range and partial on live
-- rows. Deliberately non-partial here, because partition_l0_quiet_since counts
-- tombstoned rows too (it measures arrivals, not liveness) -- without dead-row coverage
-- it degrades to a per-hypertable scan growing with all-time history.
CREATE INDEX parts_partition_idx
    ON parts (hypertable_id, partition_values, level);

-- The compaction candidate query counts *distinct runs* per (partition, level), so it
-- needs created_by_commit alongside the grouping columns -- which parts_partition_idx
-- has neither of, nor the liveness predicate, so the sweep falls back to a sequential
-- scan of every live part with an external merge sort behind it: ~1s and a 19MB disk
-- spill per pass per hypertable at 400k live parts, on a query that runs every tick.
--
-- Live rows only (the sweeps never look at tombstones) and covering, so the run counts
-- stream straight out of the index in group order: index-only scan, no sort, no spill.
-- Measured on 400k live parts / 5k partitions: the ladder candidate query 1005ms ->
-- 165ms, and the collector's compactor_backlog_groups gauge -- same run counting, every
-- 30s, across all hypertables -- 72ms. Index: 26MB against a 247MB parts table.
CREATE INDEX parts_compaction_idx
    ON parts (hypertable_id, partition_values, level, created_by_commit)
    WHERE deleted_by_commit IS NULL;

-- Kafka offsets stored transactionally with part commits: the source of truth for where
-- ingest resumes. Kafka consumer-group offsets are never used.
CREATE TABLE ingest_offsets (
    hypertable_id BIGINT NOT NULL REFERENCES hypertables(id),
    topic TEXT NOT NULL,
    kafka_partition INT NOT NULL,
    next_offset BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (hypertable_id, topic, kafka_partition)
);

-- Durable change-feed cursors for background workers (compactor, MV, ...).
CREATE TABLE worker_cursors (
    worker TEXT NOT NULL,
    hypertable_id BIGINT NOT NULL REFERENCES hypertables(id),
    last_commit_id BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (worker, hypertable_id)
);

-- Write-ahead upload intent: a writer records an object's path here BEFORE uploading
-- it. The commit that references the object deletes its row (same transaction), so a
-- row that outlives the orphan grace is an object whose commit never landed -> a
-- reapable orphan, discoverable without listing S3.
CREATE TABLE pending_objects (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    hypertable_id BIGINT NOT NULL REFERENCES hypertables(id),
    path TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX pending_objects_sweep_idx ON pending_objects (hypertable_id, created_at);

-- Fencing tokens for compaction leases. A fencing token must never be reused.
--
-- A catalog-owned sequence, not a per-row counter: a clean release *deletes* the lease
-- row, so a row-local counter would restart at 1 and hand the same (partition, owner,
-- generation) triple to a later tenancy of the same process. That triple is exactly
-- what renew, release and the fenced REPLACE compare, so a token the caller happened to
-- retain across the release -- a delayed cleanup task, in-process compaction
-- concurrency, or a reconciliation lifecycle holding an operation across a rebuild --
-- could then act against the wrong tenancy. Drawing from a sequence makes the token
-- globally non-reused instead of row-locally monotonic.
--
-- Gaps are expected and harmless: a losing contender's `INSERT ... ON CONFLICT` still
-- evaluates the VALUES list, so it burns a value it never uses. The token needs
-- uniqueness and ordering, not density.
CREATE SEQUENCE compaction_lease_generation_seq AS BIGINT;

-- One renewable compaction lease per (hypertable, partition).
--
-- The partition is the exclusion domain compaction actually has: merge planning and
-- finalization both consume a partition's live parts, and concurrency *inside* one
-- partition buys no useful throughput (32 same-partition contenders land the same 58
-- swaps as 2, wasting 55.7% of attempts) while the production loser pays a full Parquet
-- read/merge/upload before REPLACE tells it that it lost.
--
-- The lease is a scheduling claim, not a correctness mechanism: optimistic REPLACE
-- conflict checking stays mandatory, because ingest, deletion and operator actions never
-- take one.
--
-- `expires_at` is always derived from PostgreSQL's clock, never a worker's: a skewed
-- worker must not be able to extend or shorten its own tenancy.
CREATE TABLE compaction_leases (
    hypertable_id BIGINT NOT NULL REFERENCES hypertables(id) ON DELETE CASCADE,
    partition_values JSONB NOT NULL,
    -- Process instance, not pod name: names are reused after a restart, and a reused
    -- identity would let a fresh process inherit a zombie's tenancy.
    owner_id UUID NOT NULL,
    -- Drawn from compaction_lease_generation_seq on every new tenancy (fresh row, or
    -- takeover of an expired/released one), preserved by renewal and by an idempotent
    -- reacquisition from the same unexpired owner -- an in-flight merge's fence must
    -- survive its own retry. The REPLACE transaction fences on it, so an expired owner
    -- that comes back to life cannot commit over its successor.
    generation BIGINT NOT NULL CHECK (generation > 0),
    expires_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (hypertable_id, partition_values)
);

-- Serves the "oldest expired lease" observability probe; the table holds at most one row
-- per partition under active compaction.
CREATE INDEX compaction_leases_expiry_idx ON compaction_leases (expires_at);

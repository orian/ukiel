CREATE TABLE hypertables (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    table_schema JSONB NOT NULL,
    partition_spec JSONB NOT NULL,
    sort_key TEXT[] NOT NULL,
    -- The i64 column whose min/max each part records for range pruning
    -- (e.g. 'tenant_id' in a multitenant deployment).
    packing_key TEXT NOT NULL,
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
    created_by_commit BIGINT NOT NULL REFERENCES commits(id),
    deleted_by_commit BIGINT REFERENCES commits(id)
);

-- Query planning: live parts of a hypertable, pruned by packing-key range.
CREATE INDEX parts_live_idx ON parts (hypertable_id, packing_key_min, packing_key_max)
    WHERE deleted_by_commit IS NULL;

-- Change feed: parts added / removed by a commit.
CREATE INDEX parts_created_by_idx ON parts (created_by_commit);
CREATE INDEX parts_deleted_by_idx ON parts (deleted_by_commit) WHERE deleted_by_commit IS NOT NULL;

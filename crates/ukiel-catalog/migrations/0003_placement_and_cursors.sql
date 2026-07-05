-- Placement policy (spec: "Placement policy: per-key deletion & retention").
-- 'packed': small keys share L1+ files, sorted by packing key.
-- 'separated': compaction splits every packing key into dedicated L1+ files,
-- making key deletion and retention metadata-only at rest.
ALTER TABLE hypertables
    ADD COLUMN placement TEXT NOT NULL DEFAULT 'packed'
    CHECK (placement IN ('packed', 'separated'));

-- Durable change-feed cursors for background workers (compactor, MV, ...).
CREATE TABLE worker_cursors (
    worker TEXT NOT NULL,
    hypertable_id BIGINT NOT NULL REFERENCES hypertables(id),
    last_commit_id BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (worker, hypertable_id)
);

-- GC bookkeeping: a tombstoned part whose object has been deleted keeps its
-- catalog row (change-feed replay needs it) but is stamped purged.
ALTER TABLE parts ADD COLUMN purged_at TIMESTAMPTZ;

CREATE INDEX parts_reapable_idx ON parts (hypertable_id)
    WHERE deleted_by_commit IS NOT NULL AND purged_at IS NULL;

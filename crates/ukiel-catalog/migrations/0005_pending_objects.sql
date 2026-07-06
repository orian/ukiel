-- Write-ahead upload intent: a writer records an object's path here BEFORE
-- uploading it. The commit that references the object deletes its row (same
-- transaction), so a row that outlives the orphan grace is an object whose
-- commit never landed -> a reapable orphan, discoverable without listing S3.
CREATE TABLE pending_objects (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    hypertable_id BIGINT NOT NULL REFERENCES hypertables(id),
    path TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX pending_objects_sweep_idx ON pending_objects (hypertable_id, created_at);

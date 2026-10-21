-- Immutable definitions. Cursor registration is a separate, barrier-protected
-- operation performed only when a worker first selects an MV.
CREATE TABLE stream_tables (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name TEXT NOT NULL UNIQUE CHECK (length(btrim(name)) > 0),
    topic TEXT NOT NULL UNIQUE CHECK (length(btrim(topic)) > 0),
    format TEXT NOT NULL CHECK (format = 'json'),
    table_schema JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE pipelines (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name TEXT NOT NULL UNIQUE CHECK (length(btrim(name)) > 0),
    source_kind TEXT NOT NULL CHECK (source_kind IN ('kafka', 'parquet')),
    source_id BIGINT NOT NULL CHECK (source_id > 0),
    target_kind TEXT NOT NULL DEFAULT 'parquet' CHECK (target_kind = 'parquet'),
    target_id BIGINT NOT NULL REFERENCES hypertables(id),
    sql TEXT NOT NULL CHECK (length(btrim(sql)) > 0),
    delivery TEXT NOT NULL CHECK (delivery = 'exactly_once'),
    error_policy TEXT NOT NULL CHECK (error_policy IN ('stall', 'skip')),
    event_time_column TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (source_kind <> 'parquet' OR source_id <> target_id)
);
CREATE INDEX pipelines_graph_idx ON pipelines(source_id, target_id) WHERE source_kind = 'parquet';

ALTER TABLE ingest_offsets ADD COLUMN stream_table_id BIGINT REFERENCES stream_tables(id);
CREATE UNIQUE INDEX ingest_offsets_stream_edge_idx
    ON ingest_offsets(stream_table_id, hypertable_id, kafka_partition)
    WHERE stream_table_id IS NOT NULL;

-- Kafka offsets stored transactionally with part commits: the source of truth
-- for where ingest resumes. Kafka consumer-group offsets are never used.
CREATE TABLE ingest_offsets (
    hypertable_id BIGINT NOT NULL REFERENCES hypertables(id),
    topic TEXT NOT NULL,
    kafka_partition INT NOT NULL,
    next_offset BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (hypertable_id, topic, kafka_partition)
);

-- JSONBench — the 5-query Bluesky benchmark.
-- Source: https://github.com/ClickHouse/JSONBench (clickhouse/queries_formatted.sql), Apache-2.0.
-- Upstream, byte-for-byte: queries-upstream.sql (ClickHouse dialect, JSON-path form).
-- This suite is STRUCTURALLY adapted by design: ukiel flattens the Jetstream
-- event at ingest, so what JSONBench queries as JSON paths (`data.commit.collection`)
-- ukiel queries as columns (`collection`). Every JSONBench participant adapts to
-- its own dialect; ours is logged query-by-query in ADAPTATIONS.md.
-- One query per line, in upstream order (q1..q5).
SELECT collection AS event, count(*) AS event_count FROM bsky GROUP BY event ORDER BY event_count DESC;
SELECT collection AS event, count(*) AS event_count, count(DISTINCT did) AS users FROM bsky WHERE kind = 'commit' AND operation = 'create' GROUP BY event ORDER BY event_count DESC;
SELECT collection AS event, extract(hour FROM to_timestamp_millis(ts)) AS hour_of_day, count(*) AS event_count FROM bsky WHERE kind = 'commit' AND operation = 'create' AND collection IN ('app.bsky.feed.post', 'app.bsky.feed.repost', 'app.bsky.feed.like') GROUP BY event, hour_of_day ORDER BY hour_of_day, event;
SELECT did AS user_id, min(to_timestamp_millis(ts)) AS first_post_ts FROM bsky WHERE kind = 'commit' AND operation = 'create' AND collection = 'app.bsky.feed.post' GROUP BY user_id ORDER BY first_post_ts ASC LIMIT 3;
SELECT did AS user_id, max(ts) - min(ts) AS activity_span FROM bsky WHERE kind = 'commit' AND operation = 'create' AND collection = 'app.bsky.feed.post' GROUP BY user_id ORDER BY activity_span DESC LIMIT 3;

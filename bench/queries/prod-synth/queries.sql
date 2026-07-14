-- prod-synth query suite.
--
-- Six event queries over the generated `events` table. They are the shapes a product
-- actually asks of an event store: how many events, in a window, which events, how many
-- people, which pages, which SDKs.
--
-- **`team_id` never appears here.** That is the point of the suite, not an oversight.
-- The Ukiel arm runs these verbatim through the namespace-scoped path, where the tenant
-- slice is a property of the *session*, not of the SQL — a real client cannot write a
-- query that reads another tenant's rows, because it has no way to name one. The raw
-- DataFusion reference reads the very same Parquet files and adds the equivalent
-- explicit `team_id = ?` itself; the two must return identical batches before either is
-- timed, or the comparison is between two different questions.
--
-- The `mat_$current_url` and `mat_$lib` identifiers must stay double-quoted: `$` is not
-- a bare identifier character in SQL, and an unquoted `mat_$lib` parses as `mat_`
-- followed by a parameter placeholder. Silently.
--
-- Every query is deterministically ordered. An unordered `GROUP BY` result compares
-- equal to itself only by luck, and a suite that passes by luck will one day fail by
-- luck and nobody will know which.

-- q1: tenant event count.
-- The cheapest possible question, and the one that most directly exposes over-fetch:
-- the answer needs no column but the packing key, so anything the scan reads beyond the
-- parts that hold the tenant is pure waste.
SELECT count(*) AS events FROM events;

-- q2: events in a 24-hour window.
-- Adds a timestamp predicate on the second sort-key column, so row-group statistics can
-- prune inside the surviving files.
SELECT count(*) AS events
FROM events
WHERE timestamp >= 1783036800000 AND timestamp < 1783123200000;

-- q3: top events.
-- A grouped aggregate over a skewed low-cardinality column — the shape behind every
-- "what are people doing" dashboard.
SELECT event, count(*) AS n
FROM events
GROUP BY event
ORDER BY n DESC, event ASC
LIMIT 10;

-- q4: distinct persons.
-- The generator gives each tenant a Zipf-distributed person population with repeated
-- `distinct_id`s, so this is a real distinct-count and not a row count wearing a hat.
SELECT count(DISTINCT distinct_id) AS persons FROM events;

-- q5: top promoted current URLs.
-- Reads `mat_$current_url`, the column promoted out of the `properties` JSON. Its values
-- are exactly what the JSON holds, so a future extraction query over `properties` must
-- return the same rows — which is what makes this fixture useful to plan 44 later.
SELECT "mat_$current_url" AS url, count(*) AS n
FROM events
GROUP BY "mat_$current_url"
ORDER BY n DESC, url ASC
LIMIT 10;

-- q6: event counts by promoted library.
SELECT "mat_$lib" AS lib, count(*) AS n
FROM events
GROUP BY "mat_$lib"
ORDER BY n DESC, lib ASC;

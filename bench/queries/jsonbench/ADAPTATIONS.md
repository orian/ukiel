# JSONBench adaptations

The 5 queries in `queries.sql` are JSONBench's Bluesky suite
([ClickHouse/JSONBench](https://github.com/ClickHouse/JSONBench), `clickhouse/queries_formatted.sql`,
Apache-2.0), adapted for ukiel. `queries-upstream.sql` is the pristine upstream file.

## The structural adaptation (the whole suite)

JSONBench measures **querying raw JSON**: its participants store the Jetstream event as a JSON
blob and pay to extract fields at query time (`data.commit.collection`, `j->>'$.commit.collection'`,
…). **Ukiel flattens the event at ingest**, so those fields are ordinary typed columns.

That is a real, declared system difference, not a trick — and it is the difference every
JSONBench participant expresses in its own dialect (compare the ClickHouse, DuckDB, and Postgres
query files upstream: three different extraction syntaxes over the same logical query). Ours is
"the extraction already happened at write time." **Read our numbers as *ukiel answering
JSONBench's questions*, not as a like-for-like JSON-extraction benchmark** — a JSON-native engine
does strictly more work per row at query time than we do here, and we would rather say so than
quietly bank the difference.

Field mapping (ingest-side, `map_bluesky_event`):

| JSONBench path | ukiel column |
|---|---|
| `data.commit.collection` | `collection` (utf8) |
| `data.kind` | `kind` (utf8) |
| `data.commit.operation` | `operation` (utf8) |
| `data.did` | `did` (utf8) — **added in plan 32** |
| `data.time_us` (µs) | `ts` (`timestamp_ms`, epoch **ms**) |

`did` is also hashed (fnv1a-64, sign bit masked) into `tenant_id`, the packing key. It is stored
verbatim *as well* because q2/q4/q5 group by the account: grouping by the hash would be
collision-equivalent but would return unreadable integers instead of dids.

---

## Per-query

**q1 — top event types.** `data.commit.collection` → `collection`; `count()` → `count(*)`.
The output alias `count` is renamed **`event_count`**: `count` is a reserved function name in
DataFusion's parser and cannot be used bare as a projection alias here.

**q2 — top event types with unique users.** As q1, plus `uniqExact(data.did)` →
`count(DISTINCT did)`. ClickHouse's `uniqExact` is exactly-distinct, so `count(DISTINCT …)` is
the faithful translation (not the approximate `uniq`).

**q3 — when do people use Bluesky.** `toHour(fromUnixTimestamp64Micro(data.time_us))` →
`extract(hour FROM to_timestamp_millis(ts))`. The `IN [...]` array literal becomes a SQL
`IN (...)` list.
*Precision:* ukiel stores `ts` in milliseconds (`time_us / 1000` at ingest), so the hour is
derived from ms rather than µs. Identical result — an hour bucket cannot straddle a microsecond.

**q4 — top 3 post veterans.** `data.did::String` → `did`;
`min(fromUnixTimestamp64Micro(data.time_us))` → `min(to_timestamp_millis(ts))`.
*Precision:* the ms-truncated `ts` could in principle tie two accounts whose first posts are
within the same millisecond, ordering them differently from upstream. A sub-millisecond tie for
"first ever post" is not a meaningful distinction, and the truncation happens at ingest (it is a
property of ukiel's fixture, not of the query).

**q5 — top 3 users by activity span.** `date_diff('milliseconds', min(...), max(...))` →
`max(ts) - min(ts)`. Since `ts` is *already* epoch-ms, the subtraction **is** the millisecond
difference — same number, no timestamp round-trip. Same ms-truncation caveat as q4.

---

## Methodology

The same as the ClickBench suite (see `../clickbench/ADAPTATIONS.md`): per query, one **cold**
run (fresh session, fresh parquet-metadata cache) then N **hot** runs; the headline is the hot
minimum. "Cold" is cold only at the ukiel layer — the OS page cache still holds the bytes.

The fixture is **whatever was last ingested** by `bench bluesky run --files N`, so the report
records the table's row count and every result is self-describing. There is no raw-DataFusion
reference runner for this suite: the raw fixture is gzipped ndjson, not parquet, so "the same
data through the bare engine" is not a comparison that exists without re-implementing the ingest
mapping — the ClickBench suite is where the `ukiel / raw` ratio is measured.

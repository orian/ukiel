# Columnar result formats for the query API

`POST /api/query` currently returns row-oriented JSON: `{"rows": [{"col":
val, ...}, ...]}`. That is convenient for small app queries and terrible for
analytical result sets: every row repeats every column name, numbers lose
type fidelity in JSON, and clients that want columns (charts, dataframes)
re-pivot on arrival. Ukiel's engine is columnar end to end — the API should
be able to keep results columnar too.

## Request shape

A new optional `format` field on the existing endpoint (default preserves the
current behavior; no breaking change):

```json
{"namespace_id": 1, "sql": "SELECT ts, amount FROM events", "format": "columns"}
```

| `format` | Response | Content-Type |
|---|---|---|
| `rows` (default) | `{"schema": [...], "rows": [{...}, ...]}` | `application/json` |
| `columns` | JSON columnar (below) | `application/json` |
| `arrow` | Arrow IPC stream, raw bytes | `application/vnd.apache.arrow.stream` |

`Accept: application/vnd.apache.arrow.stream` acts as an alternative to
`format: "arrow"` for header-driven clients; an explicit `format` field wins
on conflict.

## Type hints (`schema` block, all JSON formats)

Both JSON formats carry the same `schema` array describing the result
columns in projection order:

```json
"schema": [
  {"name": "ts",     "type": "timestamp_ms", "nullable": true},
  {"name": "amount", "type": "float64",      "nullable": true},
  {"name": "n",      "type": "int64",        "nullable": false}
]
```

Adding `schema` to the `rows` format is additive — existing clients keep
reading `rows` untouched.

**Type resolution rule.** JSON alone cannot distinguish an epoch-millis
column from a plain integer — which is precisely the hint clients need most.
Resolution, per result column:

1. If the Arrow field carries the `ukiel:type` metadata key, use it. The
   schema builder (`arrow_schema_from_json`) attaches
   `{"ukiel:type": "timestamp_ms"}` to fields declared `timestamp_ms`;
   Arrow/DataFusion propagate field metadata through plain column
   references, so `SELECT ts FROM events` hints `timestamp_ms`.
2. Otherwise map the Arrow type: `Int64 → int64`, `Float64 → float64`,
   `Utf8 → utf8`, `Boolean → bool`. Computed expressions land here —
   `SELECT ts + 1` is just `int64` (the declared hint does not survive
   arithmetic, correctly: the result is no longer guaranteed to be a
   timestamp).
3. Any other Arrow type an expression may produce (dates, decimals from
   future functions) is emitted as its lowercase Arrow name. Only the five
   Ukiel core names are **stable API**; everything else is best-effort and
   may change with the engine version.

The `arrow` format needs no separate block — the IPC schema carries the
types natively, including the `ukiel:type` field metadata.

## `columns` — JSON columnar

Modeled on ClickHouse's `JSONColumnsWithMetadata`: a schema block plus one
value array per column, positionally aligned.

```json
{
  "schema": [
    {"name": "ts",     "type": "int64",   "nullable": true},
    {"name": "amount", "type": "float64", "nullable": true}
  ],
  "columns": {
    "ts":     [1751800000000, 1751800001000, null],
    "amount": [10.5, 0.0, 3.25]
  },
  "row_count": 3
}
```

- `type` uses Ukiel's schema type names (`int64`, `float64`, `utf8`, `bool`,
  `timestamp_ms` — the latter appears as `int64` in v1, consistent with
  storage).
- NULLs are JSON `null` inside the value arrays.
- Column order in `schema` is the projection order; `columns` is an object
  keyed by name (JSON objects are ordered in practice, but clients must key
  by name, not position).
- Typical size win vs `rows`: no per-row key repetition — 40–60% smaller
  uncompressed on wide results, and much cheaper to feed to chart libraries
  and dataframes.

## `arrow` — Arrow IPC stream

The lossless option: the collected `RecordBatch`es serialized with
`arrow::ipc::writer::StreamWriter` into the response body. Exact types
(no JSON number coercion), zero re-parse for Arrow-native clients:

```python
import pyarrow as pa, requests
r = requests.post(url, json={"namespace_id": 1, "sql": q, "format": "arrow"})
table = pa.ipc.open_stream(r.content).read_all()   # -> pandas/polars for free
```

Errors are always JSON (status ≥ 400), regardless of requested format — a
client never has to sniff whether an error body is Arrow.

## Implementation sketch (small; one task in a future plan)

All formatting happens **after** `df.collect()` in `run_query`
(`crates/ukiel-query/src/server.rs`) — isolation, SQLOptions, and every other
security property are upstream of formatting and unaffected:

- `schema` block: shared helper mapping the result `SchemaRef` per the type
  resolution rule; requires `arrow_schema_from_json` to attach the
  `ukiel:type` metadata for `timestamp_ms` fields (a two-line change in
  `ukiel-core` — metadata is inert for storage and query, it only rides
  along).
- `rows`: existing `arrow::json::ArrayWriter` path plus the `schema` sibling
  field.
- `columns`: iterate the batches' columns per field, downcasting on the five
  supported `DataType`s to emit value arrays (or reuse `ArrayWriter` per
  single-column batch); concatenate across batches.
- `arrow`: `StreamWriter::try_new(Vec::new(), &schema)` → write batches →
  `into_inner()` → response with the Arrow content type.
- `QueryRequest` gains `#[serde(default)] format: ResultFormat`.

## Interactions & caveats

- **Response size limits**: columnar formats make big results *cheaper*, which
  makes runaway result sets *likelier*. This lands together with the
  statement-timeout/quota work (issue 0002) — a `max_result_rows`/bytes cap
  should apply to all formats uniformly.
- **Arrow Flight SQL** (post-v1) remains the eventual bulk/BI protocol; the
  `arrow` format here is the 90% answer for HTTP clients until then, and both
  share the same "results stay Arrow" principle.
- **Compression**: standard HTTP `Content-Encoding` (gzip/zstd via a tower
  layer) composes with all three formats; do not invent per-format
  compression.

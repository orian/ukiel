# Materialized columns, defaults & aliases

ClickHouse-style column semantics, mapped onto Ukiel's pipeline. Declared as
DataFusion SQL expression strings in the hypertable schema JSON:

    {"fields": [
      {"name": "ts",         "type": "timestamp_ms"},
      {"name": "tenant_id",  "type": "int64"},
      {"name": "amount",     "type": "float64", "default": "0.0"},
      {"name": "doubled",    "type": "float64", "materialized": "amount * 2.0"},
      {"name": "half",       "type": "float64", "alias": "amount / 2.0"}
    ]}

| Kind | Semantics | Evaluated | Stored |
|---|---|---|---|
| `default` | fills NULL/omitted insert slots | ingest flush (+ rewrites) | yes |
| `materialized` | always computed; insert values ignored | ingest flush + **every rewrite** | yes |
| `alias` | computed per query | provider projection | no |

Three schemas derive from one spec list (`ukiel_core::TableColumns`):
**input** (stored + default — what ingest rows may carry), **physical**
(+ materialized — what Parquet files contain), **query** (+ alias — what SQL
sees).

Rules:

- **Deterministic expressions only** — `random()`/`now()` are rejected at
  compile time (`ukiel-expr`). Required because rewrites re-evaluate
  expressions; volatile ones would make compaction change data.
- Expressions reference only stored/default columns (no chaining on other
  materialized/alias columns). Write-time expressions are validated against
  the input schema; aliases against the physical schema.
- Materialized columns cannot be sort keys in v1 (ingest sorts before
  computing them).
- Validation: call `ukiel_expr::validate_table_schema` before
  `create_hypertable` — the catalog itself does not validate expressions (it
  stays DataFusion-free); bad expressions otherwise fail at first use.

**Organic backfill.** Adding a materialized column later needs no backfill
job: old files read as NULL for it (schema-adapting reads fill missing
columns), and every compactor rewrite recomputes write-time columns — so the
column materializes as compaction naturally touches each file.

**Divergences from ClickHouse (v1):** materialized and alias columns are
visible in `SELECT *` (DataFusion has no hidden columns); defaults apply only
to NULL/omitted values, and MATERIALIZED silently ignores (rather than
rejects) values supplied by the insert.

GDPR note: materialized values derived from PII are stored bytes; key
deletion rewrites whole rows, so they are erased with everything else.

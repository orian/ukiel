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

- **Clock and random expressions are supported** — `now()`/`random()` use
  DataFusion's normal expression semantics. Insert-time defaults and materialized
  values are stored. Compaction and deletion rewrites preserve existing values,
  including NULLs, and evaluate expressions only for columns absent from the
  original file. Retry deduplication identifies source work, not output bytes;
  the first committed outcome wins even when attempts compute different values.
- Expressions reference only stored/default columns (no chaining on other
  materialized/alias columns). Write-time expressions are validated against
  the input schema; aliases against the physical schema.
- Materialized columns may be sort keys: ingest computes them before sorting.
- Validation: call `ukiel_expr::validate_table_schema` before
  `create_hypertable` — the catalog itself does not validate expressions (it
  stays DataFusion-free); bad expressions otherwise fail at first use.

**Organic backfill.** Adding a materialized column later needs no backfill
job: old files read as NULL for it (schema-adapting reads fill missing
columns), and a compactor rewrite evaluates expressions for those missing
columns — so the column materializes as compaction naturally touches each file.
Once present in a file, its stored value is preserved. Changing an expression
does not recompute a column already stored in old files.

**Divergences from ClickHouse (v1):** materialized and alias columns are
visible in `SELECT *` (DataFusion has no hidden columns); defaults apply only
to NULL/omitted values, and MATERIALIZED silently ignores (rather than
rejects) values supplied by the insert.

GDPR note: materialized values derived from PII are stored bytes; key
deletion rewrites whole rows, so they are erased with everything else.

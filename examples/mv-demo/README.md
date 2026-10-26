# Orders and a sales materialized view

Run a complete local example: sample orders enter Kafka, ingest writes Parquet
to MinIO, and an MV groups each source ADD into sales totals. Query both tables
over HTTP. Two tenants share physical files and have separate query namespaces.

Use a Linux or macOS terminal (or WSL). You need Docker with Compose v2,
Rust 1.98+ and the repository's native build
dependencies, and Python 3.11+. The runner uses only Python's standard library.
The first Rust build can take several minutes.

From the repository root:

```sh
make demo
# Equivalent: python3 examples/mv-demo/demo.py start
```

This builds `ukield`, starts this example's services, creates its topic and
bucket, waits for every server role to become ready, and loads 12 sample orders.
It checks raw and MV totals for both tenants before printing **Demo ready**.
Keep this terminal open. Ctrl-C gracefully stops the server and services;
named Docker volumes retain the Kafka log, catalog, and Parquet files. You can
also stop it from a second terminal with `python3 examples/mv-demo/demo.py stop`.

## Execute queries

In a second terminal, from the repository root:

```sh
python3 examples/mv-demo/demo.py query \
  "SELECT order_id, product, quantity, amount_cents FROM orders ORDER BY product"

python3 examples/mv-demo/demo.py query \
  "SELECT product, SUM(orders) AS orders, SUM(units) AS units, SUM(revenue_cents) AS revenue_cents FROM sales GROUP BY product ORDER BY product"
```

The default namespace is tenant 1. On a fresh demo, the second query returns:

| product | orders | units | revenue_cents |
|---|---:|---:|---:|
| cake | 2 | 3 | 1500 |
| coffee | 2 | 3 | 1050 |
| tea | 2 | 3 | 750 |

Run the same query for tenant 2:

```sh
python3 examples/mv-demo/demo.py query --namespace 2 \
  "SELECT product, SUM(orders) AS orders, SUM(units) AS units, SUM(revenue_cents) AS revenue_cents FROM sales GROUP BY product ORDER BY product"
```

Tenant 2 has 6 orders and 3050 cents of revenue; tenant 1 has 6 orders and
3300 cents. The namespace selects the tenant; SQL does not need a tenant filter.

[queries.sql](queries.sql) includes raw rows, final totals, currency conversion,
table discovery, and the stored partial groups with their `inserted_at` values.
The query helper prints the API's row-oriented JSON. You can also call it directly:

```sh
curl --fail-with-body -sS http://127.0.0.1:18880/api/query \
  -H 'content-type: application/json' \
  -d '{"namespace_id":1,"sql":"SELECT COUNT(*) AS orders FROM orders","format":"rows"}'
```

## Add more data

```sh
python3 examples/mv-demo/demo.py seed
python3 examples/mv-demo/demo.py check
```

Each `seed` appends another 12 orders, with new order IDs and current timestamps,
then waits for ingestion and verifies the MV. Counts and revenue double after
the first additional batch. Edit [orders.jsonl](orders.jsonl), or pass
`seed --file /path/to/orders.jsonl`, to try different quantities and products.
Each JSON line needs `tenant_id` (1 or 2), `product`, `quantity`, and
`amount_cents`; the runner supplies `ts` and `order_id`.

The MV executes once over each source ADD and stores **partial aggregates**.
Several `sales` rows can therefore represent the same product. Use `GROUP BY`
and `SUM` as above to get final totals. Compaction merges files without
retriggering the MV. The `inserted_at` default uses `now()` at write time and
remains stored through compaction.

## Inspect and restart

- Query API: `POST http://127.0.0.1:18880/api/query`.
  [Readiness](http://127.0.0.1:18880/readyz) and
  [metrics](http://127.0.0.1:18880/metrics) can be opened directly.
- MinIO console: <http://127.0.0.1:29001>, login `minioadmin` / `minioadmin`.
  Open the `ukiel-demo` bucket to see the Parquet files.
- `python3 examples/mv-demo/demo.py status` shows role readiness.
- Server log: `examples/mv-demo/.state/ukield.log`.
- Schemas, MV SQL, and worker settings: [ukield.toml](ukield.toml).

Stop with Ctrl-C and run `make demo` again. Existing topic data is consumed
using saved offsets, the MV retains its cursor, and the runner does not append
the sample again. `check` compares raw and materialized totals after recovery.

To delete this example's saved data after stopping it:

```sh
python3 examples/mv-demo/demo.py reset
make demo
```

After editing a table schema or the MV SQL in `ukield.toml`, reset before
restarting: catalog schemas and pipeline definitions are immutable in this demo.

The example uses Docker project `ukiel-mv-demo`, its own named volumes, and
localhost ports 18880 (query), 25432 (Postgres), 19092 (Kafka), and 29000/29001
(MinIO). It is separate from the repository root's compose stack. A port
conflict is reported at startup. Docker service logs are available with:

```sh
docker compose --project-name ukiel-mv-demo \
  --file examples/mv-demo/compose.yaml logs
```

MVs start at the source's current feed head on first selection. The runner waits
for MV readiness before loading the sample, so it captures every demo order.
For deployment and replay details, see
[MV operations](../../docs/notes/2026-10-22-materialized-views.md).

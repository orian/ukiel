---
name: verify
description: Build/launch/drive recipe for verifying ukield changes end-to-end against the compose stack (Postgres, Kafka, MinIO).
---

# Verifying ukield end-to-end

Surface: the all-in-one `ukield` server (HTTP query API + Prometheus metrics)
fed through Kafka. The e2e `Stack` does NOT wire the cache tier — cache
changes must be verified through `ukield` itself.

## Launch

```bash
docker compose up -d --wait          # postgres :5432, kafka :9092, minio :19000
cargo build -p ukield
# Copy ukield.example.toml, tweak knobs (e.g. set query.cache_dir to a temp dir),
# then run in the background:
./target/debug/ukield --config <play.toml> > ukield.log 2>&1 &
curl -sf http://127.0.0.1:8080/readyz   # "ready" once migrations/bootstrap done
```

## Drive

- Events topic (example config): JSON lines `{"tenant_id": N, "ts": <epoch_ms>,
  "payload": "..."}` — no namespace field; `tenant_id` doubles as the namespace
  for isolation. `ts` must be within `max_event_age_days`/`max_event_future_secs`.
- Bulk produce: write ndjson, then
  `docker exec -i <proj>-kafka-1 /opt/kafka/bin/kafka-console-producer.sh
  --bootstrap-server localhost:9092 --topic events < events.ndjson`
  (200k msgs ≈ 2 s).
- Ingest flushes every `flush_interval_ms` or `max_buffer_rows` (100k) → one L0
  file each; compactor (demo `l0_fanout = 2`) merges within ~`poll_interval_ms`.
  Merge outputs > 10 MiB stream multipart; use ~200-char random-hex payloads to
  fatten files fast (100k rows ≈ 20 MB parquet).
- Query: `POST /api/query` with `{"namespace_id": N, "sql": "...", "format":
  "rows"}` (the endpoint is `/api/query`, not `/query` — the latter returns
  an empty 404 body).
- Metrics: `GET /metrics` on the query listener.
- MinIO surgery (e.g. prove cache serving): `docker exec <proj>-minio-1 sh -c
  "mc alias set local http://127.0.0.1:9000 minioadmin minioadmin && mc rm
  --recursive --force local/ukiel"`.

## Teardown

Kill the ukield process, then `docker compose down -v`.

## Gotchas

- Container names are prefixed by the directory name (worktrees get their own
  prefix and their own volumes — safe to run in parallel with the main checkout).
- `ukield.example.toml` demo knobs (2 s flush, `l0_fanout = 2`, 60 s finalize)
  are already verification-friendly; prefer copying it over hand-writing config.

# Ukiel V1 Roadmap

Spec: `docs/superpowers/specs/2026-07-05-ukiel-design.md`. V1 is split into sequential plans (6 so far), executed in order. Each plan produces working, tested software. Detailed plans are written just-in-time (after the previous plan merges) so interfaces referenced are real, not guessed.

| # | Plan | Scope | Status |
|---|------|-------|--------|
| 1 | `2026-07-05-ukiel-v1-catalog.md` | Cargo workspace, `ukiel-core` (ids, parts, commit ops), `ukiel-catalog` (Postgres schema, optimistic commit protocol, live-part queries, change feed). Tenancy-agnostic vocabulary: namespaces + packing keys | **Executed** |
| 2 | `2026-07-05-ukiel-v1-ingest.md` | `ukiel-ingest`: Kafka consumer → sorted Parquet L0 files on object store → catalog commits. Exactly-once via offsets stored transactionally in the catalog. L0 always packed; per-hypertable placement policy arrives with the compactor | **Executed** |
| 3 | `2026-07-05-ukiel-v1-query.md` | `ukiel-query`: DataFusion `TableProvider` with catalog pruning + namespace/packing-key filter injection (tenant isolation in the SaaS deployment), HTTP SQL endpoint (Arrow Flight SQL post-v1), local-disk read-through cache. Known gap: issue 0001 (endpoint trusts caller-supplied namespace; auth is post-v1) | **Executed** |
| 4 | `2026-07-05-ukiel-v1-compactor.md` | `ukiel-compactor`: durable worker cursors, L0→L1 merge honoring per-hypertable placement policy (`packed`/`separated`), REPLACE commits racing ingest (race tests), atomic key deletion (metadata-only for dedicated files, rewrite for packed). Retention sweep deferred to a later plan on the same machinery | **Executed** |
| 5 | ukiel-v1-e2e | `crates/ukiel-e2e` per the testing design doc (`specs/2026-07-05-ukiel-testing-design.md`): docker-compose stack, model-based oracle harness, scenarios S1–S5, then S6–S7 once plan 4 lands | **Executed** — compose stack + `Stack` harness + all scenarios: S0 smoke, S1 multi-tenant, S2 crash/resume, S3 freshness, S4 poison, S5 scale (1k tenants), S6 compaction-equivalence, S7 key-deletion. All `#[ignore]`d; `make e2e` runs them |
| 6 | `2026-07-06-ukiel-v1-gc.md` | `ukiel-gc`: reap tombstoned parts' objects (grace period + worker-cursor fence for lagging feed consumers), sweep never-committed orphans (age-gated), purge stamps keep catalog rows for feed replay | **Executed** |
| 7 | `2026-07-06-ukiel-matcols.md` | ClickHouse-style column kinds: `default` / `materialized` (computed at ingest, recomputed on rewrites = organic backfill; new `ukiel-expr` crate, deterministic expressions only) / `alias` (query-time, provider projection). Includes schema-adapting compactor reads (additive evolution groundwork) | **Executed** |
| 8 | ukiel-pipelines | Table engines + pipelines per `docs/notes/2026-07-06-pipelines.md`: `stream_tables` (engine=kafka) + `pipelines` catalog entities; kafka→parquet pipeline replaces `TableRoute`; parquet→parquet pipeline = aggregation MV (subsumes the spec's demo-MV V1 item). Egress (parquet→kafka, delivery ladder) follows in a later plan | Not written |
| 10 | `2026-07-06-ukield-server.md` | `ukield`: single deployable binary — config-driven role wiring (ingest/query/compactor/gc), idempotent table bootstrap from TOML, graceful shutdown, `make play` quickstart. Independent of plans 8/9 | **Executed** |

Cross-plan constraints (repeat in every plan's Global Constraints):

- Rust edition 2024, toolchain ≥ 1.96
- Workspace deps pinned in root `Cargo.toml`: tokio 1.52 · sqlx 0.9 · serde_json 1 · thiserror 2 · chrono 0.4 · testcontainers-modules 0.15 · datafusion 54 · **arrow/parquet 58.3 and object_store 0.13** (the versions DataFusion 54 links against — never bump independently) · rdkafka 0.39 · axum 0.8
- No Claude/AI attribution in commit messages
- Integration tests use testcontainers (Docker required); unit tests must pass without Docker

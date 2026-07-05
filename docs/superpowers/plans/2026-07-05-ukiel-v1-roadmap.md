# Ukiel V1 Roadmap

Spec: `docs/superpowers/specs/2026-07-05-ukiel-design.md`. V1 is split into 5 plans, executed in order. Each plan produces working, tested software. Detailed plans are written just-in-time (after the previous plan merges) so interfaces referenced are real, not guessed.

| # | Plan | Scope | Status |
|---|------|-------|--------|
| 1 | `2026-07-05-ukiel-v1-catalog.md` | Cargo workspace, `ukiel-core` (ids, parts, commit ops), `ukiel-catalog` (Postgres schema, optimistic commit protocol, live-part queries, change feed). Tenancy-agnostic vocabulary: namespaces + packing keys | **Ready to execute** |
| 2 | ukiel-v1-ingest | `ukiel-ingest`: Kafka consumer → sorted Parquet L0 files on object store → idempotent catalog commits. Placement policy: packed vs separated files per hypertable | Not written |
| 3 | ukiel-v1-query | `ukiel-query`: DataFusion `TableProvider` with catalog pruning + namespace/packing-key filter injection (tenant isolation in the SaaS deployment), HTTP SQL endpoint, NVMe read-through cache | Not written |
| 4 | ukiel-v1-compactor | `ukiel-compactor`: change-feed cursor, L0→L1 merge (separating heavy keys into dedicated files), REPLACE commits racing ingest; key-deletion and retention sweeps build on the same rewrite machinery | Not written |
| 5 | ukiel-v1-e2e | docker-compose (Kafka, Postgres, MinIO), multi-tenant demo, freshness/isolation/race assertions | Not written |

Cross-plan constraints (repeat in every plan's Global Constraints):

- Rust edition 2024, toolchain ≥ 1.96
- Workspace deps pinned in root `Cargo.toml`: tokio 1.52 · sqlx 0.9 · serde_json 1 · thiserror 2 · chrono 0.4 · testcontainers-modules 0.15 · datafusion 54 · object_store 0.14
- No Claude/AI attribution in commit messages
- Integration tests use testcontainers (Docker required); unit tests must pass without Docker

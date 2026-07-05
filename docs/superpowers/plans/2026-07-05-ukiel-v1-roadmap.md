# Ukiel V1 Roadmap

Spec: `docs/superpowers/specs/2026-07-05-ukiel-design.md`. V1 is split into 5 plans, executed in order. Each plan produces working, tested software. Detailed plans are written just-in-time (after the previous plan merges) so interfaces referenced are real, not guessed.

| # | Plan | Scope | Status |
|---|------|-------|--------|
| 1 | `2026-07-05-ukiel-v1-catalog.md` | Cargo workspace, `ukiel-core` (ids, parts, commit ops), `ukiel-catalog` (Postgres schema, optimistic commit protocol, live-part queries, change feed). Tenancy-agnostic vocabulary: namespaces + packing keys | **Executed** |
| 2 | `2026-07-05-ukiel-v1-ingest.md` | `ukiel-ingest`: Kafka consumer → sorted Parquet L0 files on object store → catalog commits. Exactly-once via offsets stored transactionally in the catalog. L0 always packed; per-hypertable placement policy arrives with the compactor | **Executed** |
| 3 | `2026-07-05-ukiel-v1-query.md` | `ukiel-query`: DataFusion `TableProvider` with catalog pruning + namespace/packing-key filter injection (tenant isolation in the SaaS deployment), HTTP SQL endpoint (Arrow Flight SQL post-v1), local-disk read-through cache. Known gap: issue 0001 (endpoint trusts caller-supplied namespace; auth is post-v1) | **Executed** |
| 4 | ukiel-v1-compactor | `ukiel-compactor`: change-feed cursor, L0→L1 merge (separating heavy keys into dedicated files), REPLACE commits racing ingest; key-deletion and retention sweeps build on the same rewrite machinery | Not written |
| 5 | ukiel-v1-e2e | `crates/ukiel-e2e` per the testing design doc (`specs/2026-07-05-ukiel-testing-design.md`): docker-compose stack, model-based oracle harness, scenarios S1–S5 (S6–S7 activate with plan 4) | Started early: compose stack, Makefile, `Stack` harness and S0 smoke exist; plan for S1–S5 not written |

Cross-plan constraints (repeat in every plan's Global Constraints):

- Rust edition 2024, toolchain ≥ 1.96
- Workspace deps pinned in root `Cargo.toml`: tokio 1.52 · sqlx 0.9 · serde_json 1 · thiserror 2 · chrono 0.4 · testcontainers-modules 0.15 · datafusion 54 · **arrow/parquet 58.3 and object_store 0.13** (the versions DataFusion 54 links against — never bump independently) · rdkafka 0.39 · axum 0.8
- No Claude/AI attribution in commit messages
- Integration tests use testcontainers (Docker required); unit tests must pass without Docker

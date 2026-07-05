# Ukiel Testing Design: Philosophy & End-to-End Tests

Companion to `2026-07-05-ukiel-design.md`. This document defines how Ukiel is tested at every layer and designs the end-to-end (e2e) suite that runs against real Kafka, Postgres, and MinIO. The task-level implementation plan for the e2e suite is plan 5 of the roadmap and derives from this document.

## Testing philosophy

### The pyramid, adapted to a data system

| Layer | Dependencies | Where | Budget | Runs |
|---|---|---|---|---|
| **Unit** | none (no I/O, no Docker) | `#[cfg(test)]` in each crate | < 1s each | every `cargo test` |
| **Component** | real Postgres/Kafka via testcontainers, in-memory object store | `crates/*/tests/` | < 2 min per crate | every `cargo test` (Docker required) |
| **End-to-end** | docker-compose: Kafka + Postgres + MinIO, all Ukiel components wired | `crates/ukiel-e2e/` | < 10 min total | on demand + CI nightly |

Unit tests cover pure logic: schema mapping, Parquet encoding and sort order, config parsing. Component tests prove each crate's *contract* against its real dependency (all of plans 1–3's integration tests are this layer). E2e tests prove *system invariants* across process boundaries — things no single crate can prove alone.

Component tests run with `--test-threads=2` (`make test`): each test starts its own containers, and unbounded parallelism (one container start per CPU) makes testcontainers' log-wait flake with `EndOfStream` errors.

### Principles

1. **Real dependencies over mocks.** Ukiel's correctness claims — commit atomicity, optimistic concurrency, exactly-once ingest — *are* Postgres transaction semantics and Kafka replay semantics. A mocked Postgres would test our assumptions instead of our claims. We mock only what is both slow *and* semantically boring for the test at hand: component tests use `object_store::memory::InMemory` because S3 semantics aren't what they're proving; the e2e layer uses MinIO because there they are.

2. **Test invariants, not implementations.** E2e assertions are statements about the system a user could observe ("every produced event is queryable exactly once"), never about internals ("the buffer flushed twice"). Internals are free to change; invariants are the spec.

3. **Adversity is first-class.** Crashes, restarts, races, and poison inputs are scripted scenarios with the same status as happy paths — the architecture's whole point is surviving them. Every "the system tolerates X" claim in the design doc must map to a scenario here.

4. **Deterministic by construction.** No bare sleeps: every wait is a poll with a deadline against an observable condition. Test data is generated from a fixed seed. Assertions compare *sets and totals*, never timing-dependent orderings (unless `ORDER BY` makes them deterministic).

5. **The oracle is a model, not a snapshot.** The e2e harness records everything it produces into an in-memory expected-state model (per namespace: the multiset of rows sent, minus rows deleted). Every assertion is a diff between a SQL query result and the model. No golden files — they rot.

6. **Tests state their layer honestly.** If a test needs Docker, it lives in `tests/`; if it needs the full compose stack, it lives in `ukiel-e2e` and is `#[ignore]`d so a plain `cargo test` never hangs on missing infrastructure.

## End-to-end suite design

### Environment

One `docker-compose.yml` at the repo root:

- `kafka` — `apache/kafka` (KRaft, no zookeeper), port 9092.
- `postgres` — `postgres:17`, port 5432.
- `minio` — `minio/minio` + a one-shot `mc` init container creating bucket `ukiel`, host ports 19000/19001 (9000 is contested on dev machines — ClickHouse native protocol lives there).

The e2e harness does **not** manage containers (unlike component tests): it connects to whatever the compose stack exposes, configured via env vars with compose-matching defaults — `UKIEL_E2E_KAFKA` (default `127.0.0.1:9092`), `UKIEL_E2E_PG` (default `postgres://postgres:postgres@127.0.0.1:5432/postgres`), `UKIEL_E2E_S3` (default `http://127.0.0.1:19000`, bucket `ukiel`, credentials `minioadmin`/`minioadmin`). This keeps the suite equally runnable against a laptop compose stack, a CI service block, or a staging environment.

Workflow:

```
docker compose up -d --wait
cargo test -p ukiel-e2e -- --ignored --test-threads=1
docker compose down -v
```

wrapped in a `Makefile` target (`make e2e`). `--test-threads=1` because scenarios share the Kafka/Postgres instances; each scenario isolates itself by using fresh topic/hypertable/namespace names derived from a per-run nonce, so a dirty stack never causes false failures.

### Harness (`crates/ukiel-e2e`)

A test-only workspace member (no library code beyond the harness):

- **`Stack`** — connects to env-configured endpoints; builds `PostgresCatalog` (runs migrations), the S3 `object_store` (MinIO, path-style), an rdkafka producer, and spawns in-process components: `IngestWorker` (real Kafka → real MinIO) and the query HTTP server (`ukiel_query::server::router`) on an ephemeral port. Components run as in-process tokio tasks in v1 — crash injection = aborting the task and spawning a fresh instance, which exercises the same recovery paths as a process kill (catalog-anchored offsets, no shared memory). Separate-process orchestration is a later hardening step, noted per scenario.
- **`Model`** — the oracle: `HashMap<NamespaceId, Vec<ExpectedRow>>` populated by the produce helpers; `assert_converged(deadline)` polls the HTTP endpoint per namespace until results equal the model or the deadline expires (then fails with the diff).
- **Scenario files** — one file per scenario below, each a `#[tokio::test] #[ignore]`.

### System invariants

- **I1 — Exactly-once:** every message produced to Kafka appears in query results exactly once, regardless of worker crashes and restarts.
- **I2 — Isolation:** namespace A's SQL can never observe namespace B's rows, including rows that share a packed Parquet file.
- **I3 — Freshness:** an event is queryable within `flush_interval + grace` of production (asserted ≤ 20s with a 10s interval, per spec target).
- **I4 — Rewrite equivalence:** background rewrites (compaction) never change query results, only file layout (part count / levels).
- **I5 — Race safety:** concurrent ingest and compaction commits never lose or duplicate rows.
- **I6 — Progress under poison:** malformed messages are skipped and never stall the stream (offsets advance past them).
- **I7 — Deletion:** after a packing key's data is deleted, no query returns it; all other keys are unaffected. *(activates with plan 4's deletion worker)*

### Scenarios

| # | Scenario | Proves | Sketch |
|---|---|---|---|
| S1 | **Multi-tenant happy path** | I1, I2 | Seeded producer sends interleaved events for 10 namespaces (sizes skewed so several share packed files); model-converge per namespace over HTTP; cross-check one namespace sees exactly 0 of another's marker rows. |
| S2 | **Crash & resume** | I1 | Produce continuously; abort the ingest task mid-stream (between flushes, and — via a second run — immediately after a flush); start a fresh worker; keep producing; assert convergence with zero duplicates and zero losses. |
| S3 | **Freshness** | I3 | Timestamp each produce; poll the query endpoint for that row; assert p95 visible-latency ≤ 20s with `flush_interval_ms = 10_000`. |
| S4 | **Poison stream** | I6 | Interleave garbage payloads (non-JSON, missing ts, wrong types) with valid events; assert all valid events converge and `ingest_offsets` advances past the garbage. |
| S5 | **Scale smoke** | I2, catalog planning | 1,000 namespaces × few rows in one hypertable (heavily packed); query a random sample of 50 namespaces; assert correctness and that per-query part pruning returns bounded file counts (catalog query, not a perf benchmark). |
| S6 | **Compaction equivalence** *(with plan 4)* | I4, I5 | Run S1's load; snapshot per-namespace results; run compactor to quiescence *while ingest continues*; assert results identical, L0 part count reduced, no commit conflicts surfaced as data errors. |
| S7 | **Key deletion** *(with plan 4)* | I7 | After S1 converges, delete one namespace's key; assert its queries return empty, neighbors in the same packed files are intact. |

S1–S5 are implementable immediately (plans 1–3 shipped); S6–S7 are specified now so the compactor plan is written against them.

### CI

- **Every push/PR:** `cargo fmt --check`, `clippy -D warnings`, `cargo test` (unit + component; the runner provides Docker for testcontainers).
- **Nightly + manual dispatch:** the compose-based e2e job (`make e2e`). E2e is not PR-gating until its flake rate proves negligible; a failed nightly is treated as a release blocker, not ignored.

### Out of scope (deliberate)

Performance benchmarking (separate criterion-based effort once the compactor exists), network-partition injection (toxiproxy — later hardening), multi-node worker fleets (v1 runs one ingest worker; the partition-sharding design lands first), and UI/BI-protocol testing (Arrow Flight SQL is post-v1).

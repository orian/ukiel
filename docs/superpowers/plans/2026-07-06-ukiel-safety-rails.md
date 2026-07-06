# Ukiel Plan 19: Safety Rails (Statement Timeout, Offset CAS, Event-Time Bounds) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close three open correctness/hygiene issues with small, independent rails: (a) **statement timeout** on the query endpoint plus the `query_timeout < gc.tombstone_grace` startup invariant (issue 0002 — GC's safety window is currently enforced by nobody); (b) **ingest offset CAS** so two ingest workers on one topic become a loud error instead of silent data duplication (issue 0003); (c) **event-time bounds** at ingest so bad timestamps are skipped like poison instead of minting junk partitions or the permanent `"unknown"` day (issue 0004).

**Architecture:** All three are independent of each other and of plans 17/18. (a) lives in `ukiel-query::server` (wrap planning+collect in `tokio::time::timeout`, HTTP 408 on expiry) and `ukield` (config + refuse-to-start validation). (b) is one SQL change in `ukiel-catalog::commit_inner` — the offsets upsert becomes conditional on `ingest_offsets.next_offset <= range.first` (`<=`, not `=`, so offset gaps from compacted topics stay legal) plus a dedicated `CatalogError::OffsetRace` that rolls the whole commit transaction back. (c) is a pure bounds predicate in `ukiel-ingest` applied in `buffer_message`, removing the `"unknown"` fallback (an unrepresentable `ts` is by definition out of bounds); skipped rows advance offsets exactly like poison messages. The bounds are **asymmetric**: the future bound is server-wide and hard (nothing legitimate is from the future), but the past bound is a server *default* overridable per table with `0` = unbounded — **customer migrations from other systems legitimately carry decades-old history and must never be silently dropped**; a backfill table lifts its own bound without opening the window for every other table.

**Tech Stack:** Rust edition 2024, sqlx/Postgres, axum/tokio, testcontainers component tests.

**Prerequisites:** None (independent of plans 17/18; runs before or after them per the roadmap's recommended order).

## Global Constraints

- Rust edition 2024, toolchain ≥ 1.96.
- Component tests via `make test` (Docker required); the pure-function tests added by Tasks 1 and 3 must pass without Docker (`cargo test -p <crate> --lib`).
- **Every existing test stays green after every task.** Defaults are chosen so nothing existing trips: timeout 300s (no test query runs that long), CAS holds for any single writer (all existing ingest tests are single-writer), event-time bounds default to ±10 years past / 1h future (all test fixtures use near-epoch or current timestamps — verify and widen the test config if any fixture uses tiny epoch-ms values like `ts: 10`; the *ingest worker* applies bounds, `rows_to_parquet`/`encode_rows` unit helpers do not).
- Commit messages: conventional style, no Claude/AI attribution.
- Every task ends with `cargo fmt && cargo clippy --all-targets -- -D warnings` clean.

---

### Task 1: Statement timeout in ukiel-query + ukield invariant check

**Files:**
- Modify: `crates/ukiel-query/src/server.rs`
- Modify: `crates/ukield/src/config.rs` (`QuerySection`), `crates/ukield/src/run.rs` (AppState construction)
- Modify: `ukield.example.toml`
- Test: the query crate's existing HTTP/component test file (wherever `AppState`/`router` is exercised today — adapt to its harness), plus a ukield config-validation unit test

**Interfaces:**
- Produces: `AppState.statement_timeout: std::time::Duration`; queries exceeding it return HTTP 408 with JSON `{"error": "query exceeded statement timeout (Ns)"}`; `QuerySection.statement_timeout_secs: u64` (serde default 300); `ukield` **refuses to start** when the gc role is enabled and `query.statement_timeout_secs >= gc.tombstone_grace_secs` (the issue-0002 invariant, enforced at the wiring point).

- [ ] **Step 1: Write the failing timeout test**

In the query crate's component test harness, add a test that builds `AppState` with `statement_timeout: Duration::ZERO` and posts any valid query:

```rust
#[tokio::test]
async fn statement_timeout_returns_408() {
    // Same fixture as the existing query endpoint tests, but with
    // statement_timeout: Duration::ZERO on AppState.
    // POST /api/query {namespace_id, sql: "SELECT 1"}.
    // Assert: status 408, body is JSON with an "error" field mentioning
    // the statement timeout. (ZERO makes expiry deterministic — no need
    // for a genuinely slow query.)
}
```

Write it fully against the harness's real fixture names. All *other* `AppState` construction sites (tests + `run.rs`) get `statement_timeout: Duration::from_secs(300)` — compile errors enumerate them.

- [ ] **Step 2: Verify it fails**

Run: `cargo test -p ukiel-query -- --test-threads=2`
Expected: compile error (`statement_timeout` field doesn't exist).

- [ ] **Step 3: Implement the timeout**

In `crates/ukiel-query/src/server.rs`:

1. Add to `AppState`:

```rust
    /// Issue 0002: bounds query duration so the GC tombstone grace is a real
    /// guarantee (deployment invariant: this < gc.tombstone_grace). Also the
    /// runaway-query rail for a tenant-facing SQL endpoint.
    pub statement_timeout: std::time::Duration,
```

2. In `run_query_inner`, wrap planning + execution (from `session_for_namespace` through `df.collect()`) in one timeout — replace that span of the body with:

```rust
    let timeout = state.statement_timeout;
    let (planned_schema, batches): (SchemaRef, Vec<_>) =
        tokio::time::timeout(timeout, async {
            let ctx = session_for_namespace(
                &state.catalog,
                NamespaceId(req.namespace_id),
                state.store.clone(),
                &state.store_url,
            )
            .await
            .map_err(bad_request)?;

            let options = SQLOptions::new()
                .with_allow_ddl(false)
                .with_allow_dml(false)
                .with_allow_statements(false);
            let df = ctx
                .sql_with_options(&req.sql, options)
                .await
                .map_err(bad_request)?;

            let planned_schema: SchemaRef = df.schema().inner().clone();
            let batches = df.collect().await.map_err(bad_request)?;
            Ok::<_, (StatusCode, String)>((planned_schema, batches))
        })
        .await
        .map_err(|_| {
            (
                StatusCode::REQUEST_TIMEOUT,
                format!(
                    "query exceeded statement timeout ({}s)",
                    timeout.as_secs()
                ),
            )
        })??;
```

(Result formatting stays outside the timeout — it's cheap and already has the data.)

- [ ] **Step 4: Wire ukield config + the invariant**

1. `crates/ukield/src/config.rs` — add to `QuerySection` (it is `#[serde(default)]`; extend its `Default` accordingly):

```rust
    /// Statement timeout in seconds (issue 0002: must stay below
    /// gc.tombstone_grace_secs — validated at startup).
    pub statement_timeout_secs: u64, // Default: 300
```

2. Add a validation function in `config.rs` and call it wherever the config is loaded/validated before workers spawn (adapt to the existing load path in `main.rs`/`run.rs`):

```rust
/// Issue 0002 invariant: a query that outlives the GC tombstone grace can
/// observe deleted objects. Refuse the configuration outright.
pub fn validate(cfg: &Config) -> anyhow::Result<()> {
    if cfg.roles.contains(&Role::Query)
        && cfg.roles.contains(&Role::Gc)
        && cfg.query.statement_timeout_secs >= cfg.gc.tombstone_grace_secs
    {
        anyhow::bail!(
            "query.statement_timeout_secs ({}) must be < gc.tombstone_grace_secs ({}): \
             a query outliving the grace can read reaped objects (issue 0002)",
            cfg.query.statement_timeout_secs,
            cfg.gc.tombstone_grace_secs
        );
    }
    Ok(())
}
```

Add a unit test beside the existing config tests: valid defaults pass (300 < 900); `statement_timeout_secs = 900` with both roles fails with a message containing "issue 0002". When the roles are split across processes the check can't see both sides — the invariant note stays in the config comment; do not fail single-role deployments.

3. `run.rs`: pass `statement_timeout: Duration::from_secs(cfg.query.statement_timeout_secs)` into `AppState`.

4. `ukield.example.toml` `[query]` section gains:

```toml
statement_timeout_secs = 300 # must be < gc.tombstone_grace_secs (issue 0002)
```

- [ ] **Step 5: Verify, lint, commit**

Run: `cargo test -p ukiel-query -p ukield -- --test-threads=2`
Expected: new tests + all existing PASS.

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add crates/ukiel-query crates/ukield ukield.example.toml
git commit -m "feat: statement timeout with gc-grace startup invariant (issue 0002)"
```

---

### Task 2: Ingest offset CAS (issue 0003)

**Files:**
- Modify: `crates/ukiel-catalog/src/error.rs`
- Modify: `crates/ukiel-catalog/src/commit.rs`
- Test: `crates/ukiel-catalog/tests/catalog_test.rs`

**Interfaces:**
- Produces: `CatalogError::OffsetRace { topic: String, partition: i32 }`; `commit_with_offsets` fails with it (whole transaction rolled back — **no parts inserted**) when the stored `next_offset` has advanced past the range being committed. Single-writer sequential flushes are unaffected; offset gaps (compacted topics) stay legal via `<=`.

- [ ] **Step 1: Write the failing test**

Append to `crates/ukiel-catalog/tests/catalog_test.rs` (adapting helper names as before):

```rust
#[tokio::test]
async fn duplicate_offset_commit_is_rejected_and_rolled_back() {
    let env = setup().await;
    let range = |first, last| ukiel_catalog::OffsetRange {
        topic: "events".to_string(),
        partition: 0,
        first,
        last,
    };

    // Writer A commits rows for offsets 0..=9.
    commit_add_with_offsets(&env, "p-a", &[range(0, 9)]).await.unwrap();
    let live_before = env.catalog.live_parts(env.ht, None).await.unwrap().len();

    // Writer B (a duplicate consumer) tries to commit the same offsets.
    let err = commit_add_with_offsets(&env, "p-b", &[range(0, 9)]).await.unwrap_err();
    assert!(
        matches!(err, ukiel_catalog::CatalogError::OffsetRace { ref topic, partition: 0 } if topic == "events"),
        "{err:?}"
    );

    // The whole transaction rolled back: no duplicate part appeared.
    assert_eq!(env.catalog.live_parts(env.ht, None).await.unwrap().len(), live_before);

    // The real writer continues from 10 — sequential progress still works.
    commit_add_with_offsets(&env, "p-c", &[range(10, 19)]).await.unwrap();

    // Offset gaps stay legal (compacted topics): next stored is 20, a range
    // starting at 25 commits fine.
    commit_add_with_offsets(&env, "p-d", &[range(25, 30)]).await.unwrap();
}
```

(`commit_add_with_offsets` = `commit_with_offsets` with one tiny `CommitOp::Add` part at the given path — inline or a small helper per the file's pattern. Export `OffsetRace` alongside the existing `CatalogError` re-export if needed.)

- [ ] **Step 2: Verify it fails**

Run: `cargo test -p ukiel-catalog --test catalog_test duplicate_offset_commit`
Expected: compile error (`OffsetRace` doesn't exist), then — once stubbed — the duplicate commit *succeeds* under the old upsert, failing the `unwrap_err`.

- [ ] **Step 3: Implement**

1. `crates/ukiel-catalog/src/error.rs` — add the variant:

```rust
    /// A concurrent writer already advanced this topic-partition's ingest
    /// offset past the range being committed (issue 0003: two ingest
    /// workers on one topic). The commit was rolled back; the losing worker
    /// must stop — its consumed batch is a duplicate.
    #[error(
        "ingest offset race on {topic}/{partition}: stored next_offset moved past this commit's range"
    )]
    OffsetRace { topic: String, partition: i32 },
```

2. `crates/ukiel-catalog/src/commit.rs` — replace the offsets loop body in `commit_inner`:

```rust
        for range in offsets {
            // CAS, not upsert (issue 0003): the update only applies while the
            // stored cursor has not advanced past this range's start. `<=`
            // (not `=`) keeps offset gaps legal (compacted topics). Zero rows
            // affected = another writer got here first: roll everything back.
            let affected = sqlx::query(
                "INSERT INTO ingest_offsets (hypertable_id, topic, kafka_partition, next_offset)
                 VALUES ($1, $2, $3, $4)
                 ON CONFLICT (hypertable_id, topic, kafka_partition)
                 DO UPDATE SET next_offset = EXCLUDED.next_offset, updated_at = now()
                 WHERE ingest_offsets.next_offset <= $5",
            )
            .bind(hypertable_id.0)
            .bind(&range.topic)
            .bind(range.partition)
            .bind(range.last + 1)
            .bind(range.first)
            .execute(&mut *tx)
            .await?
            .rows_affected();

            if affected == 0 {
                return Err(CatalogError::OffsetRace {
                    topic: range.topic.clone(),
                    partition: range.partition,
                });
            }
        }
```

(Returning early drops `tx` → rollback, same mechanism as `tombstone_parts` conflicts.)

- [ ] **Step 4: Verify pass — including the ingest suite**

Run: `cargo test -p ukiel-catalog -p ukiel-ingest -- --test-threads=2`
Expected: the new test passes; every existing ingest test (single-writer crash/resume, poison, offset-only flushes) stays green — a single writer's `range.first` always equals or exceeds the stored cursor. The ingest worker needs no change: `OffsetRace` propagates as a fatal route error and the worker exits loudly, which is the guardrail's intended behavior (misconfiguration → error, not duplicates).

- [ ] **Step 5: Lint and commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add crates/ukiel-catalog
git commit -m "feat: ingest offset CAS - duplicate writers fail loudly (issue 0003)"
```

---

### Task 3: Event-time bounds at ingest (issue 0004)

**Files:**
- Modify: `crates/ukiel-ingest/src/config.rs`
- Modify: `crates/ukiel-ingest/src/consumer.rs`
- Modify: `crates/ukield/src/config.rs` (`IngestSection`), `crates/ukield/src/run.rs`
- Modify: `ukield.example.toml`

**Interfaces:**
- Produces: `IngestConfig { max_event_age_days: u64 (serde default 3650; server-wide default), max_event_future_secs: u64 (serde default 3600; server-wide, hard), .. }`; `TableRoute.max_event_age_days: Option<u64>` (serde default `None` = use the server default; `Some(0)` = **unbounded past** — the migration/backfill setting); `pub(crate) fn event_time_in_bounds(ts_ms: i64, now_ms: i64, max_age_days: u64 /* 0 = unbounded */, max_future_secs: u64) -> bool`; out-of-bounds rows are skipped with offsets advancing (poison semantics); the `day = "unknown"` fallback is deleted.

- [ ] **Step 1: Write the failing unit tests (no Docker)**

Extend the `mod tests` in `crates/ukiel-ingest/src/consumer.rs` (the `cfg()` helper from plan 18 gains the two fields; if plan 18 hasn't landed, create the helper the same way):

```rust
    #[test]
    fn event_time_bounds() {
        let now_ms: i64 = 1_780_000_000_000; // fixed "now" for determinism

        // Normal streaming table: (max_age_days, max_future_secs) = (3650, 3600).
        assert!(event_time_in_bounds(now_ms, now_ms, 3650, 3600));
        assert!(event_time_in_bounds(now_ms - 24 * 3600 * 1000, now_ms, 3650, 3600)); // yesterday
        assert!(event_time_in_bounds(now_ms + 3_599_000, now_ms, 3650, 3600)); // 59m59s ahead

        // Too far future: year-3000-style producer bugs.
        assert!(!event_time_in_bounds(now_ms + 3_601_000, now_ms, 3650, 3600));
        // Too old: beyond the age window.
        let age_ms = 3650i64 * 24 * 3600 * 1000;
        assert!(!event_time_in_bounds(now_ms - age_ms - 1000, now_ms, 3650, 3600));

        // Migration/backfill table: max_age_days = 0 -> unbounded past.
        // Decades-old history from a customer migrating off another system
        // is admitted...
        assert!(event_time_in_bounds(0, now_ms, 0, 3600)); // epoch-era events
        assert!(event_time_in_bounds(now_ms - age_ms * 5, now_ms, 0, 3600));
        // ...while the future bound still catches garbage on that table too.
        assert!(!event_time_in_bounds(now_ms + 3_601_000, now_ms, 0, 3600));

        // Unrepresentable timestamps are out of bounds regardless of config.
        assert!(!event_time_in_bounds(i64::MAX, now_ms, 0, 3600));
        assert!(!event_time_in_bounds(i64::MIN, now_ms, 0, 3600));
    }
```

- [ ] **Step 2: Verify it fails**

Run: `cargo test -p ukiel-ingest --lib`
Expected: compile error (fields + function missing).

- [ ] **Step 3: Implement**

1. `crates/ukiel-ingest/src/config.rs` — add to `IngestConfig` with serde defaults (same pattern as plan 18's fields):

```rust
    /// Server default: reject (skip like poison) events older than this
    /// many days (issue 0004). Overridable per table via
    /// `TableRoute.max_event_age_days` — migration/backfill tables set 0
    /// (unbounded) so decades-old history is never dropped.
    #[serde(default = "default_max_event_age_days")]
    pub max_event_age_days: u64,
    /// Server-wide, hard: reject (skip like poison) events further in the
    /// future than this (clock skew allowance; catches sec-vs-ms unit
    /// bugs). Not overridable — nothing legitimate is from the future.
    #[serde(default = "default_max_event_future_secs")]
    pub max_event_future_secs: u64,
```

```rust
fn default_max_event_age_days() -> u64 {
    3650
}

fn default_max_event_future_secs() -> u64 {
    3600
}
```

and to `TableRoute`:

```rust
    /// Per-table past bound override (issue 0004): `None` = the server
    /// default (`IngestConfig.max_event_age_days`), `Some(0)` = unbounded —
    /// the migration/backfill setting. Post-plan-8 this becomes a
    /// per-pipeline property.
    #[serde(default)]
    pub max_event_age_days: Option<u64>,
```

2. `crates/ukiel-ingest/src/consumer.rs`:

Add near `flush_decision`:

```rust
/// Issue 0004: bounds event time so bad producer timestamps can't mint junk
/// partitions (or the old permanent "unknown" day). Asymmetric by design:
/// `max_age_days` is the per-table-resolvable past bound (0 = unbounded,
/// for migrations importing decades-old history), the future bound is hard.
/// Pure for testability; an unrepresentable ts is out of bounds regardless.
pub(crate) fn event_time_in_bounds(
    ts_ms: i64,
    now_ms: i64,
    max_age_days: u64,
    max_future_secs: u64,
) -> bool {
    if chrono::DateTime::from_timestamp_millis(ts_ms).is_none() {
        return false; // no partition value can be derived: junk by definition
    }
    if max_age_days > 0 {
        let max_age_ms = (max_age_days as i64).saturating_mul(24 * 3600 * 1000);
        if ts_ms < now_ms.saturating_sub(max_age_ms) {
            return false;
        }
    }
    let max_future_ms = (max_future_secs as i64).saturating_mul(1000);
    ts_ms <= now_ms.saturating_add(max_future_ms)
}
```

In `buffer_message`, after the `ts` extraction and before the packing-key check, insert:

```rust
        // Out-of-bounds event time: skip like poison — the offset above has
        // already advanced, the row is dropped visibly (warn), and no junk
        // partition is minted (issue 0004). The past bound resolves per
        // table so migration streams can carry old history.
        let max_age = self
            .route
            .max_event_age_days
            .unwrap_or(self.config.max_event_age_days);
        if !event_time_in_bounds(
            ts,
            chrono::Utc::now().timestamp_millis(),
            max_age,
            self.config.max_event_future_secs,
        ) {
            tracing::warn!(ts, "event time out of bounds; row skipped");
            return;
        }
```

and replace the `day` derivation's fallback:

```rust
        let day = chrono::DateTime::from_timestamp_millis(ts)
            .map(|d| d.date_naive().to_string())
            .unwrap_or_else(|| "unknown".to_string());
```

with:

```rust
        let Some(day) = chrono::DateTime::from_timestamp_millis(ts)
            .map(|d| d.date_naive().to_string())
        else {
            return; // unreachable given the bounds check; never mint "unknown"
        };
```

3. `ukield`: add the two server fields to `IngestSection` + `Default` (3650 / 3600) and pass through in `run.rs`; add `max_event_age_days: Option<u64>` (serde default `None`) to `TableConfig` and plumb it into the `TableRoute` construction in `run.rs`. Append to `ukield.example.toml` `[ingest]`:

```toml
max_event_age_days = 3650    # server default past bound (per-table overridable)
max_event_future_secs = 3600 # hard future bound (unit-bug detector)
```

and under the `[[tables]]` comment block:

```toml
# max_event_age_days = 0  # per-table past-bound override; 0 = unbounded —
#                         # set on backfill tables when migrating customers
#                         # from other systems, so old history is never dropped
```

- [ ] **Step 4: Verify — unit, component, whole workspace**

Run: `cargo test -p ukiel-ingest --lib`
Expected: `event_time_bounds` PASS.

Run: `cargo test -p ukiel-ingest -p ukield -- --test-threads=2 && make test`
Expected: all green. If any existing consumer test produces fixture events with tiny epoch-ms timestamps (e.g. `ts: 10`), those rows would now be skipped as ancient — set that test route's `max_event_age_days: Some(0)` (unbounded, the same knob a migration table uses) rather than weakening the server default.

- [ ] **Step 5: Lint and commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add crates/ukiel-ingest crates/ukield ukield.example.toml
git commit -m "feat: event-time bounds at ingest - no junk partitions, no unknown day (issue 0004)"
```

---

### Task 4: Docs — issue statuses, design-doc flips, roadmap

**Files:**
- Modify: `docs/issues/0002-gc-grace-vs-query-duration.md`, `docs/issues/0003-concurrent-ingest-writers-duplicate.md`, `docs/issues/0004-unbounded-event-time.md`
- Modify: `docs/superpowers/specs/2026-07-05-ukiel-design.md`
- Modify: `docs/superpowers/plans/2026-07-05-ukiel-v1-roadmap.md`

- [ ] **Step 1: Full verification first**

Run: `cargo fmt --check && cargo clippy --all-targets -- -D warnings && make test`
Expected: all green.

- [ ] **Step 2: Flip statuses**

- Issues 0002, 0003, 0004: `Status:` → **Resolved** (plan 19), one line each on what shipped (timeout+invariant / offset CAS / bounds).
- Design doc "Known gaps & deferred work": remove or mark resolved the statement-timeout, concurrent-ingest, and event-time bullets (keep the partition-subset-leases future-work note from 0003).
- Roadmap row 19: status → **Executed**.

- [ ] **Step 3: Commit**

```bash
git add docs/
git commit -m "docs: issues 0002-0004 resolved by safety rails, roadmap row 19 executed"
```

---

## Self-review notes

- **Issue coverage**: 0002 → Task 1 (timeout + startup invariant, wrap covers planning *and* collect); 0003 → Task 2 (CAS with `<=` gap tolerance, rollback proven by the no-duplicate-part assertion); 0004 → Task 3 (asymmetric bounds — hard future, per-table past with `0` = unbounded for migrations — plus `"unknown"` removal; poison semantics preserved: offsets advance before the skip).
- **Type consistency**: `AppState.statement_timeout: Duration` matches Task 1's test and `run.rs` wiring; `CatalogError::OffsetRace { topic, partition }` matches the test's `matches!`; `event_time_in_bounds(i64, i64, u64, u64) -> bool` matches its unit tests and the `buffer_message` call site (which resolves `route.max_event_age_days.unwrap_or(config.max_event_age_days)`).
- **Independence**: no task depends on plans 17/18; the only shared surface with plan 18 is the `cfg()` test helper in `consumer.rs` (whichever plan lands second extends it — both say so).

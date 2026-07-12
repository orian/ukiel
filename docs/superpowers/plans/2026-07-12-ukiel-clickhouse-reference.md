# Ukiel Plan 34: ClickHouse Reference & the Substrate Ladder Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Plan 32 produced one number — *ukiel costs ~2.1× bare DataFusion on ClickBench* — and that number is both **unanchored** (2.1× of what, in absolute terms? is this machine fast?) and **under-decomposed** (the ratio silently bundles object-store latency, ukiel's file format, and ukiel's stack, because the reference read local files while ukiel read MinIO over HTTP). This plan fixes both:

- **(a) Anchor it.** Run ClickBench's 43 queries on **ClickHouse, on this machine**, on ClickHouse's own terms: upstream's verbatim `create.sql` + `queries.sql`, the 105-column MergeTree, loaded from the same partitioned parquet we already have on disk. Two things fall out — the purpose-built-OLAP ceiling for this workload, and a **machine calibration**: our total against ClickHouse's *published* ClickBench figure tells us whether this box is fast, slow, or ordinary, which is what makes every other number in `bench/README.md` interpretable at all.
- **(b) Decompose it** — the **substrate ladder**. Six measurements of the same 43 queries where each rung changes exactly one variable, so every gap is attributable:

  | # | rung | gap to the rung above isolates |
  |---|---|---|
  | A | DataFusion / raw parquet / **local disk** | — (baseline; exists, plan 32) |
  | B | DataFusion / **ukiel's own parts** / local disk | ukiel's **file format & layout** (int64 widening, ZSTD, sorting, day-split, compaction state) |
  | C | **ukiel** / local-filesystem store | ukiel's **catalog + provider + cache stack** |
  | D | **ukiel** / MinIO (exists, plan 32) | the **object-store substrate** |
  | E | **ClickHouse** / raw parquet / local disk | **engine quality** on bytes identical to rung A |
  | F | **ClickHouse** / MergeTree / local disk | what a **native format + primary index** buys over parquet |

  A→B→C→D decomposes ukiel's 2.1×. A-vs-E is the honest engine comparison (same files, same substrate, no format advantage to either). E→F is what ClickHouse's own format is worth. Nobody has to take "2.1×" on faith again.
- **(c) JSONBench on ClickHouse** — its 5 Bluesky queries against ClickHouse's `JSON` type over the same ndjson, so that suite has a reference too instead of a lone ukiel number.

**Architecture:** **This plan adds no product code** — everything lands in the `bench` binary (`crates/ukiel-e2e/src/bin/bench/`), `crates/ukiel-e2e/src/lib.rs` (test-harness crate), and `docker-compose.yml`. "Every existing test stays green" is therefore nearly free.

Two pieces are **already in flight in the working tree** (uncommitted at the time of writing) and this plan **builds on them rather than duplicating them** — verify their exact shape before starting, and adapt if they landed differently:
- `Stack::start()` honours **`UKIEL_E2E_STORE_DIR`**, swapping MinIO for an `object_store::local::LocalFileSystem` (`crates/ukiel-e2e/src/lib.rs:75`). This *is* rung C: ukiel with the object store removed. It is a strictly better instrument than the alternative (uploading 14 GB of raw parquet into MinIO to drag DataFusion *up* to ukiel's substrate) — it costs nothing and removes the variable instead of adding it.
- `clickbench::raw` takes **`--dir`** (`clickbench.rs:125`), so bare DataFusion can be pointed at `$UKIEL_E2E_STORE_DIR/ht/<id>/` — that is rung B. There is also a `clickbench sql` escape hatch for `EXPLAIN`.

The one structural change this plan makes to shared code is that the suite harness stops being DataFusion-shaped: `run_suite` takes a factory producing a `SessionContext` (`suite.rs:116`), which cannot express "ClickHouse over HTTP". It grows a concrete `Session` enum (`DataFusion(SessionContext)` | `Http(ClickHouseEngine)`) with one `query(&str) -> Result<usize>` method — no trait, no `async_trait` dependency, no lifetime gymnastics. ClickHouse is driven over its **HTTP interface** with `reqwest`, already a `ukiel-e2e` dep; **no new crates**. ClickHouse runs behind a compose **profile** (`--profile bench`), so `make e2e`, `make test` and a plain `docker compose up -d` never start it.

The load-bearing correctness detail is rung B. The store directory contains **dead parts** — compaction outputs whose inputs are not yet reaped — so pointing DataFusion at the prefix would read superseded files and produce *wrong answers that still look like plausible timings*. The in-flight `--dir` doc comment already notices this and sidesteps it by saying "only on a freshly-loaded fixture". That is not enough: the compacted and SizeTargeted layouts are exactly the ones worth pricing. The fix is to lean on an invariant we already prove — run **GC to quiescence** (`Stack::run_gc_to_quiescence`, `lib.rs:438`), after which the store and the live-part set are in bijection, which is literally e2e scenario **S8** (`gc_leaves_bucket_and_live_parts_in_bijection`) — and then **assert the row count** against the catalog. Both defenses are cheap and they convert a silent-wrong-answer failure into a loud one.

**Tech Stack:** Rust edition 2024; existing pins (arrow/parquet 58.3, object_store 0.13, datafusion 54 — never bump independently). `reqwest` + `serde_json` (existing deps) for the ClickHouse HTTP interface. `clickhouse/clickhouse-server` (pin the current stable tag) as a compose service under the `bench` profile. Vendored query files keep their ClickBench/JSONBench attribution (both Apache-2.0).

**Prerequisites:** **Plan 32 executed** (2026-07-12) — provides the vendored suites (`bench/queries/clickbench/{queries,queries-upstream}.sql` + `ADAPTATIONS.md`), the `hits_cb` fixture and its `Dataset` descriptor (`hits.rs`, `HITS_CB`), the shared harness (`suite.rs`), the raw-DataFusion runner and the `compare` ratio table (`clickbench.rs`), the operator session, and the `bsky` fixture with its `did` column. Plan 30 provides the bench chassis and `Stack`. **The in-flight `UKIEL_E2E_STORE_DIR` + `--dir` work must be committed first** (it is rungs B and C; this plan hardens and completes it rather than re-deriving it). Ungated otherwise.

**Do not re-fetch the datasets.** `bench/datasets/hits` (14 GB, 100 files) and `bench/datasets/bluesky` (1.3 GB) are on disk and every command here reads them **in place**.

**Disk cost (state it):** ClickHouse's 105-column MergeTree is ~15 GB and the 25-column one ~8 GB; a local-filesystem ukiel store for rungs B/C is another ~10–14 GB per layout. Call it **~40 GB** on top of what exists. The box has 1.4 TB free. `docker compose down -v` plus removing the store dir reclaims all of it.

## Global Constraints

- Rust edition 2024, toolchain ≥ 1.96; workspace dep pins per the roadmap. **No new crates** — `reqwest` and `serde_json` are already `ukiel-e2e` deps.
- **Every existing test stays green after every task.** No product crate is touched; the `suite.rs` refactor is behavior-preserving and updates its three callers in the same commit.
- **ClickHouse never starts outside the bench.** It lives behind the compose `bench` profile; `make e2e`, `make test`, and plain `docker compose up -d` must not launch it. Verify with `docker compose ps` after `make e2e`.
- **The bench binary stays manual-only**, `--release`, never CI.
- **Vendored queries stay verbatim-first**: every ClickHouse-dialect deviation is a numbered entry in `bench/queries/clickbench/ADAPTATIONS.md`, with the pristine upstream file beside it so `diff` *is* the adaptation set.
- **One timing methodology for every rung**: wall-clock, client-side, whole result collected; 1 cold + N hot; headline = hot minimum. A per-engine methodology would make the ladder meaningless.
- **Every rung must agree on every answer.** Row counts are compared across rungs and a mismatch fails the run. This is the plan's correctness net — six rungs are only harder to fool ourselves with than two if each one is allowed to contradict the others.
- Commit messages: conventional style, no Claude/AI attribution.
- Every task ends with `cargo fmt && cargo clippy --all-targets -- -D warnings` clean.

---

### Task 1: Engine-agnostic suite harness (enables every rung to share one methodology)

**Files:**
- Modify: `crates/ukiel-e2e/src/bin/bench/suite.rs` (`timed` at `suite.rs:93`, `cold_ukiel_session` at `suite.rs:100`, `run_suite` at `suite.rs:116`)
- Modify: `crates/ukiel-e2e/src/bin/bench/clickbench.rs` (`run`, `raw` — both call `suite::run_suite`)
- Modify: `crates/ukiel-e2e/src/bin/bench/bluesky.rs` (`jsonbench` — calls `suite::run_suite`)
- Test: `crates/ukiel-e2e/src/bin/bench/suite.rs` (`#[cfg(test)] mod tests`)

**Interfaces:**
- Produces:
  ```rust
  /// A SQL engine one of the official suites can drive. Concrete, not a trait:
  /// there are exactly two shapes, and a trait would buy nothing but an
  /// `async_trait` dependency.
  pub enum Session {
      DataFusion(SessionContext),
      Http(ClickHouseEngine),
  }

  impl Session {
      /// Runs `sql` to completion and returns the row count. Collecting the full
      /// result — not merely planning it — is what makes the engines comparable:
      /// DataFusion is lazy, ClickHouse is not, and timing either at "first row"
      /// would flatter one of them.
      pub async fn query(&self, sql: &str) -> anyhow::Result<usize>;
  }

  pub async fn run_suite<F, Fut>(
      engine: &str, label: &str, iters: usize, table_rows: i64,
      queries: &[Query], new_session: F,
  ) -> anyhow::Result<SuiteReport>
  where F: Fn() -> Fut, Fut: Future<Output = anyhow::Result<Session>>;
  //                              ^ was Result<SessionContext>
  ```
- Consumes: `SuiteReport` / `QueryResult` (`suite.rs:56,71`) — **the JSON shape must not move**, or plan 32's recorded reports stop deserializing and `compare` breaks against them.
- `Session::query` returns only a row count, never the batches: no caller needs the data, and materializing a 100M-row result would be a memory bug in waiting. The count is retained *precisely* so cross-rung answer disagreement can be detected (the check at `clickbench.rs:168`).

- [ ] **Step 1: Write the failing test**

The harness's Docker-free logic is what is worth pinning — "a failing query is recorded and the suite continues", "a truncated suite file is rejected", "the hot minimum is the headline":

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn load_queries_rejects_a_truncated_suite() {
        // 42 queries in a file claiming to be ClickBench must be an error, not a
        // silent 42-query benchmark wearing ClickBench's name.
    }

    #[test]
    fn finish_writes_the_report_then_fails_when_any_query_errored() {
        // Partial coverage is loud: the JSON exists (so the 42 good results are
        // not lost) AND the call returns Err naming the failures.
        let err = finish(report_with_one_error(), "test-partial.json").unwrap_err();
        assert!(err.to_string().contains("not fully covered"));
    }
}
```

- [ ] **Step 2: Verify it fails**

Run: `cargo test -p ukiel-e2e --bin bench`
Expected: compile error — `Session` does not exist; `finish`'s failure path has no test today.

- [ ] **Step 3: Implement**

Add the `Session` enum (its `Http` arm lands with Task 2's `ClickHouseEngine`; here it may be a stub the compiler accepts), change `run_suite`'s bound, and update the three call sites — grep confirms there are exactly three: `clickbench::run`, `clickbench::raw`, `bluesky::jsonbench`. `cold_ukiel_session` returns `Session::DataFusion(..)`.

- [ ] **Step 4: Verify pass**

Run: `cargo test -p ukiel-e2e --bin bench` — the new unit tests plus the 10 existing bench unit tests.
Then prove the refactor changed nothing observable: `bench clickbench run --iters 1 --label refactor-smoke` on a 2-file fixture still runs 43 queries with the same row counts.

- [ ] **Step 5: Lint and commit**

```bash
cargo fmt && cargo clippy -p ukiel-e2e --all-targets -- -D warnings
git add crates/ukiel-e2e
git commit -m "refactor: engine-agnostic suite harness - Session enum replaces the SessionContext-shaped runner"
```

---

### Task 2: ClickHouse in compose + the ClickBench fixtures (goal a)

**Files:**
- Modify: `docker-compose.yml` (a `clickhouse` service under `profiles: [bench]`)
- Modify: `Makefile` (`bench-clickhouse-up`, `bench-clickhouse-down`)
- Create: `crates/ukiel-e2e/src/bin/bench/clickhouse.rs`
- Modify: `crates/ukiel-e2e/src/bin/bench/main.rs` (`mod clickhouse;` + subcommands + USAGE)
- Create: `bench/queries/clickbench/create-clickhouse.sql` (upstream's, **verbatim**)

**Interfaces:**
- Produces:
  ```rust
  /// ClickHouse over its HTTP interface (8123). The native protocol's 9000 is
  /// deliberately NOT exposed: it collides with MinIO's default, which
  /// docker-compose.yml already warns about.
  pub struct ClickHouseEngine { client: reqwest::Client, url: String }

  impl ClickHouseEngine {
      pub fn new(url: &str) -> Self;                    // default http://127.0.0.1:8123
      /// Executes a statement and returns the row count (SELECTs run with
      /// `FORMAT JSONCompact` and report their `rows`; DDL/INSERT return 0).
      pub async fn query(&self, sql: &str) -> anyhow::Result<usize>;
      /// `SYSTEM DROP MARK CACHE` + `SYSTEM DROP UNCOMPRESSED CACHE` — as cold as
      /// ClickHouse can be made without sudo, and exactly as cold as ukiel's
      /// "fresh session, empty metadata cache". See the cold-run note in Task 3.
      pub async fn drop_caches(&self) -> anyhow::Result<()>;
  }

  /// Which ClickHouse fixture. `bench clickhouse load --table official|cb [--files N]`
  pub enum Table { Official, Cb }
  ```
- **Two tables, and each earns its keep:**
  - **`hits` (Official)** — upstream's **verbatim 105-column** `create.sql`: `ENGINE = MergeTree`, `PRIMARY KEY (CounterID, EventDate, UserID, EventTime, WatchID)`. This is the *calibration* table; it exists so our total can be held against ClickHouse's **published** ClickBench number. Loaded exactly as upstream's `clickhouse/load` does it (verified): `INSERT INTO hits SELECT * FROM file('hits_*.parquet')` with `max_insert_threads = nproc/4`. ClickHouse coerces the parquet's `UInt16` `EventDate` into `Date` on insert — that is upstream's own path, not an adaptation of ours.
  - **`hits_cb` (Cb)** — the **same 25 columns as ukiel's fixture**, `PARTITION BY EventDate ORDER BY (CounterID, EventTime)`, deliberately mirroring ukiel's day-partition and sort key. This is the apples-to-apples table (rung F). `hits` is *not* apples-to-apples: 105 columns versus 25 makes q24's `SELECT *` incomparable — plan 32 already learned this the hard way and solved it with a projecting view.
- **Semantics — why a compose profile:** an unprofiled `clickhouse` service would be started by `docker compose up -d --wait` inside `make e2e`, adding ~15 GB and a minute to every e2e run for nothing. `profiles: [bench]` makes it opt-in and invisible to every existing path.
- The fixture directory is mounted **read-only** at ClickHouse's `user_files` — never copied, never re-fetched.

- [ ] **Step 1: Write the failing test**

The load is Docker-shaped, so the *pure* part gets the test — the table descriptors:

```rust
#[test]
fn cb_table_mirrors_ukiels_physical_layout_and_carries_its_25_columns() {
    assert_eq!(Table::Official.name(), "hits");
    assert_eq!(Table::Cb.name(), "hits_cb");
    let ddl = Table::Cb.ddl();
    // Rung F is only apples-to-apples if the physical organization matches:
    // same sort key, same day partitioning, same columns as HITS_CB.
    assert!(ddl.contains("ORDER BY (CounterID, EventTime)"));
    assert!(ddl.contains("PARTITION BY EventDate"));
    for c in HITS_CB.cols {
        assert!(ddl.contains(c.ukiel), "{} missing from the hits_cb DDL", c.ukiel);
    }
}
```

- [ ] **Step 2: Verify it fails**

Run: `cargo test -p ukiel-e2e --bin bench`
Expected: compile error — `clickhouse::Table` does not exist.

- [ ] **Step 3: Implement**

```yaml
  # ClickHouse reference for the official benchmark suites (plan 34).
  # Opt-in only: `docker compose --profile bench up -d clickhouse`. Without the
  # profile it never starts, so `make e2e` and `make test` are untouched.
  clickhouse:
    image: clickhouse/clickhouse-server:25.8
    profiles: [bench]
    ports:
      - "${UKIEL_CLICKHOUSE_PORT:-8123}:8123"   # HTTP only. 9000 (native) collides with MinIO's default.
    volumes:
      # The 14 GB fixture, read-only and in place: never copied, never re-fetched.
      - ./bench/datasets/hits:/var/lib/clickhouse/user_files:ro
    ulimits:
      nofile: { soft: 262144, hard: 262144 }
    healthcheck:
      test: ["CMD-SHELL", "wget -qO- http://127.0.0.1:8123/ping || exit 1"]
      interval: 3s
      timeout: 5s
      retries: 40
```

`load()` runs the DDL, then the globbed `INSERT`, timing it (`PERF clickhouse/<table>/load_secs=`), then asserts `SELECT count()` equals **99,997,497** — the official ClickBench row count, which plan 32's ukiel fixture also hit exactly. **A load producing any other number is a broken fixture and must fail the run**, not proceed to benchmark it.

- [ ] **Step 4: Verify pass**

```bash
cargo test -p ukiel-e2e --bin bench
docker compose --profile bench up -d clickhouse
cargo run --release -p ukiel-e2e --bin bench -- clickhouse load --table cb --files 2
make e2e && docker compose ps          # ClickHouse must NOT be among the containers
```

- [ ] **Step 5: Lint and commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add docker-compose.yml Makefile crates/ukiel-e2e bench/queries
git commit -m "bench: ClickHouse compose profile and ClickBench fixture loader over the HTTP interface"
```

---

### Task 3: ClickHouse runs the 43 — calibration, apples-to-apples, and over raw parquet (goals a + b, rungs E and F)

**Files:**
- Modify: `crates/ukiel-e2e/src/bin/bench/clickhouse.rs` (`run`)
- Create: `bench/queries/clickbench/queries-clickhouse.sql` (the 25-column dialect)
- Modify: `bench/queries/clickbench/ADAPTATIONS.md` (a "ClickHouse dialect" section)
- Modify: `crates/ukiel-e2e/src/bin/bench/main.rs`

**Interfaces:**
- Produces: `bench clickhouse run --table official|cb|parquet [--iters N] [--label L]` → `bench/results/clickbench-clickhouse-{official,cb,parquet}-<label>.json`. Same `SuiteReport` shape as every other rung.
  - **`official`** — upstream's **verbatim** `clickhouse/queries.sql` against the 105-column MergeTree. Zero adaptations; that is the point. **Its total is compared to ClickHouse's published ClickBench result, and that comparison is this task's real assertion**: if this machine is wildly off the published figure, something is misconfigured and every other number in the plan is suspect.
  - **`cb`** — rung **F**: the 25-column MergeTree (native format, primary index, local disk).
  - **`parquet`** — rung **E**: ClickHouse reading **the same raw parquet files as rung A**, via a view over the `file()` table function — no MergeTree, no native format:
    ```sql
    CREATE OR REPLACE VIEW hits_cb AS
    SELECT <the same 25 columns every other rung projects>   -- so q24's SELECT * means the same thing
    FROM file('hits_*.parquet', Parquet);
    ```
    **This is the most interesting rung in the plan**: it is the only measurement where ClickHouse and DataFusion differ in *nothing but the engine* — same files, same bytes, same substrate, same 25 columns, same queries. It prices engine quality with no format advantage to either side. Rung F then shows what ClickHouse's own format is worth on top.
- **The ClickHouse dialect file.** `queries-clickhouse.sql` is plan 32's adapted `queries.sql` (already `hits_cb`-named, already using day-number `EventDate` literals) translated to ClickHouse. Expected deviations, **each a numbered ADAPTATIONS entry**: `to_timestamp_seconds(x)` → `toDateTime(x)` (q19, q43); `extract(minute FROM …)` → `toMinute(…)` (q19); `DATE_TRUNC('minute', …)` → `toStartOfMinute(…)` (q43); `REGEXP_REPLACE` → `replaceRegexpOne` (q29). **Check `length()`**: ClickHouse's `length` on a String is *bytes*, DataFusion's is *characters* (q28, q29 both average it over URLs). If they differ on this data, use `lengthUTF8` and log it — **and if that changes the answer, say so rather than quietly picking whichever matches.**
- **Cold, honestly.** `drop_caches()` issues `SYSTEM DROP MARK CACHE` + `SYSTEM DROP UNCOMPRESSED CACHE` before each cold run. This is **not** ClickBench's cold (upstream restarts the daemon and drops the OS page cache under sudo; we can do neither from an unprivileged container — and we do neither for ukiel). It is exactly as cold as ukiel's "fresh session, empty metadata cache", which is the entire point: **the ladder's rungs must be cold in the same sense or the cold column is noise.** Extend the README caveat that already says this for ukiel to cover every engine.
- **The HTTP tax, measured rather than waved away.** Client-side wall-clock includes ~0.5–1 ms of HTTP round-trip: nothing on a 400 ms query, *everything* on q01 (ClickHouse answers `COUNT(*)` from metadata in ~1 ms). So also capture ClickHouse's server-side elapsed from the `X-ClickHouse-Summary` response header into a new `server_ms: Option<f64>` on `QueryResult`, and print both for sub-5 ms queries. **Give the field `#[serde(default)]`** so plan 32's already-recorded reports still deserialize — re-verify `compare` reads them after the change.

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn clickhouse_dialect_is_43_queries_with_no_datafusion_only_functions() {
    let qs = suite::load_queries(Path::new("bench/queries/clickbench/queries-clickhouse.sql"), 43).unwrap();
    for (name, sql) in &qs {
        for banned in ["to_timestamp_seconds", "DATE_TRUNC", "REGEXP_REPLACE"] {
            assert!(!sql.contains(banned), "{name} still uses the DataFusion function {banned}");
        }
        assert!(sql.contains("hits_cb"), "{name} must target the 25-column table");
    }
}
```

- [ ] **Step 2: Verify it fails**

Run: `cargo test -p ukiel-e2e --bin bench`
Expected: `load_queries` errors — the file does not exist.

- [ ] **Step 3: Implement** — vendor, translate, log every deviation; wire the three `--table` modes.

- [ ] **Step 4: Verify pass**

Full-fixture `clickhouse load --table official` then `clickhouse run --table official --label baseline`.
Expected: all 43 execute; **write down our total next to ClickHouse's published number** and sanity-check the ratio. Then `--table cb` and `--table parquet` — all three must return **the same row counts as the ukiel and DataFusion rungs on every query**.

- [ ] **Step 5: Lint and commit**

```bash
git add crates/ukiel-e2e bench/queries
git commit -m "bench: ClickHouse runs the official 43 - verbatim calibration, native MergeTree, and over the same parquet"
```

---

### Task 4: Harden the ukiel-side rungs (goal b, rungs B and C)

**Files:**
- Modify: `crates/ukiel-e2e/src/bin/bench/clickbench.rs` (`raw`, `raw_session` — the in-flight `--dir` work at `clickbench.rs:90,125`)
- Modify: `crates/ukiel-e2e/src/lib.rs` if the in-flight `UKIEL_E2E_STORE_DIR` branch needs a hook (`lib.rs:75`)

**Interfaces:**
- Consumes: `Stack::run_gc_to_quiescence` (`lib.rs:438`), `Stack::object_paths` (`lib.rs:459`), `PostgresCatalog::live_parts`.
- **The problem this task exists to solve.** Rung B (bare DataFusion over ukiel's parts) reads a *directory*, but the directory is not the live set: compaction leaves superseded parts behind until GC reaps them. The in-flight `--dir` doc comment sidesteps this by restricting the rung to "a freshly-loaded fixture" — but the **compacted and SizeTargeted layouts are precisely the ones worth pricing**, and plan 32 showed layout dominates everything. So the restriction has to go, and the hazard has to be closed properly:
  1. **GC to quiescence first.** Afterwards the store and the live-part set are in bijection — exactly what e2e scenario **S8** (`gc_leaves_bucket_and_live_parts_in_bijection`) proves. The rung *consumes* an invariant the suite already guarantees instead of inventing a new one.
  2. **Assert the row count.** `SELECT count(*)` over the registered directory must equal `sum(live_parts.row_count)` from the catalog. Mismatch ⇒ `bail!`. **Never benchmark bytes you cannot account for** — the failure mode here is not a crash, it is a plausible-looking wrong number, which is the worst kind.
- Produces: `bench clickbench raw --dir <path> --label <l>` gains a `--gc-first` (default **on** when `--dir` points inside a ukiel store) and the row-count assertion. Add a `bench parts-gc` (or reuse an existing entry point) so the operator can run GC explicitly and see it happen.
- **Semantics:** rung B reads whatever layout the fixture is *currently* in (loaded / packed / SizeTargeted), which is a feature: rung B and rung C are then always measured over the identical bytes, so their gap is purely the stack. **The label must record the layout** (`--label sized-parts`), or the pair is uninterpretable — enforce it by putting the layout in the printed banner and the JSON.

- [ ] **Step 1: Write the failing test**

```rust
// Component test (testcontainers + a temp store dir), or an assertion inside the
// bench path if a component test is too heavy — but the property must be pinned:
#[tokio::test]
async fn raw_over_ukiel_parts_refuses_a_store_with_dead_parts() {
    // Load, compact (leaving superseded parts on disk, pre-GC), then point the
    // raw runner at the store: it must FAIL the row-count assertion, not
    // silently double-count. After run_gc_to_quiescence(), it must pass and
    // match sum(live_parts.row_count).
}
```

- [ ] **Step 2: Verify it fails** — today the rung happily reads dead parts and reports an inflated count as if it were data.

- [ ] **Step 3: Implement** — GC-to-quiescence + the assertion + the layout in the label/banner.

- [ ] **Step 4: Verify pass** — the test above, then rung B over each of the three layouts, each agreeing with its ukiel counterpart on every query's row count.

- [ ] **Step 5: Lint and commit**

```bash
git commit -m "bench: rung B reads only live parts - GC to quiescence plus a row-count assertion"
```

---

### Task 5: The ladder report (goal b — the deliverable)

**Files:**
- Modify: `crates/ukiel-e2e/src/bin/bench/clickbench.rs` (`compare` at `clickbench.rs:159`; add `matrix`)
- Modify: `crates/ukiel-e2e/src/bin/bench/main.rs`

**Interfaces:**
- Produces: `bench clickbench matrix --labels <l1,l2,…>` — reads every `clickbench-*-<label>.json` present and prints (a) a per-query table across all rungs, and (b) **the decomposition**:

  ```
  rung                                   total    vs above   attributed to
  A  DataFusion / raw parquet / local    24.7s        —      (baseline)
  B  DataFusion / ukiel parts / local     ??.?s    +X.X%     ukiel file format & layout
  C  ukiel / local store                  ??.?s    +X.X%     ukiel catalog + provider + cache
  D  ukiel / MinIO                        52.9s    +X.X%     object-store substrate
  ---
  E  ClickHouse / raw parquet / local     ??.?s              engine quality, identical bytes to A
  F  ClickHouse / MergeTree / local       ??.?s              native format + primary index
  ```
- **Semantics — refuse to lie:** `matrix` **must** verify that every rung it prints agrees on `table_rows` *and* on every per-query row count, and `bail!` otherwise (extend the check at `clickbench.rs:168`). A ladder whose rungs answer different questions is worse than no ladder, and the failure mode — a stale report from a differently-loaded fixture — is trivially easy to hit by accident.
- A rung whose report is absent prints as `—`, never silently omitted: **a missing rung must look missing.**

- [ ] **Step 1: Write the failing test** — `matrix` over two synthetic reports with mismatched `table_rows` returns an error naming both; over reports whose q13 row counts differ, likewise.
- [ ] **Step 2: Verify it fails.**
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Verify pass** — run across the real reports; every rung agrees on every answer.
- [ ] **Step 5: Lint and commit**

```bash
git commit -m "bench: clickbench matrix - the substrate ladder with per-rung attributed deltas"
```

---

### Task 6: JSONBench on ClickHouse (goal c)

**Files:**
- Modify: `crates/ukiel-e2e/src/bin/bench/clickhouse.rs` (`jsonbench_load`, `jsonbench_run`)
- Modify: `bench/queries/jsonbench/ADAPTATIONS.md`
- (`bench/queries/jsonbench/queries-upstream.sql` is already vendered verbatim by plan 32 — **it is the ClickHouse dialect**, so it runs as-is.)

**Interfaces:**
- Produces: `bench clickhouse jsonbench load --files N` and `bench clickhouse jsonbench run [--iters N] [--label L]`. Table per upstream: `CREATE TABLE bluesky (data JSON) ENGINE = MergeTree ORDER BY (…)`, loaded from the same `bench/datasets/bluesky/*.json.gz`, **in place**.
- **What this comparison is, stated before the numbers are taken:** ClickHouse stores the raw JSON and extracts at query time; ukiel flattened at ingest and reads typed columns. **Ukiel will win, and the win means very little on its own** — it is a write-time-versus-read-time trade, not a speed result. What makes it worth measuring is the other side of the ledger, which the report is **required** to print next to the ratio: ukiel's flattening is **lossy and schema-committed** (a field we did not map is simply gone; ClickHouse can answer a question about a field nobody anticipated), and adding a *single* column cost 5.8% of write throughput (plan 32). Print the ratio, then print that. `bench/queries/jsonbench/ADAPTATIONS.md` already argues this — extend it with the measured numbers.

- [ ] **Step 1: Write the failing test** — the vendored upstream file parses to exactly 5 queries and needs no dialect translation.
- [ ] **Step 2: Verify it fails.**
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Verify pass** — all 5 run; **q1's per-collection counts must agree with ukiel's q1** (same data, same question). A disagreement means one of the two mappings is wrong: that is a finding, not a rounding error, and it blocks the task.
- [ ] **Step 5: Lint and commit**

```bash
git commit -m "bench: JSONBench's 5 queries on ClickHouse over the raw Bluesky JSON"
```

---

### Task 7: Full-tier runs, docs & roadmap flip

**Files:**
- Modify: `bench/README.md` (§3 commands, §4 runbook, §5 caveats, §6 results — a new plan-34 block, **append-only**; plan 32's numbers are history and do not move)
- Modify: `docs/notes/2026-07-08-macro-perf.md` (the reference-engine and ladder rationale)
- Modify: `README.md` (root — the headline becomes *decomposed*, and gains the ClickHouse anchor)
- Modify: `docs/issues/0010-aggregates-rescan-what-the-catalog-already-knows.md` if the ladder changes what we believe about it (ClickHouse answers q07 from its primary index; the ladder will show whether that is the same win we left on the table)
- Modify: `docs/superpowers/plans/2026-07-05-ukiel-v1-roadmap.md` (row 34 → **Executed**)

No `ukield` wiring and no monitoring-spec rows: **this plan ships no product code and no new metrics.**

- [ ] **Step 1: Full verification** — `cargo fmt --check && cargo clippy --all-targets -- -D warnings && make test`. Run `make e2e` too: not because product code changed (none did) but because **Task 4's rung B is only correct while S8 is green**, and a green S8 is what licenses it.

- [ ] **Step 2: Record the numbers.** All six rungs at the 100M tier, plus the JSONBench pair. State machine spec, SHA, build profile, and **which layout each ukiel/parts rung was measured in** (loaded / packed / SizeTargeted). Plan 32's §6 block is the format to match.

- [ ] **Step 3: Interpret honestly.** The results section must answer, in plain sentences a reader can act on:
  - *How much of ukiel's 2.1× is the object store, how much is our file format, and how much is our stack?*
  - *How far is ukiel from ClickHouse on identical bytes (A vs E), and from ClickHouse on its own turf (F)?*
  - *Is this machine normal?* (our official ClickHouse total vs the published one)

  If the decomposition shows the stack is a small part of the gap, say so. **If it shows the stack is most of it, say that, and file the issue** — plan 32's pushdown-filters correction, which cost ukiel its only apparent win, is the precedent and the standard.

- [ ] **Step 4: Roadmap row 34 → Executed; commit**

```bash
git add bench/ docs/ README.md crates/
git commit -m "bench: ClickHouse reference and the substrate ladder; docs mark plan 34 executed"
```

---

## Self-review notes

- **Coverage:** goal (a) machine-calibrated ClickHouse reference → Tasks 2–3 (the verbatim 105-column table is what makes "calibration" mean anything; the 25-column table is what makes it *comparable* — both are needed, and for different reasons). Goal (b) the decomposition → rungs A and D exist from plan 32; rungs B and C exist in the in-flight `--dir` / `UKIEL_E2E_STORE_DIR` work and are **hardened** by Task 4; rungs E and F land in Task 3; the report is Task 5. Goal (c) → Task 6. Task 1 is the enabler (the harness cannot express a non-DataFusion engine today); Task 7 closes.
- **Type consistency:** `run_suite`'s bound moves from `Fut::Output = Result<SessionContext>` to `Result<Session>`; grep confirms exactly three call sites (`clickbench::run`, `clickbench::raw`, `bluesky::jsonbench`), all updated in Task 1. `QueryResult` gains `server_ms: Option<f64>` with `#[serde(default)]` in Task 3, so plan 32's recorded JSON still deserializes and `compare` keeps working — re-verify, do not assume.
- **Semantics reviewed:** ClickHouse's "cold" is a mark/uncompressed-cache drop, **not** upstream's daemon restart + page-cache drop — deliberately, so that it is cold in the same sense ukiel's cold is; the README says this rather than implying we reproduced ClickBench's protocol. `file()` reads the fixture from the read-only `user_files` mount, so ClickHouse and DataFusion read byte-identical files from the same local disk. Rung B reads *only live parts*, enforced by GC-to-quiescence **and** a row-count assertion, because reading superseded parts yields plausible-looking wrong numbers rather than an error.
- **Sequencing:** the in-flight `UKIEL_E2E_STORE_DIR` + `--dir` work must land first (it *is* rungs B and C). Then 1 gates everything (harness); 2 → 3 (load before run); 4 is independent of 2–3 and can proceed in parallel; 5 needs 3 and 4 to have produced reports; 6 is independent and is the one task that could be dropped without harming the ladder if the row needs to shrink; 7 closes.
- **Safety argument:** no product code changes, so exactly-once, isolation, and conflict-replan are untouched by construction. The one invariant this plan *consumes* is the GC bijection (S8): rung B is correct only because the store equals the live-part set after GC quiescence. If S8 ever goes red, rung B is invalid — hence `make e2e` in Task 7.
- **The honesty constraints are requirements, not aspirations:** one timing methodology across all rungs; all rungs must agree on all answers or the run fails; a missing rung prints as missing; the ClickHouse comparison is *expected* to be unflattering and the results section is *required* to say so plainly. Plan 32 shipped a "10× faster than DataFusion" result that turned out to be a misconfigured opponent. Six rungs are only harder to fool ourselves with than two if each rung is allowed to embarrass us — that is the whole design.

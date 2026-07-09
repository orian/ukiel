# Ukiel Plan 32: Official Benchmark Suites (ClickBench 43, JSONBench 5) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Run the *official* benchmark query suites against Ukiel — not just plan 30's per-tenant adaptations: (a) **ClickBench's 43 queries** in their DataFusion variant (verified: 43 queries, 24 distinct columns, quoted-CamelCase identifiers, DataFusion-compatible functions — the same file behind DataFusion's published ClickBench numbers, which makes our results directly comparable to the engine's own ceiling); (b) **JSONBench's 5 Bluesky queries** (verified: they touch `data.commit.collection`, `data.kind`, `data.commit.operation`, `data.did`, `data.time_us` — all present in our flattened `bsky` schema); (c) a **raw-DataFusion reference runner** over the same parquet on the same machine, so every query gets an `ukiel / raw-DataFusion` ratio — the measured cost of Ukiel's stack (catalog planning, provider, cache) versus the bare engine.

**Architecture:** Both official suites are **whole-table**, and Ukiel's product path is namespace-scoped by construction — so the one piece of product code this plan adds is an **operator (unscoped) session** in `ukiel-query`: `operator_session(catalog, store, store_url)` registers every hypertable by name with a provider in unscoped mode (catalog pruning and caches fully active; **no packing-key isolation predicate**). It is a library function with a loud issue-0001-adjacent warning, used by the bench binary only — never constructed by the HTTP server. Everything else lives in the bench: a second hits fixture (`hits_cb`) loaded with **verbatim ClickBench column names** so the official file runs with only documented type-level deltas (renaming our existing lowercase `hits` fixture would break plan 30's append-only scenario history — a new dataset is the append-only-compatible move), vendored query files with a per-query adaptation log, ClickBench's own run methodology (cold + 3 hot runs) alongside house warm-medians, and side-by-side reports.

**Tech Stack:** existing pins. `ukiel-e2e` gains the workspace `datafusion` dep (already in the tree via `ukiel-query`) for the raw-reference runner. Vendored query files carry ClickBench/JSONBench attribution headers (both repos are Apache-2.0).

**Prerequisites:** Plan 30 executed (the bench chassis, fetch targets, `Stack`). Ungated otherwise — runs against the uncompacted fixture exactly like plan 30 did; **after 29 lands, re-run over the compacted fixture** (the fan-out story), which is why this slots right after 29 in the roadmap order: the official suites should exist before the read-side tail (15/16) churns what they measure.

**Fixture cost (state it):** `hits_cb` is ~24 columns vs `hits`' 14 → roughly +10–13 GB in MinIO alongside the existing fixture (both fit the bench box comfortably; `make e2e-down` wipes all). The `bsky` schema gains a `did` utf8 column (JSONBench q2/q4/q5 group by `did`; we only stored the fnv1a hash) — a fixture-schema addition, so pre-existing `bsky` runs don't serve the official suite until re-produced; recorded runs keep their labels.

## Global Constraints

- Rust edition 2024, toolchain ≥ 1.96; workspace dep pins per the roadmap.
- **Every existing test stays green after every task**; the bench binary keeps building; plan 30's recorded suites/baselines are untouched (append-only — new dataset + new subcommands, no renames).
- **The unscoped session must be unreachable from the HTTP server**: it lives outside `ukiel_query::server`, nothing in `server.rs`/router construction references it, and its doc comment says why (issue 0001 — isolation is plan-time; this bypasses it by design, for operator/bench use).
- **Vendored queries are verbatim-first**: every deviation from the upstream file is a numbered entry in `bench/queries/*/ADAPTATIONS.md` with the reason (type-system delta, hash-for-did). No silent edits.
- Commit messages: conventional style, no Claude/AI attribution.
- Every task ends with `cargo fmt && cargo clippy --all-targets -- -D warnings` clean.

---

### Task 1: Operator (unscoped) session in ukiel-query

**Files:**
- Modify: `crates/ukiel-query/src/provider.rs` (unscoped mode), `src/context.rs` (`operator_session`)
- Test: `crates/ukiel-query/tests/query_test.rs`

**Interfaces:**
- The provider's tenant slice becomes explicit: `Option<i64>` — `Some(key)` is today's behavior exactly (catalog `live_parts(ht, Some(key))` + the plan-time isolation predicate); `None` scans all live parts with **no isolation predicate** (catalog pruning, stats pruning, footer cache, output ordering all unchanged — this benchmarks the real read stack minus scoping). Adapt to the provider's actual construction (`context.rs:112` is where the v1 slice convention is applied today).
- `pub async fn operator_session(catalog, store, store_url) -> Result<SessionContext>` — registers every hypertable (`list_hypertables`) by its hypertable name via `ctx.register_table`, providers unscoped. Doc comment: *operator/bench only; no tenant isolation; never wire into the HTTP server (issue 0001)*.

- [ ] **Step 1: Write the failing test** — two namespaces' rows in one packed hypertable: the namespace session sees only its own (existing behavior, re-asserted next to the new path), `operator_session` sees both and the total count; the server router still has no unscoped route (assert by construction: `operator_session` is not referenced from `server.rs` — a comment in the test states the review invariant).
- [ ] **Step 2: Verify it fails; implement; verify** — `cargo test -p ukiel-query -- --test-threads=2`.
- [ ] **Step 3: Lint and commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add crates/ukiel-query
git commit -m "feat: operator (unscoped) session for benchmarks - full-table scans, isolation predicate off"
```

---

### Task 2: `hits_cb` fixture — verbatim ClickBench columns

**Files:**
- Modify: `crates/ukiel-e2e/src/bin/bench/hits.rs` (generalize the cast machinery), `main.rs` (subcommand)

**Interfaces:**
- `bench clickbench load [--files N]` → hypertable `hits_cb`: the 24 columns the 43 queries reference, **names verbatim** (`"CounterID"`, `"EventTime"`, …) so quoted identifiers resolve; types mapped to ukiel's system — ints → `int64` (including `EventTime`, kept as **unix seconds**, since the queries call `to_timestamp_seconds("EventTime")`), strings → `utf8`, `EventDate` → `int64` (raw Date32 day number; queries comparing it to dates get a documented adaptation in Task 3). Packing key `CounterID`, sort key `["CounterID", "EventTime"]` (plan-27 validation: packing key leads — holds). Day partition derived from `EventDate` at load; per-file L1 commits; census not needed (global suite).
- Implementation is the existing loader generalized: the `(ukiel name, source column, cast)` table becomes a parameter, `cast_hits_batch`/upload/commit logic shared between `hits` and `hits_cb` — one loader, two dataset descriptors (the plan-30 pure-fn tests keep covering the shared path; add one descriptor test for verbatim names + seconds-not-ms).

- [ ] **Step 1: Failing unit test** (descriptor: verbatim names, EventTime seconds preserved, EventDate day number, day split from EventDate).
- [ ] **Step 2: Implement; verify** — unit tests; smoke `--files 2` against the compose stack (count via `operator_session` equals the two files' row count).
- [ ] **Step 3: Lint and commit**

```bash
git commit -m "feat: bench clickbench fixture - verbatim-named 24-column hits_cb hypertable"
```

---

### Task 3: The 43 ClickBench queries, vendored + runner

**Files:**
- Create: `bench/queries/clickbench/queries.sql` (vendored, attribution header) + `bench/queries/clickbench/ADAPTATIONS.md`
- Modify: `crates/ukiel-e2e/src/bin/bench/` (runner + report)

**Interfaces:**
- Vendor ClickBench's `datafusion/queries.sql` (43 queries; the file behind DataFusion's published numbers). Adapt **minimally and loggedly**: expected deltas are only around `EventDate` (int64 day number vs date literal — rewrite the comparison to the day number, one ADAPTATIONS.md entry per query touched) and any function drift against DataFusion 54 (the upstream file targets newer DataFusion; on a parse error, adapt with an entry). Everything else runs verbatim — that is the point of the verbatim column names.
- `bench clickbench run [--iters 3] [--label L]` — via `operator_session`, ClickBench's own methodology per query: one **cold** run (fresh session; the NVMe cache persists — note it, ClickBench's "cold" is also OS-cache-warm on repeat runs) + 3 **hot** runs; report `min`, `median`, and the raw triple. Output: `PERF clickbench/q{NN}={hot_min_ms}` lines + `bench/results/clickbench-{label}.json` (per query: cold, hot runs, row count returned). A query that errors is reported as failed in the JSON and fails the run at the end (after running the rest) — partial coverage must be loud, not silent.

- [ ] **Step 1: Vendor the file, write ADAPTATIONS.md entries as they surface.**
- [ ] **Step 2: Implement the runner; verify on the `--files 2` smoke fixture** — all 43 parse and execute (row counts sane), report written.
- [ ] **Step 3: Lint and commit**

```bash
git commit -m "feat: official ClickBench 43-query suite over hits_cb via the operator session"
```

---

### Task 4: Raw-DataFusion reference runner

**Files:**
- Modify: `crates/ukiel-e2e/Cargo.toml` (workspace `datafusion`), bench runner

**Interfaces:**
- `bench clickbench raw [--iters 3] [--label L]` — plain DataFusion 54 `SessionContext` with `register_parquet`/listing over `bench/datasets/hits/hits_*.parquet` **directly** (no Ukiel anywhere), same 43 queries (the raw files have the original columns, so the upstream file applies with only the same EventDate-class adaptations), same methodology. Report shape identical.
- `bench clickbench compare --ukiel <label> --raw <label>` (or fold into `run`'s report): per-query `ukiel_ms / raw_ms` ratio table — the stack-overhead number. This is the honest frame: raw DataFusion is the ceiling (same engine, zero catalog/provider/cache), and published ClickBench-DataFusion numbers cross-check the raw runner itself (same file, same engine family — sanity, not identity: hardware and version differ).

- [ ] **Step 1: Implement; verify on the smoke fixture** (ratios computed, report written).
- [ ] **Step 2: Lint and commit**

```bash
git commit -m "feat: raw-DataFusion reference runner and per-query overhead ratios"
```

---

### Task 5: JSONBench's 5 queries over `bsky`

**Files:**
- Create: `bench/queries/jsonbench/queries.sql` + `ADAPTATIONS.md`
- Modify: `crates/ukiel-e2e/src/bin/bench/bluesky.rs` (schema + runner)

**Interfaces:**
- The `bsky` schema gains `did: utf8` (the mapper emits it alongside the hash) — q2/q4/q5 group by `data.did`, and grouping by the fnv1a hash would be collision-equivalent but unreadable; storing the string makes the suite faithful. Fixture-schema addition: documented in `bench/README.md`; official-suite runs require a fixture produced at ≥ this version.
- The 5 queries adapted from JSON-path form to the flattened columns (each an ADAPTATIONS.md entry — this suite is *structurally* adapted by design, since Ukiel flattened at ingest what JSONBench queries as raw JSON; that is a legitimate system difference every JSONBench participant handles in its own dialect): q1 counts by `collection`; q2 count + `count(DISTINCT did)` where `kind='commit' AND operation='create'` by `collection`; q3 hourly (`ts` epoch-ms arithmetic) distribution for posts/reposts/likes; q4 earliest post `ts` per `did`, top 3; q5 `max(ts)-min(ts)` activity span per `did`, top 3.
- `bench bluesky jsonbench [--iters 3] [--label L]` — operator session, same methodology/report shape. Runbook: run after `bluesky run --files N` (the fixture is whatever was ingested — the suite reports the fixture row count so results are self-describing).

- [ ] **Step 1: Failing mapper test** (`did` present verbatim + hash unchanged); vendor + adapt queries.
- [ ] **Step 2: Implement runner; verify** — `--files 1` fixture, all 5 execute with sane shapes (q1 collection counts sum to commit-kind events, q4/q5 return 3 rows).
- [ ] **Step 3: Lint and commit**

```bash
git commit -m "feat: JSONBench 5-query suite over the flattened bsky schema"
```

---

### Task 6: Full-tier runs, runbook, docs & roadmap flip

**Files:**
- Modify: `bench/README.md` (§3 commands, §4 runbook, §6 results, adaptation-log pointers), `docs/notes/2026-07-08-macro-perf.md` (design note: the two-suite structure), `docs/superpowers/plans/2026-07-05-ukiel-v1-roadmap.md` (row 32 → Executed)

- [ ] **Step 1: Full verification** — `cargo fmt --check && cargo clippy --all-targets -- -D warnings && make test`; bench builds in release.
- [ ] **Step 2: Record the first official numbers** — `clickbench load` (all 100 files) + `clickbench run --label baseline` + `clickbench raw --label baseline` + the ratio table; `bluesky run --files 10` (new fixture with `did`) + `bluesky jsonbench --label baseline`. Machine spec + SHA + **layout state** (compacted or not — post-29 this matters; state which) in the results section, per the bench README's profile-stating rule.
- [ ] **Step 3: Docs + roadmap row 32 → Executed; commit**

```bash
git add bench/ docs/ crates/
git commit -m "bench: official ClickBench + JSONBench baselines with raw-DataFusion ratios; docs mark plan 32 executed"
```

---

## Self-review notes

- **Coverage:** official ClickBench suite → Tasks 2–3 (verbatim columns are what make "official" honest); raw-engine comparison → Task 4 (the ratio is the deliverable — Ukiel's stack cost per query shape); JSONBench → Task 5 (flattening is a declared, logged adaptation, as every JSONBench dialect does); the one product-code addition (unscoped session) → Task 1 with the security posture stated.
- **Verified inputs:** ClickBench DataFusion variant = 43 queries / 24 columns / quoted CamelCase / `to_timestamp_seconds`-style functions (fetched 2026-07-09); JSONBench = 5 queries over `collection`/`kind`/`operation`/`did`/`time_us` (fetched 2026-07-09) — all representable on the flattened schema once `did` is stored; the provider's slice convention lives at `context.rs:112`; plan-27 validation holds for `hits_cb` (`CounterID` leads its sort key).
- **Append-only discipline:** plan 30's `hits` fixture, per-tenant suites, and recorded baselines are untouched — new dataset, new subcommands. The `bsky` schema addition is a fixture-version note, not a mutation of recorded history.
- **Semantics reviewed:** the unscoped session changes no pruning/cache behavior (slice `Some` → identical to today; `None` → predicate off), and its unreachability from the server is a stated constraint, not an accident; a failing query fails the run *after* running the rest (loud partial coverage); "cold" runs are honestly caveated (NVMe cache persists across processes).
- **Sequencing:** 1 → {2 → 3 → 4} and 1 → 5 chains; 6 closes. Slots right after 29 in the roadmap order — and if 29 has landed first, Task 6 records the compacted-layout numbers and says so.

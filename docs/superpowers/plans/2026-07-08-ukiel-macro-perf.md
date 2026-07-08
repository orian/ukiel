# Ukiel Plan 30: Macro Perf Harness (10–100 GB Real Datasets) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Volume-scale benchmarks on real datasets, closing the gap between the plan-11 micro harness (~100k synthetic rows, read-path latency effects) and the README's stated intent ("run it in prod to test query performance"): (a) a **read benchmark** on ClickBench `hits` (~100M rows, 14.8 GB Parquet) — per-tenant analytics scenarios with recorded baselines *before* plans 27/14/29/13/15/16 churn the write and read paths (same baseline-before-churn rationale that put row 11 before 17); (b) a **write-path benchmark** on the Bluesky/JSONBench dataset (real ndjson events; 100M tier ≈ 12.5 GB gz, 1B tier = 125 GB gz) driven through Kafka → ingest → backpressure → ladder → finalization — the first time plans 17/18 run at the volume they were designed for; (c) machine-readable run reports plus a runbook/baselines note.

**Architecture:** A `bench` binary inside `ukiel-e2e` (`[[bin]]`), reusing the e2e `Stack` (compose-stack endpoints via `UKIEL_E2E_{KAFKA,PG,S3}` env, in-process ingest/compactor workers, HTTP query server) — the benchmark measures the same components the e2e suite proves correct. Datasets live in a gitignored `bench/` directory, fetched by Makefile targets. All parsing/mapping/casting logic is pure functions unit-tested without Docker; the binary itself is manual-only (never CI), run in `--release`. Reports are JSON files under `bench/results/` (gitignored); durable baselines get recorded in the runbook note, exactly as plan 11's medians live in the perf note.

**Tech Stack:** Rust edition 2024; existing workspace pins (arrow/parquet 58.3, rdkafka 0.39, reqwest in ukiel-e2e). One new direct dependency: `flate2` in `ukiel-e2e` (streaming `.json.gz` decode; already in the dependency tree transitively via parquet's gzip codec — no new tree weight).

**Prerequisites:** Rows 5/10 (Stack, compose), 11 (perf conventions), 17/18 (ladder, backpressure), 28 (merge cap — the 100 GB tier is its first real exercise): all **Executed**. Ungated otherwise. **Should execute before** 27/14/29 (write-path churn) and 13/15/16 (read-path churn) so baselines predate the optimizations they will measure.

**Resource requirements (document, don't hide):** hits tier — 15 GB download + ~15 GB rewritten in MinIO ⇒ ~50 GB free disk. Bluesky 100M tier — 12.5 GB gz download; Kafka holds ~45 GB uncompressed messages unless produced in drain-waves (`--wave-files`, Task 4) ⇒ ~80 GB free, or waves. 1B tier is a stretch goal: 125 GB gz, waves mandatory, hours of wall clock.

## Global Constraints

- Rust edition 2024, toolchain ≥ 1.96; workspace dep pins per the roadmap (arrow/parquet 58.3 and object_store 0.13 — never bump independently).
- **Every existing test stays green after every task.** The bench binary is additive; `make test` and `make e2e` are untouched by it.
- Pure logic (`cast_hits_batch`, `map_bluesky_event`, tenant selection) unit-tested via `cargo test -p ukiel-e2e --lib` without Docker; the binary itself is manual-only and never runs in CI.
- Benchmarks run in `--release`; the runbook says so on every command.
- Commit messages: conventional style, no Claude/AI attribution.
- Every task ends with `cargo fmt && cargo clippy --all-targets -- -D warnings` clean.

---

### Task 1: Bench binary scaffolding, dataset fetch targets

**Files:**
- Modify: `crates/ukiel-e2e/Cargo.toml` (`[[bin]] name = "bench"`, add `flate2`)
- Create: `crates/ukiel-e2e/src/bin/bench/main.rs` (+ `hits.rs`, `bluesky.rs`, `report.rs` modules as they land in later tasks)
- Modify: `Makefile`, `.gitignore`
- Create: `bench/README.md` (one paragraph pointing at the runbook note)

**Interfaces:**
- Produces: `bench <subcommand>` with manual `std::env::args` parsing (no clap — the workspace has no CLI dep and doesn't need one): `bench hits load|queries`, `bench bluesky produce|run` (implemented in Tasks 2–5; Task 1 lands the dispatch skeleton printing usage), common `--help`.
- Dataset layout: `bench/datasets/hits/hits_{0..99}.parquet`, `bench/datasets/bluesky/file_{0001..1000}.json.gz`, `bench/results/` — all gitignored.
- Makefile targets (curl loops, `-C -` so re-runs resume):

```makefile
# Macro perf datasets (see docs/notes/2026-07-08-macro-perf.md).
# ClickBench hits, partitioned: ~15 GB total, 100 files.
bench-fetch-hits:
	mkdir -p bench/datasets/hits
	for i in $$(seq 0 99); do \
	  curl -f -C - -o bench/datasets/hits/hits_$$i.parquet \
	    https://datasets.clickhouse.com/hits_compatible/athena_partitioned/hits_$$i.parquet; \
	done

# Bluesky (JSONBench): FILES=100 is the 100M tier (~12.5 GB), FILES=1000 the 1B tier.
bench-fetch-bluesky:
	mkdir -p bench/datasets/bluesky
	for i in $$(seq -f '%04g' 1 $(or $(FILES),100)); do \
	  curl -f -C - -o bench/datasets/bluesky/file_$$i.json.gz \
	    https://clickhouse-public-datasets.s3.amazonaws.com/bluesky/file_$$i.json.gz; \
	done
```

- [ ] **Step 1: Implement** the bin skeleton, Cargo/Makefile/.gitignore changes, `bench/README.md`.

- [ ] **Step 2: Verify**

Run: `cargo run -p ukiel-e2e --bin bench -- --help` (prints usage, exits 0); `cargo test -- --test-threads=2` untouched; `make bench-fetch-hits` interrupted after file 0 downloads and resumes (spot check, don't pull all 15 GB here).

- [ ] **Step 3: Lint and commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add crates/ukiel-e2e Makefile .gitignore bench/README.md
git commit -m "feat: bench binary scaffolding and dataset fetch targets (plan 30)"
```

---

### Task 2: ClickBench hits loader (read-benchmark fixture)

**Files:**
- Create: `crates/ukiel-e2e/src/bin/bench/hits.rs`
- Modify: `crates/ukiel-e2e/src/lib.rs` (only if a helper needs `pub`; prefer keeping bench logic in the bin)
- Test: pure-fn unit tests in `hits.rs` `#[cfg(test)]`

**Interfaces:**
- Produces: `bench hits load [--files N]` (default all present in `bench/datasets/hits/`).
- The Ukiel `hits` schema — a 14-column benchmark subset, lowercase names (unquoted SQL), every type one of the supported `int64 | timestamp_ms | utf8`:
  `counter_id` (packing key, from `CounterID`), `ts` (`timestamp_ms`, from `EventTime` unix-seconds × 1000), `watch_id`, `user_id`, `region_id`, `adv_engine_id`, `is_refresh`, `resolution_width`, `os`, `user_agent` (all → `int64`), `url`, `referer`, `title`, `search_phrase` (→ `utf8`). Sort key `["counter_id", "ts"]`, day partition derived from `EventDate`.
- Pure fn `cast_hits_batch(input: &RecordBatch) -> Result<Vec<(String, RecordBatch)>>` — casts the subset (arrow `cast` kernel; date/time arithmetic explicit) and splits by day, returning `(day, batch)` pairs. Unit-tested on a small in-code batch (types, day split, ms conversion).
- Load semantics: one input file at a time (~150 MB — memory-bounded by construction); each `(input file, day)` batch is sorted by `(counter_id, ts)` (`lexsort`, matching house write convention), written as ZSTD Parquet (row groups 64k rows — pre-plan-14 convention), uploaded to MinIO, and committed with `CommitOp::Add` at **level 1** with `ukiel_core::stats::int64_column_stats` populated — **one commit per input file** (each file = its own run, mirroring how runs are commit-derived). The **compactor is deliberately not run** over this fixture: files are row-range slices, so cross-file key ranges overlap and a merge would rewrite the whole 15 GB (and trip the plan-28 cap). Overlap is a pruning-effectiveness caveat, not a correctness issue — note it in the report output.
- Tenant census: the loader accumulates per-`counter_id` row counts and writes `bench/results/hits-tenants.json` — `{heavy, median, light, top20}` (heavy = max-rows tenant, light = smallest tenant above 1k rows) — and creates logical tables named `hits` for those tenants' namespaces (namespace id = counter_id, the v1 packing-key convention).

- [ ] **Step 1: Write the failing pure-fn tests** (`cast_hits_batch`: subset + types + day split; census pick logic).
- [ ] **Step 2: Verify they fail** — `cargo test -p ukiel-e2e --lib` (fns don't exist).
- [ ] **Step 3: Implement** the pure fns, then the load loop against `Stack::start()` (its `catalog`/`store` fields are pub).
- [ ] **Step 4: Verify** — unit tests pass; then a real smoke: `make e2e-up && cargo run --release -p ukiel-e2e --bin bench -- hits load --files 2`, followed by a manual count query through the stack for the reported heavy tenant (row count must equal the census's number for that tenant).
- [ ] **Step 5: Lint and commit**

```bash
git commit -m "feat: bench hits loader - ClickBench parquet to ukiel parts with tenant census"
```

---

### Task 3: hits query scenario suite

**Files:**
- Create: `crates/ukiel-e2e/src/bin/bench/report.rs`
- Modify: `crates/ukiel-e2e/src/bin/bench/hits.rs`

**Interfaces:**
- Produces: `bench hits queries [--iters 10]` — reads `hits-tenants.json`, runs the scenario list for each of `{heavy, median, light}` through `Stack::query` (the real HTTP endpoint, namespace-scoped — the product path), reports warm medians (plan-11 convention: one warm-up, then median of `--iters`).
- Scenario list (**append-only**, plan-11 rule — existing entries never change semantics), per tenant, ClickBench-derived:

```text
count            SELECT count(*) FROM hits
adv_filter       SELECT count(*) FROM hits WHERE adv_engine_id <> 0
distinct_users   SELECT count(DISTINCT user_id) FROM hits
top_urls         SELECT url, count(*) AS c FROM hits GROUP BY url ORDER BY c DESC LIMIT 10
search_phrases   SELECT search_phrase, count(*) AS c FROM hits WHERE search_phrase <> '' GROUP BY search_phrase ORDER BY c DESC LIMIT 10
ts_window        SELECT count(*) FROM hits WHERE ts BETWEEN {mid-week 24h window} (stats-pruning probe)
minutely         SELECT ts / 60000 AS m, count(*) FROM hits WHERE {same window} GROUP BY m ORDER BY m
```

- Output: `PERF hits/{tenant_class}/{scenario}={ms:.1}` lines (greppable, plan-11 style) plus `bench/results/hits-queries-<label>.json` (`--label` arg; scenario → median ms, tenant row counts, live-part fan-out per query from `live_parts(ht, Some(key))` — the file-set size a scan opens).

- [ ] **Step 1: Implement** (median loop lifts from `perf_smoke::median_ms`; keep the bin's copy trivial rather than exporting test code).
- [ ] **Step 2: Verify** — against the Task-4-smoke fixture (2 files loaded): all scenarios return, JSON written, per-tenant counts match the census. Sanity assertion (not perf): heavy tenant's `count` equals its census rows — namespace isolation at volume.
- [ ] **Step 3: Lint and commit**

```bash
git commit -m "feat: bench hits query suite - per-tenant ClickBench scenarios with medians"
```

---

### Task 4: Bluesky event mapper + Kafka producer

**Files:**
- Create: `crates/ukiel-e2e/src/bin/bench/bluesky.rs`
- Test: pure-fn unit tests in-module

**Interfaces:**
- The Ukiel `bsky` schema: `tenant_id` (`int64`, = fnv1a-64 hash of `did` masked to positive — the design-blessed embedder-side hash for string keys), `ts` (`timestamp_ms`, from `time_us / 1000`), `kind`, `operation`, `collection` (`utf8`), `payload` (`utf8`, the `commit.record` JSON re-serialized; empty for non-commit kinds). Sort key `["tenant_id", "ts"]`, day partitions.
- Pure fn `map_bluesky_event(line: &str) -> Option<serde_json::Value>` — flat JSON for the ingest route, or `None` for lines that should flow as poison (unparseable) — plus the inline `fn fnv1a64(s: &str) -> i64` (no new dep; mask sign bit). Unit tests: a `commit`/create post, an `identity` event (no commit block), a delete (no record), garbage line, did-hash stability and positivity.
- Produces: `bench bluesky produce --files N [--topic T] [--wave-files W]` — streams each `.json.gz` (flate2 `MultiGzDecoder`, line-buffered — memory O(line)), maps, produces to Kafka (batched sends, in-flight window like `produce_events`' pattern but pipelined). `--wave-files W`: after each W files, poll `Stack::ingest_offset_sum` until ingest catches up before producing more — bounds Kafka disk to ~W files uncompressed (mandatory for the 1B tier).
- Census on the fly: top-100 dids by event count + total per kind → `bench/results/bsky-tenants.json` (query tenants + expected counts for Task 5's assertions).

- [ ] **Step 1: Write the failing mapper tests.**
- [ ] **Step 2: Verify they fail** — `cargo test -p ukiel-e2e --lib`.
- [ ] **Step 3: Implement** mapper + producer loop.
- [ ] **Step 4: Verify** — unit tests pass; smoke against the compose stack with `--files 1 --topic bsky_smoke` (1M events): reported produced-count == mapped lines, census file written.
- [ ] **Step 5: Lint and commit**

```bash
git commit -m "feat: bench bluesky mapper and rate-aware Kafka producer"
```

---

### Task 5: Bluesky write-path run (ingest → ladder → finalization, measured)

**Files:**
- Modify: `crates/ukiel-e2e/src/bin/bench/bluesky.rs`, `report.rs`
- Modify: `crates/ukiel-e2e/Cargo.toml` (add `metrics-exporter-prometheus`, workspace-pinned — recorder for counter capture)

**Interfaces:**
- Produces: `bench bluesky run --files N [--wave-files W] [--flush-ms 10000]` — the full pipeline in one command:
  1. Creates the `bsky` hypertable + logical tables for the census tenants (`Stack.catalog` directly; `Stack::create_events_table` is events-schema-hardcoded, so bench builds its own `Table { .. }` — all fields are pub).
  2. Installs a `PrometheusBuilder` recorder (no HTTP listener; `render()` at the end captures `ingest_*`/`compactor_*` counters — notably backpressure deferrals and `compactor_capped_merges_total`).
  3. Spawns in-process ingest (`IngestConfig` with production defaults from `ukiel_ingest::config::defaults`, spec 10s flush) **and** the compactor loop (`Stack::spawn_compactor` shape, production `CompactorConfig` — 4/10 fanouts, not the 2/2 test-compat library defaults) — the product topology.
  4. Runs the Task-4 producer; samples every 10s: `ingest_offset_sum`, parts by level (`live_parts` grouped), queryable row count.
  5. After produce completes: wait `wait_for_row_count`-style until row count == mapped total (exactly-once at volume — an *assertion*, not just a metric), stop ingest (clean shutdown → final flush), then drive `run_compaction_to_quiescence` + `finalize_once` loops until both return 0, timing each phase.
  6. Report → `bench/results/bsky-run-<label>.json`: produce rate (rows/s), ingest throughput (offset-sum slope), time-to-quiescence (compaction, finalization), final parts by level with sizes, deferral/capped counters, rendered metrics dump; then runs a small per-tenant query suite (census tenants: `count`, `by_collection`, `ts_window`) with medians, same format as Task 3.
- Failure semantics: any invariant miss (row count short, finalization non-terminal with cap counter zero) exits non-zero with the report still written — a red bench run is a finding (S-suite philosophy).

- [ ] **Step 1: Implement.**
- [ ] **Step 2: Verify** — smoke: `--files 1` end-to-end on the compose stack; row count == mapped total; report JSON complete; finalization reaches one run for quiet partitions (or capped counter explains why not).
- [ ] **Step 3: Lint and commit**

```bash
git commit -m "feat: bench bluesky write-path run - ingest/ladder/finalization measured end to end"
```

---

### Task 6: Runbook note, first baselines, docs & roadmap flips

**Files:**
- Create: `docs/notes/2026-07-08-macro-perf.md`
- Modify: `docs/notes/2026-07-06-parquet-read-performance.md` (pointer: micro medians live here, macro baselines in the new note)
- Modify: `docs/superpowers/plans/2026-07-05-ukiel-v1-roadmap.md` (row 30 → Executed)
- Modify: `docs/superpowers/specs/2026-07-05-ukiel-design.md` ("What already works" → Operations bullet: macro perf harness + baseline pointer)

- [ ] **Step 1: Write the runbook note** — dataset provenance + sizes (hits: 14.8 GB / ~100M rows; Bluesky: 1M events/file, 100M tier ≈ 12.5 GB gz, 1B = 125 GB gz), disk/time requirements, the exact command sequences (`make e2e-up && make bench-fetch-hits && cargo run --release -p ukiel-e2e --bin bench -- hits load && … hits queries --label baseline`), the run-semantics caveat (hits files' key ranges overlap — pruning under-measures vs a compacted layout; revisit after row 29 makes compacting the fixture feasible), and empty baseline tables.
- [ ] **Step 2: Run the 10 GB tier and record baselines** — `hits` full load + query suite, `bluesky --files 10` (10M events; the 100M tier is a documented optional long run, the 1B tier a stretch goal with `--wave-files`). Paste the PERF medians and the write-path report numbers into the note's tables, with machine specs (the README's dev boxes), dataset label, and git SHA.
- [ ] **Step 3: Full verification** — `cargo fmt --check && cargo clippy --all-targets -- -D warnings && make test`; `make e2e` still green (bench is additive).
- [ ] **Step 4: Docs flips** — roadmap row 30 → **Executed** (list what landed + where baselines live); design-doc Operations bullet; perf-note pointer.
- [ ] **Step 5: Commit**

```bash
git add crates/ukiel-e2e bench/README.md docs/
git commit -m "feat: macro perf runbook and first 10GB-tier baselines; docs mark plan 30 executed"
```

---

## Self-review notes

- **Coverage:** Goal (a) read benchmark → Tasks 2–3; (b) write path → Tasks 4–5; (c) reports + runbook + baselines → report.rs (T3/T5) + Task 6. Deliberately absent: a compacted hits layout (blocked on row 29 — a 15 GB merge exceeds the plan-28 cap by design; caveat recorded in the report and note) and CI integration (manual-only is the point; CI perf gating would need dedicated hardware).
- **Type consistency:** every referenced `Stack` member is real and pub (`catalog`, `store`, `producer`, `query`, `ingest_offset_sum`, `spawn_compactor`, `run_compaction_to_quiescence`, `live_parts` via catalog); `Table` fields are pub (bench constructs its own for the bsky schema); ingest defaults come from `ukiel_ingest::config::defaults` (plan 28); `int64_column_stats(&RecordBatch) -> Option<Value>` matches the loader's use; supported schema types verified (`int64`/`timestamp_ms`/`float64`/`utf8`/`bool` — the subset uses the first three plus utf8).
- **Semantics reviewed:** hits parts registered one-commit-per-file at L1 with compactor off (overlapping key ranges — merging would rewrite 15 GB and trip the cap; run semantics are commit-derived so per-file commits are honest); bluesky run *asserts* exactly-once at volume rather than only measuring; red runs exit non-zero with the report written.
- **Sequencing:** Tasks 1→2→3 and 1→4→5 are chains; 2/3 and 4/5 are independent pairs. Plan slots after 21 and before 27/14/29/13/15/16 (baseline-before-churn); nothing here blocks plan 8.
- **Safety argument:** the bench only writes to per-run-named hypertables/topics against the disposable compose stack (`make e2e-down` wipes volumes); it never touches production paths, and `bench/` plus results are gitignored so 100 GB can never land in the repo.

# Ukiel Plan 40: Catalog Scalability Bench (surge ceilings, cores-limited, options verdicts) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.
>
> **Investigation-first, plan-37 shape:** Tasks 1–3 build permanent instruments, Task 4 runs the measurement matrix, Task 5 hands each pre-registered option a **verdict** (pursue as its own row / decline with the number that declined it). No product code ships from this plan — a confirmed fix files a roadmap row (the plan-37 sizing rule); the deliverable is the ceiling numbers and the verdict table.

**Goal:** Measure what the single-Postgres catalog sustains under query/ingest surges — the horizontal-scaling question in `docs/notes/2026-07-13-catalog-scalability.md` (the sketch; read it first). Concretely: achieved ops/s + latency percentiles + **saturation attribution** for the four workloads (read-surge / write-surge / conflict-storm / mixed) across PG cores × pool size × `parts` cardinality, and a verdict for each pre-registered scaling option (pool knob, metadata cache, live-parts snapshot cache, replica reads, hypertable sharding). Driver: roadmap row 40; prerequisite knowledge for rows 26 and 8.

**Architecture:** everything lands in the bench binary (`crates/ukiel-e2e/src/bin/bench/`, new `catalog.rs` module + `main.rs` wiring) driving `PostgresCatalog` **directly** — no Kafka, no object store, no DataFusion session. Seeding uses real `commit()`s with synthetic paths (`bench/ht/{i}/L1/{uuid}.parquet` — parts rows need no objects behind them), real per-part Int64 `column_stats`, and a keyspace shaped like the product (packed multi-key parts with overlapping ranges, a configurable dedicated-part fraction). The CPU limit is applied to the compose `postgres` service **live** (`docker update --cpus=<n> $(docker compose ps -q postgres)`), so one seeded catalog serves the whole cores sweep — seeding is the expensive step and happens once per cardinality point. Wait-event sampling runs on a **separate single connection** (never the bench pool — the instrument must not consume what it measures), polling `pg_stat_activity`/`pg_stat_database` at ~250 ms during every run and folding the samples into the run report.

Nothing here touches product code or existing tests; the only shared surface is the bench binary. One measurement-honesty rule carried from the house protocol: the bench process and Postgres share this machine, so every run records the *bench side's* CPU too — a run where the driver is the bottleneck is a driver bug, not a ceiling, and the report must be able to tell them apart (drivers are cheap async loops; if driver CPU exceeds ~2 cores at saturation, raise workers' efficiency before trusting numbers).

**Tech Stack:** existing deps only (`sqlx` via `ukiel-catalog`, `tokio`, `serde_json`; percentiles by sorting a `Vec<f64>` — no new histogram crate for a manual bench). Compose service verified: `postgres` (postgres:17, stock config — record `shared_buffers` etc. once in the report header via `SHOW ALL` so runs are self-describing).

**Prerequisites:** none unexecuted — `PostgresCatalog` API verified 2026-07-13 (`connect` with hardcoded `max_connections(16)` at `lib.rs:27–29`; `live_parts_pruned`, `commit_with_offsets`, `max_live_l0_parts`, `changes_since`, `finalize_candidates` per the sketch's cost table). Pool size is a bench axis, so Task 1 needs a way to open pools of configurable size: add `connect_with_pool_size(url, n)` to `ukiel-catalog` — the one tiny library addition, behavior-identical for all existing callers (`connect` delegates with 16).

## Global Constraints

- Rust edition 2024, toolchain ≥ 1.96; pinned deps; conventional commits, no Claude/AI attribution; fmt + `clippy -D warnings` per task.
- **Every existing test stays green after every task** (the `connect_with_pool_size` addition is delegation-only; everything else is bench code).
- **Measurement protocol:** ≥ 2 runs per matrix point, interleaved when comparing configs; report achieved ops/s, p50/p95/p99, error/conflict counts, wait-event profile, PG CPU%, driver CPU%; state cardinality, cores, pool, worker count with every number; results JSON to `bench/results/catalog-*.json` (append-only, labeled).
- Runs are manual-only (never CI); `docker update --cpus` is restored to unlimited after each session (record in the runbook).

---

### Task 1: Catalog workload drivers (`bench catalog seed|read|write|conflict|mixed`)

**Files:**
- Create: `crates/ukiel-e2e/src/bin/bench/catalog.rs`
- Modify: `crates/ukiel-e2e/src/bin/bench/main.rs` (subcommands + usage), `crates/ukiel-catalog/src/lib.rs` (`connect_with_pool_size`)
- Test: pure helpers in-module (`cargo test -p ukiel-e2e --bin bench` if the bin has unit tests today — else the crate's convention; plus one `#[ignore]`d smoke against the compose stack)

**Interfaces:**
- `bench catalog seed --tables T --parts-per-table P [--dedicated-frac F]` — creates T hypertables (`bench_cat_{i}`), commits P parts each in batches (realistic `PartMeta`: overlapping key ranges for packed parts, `key_min == key_max` for the dedicated fraction, Int64 `column_stats` with plausible ts bounds, day-partition values across ~30 days). Idempotent per label: refuses to re-seed an existing table count mismatch loudly.
- `bench catalog read --workers N --duration S [--pool P] [--key-dist uniform|zipf]` — N tasks looping `live_parts_pruned(ht, Some(key), &ranges)` with keys drawn from the distribution and a ts-range predicate on half the calls (the product mix). Zipf matters: pool and PG cache behavior differ between uniform and hot-key skew — implement zipf as a pure helper with a unit test (house rule: decision logic extracted pure).
- `bench catalog write --workers N --duration S [--pool P] [--parts-per-commit K]` — each worker owns a distinct `(topic, partition)` lane and loops `commit_with_offsets` (monotonic offsets — CAS always succeeds; that is the *conflict-free* ingest shape) plus one `max_live_l0_parts` probe per M commits (the flush-tick ratio).
- `bench catalog conflict --workers N --duration S --tables 1` — workers race REPLACE `commit`s over the same hypertable's parts (read live set → commit swap); reports useful-work fraction = successful swaps / attempts, and the conflict-retry latency distribution.
- `bench catalog mixed --read-workers R --write-workers W --duration S` — both at once plus a background tick loop (`finalize_candidates` + `changes_since`) at the compactor cadence.
- Every subcommand: warmup period excluded from stats; results JSON `{label, workload, axes…, achieved_ops, p50, p95, p99, errors, conflicts, driver_cpu_pct}`.

- [ ] **Step 1: failing unit tests** — zipf sampler distribution sanity; seed-plan generator (part ranges overlap/dedicated fraction as specified) as a pure fn.
- [ ] **Step 2–4: fail → implement → unit tests + one smoke** (`seed --tables 2 --parts-per-table 100` then `read --workers 4 --duration 5` against the compose stack; numbers print, JSON written).
- [ ] **Step 5: lint and commit**

```bash
git commit -m "bench: catalog workload drivers - seed, read/write/conflict/mixed surges"
```

---

### Task 2: Saturation attribution sampler

**Files:**
- Modify: `crates/ukiel-e2e/src/bin/bench/catalog.rs`

**Interfaces:**
- A sampler task on its own **single** connection (not the bench pool), started by every workload run: every ~250 ms record `pg_stat_activity` rows for the bench DB (state, `wait_event_type`/`wait_event`) and `pg_stat_database` deltas (xact_commit/rollback, blks_hit/read); on run end, fold into the report: `{active_avg, waiting_by_event: {…}, xact_per_s, cache_hit_ratio}` plus PG container CPU% (`docker stats --no-stream`) and driver process CPU%. The report's one-line verdict field is derived, not vibed: dominant wait class ∈ {pool-wait (bench-side queue time — measured as acquire latency on the sqlx pool), pg-cpu, lock, io}.
- Pool acquire time: wrap catalog calls with an acquire-latency measurement (sqlx exposes pool acquire via timing the call itself minus query time is unreliable — simplest honest instrument: `pool.acquire()` timing in a probe task at 4 Hz, reported as its own percentile series).

- [ ] **Steps: unit test the folding (pure over synthetic samples) → implement → smoke (sampler output present in the JSON) → lint and commit**

```bash
git commit -m "bench: catalog saturation sampler - wait events, xact rates, pool acquire latency"
```

---

### Task 3: Cores-limit + runbook wiring

**Files:**
- Modify: `crates/ukiel-e2e/src/bin/bench/catalog.rs` (record the current `--cpus` limit in every report by reading the container's `NanoCpus` via `docker inspect`), `bench/README.md` (§commands + a new §catalog runbook)

**Interfaces:**
- The bench does **not** change the limit itself (state-changing docker ops stay manual and visible); it *records* it. Runbook documents the sweep loop:

```bash
PG=$(docker compose ps -q postgres)
for n in 1 2 4 8; do
  docker update --cpus=$n $PG
  ./target/release/bench catalog read --workers 64 --duration 30 --label cores$n
done
docker update --cpus=0 $PG   # unlimited again — always restore
```

- [ ] **Steps: implement recording + runbook → smoke (report carries the limit) → lint and commit**

```bash
git commit -m "bench: catalog runs record the live PG cpu limit; cores-sweep runbook"
```

---

### Task 4: The measurement matrix

No product code. On the compose stack, release build, per the sketch's axes — recorded labels, ≥ 2 runs each:

- [ ] **Step 1 (cardinality × read ceiling):** seed 10⁴ / 10⁶ / 10⁷ parts-per-catalog points (one table count, e.g. T=16); `read` sweep workers 16/64/256 at pool 16 vs 64, cores 4. Deliverable: QPS ceiling vs cardinality, and whether the wall is pool-wait or PG CPU at each point. `EXPLAIN (ANALYZE, BUFFERS)` one representative `live_parts_pruned` at 10⁷ and record the plan (index-or-seqscan is a finding, not a guess).
- [ ] **Step 2 (cores sweep):** at the middle cardinality, `read` and `write` across cores 1/2/4/8, fixed workers/pool. Deliverable: ops/s-per-core curves — the "how many QPS at N cores" answer, both directions.
- [ ] **Step 3 (write + conflict):** `write` worker sweep (TPS ceiling, commit latency percentiles); `conflict` at 2/8/32 workers (useful-work fraction curve — the optimistic-concurrency tax under compactor/MV-shaped contention).
- [ ] **Step 4 (mixed surge):** product-ratio mix at the read ceiling's edge; which side degrades first and what the sampler blames.
- [ ] **Step 5:** write all of it into the sketch note (new "Measured" section: the ceilings table + saturation attributions + the EXPLAIN finding), commit:

```bash
git commit -m "bench: catalog surge matrix recorded - ceilings, per-core curves, conflict tax"
```

---

### Task 5: Options verdicts + close out

**Files:**
- Modify: `docs/notes/2026-07-13-catalog-scalability.md` (verdict per option), `bench/README.md` (results addendum), `docs/superpowers/plans/2026-07-05-ukiel-v1-roadmap.md` (row 40 → **Executed**; file follow-up rows the verdicts earn)

Each pre-registered option gets a verdict **derived from the matrix**, with the number that decided it: pool knob (expected: cheap win if pool-wait dominates anywhere — file the config row); hypertable/table metadata cache (session-build read share); **live-parts snapshot cache** (what fraction of read-surge load it would remove; the tombstone-grace staleness argument from the sketch rides along); replica reads (only if PG CPU is the read wall); hypertable sharding (only if single-primary commit TPS is a real wall at product-shaped load — say the measured TPS and the target that would exceed it). Declined options get declined *in writing with numbers* — the sketch's whole point.

- [ ] **Step 1:** verdict table into the sketch note; follow-up roadmap rows for pursued options (each with its measured motivation, plan-37-style).
- [ ] **Step 2:** full-workspace check — `cargo fmt --check && cargo clippy --all-targets -- -D warnings && make test`; restore the PG cpu limit; roadmap flip.
- [ ] **Step 3: commit**

```bash
git commit -m "docs: catalog scalability measured - ceilings recorded, options verdicted, row 40 executed"
```

---

## Self-review notes

**Coverage mapping.** The user question decomposes as: "how many QPS at N cores" → Task 4 Steps 1–2 (ceilings + per-core curves, both read and write); "can the catalog be parallelized" → replica/sharding verdicts (Task 5) fed by saturation attribution (Task 2); "what could be cached" → the two cache options with measured shares (Task 5, sketch §options); "where are the bottlenecks" → the sampler's dominant-wait classification per run + the 10⁷-row EXPLAIN. Sketch note carries the standing analysis; row 40 the charter.

**Type consistency.** Drivers consume the verified `PostgresCatalog` API exactly as product code does (`live_parts_pruned(HypertableId, Option<i64>, &[ColumnRange])`, `commit_with_offsets`, probes); `connect_with_pool_size(url, n)` is the only library addition and `connect` delegates to it with today's 16 — all existing callers unchanged. Seeded `PartMeta` matches the struct the writers build (path/partition_values/key range/row_count/size/level/column_stats).

**Sequencing.** 1 → 2 → 3 (instruments; each committable and independently useful) → 4 (matrix; needs all three) → 5 (verdicts need 4). Independent of every in-flight row: new bench module, one delegation-only library fn.

**Safety argument.** No product behavior changes. The bench writes only to its own `bench_cat_*` hypertables in the disposable compose Postgres; seeding commits synthetic paths that no reader ever fetches (nothing scans these tables through the query path); `docker update --cpus` affects only the local compose container and the runbook restores it. The one library addition cannot change behavior (pure delegation). Measurement honesty risks are named in-plan: driver-side saturation is detected (driver CPU recorded), the sampler runs outside the measured pool, and every claim carries its axes.

**Sizing.** 5 tasks, ~5 commits. The follow-up work (pool knob, snapshot cache, replicas) is deliberately out — rows file from verdicts.

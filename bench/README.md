# Macro perf harness (plan 30)

Volume-scale benchmarks on real datasets — ClickBench `hits` (read path) and
Bluesky ndjson (write path) — run manually against the docker-compose stack via
the `bench` binary in `ukiel-e2e`. This bridges the plan-11 micro harness
(~100k synthetic rows, `crates/ukiel-query/tests/perf_smoke.rs`) to the README's
PB+ intent.

The harness reuses the e2e `Stack` (real Kafka/Postgres/MinIO, in-process
ingest/compactor workers, HTTP query server), so it measures the same
components the e2e suite proves correct.

- **Manual-only** — never runs in CI (it moves tens of GB and runs for minutes
  to hours).
- **Build profile.** `--release` resolves to a tuned profile (fat LTO +
  `codegen-units = 1`, root `Cargo.toml`) — cross-crate inlining across the
  whole graph (datafusion/arrow/parquet included), at the cost of much slower
  compiles. For the absolute fastest numbers also pass architecture-specific
  SIMD: `RUSTFLAGS="-C target-cpu=native" cargo run --release …` (non-portable,
  so opt-in rather than baked in). **State the profile with any numbers you
  record.**
- Datasets and results are **gitignored** (`bench/datasets/`, `bench/results/`).
- This README is the single source of truth for the runbook and the recorded
  results. Design rationale and provenance: `docs/notes/2026-07-08-macro-perf.md`.

---

## 1. Prerequisites

- Docker (for the compose stack: Kafka + Postgres + MinIO).
- Rust toolchain ≥ 1.96.
- Free disk:
  - **hits**: ~15 GB download + ~15 GB rewritten into MinIO ⇒ **~50 GB**.
  - **Bluesky 100M**: 12.5 GB gz download; Kafka holds ~45 GB uncompressed
    unless produced in drain-waves ⇒ **~80 GB**, or use `--wave-files`.
  - **Bluesky 1B** (stretch): 125 GB gz, waves mandatory, hours of wall clock.

Bring the stack up / down:

```bash
make e2e-up     # docker compose up -d --wait
make e2e-down   # docker compose down -v   (wipes all volumes)
```

The bench connects to the stack via the same env vars the e2e suite uses
(`UKIEL_E2E_KAFKA`, `UKIEL_E2E_PG`, `UKIEL_E2E_S3`), with compose-matching
defaults — no config needed for the default local stack.

---

## 2. Datasets

| Dataset | Source | Size | Rows |
|---|---|---|---|
| ClickBench `hits` | `datasets.clickhouse.com/hits_compatible/athena_partitioned/hits_{0..99}.parquet` | ~14.8 GB (100 files, ~150 MB each) | ~100M |
| Bluesky (JSONBench) | `clickhouse-public-datasets.s3.amazonaws.com/bluesky/file_{0001..1000}.json.gz` | 100M tier ≈ 12.5 GB gz (100 files, 1M events each); 1B tier = 125 GB gz | 1M/file |

Fetch (both use `curl -C -`, so interrupted downloads resume):

```bash
make bench-fetch-hits                 # all 100 hits files (~15 GB)
make bench-fetch-bluesky              # FILES=100 default (100M tier, ~12.5 GB)
make bench-fetch-bluesky FILES=10     # smaller: 10 files (10M events)
make bench-fetch-bluesky FILES=1000   # 1B tier (125 GB)
```

Files land under `bench/datasets/hits/hits_*.parquet` and
`bench/datasets/bluesky/file_*.json.gz`. You don't have to fetch all of them —
every command takes a `--files N` cap and uses whatever is present.

---

## 3. Commands

All commands are subcommands of the `bench` binary. Build once implicitly with
`cargo run --release -p ukiel-e2e --bin bench -- <args>`.

```
bench hits load [--files N]
bench hits queries [--iters N] [--label LABEL]
bench bluesky produce --files N [--topic T]
bench bluesky run --files N [--wave-files W] [--flush-ms MS] [--label LABEL]
bench --help
```

### `hits load [--files N]`

Loads ClickBench parquet into the `hits` hypertable. For each input file:
casts the 14-column subset (`counter_id`, `ts`, and 12 more) to supported
ukiel types, splits by `day`, sorts each `(day)` batch by `(counter_id, ts)`,
writes ZSTD Parquet, uploads to MinIO, and commits **one commit per input file**
at level 1 with column stats populated. Accumulates a per-`counter_id` census
and writes `bench/results/hits-tenants.json` (heavy / median / light / top-20),
creating logical `hits` tables for those tenants' namespaces (namespace id =
`counter_id`). `--files` caps how many present files to load (default: all).

The **compactor is deliberately not run** over this fixture — see caveats.

### `hits queries [--iters N] [--label LABEL]`

Reads `hits-tenants.json` and runs the per-tenant ClickBench scenario suite for
`{heavy, median, light}` through the real HTTP query endpoint (namespace-scoped
— the product path), reporting warm medians (one warm-up, then median of
`--iters`, default 10). Emits greppable `PERF hits/<class>/<scenario>=<ms>`
lines and `bench/results/hits-queries-<label>.json` (default label `baseline`).
Asserts each tenant's `count(*)` equals its census rows (namespace isolation at
volume) — a mismatch fails the run.

Scenario list (append-only; existing entries never change semantics):

| name | SQL |
|---|---|
| `count` | `SELECT count(*) FROM hits` |
| `adv_filter` | `SELECT count(*) FROM hits WHERE adv_engine_id <> 0` |
| `distinct_users` | `SELECT count(DISTINCT user_id) FROM hits` |
| `top_urls` | `SELECT url, count(*) c FROM hits GROUP BY url ORDER BY c DESC LIMIT 10` |
| `search_phrases` | `... WHERE search_phrase <> '' GROUP BY search_phrase ...` |
| `ts_window` | `SELECT count(*) FROM hits WHERE ts BETWEEN <mid-window 24h>` (stats-pruning probe) |
| `minutely` | `SELECT ts/60000 m, count(*) FROM hits WHERE <same window> GROUP BY m ORDER BY m` |

### `bluesky produce --files N [--topic T]`

Standalone producer: streams each `.json.gz` (memory O(line)), maps each event
to the `bsky` schema (or passes unparseable lines through as poison), and
produces to Kafka `--topic` (default `bsky`) with a bounded in-flight window.
Writes the tenant census to `bench/results/bsky-tenants.json`. No in-process
ingest runs here, so `--wave-files` does not apply — use `run`.

### `bluesky run --files N [--wave-files W] [--flush-ms MS] [--label LABEL]`

The full write path in one command:

1. Creates the `bsky` hypertable.
2. Installs a Prometheus recorder (no HTTP listener) to capture
   `ingest_*`/`compactor_*` counters.
3. Spawns **in-process ingest** (production guardrail defaults; wide
   event-time bound for the historical dataset; `--flush-ms`, default 10000)
   **and a compactor loop** (production 4/10 fanouts).
4. Streams + produces the events (`--wave-files W` throttles the producer to
   ~W files of un-ingested Kafka backlog — bounds Kafka disk for big tiers).
5. Waits for ingest to consume everything, stops it (final flush), then drives
   compaction + finalization to quiescence, timing each phase.
6. Writes `bench/results/bsky-run-<label>.json` and runs a small per-tenant
   read suite (`count`, `by_collection`, `ts_window`).

**Asserts exactly-once at volume** (not just measures): stored rows == mapped
events, ingest offsets == produced lines, and each census tenant's queryable
count == its census. Any violation exits non-zero with the report still
written (a red bench run is a finding).

---

## 4. Runbook

```bash
make e2e-up                                              # compose stack

# Read path
make bench-fetch-hits                                    # ~15 GB
cargo run --release -p ukiel-e2e --bin bench -- hits load
cargo run --release -p ukiel-e2e --bin bench -- hits queries --label baseline
#   → PERF lines + bench/results/hits-queries-baseline.json

# Write path (10M-event tier)
make bench-fetch-bluesky FILES=10
cargo run --release -p ukiel-e2e --bin bench -- bluesky run --files 10 --label baseline
#   → PERF lines + bench/results/bsky-run-baseline.json

make e2e-down                                            # wipe volumes
```

Big tiers: `bluesky run --files 100 --wave-files 10` (100M with bounded Kafka
disk); `--files 1000 --wave-files 10` for the 1B stretch (hours).

Fastest numbers: prefix each `cargo run` with `RUSTFLAGS="-C target-cpu=native"`
(the tuned LTO profile is already applied by `--release`). The first
`--release` build after enabling LTO is slow (minutes, datafusion included);
subsequent bench runs reuse it.

---

## 5. Run-semantics caveats

- **hits parts overlap by key.** ClickBench files are row-range slices, so
  their `counter_id` ranges overlap; the loader commits one part per (file,
  day) at level 1 and **does not run the compactor** (a merge would rewrite the
  whole ~15 GB and trip the plan-28 cap). So per-tenant *fan-out* (files a scan
  opens) equals roughly the day-count, not 1 — pruning is under-measured
  relative to a compacted layout. Revisit once row 29 (streaming merge) makes
  compacting the fixture feasible. The `day` partition is derived from `ts`
  (the same `chrono::from_timestamp_millis` ingest uses), not `EventDate`.
- **hits row-group size** is the arrow-writer default (via the shared
  `rewrite::batch_to_parquet`), not a hand-tuned 64k — row-group sizing is
  plan 14's territory, and the bench deliberately measures what the system
  writes today.
- **Bluesky exactly-once is asserted at the storage layer + per-tenant.** Rows
  span millions of hashed-`did` namespaces, so there is no single global count
  query; the run instead asserts `sum(live_parts.row_count) == mapped`,
  `ingest_offset_sum == produced_lines`, and per-census-tenant queryable count.
- **Historical timestamps.** Bluesky events are from 2023+, so the run widens
  `max_event_age_days` (like the e2e Stack) to keep the event-time rail (issue
  0004) from rejecting them.

---

## 6. Results

Warm medians (one warm-up, then median of N). Micro-tier medians live in
`docs/notes/2026-07-06-parquet-read-performance.md`.

> **Profile note.** The tables below were measured under the *default* release
> profile (LTO off, `codegen-units = 16`), before the tuned `[profile.release]`
> (fat LTO + one codegen unit) landed. Re-runs under the tuned profile — more so
> with `-C target-cpu=native` — will be faster across the board; re-capture and
> label the profile when you next run.

### Smoke tier (1 file each) — SHA `f565c46`, 2026-07-09

Machine: AMD Ryzen AI 9 HX 370 (24 threads), 91 GB RAM, NVMe; compose stack
local. First recorded run, validating the harness end to end — **not** the
headline baseline (the full tiers below are pending a dedicated run), but the
numbers are real and recorded for regression visibility.

**Bluesky write path, `bluesky run --files 1`** (1,000,000 events, 0 poison):

| Metric | Value |
|---|---|
| produce rate | ~240k rows/s |
| ingest catch-up | 6.3 s |
| stored rows | 1,000,000 (= mapped ✓ exactly-once) |
| offset sum | 1,000,000 (= produced lines ✓) |
| compaction / finalization | 0.0 s / 0.8 s |
| final layout | 1 part at level 2, ~57.9 MB (terminal invariant: one run) |
| backpressure deferrals / capped merges | 0 / 0 |

Per-tenant read (top-3 census tenants), warm median ms:

| scenario | t1 | t2 | t3 |
|---|---|---|---|
| `count` | 9.4 | 8.8 | 8.6 |
| `by_collection` | 11.7 | 10.9 | 11.0 |
| `ts_window` | 14.2 | 16.1 | 17.4 |

**hits read, `hits load --files 1` + `hits queries`** (7 tenants; heavy=508k
rows, median=10 rows, light=78k rows; fan-out 2 per the overlap caveat), warm
median ms:

| scenario | heavy | median | light |
|---|---|---|---|
| `count` | 13.5 | 13.8 | 13.3 |
| `adv_filter` | 15.2 | 12.1 | 15.8 |
| `distinct_users` | 19.2 | 14.2 | 18.1 |
| `top_urls` | 50.7 | 15.0 | 35.9 |
| `search_phrases` | 50.0 | 18.0 | 21.6 |
| `ts_window` | 18.9 | 15.9 | 14.8 |
| `minutely` | 18.7 | 15.9 | 15.5 |

### 10 GB tier (hits full, Bluesky 10M) — SHA `4d5f93a`, 2026-07-09

Same machine as the smoke tier. One pass each, per the runbook (fetch ~12 min,
load ~9 min, queries ~1 min, bluesky run ~3 min).

**hits, all 100 files**: loaded 99,997,497 rows into **1,102 live L1 parts /
6.46 GB** (the 14-column ZSTD subset of the 14.8 GB source). Census: 6,506
tenants; heavy = 3922 (8.53M rows), median = 150550 (123 rows), light = 240030
(1,003 rows). Warm median ms:

| scenario | heavy (fan-out 67) | median (fan-out 108) | light (fan-out 82) |
|---|---|---|---|
| `count` | 39.5 | 51.7 | 42.4 |
| `adv_filter` | 52.5 | 53.3 | 46.2 |
| `distinct_users` | 78.3 | 54.0 | 46.0 |
| `top_urls` | 363.3 | 56.6 | 51.0 |
| `search_phrases` | 208.9 | 55.1 | 48.6 |
| `ts_window` | 2.7 | 3.1 | 2.7 |
| `minutely` | 4.1 | 4.1 | 4.7 |

Readings:

- **Key-range pruning holds up even on this overlapping layout**: fan-out
  67–108 of 1,102 live parts — 91–94% of files never opened. The §5 overlap
  caveat cost less than feared.
- **Tiny tenants pay a flat ~45–55 ms floor** — a 123-row tenant costs what a
  1k-row tenant costs. That is per-file open/footer cost × fan-out, not data:
  direct, quantified motivation for plan 13 (Parquet metadata cache) and for
  fan-out-shrinking compaction/coalescing (rows 29/22). Re-measure both
  against this table.
- **ts-stats pruning (plan 12) is decisive**: `ts_window`/`minutely` run
  2.7–4.7 ms *regardless of tenant size* — part-level ts min/max prunes the
  24 h window before any I/O.
- Heavy-tenant string aggregations do real scan work (`top_urls` 363 ms over
  8.5M rows) — the scan path, not the catalog, is the cost there.

**Bluesky write path, `bluesky run --files 10`** (10,000,000 lines, 6 poison;
production guardrail defaults):

| Metric | Value |
|---|---|
| produce rate | 220.6k rows/s (45.3 s) |
| ingest catch-up | 91.9 s (~109k rows/s through flush → upload → commit) |
| stored rows | 9,999,994 (= mapped ✓ exactly-once; offsets = 10,000,000 produced ✓) |
| compaction / finalization after drain | 0.05 s / 7.7 s |
| final layout | 1 part at level 2, 549 MB (terminal invariant: one run) |
| backpressure deferrals | **67** — plan 18's slowdown band engaged during catch-up |
| capped merges | **6** — plan 28's cap fired transiently mid-run |
| per-tenant reads (top-3 census) | `count` ~15 ms · `by_collection` ~16–18 ms · `ts_window` ~19–20 ms |

Readings: **the first real-volume observation of both guardrails.** A single
in-process compactor lags a ~220k rows/s firehose aimed at roughly one
day-partition, live L0 crosses the slowdown threshold (30), and ingest
self-throttles — exactly the plan-18 design. The transient plan-28 caps
resolved as the ladder climbed, and the terminal invariant still landed
(one run). This is the run-shape the P2 collector gauges (row 21) now make
visible in production.

### 100M / 1B tiers — optional / stretch

100M Bluesky is a documented long run; 1B needs `--wave-files` and hours.

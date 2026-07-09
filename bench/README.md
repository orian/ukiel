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
- **Build profile.** `--release` resolves to a tuned profile (ThinLTO, root
  `Cargo.toml`) — cross-crate inlining across the whole graph
  (datafusion/arrow/parquet included) while keeping the compile *parallel*
  (ThinLTO's optimization and per-crate codegen both multithread; fat LTO +
  `codegen-units = 1` would serialize the datafusion link). For the absolute
  fastest numbers also pass architecture-specific SIMD:
  `RUSTFLAGS="-C target-cpu=native" cargo run --release …` (non-portable, so
  opt-in rather than baked in). **State the profile with any numbers you
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

**Keep the datasets — do not delete them between runs.** They are gitignored
(`bench/datasets/`), so they never touch the repo, and ~16 GB is cheap disk.
Re-running `make bench-fetch-*` with the files already present is **resume-only**
(`curl -C -` issues a HEAD per file and transfers nothing), so it costs seconds,
not another 15 GB download. `bench/bench.sh` never touches the datasets — only
`docker compose down -v` (which wipes MinIO/Postgres/Kafka volumes, not
`bench/datasets/`).

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

## 4a. Running & comparing (no LLM needed)

Two helpers under `bench/` automate a full run and A/B comparison — pure
Python 3 + bash, no extra deps.

**`bench/bench.sh LABEL [BASELINE] [opts]`** — builds the tuned binary, resets
the compose stack, runs the benches, saves `bench/results/*-LABEL.json`,
regenerates the HTML page, and (if `BASELINE` given) prints a terminal delta:

```bash
# Baseline on the current code, then a candidate after your change:
git stash && bench/bench.sh before
git stash pop && bench/bench.sh after before      # prints `before → after` deltas
```

Options: `--skip-hits`, `--skip-bluesky`, `--files N` (bluesky, default 10),
`--iters N` (hits queries, default 10), `--no-native` (portable build),
`--no-reset` (reuse a clean running stack). It brings the stack up/down itself;
you still fetch datasets once with `make bench-fetch-*`.

**`bench/report.py`** — the reporter `bench.sh` calls, also usable directly:

```bash
python3 bench/report.py list                 # available run labels
python3 bench/report.py compare A B          # terminal delta tables (A → B)
python3 bench/report.py html                 # → bench/results/report.html
make bench-report                            # same as `report.py html`
```

`compare` reads `hits-queries-<label>.json` and `bsky-run-<label>.json` for the
two labels and prints per-scenario and per-metric deltas (Δ%; lower ms is
better, higher throughput is better). `html` embeds every run's JSON into one
self-contained page (`bench/results/report.html`, gitignored) — open it
directly, no server: it shows every run as a table plus a baseline/candidate
picker that colours regressions red and improvements green.

Both `bench/results/report.html` and the `*.json` reports are gitignored, so
comparisons stay local.

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

> **Profile note.** Each result block below states the build profile it was
> measured under. The **default** profile is stock `--release` (LTO off,
> `codegen-units = 16`); the **tuned** profile is the ThinLTO
> `[profile.release]` in the root `Cargo.toml`, optionally with
> `-C target-cpu=native`. Always label the profile with new numbers.

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

### 10 GB tier — tuned profile (ThinLTO + `-C target-cpu=native`), 2026-07-09

Same machine and same workload as the default-profile block above, rerun on a
clean stack under the tuned build (see **Build profile**). Identical census
(heavy = 3922 / 8.53M rows, median = 150550 / 123 rows, light = 240030 / 1,003
rows; fan-out 67 / 108 / 82), so the two blocks are directly comparable.

hits read, warm median ms — **default → tuned**:

| scenario | heavy | median | light |
|---|---|---|---|
| `count` | 39.5 → 40.4 | 51.7 → 48.5 | 42.4 → 42.3 |
| `adv_filter` | 52.5 → 51.2 | 53.3 → 60.7 | 46.2 → 47.5 |
| `distinct_users` | 78.3 → 67.5 | 54.0 → 52.7 | 46.0 → 45.7 |
| `top_urls` | 363.3 → 366.2 | 56.6 → 52.4 | 51.0 → 50.8 |
| `search_phrases` | 208.9 → 182.9 | 55.1 → 54.5 | 48.6 → 50.3 |
| `ts_window` | 2.7 → 2.5 | 3.1 → 2.3 | 2.7 → 2.6 |
| `minutely` | 4.1 → 3.7 | 4.1 → 3.9 | 4.7 → 3.8 |

Bluesky write path, `bluesky run --files 10` — **default → tuned**:

| Metric | default | tuned |
|---|---|---|
| produce rate | 220.6k rows/s | 220.9k rows/s |
| ingest catch-up | 91.9 s (~109k rows/s) | **81.0 s (~123k rows/s)** |
| stored rows | 9,999,994 ✓ | 9,999,994 ✓ |
| compaction / finalization | 0.05 s / 7.7 s | 0.05 s / 7.5 s |
| final layout | 1 part L2, 549 MB | 1 part L2, 576 MB |
| deferrals / capped merges | 67 / 6 | 60 / 6 |
| per-tenant reads (count / by_collection / ts_window) | ~15 / ~16–18 / ~19–20 ms | ~13 / ~15–17 / ~18–21 ms |

Comparison readings:

- **Ingest is the clear winner: catch-up ~12% faster** (91.9 → 81.0 s, 109k →
  123k rows/s). The ingest hot path — JSON decode, per-batch lexsort, Parquet
  encode — is CPU-bound, so cross-crate inlining + native SIMD land directly.
- **Read path is largely flat** — DataFusion is already heavily optimized and
  the numbers sit in run-to-run noise, with small heavy-tenant wins where the
  scan does the most CPU work (`distinct_users` 78 → 68 ms, `search_phrases`
  209 → 183 ms). `ts_window`/`minutely` stay 2–4 ms (stats-pruning, not CPU).
- **Producer and finalization unchanged** — the producer is Kafka-bound and
  finalization is object-store-I/O-bound, neither CPU-limited.
- Guardrails engaged the same way (deferrals ~60–67, caps 6); exactly-once held
  (stored == mapped, offsets == produced). The final-layout byte difference
  (549 vs 576 MB) is merge row-group/ordering variance, not a row-count change.

Net: the tuned profile pays off most on the **write/ingest CPU path**; the read
path is I/O- and DataFusion-bound and barely moves.

### 10 GB tier — plan 13 (Parquet metadata cache), 2026-07-09

Same machine, tuned profile, identical fixture (census heavy = 3922 / 8.53M
rows, median = 150550 / 123 rows, light = 240030 / 1,003 rows; fan-out
67 / 108 / 82 — identical to the `tuned` block, so the cache is the only
variable). hits read, warm median ms — **tuned (no cache) → cache13**:

| scenario | heavy | median | light |
|---|---|---|---|
| `count` | 40.4 → 24.1 (−40%) | 48.5 → 32.1 (−34%) | 42.3 → 27.1 (−36%) |
| `adv_filter` | 51.2 → 36.0 (−30%) | 60.7 → 37.4 (−38%) | 47.5 → 29.8 (−37%) |
| `distinct_users` | 67.5 → 52.9 (−21%) | 52.7 → 35.6 (−33%) | 45.7 → 27.8 (−39%) |
| `top_urls` | 366.2 → 366.8 (+0%) | 52.4 → 38.0 (−27%) | 50.8 → 31.8 (−37%) |
| `search_phrases` | 182.9 → 181.4 (−1%) | 54.5 → 43.1 (−21%) | 50.3 → 32.6 (−35%) |
| `ts_window` | 2.5 → 2.7 | 2.3 → 2.8 | 2.6 → 2.5 |
| `minutely` | 3.7 → 3.8 | 3.9 → 4.4 | 3.8 → 3.9 |

Reading — **the metadata cache does exactly what plan 13 predicted**:

- **The per-tenant fan-out floor collapses ~30–40%.** Every query paid
  67–108 footer reads (2 object-store roundtrips each) before touching data;
  caching those parsed footers process-wide removes that fixed cost. `count`,
  the filters, and the small/median-tenant aggregations — all dominated by the
  floor — drop a third or more.
- **Scan-bound queries are correctly unaffected.** The two heavy-tenant string
  aggregations (`top_urls` 367 ms, `search_phrases` 181 ms over 8.5M rows) are
  dominated by actual scan work, not footer reads, so they stay flat (within
  noise) — the cache never touches the scan path.
- **Already-pruned queries stay near-zero.** `ts_window`/`minutely` were 2–4 ms
  via plan-12 stats pruning (no fan-out to pay), so there was nothing to save;
  the ±0.5 ms wiggle is run-to-run noise.

Reproduce: `bench/bench.sh cache13 tuned --skip-bluesky` (loads hits, runs the
suite under `cache13`, prints the `tuned → cache13` delta).

### 10M write path — plan 27 (ingest sort by declared sort_key), 2026-07-09

Same machine, tuned profile, same `bsky` schema (`sort_key = [tenant_id, ts]`).
Plan 27 replaced the ingest pre-decode `serde_json`-row sort with a columnar
`lexsort` on the decoded Arrow batch. `bluesky run --files 10` (10M events),
**tuned (pre-27) → sort27**:

| metric | tuned | sort27 | Δ |
|---|---|---|---|
| produce rate | 220.9k rows/s | 221.2k rows/s | +0.1% |
| ingest catch-up | 81.0 s | **38.5 s** | **−52%** |
| backpressure deferrals | 60 | 0 | −100% |
| capped merges | 6 | 0 | −100% |
| stored rows | 9,999,994 ✓ | 9,999,994 ✓ | exactly-once held |
| final layout | 1 part L2, 575.60 MB | 1 part L3, 575.63 MB | ~identical |

Readings:

- **The write path is the real, attributable win.** The old sort did two
  `serde_json::Value` map lookups per comparison (O(N log N) of them per
  flush); the columnar `lexsort` on contiguous i64 arrays is far cheaper, so
  ingest keeps up with the ~220k rows/s firehose and **never trips backpressure
  (60 → 0 deferrals) or the merge cap (6 → 0)** — which kept compaction clean.
- **Reads are NOT a plan-27 effect here.** For `[tenant_id, ts]` the physical
  row order is unchanged, so the finalized files are byte-for-byte ~equal
  (575.6 MB both, 1 part). Plan 27 only flips the sorting-column *metadata*
  (`nulls_first`) to match the provider's declared ordering — a correctness
  fix, not a layout change. The per-tenant read wobble seen in this A/B
  (`count` ~12→10 ms) is run-to-run/system-state noise (tuned's reads ran right
  after heavy backpressure + caps; sort27's on a calm system), not a real
  read win. **Changing *how* files are laid out (a richer clustering
  `sort_key`) is the lever that would move reads — see below.**

Reproduce: `bench/bench.sh sort27 tuned --skip-bluesky` (runs the write path
under `sort27`, prints the `tuned → sort27` delta).

### 10 GB / 10M — plan 14 (Tier-3 file layout), 2026-07-09

Same machine, tuned profile. Plan 14 added key-aligned row groups (compaction),
per-level compression (L0 LZ4_RAW / L1+ ZSTD), delta-packed ts, and a 128k
row-group cap. Two comparisons, each isolating what the fixture actually
exercises.

**Bluesky write path — `sort27 → plan14`** (the real measurement: the compacted
file goes through the full key-aligned layout). `bluesky run --files 10`:

| metric | sort27 | plan14 | Δ |
|---|---|---|---|
| ingest catch-up | 38.5 s | 36.0 s | −6.4% (LZ4_RAW L0 encodes faster) |
| finalization | 7.5 s | 9.2 s | +23% (key-aligned flushes cost extra) |
| final file | 575.6 MB | 571.3 MB | −0.75% (delta-ts; payload dominates) |
| stored rows | 9,999,994 ✓ | 9,999,994 ✓ | exactly-once held |

Per-tenant reads, median ms (top-3 census tenants):

| scenario | tenant A | tenant B | tenant C |
|---|---|---|---|
| `count` | 10.2 → 10.4 | 11.0 → 10.5 | 9.8 → 9.8 |
| `by_collection` | 12.3 → 12.5 | 13.3 → 10.6 (−20%) | 12.8 → 11.7 (−9%) |
| `ts_window` | 16.4 → 11.9 (**−28%**) | 17.6 → 11.6 (**−34%**) | 17.3 → 11.3 (**−35%**) |

**This is the layout → reads win plan 14 was for.** `ts_window` (a
`ts BETWEEN` filter within one tenant) drops **~30%**: key-aligned row groups
put each tenant's rows in contiguous groups, and delta-packed ts sharpens the
per-group ts stats, so DataFusion prunes row groups/pages by ts range instead
of scanning. `count` is flat (no filter → nothing to prune); `by_collection`
gains from finer row groups. Write cost is a small, honest trade: LZ4 L0 is
faster (−6% ingest) but the key-aligned flushes make finalization a bit slower.

**hits read path — `cache13 → plan14`** (partial: the hits loader writes plain
files with the 128k cap, but is **not** compacted, so it never exercises
key-alignment/delta-ts). Mixed and noisy as expected; the consistent signal is
the heavy-tenant scan-bound aggregations improving from finer row groups —
`top_urls` 366.8 → 323.6 ms (−12%), `search_phrases` 181.4 → 148.6 ms (−18%) —
while `count`/`adv_filter` wobble in noise. A hits fixture that measures the
full plan-14 layout needs compaction (row 16 revalidates the population).

Reproduce: `bench/bench.sh plan14 sort27 --skip-hits` (Bluesky) and
`bench/bench.sh plan14 cache13 --skip-bluesky` (hits).

### 100M / 1B tiers — optional / stretch

100M Bluesky is a documented long run; 1B needs `--wave-files` and hours.

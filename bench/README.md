# Macro perf harness (plans 30 & 32)

Volume-scale benchmarks on real datasets — ClickBench `hits` (read path) and
Bluesky ndjson (write path) — run manually against the docker-compose stack via
the `bench` binary in `ukiel-e2e`. This bridges the plan-11 micro harness
(~100k synthetic rows, `crates/ukiel-query/tests/perf_smoke.rs`) to the README's
PB+ intent.

The harness reuses the e2e `Stack` (real Kafka/Postgres/MinIO, in-process
ingest/compactor workers, HTTP query server), so it measures the same
components the e2e suite proves correct.

**Two families of suite, and the difference matters:**

| | what it is | how it queries | plan |
|---|---|---|---|
| `hits queries`, `bluesky run` | ukiel's **own** per-tenant SaaS scenarios — the product's actual query shape | namespace-scoped, through the **HTTP endpoint** | 30 |
| `clickbench`, `bluesky jsonbench` | the **official** upstream suites, vendored | whole-table, through the **unscoped operator session** | 32 |

The official suites answer "how does ukiel do on the benchmark everyone
publishes?"; the house suites answer "how does ukiel do at the thing it is
*for*?" Neither substitutes for the other. The official suites are whole-table
by construction, which ukiel's product path is not — so they run through an
**operator (unscoped) session** (`ukiel_query::context::operator_session`), a
library-only door with no tenant isolation that is deliberately unreachable from
the HTTP server (issue 0001; a test asserts `server.rs` never references it).

Every deviation of a vendored query from its upstream is logged, per query, in
`bench/queries/<suite>/ADAPTATIONS.md`, with the pristine upstream file kept
next to it so the adaptation set is a reviewable `diff`, not a claim.

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
defaults. **`UKIEL_E2E_STORE_DIR=<dir>` swaps MinIO for a local-filesystem
object store** — set it for *both* the load and every later command against
that fixture (the catalog stores store-relative paths; mixing stores 404s).
Added for the gap-attribution runs (`docs/notes/2026-07-12-official-suite-gap.md`):
it removes the HTTP transport layer, which the plan-32 headline showed was
half the measured ukiel-vs-raw gap. `clickbench raw --dir` points the raw
reference at any parquet directory — e.g. the local store's own `ht/<id>/L1/`
right after a load, which prices ukiel's file layout with zero ukiel stack
(only on a freshly-loaded fixture: post-compaction the dir still holds
superseded objects until GC, and a listing would double-count). No config is
needed for the default local stack.

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
# House suites (plan 30) — per-tenant, namespace-scoped, via the HTTP endpoint
bench hits load [--files N]
bench hits compact [--target-mb N]
bench hits queries [--iters N] [--label LABEL]
bench bluesky produce --files N [--topic T]
bench bluesky run --files N [--wave-files W] [--flush-ms MS] [--label LABEL]

# Official suites (plan 32) — whole-table, via the unscoped operator session
bench clickbench load [--files N]
bench clickbench compact [--target-mb N]
bench clickbench run [--iters N] [--label LABEL]        # 43 queries, ukiel stack
bench clickbench raw [--iters N] [--label LABEL] [--dir D]  # same 43, bare DataFusion
bench clickbench sql "SQL"                              # one statement (EXPLAIN etc.), operator session
bench clickbench compare --ukiel LABEL --raw LABEL      # per-query overhead ratios
bench bluesky jsonbench [--iters N] [--label LABEL]     # JSONBench's 5 queries

# ClickHouse reference (plan 34) — needs `docker compose --profile bench up -d clickhouse`
bench clickhouse load --table official|cb|parquet [--files N]
bench clickhouse run  --table official|cb|parquet [--iters N] [--label LABEL]

bench --help
```

**The ClickHouse reference** answers the question plan 32 could not: *2.1x of
what?* Three fixtures, each in its own ClickHouse database so they cannot
overwrite one another:

- `official` — upstream's **verbatim** 105-column MergeTree and **verbatim**
  queries. Zero adaptations. Its total is held against ClickHouse's *published*
  ClickBench figure: the **machine calibration** that makes every other number in
  this file interpretable.
- `cb` — the same 25 columns as ukiel's fixture, with ukiel's sort key and day
  partitioning mirrored, and ukiel's `int64`/`String` widths (so ClickHouse is
  not handed narrower, cheaper data). The apples-to-apples OLAP ceiling.
- `parquet` — ClickHouse reading **the same parquet files bare DataFusion
  reads**, via `file()`. The only measurement where the two engines differ in
  *nothing but the engine*. See §6 for what it found.

### `hits load [--files N]`

Loads ClickBench parquet into the `hits` hypertable. For each input file:
casts the 14-column subset (`counter_id`, `ts`, and 12 more) to supported
ukiel types, splits by `day`, sorts each `(day)` batch by `(counter_id, ts)`,
writes ZSTD Parquet, uploads to MinIO, and commits **one commit per input file**
at level 1 with column stats populated. Accumulates a per-`counter_id` census
and writes `bench/results/hits-tenants.json` (heavy / median / light / top-20),
creating logical `hits` tables for those tenants' namespaces (namespace id =
`counter_id`). `--files` caps how many present files to load (default: all).

`hits load` leaves the fixture uncompacted (per-file L1 runs); run
`hits compact` to fold it — see below and §5.

### `hits compact [--target-mb N]`

Drives compaction + finalization to quiescence over the loaded `hits`
hypertable, folding the per-file L1 runs into one run per day-partition (the
plan-17 terminal invariant). This is the plan-29 streaming merge at volume —
the ~15 GB fold streams in O(K·batch + row-group) memory where the retired
plan-28 cap once forbade it. Prints the fan-out collapse (parts before/after,
worst per-day run count, per-census-tenant fan-out). With `--target-mb N` it
first switches the hypertable to `SizeTargeted(N MiB)` placement, so heavy
keys **whale-split** into dedicated key-disjoint slices instead of collapsing
into one large packed file per day. Run `hits queries` before and after to
quantify the read impact.

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

### The official suites (plan 32)

#### `clickbench load [--files N]`

Loads the same ClickBench parquet into a **second** hypertable, `hits_cb`, whose
25 columns are named **verbatim** (`"CounterID"`, `"EventTime"`, …) so the
upstream file's quoted CamelCase identifiers resolve. Separate from `hits` on
purpose: renaming the plan-30 fixture would invalidate its recorded baselines,
and the bench results are append-only. Packing key `CounterID`, sort key
`("CounterID", "EventTime")`, day-split, one L1 commit per input file — same
loader, different descriptor.

Costs ~10–13 GB in MinIO *alongside* the `hits` fixture (both fit; `make e2e-down`
wipes everything).

#### `clickbench run|raw [--iters N] [--label LABEL]`

The 43 official queries from `bench/queries/clickbench/queries.sql`:

- **`run`** — through ukiel's full read stack (catalog planning, part pruning,
  provider, footer cache) via the operator session.
- **`raw`** — the *same SQL over the same parquet* on **bare DataFusion**, no
  ukiel at all. Upstream's own harness pins DataFusion 54, which is ukiel's pin,
  so this is both a reference ceiling and a sanity check against ClickBench's
  published DataFusion numbers.

Methodology is ClickBench's own: per query, **1 cold run** (fresh session, fresh
metadata cache) then `--iters` **hot runs** (default 3); the headline is the hot
minimum. A failing query is recorded and the suite *keeps going*, then the run
exits non-zero — partial coverage is loud, never silent. Writes
`bench/results/clickbench-{ukiel,raw}-<label>.json`.

#### `clickbench compare --ukiel LABEL --raw LABEL`

The deliverable: a per-query `ukiel / raw-DataFusion` **ratio** table — the
measured cost of the catalog/provider/cache stack over the bare engine on
identical queries and data. Refuses to compare runs whose fixtures differ in row
count, and fails if the two engines ever returned *different row counts* for the
same query (they disagree on the answer, so the timings are not comparable).

#### `bluesky jsonbench [--iters N] [--label LABEL]`

JSONBench's 5 Bluesky queries over whatever `bluesky run` last ingested (the
report records the fixture's row count, so results are self-describing). Same
cold/hot methodology.

Requires a `bsky` fixture carrying the `did` column (added in plan 32 — q2/q4/q5
group by the account); an older fixture is rejected with an explicit re-produce
instruction rather than silently answering a different question.

> **What this suite does and does not claim.** JSONBench measures *querying raw
> JSON*; ukiel flattens the event at ingest, so it queries typed columns. That
> is a real, declared system difference — a JSON-native engine does strictly more
> work per row here than we do. Read the numbers as *ukiel answering JSONBench's
> questions*, not as a like-for-like JSON-extraction benchmark. Full reasoning:
> `bench/queries/jsonbench/ADAPTATIONS.md`.

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

Official suites (plan 32) — same datasets, no extra download:

```bash
B="cargo run --release -p ukiel-e2e --bin bench --"

# ClickBench: load the verbatim-named fixture, run ukiel + the raw reference,
# then diff them. Both runs must see the same data for the ratios to mean
# anything, so load once and run both against it.
$B clickbench load                                       # → hits_cb (~100M rows)
$B clickbench run  --label loaded                        # ukiel, uncompacted
$B clickbench raw  --label loaded                        # bare DataFusion
$B clickbench compare --ukiel loaded --raw loaded        # the ratio table

$B clickbench compact                                    # fold to 1 run/day
$B clickbench run  --label compacted                     # ukiel, compacted
$B clickbench compare --ukiel compacted --raw loaded     # raw is layout-free

# JSONBench: needs a bsky fixture with the `did` column.
$B bluesky run --files 10 --label baseline
$B bluesky jsonbench --label baseline
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

- **hits parts overlap by key (loaded fixture).** ClickBench files are
  row-range slices, so their `counter_id` ranges overlap; `hits load` commits
  one part per (file, day) at level 1, giving a per-tenant *fan-out* (files a
  scan opens) of ~67–108. **`hits compact` folds this away** — plan 29's
  streaming merge makes the ~15 GB fold feasible in bounded memory (the
  plan-28 cap that once forbade it is retired), so the caveat is now
  measurable, not hypothetical: packed compaction collapses each day-partition
  to one run (fan-out → day-count ≈ 14), and `--target-mb N` uses SizeTargeted
  placement so heavy keys whale-split into dedicated slices instead. See §6
  "plan 29" for the before/after. The `day` partition is derived from `ts`
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
- **"Cold" is only cold at the ukiel layer** (official suites). A fresh session
  drops the parquet footer/metadata cache, so footers are re-read — but MinIO
  and the OS page cache still hold the object bytes. Our cold numbers are
  therefore optimistic relative to a true cold-storage read, exactly as
  ClickBench's are on a repeat run. The hot minimum is the headline for this
  reason.
- **The `ukiel / raw` ratio is not purely "stack overhead."** Ukiel's type
  system has no integer narrower than `int64`, so `hits_cb` widens the source's
  `Int16`/`Int32`/`UInt16` columns; ukiel reads more bytes per row than the raw
  engine does for the same column. Part of any ratio above 1 is that widening.
  Stated in full — with the other type-level deltas — in
  `bench/queries/clickbench/ADAPTATIONS.md`.
- **JSONBench q3's row count scales with the fixture.** One Bluesky file spans
  ~14 minutes, so a 1-file fixture puts every event in a single UTC hour bucket
  and q3 ("when do people use Bluesky") returns 3 rows, not 72. Not a bug — the
  suite reports the fixture's row count so results are self-describing.

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

### 10 GB / 10M — plan 29 (streaming merge + whale-key splitting), 2026-07-09

Same machine, tuned profile. Plan 29 rewrote `merge_parts` to stream
(`part_streams` → k-way `merge_streams` → `StreamingChunkWriter`) in
O(K·batch + row-group) memory and split whale keys into ts-sliced dedicated
files; the plan-28 input-size cap is retired. **This is the first run that
compacts the hits fixture** — the fold the cap used to forbid (§5 caveat
lifted).

**Bluesky write path — `plan14 → post-29`** (`bluesky run --files 10`, 10M
events): the streaming writer must not regress the write path.

| metric | plan14 | post-29 | Δ |
|---|---|---|---|
| produce rate | 221.4k rows/s | 219.0k rows/s | −1.1% (noise) |
| finalization | 9.2 s | 7.3 s | −21% |
| stored rows | 9,999,994 ✓ | 9,999,994 ✓ | exactly-once held |

Reads wobble ±10% at ~10 ms (noise). No write-path regression; finalization is
if anything faster (bounded-memory streaming vs read-all-then-sort).

**hits fold — `hits compact`** on the full fixture (1,102 live parts across 22
day-partitions, ~640 MB/day). The headline is that it completes at all:

| placement | result | fold time | heavy fan-out | median/light fan-out |
|---|---|---|---|---|
| Packed (default) | 1,102 → **22** parts (1 run/day) | 94 s | 84 → **14** | 84 → **14** |
| SizeTargeted(16 MiB) | 1,102 → **545** parts | 108 s | 84 → **59** (whale-split) | 84 → **14** |

**hits read path — `post-29` (loaded) → packed-compacted → SizeTargeted**, warm
median ms (census heavy = 3922 / 8.53M rows, median = 150550 / 123 rows, light =
240030 / 1,003 rows):

| scenario | heavy: loaded → packed → **sized** | median: loaded → packed → **sized** | light: loaded → packed → **sized** |
|---|---|---|---|
| `count` | 26.6 → 22.6 → **16.7** | 33.2 → 18.4 → **10.4** | 25.4 → 18.6 → **9.8** |
| `adv_filter` | 47.3 → 55.3 → **37.9** | 41.4 → 19.4 → **12.5** | 32.5 → 20.9 → **12.7** |
| `distinct_users` | 57.8 → 78.7 → **44.2** | 40.1 → 22.1 → **12.6** | 30.4 → 21.9 → **11.7** |
| `top_urls` | 321 → 571 → **312** | 46.6 → 24.3 → **15.4** | 33.1 → 24.2 → **15.4** |
| `search_phrases` | 136 → 300 → **131** | 42.4 → 23.5 → **15.2** | 34.9 → 23.4 → **14.2** |
| `ts_window` | 2.7 → 2.5 → **2.5** | 2.7 → 2.5 → **2.3** | 2.6 → 2.4 → **2.2** |

Readings — **compaction's value depends on placement, and whale-splitting is
why**:

- **The fold is now possible in bounded memory.** 14 GB / 1,102 parts collapse
  to one run per day-partition in ~94 s without the plan-28 cap ever engaging —
  the terminal invariant (cold partition = one run) holds at volume, which is
  the entire point of plan 29. RSS stays flat; nothing is ever fully resident.
- **The tail wins big under any placement.** Median/light tenants drop **~45%**
  on the scan-heavy scenarios (`top_urls` median 46.6 → 24.3 packed → 15.4
  sized) — fewer files to open, the fan-out floor plan 13 targeted is now
  structurally lower.
- **Packed compaction *regresses* the whale tenant** (heavy `top_urls` 321 →
  571, `search_phrases` 136 → 300): consolidating a dominant tenant's 8.5M rows
  into one big file per day collapses the read parallelism the 84-small-files
  layout had. Honest confounder — and exactly the failure mode whale-splitting
  exists to prevent.
- **SizeTargeted whale-splitting fixes it and then some.** With
  `--target-mb 16` the heavy tenant splits into 59 dedicated key-disjoint
  slices (parallelism restored) while median/light keep fan-out 14: heavy
  `top_urls` recovers to **312 ms** (below the uncompacted 321) and every other
  heavy scenario beats both other layouts. This is the recommended SaaS
  posture — heavy keys separate organically, the long tail packs.

Reproduce: `bench hits load && bench hits queries --label post-29` (loaded),
`bench hits compact && bench hits queries --label post-29-compacted` (packed),
then a fresh `hits load && bench hits compact --target-mb 16 && bench hits
queries --label post-29-sizetargeted`. Bluesky: `bench/bench.sh post-29 plan14
--skip-hits`.

### OFFICIAL SUITES — plan 32 (ClickBench 43 + JSONBench 5), 2026-07-12

SHA `9ba98b2`, same machine (AMD Ryzen AI 9 HX 370, 24 threads, 91 GB RAM,
NVMe), tuned profile, compose stack local. **First official numbers.**

Fixture: `hits_cb`, all 100 ClickBench files → **99,997,497 rows** (exactly
ClickBench's official count), 25 verbatim-named columns, 1,102 live parts across
22 day-partitions.

#### ClickBench 43 — ukiel vs bare DataFusion

Hot minimum per query (ClickBench's own methodology: 1 cold + 3 hot). The
reference is **the same SQL over the same parquet on bare DataFusion 54** — the
version upstream's own harness pins, which is ours — **with the same Parquet
reader settings** (see the methodology note below). So the ratio is the price of
ukiel's stack — catalog planning, provider, part pruning, footer cache — over the
engine's own ceiling, and nothing else.

| layout | total (sum of 43 hot minima) | vs raw | median per-query ratio |
|---|---|---|---|
| raw DataFusion (reference) | **24.7 s** | 1.00× | 1.00× |
| ukiel, loaded (1,102 parts) | 53.4 s | **2.16×** | 2.28× |
| ukiel, packed-compacted (22 parts) | 52.4 s | **2.12×** | 2.51× |
| ukiel, SizeTargeted 16 MiB (667 parts) | 52.9 s | **2.14×** | **2.20×** |

**Ukiel costs ~2.1× the bare engine on the official suite, and beats it on
nothing.** That is the headline, and it is not a flattering one. For a system
that adds a Postgres catalog, per-part planning, an isolation-capable provider
and a cache layer on top of DataFusion — and that widens every integer to `int64`
because it has no narrower type (ADAPTATIONS.md) — 2.1× is the honest price of
admission. It is the number to beat, and we are not going to round it away.

> **Methodology note — the confounder we found and killed.** The first raw
> reference ran DataFusion's *stock* Parquet settings, where `pushdown_filters`
> is **off by default**; ukiel's provider turns it **on**. That single flag was
> worth **14.8×** on q24 (raw 2958 ms → 199 ms) and inflated the reference on
> every query with a `WHERE` clause — which had made ukiel look *10× faster than
> DataFusion* on q24. It was never a ukiel win; it was a misconfigured opponent.
> The reference now matches ukiel's reader settings exactly, the "ukiel faster"
> column is empty, and the totals above are the corrected ones. If you take one
> methodological lesson from this file, take that one.

**No layout dominates** — and the three-way trade is the interesting result:

| query | loaded | packed | sized | raw | what it is |
|---|---|---|---|---|---|
| q01 | 60.2 | **4.8** | 39.0 | 1.0 | `COUNT(*)` |
| q02 | 271 | **95** | 183 | 14.3 | `COUNT(*) WHERE AdvEngineID <> 0` |
| q08 | 291 | **115** | 198 | 16.7 | `GROUP BY AdvEngineID` |
| q24 | 1422 | **290** | 2798 | 199 | `SELECT * … ORDER BY EventTime LIMIT 10` |
| q25 | 174 | 580 | **123** | 64.8 | `ORDER BY EventTime LIMIT 10` |
| q27 | 187 | 992 | **152** | 95.8 | `ORDER BY EventTime, SearchPhrase LIMIT 10` |
| q28 | 2745 | 4700 | **2722** | 1255 | `GROUP BY CounterID HAVING count > 100000` |
| q37 | **63.8** | 138 | 76.2 | 59.4 | `CounterID = 62 AND EventDate BETWEEN …` |
| q43 | 22.1 | 39.3 | **19.8** | 16.0 | `DATE_TRUNC('minute', …)` on one counter |

Readings:

- **Compaction is not a free win on a whole-table suite; packed placement trades
  pruning and parallelism for file count.** Folding 1,102 parts into 22 (one run
  per day) makes the scan-everything aggregates dramatically faster — fewer
  footers to open, q01 **12.5×** — but it *costs* the TopK and key-filtered
  queries (q27 **5.3× slower**, q25 3.3×, q37 2.2×). Two mechanisms, both
  structural: 22 files means 22-way scan parallelism on a 24-thread box (was
  1,102-way), and each day-part now spans **every** `CounterID`, so catalog
  part-pruning by packing key — the thing that made q37–q43 cheap — has nothing
  left to prune. Same failure mode plan 29 found on the whale tenant.
- **SizeTargeted repairs every packed regression and gives back most of the
  wins.** q27 992 → **152 ms** (better than uncompacted), q25 580 → **123**, q28
  and q37–q43 all back to loaded-or-better; the heavy key splits into 64
  dedicated slices while the tail still collapses to 14. It pays for that with
  the whole-table aggregates (q01 4.8 → 39, q24 290 → 2798: 667 files is a lot
  of footers when the query must read all of them). **Net: the best median ratio
  of the three (2.20×) and within 1% of the others on total** — the balanced
  posture, and the one to run in a multitenant deployment, where the queries that
  matter are key-filtered.
- **The worst ratios are aggregates ukiel could answer from its own catalog and
  doesn't.** q07 (`MIN("EventDate"), MAX("EventDate")`) is **78× slower** than
  raw (78.7 ms vs 1.0 ms): DataFusion reads it straight out of the Parquet column
  statistics without touching a row group. Ukiel *stores those exact min/max
  values in Postgres* (`parts.column_stats`, plan 12) and scans anyway. Same
  shape for q01 `COUNT(*)` (the catalog has `row_count` per part — 1.0 ms of
  arithmetic it currently pays 4.8–60 ms of I/O for). **A catalog-level aggregate
  pushdown would make these near-instant on data that is already in Postgres.**
  Filed as [issue 0010](../docs/issues/0010-aggregates-rescan-what-the-catalog-already-knows.md);
  no roadmap row claims it yet.
- **Cold ≈ hot once compacted** (median cold/hot 1.03× vs 1.15× loaded): with 22
  footers instead of 1,102 there is almost nothing left for the metadata cache to
  save. Plan 13's cache earns its keep exactly where fan-out is high — which is
  the uncompacted state, and the steady state of a live ingesting table.

#### JSONBench 5 — Bluesky, 9,999,994 rows

Hot minimum, one live part (post-finalization), after `bluesky run --files 10`:

| query | | ms |
|---|---|---|
| q1 | top event types | **32.8** |
| q2 | + unique users per type (`count(DISTINCT did)`) | **232.4** |
| q3 | hourly distribution (post/repost/like) | **133.1** |
| q4 | top 3 post veterans (earliest post per did) | **124.0** |
| q5 | top 3 by activity span | **132.6** |

> These are **not** comparable to published JSONBench numbers, and the honest
> reason is in `bench/queries/jsonbench/ADAPTATIONS.md`: JSONBench measures
> querying *raw JSON*, and ukiel flattened those fields into typed columns at
> ingest. A JSON-native engine does strictly more work per row here than we do.
> The suite answers "can ukiel answer JSONBench's questions, and how fast" —
> not "is ukiel faster at JSON extraction."

**Write path (`bluesky run --files 10`), post-29 → plan-32 fixture:**

| metric | post-29 | plan-32 (`did` added) | Δ |
|---|---|---|---|
| produce rate | 219.0k rows/s | 206.4k rows/s | **−5.8%** |
| finalization | 7.3 s | 8.1 s | +11% |
| stored rows | 9,999,994 ✓ | 9,999,994 ✓ | exactly-once held |

The write-path cost is **expected and attributable**: plan 32 added the `did`
utf8 column (~32 bytes/row on a 7-column row) so the suite could group by
account. It is a *fixture* change, not a code regression — but it is a real cost
and it is recorded rather than waved off as noise.

**Compaction of `hits_cb` (the 25-column fixture), for reference:**

| placement | parts | fold time | heavy-key fan-out | median/light fan-out |
|---|---|---|---|---|
| Packed (default) | 1,102 → **22** (1 run/day) | 131 s | 84 → 14 | 84 → 14 |
| SizeTargeted(16 MiB) | 1,102 → **667** | 149 s | 84 → **64** (whale-split) | 84 → **14** |

Reproduce:

```bash
B="cargo run --release -p ukiel-e2e --bin bench --"
$B clickbench load && $B clickbench run --label loaded && $B clickbench raw --label loaded
$B clickbench compare --ukiel loaded --raw loaded
$B clickbench compact && $B clickbench run --label compacted        # packed
$B clickbench compare --ukiel compacted --raw loaded
# sized needs a fresh load (compaction is not reversible):
make e2e-down && make e2e-up
$B clickbench load && $B clickbench compact --target-mb 16 && $B clickbench run --label sized
$B clickbench compare --ukiel sized --raw loaded
# JSONBench:
$B bluesky run --files 10 --label plan32 && $B bluesky jsonbench --label plan32
```

### OFFICIAL SUITES follow-up — gap attribution + layout experiment, 2026-07-12

The plan-32 2.16× was decomposed with controlled runs (full analysis and
per-run tables: `docs/notes/2026-07-12-official-suite-gap.md`): **transport
52%** (MinIO HTTP, no data-cache tier — killed for benching by
`UKIEL_E2E_STORE_DIR`, killed for production by plan 15), **stack 38%**
(dominated by issue-0010 aggregates — row 35 — while key-filtered queries
*beat* raw DataFusion 3–4.5× on identical files), **layout 10%** (int64
widening + ZSTD — row 36). With local bytes ukiel is **1.56× raw**. The
provider now declares its output ordering only on predicated scans (measured
split, see the note; product path unaffected by construction). A time-led
fixture layout (`clickbench load --layout ts`) was measured and recorded as
*not* a net win — it trades the CounterID point-query class for
aggregate/TopK wins (ts-sized 42.4 s vs counter-loaded 38.6 s local).
Labels: `local-loaded`, `raw ukiel-files`, `ab-*` (ordering A/B),
`heuristic*`, `tslayout-*`. Anchor totals: raw 24.7 s / ukiel-local 38.6 s /
ukiel-MinIO 53.4 s.

### CLICKHOUSE REFERENCE — plan 34 (ClickHouse half), 2026-07-12

SHA `098eb58`. Same machine (AMD Ryzen AI 9 HX 370, 12C/24T, 91 GB RAM, NVMe),
ClickHouse **26.6** in the compose `bench` profile, HTTP interface, same 100
ClickBench parquet files read **in place**. Same methodology as every other rung
(1 cold + 3 hot, headline = hot minimum). All rows: **99,997,497**.

#### Is this machine normal? (the calibration)

`clickhouse run --table official` runs upstream's **verbatim** 105-column
MergeTree and **verbatim** `queries.sql` — zero adaptations — so our total is
directly comparable to ClickHouse's own published ClickBench numbers:

| | 43-query hot-min total | load |
|---|---|---|
| **this machine** (Ryzen HX 370, 24T) | **20.9 s** | **38.5 s** |
| published `c6a.4xlarge` (16 vCPU x86) | 32.3 s | 294 s |
| published `c8g.4xlarge` (16 vCPU Graviton4) | 14.2 s | 293 s |
| published `c6a.metal` (192 vCPU) | 7.7 s | 252 s |

**The machine is ordinary and the harness is sane.** A 24-thread laptop landing
between two 16-vCPU cloud boxes is exactly where it should land. Every other
number in this file can now be read as "on a box a bit faster than a
c6a.4xlarge" rather than "on some laptop." (Our load is far faster than the
published one — local NVMe, no download in the timing.)

#### The whole ladder, one table

| engine / substrate | total (43 hot minima) | vs ClickHouse MergeTree |
|---|---|---|
| ClickHouse, 105-col MergeTree (upstream verbatim) | **20.9 s** | 0.91× |
| **ClickHouse, 25-col MergeTree** (ukiel's sort key + day partitioning) | **22.9 s** | **1.00×** |
| bare DataFusion, raw parquet, local disk | 24.7 s | 1.08× |
| **ClickHouse, the *same* raw parquet** (`file()`) | **31.5 s** | **1.38×** |
| ukiel, local-filesystem store | 38.6 s | 1.69× |
| ukiel, MinIO — loaded / packed / sized | 53.4 / 52.4 / 52.9 s | ~2.3× |

#### The finding I did not expect

**On identical parquet bytes, DataFusion beats ClickHouse — 24.7 s vs 31.5 s
(1.27×).** Same files, same local disk, same 25 columns, same queries; the only
difference is the engine. ClickHouse's ClickBench dominance is therefore **not
an execution-engine advantage over DataFusion — it is a storage-format
advantage**: MergeTree (22.9 s) versus ClickHouse reading parquet (31.5 s) is
**1.38×**, and that 1.38× is the entire gap between "ClickHouse the benchmark
champion" and "an engine DataFusion outruns."

That reframes ukiel's position. We are built on the engine that *wins* the
same-substrate comparison. Our 2.3× to ClickHouse is not a wager on a slow
engine — it decomposes into things we can name and fix:

- **transport (52%)** — MinIO HTTP with no data-cache tier. Local bytes alone
  take ukiel from 53.4 s to **38.6 s**. That is roadmap **row 15** (cache
  tiering), already planned.
- **stack (38%)** — dominated by issue **0010** (aggregates we scan for that the
  catalog already knows: q07 is 78× raw). Roadmap **row 35**.
- **layout (10%)** — int64 widening + ZSTD. Roadmap **row 36**.
- **format** — the 1.38× MergeTree wins over parquet is the one *nobody* on a
  parquet-on-object-storage architecture gets for free. It is the honest,
  permanent tax of the lakehouse shape, and it is worth naming rather than
  chasing.

Fix transport and stack and the arithmetic says ukiel lands near
DataFusion-on-parquet, i.e. **at or slightly ahead of ClickHouse-on-parquet**,
with MergeTree still ~1.4× ahead on its own format. That is a defensible place
for a multitenant lakehouse to be, and it is a claim this table can be held
against.

#### The narrow-types tax, measured but not over-claimed

The 105-column table (20.9 s) beats our 25-column one (22.9 s) by **2.0 s**
despite reading 4× the columns — because upstream keeps the source's narrow
types (`Int16`/`Int32`/`UInt16`) while ours widens everything to `int64`, as
ukiel's type system requires.

But **~0.7 s of that 2.0 s is our own dialect adaptation, not types**: q28/q29
use `lengthUTF8` (character length, to match DataFusion's `length` semantics so
every rung returns the same answer) where upstream uses byte `length` — q28
alone costs +613 ms. The remaining ~1.3 s sits in the group-by-heavy q31–q35,
which is consistent with int64 keys, but the two tables also differ in column
count, so this is **suggestive, not a clean measurement of type widening**. The
clean one is roadmap row 36's.

#### Reproduce

```bash
docker compose --profile bench up -d clickhouse       # opt-in; `make e2e` never starts it
B="cargo run --release -p ukiel-e2e --bin bench --"
$B clickhouse load --table official && $B clickhouse run --table official --label baseline
$B clickhouse load --table cb       && $B clickhouse run --table cb       --label baseline
$B clickhouse load --table parquet  && $B clickhouse run --table parquet  --label baseline
```

Each fixture lives in its own ClickHouse database (`bench_official`, `bench_cb`,
`bench_parquet`) — `cb` and `parquet` both present a table called `hits_cb`, and
sharing a database would have one silently overwrite the other.

### OFFICIAL SUITES — plan 15 (cache tiering), 2026-07-12

Same machine, same fixture state as the anchors (fresh `clickbench load`:
99,997,497 rows, 1,102 parts, loaded/uncompacted), MinIO transport, the
plan-15 cache tier wired into the e2e stack via `UKIEL_E2E_CACHE_DIR` with the
cache dir on tmpfs (`/dev/shm` — memory-backed, the upper bound of the tier;
NVMe would sit between this and local-store). Cache starts cold; the per-query
cold pass populates it (the 1,102 parts are ~7 MB avg, below the 64 MiB
threshold, so this run prices the whole-file path — production compactor
outputs land hot via write-through instead of paying that first read).

| run | total (43 hot minima) | vs raw | median per-query ratio |
|---|---|---|---|
| ukiel, MinIO, no cache (plan-32 anchor) | 53.4 s | 2.16× | 2.28× |
| **ukiel, MinIO + plan-15 cache (tmpfs)** | **39.5 s** | **1.60×** | 1.76× |
| ukiel, local store (transport-free anchor) | 38.6 s | 1.56× | 1.65× |
| raw DataFusion, source files (ceiling) | 24.7 s | 1.00× | — |

**The cache recovers 94% of the measured transport share** (13.9 s of the
14.8 s the gap note attributed to MinIO HTTP), landing within 2.3% of the
local-store anchor — plan 15 does in production configuration what
`UKIEL_E2E_STORE_DIR` did as an experiment. Per-query: the mid-size scans gain
the most (q02/q04/q20/q30 at 0.30–0.32× of the no-cache time); the CPU-bound
full-scan regexes (q34/q35) don't move (1.02×) because transport was never
their cost. Residual vs local-store is small (median 1.03×) and concentrated in
the sub-500 ms TopK/point queries (q24 1.43×, q27 1.54×) — consistent with
the cache's extra per-read `stat`+open against 1,102 parts, though not
separately attributed (single run each; could be partly noise).

Label: `cache-shm`. Reproduce:

```bash
$B clickbench load
mkdir -p /dev/shm/ukiel-bench-cache
UKIEL_E2E_CACHE_DIR=/dev/shm/ukiel-bench-cache $B clickbench run --label cache-shm
$B clickbench compare --ukiel cache-shm --raw loaded
```

### KEY INDEX — plan 16 (roaring key bitmap + catalog access plans), 2026-07-13

SHA `de56b19`, same machine, tuned profile. `hits` fixture reloaded (100 files,
100M rows, 1,102 parts) so its parts carry the new index.

**The mechanism, measured** (`bench hits fanout` — files that survive *range*
pruning vs *bitmap* pruning, i.e. what the provider actually plans):

| tenant | rows | range-pruned | **bitmap-pruned** | files eliminated |
|---|---|---|---|---|
| heavy (3922) | 8,527,070 | 67 | **56** | 16% |
| median (150550) | 123 | 108 | **7** | **93%** |
| light (240030) | 1,003 | 82 | **8** | **90%** |

Those eliminated files are the sparse-tenant false positive: their key *range*
contains the tenant, so min/max pruning kept them, but they hold **none of its
rows**. The query used to open every one of them to find nothing.

**Read suite — `post-29` → `plan16`**, warm median ms (same fixture shape,
uncompacted, MinIO):

| scenario | heavy | median | light |
|---|---|---|---|
| `count` | 26.6 → 25.3 (−5%) | 33.2 → **10.2 (−69%)** | 25.4 → **10.2 (−60%)** |
| `adv_filter` | 47.3 → 54.7 (+16%) | 41.4 → **11.3 (−73%)** | 32.5 → **11.7 (−64%)** |
| `distinct_users` | 57.8 → 53.4 (−8%) | 40.1 → **11.3 (−72%)** | 30.4 → **12.3 (−60%)** |
| `top_urls` | 321 → 309 (−4%) | 46.6 → **13.5 (−71%)** | 33.1 → **15.7 (−53%)** |
| `search_phrases` | 137 → 149 (+9%) | 42.4 → **13.9 (−67%)** | 34.9 → **16.3 (−53%)** |
| `ts_window` | 2.7 → 3.0 | 2.7 → 2.4 | 2.6 → 2.5 |

Readings:

- **The long tail collapses: −53% to −73% on every scan-heavy scenario** for the
  median and light tenants. This is the multitenant case the product is *for* —
  thousands of small tenants sharing packed files — and it is where min/max
  pruning was weakest.
- **The heavy tenant is flat, and that is the correct outcome**, not a
  disappointment. A tenant with 8.5M rows genuinely has data in almost every
  part (67 → 56 files), so there is nothing for the bitmap to prune. Its ±5–16%
  wobble is noise at this fan-out. **A perf feature that helps the case it was
  designed for and does nothing elsewhere is working**; one that appeared to help
  everywhere would be the suspicious result.
- **Catalog cost: 794 bytes per part** (854 kB of `column_stats` across 1,102
  parts), for ~6,500 tenants. The 10k-distinct-key cap never engaged; 951 of
  1,102 parts carry a bitmap (the other 151 are single-key parts, which are
  already exact under range pruning and are deliberately skipped).
- **Row-group access plans (#6) are not separately measurable here.** The loaded
  fixture's parts are written by the bench loader, which does not produce
  key-aligned row groups — spans appear after `compact`. And for a *partial*
  skip, DataFusion's metrics cannot distinguish a provider-skipped group from a
  statistics-pruned one (see `docs/notes/2026-07-06-parquet-read-performance.md`).
  The bitmap is the headline; the access plan rides along.

### 100M / 1B tiers — optional / stretch

100M Bluesky is a documented long run; 1B needs `--wave-files` and hours.

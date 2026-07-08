# Macro perf harness & baselines (plan 30)

Volume-scale benchmarks bridging the plan-11 micro harness (~100k synthetic
rows, `crates/ukiel-query/tests/perf_smoke.rs`) to the README's PB+ intent.
The `bench` binary in `ukiel-e2e` reuses the e2e `Stack` (real Kafka/Postgres/
MinIO via docker-compose, in-process ingest/compactor, HTTP query server), so
the benchmark measures the same components the e2e suite proves correct.

**Manual-only** — never CI (it spawns dozens of GB and runs for minutes to
hours). Always `--release`. Datasets and results are gitignored
(`bench/datasets/`, `bench/results/`).

## Datasets

| Dataset | Source | Size | Rows |
|---|---|---|---|
| ClickBench `hits` | `datasets.clickhouse.com/hits_compatible/athena_partitioned/hits_{0..99}.parquet` | ~14.8 GB (100 files, ~150 MB each) | ~100M |
| Bluesky (JSONBench) | `clickhouse-public-datasets.s3.amazonaws.com/bluesky/file_{0001..1000}.json.gz` | 100M tier ≈ 12.5 GB gz (100 files, 1M events each); 1B tier = 125 GB gz | 1M/file |

Fetch: `make bench-fetch-hits` (all 100) and `make bench-fetch-bluesky`
(`FILES=100` default, `FILES=1000` for the 1B tier). Both use `curl -C -`, so
interrupted downloads resume.

## Resource requirements

- **hits**: 15 GB download + ~15 GB rewritten into MinIO ⇒ ~50 GB free disk.
- **Bluesky 100M**: 12.5 GB gz download; Kafka holds ~45 GB uncompressed
  messages unless produced in drain-waves ⇒ ~80 GB free, or use `--wave-files`.
- **Bluesky 1B**: 125 GB gz, waves mandatory, hours of wall clock (stretch).

## Runbook

```bash
make e2e-up                                              # compose stack
make bench-fetch-hits                                    # ~15 GB
cargo run --release -p ukiel-e2e --bin bench -- hits load
cargo run --release -p ukiel-e2e --bin bench -- hits queries --label baseline
# PERF lines + bench/results/hits-queries-baseline.json

make bench-fetch-bluesky FILES=10                        # 10M-event tier
cargo run --release -p ukiel-e2e --bin bench -- bluesky run --files 10 --label baseline
# PERF lines + bench/results/bsky-run-baseline.json
make e2e-down                                            # wipes volumes
```

Waves for the big tiers: `bluesky run --files 100 --wave-files 10` throttles
the producer to ~10 files of un-ingested Kafka backlog.

## Run-semantics caveats

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
- **bench asserts, not just measures.** The bluesky run fails (non-zero exit,
  report still written) if stored rows ≠ mapped events, offsets ≠ produced
  lines, or a census tenant's queryable count ≠ its census — exactly-once at
  volume as an assertion.

## Baselines

Machine: AMD Ryzen AI 9 HX 370 (24 threads), 91 GB RAM, NVMe; compose stack
local. Warm medians (one warm-up, then median of N). Micro-tier medians remain
in `docs/notes/2026-07-06-parquet-read-performance.md`.

### Smoke tier (1 file each) — SHA `f565c46`, 2026-07-09

First recorded run, validating the harness end to end. **Not** the headline
baseline (the full tiers below are pending a dedicated run); recorded for
regression visibility and because the numbers are real.

Bluesky write path, `--files 1` (1,000,000 events, 0 poison):

| Metric | Value |
|---|---|
| produce rate | ~240k rows/s |
| ingest catch-up | 6.3 s |
| stored rows | 1,000,000 (= mapped ✓ exactly-once) |
| compaction / finalization | 0.0 s / 0.8 s |
| final layout | 1 part at level 2 (terminal invariant: one run) |
| backpressure deferrals / capped merges | 0 / 0 |

Bluesky per-tenant read (top-3 census tenants), `count`/`by_collection`/`ts_window` medians: ~9 / ~11 / ~15 ms.

hits read, `--files 1` (7 tenants; heavy=508k rows, fan-out 2 per the overlap caveat), warm median ms:

| scenario | heavy | median | light |
|---|---|---|---|
| count | 13.5 | 13.8 | 13.3 |
| adv_filter | 15.2 | 12.1 | 15.8 |
| distinct_users | 19.2 | 14.2 | 18.1 |
| top_urls | 50.7 | 15.0 | 35.9 |
| search_phrases | 50.0 | 18.0 | 21.6 |
| ts_window | 18.9 | 15.9 | 14.8 |
| minutely | 18.7 | 15.9 | 15.5 |

### 10 GB tier (hits full, bluesky 10M) — PENDING

Run per the runbook on a box with the disk headroom above; paste the
`hits-queries-baseline.json` medians and the `bsky-run-baseline.json` write-path
numbers here with the machine spec and git SHA. The harness is smoke-verified;
the full-tier numbers are a data-collection step, not a code change.

### 100M / 1B tiers — optional / stretch

100M Bluesky is a documented long run; 1B needs `--wave-files` and hours.

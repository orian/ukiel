#!/usr/bin/env bash
# Run the ukiel macro benches (plan 30) under a label and compare against a
# previous run — no LLM needed. See bench/README.md.
#
# Usage:
#   bench/bench.sh LABEL [BASELINE] [options]
#
# Records results as bench/results/{hits-queries,bsky-run}-LABEL.json, then (if
# BASELINE given) prints a terminal delta vs those labels and regenerates
# bench/results/report.html.
#
# Options:
#   --skip-hits        skip hits load + query suite
#   --skip-bluesky     skip the bluesky write-path run
#   --files N          bluesky files (default 10)
#   --iters N          hits query iterations (default 10)
#   --no-native        drop -C target-cpu=native (portable build)
#   --no-reset         reuse the running stack (assumes it is clean)
#
# Typical A/B workflow:
#   git stash && bench/bench.sh before                # baseline on old code
#   git stash pop && bench/bench.sh after before      # candidate, prints delta
set -euo pipefail
cd "$(dirname "$0")/.."

LABEL="${1:-}"; [ -n "$LABEL" ] || { echo "usage: bench/bench.sh LABEL [BASELINE] [opts]"; exit 1; }
BASELINE=""
[ "${2:-}" ] && [[ "${2:-}" != --* ]] && BASELINE="$2"

SKIP_HITS=0; SKIP_BLUESKY=0; FILES=10; ITERS=10; NATIVE=1; RESET=1
for a in "$@"; do case "$a" in
  --skip-hits) SKIP_HITS=1;; --skip-bluesky) SKIP_BLUESKY=1;;
  --files) shift_next=FILES;; --iters) shift_next=ITERS;;
  --no-native) NATIVE=0;; --no-reset) RESET=0;;
  *) if [ "${shift_next:-}" ]; then eval "$shift_next=$a"; unset shift_next; fi;;
esac; done

[ "$NATIVE" = 1 ] && export RUSTFLAGS="-C target-cpu=native"
BIN=(cargo run --release -j "$(nproc)" -p ukiel-e2e --bin bench --)
HITS_PRESENT=$(ls bench/datasets/hits/*.parquet 2>/dev/null | wc -l)
BSKY_PRESENT=$(ls bench/datasets/bluesky/*.json.gz 2>/dev/null | wc -l)

echo ">> building bench (release${RUSTFLAGS:+, $RUSTFLAGS})"
cargo build --release -j "$(nproc)" -p ukiel-e2e --bin bench

if [ "$RESET" = 1 ]; then
  echo ">> resetting compose stack (down -v && up)"
  docker compose down -v >/dev/null 2>&1 || true
  docker compose up -d --wait
fi

if [ "$SKIP_HITS" = 0 ] && [ "$HITS_PRESENT" -gt 0 ]; then
  echo ">> hits load ($HITS_PRESENT files)"; "${BIN[@]}" hits load
  echo ">> hits queries --label $LABEL"; "${BIN[@]}" hits queries --iters "$ITERS" --label "$LABEL"
elif [ "$SKIP_HITS" = 0 ]; then
  echo "!! no hits dataset (run: make bench-fetch-hits) — skipping hits"
fi

if [ "$SKIP_BLUESKY" = 0 ] && [ "$BSKY_PRESENT" -gt 0 ]; then
  echo ">> bluesky run --files $FILES --label $LABEL"; "${BIN[@]}" bluesky run --files "$FILES" --label "$LABEL"
elif [ "$SKIP_BLUESKY" = 0 ]; then
  echo "!! no bluesky dataset (run: make bench-fetch-bluesky) — skipping bluesky"
fi

echo ">> regenerating bench/results/report.html"
python3 bench/report.py html

if [ -n "$BASELINE" ]; then
  echo ">> comparing $BASELINE → $LABEL"
  python3 bench/report.py compare "$BASELINE" "$LABEL"
else
  echo ">> no baseline given; run again with a baseline label to see a delta:"
  echo "   bench/bench.sh <new-label> $LABEL"
fi

#!/usr/bin/env bash
# Plan 49: execute ONE declared parquet-performance schedule entry.
#
# This script contains no writer/reader/query logic of its own — it dispatches to the
# single-purpose offline tools (parquet-write-bench, file-read-bench, parquet-scan-bench,
# parquet-lab-bench) by the entry's layer, then publishes the report through a temporary file
# and an atomic rename. It refuses an existing or partially-written destination so a rerun
# can never silently overwrite or resume onto a half-finished report.
set -euo pipefail

usage() {
  cat >&2 <<'EOF'
usage: parquet-perf.sh --layer <writer|raw_read|scan|sql> \
                       --manifest ARTIFACT.json --out REPORT.json \
                       [--workload W.json] [--role R] [--selection S] \
                       [--plan P] [--receipt CACHE.json] [--suite SUITE.json] \
                       [--config CONFIG.json] [--samples N]
Executes exactly one measurement and atomically publishes REPORT.json.
EOF
  exit 2
}

LAYER="" MANIFEST="" OUT="" WORKLOAD="" ROLE="" SELECTION="" PLAN="" RECEIPT="" SUITE="" CONFIG="" SAMPLES=7
while [ $# -gt 0 ]; do
  case "$1" in
    --layer) LAYER="$2"; shift 2;;
    --manifest) MANIFEST="$2"; shift 2;;
    --out) OUT="$2"; shift 2;;
    --workload) WORKLOAD="$2"; shift 2;;
    --role) ROLE="$2"; shift 2;;
    --selection) SELECTION="$2"; shift 2;;
    --plan) PLAN="$2"; shift 2;;
    --receipt) RECEIPT="$2"; shift 2;;
    --suite) SUITE="$2"; shift 2;;
    --config) CONFIG="$2"; shift 2;;
    --samples) SAMPLES="$2"; shift 2;;
    -h|--help) usage;;
    *) echo "error: unknown argument '$1'" >&2; usage;;
  esac
done

[ -n "$LAYER" ] && [ -n "$MANIFEST" ] && [ -n "$OUT" ] || usage
[ -f "$MANIFEST" ] || { echo "error: manifest '$MANIFEST' not found" >&2; exit 1; }

# Refuse an existing or partially-written destination. A rerun must target a fresh path.
if [ -e "$OUT" ]; then
  echo "error: destination '$OUT' already exists; refusing to overwrite a report" >&2
  exit 1
fi
TMP="${OUT}.partial.$$"
if [ -e "$TMP" ]; then
  echo "error: partial output '$TMP' exists; a prior run did not finish cleanly" >&2
  exit 1
fi
mkdir -p "$(dirname "$OUT")"
# Clean up a partial file on any failure so the next run sees a clean slate.
trap 'rm -f "$TMP"' EXIT

receipt_arg=()
[ -n "$RECEIPT" ] && receipt_arg=(--receipt "$RECEIPT")

case "$LAYER" in
  writer)
    [ -n "$CONFIG" ] || { echo "error: writer layer needs --config" >&2; exit 2; }
    parquet-write-bench --artifact "$MANIFEST" --config "$CONFIG" --samples "$SAMPLES" --report "$TMP"
    ;;
  raw_read)
    [ -n "$PLAN" ] || { echo "error: raw_read layer needs --plan" >&2; exit 2; }
    file-read-bench --manifest "$MANIFEST" --plan "$PLAN" --samples "$SAMPLES" \
      "${receipt_arg[@]}" --report "$TMP"
    ;;
  scan)
    [ -n "$WORKLOAD" ] && [ -n "$ROLE" ] && [ -n "$SELECTION" ] || {
      echo "error: scan layer needs --workload --role --selection" >&2; exit 2; }
    parquet-scan-bench --manifest "$MANIFEST" --workload "$WORKLOAD" --role "$ROLE" \
      --selection "$SELECTION" --samples "$SAMPLES" "${receipt_arg[@]}" --report "$TMP"
    ;;
  sql)
    [ -n "$SUITE" ] || { echo "error: sql layer needs --suite" >&2; exit 2; }
    cache_arg=()
    [ -n "$RECEIPT" ] && cache_arg=(--cache-receipt "$RECEIPT")
    parquet-lab-bench run --manifest "$MANIFEST" --suite "$SUITE" --result "$TMP" \
      --mode local "${cache_arg[@]}"
    ;;
  *)
    echo "error: unknown layer '$LAYER'" >&2; exit 2;;
esac

# Atomic publish: the report appears at OUT only once it is fully written.
mv -n "$TMP" "$OUT"
trap - EXIT
echo "published $OUT"

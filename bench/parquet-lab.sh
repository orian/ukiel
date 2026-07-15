#!/usr/bin/env bash
# Plan 47: orchestrate ONE block of the parquet storage laboratory matrix.
#
# It implements no snapshot, rewrite, index, or query logic. It invokes the public CLIs of
# the laboratory tools in order, against an existing immutable snapshot and a compiled
# suite:
#
#   parquet-census      (the product control: original snapshot bytes)
#   parquet-lab-bench   (the product control: queries over the original bytes)
#   for each variant spec in the block:
#     parquet-rewrite   (one explicit spec -> one verified variant)
#     parquet-census    (what the writer actually did)
#     parquet-lab-bench (queries + I/O over the variant, answer-checked vs the control)
#
# Every block reruns the product control, so machine drift stays visible. The script runs
# ONE block; the whole matrix is several invocations, one per block directory. Selection,
# noise banding, and dominated/Pareto labelling are computed from the raw reports later —
# this script never mutates a spec or picks a winner.
set -euo pipefail
cd "$(dirname "$0")/.."
REPO_ROOT="$(pwd -P)"

usage() {
  cat <<'USAGE'
Usage: bench/parquet-lab.sh \
  --snapshot SNAPSHOT_DIR --suite SUITE.json --block BLOCK_DIR --out RUN_DIR \
  [--mode local|object-store] [--warm N] [--cold N] [--replace]

  SNAPSHOT_DIR   directory holding an immutable snapshot's manifest.json (the control)
  SUITE.json     a suite compiled from the control by `parquet-lab-bench compile`
  BLOCK_DIR      directory of variant *.toml specs, each changing ONE axis from the control
  RUN_DIR        fresh output directory for variants, censuses, and results
USAGE
}

# --- Argument parsing and validation. Everything here happens BEFORE any tool runs, so a
# --- bad invocation is refused without touching a file or launching a benchmark.
SNAP="" SUITE="" BLOCK="" OUT="" MODE="local" WARM=5 COLD=1 REP=0 REPLACE=0
while [[ $# -gt 0 ]]; do
  case "$1" in
    --snapshot) SNAP="${2:-}"; shift 2;;
    --suite) SUITE="${2:-}"; shift 2;;
    --block) BLOCK="${2:-}"; shift 2;;
    --out) OUT="${2:-}"; shift 2;;
    --mode) MODE="${2:-}"; shift 2;;
    --warm) WARM="${2:-}"; shift 2;;
    --cold) COLD="${2:-}"; shift 2;;
    --rep) REP="${2:-}"; shift 2;;
    --replace) REPLACE=1; shift;;
    -h|--help) usage; exit 0;;
    *) echo "error: unknown argument '$1'" >&2; usage; exit 2;;
  esac
done

die() { echo "error: $*" >&2; exit 2; }
MARKER=".parquet-lab-marker"

[[ -n "$SNAP"  ]] || die "missing --snapshot"
[[ -n "$SUITE" ]] || die "missing --suite"
[[ -n "$BLOCK" ]] || die "missing --block"
[[ -n "$OUT"   ]] || die "missing --out"
[[ -f "$SNAP/manifest.json" ]] || die "no snapshot manifest at '$SNAP/manifest.json'"
[[ -f "$SUITE" ]] || die "suite '$SUITE' does not exist"
[[ -d "$BLOCK" ]] || die "block directory '$BLOCK' does not exist"
case "$MODE" in memory|local|object-store) ;; *) die "invalid --mode '$MODE' (memory|local|object-store)";; esac

shopt -s nullglob
SPECS=("$BLOCK"/*.toml)
[[ ${#SPECS[@]} -gt 0 ]] || die "block '$BLOCK' holds no *.toml variant specs"

# --- Safe output handling. A run publishes into $OUT only by an atomic rename of a fresh
# --- temp sibling. --replace removes an EXISTING output only when it is a real directory
# --- carrying this laboratory's marker — never a symlink, /, ., the repo root, or an
# --- unmarked directory someone might be storing real data in.
OUT_ABS="$(cd "$(dirname "$OUT")" 2>/dev/null && pwd -P)/$(basename "$OUT")" || die "invalid --out path '$OUT'"
case "$OUT_ABS" in
  "/"|"$REPO_ROOT"|"") die "refusing to write the repository root or filesystem root as --out";;
esac
[[ "$OUT" == *".."* ]] && die "refusing a traversing --out path '$OUT'"
if [[ -L "$OUT" ]]; then die "refusing to replace a symlink '$OUT'"; fi
if [[ -e "$OUT" ]]; then
  if [[ $REPLACE -eq 0 ]]; then die "output '$OUT' already exists; pass --replace"; fi
  [[ -d "$OUT" ]] || die "refusing to --replace a non-directory '$OUT'"
  [[ -f "$OUT/$MARKER" ]] || die "refusing to --replace '$OUT': it lacks the laboratory marker (not a laboratory run directory)"
fi

# Build into a fresh temp sibling; on any failure the trap records a failed state and leaves
# the temp tree for inspection. "block complete" is impossible until every report parses.
TMP="${OUT}.tmp-$$"
rm -rf "$TMP"
mkdir -p "$TMP/variants" "$TMP/census" "$TMP/bench"
printf 'block=%s\nmode=%s\nrep=%s\n' "$BLOCK" "$MODE" "$REP" > "$TMP/$MARKER"

published=0
cleanup() {
  local code=$?
  if [[ $published -eq 0 ]]; then
    echo "state=failed" >> "$TMP/$MARKER" 2>/dev/null || true
    echo "run FAILED (exit $code); temp tree left at $TMP" >&2
  fi
}
trap cleanup EXIT

run_tool() { # run_tool <package> [args...]
  local pkg="$1"; shift
  if [[ -n "${PARQUET_LAB_BIN:-}" ]]; then
    "$PARQUET_LAB_BIN/$pkg" "$@"
  else
    cargo run -q -p "$pkg" -- "$@"
  fi
}

# Fail if a report file is missing or not valid JSON — a benchmark claim requires a report
# that parses and binds.
require_report() {
  local f="$1"
  [[ -f "$f" ]] || die "expected report '$f' was not produced"
  python3 -c "import json,sys; json.load(open(sys.argv[1]))" "$f" || die "report '$f' is not valid JSON"
}

echo "== product control (original snapshot bytes), rep $REP =="
run_tool parquet-census --manifest "$SNAP/manifest.json" --report "$TMP/census/product-control.json" --replace
run_tool parquet-lab-bench run --manifest "$SNAP/manifest.json" --suite "$SUITE" \
  --result "$TMP/bench/product-control.json" --mode "$MODE" --cold-iters "$COLD" --warm-iters "$WARM" \
  --run-order 0 --replace
require_report "$TMP/bench/product-control.json"

order=1
for spec in "${SPECS[@]}"; do
  label="$(basename "$spec" .toml)"
  echo "== variant: $label =="
  vdir="$TMP/variants/$label"
  run_tool parquet-rewrite --manifest "$SNAP/manifest.json" --spec "$spec" --output "$vdir" --replace
  run_tool parquet-census --manifest "$vdir/manifest.json" --report "$TMP/census/$label.json" --replace
  run_tool parquet-lab-bench run --manifest "$vdir/manifest.json" --suite "$SUITE" \
    --result "$TMP/bench/$label.json" --mode "$MODE" --cold-iters "$COLD" --warm-iters "$WARM" \
    --run-order "$order" --replace
  require_report "$TMP/bench/$label.json"
  order=$((order+1))
done

# Everything produced and parsed: publish atomically.
echo "state=complete" >> "$TMP/$MARKER"
if [[ -e "$OUT" ]]; then rm -rf "$OUT"; fi
mv "$TMP" "$OUT"
published=1
echo "block complete: $OUT (${#SPECS[@]} variants + product control, mode=$MODE, rep=$REP)"

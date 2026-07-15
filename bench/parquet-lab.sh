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
set -uo pipefail
cd "$(dirname "$0")/.."

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
SNAP="" SUITE="" BLOCK="" OUT="" MODE="local" WARM=5 COLD=1 REPLACE=0
while [[ $# -gt 0 ]]; do
  case "$1" in
    --snapshot) SNAP="${2:-}"; shift 2;;
    --suite) SUITE="${2:-}"; shift 2;;
    --block) BLOCK="${2:-}"; shift 2;;
    --out) OUT="${2:-}"; shift 2;;
    --mode) MODE="${2:-}"; shift 2;;
    --warm) WARM="${2:-}"; shift 2;;
    --cold) COLD="${2:-}"; shift 2;;
    --replace) REPLACE=1; shift;;
    -h|--help) usage; exit 0;;
    *) echo "error: unknown argument '$1'" >&2; usage; exit 2;;
  esac
done

die() { echo "error: $*" >&2; exit 2; }

[[ -n "$SNAP"  ]] || die "missing --snapshot"
[[ -n "$SUITE" ]] || die "missing --suite"
[[ -n "$BLOCK" ]] || die "missing --block"
[[ -n "$OUT"   ]] || die "missing --out"
[[ -f "$SNAP/manifest.json" ]] || die "no snapshot manifest at '$SNAP/manifest.json'"
[[ -f "$SUITE" ]] || die "suite '$SUITE' does not exist"
[[ -d "$BLOCK" ]] || die "block directory '$BLOCK' does not exist"
case "$MODE" in local|object-store) ;; *) die "invalid --mode '$MODE' (local|object-store)";; esac

shopt -s nullglob
SPECS=("$BLOCK"/*.toml)
[[ ${#SPECS[@]} -gt 0 ]] || die "block '$BLOCK' holds no *.toml variant specs"

if [[ -e "$OUT" && $REPLACE -eq 0 ]]; then
  die "output '$OUT' already exists; pass --replace to overwrite"
fi

# All validated. Now the run directory and the tool invocations.
rm -rf "$OUT"
mkdir -p "$OUT/variants" "$OUT/census" "$OUT/bench"

# The laboratory tools, run from the workspace. Override PARQUET_LAB_RUN to point at
# installed release binaries for a real matrix (e.g. 'command' with binaries on PATH).
run_tool() { # run_tool <package> [args...]
  local pkg="$1"; shift
  if [[ -n "${PARQUET_LAB_BIN:-}" ]]; then
    "$PARQUET_LAB_BIN/$pkg" "$@"
  else
    cargo run -q -p "$pkg" -- "$@"
  fi
}

echo "== product control (original snapshot bytes) =="
run_tool parquet-census --manifest "$SNAP/manifest.json" --report "$OUT/census/product-control.json" --replace
run_tool parquet-lab-bench run --manifest "$SNAP/manifest.json" --suite "$SUITE" \
  --result "$OUT/bench/product-control.json" --mode "$MODE" --cold-iters "$COLD" --warm-iters "$WARM" \
  --run-order 0 --replace

order=1
for spec in "${SPECS[@]}"; do
  label="$(basename "$spec" .toml)"
  echo "== variant: $label =="
  vdir="$OUT/variants/$label"
  run_tool parquet-rewrite --manifest "$SNAP/manifest.json" --spec "$spec" --output "$vdir" --replace
  run_tool parquet-census --manifest "$vdir/manifest.json" --report "$OUT/census/$label.json" --replace
  run_tool parquet-lab-bench run --manifest "$vdir/manifest.json" --suite "$SUITE" \
    --result "$OUT/bench/$label.json" --mode "$MODE" --cold-iters "$COLD" --warm-iters "$WARM" \
    --run-order "$order" --replace
  order=$((order+1))
done

echo "block complete: $OUT (${#SPECS[@]} variants + product control, mode=$MODE)"

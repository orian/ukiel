#!/usr/bin/env bash
# Plan 46: orchestrate ONE placement arm of the actual part-shape experiment.
#
# It does not implement generation, loading, compaction, or measurement — it invokes the
# public CLIs of the four tools in order, against ONE explicitly disposable stack:
#
#   compaction-input  (load the staged L0 as real L0 input)
#   ukield             (compactor-only, the real leased/fenced compactor)
#   wait-compacted     (poll until one final run per partition)
#   part-shape         (scan the final objects)
#   admission          (unvacuumed) --> operator VACUUM --> admission (vacuumed)
#   queries            (compacted equivalence)
#
# It starts and stops exactly the ONE compactor process it launches, and preserves every
# raw report even on failure or Ctrl-C. It does NOT run VACUUM: that is an explicit
# operator step, prompted for here, so a post-vacuum number is never produced by accident.
#
# The whole matrix is several invocations of this script, one per (tier, placement) arm,
# each against its own reset stack. This script runs ONE arm.
set -uo pipefail
cd "$(dirname "$0")/.."

usage() {
  cat <<'USAGE'
Usage: bench/prod-synth-part-shape.sh \
  --l0-manifest FILE --placement packed|separated|size-targeted \
  [--target-file-mb N] --label L --config FILE --out DIR --ephemeral \
  [--noninteractive-vacuum]

Runs one placement arm. --ephemeral is mandatory. --target-file-mb is required only for
size-targeted and rejected otherwise. --noninteractive-vacuum skips the operator VACUUM
prompt (CI only: it runs the unvacuumed phase and stops, since no one can VACUUM).
USAGE
}

L0_MANIFEST=""; PLACEMENT=""; TARGET_MB=""; LABEL=""; CONFIG=""; OUT=""
EPHEMERAL=0; NONINTERACTIVE_VACUUM=0
while [[ $# -gt 0 ]]; do
  case "$1" in
    --l0-manifest) L0_MANIFEST="$2"; shift 2;;
    --placement) PLACEMENT="$2"; shift 2;;
    --target-file-mb) TARGET_MB="$2"; shift 2;;
    --label) LABEL="$2"; shift 2;;
    --config) CONFIG="$2"; shift 2;;
    --out) OUT="$2"; shift 2;;
    --ephemeral) EPHEMERAL=1; shift;;
    --noninteractive-vacuum) NONINTERACTIVE_VACUUM=1; shift;;
    -h|--help) usage; exit 0;;
    *) echo "unknown argument: $1" >&2; usage; exit 2;;
  esac
done

# --- Argument validation, before any mutation ---
die() { echo "prod-synth-part-shape: $*" >&2; exit 2; }
[[ -n "$L0_MANIFEST" ]] || die "missing --l0-manifest"
[[ -n "$LABEL" ]]       || die "missing --label"
[[ -n "$CONFIG" ]]      || die "missing --config"
[[ -n "$OUT" ]]         || die "missing --out"
[[ "$EPHEMERAL" == 1 ]] || die "refusing to run without --ephemeral: this mutates a stack the compactor then rewrites"
[[ -f "$L0_MANIFEST" ]] || die "no such L0 manifest: $L0_MANIFEST"
[[ -f "$CONFIG" ]]      || die "no such config: $CONFIG"

case "$PLACEMENT" in
  packed|separated)
    [[ -z "$TARGET_MB" ]] || die "--target-file-mb is only valid with --placement size-targeted";;
  size-targeted)
    [[ -n "$TARGET_MB" ]] || die "--placement size-targeted requires --target-file-mb";;
  *) die "invalid --placement '$PLACEMENT' (packed | separated | size-targeted)";;
esac

# The default compose project is not disposable. Guard against pointing at it by accident:
# the config's catalog URL must not be the primary 'postgres' database.
if grep -qE 'url *= *"postgres://[^"]*/postgres"' "$CONFIG"; then
  die "config points at the default 'postgres' database — use a disposable database, not the primary"
fi

if [[ -e "$OUT" ]]; then
  die "output directory $OUT already exists; pick a fresh --out so a prior arm's reports are never overwritten"
fi
mkdir -p "$OUT"

LOAD=./target/release/ukiel-prod-load
BENCH=./target/release/ukiel-prod-bench
UKIELD=./target/release/ukield
for bin in "$LOAD" "$BENCH" "$UKIELD"; do
  [[ -x "$bin" ]] || die "missing $bin — run: cargo build --release -p ukiel-prod-load -p ukiel-prod-bench -p ukield"
done

RECEIPT="$OUT/receipt.json"
COMPACTOR_LOG="$OUT/compactor.log"
COMPACTOR_PID=""

# --- Cleanup: stop ONLY the compactor we started; keep every report ---
cleanup() {
  if [[ -n "$COMPACTOR_PID" ]] && kill -0 "$COMPACTOR_PID" 2>/dev/null; then
    echo ">> stopping compactor (pid $COMPACTOR_PID)"
    kill "$COMPACTOR_PID" 2>/dev/null || true
    for _ in $(seq 1 20); do kill -0 "$COMPACTOR_PID" 2>/dev/null || break; sleep 0.2; done
    kill -9 "$COMPACTOR_PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT INT TERM

TARGET_FLAG=()
[[ "$PLACEMENT" == "size-targeted" ]] && TARGET_FLAG=(--target-file-mb "$TARGET_MB")

echo ">> [1/6] load staged L0 as compaction input ($PLACEMENT)"
"$LOAD" compaction-input \
  --l0-manifest "$L0_MANIFEST" --label "$LABEL" --config "$CONFIG" \
  --receipt "$RECEIPT" --placement "$PLACEMENT" "${TARGET_FLAG[@]}" --ephemeral \
  || die "compaction-input load failed"

echo ">> [2/6] launch the compactor-only ukield"
"$UKIELD" --config "$CONFIG" > "$COMPACTOR_LOG" 2>&1 &
COMPACTOR_PID=$!
sleep 2
kill -0 "$COMPACTOR_PID" 2>/dev/null || die "compactor exited immediately; see $COMPACTOR_LOG"

echo ">> [3/6] wait for convergence"
"$BENCH" wait-compacted --receipt "$RECEIPT" --config "$CONFIG" --timeout-secs "${WAIT_TIMEOUT:-1800}" \
  || die "the fixture never converged; see $COMPACTOR_LOG"

echo ">> stopping the compactor before measuring (an idle benchmark is a stable one)"
cleanup
COMPACTOR_PID=""

echo ">> [4/6] scan the final part shape"
"$BENCH" part-shape --receipt "$RECEIPT" --config "$CONFIG" --result "$OUT/part-shape.json" \
  || die "part-shape scan failed"

echo ">> [5/6] admission A/B, unvacuumed phase"
"$BENCH" admission --receipt "$RECEIPT" --config "$CONFIG" --phase unvacuumed \
  --result "$OUT/admission-unvacuumed.json" \
  --workers "${WORKERS:-16}" --warmup-secs "${WARMUP:-5}" --duration-secs "${DURATION:-30}" \
  || die "unvacuumed admission failed"

# --- The explicit VACUUM step. Never performed by the benchmark. ---
if [[ "$NONINTERACTIVE_VACUUM" == 1 ]]; then
  echo ">> [6/6] --noninteractive-vacuum: skipping the vacuumed phase and the compacted queries."
  echo "   The vacuumed phase needs an operator 'VACUUM (ANALYZE) parts'; a CI run cannot issue it."
  echo ">> unvacuumed arm complete. Reports in $OUT"
  exit 0
fi

cat <<PROMPT

  >> Now run, as an operator, against this arm's catalog:

       VACUUM (ANALYZE) parts;

     Fresh REPLACE churn can clear visibility-map bits, so the unvacuumed and vacuumed
     numbers can differ materially — that difference is a RESULT, not noise to hide. The
     benchmark will not run VACUUM for you.

PROMPT
read -r -p "  Press Enter once VACUUM has completed (or Ctrl-C to stop with only the unvacuumed phase): " _

echo ">> admission A/B, vacuumed phase"
"$BENCH" admission --receipt "$RECEIPT" --config "$CONFIG" --phase vacuumed \
  --result "$OUT/admission-vacuumed.json" \
  --workers "${WORKERS:-16}" --warmup-secs "${WARMUP:-5}" --duration-secs "${DURATION:-30}" \
  || die "vacuumed admission failed"

echo ">> [6/6] compacted query equivalence"
"$BENCH" queries --receipt "$RECEIPT" --config "$CONFIG" --result "$OUT/queries.json" --iters "${ITERS:-5}" \
  || die "compacted queries failed (a scoped/raw disagreement is a correctness failure)"

cat <<DONE

>> Arm complete: $PLACEMENT / $LABEL
   Reports in $OUT:
     receipt.json               the load identity
     part-shape.json            exact key cardinality, filter coverage, storage
     admission-unvacuumed.json  range vs filtered, before VACUUM
     admission-vacuumed.json    range vs filtered, after VACUUM
     queries.json               scoped == raw, after compaction

   Reset this disposable stack before the next arm.
DONE

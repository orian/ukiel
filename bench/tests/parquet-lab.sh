#!/usr/bin/env bash
# Argument- and refusal-behaviour tests for bench/parquet-lab.sh.
#
# These need NO services and NO build: they prove the orchestrator refuses every unsafe or
# meaningless invocation BEFORE it would rewrite, census, or benchmark anything, and that a
# refused run emits no result. The happy path is exercised by the Rust integration tests
# behind each tool and by a smoke matrix run. Plain bash to match bench/'s convention.
#   bench/tests/parquet-lab.sh
set -uo pipefail
cd "$(dirname "$0")/../.."

SCRIPT=bench/parquet-lab.sh
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

pass=0
fail=0

refuses() {
  local desc="$1" pattern="$2"; shift 2
  local out code
  out="$("$SCRIPT" "$@" 2>&1)"; code=$?
  if [[ $code -eq 0 ]]; then
    echo "FAIL: $desc — expected refusal, got success"; fail=$((fail+1)); return
  fi
  if ! grep -qE "$pattern" <<<"$out"; then
    echo "FAIL: $desc — message did not match /$pattern/:"; echo "  $out"; fail=$((fail+1)); return
  fi
  echo "ok: $desc"; pass=$((pass+1))
}

# A valid-looking snapshot dir, suite, and block so validation reaches the check under test.
SNAP="$TMP/snap"; mkdir -p "$SNAP"; echo '{}' > "$SNAP/manifest.json"
SUITE="$TMP/suite.json"; echo '{}' > "$SUITE"
BLOCK="$TMP/block"; mkdir -p "$BLOCK"; echo 'label="x"' > "$BLOCK/a.toml"

refuses "missing --snapshot" "missing --snapshot" \
  --suite "$SUITE" --block "$BLOCK" --out "$TMP/out1"
refuses "missing --suite" "missing --suite" \
  --snapshot "$SNAP" --block "$BLOCK" --out "$TMP/out2"
refuses "missing --block" "missing --block" \
  --snapshot "$SNAP" --suite "$SUITE" --out "$TMP/out3"
refuses "missing --out" "missing --out" \
  --snapshot "$SNAP" --suite "$SUITE" --block "$BLOCK"
refuses "absent snapshot manifest" "no snapshot manifest" \
  --snapshot "$TMP/nope" --suite "$SUITE" --block "$BLOCK" --out "$TMP/out4"
refuses "absent suite" "suite .* does not exist" \
  --snapshot "$SNAP" --suite "$TMP/nope.json" --block "$BLOCK" --out "$TMP/out5"
refuses "absent block dir" "block directory .* does not exist" \
  --snapshot "$SNAP" --suite "$SUITE" --block "$TMP/nope" --out "$TMP/out6"
refuses "empty block dir" "holds no .* specs" \
  --snapshot "$SNAP" --suite "$SUITE" --block "$TMP" --out "$TMP/out7"
refuses "invalid mode" "invalid --mode" \
  --snapshot "$SNAP" --suite "$SUITE" --block "$BLOCK" --out "$TMP/out8" --mode s3
refuses "unknown argument" "unknown argument" \
  --snapshot "$SNAP" --suite "$SUITE" --block "$BLOCK" --out "$TMP/out9" --frobnicate

# Output that already exists is refused without --replace, and NOT clobbered.
EXISTING="$TMP/existing"; mkdir -p "$EXISTING"; echo keep > "$EXISTING/sentinel"
refuses "existing output without --replace" "already exists" \
  --snapshot "$SNAP" --suite "$SUITE" --block "$BLOCK" --out "$EXISTING"
if [[ -f "$EXISTING/sentinel" ]]; then
  echo "ok: refused run left the existing output untouched"; pass=$((pass+1))
else
  echo "FAIL: refused run clobbered existing output"; fail=$((fail+1))
fi

echo
echo "parquet-lab.sh refusal tests: $pass passed, $fail failed"
[[ $fail -eq 0 ]]

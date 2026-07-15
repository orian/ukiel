#!/usr/bin/env bash
# Argument- and refusal-behaviour tests for bench/prod-synth-part-shape.sh.
#
# These need NO services: they prove the script refuses every unsafe or meaningless
# invocation BEFORE it would touch a catalog or an object store, and that a refused run
# leaves no output directory behind. The full happy path is exercised by an operator run
# and by the Rust integration tests behind each tool; this guards the orchestration seam.
#
# Plain bash to match bench/'s convention (bats is not installed in this repo). Run:
#   bench/tests/prod-synth-part-shape.sh
set -uo pipefail
cd "$(dirname "$0")/../.."

SCRIPT=bench/prod-synth-part-shape.sh
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

pass=0
fail=0

# Asserts the script exits non-zero, prints a message matching $2, and leaves no --out dir.
refuses() {
  local desc="$1" pattern="$2"; shift 2
  local out
  out="$("$SCRIPT" "$@" 2>&1)"
  local code=$?
  if [[ $code -eq 0 ]]; then
    echo "FAIL: $desc — expected refusal, got success"; fail=$((fail+1)); return
  fi
  if ! grep -qE "$pattern" <<<"$out"; then
    echo "FAIL: $desc — message did not match /$pattern/:"; echo "  $out"; fail=$((fail+1)); return
  fi
  echo "ok: $desc"; pass=$((pass+1))
}

# A valid-looking L0 manifest and config so validation reaches the check under test.
L0="$TMP/l0.json"; CFG="$TMP/cfg.toml"
echo '{}' > "$L0"
printf 'url = "postgres://postgres:postgres@127.0.0.1:5432/disposable_db"\n' > "$CFG"

refuses "missing --ephemeral" "without --ephemeral" \
  --l0-manifest "$L0" --placement packed --label l --config "$CFG" --out "$TMP/a"

refuses "missing --l0-manifest" "missing --l0-manifest" \
  --placement packed --label l --config "$CFG" --out "$TMP/b" --ephemeral

refuses "nonexistent L0 manifest" "no such L0 manifest" \
  --l0-manifest "$TMP/nope.json" --placement packed --label l --config "$CFG" --out "$TMP/c" --ephemeral

refuses "invalid placement" "invalid --placement" \
  --l0-manifest "$L0" --placement sideways --label l --config "$CFG" --out "$TMP/d" --ephemeral

refuses "--target-file-mb on packed" "only valid with .*size-targeted" \
  --l0-manifest "$L0" --placement packed --target-file-mb 256 --label l --config "$CFG" --out "$TMP/e" --ephemeral

refuses "size-targeted without --target-file-mb" "requires --target-file-mb" \
  --l0-manifest "$L0" --placement size-targeted --label l --config "$CFG" --out "$TMP/f" --ephemeral

# The default compose project's primary database is not disposable.
DEFCFG="$TMP/default.toml"
printf 'url = "postgres://postgres:postgres@127.0.0.1:5432/postgres"\n' > "$DEFCFG"
refuses "default 'postgres' database" "default 'postgres' database" \
  --l0-manifest "$L0" --placement packed --label l --config "$DEFCFG" --out "$TMP/g" --ephemeral

# A pre-existing output directory must not be clobbered.
mkdir -p "$TMP/existing"
refuses "existing --out directory" "already exists" \
  --l0-manifest "$L0" --placement packed --label l --config "$CFG" --out "$TMP/existing" --ephemeral

# No refused run may have created its output directory.
for d in a b c d e f g; do
  if [[ -e "$TMP/$d" ]]; then
    echo "FAIL: refused run created $TMP/$d"; fail=$((fail+1))
  fi
done

echo
echo "prod-synth-part-shape refusal tests: $pass passed, $fail failed"
[[ $fail -eq 0 ]]

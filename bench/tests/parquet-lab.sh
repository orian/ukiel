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

# --- Adversarial execution tests: fail-fast, safe replace, atomic publish. These use FAKE
# --- laboratory binaries (PARQUET_LAB_BIN) so no real build or services are needed.
FAKEBIN="$TMP/fakebin"; mkdir -p "$FAKEBIN"

make_fakes() { # make_fakes <mode: ok|badjson|censusfail>
  local mode="$1"
  # parquet-census: write a report (valid or invalid JSON) at --report.
  cat > "$FAKEBIN/parquet-census" <<CENSUS
#!/usr/bin/env bash
$([[ "$mode" == censusfail ]] && echo 'exit 3')
out=""; while [[ \$# -gt 0 ]]; do [[ "\$1" == --report ]] && out="\$2"; shift; done
[[ -n "\$out" ]] && echo '{"ok":true}' > "\$out"
exit 0
CENSUS
  # parquet-rewrite: create --output/manifest.json.
  cat > "$FAKEBIN/parquet-rewrite" <<'REWRITE'
#!/usr/bin/env bash
out=""; while [[ $# -gt 0 ]]; do [[ "$1" == --output ]] && out="$2"; shift; done
mkdir -p "$out"; echo '{"manifest_version":"ukiel-parquet-variant/v1"}' > "$out/manifest.json"
exit 0
REWRITE
  # parquet-lab-bench: write a report (valid or garbage) at --result.
  cat > "$FAKEBIN/parquet-lab-bench" <<BENCH
#!/usr/bin/env bash
out=""; while [[ \$# -gt 0 ]]; do [[ "\$1" == --result ]] && out="\$2"; shift; done
$([[ "$mode" == badjson ]] && echo 'echo "not json {{{" > "$out"; exit 0')
[[ -n "\$out" ]] && echo '{"identity":{}}' > "\$out"
exit 0
BENCH
  chmod +x "$FAKEBIN"/*
}

BLK2="$TMP/blk"; mkdir -p "$BLK2"; printf 'label="v1"\n' > "$BLK2/v1.toml"

run_script() { PARQUET_LAB_BIN="$FAKEBIN" "$SCRIPT" --snapshot "$SNAP" --suite "$SUITE" --block "$BLK2" "$@"; }

# 1. Happy path publishes atomically with a complete marker.
make_fakes ok
if run_script --out "$TMP/run-ok" --mode memory >/dev/null 2>&1 && grep -q "state=complete" "$TMP/run-ok/.parquet-lab-marker"; then
  echo "ok: successful run publishes a complete marked directory"; pass=$((pass+1))
else echo "FAIL: happy path did not publish a complete run"; fail=$((fail+1)); fi

# 2. A child failure leaves no published output and a failed temp marker.
make_fakes censusfail
if ! run_script --out "$TMP/run-fail" --mode memory >/dev/null 2>&1 && [[ ! -e "$TMP/run-fail" ]] && grep -q "state=failed" "$TMP"/run-fail.tmp-*/.parquet-lab-marker 2>/dev/null; then
  echo "ok: a child failure publishes nothing and records a failed state"; pass=$((pass+1))
else echo "FAIL: child failure was not handled fail-fast"; fail=$((fail+1)); fi
rm -rf "$TMP"/run-fail.tmp-*

# 3. A non-JSON report is refused (no publish).
make_fakes badjson
if ! run_script --out "$TMP/run-badjson" --mode memory >/dev/null 2>&1 && [[ ! -e "$TMP/run-badjson" ]]; then
  echo "ok: a non-JSON report fails the run before publishing"; pass=$((pass+1))
else echo "FAIL: non-JSON report was not caught"; fail=$((fail+1)); fi
rm -rf "$TMP"/run-badjson.tmp-*

# 4. --replace refuses a symlink.
make_fakes ok
ln -s "$TMP" "$TMP/link-out"
if ! run_script --out "$TMP/link-out" --replace >/dev/null 2>&1; then
  echo "ok: --replace refuses a symlink"; pass=$((pass+1))
else echo "FAIL: --replace clobbered a symlink"; fail=$((fail+1)); fi

# 5. --replace refuses an unmarked existing directory.
mkdir -p "$TMP/unmarked"; echo important > "$TMP/unmarked/data"
if ! run_script --out "$TMP/unmarked" --replace >/dev/null 2>&1 && [[ -f "$TMP/unmarked/data" ]]; then
  echo "ok: --replace refuses an unmarked directory and leaves it intact"; pass=$((pass+1))
else echo "FAIL: --replace clobbered an unmarked directory"; fail=$((fail+1)); fi

echo
echo "parquet-lab.sh tests: $pass passed, $fail failed"
[[ $fail -eq 0 ]]

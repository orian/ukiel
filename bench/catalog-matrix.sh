#!/usr/bin/env bash
# Plan 40, Tasks 4+5: the catalog scalability matrix.
#
# CATALOG ONLY. This writes rows to Postgres and calls the catalog API. It never
# creates a Parquet file, never touches the object store, and never runs a
# DataFusion query — the point is to find where the single primary stops meeting
# the SLO, not to re-measure the scan layer.
#
# The Postgres CPU limit is set by THIS script and always restored, including on
# error or Ctrl-C (see the trap). A run whose recorded limit differs from its
# label is invalid and must be discarded.
set -uo pipefail
cd "$(dirname "$0")/.."

B=./target/release/bench
PG=ukiel-postgres-1
PARTS=${PARTS:-25000}
TABLES=${TABLES:-4}
DUR=${DUR:-10}
WARM=${WARM:-3}

# Docker CANNOT unset a CPU limit: `docker update --cpus 0` returns success and
# changes nothing, and `--cpu-quota -1` is refused once NanoCPUs is set. The only
# way back to unlimited is to RECREATE the container. Learned the hard way: a
# earlier version of this script "restored" the limit, and the sections after the
# cores sweep silently ran at 8 cores while labelled unlimited. (The reports were
# still honest — every one records its own `pg_cpu_limit` — which is why the
# mislabelling was caught rather than published.)
restore_cpu() {
  local now
  now=$(docker inspect -f '{{.HostConfig.NanoCpus}}' "$PG" 2>/dev/null || echo 0)
  if [ "$now" != "0" ]; then
    echo "--- restoring postgres to unlimited CPU (recreating the container) ---"
    docker compose up -d --force-recreate postgres >/dev/null 2>&1 || true
    sleep 6
  fi
  docker inspect -f 'postgres NanoCpus now: {{.HostConfig.NanoCpus}} (0 = unlimited)' "$PG" 2>/dev/null || true
}
trap restore_cpu EXIT INT TERM

set_cpu() {
  docker update --cpus "$1" "$PG" >/dev/null
  echo "### postgres CPU limit = $1"
}

seed() { # seed <state>
  $B catalog seed --label "matrix-$1" --tables "$TABLES" --parts "$PARTS" --state "$1" \
    --packing-keys 1000000 2>&1 | grep -E "seeded|verified"
}

echo "================= DEMAND ENVELOPES ================="
for s in steady dashboard-surge backfill; do $B catalog demand --scenario "$s"; done

echo
echo "================= 5.1 CARDINALITY / STATE (reads) ================="
# History vs live vs hot-table concentration, at the same total parts.
for state in fresh mature backlogged; do
  seed "$state"
  for dist in uniform zipf; do
    echo "--- state=$state key-dist=$dist ---"
    $B catalog read --label "s-$state-$dist" --mode closed --workers 32 \
      --duration "$DUR" --warmup "$WARM" --key-dist "$dist" 2>&1 \
      | grep -E "achieved|saturation|rows returned"
  done
done

echo
echo "================= 5.2 FLEET TOPOLOGY (same total connections) ================="
seed mature
for topo in "1 16" "4 16" "8 8" "16 4"; do
  set -- $topo
  echo "--- ${1} pools x ${2} conns = $(( $1 * $2 )) total ---"
  $B catalog read --label "t-${1}x${2}" --mode closed --workers 32 \
    --duration "$DUR" --warmup "$WARM" --pools "$1" --connections-per-pool "$2" 2>&1 \
    | grep -E "achieved|saturation"
done

echo
echo "--- connection surge: does the fleet fit under max_connections? ---"
for p in 2 4 6 8 12; do
  $B catalog connections --pools "$p" --connections-per-pool 16 2>&1 \
    | grep -E "surge:|SIMULTANEOUS|GLOBAL"
done

echo
echo "================= 5.3 CORES x OFFERED LOAD ================="
seed mature
for cores in 1 2 4 8; do
  set_cpu "$cores"
  echo "--- closed-loop knee @ ${cores} cores ---"
  for w in 8 32 64; do
    printf "  workers=%-3s " "$w"
    $B catalog read --label "c${cores}-w${w}" --mode closed --workers "$w" \
      --duration "$DUR" --warmup "$WARM" 2>&1 | grep -E "achieved" | head -1
  done
  echo "--- open-loop around the knee @ ${cores} cores ---"
  for rate in 2000 10000 30000; do
    printf "  rate=%-6s " "$rate"
    $B catalog read --label "c${cores}-r${rate}" --mode open --rate "$rate" --workers 64 \
      --duration "$DUR" --warmup "$WARM" --max-inflight 8192 2>&1 \
      | grep -E "TIMED OUT|achieved" | tr '\n' ' '; echo
  done
done
restore_cpu

echo
echo "================= 5.4 WRITE SHAPE + CONFLICTS ================="
for ppc in 1 4 16; do
  seed mature >/dev/null
  echo "--- ${ppc} parts/commit ---"
  $B catalog write --label "w-ppc$ppc" --mode closed --workers 16 \
    --duration "$DUR" --warmup "$WARM" --parts-per-commit "$ppc" 2>&1 \
    | grep -E "achieved|saturation|catalog grew"
done

for shape in same-partition zipf spread; do
  for n in 2 8 32; do
    seed mature >/dev/null
    printf -- "--- conflict shape=%-15s contenders=%-3s " "$shape" "$n"
    $B catalog conflict --label "cf-$shape-$n" --mode closed --workers "$n" \
      --duration "$DUR" --warmup "$WARM" --shape "$shape" 2>&1 \
      | grep -E "useful swaps/s" | head -1
  done
done

echo
echo "================= 5.5 MIXED PRODUCT ENVELOPES ================="
for s in steady dashboard-surge backfill; do
  seed mature >/dev/null
  echo "--- scenario=$s ---"
  $B catalog mixed --label "mx-$s" --mode closed --workers 32 \
    --duration "$DUR" --warmup "$WARM" --scenario "$s" 2>&1 \
    | grep -E "achieved|saturation|mix:|PASS|FAIL|!"
done

echo
echo "matrix complete; postgres CPU restored to unlimited"

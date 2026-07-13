#!/usr/bin/env bash
# Issue 0011 — the million-logical-table catalog proof.
#
# Plan 40 concluded that one PostgreSQL primary is nowhere near the catalog wall
# for a million-tenant deployment. The throughput numbers were real; the fixture
# was not the claim. It had 25k parts, four hypertables and **zero logical
# tables**, and its read workload resolved one hypertable *once* before the run
# and then looped on `live_parts_pruned` — the one call in admission whose cost
# does not grow with tenant count.
#
# This script seeds the metadata cardinality the claim is actually about and
# measures the whole admission path through it. Every point is run twice, and
# both runs are kept: a headline with one sample is an anecdote.
#
# Usage:  bench/run-issue-0011.sh [10000 1000000 10000000]
# Needs:  docker compose up -d postgres   (~20 GB free for the 10M point)
set -euo pipefail

cd "$(dirname "$0")/.."
BENCH=./target/release/bench
POINTS=("${@:-10000 1000000 10000000}")
read -r -a POINTS <<< "${POINTS[*]}"

# The P3 shape (performance spec): a million tenants, thousands of hypertables.
NAMESPACES=1000000        # == packing keys: the namespace id IS the slice
HYPERTABLES=2000
STATE=mature              # the intended steady state: large history, small live set
DUR=20
WARM=5
WORKERS=16

cargo build --release -p ukiel-e2e --bin bench

pg_restart() {
  # A cold cache is not a label, it is a state. Restarting Postgres empties its
  # shared buffers; the OS page cache survives, so every cold report carries its
  # *measured* buffer-hit ratio and the reader can see how cold it really was.
  docker compose restart postgres >/dev/null
  until docker compose exec -T postgres pg_isready -q 2>/dev/null; do sleep 0.5; done
  sleep 2
}

for PARTS in "${POINTS[@]}"; do
  P=$(printf '%s' "$PARTS" | sed 's/000000$/M/; s/000$/k/')
  echo "############ parts=$PARTS ($P) ############"

  $BENCH catalog seed --label "p3-$P" \
    --tables $HYPERTABLES --parts "$PARTS" --state $STATE \
    --packing-keys $NAMESPACES --namespaces $NAMESPACES \
    --target-fanout 7

  # The plans, before the numbers. A latency says what happened; only the plan
  # says why, and only the plan survives a change of machine.
  $BENCH catalog explain --label "p3-$P" --packing-keys $NAMESPACES

  common="--duration $DUR --warmup $WARM --workers $WORKERS --packing-keys $NAMESPACES"

  # (1) Parts-only — plan 40's workload, kept so the metadata cost is *separated*
  #     rather than hidden inside one number.
  #
  #     It probes the HOT hypertable, so its key space is that table's own tenant
  #     slice, not all million keys. Getting this wrong is not a small error: a key
  #     a hypertable does not hold still satisfies the index's leading `kmin <= k`
  #     bound, so the scan walks every live part before returning nothing — the
  #     first run of this measured 1,070 op/s and looked like a scaling wall.
  KEYS_PER_TABLE=$((NAMESPACES / HYPERTABLES))
  for rep in 1 2; do
    $BENCH catalog read --label "p3-$P-partsonly-rep$rep" \
      --duration $DUR --warmup $WARM --workers $WORKERS \
      --packing-keys $KEYS_PER_TABLE --key-dist zipf
  done

  # (1b) Cost vs the tenant's key RANK inside its hypertable. `live_parts_pruned`
  #      is a range-containment query, and a btree can only use its leading bound —
  #      so the scan grows with where the tenant sits in the key space, not with how
  #      many parts actually hold its rows.
  $BENCH catalog keyrank --label "p3-$P" --table bench_cat_0 --keys-per-table $KEYS_PER_TABLE

  # (2) Catalog-only admission: list_logical_tables + get_logical_table +
  #     get_hypertable_by_id + live_parts_pruned. The four round trips a request
  #     makes, without DataFusion — this is the tier that answers "does the
  #     *catalog* scale to a million tenants".
  $BENCH catalog admission --label "p3-$P-zipf"    --tier catalog --probe key      $common --key-dist zipf    --reps 2
  $BENCH catalog admission --label "p3-$P-uniform" --tier catalog --probe key      $common --key-dist uniform --reps 2
  $BENCH catalog admission --label "p3-$P-time"    --tier catalog --probe key-time $common --key-dist zipf    --reps 2

  # (3) Full admission: + the SessionContext build and the physical plan. This is
  #     what a request costs; (2) is what the database costs. They have different
  #     walls, and reporting only one of them is how a system gets a capacity
  #     number it cannot spend.
  $BENCH catalog admission --label "p3-$P-full" --tier session --probe key $common --key-dist zipf --reps 2

  # (4) Cold: shared buffers emptied before each rep. The mature state is 90%
  #     tombstoned history — the working set is small *if* the indexes stay warm,
  #     and this is the point where that assumption is checked rather than made.
  for rep in 1 2; do
    pg_restart
    $BENCH catalog admission --label "p3-$P-cold-rep$rep" --tier catalog --probe key $common --key-dist zipf
  done
done

echo
echo "reports: bench/results/  (gitignored — check in the manifest, not the raw JSON)"
echo "next:    bench/manifest-issue-0011.sh   to record what supports the tables"

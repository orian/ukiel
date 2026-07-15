# ukiel-prod-bench — the benchmark runbook

Measures a `prod-synth` fixture that has **already been loaded** by `ukiel-prod-load`.

**Read-only, and enforced.** It cannot generate a fixture, upload an object, create a
table, mutate a catalog, or clean anything up. Generation and loading are facts it reads
from the manifest and the catalog, not actions it may take — which is what makes the
numbers it prints reproducible. A benchmark runner that can repair its own inputs will
eventually repair them, and then its numbers describe an input nobody chose.

## Two families of command

The **manifest/label** commands measure a plan-45 materialized load. The **receipt**
commands (plan 46) measure a compaction-input load whose paths and part counts change
under compaction, so they carry fixture identity in the receipt instead.

### Plan 46: the compacted part-shape experiment

```bash
# Wait for a compaction-input load to reach one final run per partition. Read-only polling.
ukiel-prod-bench wait-compacted --receipt receipt.json --config compactor.toml --timeout-secs 600

# Scan the final objects and report the actual part shape.
ukiel-prod-bench part-shape --receipt receipt.json --config compactor.toml --result shape.json
```

`wait-compacted` succeeds only when the fixture has **no live L0 parts**, every partition
is **one live run**, two consecutive polls see the **same part ids**, the **row census**
equals the receipt, and every part carries the **artifact marker**. It prints why an
unfinished fixture is still changing, and times out loudly.

`part-shape` reads the final catalog rows and **scans the sorted packing-key column of
every object** to count exact distinct keys — never inferring cardinality from min/max, a
Bloom filter, or the source topology. In one integrity pass it streams the full schema
through the shared row-multiset fingerprint and checks it against the staged input's, so
equal row counts cannot hide a changed, duplicated, or lost row. It refuses to run
against a fixture that has not converged. The report covers file/row/byte quantiles, exact
distinct-key and density quantiles, the level/run/partition distribution, the dedicated
fraction, exact-bitmap present/omitted counts, key-filter NULL and size counts (read by
benchmark-local read-only SQL), the issue-0014 key-count bands, and the filter's storage
cost as a fraction of the parts table and live index.

```bash
# The honest admission A/B: pre-0014 range-only vs the real filtered path, closed-loop.
ukiel-prod-bench admission --receipt receipt.json --config compactor.toml \
  --phase unvacuumed|vacuumed --result adm.json \
  --workers 16 --warmup-secs 5 --duration-secs 30 [--path-order range-first|filter-first]

# Compacted query equivalence: the same suite, receipt form.
ukiel-prod-bench queries --receipt receipt.json --config compactor.toml --result q.json --iters 5
```

`admission` runs two read-only paths over the same deterministic tenant sequence with a
fixed pool of closed-loop workers: **range-only** (benchmark-local SQL reproducing the
pre-0014 predicate with the full part-row projection, so it ships every range candidate
carrying its JSONB `column_stats`) and **filtered** (the real `live_parts_pruned`). Both
deserialize and touch the returned metadata — the cost issue 0014 fixed is the
JSONB/TOAST/SQLx/network cost of shipping rows nobody needed, which a `count(*)` would
miss. It reports offered/completed/failed, throughput, p50/p95/p99/max, mean parts and
tuple bytes per query, and the provider's exact residue, plus `EXPLAIN (ANALYZE, BUFFERS,
FORMAT JSON)` for both paths.

The range-only SQL lives in the tool, never in the product — Ukiel has no range-only
mode, and adding a worse one so a benchmark could measure it would be putting a regression
in the product to make a graph.

**Phases.** `--phase` is *recorded, never performed*: the command never runs `VACUUM`.
Measure `unvacuumed` immediately after convergence, then issue an operator
`VACUUM (ANALYZE) parts` yourself, then measure `vacuumed`. Each phase writes its own
result file; the command refuses to overwrite one or merge two phases.

### Plan 45: the materialized load

```bash
# The issue-0014 A/B: range candidates vs filter candidates vs exact members.
ukiel-prod-bench catalog --manifest <manifest> --label baseline \
  --config ukield.example.toml --result catalog.json

# The six-query suite: scoped Ukiel against raw DataFusion over the same files.
ukiel-prod-bench queries --manifest <manifest> --label baseline \
  --config ukield.example.toml --result queries.json --iters 5
```

`catalog` works against either loader arm. `queries` needs the **materialized** arm —
the catalog-only arm writes no objects, so there is nothing to scan.

## Reading the catalog result

Three numbers, always reported together, because any two of them tell a story the third
would spoil:

- **range** — what min/max pruning alone would ship. For a packed file this is almost
  always a lie of omission: the part's key range *spans* the tenant, it does not
  *contain* it.
- **shipped** — what the catalog actually returns, once the per-part Bloom filter has
  rejected the parts it can prove do not hold the tenant. The gap from `range` is what
  issue 0014 removed before a single row left PostgreSQL.
- **exact** — the truth, from the topology. The gap from `shipped` is what the Bloom
  filter's false positives still cost, and it is the honest ceiling on how much better
  this could get.

68 parts is a **geometry** measurement, not a catalog saturation test. Plan 40 remains the
capacity proof.

## Reading the query result

The Ukiel arm runs the suite through the **real namespace-scoped path**: the tenant is a
property of the session, and `team_id` never appears in the SQL. A real client cannot write
a query that reads another tenant's rows, because it has no way to name one.

The raw arm reads the **same Parquet objects** — the manifest's explicit file list, not a
prefix listing — and adds the equivalent `team_id = ?` itself. It has no catalog, no
pruning and no scoping: it reads every file and filters. That is what makes it a reference.

**Result batches are compared and required to be identical before either arm is timed.** A
difference there is not a performance result; it is a pruning bug (a part that holds rows was
skipped) or a scoping bug (rows from another tenant leaked in). Both are silent in
production and both would be invisible in a benchmark that only timed things.

One warmup, then the median of five. The median, not the mean: one slow run should not
become the number.

### What the ratio means, and where it inverts

`raw` reads all 68 files regardless of tenant, so its time is roughly flat. Ukiel's scales
with the parts the catalog shipped. The crossover is what the suite is for:

| class | parts shipped | ukiel/raw |
|---|---:|---:|
| high_overfetch | 1 of 68 | **0.17x – 0.43x** |
| light | 1 | 0.34x – 0.58x |
| median | 12 | 0.62x – 0.77x |
| low_overfetch | 33 | 0.94x – 1.54x |
| heavy | 63 | **1.19x – 2.16x** |

The heavy tenant is **slower under Ukiel**, and that is a real result rather than a
failure. It lives in 63 of 68 parts, so there is nothing to prune; Ukiel pays a catalog
round-trip and an isolation `FilterExec` on top of a scan that has to happen anyway. Pruning
pays when a tenant is sparse, which is the ordinary case in the production shape and is
exactly where the fixture puts the high-overfetch class.

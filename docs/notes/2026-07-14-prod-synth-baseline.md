# prod-synth: the production-shaped baseline

**Date:** 2026-07-14 · **Plan:** 45 · **Status:** executed

The first Ukiel benchmark whose *geometry* is the production geometry: the tenant-to-part
membership graph, the activity skew, and the key sparsity are reconstructed from the
anonymized capture in `docs/prod-info/` rather than drawn from independent random
distributions. Everything else — event names, URLs, persons, `properties` — is declared
synthetic and claims nothing.

The point of the exercise: issue 0014 showed that a plausible-looking random fixture can
answer the wrong question. This is the fixture that asks the right one.

## What was built

Three executables joined only by a versioned manifest on disk, with no executable
depending on another:

- **`prod-synth`** — offline, deterministic. Compiles the profile into sorted Parquet +
  `manifest.json` + `topology.json`. Installs and runs with nothing but a Rust toolchain;
  `tests/boundary.rs` inspects the built dependency graph and fails if a service client
  ever appears in it.
- **`ukiel-prod-load`** — the only mutating tool. Uploads objects and registers them
  through the product's ordinary write path, or seeds the same truthful topology with no
  objects at all.
- **`ukiel-prod-bench`** — read-only. Measures a load that already exists.

## Fidelity: every gate passes at baseline

10,000 tenants, 68 parts, 168,184 memberships, key universe 36,003, seed 0.

| gate | source | generated | tolerance |
|---|---:|---:|---|
| exact_parts p50 | 11 | **11** | ±1 |
| exact_parts p90 | 43 | **43** | ±1 |
| range_parts p50 | 63 | 61 | ±3 |
| range_overfetch p50 | 5.727 | 5.545 | ±15% |
| range_overfetch p90 (saturation) | 63 | 61 | ≥90% of the achievable max |
| key_density p10 | 0.0062518 | 0.0063757 | ±20% |
| key_density p50 | 0.1026405 | 0.1026275 | ±20% |
| key_density p90 | 0.1374929 | 0.1371181 | ±20% |
| tenant_rows p90/p50 | 98.4 | 80.4 | ±30% |
| tenant_rows p99/p50 | 3460 | 3141 | ±30% |
| degree repairs | — | 1.1% | ≤2% |

The key-density agreement is the one to look at: p50 is within **0.01%**. Nothing assigns
a density. Tenant IDs are drawn across a scaled key universe, the graph is realized, and
each part's range is then *read off its actual members*. The sparsity that makes range
pruning over-select is an emergent property of the construction, which is the whole
difference from the fixture it replaces.

Byte-deterministic: the same profile, config and seed produce identical files, and a
different seed produces a different fixture.

## The catalog A/B — issue 0014 on production geometry

Baseline, catalog-only arm, per representative tenant:

| class | exact | shipped | range | range o/f | shipped o/f |
|---|---:|---:|---:|---:|---:|
| heavy | 63 | 63 | 63 | 1.0x | 1.0x |
| median | 13 | 30 | 61 | 4.7x | 2.3x |
| light | 1 | 1 | 45 | 45.0x | 1.0x |
| high_overfetch | 1 | 1 | 61 | **61.0x** | **1.0x** |
| low_overfetch | 44 | 44 | 44 | 1.0x | 1.0x |

Across these tenants the key filter removed **88.8%** of the candidates that range pruning
alone would have shipped and did not need to (274 range → 139 shipped, against 122 exact).
The high-overfetch tenant — the one the production shape says is the ordinary case — goes
from **61 range candidates to 1 shipped part**.

`shipped` above `exact` is the Bloom filter's false positives, and it is reported rather
than hidden: the median tenant still ships 30 parts for 13 real ones. That is the honest
ceiling on how much better this could get.

68 parts is a **geometry** measurement, not a saturation test. Plan 40 remains the capacity
proof.

## The query suite — scoped Ukiel against raw DataFusion

2M rows, 2,000 tenants, 68 real Parquet objects in MinIO. All **30 query/class pairs
returned identical result batches** before anything was timed. The Ukiel arm runs the real
namespace-scoped path and never names `team_id`; the raw arm reads the same files and adds
the predicate itself.

| class | parts shipped (of 68) | ukiel / raw |
|---|---:|---:|
| high_overfetch | 1 | **0.17x – 0.43x** |
| light | 1 | 0.34x – 0.58x |
| median | 12 | 0.62x – 0.77x |
| low_overfetch | 33 | 0.94x – 1.54x |
| heavy | 63 | **1.19x – 2.16x** |

Raw reads all 68 files whatever the tenant, so its time is roughly flat (~10–16ms). Ukiel's
scales with what the catalog shipped. **The heavy tenant is slower under Ukiel**, and that is
a real result, not a failure: it lives in 63 of 68 parts, so there is nothing to prune, and
Ukiel pays a catalog round-trip and an isolation `FilterExec` on top of a scan that has to
happen regardless. Pruning pays when a tenant is sparse — which is the ordinary case in the
production shape, and is precisely where the high-overfetch class sits.

## Four things the plan did not anticipate

All four were found by building it, and all four changed the artifact.

**1. The topology algorithm as specified cannot run.** Scaling part degrees down while
bounding tenant degrees only by `1..part_count` produces a degree sequence that is *not
bigraphic*: 7 source parts hold exactly one key, so they scale to capacity 1, while the
resample yields ~37 tenants needing two or more of those 7 slots. Havel-Hakimi fails
outright, whatever the sums say. The repair now bounds on **Gale-Ryser feasibility**. It
costs 1.1% of tenants at baseline, all in the extreme tail, and moves neither gated
quantile.

**2. The two capture files disagree about the part population.** `tenant-fanout.jsonl`
reports 63 range candidates for the median tenant, but `part-geometry.jsonl` holds only 61
parts whose key range is wider than a single point — and a single-key part is a range
candidate for exactly one tenant, its own. **No graph over 61 multi-key parts can put a
tenant in 63 ranges.** The files were captured at different part scopes, exactly the hazard
`docs/prod-info/README.md` warns about. This makes the plan's "p90 saturation ≥ 90% of 68
parts" gate unsatisfiable by 0.2 parts. Saturation is asserted against the achievable
maximum instead, and the discrepancy is carried into every manifest rather than absorbed.

**3. A 5M-row baseline cannot carry the source's activity skew.** With ~168k memberships and
a one-row-per-membership floor — which is not negotiable, since a part must actually contain
the tenants the catalog says it contains — 70% of tenants would take their row count from the
floor rather than their activity weight. The measured p90/p50 collapses to 18 against the
source's 98, and `heavy`/`median`/`light` stop being different queries. 26.4M rows is where
the median tenant's weight-share clears its floor; **the baseline default is now 30M**, at
~1.3GB. (Full fidelity to the source's own density of 1,871 rows/membership would need 315M
rows and ~14GB. Baseline trades row density for size while keeping the geometry exact.)

**4. A packaging leak.** The workspace enabled `parquet`'s `async` + `object_store` features
globally, so `ukiel-core` — and therefore any offline tool built on it — dragged in tokio,
`object_store` and reqwest. Moved to the two crates that genuinely read asynchronously from
an object store. `prod-synth` now installs with a Rust toolchain and nothing else, and the
boundary test keeps it that way.

## Reproducing it

Three runbooks: `tools/prod-synth/README.md` (generate), `tools/ukiel-prod-load/README.md`
(load), `tools/ukiel-prod-bench/README.md` (measure). The generator runs offline; the loader
and runner need a stack.

```bash
prod-synth generate --tier baseline --profile docs/prod-info --output <dir>
prod-synth verify <dir>/manifest.json          # on the benchmark host, before loading
ukiel-prod-load materialized --manifest <dir>/manifest.json --label baseline --config <cfg>
ukiel-prod-bench catalog --manifest <dir>/manifest.json --label baseline --config <cfg> --result cat.json
ukiel-prod-bench queries --manifest <dir>/manifest.json --label baseline --config <cfg> --result q.json
```

## What this fixture may not be used to claim

- **Nothing about production performance.** It is synthetic and profile-derived. Every
  manifest says so, and every reader in the pipeline refuses an artifact that does not.
- **Nothing about compaction.** ClickHouse merge levels are provenance; every generated part
  sits at one fixed non-L0 level. ClickHouse parts are not Ukiel day partitions, and reusing
  their levels would give a real number a false meaning.
- **Nothing about catalog capacity.** 68 parts. Plan 40 owns that.
- **Nothing about byte totals.** The source's `bytes_on_disk` describes a different storage
  engine with different compression, and is never copied into an Ukiel part's `size_bytes`.
- **Nothing about column values.** The event vocabulary, URLs, persons and `properties` are a
  declared, versioned invention, stored verbatim in every manifest so a reader can see at a
  glance which columns are evidence and which are not.

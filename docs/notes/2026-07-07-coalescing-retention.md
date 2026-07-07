# Partition coalescing & retention (sketch, roadmap row 22)

Status: sketch. Plan written after plan 17 (level ladder + finalization)
lands — both features are change-feed REPLACE/DELETE authors on that
machinery. Doctrine constraint throughout: **time-agnostic core** (design
doc, "Catalog data model") — no worker parses partition values or compares
event timestamps; everything time-shaped is a declared expression.

## Coalescing

Kills the per-partition file floor (a tiny hypertable accumulates ≥1 file
per partition forever).

- **Declared coarsening mapping** on the hypertable: a deterministic
  `ukiel-expr` expression per partition column mapping fine values to
  coarse ones — e.g. `{"day": "substring(day, 1, 7)"}` makes `day → month`
  one deployment's choice, not an engine concept. Evaluated over a one-row
  batch of partition values; the worker applies results opaquely.
- **Trigger** (all processing-time): a partition is coalescible when it is
  *final* (single run, plan 17's terminal invariant), has been quiet for
  `coalesce_after_secs`, and its coarse value groups ≥ 2 fine partitions
  (or the group's total bytes sit under the size target — don't coalesce
  what's already big enough).
- **Action**: one REPLACE per coarse group — all parts of the fine
  partitions in, one run tagged with the coarse `partition_values` out,
  through `plan_chunks` (key-disjointness and whale isolation survive),
  output level = max input + 1. Losers of races replan; same as every
  other author.
- Effect on probes: `partition_l0_quiet_since` and finalize candidates key
  on partition_values equality — coalesced partitions simply become the
  new (coarse) grouping unit. No special-casing.

## Retention

Two tiers, deliberately:

1. **Partition-drop retention (v1 of this plan)** — per-hypertable declared
   predicate over partition columns, evaluated against a **cutoff value
   computed at the deployment boundary** (ukield derives e.g. "today − 90d"
   and passes it as an opaque parameter; the engine never computes it). A
   partition whose every part is final and whose values satisfy the
   predicate is dropped with a metadata-only DELETE commit. GC reaps
   objects on the normal grace.
2. **Row-level / per-namespace retention (explicitly deferred)** — the
   design doc's per-logical-table policy needs key-scoped rewrites
   (`delete_key`-shaped, predicate instead of key) and retention-aware
   compaction filters. Rides this machinery later; not in the row-22 plan.

**Ordering**: retention runs before coalescing in the worker loop — never
pay a rewrite for data about to be dropped.

## Open question for the plan

Where the coarsening/retention declarations live in config: `[[tables]]`
fields (matching placement) is the obvious answer; validate expressions at
bootstrap exactly like pipeline SQL (deterministic, compiles against the
partition-column schema).

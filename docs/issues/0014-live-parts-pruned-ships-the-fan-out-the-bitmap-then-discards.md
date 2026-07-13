# 0014 — `live_parts_pruned` ships the fan-out that the key bitmap then discards

- **Severity:** Medium (query-admission latency and catalog CPU scale with a hypertable's *total* parts rather than a tenant's; no correctness impact)
- **Status:** Open
- **Components:** `crates/ukiel-catalog` (`live_parts_pruned`), `crates/ukiel-query` (provider)
- **Found by:** issue-0011 benchmark (the million-logical-table catalog fixture), 2026-07-13

## Summary

Query admission asks the catalog for a tenant's live parts:

```rust
// provider.rs
let parts = self.catalog.live_parts_pruned(self.hypertable.id, self.slice, &ranges).await?;
```

`live_parts_pruned` filters on the **key range** each part declares
(`packing_key_min <= k AND packing_key_max >= k`) plus any Int64 `column_stats`
ranges. Range containment is conservative by design: a packed part spanning keys
100..4500 "contains" a tenant that owns no rows in it.

Plan 16 fixed that — but in the **provider**, not in the catalog:

```rust
// provider.rs:107-113 — after the rows have been fetched
part.meta.column_stats
    .and_then(|s| s.get(PACKING_KEYS_STAT))
    .and_then(|encoded| bitmap_contains(encoded, key))
    .is_some_and(|present| !present)   // drop the part
```

So the catalog returns every range-overlapping part row, each carrying its
`column_stats` JSONB — which contains the base64 roaring treemap, ~1 KB — and the
client then uses that bitmap to throw most of those rows away. **The catalog ships
the very structure whose purpose is to prove that the row it arrived in was
irrelevant.**

Measured on the issue-0011 P3 fixture (10M parts, 2,000 hypertables, 1M tenants;
hot hypertable = 300,000 live parts over 500 tenants), Postgres 17:

| probe | rows returned | time | buffers |
|---|---:|---:|---|
| tenant with data (key 251) | **1,078** | **4.41 ms** | 166 hit + 1,062 read |
| tenant with no matching part (key 502) | 0 | 0.87 ms | 298 hit |

Plan 16 measured the reduction the bitmap achieves on the real 100M-row `hits`
fixture: **the median tenant's planned files fall 108 → 7 (−93%)**. Those 108 rows
were all fetched, decoded and shipped by the catalog in order to plan 7 files.

## Impact

Query-admission cost grows with the number of parts whose key *range* covers the
tenant — which, for a packed or size-targeted hypertable, grows with the
hypertable's total part count, not with how much data the tenant has. A tenant
with a handful of files pays for the fan-out of the table it happens to share.

Three consequences, all performance/operability rather than correctness:

- **Latency and Postgres CPU.** 4.4 ms and ~1,000 heap-buffer reads per admission
  at the 10M-part point, to plan a query that will open single-digit files.
- **Driver CPU.** Plan 40 recorded "~1 core per 3.5k catalog reads/s" of
  client-side JSONB decode and named it a fleet-sizing fact. A large share of that
  is deserializing `column_stats` for parts that are about to be discarded — so it
  is a *symptom*, not a fixed cost.
- **Every catalog read number is measured on the pre-bitmap fan-out.** Plan 40's
  read QPS, and issue 0011's, are ceilings for shipping rows that a smarter query
  would not return.

**What is safe.** Correctness is unaffected: range pruning is conservative
(never drops a part that might contain the key) and the bitmap only ever *removes*
parts it can prove are irrelevant, degrading to "keep" on any ambiguity. Dedicated
(single-key) parts are already exact and carry no bitmap. Deletion, compaction and
GC are untouched.

**Scope limit — this is not an index problem.** A key-rank / index-shape
hypothesis was raised and *tested*, and the evidence refutes it: `EXPLAIN` shows
`parts_live_idx` chosen with **both** bounds in the `Index Cond`, and the cost
tracks the rows *returned*, not the index entries traversed — the tenant whose key
matches nothing is **five times cheaper** (0.87 ms) than the one that matches 1,078
parts (4.41 ms), despite traversing the same index range. Rewriting the index will
not help. Returning fewer rows will.

## Proposed resolution

Make the exactness the bitmap already encodes available to **SQL**, so the catalog
returns parts that provably contain the key instead of parts whose range merely
overlaps it. Directions, to be measured rather than preselected:

1. **A key-set column Postgres can test.** Store the multi-key part's key set as
   `int8[]` beside the bitmap and test `keys @> ARRAY[k]` with a GIN index. Same
   information, expressible as an index condition.
2. **A roaring extension** (`rb_contains`). Exact and compact, at the cost of a
   PostgreSQL extension dependency in the managed-database contract (see the HA
   spec's "managed database contract" — an extension is a real constraint there).
3. **Narrow the key bands at write time.** A placement/compaction policy that
   keeps packed parts' key ranges tight makes range pruning near-exact and needs no
   catalog change. Cheapest to try, and it addresses the cause rather than the
   symptom — but it trades against file size.
4. **Two-phase fetch.** Select `id, column_stats->'packing_keys'` first, filter
   client-side, then fetch the surviving rows in full. Cuts the bytes, not the row
   count — a mitigation, not a fix.

Whatever is chosen must preserve the partial `deleted_by_commit IS NULL` index
behaviour, must not explode into a row per `(part, key)` pair at PB scale, and must
keep the degrade-to-*keep* rule for a part whose key set is missing or
undecodable.

## Acceptance criteria

- On the issue-0011 P3 fixture (10M parts; hot hypertable with 300k live parts),
  `live_parts_pruned` for a tenant returns a count on the order of that tenant's
  own parts rather than the range-overlapping fan-out. The before/after row counts
  are recorded, not just the latency.
- The provider's post-fetch bitmap filter becomes a no-op for parts the catalog
  already excluded, and the "ambiguous ⇒ keep" rule is still proven by test: a part
  with a missing or malformed key set is still returned and still read.
- Admission p99 at the 10M point improves, and the improvement is *attributed* —
  rows returned, buffers, and Postgres CPU, not just a wall-clock number.
- A regression test pins the failure mode: a tenant sharing a packed hypertable
  with many others plans only the files that contain its rows, and the catalog
  returns only those parts.

## Notes

- **Refuted hypothesis, recorded so it is not re-raised.** The first explanation
  for the slow probe was an index-shape effect — that `kmin <= k AND kmax >= k` is
  range containment, so a btree could only use the leading bound and cost would
  grow with the tenant's key *rank*. Measured at 30k and 300k live parts, the rank
  effect does not exist (0.556 / 0.498 / 0.423 ms at first / median / last key; the
  last is the *fastest*). It was the rows returned all along.
- Relates to **plan 16**, which introduced the bitmap and measured the 108 → 7
  collapse — in the provider. The catalog side was left as it was, and at the
  cardinality of issue 0011's fixture that is where the cost now sits.
- Adjacent to **roadmap row 35** (catalog aggregate pushdown): both are the same
  shape of fix — let the catalog answer more of the question instead of shipping
  rows for the client to answer it.
- Found because issue 0011's fixture was the first to give a hypertable enough
  parts for the fan-out to matter. Plan 40's 25k-part fixture could not have
  surfaced it.

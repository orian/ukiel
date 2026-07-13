# 0014 — `live_parts_pruned` ships candidates that exact key pruning then discards

- **Severity:** Medium (query-admission latency and catalog CPU scale with a hypertable's range fan-out; no known correctness impact)
- **Status:** Open — verified, with the P3 fixture correction required before selecting a representation
- **Components:** `crates/ukiel-catalog` (`live_parts_pruned`), `crates/ukiel-query` (provider), `crates/ukiel-e2e` (catalog benchmark)
- **Found by:** issue 0011's million-logical-table catalog fixture, 2026-07-13

## Summary

Query admission asks the catalog for a tenant's live parts. The catalog filters
by a part's conservative key range:

```sql
packing_key_min <= $key
AND packing_key_max >= $key
```

It returns the full part row, including `column_stats`. The provider then decodes
`column_stats.packing_keys` as a roaring treemap and drops any part whose exact
key set proves the tenant absent. This is safe, but late: PostgreSQL has already
read and sent every range candidate and its bitmap.

The performance problem is real. The original P3 evidence, however, overstated
what that fixture proves. P3 assigns the same synthetic `fake_bitmap()` to every
multi-key part. That bitmap is unrelated to each part's generated
`packing_key_min` and `packing_key_max`; it can include keys outside the declared
range and omit keys inside it. Consequently:

- P3's 1,078 rows for key 251 prove the cost of the range-candidate fan-out.
- P3 does **not** yet provide a trustworthy count of exact bitmap survivors.
- Its key 502 control is outside the hot hypertable's 1..500 key slice, not a
  tenant inside the slice that happens to have no matching part.

Independent product evidence does prove that exact pruning is valuable: Plan 16
measured the real 100M-row `hits` fixture and reduced median planned files from
108 to 7 (−93%), and the light tenant from 82 to 8 (−90%). Today those discarded
candidates still cross the catalog boundary before the provider removes them.

## Verified current path

`crates/ukiel-catalog/src/parts.rs::live_parts_pruned`:

1. selects the full `PART_COLUMNS`, including `column_stats`;
2. applies live-part, slice and conservative key-range predicates;
3. applies supported Int64 min/max statistics predicates; and
4. returns every surviving row to the query process.

`crates/ukiel-query/src/provider.rs` subsequently calls `part_excludes_key`,
decodes `column_stats["packing_keys"]`, and removes exact-absent parts.

The safety contract is already conservative:

- a decoded bitmap that excludes the key may drop the part;
- a missing or malformed bitmap must keep the part; and
- dedicated single-key parts are exact from their range and need no bitmap.

## Reproduced baseline

The P3 read probe was independently rerun against the local PostgreSQL 17
fixture: 10M parts across 2,000 hypertables, with 300,000 live parts in the hot
hypertable.

| probe | rows returned | execution time | buffers |
|---|---:|---:|---|
| in-slice key 251 | 1,078 range candidates | 4.194 ms | 163 hit + 1,065 read |
| out-of-slice key 502 | 0 | 0.850 ms | 298 hit |

These values reproduce the original 4.41 ms / 1,078-row observation closely.
`EXPLAIN` uses `parts_live_idx` and places **both** key bounds in the index
condition. The earlier hypothesis that the btree could use only the leading
bound is therefore refuted for this query and fixture. The remaining cost follows
the number of candidate rows read and returned.

## Impact

Query-admission work grows with the number of parts whose key range covers the
tenant rather than only with parts that contain the tenant. Packed parts can have
wide ranges but sparse exact key membership.

This creates avoidable:

- PostgreSQL heap reads, JSONB projection and result serialization;
- network bytes between the catalog database and `ukield`;
- driver JSONB decoding and roaring-bitmap decoding; and
- admission latency before any Parquet file is opened.

This is a performance and capacity issue, not a known correctness issue. The
provider's exact filter currently protects correctness and must remain during
rollout.

## Design constraints

The fix must provide two-stage pruning:

1. retain the existing btree-friendly range filter to produce candidates; then
2. apply catalog-side exact or negative membership pruning to those candidates.

The membership predicate may exclude a part **only when absence is proven**.
Missing, unknown-version or malformed metadata must degrade to keep. The provider
filter remains the final safety check until production evidence establishes that
the catalog path is complete.

Do not default to `BIGINT[]` plus GIN without measuring its write and storage
shape. PostgreSQL GIN is an inverted index with an entry for each indexed
component. For per-part key arrays, that can recreate the forbidden `(part, key)`
expansion inside the index and amplify compaction replacement writes. A two-phase
fetch reduces bytes but does not remove catalog row fan-out, so it is only a
fallback mitigation.

## Proposed resolution

### 1. Repair P3 before choosing a representation

Replace the global `fake_bitmap()` with deterministic, truthful per-part key
sets. Keep fixture generation set-based: precompute a bounded pool of key-set
shapes in Rust and join/select those shapes during bulk insertion; do not issue
one client round trip per part.

The fixture must assert for sampled and boundary parts that:

- every encoded key lies within the declared min/max range;
- at least one known key is present;
- at least one in-range key is absent for sparse multi-key parts; and
- decoding the stored bitmap reproduces the generated key set.

Record three different counts for each read probe:

1. range candidates returned by today's SQL;
2. exact survivors according to the truthful bitmap; and
3. files finally planned by the provider.

Add an in-slice absent-key/control case. Correct the stale key-rank benchmark
comment so it no longer claims that only one range bound reaches the index.

### 2. Spike two bounded catalog-side representations

Choose exactly one after measuring the repaired fixture.

**Candidate A — PostgreSQL `roaringbitmap64` membership.** This is preferred if
the deployment contract accepts it. Aurora PostgreSQL 17 currently lists
`pg_roaringbitmap` 1.1.0, and the extension exposes 64-bit bitmap construction and
membership operations. Before adoption:

- pin and test the extension in the local PostgreSQL image;
- add golden-vector tests against Rust `roaring::RoaringTreemap` keys and bytes;
- do not assume its binary format matches the existing base64 payload;
- benchmark a range-first membership predicate without a membership index first;
  and
- document extension/version/upgrade requirements in the managed-database
  contract.

**Candidate B — versioned Bloom membership in `BYTEA`.** This is the portable
fallback. Writers store a compact Bloom filter derived from the same exact key
set. SQL may drop a range candidate only when at least one required bit is zero;
positive matches, unsupported versions and malformed filters remain candidates
and are checked by the exact provider bitmap. Select the bit budget and hash
count from measured key cardinalities and record the observed false-positive
rate.

`int8multirange` with GiST is an optional spike only if real key sets collapse to
a small number of contiguous runs. Sparse tenant IDs are likely to make it too
large, but PostgreSQL GiST can test multirange containment and the repaired
fixture can settle that cheaply.

### 3. Add the selected metadata to every writer

Add a nullable, versioned column or field. Populate it from the same canonical
key set used to build `column_stats.packing_keys` in:

- ingest/finalization;
- streaming compaction output; and
- deletion/rewrite output.

Replacement and commit visibility must remain atomic. Existing rows without the
new metadata remain readable and conservatively unpruned. Avoid any row-per-key
side table or index.

### 4. Push the negative membership filter into catalog SQL

Keep the current live/slice/range predicates. Add the selected membership
predicate so SQL rejects only proven-absent range candidates. Continue selecting
the exact bitmap and running `part_excludes_key` in the provider during rollout;
that check should become a no-op for catalog-pruned rows but remains a correctness
backstop for legacy and unknown metadata.

If Candidate B is selected, the intended shape is conceptually:

```sql
AND (
  key_filter IS NULL
  OR key_filter_version IS DISTINCT FROM $supported_version
  OR bloom_might_contain(key_filter, $key)
)
```

Malformed values must be mapped to keep, not query failure or false absence.

### 5. Prove safety and performance

Add tests for present keys, absent keys inside the declared range, range
boundaries, legacy rows, unknown versions and malformed metadata. Exercise every
writer and a compaction replacement. Then rerun P3 and the real Plan 16 `hits`
probe, recording candidates, survivors, bytes, buffers, PostgreSQL CPU, admission
p50/p99, write throughput and catalog size.

## Acceptance criteria

- P3 stores truthful key sets and reports range candidates, exact survivors and
  provider-planned files separately.
- No present key is filtered. Missing, malformed and unknown-version metadata
  conservatively keep the part.
- All part writers populate the selected representation from the same canonical
  key set as the exact provider bitmap.
- On P3, catalog rows returned approach exact survivors rather than range
  candidates. For a Bloom implementation, measured false positives are at most
  1% at the supported key-cardinality threshold.
- On the real `hits` fixture, catalog-returned rows move materially toward Plan
  16's 108 → 7 file count, and admission p99 plus PostgreSQL/driver CPU improve
  outside run-to-run noise.
- Added catalog storage is at most 15%, and part insert/replacement throughput
  regresses by at most 5% at the 10M-part point.
- No `(part, key)` row or equivalent per-key inverted-index expansion is added.
- If `pg_roaringbitmap` is selected, local-image pinning, Aurora support,
  extension upgrade policy and golden compatibility tests are part of the same
  change.
- The provider exact filter is removed only in a later change with production
  evidence; it is not removed to close this issue.

## References

- Plan 16 established the product selectivity signal: real planned files fell
  from 108 to 7 at the median tenant.
- Issue 0011/P3 established the catalog range-candidate fan-out at 10M parts, but
  its bitmap fixture must be corrected as described above.
- PostgreSQL documents GIN as an inverted index with a separate entry for each
  component: <https://www.postgresql.org/docs/17/indexes-types.html>.
- PostgreSQL documents GiST multirange support, including `@>` element
  containment: <https://www.postgresql.org/docs/17/gist.html>.
- Aurora PostgreSQL's extension matrix lists `pg_roaringbitmap` 1.1.0 for
  PostgreSQL 17: <https://docs.aws.amazon.com/AmazonRDS/latest/AuroraPostgreSQLReleaseNotes/AuroraPostgreSQL.Extensions.html>.
- `pg_roaringbitmap` 1.x documents `roaringbitmap64`, `rb64_build(bigint[])` and
  `@> bigint`: <https://pgxn.org/dist/pg_roaringbitmap/1.0.0/>.

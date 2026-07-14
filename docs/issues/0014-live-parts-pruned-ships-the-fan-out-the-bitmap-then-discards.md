# 0014 — `live_parts_pruned` ships candidates that exact key pruning then discards

- **Severity:** Medium (query-admission latency and catalog CPU scale with a hypertable's range fan-out; no known correctness impact)
- **Status:** **Resolved** — Candidate B (versioned Bloom in `BYTEA`) selected on measurements, shipped in migration 0012. See [Resolution](#resolution).
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

**The original 4.41 ms / 1,078-row observation was a fixture artefact, and the
real number is worse.** That fixture gave every part a key band of ~1 key, so the
1,078 parts it returned *genuinely held* the tenant — a bitmap would have kept all
of them. It was 600 parts per tenant, a state compaction would never leave, and it
did not exhibit this issue at all. (It also refutes an index-shape hypothesis
raised early on: `EXPLAIN` puts **both** key bounds in the `Index Cond`, and the
tenant matching nothing is *five times cheaper* than the one matching 1,078 parts.
Cost follows the rows returned, never the index walked. Recorded so it is not
re-raised.)

Re-measured on a fixture shaped like the product — **one hypertable holding many
tenants, heavily packed**, which the design doc calls the thing that "makes the
1M-tenant math work" and which S5 tests directly (1,000 namespaces in one
hypertable). 100,000 tenants per hypertable, 20,000 live parts; a part's keys are
*scattered* across the tenants active in its window and its `min`/`max` are derived
from that set, as the real writers derive them. PostgreSQL 17, fully warm on 24 GB
`shared_buffers` — so this is CPU, not I/O, and giving the database more memory
does not help:

| the median tenant's parts | count |
|---|---:|
| matched by **key range** — what the catalog shipped | 12,866 |
| matched by **exact key set** — what the query needed | **6** |

**A 2,144× over-fetch.** Range pruning does essentially nothing at this shape, which
is unsurprising once stated: a part's keys are scattered across whoever was active in
its window, so its endpoints bracket almost the entire tenant space. The bitmap was
doing **all** of the pruning — in the client, after the catalog had shipped every
row, each carrying the ~800-byte bitmap that proved it was not needed. Plan 16
measured the same effect on real data and reported it as the median tenant's planned
files falling 108 → 7.

Closed-loop, that cost 320 admission queries/s at a p99 of 328 ms, with the *driver*
burning 11.7 cores decoding bitmaps it then discarded — a **failing** grade against
the demand model. The full before/after, and the two designs that were rejected on
the way, are in [Resolution](#resolution).

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

---

## Resolution

**Candidate B — a versioned Bloom filter in `BYTEA`, in the range index's `INCLUDE`
payload.** Candidate A (`pg_roaringbitmap`) was not needed: the portable fallback
reduced the median tenant's rows to the exact floor, so the extension's deployment
contract bought nothing worth its cost.

Shipped in migration `0012_part_packing_keys.sql`; the layout lives in
`ukiel_core::keyfilter`.

### The fixture had to be repaired first, and it was wrong twice

The original evidence (4.41 ms, 1,078 rows) was an artefact: that fixture's key band
was ~1 key wide, so the 1,078 parts it returned *genuinely held* the tenant. It did
not exhibit this issue at all. Repairing it also refuted an index-shape hypothesis
raised early on — `EXPLAIN` puts **both** key bounds in the `Index Cond`, and the
tenant matching nothing is five times *cheaper* than the one matching 1,078 parts.
Cost follows the rows a scan returns, not the rank of the key it probes.

Two further fixture bugs were found and fixed by guards that now fail the seed:

- The key-set shape pool **sampled** the tenant space instead of partitioning it, so
  four tenants in five appeared in no part at all.
- `random()` inside an uncorrelated `LATERAL` is hoisted by PostgreSQL and evaluated
  **once per batch**, not per row — so `--dedicated-frac 0.2` was a coin flip per
  batch, and it landed heads on the hot hypertable, making it 100% single-key parts
  while every other table was 100% packed. The one table every read probe lands on had
  no packed parts in it, so the issue could not occur there.

Both now `bail!` at seed time. A fixture that can lie will.

### What was measured

One hypertable, 100k tenants, 20k live parts (2M total), keys scattered across the
tenants active in each part's window — the shape the design doc calls the thing that
"makes the 1M-tenant math work". PostgreSQL 17, warm.

The three counts, for the median tenant:

| stage | parts | |
|---|---:|---|
| by **range** — what the catalog shipped | 12,866 | candidates the index brackets |
| by **filter** — what the catalog returns now | **6** | 2,144× fewer rows |
| by **key set** — what the provider plans | 6 | the floor; the filter reaches it |

`EXPLAIN`, same tenant, after `VACUUM`:

| | rows | time | buffers | heap fetches |
|---|---:|---:|---:|---:|
| range only (pre-0014) | 16,000 | 11.1 ms | 16,536 | — |
| range + key filter | **6** | **4.5 ms** | **586** | **0** |

But `EXPLAIN` measures the smaller half. The cost that actually bit was shipping
16,000 rows — each carrying the ~800-byte roaring bitmap that proved it was not
needed — to a driver that then decoded them to throw them away. Closed-loop, 16
workers, same fixture, same harness, one flag apart (`bench catalog read
[--range-only]`):

| | throughput | p50 | p99 | rows/query | verdict |
|---|---:|---:|---:|---:|---|
| pre-0014 (range only) | 320 op/s | 5.3 ms | 328 ms | 3,121 | **FAIL** — 1.92× headroom, driver burned 11.7 cores |
| shipped (range + filter) | **6,562 op/s** | **1.7 ms** | **6.5 ms** | **3.4** | **PASS** — 39× headroom |

**20× the throughput, 50× the p99**, and it turns the admission SLO from a failure
into a pass. Writes are unchanged: **8,026 commits/s, p99 2.6 ms, PostgreSQL 4.4
cores** — at the 7,815/s pre-change baseline.

### Two designs were tried and rejected on measurements

- **GIN on a `BIGINT[]` of the keys.** Reads beautifully, writes catastrophically:
  commits collapsed **7,815/s → 2,254/s**, p99 **2.9 → 28.7 ms**, PostgreSQL **4.4 →
  11.7 of 12 cores**. One index entry per key per part *is* the forbidden `(part,
  key)` expansion, hiding inside an index — exactly as the issue warned. (Its
  `fastupdate` pending list also hides a 30× latency cliff: 4 buffers/0.04 ms after a
  `VACUUM`, 434 buffers/3.6 ms before one.)
- **Sizing the filter per part and hashing in SQL.** A plpgsql call per candidate row
  made the query **slower than the scan it replaced** — 16.5 ms against 12.8 ms —
  while reading a sixth of the buffers. Hashing had to leave the database.

That second failure is what shaped the final design. The bit a key lands on depends
on how many bits there are, so a *variable* size means only the database can compute
the probe positions. The sizes are therefore **fixed**: the client computes the four
(byte, mask) pairs from the key alone, once per query, and SQL is left with four
`get_byte(...) & mask <> 0` tests. There is no SQL half of the hash to keep in step —
`keyfilter::sql_predicate` and `keyfilter::sql_binds` generate the predicate and its
binds, and the catalog and the benchmark both use them.

There are **two** sizes (1024 and 8192 bits) because one cannot serve both shapes of
part: 1024 bits holds false positives under 1% to ~80 keys and is 76% saturated at
500 — useless for a heavily packed file, which is the file this exists for. SQL picks
the branch by `octet_length`, so a part pays for the size it needs and the per-row
cost is four `get_byte`s either way. Past the last tier the filter saturates and
degrades **toward keeping**, which is the behaviour that preceded it.

A part with a single key gets **no** filter: `packing_key_min == packing_key_max` is
already its exact key set, so the range predicate is exact for it. Dedicated parts
are all of that shape.

### What this does *not* fix, and why that is the right answer

`docs/prod-info` landed while this was being measured, and it changes the claim. It
snapshots 68 real parts of `posthog.sharded_events`, and the median part holds
**49,300 distinct keys** — 10% of its 505,300-key span. My fixture's parts hold 35.

The over-fetch is `tenants / keys-per-part`, so the two geometries are opposite ends
of the same identity:

| | tenants | keys/part | live parts | range → exact | over-fetch |
|---|---:|---:|---:|---:|---:|
| production snapshot | 505,300 | 49,300 | 63 | 63 → 11 | **5.7×** |
| the packed fixture | 100,000 | 35 | 20,000 | 12,866 → 6 | **2,144×** |

Seeded at the production geometry, the key filter is a **no-op**: 56 range
candidates, 56 survivors, 6 exact — and **0 bytes stored**, because at 49,300 keys
every bounded filter that fits in an index tuple is 100% saturated. `build` therefore
declines past `MAX_KEYS_WORTH_FILTERING`, so such a part carries no filter and is
kept, exactly as before.

That is the correct outcome rather than a shortfall. A 5.7× over-fetch of **56 rows**
costs nothing worth an index; it is the 2,144× over-fetch of **12,866 rows** that
fails the admission SLO, and that regime is precisely the one a bounded filter can
represent — few keys per part is what makes a filter *fit* and what makes the fan-out
*hurt*, at the same time.

Which regime Ukiel lands in depends on `target_file_bytes`: ClickHouse merges to 60M-row
parts, and Ukiel's files are far smaller, so its parts should hold proportionally fewer
tenants. **That is a prediction, not a measurement** — there is no production Ukiel to
sample. The filter is free where it does not help and decisive where it does, which is
the right way round to be uncertain.

### Cost

- **Storage:** 20.3 MiB of filters across 200k live parts; the range index carrying
  them grew to 43.3 MiB, on a 2,110 MiB `parts` table — **~1% added**, against a 15%
  budget.
- **Writes:** no measurable regression (8,026/s vs a 7,815/s baseline), against a 5%
  budget.
- **No `(part, key)` expansion**, in a row or in an index.

### Acceptance criteria

- [x] P3 stores truthful key sets and reports range candidates, filter survivors and
      exact survivors separately — and now *fails the seed* if any of them is a lie.
- [x] No present key is filtered; missing, malformed and unknown-version metadata
      conservatively keep the part. Proven in `keyfilter`'s unit tests and, through
      the real query on real rows, in `the_key_filter_agrees_between_rust_and_sql`.
- [x] All part writers populate it — derived in `insert_parts` from the roaring bitmap
      the writers already produce, so no writer changed. A compaction REPLACE rebuilds
      it from the merged part's own keys
      (`every_writer_and_a_compaction_replace_carry_a_key_filter`).
- [x] Catalog rows returned reach the exact floor (6 of 12,866 range candidates), and
      measured false positives are under 1% at each tier's stated key count.
- [x] Added storage ~1% (budget 15%); write throughput unregressed (budget 5%).
- [x] No per-key row or inverted-index expansion.
- [x] `pg_roaringbitmap` not adopted, so its deployment contract does not apply.
- [x] The provider's exact filter **remains** as the backstop, instrumented with
      `query_parts_pruned_by_provider_total` so its residue can be watched. It is not
      removed by this change.
- [ ] The real `hits` fixture probe (plan 16's 108 → 7) has **not** been rerun here;
      the product-shaped catalog fixture was the harder case and is the one that
      failed. Worth doing when that fixture is next loaded.

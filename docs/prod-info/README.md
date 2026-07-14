# Production ClickHouse workload snapshot

This directory contains an anonymized shape of the production `posthog.sharded_events`
workload. It is intended to guide Ukiel catalog benchmarks and synthetic-data
generation. It is not a backup and contains no event rows.

## Scope

The row-level analysis was deliberately limited to:

- a 14-day interval selected by the extraction query; and
- one logical shard out of ten.

The ten shards are assumed to have uniform data distributions. That makes this
shard a useful representative sample, but it does not make every number safely
multipliable by ten. The 14-day window describes the active working set, not the
table's complete retention history.

At capture time the directory contained 68 part-geometry records and 534 sampled
tenant-fanout records. Identifiers for parts, partitions, and tenants have either
been hashed or omitted.

## Files

### `table.json`

One JSON object containing table-level metadata reported by ClickHouse:

- `clickhouse_version`: server version used for the capture.
- `database` and `table_name`: source table identity.
- `engine`: the MergeTree engine. `ReplicatedReplacingMergeTree` means the table
  is replicated and background merges can replace older row versions.
- `partition_key`: the physical partitioning expression. Here, data is divided
  into calendar-month partitions.
- `sorting_key`: the on-disk row order. `team_id` is first, so rows for nearby
  tenants tend to be packed together before date, event, and hashed identifiers
  are considered.
- `primary_key`: the sparse-index key. In this table it matches the sorting key.
- `sampling_key`: ClickHouse's deterministic sampling expression.
- `total_rows` and `total_bytes`: table totals reported by ClickHouse at capture
  time. They are not restricted to the 14-day analysis window and their exact
  host/replica scope depends on where the metadata query ran.

Use this file to reproduce the broad table organization. Do not compare its totals
directly with sums from `part-geometry.jsonl`.

### `show-create.sql`

The complete `SHOW CREATE TABLE` result. It records the real column types,
defaults, materialized and ephemeral columns, codecs, data-skipping indexes,
engine parameters, partition key, sort order, sampling key, and index granularity.

This is schema metadata, not row data. It is still less anonymized than the JSONL
files because column, database, table, ZooKeeper path, and index names are visible.

For Ukiel, the most relevant clauses are:

- monthly partitioning by `timestamp`;
- sorting with `team_id` first;
- `ReplicatedReplacingMergeTree` replacement semantics; and
- an index granularity of 8,192 rows.

### `part-geometry.jsonl`

One JSON object per observed ClickHouse part. JSON Lines is used so the file can
be streamed without loading the entire dataset. Each record describes both the
part's physical size and the geometry seen through the 14-day filter.

- `part_id`: deterministic 64-bit hash of the ClickHouse part name. It is an
  anonymous join key within this capture, not an Ukiel part ID.
- `partition_id`: deterministic 64-bit hash of the ClickHouse partition ID.
  Equal values belong to the same source partition; the original value is absent.
- `level`: ClickHouse MergeTree merge level for the part. It reflects merge
  ancestry and must not be interpreted as an Ukiel compaction level.
- `part_type`: ClickHouse on-disk layout. `Wide` stores columns in separate files;
  `Compact` packs column data into fewer files for small parts.
- `physical_rows`: all rows reported for the physical part.
- `bytes_on_disk`: total space occupied by the part, including data and auxiliary
  files.
- `data_compressed_bytes`: compressed bytes for column data.
- `data_uncompressed_bytes`: column-data size before compression. Dividing this
  by `data_compressed_bytes` gives the part's compression ratio.
- `observed_rows`: rows from the part that matched the 14-day extraction filter.
  It can be smaller than `physical_rows` because a physical part may cross the
  sampled time boundary.
- `distinct_keys`: distinct `team_id` values observed in the filtered rows.
- `key_span`: inclusive numeric interval from the smallest to the largest observed
  `team_id`, calculated as `max - min + 1`. It is not a tenant count.
- `key_density`: `distinct_keys / key_span`. A value near 1 describes a densely
  occupied tenant interval; a small value describes a sparse interval for which
  min/max range pruning will produce many false candidates.
- `time_span_ms`: milliseconds between the smallest and largest timestamps
  observed in the filtered rows.

The physical byte and row fields describe whole parts, while the key and time
geometry describes only matching rows. Preserve that distinction when generating
fixtures.

### `tenant-fanout.jsonl`

One JSON object per sampled tenant. Tenant identifiers are intentionally omitted.
This file measures how much a min/max tenant range over-selects ClickHouse parts:

- `tenant_rows`: rows belonging to the tenant in the 14-day, one-shard sample.
- `exact_parts`: parts that actually contain at least one sampled row for the
  tenant.
- `range_parts`: parts whose sampled minimum/maximum `team_id` interval contains
  the tenant, whether or not that tenant is present.
- `range_overfetch`: `range_parts / exact_parts`. `1` means range pruning is
  exact; `10` means it returns ten candidates for every part that actually
  contains the tenant.

This is the most direct input for evaluating Ukiel's exact-key summaries and Bloom
filters. The gap between `range_parts` and `exact_parts` is the work those catalog
features can avoid. It is not a query-latency measurement and does not describe
the ClickHouse query planner as a whole.

## Scaling and interpretation

For a whole ten-shard estimate, distinguish additive totals from distributions:

- Per-part sizes, key density, compression ratio, time span, and overfetch ratios
  should be used as observed. Do not multiply them by ten.
- Logical row and byte totals may be approximately ten times the one-shard value
  if the uniform-shard assumption holds and exactly one replica is represented.
- For fanout across a distributed query, model ten independently laid-out shards
  and add their candidate counts. This preserves shard-local variation better than
  multiplying one tenant record in place.
- Replicas increase physical storage and availability, not logical row count. Do
  not count replica copies as additional workload data.
- Do not extrapolate the number of parts linearly from 14 days to full retention.
  Monthly partitioning and background merging change part geometry with age.

The extract does not include host, shard, or replica columns. Its aggregate part
bytes do not reconcile with the point-in-time `table.json` total, which is
consistent with different query scopes or replica coverage. Therefore, treat the
JSONL files as distribution samples unless the original query scope is available;
do not use their summed bytes as an authoritative single-shard capacity figure.

### The two JSONL files were captured at different part scopes

Found while building plan 45's generator, and load-bearing for anyone who reads these
numbers:

`tenant-fanout.jsonl` reports **63** `range_parts` for 502 of its 534 tenants. But
`part-geometry.jsonl` contains only **61** parts whose `key_span` is greater than 1 — the
other 7 hold a single key each, and a single-key part's `[min, max]` interval contains
exactly one tenant, its own.

A tenant cannot be inside 63 part-ranges when only 61 ranges have any width at all. The
two files therefore do not describe the same set of parts: one of the extraction queries
saw a part population the other did not. This is exactly the hazard the paragraph above
warns about, now confirmed.

What it means in practice:

- **61, not 68, is the ceiling** on any tenant's range-candidate count against these part
  records. A fidelity gate stated as a fraction of 68 (e.g. "range saturation ≥ 90% of the
  part count") is unsatisfiable by construction.
- `range_parts` and `range_overfetch` remain useful as **distributions** — the shape of the
  over-fetch is the finding, and it is dramatic either way — but their absolute counts
  cannot be joined to `part-geometry.jsonl` record-for-record.
- `exact_parts` has the same problem in the tail: a handful of sampled tenants report being
  in 62–63 parts, which is not realizable against 61 multi-key parts plus 7 single-key ones
  unless one tenant owns most of the single-key parts. Plan 45's generator repairs this by
  bounding the degree sequence on Gale-Ryser feasibility, and records every repair.

## Using the data for Ukiel benchmarks

Sample complete JSONL records rather than drawing every field independently. Part
size, tenant count, density, time span, and fanout are correlated, and independent
averages would erase the behavior the benchmark is meant to reproduce.

Useful baseline applications are:

- generate part metadata with realistic correlations among rows, bytes, tenant
  density, and time span;
- reproduce the observed tenant-activity skew from `tenant_rows`;
- compare range-only candidate counts with exact membership or Bloom-filter
  pruning using `range_overfetch`; and
- weight tenants uniformly for a tenant-oriented workload, or by `tenant_rows`
  for a row/event-oriented workload.

ClickHouse parts are not literal Ukiel object-store parts. Their absolute sizes,
merge levels, and file layouts should be normalized to Ukiel's target part size.
The valuable signal here is the workload geometry and the relationships between
fields, not a requirement to copy ClickHouse's storage units exactly.

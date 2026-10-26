# 0016 — Disk cache and memoized object sizes may grow without a capacity bound

- **Severity:** Medium (provisional: sustained object churn may exhaust local disk or memory)
- **Status:** Resolved — bounded LRU data/size caches, shared temporary reservations and cache-full bypass
- **Components:** `crates/ukiel-query/src/cache.rs`, `crates/ukield/src/config.rs`
- **Found by:** source review during the Clicklake comparison, 2026-10-21; checkout `6932d8b`

## Summary

The optional local disk cache stores whole objects or aligned chunks, but its
configuration has no byte budget or eviction policy. A separate object-size
`HashMap` also has no capacity bound. Object immutability makes these entries
stable; it does not bound how many objects a long-running process encounters.

Evidence in the reviewed checkout's [`CachingObjectStore`](../../crates/ukiel-query/src/cache.rs), before the fix below:

- `CacheConfig` exposes large-object threshold, chunk size and write-through,
  but no disk capacity, entry count or eviction setting.
- Whole-file and chunk reads create local files; write-through may prewarm
  complete objects regardless of the chunk threshold.
- `sizes` is a `Mutex<HashMap<String, u64>>` populated by object-size lookups
  and HEAD requests, with no removal or capacity enforcement.
- `delete_stream` delegates deletion to the underlying store without removing
  the corresponding local files or memoized size.

The [Parquet metadata cache](../../crates/ukiel-query/src/metadata_cache.rs)
already has an entry-count LRU. This issue concerns the separate data cache and
size map. The review did not reproduce disk exhaustion or measure growth under
a sustained workload, and it did not identify a runtime cache eviction worker.

## Impact

Queries, ingestion prewarming and compaction can introduce new immutable paths
continually. GC of backing objects does not itself reclaim cached copies, so
local footprint may follow historical activity rather than the current working
set. A long-lived reader may fill its cache filesystem or accumulate metadata
even when the object-store live set is stable.

Read-through cache writes propagate I/O errors, so exhausted cache space can
fail a query despite healthy backing storage. Write-through prewarming has
best-effort paths; do not generalize the read behavior to every write path.
The operational impact depends on whether the cache shares a filesystem with
other service state. With disk caching disabled, this data-cache concern does
not apply.

Cached bytes are disposable, but retaining a copy after backing-object GC can
also help an already-planned query. Eviction must preserve open-reader safety
and must not be conflated with authorization to delete backing objects.

## Proposed resolution

Measure whole-file bytes, chunk bytes, temporary files, size-map cardinality and
free space while repeatedly uploading, reading, compacting and deleting paths.
Include a process restart and a workload larger than the intended cache budget.

Consider a configurable local byte budget with eviction, bounded size metadata,
and explicit behavior when the cache cannot admit a new object. Evaluate
best-effort bypass versus a documented failure policy for read-through writes.
Account for write-through, chunks, concurrent fills and interrupted temporary
files; a chunk-size threshold alone is not a cache capacity limit.

## Acceptance criteria

- A sustained churn test demonstrates a documented bound on disk and size-map
  growth, including after process restart.
- Whole-file, chunk and write-through paths obey the chosen budget, with a
  documented bound on temporary/in-flight overshoot.
- Eviction during concurrent reads or fills does not return corrupt or partial
  data; cache misses continue to fetch the correct immutable object.
- Cache-full behavior is explicit and tested without corrupting backing storage
  or weakening query-lifetime/GC guarantees.
- Metrics expose occupancy, eviction, admission failures and relevant free space.

## Notes

- Establish supported working-set and churn rates before choosing default limits.
- This is a capacity investigation, not evidence that cached immutable bytes
  become stale or that a benchmark result is incorrect.

## Fix and verification (2026-10-26)

The cache now has configurable byte, data-entry and size-entry limits. Defaults
are 16 GiB, 100,000 data entries and a 10,000-entry size LRU; disk caching remains
disabled unless `query.cache_dir` is configured. Completed files and temporary
fills share the budgets. Multipart prewarming reserves capacity before each
part; overflow abandons prewarming while the authoritative upload continues.

LRU eviction skips pinned readers. Read streams and blocking filesystem tasks
retain their pins/reservations through completion or cancellation, closing file
handles before releasing accounting. Admission/full/cache-fill I/O failures
bypass to backing storage. Startup holds an exclusive directory-owner lock,
cleans recognized interrupted fills, reindexes and trims existing data, and
refuses to manage unexpected directory contents before evicting anything.
Hashed keys avoid flattened-path aliases; chunk geometry is part of each key.
GC does not eagerly remove copies that may help already-planned queries;
capacity eviction reclaims them without changing the query-timeout/GC contract.

There is no admitted completed/temporary **file-length** overshoot. Block
rounding and filesystem metadata add space; the entry limit bounds tiny-file
and inode churn. Occupancy, reservations, limits, evictions, bypass and cache
error metrics complement the existing filesystem-free-space collector.

The manual four-worker churn test creates and deletes **40,000 paths / about
417 MiB** of source data across a cache-owner restart, using a **64 KiB / eight
entry** data budget and a **four-entry** size LRU. All concurrent checks pass.
Epoch-end data occupies **40–48 KiB**, including actual allocated blocks, with
eight data entries, four size entries and zero temporary reservations. The
standalone test process reports about **7.2 MiB peak RSS**. This is cache-bound
evidence on an in-memory backing store, not a production throughput claim.

Focused tests cover whole/chunk/multipart admission, active and cancelled reads,
unpolled/out-of-order parts, empty files, HEAD churn, restart/legacy recovery,
directory safety and fallback. The MinIO S12 test also passes with caching
enabled, covering Kafka ingestion, active-active MVs and compaction.

Details, configuration, limits and validation commands:
[cache-capacity note](../notes/2026-10-26-cache-capacity.md).
Raw evidence: `bench/results/issue0016-cache-capacity-20261026/` (gitignored).

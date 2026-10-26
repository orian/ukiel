# Bounded local object cache (issue 0016)

The optional cache now limits completed files **and temporary/in-flight fills**
with one byte budget and one entry budget. Object sizes use a separate bounded
LRU. The backing store remains authoritative; failed cache admission or a failed
cache fill bypasses caching rather than failing a healthy backing-store read or
upload.

## Configuration

`[query]` settings, shared by all roles using the process's store:

| Setting | Default | Meaning |
|---|---:|---|
| `cache_dir` | empty | Empty disables data caching |
| `cache_max_bytes` | 17,179,869,184 (16 GiB) | Completed file lengths plus reserved temporary bytes |
| `cache_max_entries` | 100,000 | Completed files plus active fills, including empty files |
| `cache_size_entries` | 10,000 | Independently bounded object-size LRU |
| `cache_large_object_threshold` | 67,108,864 | Larger read-through objects use chunks |
| `cache_chunk_size` | 8,388,608 | Aligned read-through chunk length |
| `cache_write_through` | true | Best-effort whole-file prewarming after successful writes |

The daemon rejects zero capacities or chunk size when caching is enabled. The
16 GiB starting budget can hold the dependency comparison's combined 14.64 GB
read fixtures; it is an operational default, not a production capacity result.
Choose it for the node's working set and filesystem. Cache limits do not reserve
filesystem free space against other applications.

## Admission and lifetime

- A fill reserves bytes before creating/writing data. Concurrent fills share the
  same accounting. Whole-file, chunk, ordinary-put and multipart paths all use it.
- Admission evicts the least recently used **unpinned** entries until both
  limits fit. An oversized object, busy fill, or cache fully occupied by active
  readers/fills bypasses to backing storage. A range bypass fetches the requested
  bytes rather than a whole object or an unadmittable aligned chunk.
- Multipart uploads reserve each part before teeing it. If a part cannot fit,
  prewarming is abandoned; the authoritative upload continues unchanged. Parts
  use positioned writes, so polling their futures out of order is safe.
- Hits open and pin their files under the eviction lock. A returned stream holds
  the pin until completion/cancellation. Local reads emit at most 1 MiB per
  stream item. Blocking I/O tasks retain their reservations/pins even when their
  callers are cancelled, and close files before releasing accounting.
- No completed/temporary file-length overshoot is admitted: their sum stays at
  most `cache_max_bytes`; their count stays at most `cache_max_entries`.
  Filesystem block rounding, directory entries, inodes, the owner lock and
  filesystem bookkeeping add space beyond file lengths. The entry cap bounds
  inode/tiny-file growth; this is not a physical-volume quota.
- Failed cleanup retains its reservation instead of undercounting leftover
  data. Admission can consequently degrade to bypass until a restart can clean
  the temporary file. Cache storage errors are observable. An I/O error while
  consuming an already-open cached stream can still fail that read.

The cache does not replace the statement-timeout/GC-grace contract. Backing GC
does not eagerly invalidate cached copies that may help an already-planned
query, and bounded eviction can remove those copies. No query may rely on their
survival after the GC grace expires.

## Restart and directory ownership

`cache_dir` is a **dedicated, flat directory of disposable cache files**. Do not
put other application state there. A filesystem owner lock prevents two stores
from applying independent budgets to one directory. An unavailable, unexpected,
or already-owned directory disables caching and emits a warning plus a startup
admission-failure metric; backing storage remains usable.

Startup validates all filenames before removing anything; unexpected files or
directories disable caching and are preserved. Only recognizable cache names,
including legacy production `ht__<id>__L<level>__<uuid>.parquet` names, are managed.
Custom/unknown old filenames require a fresh dedicated directory.

Startup scans regular cache files incrementally, removes recognized interrupted
temporary files, and trims existing contents to the configured budgets before
serving cache hits. Startup memory stays bounded by the entry limit. Runtime LRU
recency resets on restart; scan order is the initial eviction order. Legacy
flattened filenames remain accounted for and reclaimable, but reads repopulate
under fixed-length hashed names. Chunk geometry is part of the new filename,
so changing chunk size cannot reuse the wrong byte ranges. Size memoization
starts empty and repopulates within its entry cap.

## Observability

| Metric | Meaning |
|---|---|
| `cache_enabled` | Whether this process successfully owns its data cache |
| `cache_capacity_bytes`, `cache_capacity_entries`, `cache_size_capacity_entries` | Configured limits |
| `cache_resident_bytes`, `cache_reserved_bytes` | Completed file lengths and in-flight reservations |
| `cache_entries`, `cache_inflight_entries`, `cache_size_entries` | Current cardinalities |
| `cache_evictions_total`, `cache_size_evictions_total` | Data/size-LRU reclamation |
| `cache_admission_failures_total{reason}` | Capacity, oversized, duplicate-fill, startup or I/O bypass |
| `cache_errors_total{operation}` | Cache filesystem failures |

Existing `cache_hits_total`, `cache_misses_total`, and `cache_prewarms_total`
remain. The collector's `ukield_cache_dir_free_ratio` and `ukield_cache_dir_bytes`
continue to expose filesystem free space and directory usage. Gauges describe
the single cache normally owned by a process; paths are not metric labels.

## Validation and churn measurement

The focused tests cover LRU eviction, chunk working sets larger than capacity,
empty-file and HEAD-only churn, pinned/cancelled reads, multipart overflow,
out-of-order and abandoned part futures, restart trimming/orphan cleanup,
exclusive ownership, changed chunk geometry, path-name aliasing, cache I/O
bypass, and concurrent upload/read/delete churn across restart.

The manual measurement runs four concurrent workers through two epochs, with
a cache-owner restart between them: 40,000 immutable paths, a mix of ordinary
puts, chunked reads and multipart uploads, followed by backing-object deletion.
It uses a 64 KiB / 8-entry data cache and a four-entry size LRU, checks bounds
during concurrent activity, and records whole/chunk bytes, temporary reservations,
cardinality and process peak RSS. It exercises the cache contract on an in-memory
backing store, not production object-store throughput.

```bash
rtk proxy cargo test -p ukiel-query --test cache_test sustained_cache_churn_measurement -- --ignored --exact --nocapture
```

The filesystem-instrumented measurement (`debug` test profile, Rust 1.98.1) introduced
**436,928,512 bytes** across 40,000 paths in 2.41 s. Epoch-end occupancy was
**49,152 / 40,960 bytes**, with the same allocated-block totals on this 4 KiB
filesystem. Both epochs ended at **eight data entries / four size entries /
zero temporary reservations**. The highest sampled reservation was **24,576
bytes**; the enforced total budget remained 65,536 bytes. Process peak RSS was
**7,412 KiB**. Filesystem free-space snapshots before/after are retained; their
delta includes build/log/other-process activity and is not attributed to cache
occupancy. The earlier run is also retained (6.455 s / 7,552 KiB peak RSS);
neither timing is a production throughput result.

The query/daemon suites pass **141 tests**, with two manual tests ignored;
the final cache suite passes **24 tests** with the manual churn test ignored.
That manual test passes when invoked explicitly. Workspace Clippy with fault
injection, formatting, and the cache-enabled MinIO S12 test pass. S12 exercised real Kafka
ingestion, competing MV workers and compaction in 3.88 s, leaving 11 completed
cache files / 16,037 bytes and no temporary files. Its default-sized cache did
not experience capacity pressure; the focused churn tests exercise that case.

Raw logs, command and filesystem metadata are retained under
`bench/results/issue0016-cache-capacity-20261026/` (gitignored). The primary files
are `churn-with-filesystem.log`, `measurement.json`, and the retained `churn.log`.

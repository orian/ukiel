# Ukiel Plan 13: Parquet Metadata Cache (Tier-4 read latency) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Tier 4 item #11 of `docs/notes/2026-07-06-parquet-read-performance.md`: a process-wide Parquet footer/metadata cache. Today every query re-fetches and re-parses every file's Parquet footer (~2 object-store roundtrips per file). Ukiel parts are **immutable** — compaction writes new paths and tombstones old ones, a path's bytes never change — so cached metadata never invalidates; the cache only needs a size bound (LRU on entry count). Biggest *latency* win for interactive queries.

**Architecture:** One `Arc<ParquetMetadataCache>` per process (built in `ukield` `run.rs`, carried by `AppState`, injected into `session_for_namespace`). Each session builds a cheap `CachingParquetFileReaderFactory` (wrapping DataFusion's `DefaultParquetFileReaderFactory`) over that shared cache; the schema provider hands it to every `UkielTableProvider`, whose `scan()` attaches it via `ParquetSource::with_parquet_file_reader_factory`. The factory's reader serves `get_metadata` from the cache (key = object-store path string) and delegates all byte-range reads untouched. Nothing about namespace isolation changes — this plan never touches the `FilterExec` boundary or the catalog pruning.

**Tech Stack:** datafusion 54 / arrow+parquet 58.3 (pinned); new workspace dep `lru = "0.18"` (verified latest via `cargo search lru`). API shapes below were verified against the workspace's exact versions: `ParquetFileReaderFactory::create_reader(&self, partition_index: usize, partitioned_file: PartitionedFile, metadata_size_hint: Option<usize>, metrics: &ExecutionPlanMetricsSet) -> Result<Box<dyn AsyncFileReader + Send>>` (re-exported at `datafusion::datasource::physical_plan::ParquetFileReaderFactory`; `DefaultParquetFileReaderFactory` at `datafusion::datasource::physical_plan::parquet::`), `AsyncFileReader::{get_bytes(Range<u64>), get_byte_ranges(Vec<Range<u64>>), get_metadata(Option<&ArrowReaderOptions>)}` (parquet 58.3, reached via the `datafusion::parquet` re-export so versions can never skew), and `ParquetSource::with_parquet_file_reader_factory(Arc<dyn ParquetFileReaderFactory>)`. On any signature mismatch check docs.rs for the workspace's exact version and adapt minimally, keeping the plan shape. (DataFusion 54 also ships its own `CachedParquetFileReaderFactory` over a `FileMetadataCache`; we deliberately keep our own small LRU for explicit hit/miss counters and a process-wide handle — but if the hand-rolled reader fights the API, that built-in is the fallback shape to imitate.)

**Prerequisites:** Plans 1–7 and 10 executed and green. Plan 11 (scan pushdown) is **not** a prerequisite — see the ordering constraint below.

## Global Constraints

- Rust edition 2024, toolchain ≥ 1.96.
- Workspace pins: datafusion 54 · arrow/parquet 58.3 · object_store 0.13 — never bump independently. New dep `lru = "0.18"` goes in the root `Cargo.toml` `[workspace.dependencies]`.
- Component tests via `make test` (`--test-threads=2`).
- **Every existing `ukiel-query` test must stay green after every task** — especially the isolation tests (`namespace_sees_only_its_rows_even_in_packed_files`, `projection_excluding_packing_key_still_isolates`, the alias tests, `information_schema` scoping).
- Commit messages: conventional style, no Claude/AI attribution.
- Every task ends with `cargo fmt && cargo clippy --all-targets -- -D warnings` clean.
- DataFusion/parquet APIs drift between majors: on any mismatch with the snippets below, check docs.rs for the pinned version and adapt minimally, keeping the plan shape.
- **Ordering vs. plan 11 (scan-pushdown): this plan may execute before OR after it.** The snippets below show the provider in its *pre-plan-11* shape (`let source = Arc::new(ParquetSource::new(self.physical_schema.clone()));`). If plan 11 has already executed, `scan()` will instead build `ParquetSource::new(...).with_predicate(...).with_pushdown_filters(true).with_reorder_filters(true)` inside a reworked scan — in that case **append `.with_parquet_file_reader_factory(self.reader_factory.clone())` to whatever builder chain exists**; change nothing else about the chain. Likewise `tests/perf_smoke.rs` (created by plan 11) may exist with extra `session_for_namespace` call sites — grep, don't assume. The factory attachment composes with either provider state.

---

### Task 1: `ParquetMetadataCache` + unit tests (no Docker)

**Files:**
- Modify: `Cargo.toml` (workspace root — add `lru`)
- Modify: `crates/ukiel-query/Cargo.toml` (add `lru`)
- Modify: `crates/ukiel-query/src/lib.rs` (register module)
- Create: `crates/ukiel-query/src/metadata_cache.rs`

**Interfaces:**
- Produces: `ukiel_query::metadata_cache::ParquetMetadataCache` — `Mutex<lru::LruCache<String, Arc<ParquetMetaData>>>` with `AtomicU64` hit/miss counters. `new(capacity_entries)` (0 falls back to the default 10_000), `Default`, `get(path, wants_page_index)`, `put(path, meta)`, `hits()`, `misses()`, `len()`, `is_empty()`.
- The one non-obvious rule: a cached footer parsed **without** page indexes does not satisfy a lookup that wants them — otherwise the first footer-only load would pin a trimmed entry and silently disable page pruning for that file forever. Such a lookup counts as a miss so the caller re-reads and `put` replaces the entry with the richer metadata.

- [ ] **Step 1: Add the dependency and the failing tests**

In the workspace root `Cargo.toml`, add to `[workspace.dependencies]` (after the `reqwest` line):

```toml
lru = "0.18"
```

In `crates/ukiel-query/Cargo.toml`, add to `[dependencies]` (after the `datafusion` line):

```toml
lru = { workspace = true }
```

In `crates/ukiel-query/src/lib.rs`, add after `pub mod context;`:

```rust
pub mod metadata_cache;
```

Create `crates/ukiel-query/src/metadata_cache.rs` with a stub implementation (so the tests compile and fail, proving they test something) plus the full test module:

```rust
//! Process-wide Parquet footer/metadata cache.
//!
//! Every query otherwise re-fetches and re-parses every file's footer (~2
//! object-store roundtrips per file). Ukiel parts are IMMUTABLE — a path's
//! bytes never change once committed (compaction writes new paths and
//! tombstones old ones) — so cached metadata never invalidates; the cache
//! only needs a size bound (LRU on entry count).

use std::sync::Arc;

use datafusion::parquet::file::metadata::ParquetMetaData;

/// Default number of cached footers (entries, not bytes). A parsed footer for
/// our files is a few KB, so 10k entries is tens of MB — bounded, and larger
/// than any current deployment's live part count.
pub const DEFAULT_METADATA_CACHE_ENTRIES: usize = 10_000;

/// Process-wide LRU of parsed Parquet footers, keyed by object-store path.
pub struct ParquetMetadataCache;

impl Default for ParquetMetadataCache {
    fn default() -> Self {
        Self::new(DEFAULT_METADATA_CACHE_ENTRIES)
    }
}

impl ParquetMetadataCache {
    /// `capacity` bounds the number of cached files. Zero falls back to the
    /// default.
    pub fn new(_capacity: usize) -> Self {
        todo!("task 1 step 3")
    }

    pub fn get(&self, _path: &str, _wants_page_index: bool) -> Option<Arc<ParquetMetaData>> {
        todo!("task 1 step 3")
    }

    pub fn put(&self, _path: String, _metadata: Arc<ParquetMetaData>) {
        todo!("task 1 step 3")
    }

    pub fn hits(&self) -> u64 {
        todo!("task 1 step 3")
    }

    pub fn misses(&self) -> u64 {
        todo!("task 1 step 3")
    }

    pub fn len(&self) -> usize {
        todo!("task 1 step 3")
    }

    pub fn is_empty(&self) -> bool {
        todo!("task 1 step 3")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::parquet::arrow::ArrowWriter;
    use datafusion::parquet::file::metadata::{PageIndexPolicy, ParquetMetaDataReader};

    /// A tiny real Parquet file in memory, to parse real metadata from.
    fn sample_file() -> bytes::Bytes {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1_i64, 2, 3]))],
        )
        .unwrap();
        let mut bytes = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut bytes, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        bytes::Bytes::from(bytes)
    }

    /// Footer metadata without page indexes (the default read policy).
    fn footer_only() -> Arc<ParquetMetaData> {
        Arc::new(
            ParquetMetaDataReader::new()
                .parse_and_finish(&sample_file())
                .unwrap(),
        )
    }

    /// Footer metadata including page indexes.
    fn with_page_index() -> Arc<ParquetMetaData> {
        let meta = ParquetMetaDataReader::new()
            .with_page_index_policy(PageIndexPolicy::Optional)
            .parse_and_finish(&sample_file())
            .unwrap();
        assert!(
            meta.column_index().is_some() || meta.offset_index().is_some(),
            "test premise: the sample file must carry page indexes"
        );
        Arc::new(meta)
    }

    #[test]
    fn counts_hits_and_misses() {
        let cache = ParquetMetadataCache::new(4);
        assert!(cache.get("a.parquet", false).is_none());
        assert_eq!((cache.hits(), cache.misses()), (0, 1));

        let meta = footer_only();
        cache.put("a.parquet".to_string(), Arc::clone(&meta));
        let got = cache.get("a.parquet", false).expect("hit");
        assert!(Arc::ptr_eq(&got, &meta), "cache must return the stored Arc");
        assert_eq!((cache.hits(), cache.misses()), (1, 1));
    }

    #[test]
    fn evicts_least_recently_used() {
        let cache = ParquetMetadataCache::new(2);
        let meta = footer_only();
        cache.put("a".to_string(), Arc::clone(&meta));
        cache.put("b".to_string(), Arc::clone(&meta));
        // Touch `a` so `b` is the least recently used, then insert `c`.
        assert!(cache.get("a", false).is_some());
        cache.put("c".to_string(), Arc::clone(&meta));

        assert_eq!(cache.len(), 2);
        assert!(cache.get("b", false).is_none(), "b was evicted");
        assert!(cache.get("a", false).is_some());
        assert!(cache.get("c", false).is_some());
    }

    #[test]
    fn page_index_wanting_lookup_skips_trimmed_entries() {
        let cache = ParquetMetadataCache::new(4);
        cache.put("a".to_string(), footer_only());

        // A footer-only entry satisfies a footer-only request...
        assert!(cache.get("a", false).is_some());
        // ...but not one that wants page indexes: that must re-read the file
        // (a miss) so `put` can replace the entry with the richer metadata.
        assert!(cache.get("a", true).is_none());

        cache.put("a".to_string(), with_page_index());
        assert!(cache.get("a", true).is_some());
        assert!(cache.get("a", false).is_some());
    }

    #[test]
    fn zero_capacity_falls_back_to_default() {
        let cache = ParquetMetadataCache::new(0);
        cache.put("a".to_string(), footer_only());
        assert!(cache.get("a", false).is_some());
    }
}
```

- [ ] **Step 2: Run to verify the tests fail**

Run: `cargo test -p ukiel-query --lib metadata_cache`
Expected: 4 tests, all failing with `not yet implemented: task 1 step 3` panics.

- [ ] **Step 3: Implement the cache**

In `crates/ukiel-query/src/metadata_cache.rs`, replace everything from `use std::sync::Arc;` down to (but not including) the `#[cfg(test)]` line with:

```rust
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use datafusion::parquet::file::metadata::ParquetMetaData;
use lru::LruCache;

/// Default number of cached footers (entries, not bytes). A parsed footer for
/// our files is a few KB, so 10k entries is tens of MB — bounded, and larger
/// than any current deployment's live part count.
pub const DEFAULT_METADATA_CACHE_ENTRIES: usize = 10_000;

/// Process-wide LRU of parsed Parquet footers, keyed by object-store path.
pub struct ParquetMetadataCache {
    entries: Mutex<LruCache<String, Arc<ParquetMetaData>>>,
    hits: AtomicU64,
    misses: AtomicU64,
}

impl std::fmt::Debug for ParquetMetadataCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ParquetMetadataCache")
            .field("len", &self.len())
            .field("hits", &self.hits())
            .field("misses", &self.misses())
            .finish()
    }
}

impl Default for ParquetMetadataCache {
    fn default() -> Self {
        Self::new(DEFAULT_METADATA_CACHE_ENTRIES)
    }
}

impl ParquetMetadataCache {
    /// `capacity` bounds the number of cached files. Zero falls back to the
    /// default.
    pub fn new(capacity: usize) -> Self {
        let capacity = NonZeroUsize::new(capacity)
            .unwrap_or_else(|| NonZeroUsize::new(DEFAULT_METADATA_CACHE_ENTRIES).unwrap());
        Self {
            entries: Mutex::new(LruCache::new(capacity)),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    /// Cached metadata for `path`, counting a hit or a miss.
    ///
    /// An entry parsed without page indexes does not satisfy a request that
    /// wants them (`wants_page_index`): serving it would silently disable
    /// page pruning for that file forever, because the reader would keep
    /// getting the trimmed footer back. Such a lookup is a miss; the caller
    /// re-reads and `put` replaces the entry with the richer metadata.
    pub fn get(&self, path: &str, wants_page_index: bool) -> Option<Arc<ParquetMetaData>> {
        let mut entries = self.entries.lock().expect("metadata cache lock poisoned");
        let sufficient = entries.get(path).filter(|meta| {
            !wants_page_index || meta.column_index().is_some() || meta.offset_index().is_some()
        });
        match sufficient {
            Some(meta) => {
                let meta = Arc::clone(meta);
                self.hits.fetch_add(1, Ordering::Relaxed);
                Some(meta)
            }
            None => {
                self.misses.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }

    /// Stores (or replaces) the metadata for `path`, evicting the least
    /// recently used entry when full. Immutable files: replacement only ever
    /// enriches (footer-only -> with page index), never conflicts.
    pub fn put(&self, path: String, metadata: Arc<ParquetMetaData>) {
        self.entries
            .lock()
            .expect("metadata cache lock poisoned")
            .put(path, metadata);
    }

    pub fn hits(&self) -> u64 {
        self.hits.load(Ordering::Relaxed)
    }

    pub fn misses(&self) -> u64 {
        self.misses.load(Ordering::Relaxed)
    }

    pub fn len(&self) -> usize {
        self.entries
            .lock()
            .expect("metadata cache lock poisoned")
            .len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
```

(Keep the module doc comment at the top of the file and the whole `#[cfg(test)]` module unchanged. `lru` 0.18's `LruCache::new` takes `NonZeroUsize` and `get` takes `&mut self` — both accounted for above; if the pinned lru release differs, check docs.rs and adapt minimally.)

- [ ] **Step 4: Run to verify the tests pass**

Run: `cargo test -p ukiel-query --lib metadata_cache`
Expected: 4 passed.

- [ ] **Step 5: Lint and commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add Cargo.toml Cargo.lock crates/ukiel-query
git commit -m "feat: process-wide lru cache for parquet footer metadata"
```

---

### Task 2: Reader factory + plumbing through provider/context/server/ukield

**Files:**
- Modify: `crates/ukiel-query/src/metadata_cache.rs` (factory + reader)
- Modify: `crates/ukiel-query/src/provider.rs` (field, `try_new`, `scan`)
- Modify: `crates/ukiel-query/src/context.rs` (`session_for_namespace` param, schema provider field)
- Modify: `crates/ukiel-query/src/server.rs` (`AppState` field + call)
- Modify: `crates/ukiel-query/tests/query_test.rs` (helper, all call sites, new integration test)
- Modify: `crates/ukiel-query/tests/server_test.rs` (`AppState` literal)
- Modify: `crates/ukiel-query/tests/perf_smoke.rs` (**only if it exists** — created by plan 11; its `session_for_namespace` call gains the new argument)
- Modify: `crates/ukiel-e2e/src/lib.rs` (`AppState` literal in `Stack::start`)
- Modify: `crates/ukield/src/run.rs` (build the cache once, `AppState` literal)

**Interfaces:**
- Produces: `CachingParquetFileReaderFactory` (implements `datafusion::datasource::physical_plan::ParquetFileReaderFactory`) wrapping `DefaultParquetFileReaderFactory`; its readers serve `get_metadata` from the shared cache and delegate `get_bytes`/`get_byte_ranges` untouched.
- Changes: `UkielTableProvider::try_new` gains a fifth parameter `reader_factory: Arc<CachingParquetFileReaderFactory>`; `session_for_namespace` gains a fifth parameter `metadata_cache: Arc<ParquetMetadataCache>`; `AppState` gains `pub metadata_cache: Arc<ParquetMetadataCache>`.
- Call-site inventory (re-verify with `grep -rn "session_for_namespace(" crates --include="*.rs"` and `grep -rn "AppState {" crates --include="*.rs"` — plan 11 may have added more): `session_for_namespace` is called from `crates/ukiel-query/src/server.rs` and 7 places in `crates/ukiel-query/tests/query_test.rs` (plus `tests/perf_smoke.rs` if plan 11 executed); `AppState { ... }` literals live in `crates/ukiel-query/tests/server_test.rs`, `crates/ukiel-e2e/src/lib.rs`, and `crates/ukield/src/run.rs`.

- [ ] **Step 1: Write the failing integration test**

Append to `crates/ukiel-query/tests/query_test.rs`:

```rust
use ukiel_query::metadata_cache::{CachingParquetFileReaderFactory, ParquetMetadataCache};

/// A fresh cache for tests that don't assert on cache behavior. (In a real
/// process there is exactly one, shared; tests isolate by making their own.)
pub fn test_metadata_cache() -> Arc<ParquetMetadataCache> {
    Arc::new(ParquetMetadataCache::default())
}

#[tokio::test]
async fn metadata_cache_hits_across_queries_with_identical_results() {
    let h = setup().await;
    let store_url = url::Url::parse(STORE_URL).unwrap();
    let cache = Arc::new(ParquetMetadataCache::default());

    // First query: cold cache — every footer read is a miss.
    let ctx = session_for_namespace(
        &h.catalog,
        NamespaceId(1),
        h.store.clone(),
        &store_url,
        cache.clone(),
    )
    .await
    .unwrap();
    let first = ctx
        .sql("SELECT payload FROM events ORDER BY ts")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(all_strings(&first, "payload"), vec!["a1", "a2"]);
    assert!(cache.misses() > 0, "cold cache: first query must miss");
    assert!(!cache.is_empty(), "first query must populate the cache");
    let misses_after_first = cache.misses();
    let hits_after_first = cache.hits();

    // Second query, new session, same process-wide cache: footers are served
    // from memory (hits), no new misses, identical results.
    let ctx = session_for_namespace(
        &h.catalog,
        NamespaceId(1),
        h.store.clone(),
        &store_url,
        cache.clone(),
    )
    .await
    .unwrap();
    let second = ctx
        .sql("SELECT payload FROM events ORDER BY ts")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(
        all_strings(&second, "payload"),
        all_strings(&first, "payload"),
        "results must be identical with and without a warm cache"
    );
    assert!(
        cache.hits() > hits_after_first,
        "warm cache: second query must hit (hits {} -> {})",
        hits_after_first,
        cache.hits()
    );
    assert_eq!(
        cache.misses(),
        misses_after_first,
        "an identical second query must add no misses"
    );
}
```

(If the strict no-new-misses equality ever fails on a DataFusion version that varies its metadata options between phases of one query, keep the `hits() > hits_after_first` and results-identical assertions — they are the contract — and relax only the equality, with a comment. Do not weaken anything else.)

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p ukiel-query --test query_test metadata_cache_hits 2>&1 | tail -20`
Expected: **compile error** — `session_for_namespace` takes 4 arguments (and `CachingParquetFileReaderFactory` does not exist yet). That is the failing state; the plumbing below turns it green.

- [ ] **Step 3: Add the factory and caching reader to `metadata_cache.rs`**

In `crates/ukiel-query/src/metadata_cache.rs`, replace the import block (everything from `use std::num::NonZeroUsize;` through `use lru::LruCache;`) with:

```rust
use std::num::NonZeroUsize;
use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use datafusion::common::Result as DfResult;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::ParquetFileReaderFactory;
use datafusion::datasource::physical_plan::parquet::DefaultParquetFileReaderFactory;
use datafusion::parquet::arrow::arrow_reader::ArrowReaderOptions;
use datafusion::parquet::arrow::async_reader::AsyncFileReader;
use datafusion::parquet::errors::Result as ParquetResult;
use datafusion::parquet::file::metadata::{PageIndexPolicy, ParquetMetaData};
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use futures::FutureExt;
use futures::future::BoxFuture;
use lru::LruCache;
use object_store::ObjectStore;
```

Then insert, between the end of `impl ParquetMetadataCache { ... }` and the `#[cfg(test)]` line:

```rust
/// A [`ParquetFileReaderFactory`] that wraps DataFusion's default
/// object-store reader and serves `get_metadata` from a shared
/// [`ParquetMetadataCache`]. Byte-range reads (row groups, page-index bytes)
/// pass straight through — only the footer fetch+parse is cached.
///
/// Cheap to build per session; the cache is the long-lived, process-wide part.
#[derive(Debug)]
pub struct CachingParquetFileReaderFactory {
    inner: DefaultParquetFileReaderFactory,
    cache: Arc<ParquetMetadataCache>,
}

impl CachingParquetFileReaderFactory {
    pub fn new(store: Arc<dyn ObjectStore>, cache: Arc<ParquetMetadataCache>) -> Self {
        Self {
            inner: DefaultParquetFileReaderFactory::new(store),
            cache,
        }
    }

    /// The shared cache (observability / tests).
    pub fn cache(&self) -> &Arc<ParquetMetadataCache> {
        &self.cache
    }
}

impl ParquetFileReaderFactory for CachingParquetFileReaderFactory {
    fn create_reader(
        &self,
        partition_index: usize,
        partitioned_file: PartitionedFile,
        metadata_size_hint: Option<usize>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> DfResult<Box<dyn AsyncFileReader + Send>> {
        let path = partitioned_file.object_meta.location.to_string();
        let inner = self.inner.create_reader(
            partition_index,
            partitioned_file,
            metadata_size_hint,
            metrics,
        )?;
        Ok(Box::new(CachingParquetFileReader {
            inner,
            cache: Arc::clone(&self.cache),
            path,
        }))
    }
}

/// Delegates all byte-range I/O to the wrapped reader; consults the cache in
/// `get_metadata` (keyed by object-store path — valid because parts are
/// immutable and one process serves one object store).
struct CachingParquetFileReader {
    inner: Box<dyn AsyncFileReader + Send>,
    cache: Arc<ParquetMetadataCache>,
    path: String,
}

impl AsyncFileReader for CachingParquetFileReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, ParquetResult<Bytes>> {
        self.inner.get_bytes(range)
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'_, ParquetResult<Vec<Bytes>>> {
        self.inner.get_byte_ranges(ranges)
    }

    fn get_metadata<'a>(
        &'a mut self,
        options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, ParquetResult<Arc<ParquetMetaData>>> {
        let wants_page_index = options.is_some_and(|o| {
            !matches!(o.column_index_policy(), PageIndexPolicy::Skip)
                || !matches!(o.offset_index_policy(), PageIndexPolicy::Skip)
        });
        async move {
            if let Some(metadata) = self.cache.get(&self.path, wants_page_index) {
                return Ok(metadata);
            }
            let metadata = self.inner.get_metadata(options).await?;
            self.cache.put(self.path.clone(), Arc::clone(&metadata));
            Ok(metadata)
        }
        .boxed()
    }
}
```

- [ ] **Step 4: Thread the factory through the provider**

In `crates/ukiel-query/src/provider.rs`:

Add to the imports:

```rust
use crate::metadata_cache::CachingParquetFileReaderFactory;
```

Add a field to `UkielTableProvider` (after `store_url: ObjectStoreUrl,`):

```rust
    reader_factory: Arc<CachingParquetFileReaderFactory>,
```

Change `try_new` to accept and store it:

```rust
    pub fn try_new(
        catalog: PostgresCatalog,
        hypertable: Hypertable,
        packing_key_value: i64,
        store_url: ObjectStoreUrl,
        reader_factory: Arc<CachingParquetFileReaderFactory>,
    ) -> Result<Self, QueryError> {
        let columns = TableColumns::parse(&hypertable.table_schema)?;
        let physical_schema = Arc::new(columns.physical_schema());
        let query_schema = Arc::new(columns.query_schema());
        Ok(Self {
            catalog,
            hypertable,
            physical_schema,
            query_schema,
            columns,
            packing_key_value,
            store_url,
            reader_factory,
        })
    }
```

In `scan()`, attach the factory to the source. Pre-plan-11 shape — replace:

```rust
        let source = Arc::new(ParquetSource::new(self.physical_schema.clone()));
```

with:

```rust
        let source = Arc::new(
            ParquetSource::new(self.physical_schema.clone())
                .with_parquet_file_reader_factory(self.reader_factory.clone()),
        );
```

**If plan 11 has already executed**, the source construction will instead be a longer chain (`.with_predicate(...)`, `.with_pushdown_filters(true)`, `.with_reorder_filters(true)`); append `.with_parquet_file_reader_factory(self.reader_factory.clone())` as one more builder call on that chain and change nothing else. (`Arc<CachingParquetFileReaderFactory>` coerces implicitly to the `Arc<dyn ParquetFileReaderFactory>` the method takes.)

- [ ] **Step 5: Thread the cache through the session builder**

Replace `crates/ukiel-query/src/context.rs` in full with:

```rust
//! Builds a per-namespace SessionContext: custom catalog/schema providers
//! resolve table names through the Ukiel catalog, scoped to one namespace.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::{CatalogProvider, SchemaProvider};
use datafusion::common::Result as DfResult;
use datafusion::datasource::TableProvider;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::error::DataFusionError;
use datafusion::execution::config::SessionConfig;
use datafusion::prelude::SessionContext;
use object_store::ObjectStore;
use ukiel_catalog::{CatalogError, PostgresCatalog};
use ukiel_core::NamespaceId;

use crate::QueryError;
use crate::metadata_cache::{CachingParquetFileReaderFactory, ParquetMetadataCache};
use crate::provider::UkielTableProvider;

pub async fn session_for_namespace(
    catalog: &PostgresCatalog,
    namespace: NamespaceId,
    store: Arc<dyn ObjectStore>,
    store_url: &url::Url,
    metadata_cache: Arc<ParquetMetadataCache>,
) -> Result<SessionContext, QueryError> {
    let table_names: Vec<String> = catalog
        .list_logical_tables(namespace)
        .await?
        .into_iter()
        .map(|t| t.name)
        .collect();

    // Enable `information_schema` so clients can introspect tables/columns over
    // the SQL endpoint (`SELECT ... FROM information_schema.tables`), scoped to
    // this namespace's registered schema provider.
    let config = SessionConfig::new()
        .with_default_catalog_and_schema("ukiel", "public")
        .with_information_schema(true);
    let ctx = SessionContext::new_with_config(config);
    ctx.register_object_store(store_url, store.clone());

    // One factory per session over the process-wide metadata cache. Sessions
    // are cheap and per-request; the cache is the long-lived part.
    let reader_factory = Arc::new(CachingParquetFileReaderFactory::new(store, metadata_cache));

    let schema = Arc::new(UkielSchemaProvider {
        catalog: catalog.clone(),
        namespace,
        table_names,
        store_url: ObjectStoreUrl::parse(store_url.as_str())?,
        reader_factory,
    });
    ctx.register_catalog("ukiel", Arc::new(UkielCatalogProvider { schema }));
    Ok(ctx)
}

#[derive(Debug)]
struct UkielCatalogProvider {
    schema: Arc<UkielSchemaProvider>,
}

impl CatalogProvider for UkielCatalogProvider {
    fn schema_names(&self) -> Vec<String> {
        vec!["public".to_string()]
    }
    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        (name == "public").then(|| self.schema.clone() as Arc<dyn SchemaProvider>)
    }
}

struct UkielSchemaProvider {
    catalog: PostgresCatalog,
    namespace: NamespaceId,
    /// Names snapshot taken at session build (used for sync `table_exist`).
    table_names: Vec<String>,
    store_url: ObjectStoreUrl,
    reader_factory: Arc<CachingParquetFileReaderFactory>,
}

impl std::fmt::Debug for UkielSchemaProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UkielSchemaProvider")
            .field("namespace", &self.namespace)
            .field("table_names", &self.table_names)
            .finish()
    }
}

#[async_trait]
impl SchemaProvider for UkielSchemaProvider {
    fn table_names(&self) -> Vec<String> {
        self.table_names.clone()
    }

    async fn table(&self, name: &str) -> DfResult<Option<Arc<dyn TableProvider>>> {
        let logical = match self.catalog.get_logical_table(self.namespace, name).await {
            Ok(t) => t,
            Err(CatalogError::NotFound(_)) => return Ok(None),
            Err(e) => return Err(DataFusionError::External(Box::new(e))),
        };
        let hypertable = self
            .catalog
            .get_hypertable_by_id(logical.hypertable_id)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let provider = UkielTableProvider::try_new(
            self.catalog.clone(),
            hypertable,
            self.namespace.0, // v1 convention: table slice = packing_key == namespace id
            self.store_url.clone(),
            self.reader_factory.clone(),
        )
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(Some(Arc::new(provider)))
    }

    fn table_exist(&self, name: &str) -> bool {
        self.table_names.iter().any(|n| n == name)
    }
}
```

- [ ] **Step 6: Server, binary, and harness call sites**

`crates/ukiel-query/src/server.rs` — add the import and field, and pass it through:

```rust
use crate::metadata_cache::ParquetMetadataCache;
```

```rust
#[derive(Clone)]
pub struct AppState {
    pub catalog: PostgresCatalog,
    pub store: Arc<dyn ObjectStore>,
    pub store_url: url::Url,
    /// Process-wide Parquet footer cache, shared by every request's session.
    pub metadata_cache: Arc<ParquetMetadataCache>,
}
```

and in `run_query_inner`:

```rust
    let ctx = session_for_namespace(
        &state.catalog,
        NamespaceId(req.namespace_id),
        state.store.clone(),
        &state.store_url,
        state.metadata_cache.clone(),
    )
    .await
    .map_err(bad_request)?;
```

`crates/ukield/src/run.rs` — add the import:

```rust
use ukiel_query::metadata_cache::ParquetMetadataCache;
```

build the cache once per process, right after the store is built (after the `let store: Arc<dyn ObjectStore> = ...;` block):

```rust
    // Process-wide Parquet footer cache: parts are immutable, so entries
    // never invalidate — the LRU bound is the only policy.
    let metadata_cache = Arc::new(ParquetMetadataCache::default());
```

and extend the `AppState` literal in the query role:

```rust
        let state = AppState {
            catalog: catalog.clone(),
            store: store.clone(),
            store_url: store_url.clone(),
            metadata_cache: metadata_cache.clone(),
        };
```

`crates/ukiel-e2e/src/lib.rs` — add the import:

```rust
use ukiel_query::metadata_cache::ParquetMetadataCache;
```

and extend the `AppState` literal in `Stack::start`:

```rust
        let state = AppState {
            catalog: catalog.clone(),
            store: store.clone(),
            store_url: store_url.clone(),
            metadata_cache: Arc::new(ParquetMetadataCache::default()),
        };
```

`crates/ukiel-query/tests/server_test.rs` — extend the `AppState` literal in `spawn_server` the same way:

```rust
    let state = AppState {
        catalog: h.catalog.clone(),
        store: h.store.clone(),
        store_url: url::Url::parse(STORE_URL).unwrap(),
        metadata_cache: std::sync::Arc::new(
            ukiel_query::metadata_cache::ParquetMetadataCache::default(),
        ),
    };
```

`crates/ukiel-query/tests/query_test.rs` — update every remaining call site:

1. `ctx_with_provider` builds a factory for the direct-provider path:

```rust
async fn ctx_with_provider(h: &Harness, namespace: i64) -> SessionContext {
    let ctx = SessionContext::new();
    ctx.register_object_store(&url::Url::parse(STORE_URL).unwrap(), h.store.clone());
    let ht = h.catalog.get_hypertable_by_id(h.ht).await.unwrap();
    let provider = UkielTableProvider::try_new(
        h.catalog.clone(),
        ht,
        namespace,
        ObjectStoreUrl::parse(STORE_URL).unwrap(),
        Arc::new(CachingParquetFileReaderFactory::new(
            h.store.clone(),
            test_metadata_cache(),
        )),
    )
    .unwrap();
    ctx.register_table("events", Arc::new(provider)).unwrap();
    ctx
}
```

2. Every other `session_for_namespace(...)` call in the file gains `test_metadata_cache()` as the fifth argument (as of this writing: two in `session_resolves_tables_by_namespace`, two in `cache_serves_reads_after_inner_store_loses_the_object`, one in `alias_columns_compute_at_query_time`, two in `information_schema_lists_namespace_tables_and_columns` — one of which is inside the local `table_names` helper). Re-enumerate with `grep -n "session_for_namespace(" crates/ukiel-query/tests/*.rs` and update them all, e.g.:

```rust
    let ctx = session_for_namespace(
        &h.catalog,
        NamespaceId(1),
        h.store.clone(),
        &store_url,
        test_metadata_cache(),
    )
    .await
    .unwrap();
```

3. If `crates/ukiel-query/tests/perf_smoke.rs` exists (plan 11), its `session_for_namespace` call gets `query_test::test_metadata_cache()` as the fifth argument too — and note the footer cache makes the perf harness's warm-up even more representative.

- [ ] **Step 7: Run the full suite**

Run: `cargo test -p ukiel-query -- --test-threads=2`
Expected: all pass, including `metadata_cache_hits_across_queries_with_identical_results`, every isolation test, the alias tests, `information_schema` scoping, and all `server_test` cases.

Then run: `cargo build -p ukield -p ukiel-e2e`
Expected: clean (their `AppState` literals compile with the new field).

- [ ] **Step 8: Lint and commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add crates/ukiel-query crates/ukiel-e2e crates/ukield
git commit -m "feat: serve parquet footers from the metadata cache in every scan"
```

---

### Task 3: Docs — perf note status + roadmap row

**Files:**
- Modify: `docs/notes/2026-07-06-parquet-read-performance.md` (Tier-4 #11 status)
- Modify: `docs/superpowers/plans/2026-07-05-ukiel-v1-roadmap.md` (row 13)

No README change — the cache is an internal read-path optimization with no user-facing surface.

- [ ] **Step 1: Update the perf note's Tier-4 item 11**

In `docs/notes/2026-07-06-parquet-read-performance.md`, replace:

```markdown
11. **Footer + page-index cache** (custom `ParquetFileReaderFactory`):
    every query currently re-fetches every file's footer (~2 object-store
    roundtrips per file). Files are **immutable** — the cache never
    invalidates. Biggest *latency* win for interactive queries.
```

with:

```markdown
11. **Footer + page-index cache** (custom `ParquetFileReaderFactory`):
    every query otherwise re-fetches every file's footer (~2 object-store
    roundtrips per file). Files are **immutable** — the cache never
    invalidates. Biggest *latency* win for interactive queries.
    *Status: plan 13 (`2026-07-06-ukiel-parquet-metadata-cache.md`) —
    process-wide LRU keyed by path, hit/miss counters, page-index-aware
    lookups, wired through provider/session/server/`ukield`.*
```

- [ ] **Step 2: Update roadmap row 13**

In `docs/superpowers/plans/2026-07-05-ukiel-v1-roadmap.md`, plan 13's row already exists — set its status column to **Executed**.

- [ ] **Step 3: Commit**

```bash
git add docs/
git commit -m "docs: record parquet metadata cache status, roadmap row 13 executed"
```

---

## Measured results (2026-07-09)

Verified on the ClickBench `hits` 10 GB tier (100M rows, 1,102 live L1 parts,
per-tenant fan-out 67–108 files/query) — the exact bottleneck this plan
targeted. Same machine and tuned build (ThinLTO + `target-cpu=native`),
identical fixture, so the footer cache is the only variable
(`bench/bench.sh cache13 tuned --skip-bluesky`). Warm-median latency,
**tuned (no cache) → cache13**:

| scenario | heavy | median | light |
|---|---|---|---|
| `count` | 40.4 → 24.1 ms (−40%) | 48.5 → 32.1 (−34%) | 42.3 → 27.1 (−36%) |
| `adv_filter` | 51.2 → 36.0 (−30%) | 60.7 → 37.4 (−38%) | 47.5 → 29.8 (−37%) |
| `distinct_users` | 67.5 → 52.9 (−21%) | 52.7 → 35.6 (−33%) | 45.7 → 27.8 (−39%) |
| `top_urls` | 366.2 → 366.8 (±0%) | 52.4 → 38.0 (−27%) | 50.8 → 31.8 (−37%) |
| `search_phrases` | 182.9 → 181.4 (−1%) | 54.5 → 43.1 (−21%) | 50.3 → 32.6 (−35%) |
| `ts_window` / `minutely` | 2–4 ms (unchanged) | | |

The cache does exactly what the plan predicted: the fixed footer-read floor
(67–108 files × ~2 object-store roundtrips) drops **~30–40%** on every
fan-out-bound query. Scan-bound heavy-tenant string aggregations (`top_urls`,
`search_phrases` over 8.5M rows) are dominated by actual scan work, not footer
reads, so they stay flat — the cache never touches the scan path. Already
stats-pruned queries (`ts_window`/`minutely`, plan 12) were near-zero and stay
there. Full tables + reproduce steps: `bench/README.md` §6.

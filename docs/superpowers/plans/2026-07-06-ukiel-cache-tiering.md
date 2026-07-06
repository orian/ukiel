# Ukiel Plan 15: Cache Tiering (Tier-4 write-through prewarm + range-granular chunks) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Tier 4 items #12 and #13 of `docs/notes/2026-07-06-parquet-read-performance.md`: (A) **write-through prewarm** — `put_opts` on `CachingObjectStore` currently delegates straight to `inner`; after a successful upload it now also lands the payload in the local cache, so compactor L1 output and ingest L0 files (ukield shares one wrapped store across roles) are hot at write time and the first query never misses; (B) **range-granular caching for large objects** — whole-file download is wrong for e.g. a 1 GB dedicated-tenant file; objects larger than a configurable threshold (default 64 MiB) are cached as fixed-size aligned chunk files (default 8 MiB, `<mangled-path>.chunk-<index>`) fetched on demand via `inner.get_range`. Small objects keep the existing whole-file path.

**Architecture:** Everything lands in `crates/ukiel-query/src/cache.rs` (read it first; it is small) plus config plumbing in `crates/ukield`. Invariants that must survive every task: **`CachingObjectStore::new(inner, dir)` keeps its signature** (a new `with_config(inner, dir, CacheConfig)` carries the knobs; `new` = `with_config` with defaults); **every read still routes through `get_opts`** (in object_store 0.13 `get`/`get_range`/`get_ranges` all funnel there, so one method covers the whole Parquet read path); **objects are immutable**, so nothing ever invalidates — prewarm and chunk files are correct forever; **a failed upload must never poison the cache** (`put_opts` delegates to `inner` first and prewarms only on success). Correctness note carried into the docs: the cache dir now grows with every write and every large-object read — eviction remains future work (a `cache-gc` sweep, noted, not implemented); GC-deleted objects leave stale cache files, which are harmless (part paths are UUID-fresh, the catalog never reuses them) but count toward disk usage.

**Tech Stack:** object_store 0.13 (pinned — the version DataFusion 54 links against; never bump). The code below matches its API shapes (`ObjectMeta.size: u64`, `Range<u64>` byte ranges, `GetOptions::head`, `impl From<PutPayload> for Bytes`); on any signature mismatch check docs.rs for the workspace's exact version and adapt minimally, keeping the plan shape. `bytes` 1 is already a dependency of `ukiel-query`.

**Prerequisites:** Plans 1–7 and 10 executed and green (`cache.rs` and `ukield` exist as read here). Independent of plan 11 (scan pushdown) — no overlapping code files; only `docs/notes/2026-07-06-parquet-read-performance.md` is shared, merge doc edits textually if both plans are in flight.

## Global Constraints

- Rust edition 2024, toolchain ≥ 1.96.
- Component tests via `make test` (`--test-threads=2`).
- **Every existing test must stay green after every task** — especially `cache_serves_reads_after_inner_store_loses_the_object` and the isolation tests in `crates/ukiel-query/tests/query_test.rs`, which exercise the read path through the (now two-mode) cache.
- The new `crates/ukiel-query/tests/cache_test.rs` is pure object-store level (in-memory inner store, tempdir cache) — it must pass **without Docker**.
- Commit messages: conventional style, no Claude/AI attribution.
- Every task ends with `cargo fmt && cargo clippy --all-targets -- -D warnings` clean.

---

### Task 1: `CacheConfig` + write-through prewarm on `put_opts`

**Files:**
- Create: `crates/ukiel-query/tests/cache_test.rs`
- Modify: `crates/ukiel-query/src/cache.rs`

**Interfaces:**
- Produces: `pub struct CacheConfig { large_object_threshold: u64, chunk_size: u64, write_through: bool }` with `Default` (64 MiB / 8 MiB / true), and `CachingObjectStore::with_config(inner, dir, config)`. `new` keeps its signature and forwards defaults.
- Changes `put_opts`: delegate to `inner` first, then on success write the payload into the cache with the same atomic tmp+rename pattern `ensure_cached` uses (extracted into a `write_atomic` helper). Cache-write failure is non-fatal (the upload is durable; the first read just misses like before). `put_multipart_opts` is **not** prewarmed — streaming payloads never exist in one piece here; out of scope, nothing in ukiel writes multipart today.

- [ ] **Step 1: Write the failing tests**

Create `crates/ukiel-query/tests/cache_test.rs`:

```rust
//! Cache-tier tests: write-through prewarm and (from the next task)
//! range-granular chunk caching. Pure object-store level — in-memory inner
//! store, tempdir cache dir, no catalog, no Docker.

use std::sync::Arc;

use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use ukiel_query::cache::{CacheConfig, CachingObjectStore};

fn cached_file_names(dir: &std::path::Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();
    names.sort();
    names
}

#[tokio::test]
async fn write_through_prewarms_cache_at_put_time() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = tempfile::tempdir().unwrap();
    let cached = CachingObjectStore::new(inner.clone(), dir.path().to_path_buf());

    let location = Path::from("ht/9/L1/prewarmed.parquet");
    let body: Vec<u8> = (0..4096u32).map(|i| (i % 251) as u8).collect();
    cached.put(&location, body.clone().into()).await.unwrap();

    // The put landed in the inner store unchanged...
    let inner_bytes = inner.get(&location).await.unwrap().bytes().await.unwrap();
    assert_eq!(inner_bytes.as_ref(), body.as_slice());
    // ...and prewarmed the local cache at write time.
    assert!(
        dir.path().join("ht__9__L1__prewarmed.parquet").exists(),
        "expected a prewarmed whole-file cache entry; dir had {:?}",
        cached_file_names(dir.path())
    );

    // Losing the object in the inner store no longer causes a first-query
    // miss: reads come straight from the cache.
    inner.delete(&location).await.unwrap();
    let got = cached.get_range(&location, 100..300).await.unwrap();
    assert_eq!(got.as_ref(), &body[100..300]);
}

#[tokio::test]
async fn write_through_disabled_leaves_cache_cold() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = tempfile::tempdir().unwrap();
    let cached = CachingObjectStore::with_config(
        inner.clone(),
        dir.path().to_path_buf(),
        CacheConfig {
            write_through: false,
            ..CacheConfig::default()
        },
    );

    let location = Path::from("ht/9/L0/cold.parquet");
    cached.put(&location, vec![1u8; 128].into()).await.unwrap();

    assert!(
        inner.get(&location).await.is_ok(),
        "put must still reach the inner store"
    );
    assert!(
        cached_file_names(dir.path()).is_empty(),
        "write_through=false must not populate the cache"
    );
}
```

- [ ] **Step 2: Verify the tests fail**

Run: `cargo test -p ukiel-query --test cache_test`
Expected: **compile error** — `CacheConfig` and `with_config` do not exist yet. That is the failing state; do not weaken the tests to get past it.

- [ ] **Step 3: Implement**

Replace the entire contents of `crates/ukiel-query/src/cache.rs` with:

```rust
//! Local-disk read-through cache with write-through prewarm: the local tier
//! of the spec's 2-layer cache. Reads (`get_opts`, which every `get`/
//! `get_range`/`get_ranges` call routes through in object_store 0.13) are
//! served from a local copy downloaded once per object; writes delegate to
//! `inner` and, on success, prewarm the cache — ukield shares one wrapped
//! store across roles, so compactor L1 output and ingest L0 files land hot
//! at write time. Everything else delegates to `inner`.
//!
//! Objects are immutable, so nothing here ever invalidates. The cache dir
//! grows with every write; eviction is future work
//! (`docs/notes/2026-07-06-parquet-read-performance.md`, Tier 4).

use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    Attributes, CopyOptions, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload,
    PutResult, Result,
};

/// Tuning for the local cache tier. Defaults suit production; tests shrink
/// the sizes to exercise the large-object path cheaply.
#[derive(Debug, Clone, Copy)]
pub struct CacheConfig {
    /// Objects strictly larger than this many bytes are cached as aligned
    /// chunks instead of whole files.
    pub large_object_threshold: u64,
    /// Chunk size in bytes for large-object caching.
    pub chunk_size: u64,
    /// Prewarm the cache with `put` payloads (write-through).
    pub write_through: bool,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            large_object_threshold: 64 * 1024 * 1024,
            chunk_size: 8 * 1024 * 1024,
            write_through: true,
        }
    }
}

#[derive(Debug)]
pub struct CachingObjectStore {
    inner: Arc<dyn ObjectStore>,
    dir: PathBuf,
    config: CacheConfig,
}

impl CachingObjectStore {
    pub fn new(inner: Arc<dyn ObjectStore>, dir: PathBuf) -> Self {
        Self::with_config(inner, dir, CacheConfig::default())
    }

    pub fn with_config(inner: Arc<dyn ObjectStore>, dir: PathBuf, config: CacheConfig) -> Self {
        Self { inner, dir, config }
    }

    fn cache_path(&self, location: &Path) -> PathBuf {
        self.dir.join(location.as_ref().replace('/', "__"))
    }

    /// Atomic tmp+rename write. Concurrent racers may both write; the rename
    /// makes that harmless.
    async fn write_atomic(&self, path: &std::path::Path, bytes: &[u8]) -> Result<()> {
        let tmp = path.with_extension(format!("tmp-{}", uuid::Uuid::new_v4()));
        tokio::fs::write(&tmp, bytes).await.map_err(io_err)?;
        tokio::fs::rename(&tmp, path).await.map_err(io_err)?;
        Ok(())
    }

    /// Downloads the whole object once; concurrent racers may download twice,
    /// the atomic rename makes that harmless.
    async fn ensure_cached(&self, location: &Path) -> Result<PathBuf> {
        let path = self.cache_path(location);
        if tokio::fs::try_exists(&path).await.unwrap_or(false) {
            return Ok(path);
        }
        let bytes = self.inner.get(location).await?.bytes().await?;
        self.write_atomic(&path, &bytes).await?;
        Ok(path)
    }
}

fn io_err(e: std::io::Error) -> object_store::Error {
    object_store::Error::Generic {
        store: "ukiel-cache",
        source: Box::new(e),
    }
}

fn generic_err<E: std::error::Error + Send + Sync + 'static>(e: E) -> object_store::Error {
    object_store::Error::Generic {
        store: "ukiel-cache",
        source: Box::new(e),
    }
}

impl fmt::Display for CachingObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CachingObjectStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for CachingObjectStore {
    /// Delegate to `inner` first — a failed upload must not poison the cache —
    /// then prewarm the local cache with the accepted payload (write-through).
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        // Cloning a PutPayload is cheap: it is a list of ref-counted chunks.
        let prewarm = self.config.write_through.then(|| payload.clone());
        let result = self.inner.put_opts(location, payload, opts).await?;
        if let Some(payload) = prewarm {
            // Best-effort: the upload is durable either way; a failed cache
            // write just means the first read misses, as before.
            let bytes = Bytes::from(payload);
            let _ = self.write_atomic(&self.cache_path(location), &bytes).await;
        }
        Ok(result)
    }

    /// Streaming uploads are not prewarmed (the payload never exists in one
    /// piece here); out of scope — nothing in ukiel writes multipart today.
    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    /// Serve reads from the local cache file. Every `get`/`get_range`/
    /// `get_ranges` in object_store 0.13 routes through here, so caching this
    /// one method covers the whole Parquet read path. The `File` payload lets
    /// object_store seek/slice the requested range itself.
    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let path = self.ensure_cached(location).await?;
        let file = std::fs::File::open(&path).map_err(io_err)?;
        let metadata = file.metadata().map_err(io_err)?;
        let total = metadata.len();
        let last_modified: DateTime<Utc> = metadata
            .modified()
            .map(DateTime::<Utc>::from)
            .unwrap_or_else(|_| Utc::now());

        let range = match &options.range {
            Some(r) => r.as_range(total).map_err(generic_err)?,
            None => 0..total,
        };

        Ok(GetResult {
            payload: GetResultPayload::File(file, path),
            meta: ObjectMeta {
                location: location.clone(),
                last_modified,
                size: total,
                e_tag: None,
                version: None,
            },
            range,
            attributes: Attributes::default(),
        })
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}
```

(object_store 0.13 note: `impl From<PutPayload> for Bytes` exists and concatenates chunks — zero-copy when the payload is a single chunk. If the pinned patch release somehow lacks it, fall back to `let mut buf = Vec::with_capacity(payload.content_length()); for chunk in payload.iter() { buf.extend_from_slice(chunk); }` and write `buf`.)

- [ ] **Step 4: Verify the tests pass**

Run: `cargo test -p ukiel-query --test cache_test`
Expected: both tests pass.

Run: `cargo test -p ukiel-query -- --test-threads=2`
Expected: all pass, including `cache_serves_reads_after_inner_store_loses_the_object` (its `CachingObjectStore::new` call is untouched by design).

- [ ] **Step 5: Lint and commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add crates/ukiel-query
git commit -m "feat: write-through prewarm in the caching object store"
```

---

### Task 2: Range-granular chunk caching for large objects

**Files:**
- Modify: `crates/ukiel-query/src/cache.rs`
- Modify: `crates/ukiel-query/tests/cache_test.rs`

**Interfaces:**
- Changes `get_opts` into a three-way dispatch: (1) a whole-file cache copy always wins (small objects, and write-through prewarms of any size — a prewarmed large object is served from its whole-file copy, never re-chunked); (2) otherwise discover the object size — one `inner.head` per path, memoized in a `Mutex<HashMap<String, u64>>` (immutable objects: never stale, and the entry outlives the object in the inner store, which keeps cached chunks readable after GC deletes it); size ≤ threshold → the existing `ensure_cached` whole-file path; (3) size > threshold → chunked: compute covering chunk indices `start/chunk_size ..= (end-1)/chunk_size`, fetch each missing chunk via `inner.get_range(location, chunk_start..min(chunk_start+chunk_size, size))` into `<mangled-path>.chunk-<index>` (atomic tmp+rename; concurrent racers may fetch a chunk twice — harmless, same as `ensure_cached`), then assemble the requested byte range from the chunk files (seek + read_exact per chunk, concatenate) and return it as a `Stream` payload.
- `get_ranges` keeps the default trait impl (loops `get_range`, each of which lands here).
- A `head` request (`options.head == true`) on a large object returns metadata with an empty payload — it must never fetch data.

- [ ] **Step 1: Write the failing tests**

Append to `crates/ukiel-query/tests/cache_test.rs`:

```rust
#[tokio::test]
async fn large_objects_cache_aligned_chunks_and_serve_ranges() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = tempfile::tempdir().unwrap();
    let cached = CachingObjectStore::with_config(
        inner.clone(),
        dir.path().to_path_buf(),
        CacheConfig {
            large_object_threshold: 1024,
            chunk_size: 512,
            write_through: true,
        },
    );

    // 4000 position-dependent bytes (a partial tail chunk: 4000 = 7*512 + 416),
    // put straight into the INNER store so the read path — not write-through —
    // is what populates the cache.
    let source: Vec<u8> = (0..4000u32).map(|i| (i % 251) as u8).collect();
    let location = Path::from("ht/9/L1/big.parquet");
    inner.put(&location, source.clone().into()).await.unwrap();

    // A range crossing the chunk boundary at 1024 (covers chunks 1 and 2).
    let got = cached.get_range(&location, 900..1300).await.unwrap();
    assert_eq!(got.as_ref(), &source[900..1300]);

    // The tail chunk is clamped to the object size.
    let tail = cached.get_range(&location, 3900..4000).await.unwrap();
    assert_eq!(tail.as_ref(), &source[3900..4000]);

    // Exactly the covering chunks were cached — and no whole-file copy.
    assert_eq!(
        cached_file_names(dir.path()),
        vec![
            "ht__9__L1__big.parquet.chunk-1".to_string(),
            "ht__9__L1__big.parquet.chunk-2".to_string(),
            "ht__9__L1__big.parquet.chunk-7".to_string(),
        ],
        "expected only the covering chunk files"
    );

    // Chunk files hold aligned slices: chunk 1 is exactly source[512..1024).
    let chunk1 = std::fs::read(dir.path().join("ht__9__L1__big.parquet.chunk-1")).unwrap();
    assert_eq!(chunk1.as_slice(), &source[512..1024]);

    // Delete from inner: previously fetched ranges still serve from chunks...
    inner.delete(&location).await.unwrap();
    let again = cached.get_range(&location, 900..1300).await.unwrap();
    assert_eq!(again.as_ref(), &source[900..1300]);
    // ...but a range needing an unfetched chunk (chunk 5) errors — proving
    // the cache really is chunk-granular, not a hidden whole-file copy.
    assert!(
        cached.get_range(&location, 3000..3100).await.is_err(),
        "unfetched chunks must miss to the inner store"
    );
}

#[tokio::test]
async fn small_objects_keep_the_whole_file_path() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = tempfile::tempdir().unwrap();
    let cached = CachingObjectStore::with_config(
        inner.clone(),
        dir.path().to_path_buf(),
        CacheConfig {
            large_object_threshold: 1024,
            chunk_size: 512,
            write_through: false,
        },
    );

    // Exactly at the threshold: still the whole-file path (strictly-larger
    // objects chunk).
    let source: Vec<u8> = (0..1024u32).map(|i| (i % 249) as u8).collect();
    let location = Path::from("ht/9/L0/small.parquet");
    inner.put(&location, source.clone().into()).await.unwrap();

    let got = cached.get_range(&location, 100..300).await.unwrap();
    assert_eq!(got.as_ref(), &source[100..300]);

    assert_eq!(
        cached_file_names(dir.path()),
        vec!["ht__9__L0__small.parquet".to_string()],
        "small objects cache as one whole file, never .chunk- files"
    );
}
```

- [ ] **Step 2: Verify the new test fails**

Run: `cargo test -p ukiel-query --test cache_test`
Expected: `large_objects_cache_aligned_chunks_and_serve_ranges` **fails** on the `cached_file_names` assertion (today the whole 4000-byte file is downloaded, so the dir holds `ht__9__L1__big.parquet` and no chunk files). `small_objects_keep_the_whole_file_path` already passes — it is the regression pin that Task 2 must not break. The two Task-1 tests stay green.

- [ ] **Step 3: Implement**

Replace the entire contents of `crates/ukiel-query/src/cache.rs` with:

```rust
//! Local-disk object cache: the local tier of the spec's 2-layer cache.
//!
//! Reads (`get_opts`, which every `get`/`get_range`/`get_ranges` call routes
//! through in object_store 0.13) are served locally, two ways:
//!
//! - objects at or under `large_object_threshold` bytes are downloaded whole,
//!   once, to `<mangled-path>` (the original behavior);
//! - larger objects are cached as fixed-size aligned chunk files
//!   (`<mangled-path>.chunk-<index>`) fetched on demand — whole-file caching
//!   is wrong for a 1 GB dedicated-tenant file when a query touches a few
//!   row groups.
//!
//! Writes (`put_opts`) delegate to `inner` and, on success, prewarm the cache
//! (write-through): ukield shares one wrapped store across roles, so
//! compactor L1 output and ingest L0 files land hot at write time.
//!
//! Objects are immutable, so nothing here ever invalidates. The cache dir
//! grows with every write and every large-object read; eviction is future
//! work (`docs/notes/2026-07-06-parquet-read-performance.md`, Tier 4).

use std::collections::HashMap;
use std::fmt;
use std::io::SeekFrom;
use std::ops::Range;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::StreamExt;
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    Attributes, CopyOptions, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload,
    PutResult, Result,
};
use tokio::io::{AsyncReadExt, AsyncSeekExt};

/// Tuning for the local cache tier. Defaults suit production; tests shrink
/// the sizes to exercise the chunked path cheaply.
#[derive(Debug, Clone, Copy)]
pub struct CacheConfig {
    /// Objects strictly larger than this many bytes are cached as aligned
    /// chunks instead of whole files.
    pub large_object_threshold: u64,
    /// Chunk size in bytes for large-object caching.
    pub chunk_size: u64,
    /// Prewarm the cache with `put` payloads (write-through).
    pub write_through: bool,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            large_object_threshold: 64 * 1024 * 1024,
            chunk_size: 8 * 1024 * 1024,
            write_through: true,
        }
    }
}

#[derive(Debug)]
pub struct CachingObjectStore {
    inner: Arc<dyn ObjectStore>,
    dir: PathBuf,
    config: CacheConfig,
    /// Object sizes discovered via `head` on the inner store, memoized so the
    /// chunked path does one HEAD per object, not one per range. Objects are
    /// immutable, so entries never go stale — and an entry outlives its
    /// object in the inner store, keeping cached chunks readable after GC.
    sizes: Mutex<HashMap<String, u64>>,
}

impl CachingObjectStore {
    pub fn new(inner: Arc<dyn ObjectStore>, dir: PathBuf) -> Self {
        Self::with_config(inner, dir, CacheConfig::default())
    }

    pub fn with_config(inner: Arc<dyn ObjectStore>, dir: PathBuf, config: CacheConfig) -> Self {
        Self {
            inner,
            dir,
            config,
            sizes: Mutex::new(HashMap::new()),
        }
    }

    fn cache_path(&self, location: &Path) -> PathBuf {
        self.dir.join(location.as_ref().replace('/', "__"))
    }

    fn chunk_path(&self, location: &Path, index: u64) -> PathBuf {
        self.dir
            .join(format!("{}.chunk-{index}", location.as_ref().replace('/', "__")))
    }

    /// Atomic tmp+rename write. Concurrent racers may both write; the rename
    /// makes that harmless.
    async fn write_atomic(&self, path: &std::path::Path, bytes: &[u8]) -> Result<()> {
        let tmp = path.with_extension(format!("tmp-{}", uuid::Uuid::new_v4()));
        tokio::fs::write(&tmp, bytes).await.map_err(io_err)?;
        tokio::fs::rename(&tmp, path).await.map_err(io_err)?;
        Ok(())
    }

    /// Downloads the whole object once; concurrent racers may download twice,
    /// the atomic rename makes that harmless.
    async fn ensure_cached(&self, location: &Path) -> Result<PathBuf> {
        let path = self.cache_path(location);
        if tokio::fs::try_exists(&path).await.unwrap_or(false) {
            return Ok(path);
        }
        let bytes = self.inner.get(location).await?.bytes().await?;
        self.write_atomic(&path, &bytes).await?;
        Ok(path)
    }

    /// Object size from the in-memory map, or one `head` on the inner store.
    async fn object_size(&self, location: &Path) -> Result<u64> {
        if let Some(size) = self.sizes.lock().expect("sizes lock").get(location.as_ref()) {
            return Ok(*size);
        }
        let size = self.inner.head(location).await?.size;
        self.sizes
            .lock()
            .expect("sizes lock")
            .insert(location.as_ref().to_string(), size);
        Ok(size)
    }

    fn meta_for(&self, location: &Path, size: u64) -> ObjectMeta {
        ObjectMeta {
            location: location.clone(),
            last_modified: Utc::now(),
            size,
            e_tag: None,
            version: None,
        }
    }

    /// Serves `options.range` of a local whole-file copy. The `File` payload
    /// lets object_store seek/slice the requested range itself.
    fn serve_local_file(
        &self,
        location: &Path,
        path: PathBuf,
        options: &GetOptions,
    ) -> Result<GetResult> {
        let file = std::fs::File::open(&path).map_err(io_err)?;
        let metadata = file.metadata().map_err(io_err)?;
        let total = metadata.len();
        let last_modified: DateTime<Utc> = metadata
            .modified()
            .map(DateTime::<Utc>::from)
            .unwrap_or_else(|_| Utc::now());

        let range = match &options.range {
            Some(r) => r.as_range(total).map_err(generic_err)?,
            None => 0..total,
        };

        Ok(GetResult {
            payload: GetResultPayload::File(file, path),
            meta: ObjectMeta {
                location: location.clone(),
                last_modified,
                size: total,
                e_tag: None,
                version: None,
            },
            range,
            attributes: Attributes::default(),
        })
    }

    /// Reads `range` of a large object from aligned chunk files, fetching any
    /// missing chunk from the inner store (aligned offsets, clamped to the
    /// object size). Concurrent racers may fetch a chunk twice; the atomic
    /// rename makes that harmless — same as `ensure_cached`.
    async fn read_range_chunked(
        &self,
        location: &Path,
        size: u64,
        range: Range<u64>,
    ) -> Result<Bytes> {
        if range.start >= range.end {
            return Ok(Bytes::new());
        }
        let chunk_size = self.config.chunk_size;
        let first = range.start / chunk_size;
        let last = (range.end - 1) / chunk_size;
        let mut out = Vec::with_capacity((range.end - range.start) as usize);
        for index in first..=last {
            let chunk_start = index * chunk_size;
            let chunk_end = (chunk_start + chunk_size).min(size);
            let path = self.chunk_path(location, index);
            if !tokio::fs::try_exists(&path).await.unwrap_or(false) {
                let bytes = self
                    .inner
                    .get_range(location, chunk_start..chunk_end)
                    .await?;
                self.write_atomic(&path, &bytes).await?;
            }
            // The slice of this chunk the request covers, chunk-relative.
            let lo = range.start.max(chunk_start) - chunk_start;
            let hi = range.end.min(chunk_end) - chunk_start;
            let mut file = tokio::fs::File::open(&path).await.map_err(io_err)?;
            file.seek(SeekFrom::Start(lo)).await.map_err(io_err)?;
            let mut buf = vec![0u8; (hi - lo) as usize];
            file.read_exact(&mut buf).await.map_err(io_err)?;
            out.extend_from_slice(&buf);
        }
        Ok(out.into())
    }
}

fn io_err(e: std::io::Error) -> object_store::Error {
    object_store::Error::Generic {
        store: "ukiel-cache",
        source: Box::new(e),
    }
}

fn generic_err<E: std::error::Error + Send + Sync + 'static>(e: E) -> object_store::Error {
    object_store::Error::Generic {
        store: "ukiel-cache",
        source: Box::new(e),
    }
}

impl fmt::Display for CachingObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CachingObjectStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for CachingObjectStore {
    /// Delegate to `inner` first — a failed upload must not poison the cache —
    /// then prewarm the local cache with the accepted payload (write-through).
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        // Cloning a PutPayload is cheap: it is a list of ref-counted chunks.
        let prewarm = self.config.write_through.then(|| payload.clone());
        let result = self.inner.put_opts(location, payload, opts).await?;
        if let Some(payload) = prewarm {
            // Best-effort: the upload is durable either way; a failed cache
            // write just means the first read misses, as before.
            let bytes = Bytes::from(payload);
            let _ = self.write_atomic(&self.cache_path(location), &bytes).await;
        }
        Ok(result)
    }

    /// Streaming uploads are not prewarmed (the payload never exists in one
    /// piece here); out of scope — nothing in ukiel writes multipart today.
    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    /// Serve reads locally. Every `get`/`get_range`/`get_ranges` in
    /// object_store 0.13 routes through here, so this one method covers the
    /// whole Parquet read path. Small objects (and write-through prewarms of
    /// any size) are whole files; large objects are chunk-granular.
    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        // A whole-file copy always wins (small objects, prewarmed writes) —
        // checked before any size lookup, so it needs no HEAD at all.
        let whole = self.cache_path(location);
        if tokio::fs::try_exists(&whole).await.unwrap_or(false) {
            return self.serve_local_file(location, whole, &options);
        }

        let size = self.object_size(location).await?;
        if size <= self.config.large_object_threshold {
            let path = self.ensure_cached(location).await?;
            return self.serve_local_file(location, path, &options);
        }

        // Large object: chunk-granular. HEAD must not fetch any data.
        if options.head {
            return Ok(GetResult {
                payload: GetResultPayload::Stream(
                    futures::stream::empty::<Result<Bytes>>().boxed(),
                ),
                meta: self.meta_for(location, size),
                range: 0..0,
                attributes: Attributes::default(),
            });
        }
        let range = match &options.range {
            Some(r) => r.as_range(size).map_err(generic_err)?,
            None => 0..size,
        };
        // Note: an un-ranged `get` of a large object buffers it in memory;
        // the Parquet read path always sends ranges, so that stays unused.
        let bytes = self
            .read_range_chunked(location, size, range.clone())
            .await?;
        Ok(GetResult {
            payload: GetResultPayload::Stream(
                futures::stream::once(async move { Ok::<_, object_store::Error>(bytes) }).boxed(),
            ),
            meta: self.meta_for(location, size),
            range,
            attributes: Attributes::default(),
        })
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}
```

(object_store 0.13 API notes, adapt minimally on mismatch per docs.rs: `GetOptions::head: bool` exists; `GetRange::as_range(u64) -> Result<Range<u64>, _>` as already used by the executed code; `ObjectMeta.size` is `u64`; `inner.get_range` takes `Range<u64>` and comes from `ObjectStoreExt`, already imported. For a `GetResultPayload::Stream`, the stream must yield exactly the bytes of the returned `range` — which `read_range_chunked` guarantees.)

- [ ] **Step 4: Verify the tests pass**

Run: `cargo test -p ukiel-query --test cache_test`
Expected: all four tests pass.

Run: `cargo test -p ukiel-query -- --test-threads=2`
Expected: all pass. `cache_serves_reads_after_inner_store_loses_the_object` proves the whole-file path (setup's files are far below the 64 MiB default threshold, so behavior is unchanged; the only new inner-store call is one `head` per object on first read).

- [ ] **Step 5: Lint and commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add crates/ukiel-query
git commit -m "feat: range-granular chunk caching for large objects"
```

---

### Task 3: ukield config plumbing + docs

**Files:**
- Modify: `crates/ukield/src/config.rs`
- Modify: `crates/ukield/src/run.rs`
- Modify: `ukield.example.toml`
- Modify: `docs/notes/2026-07-06-parquet-read-performance.md`
- Modify: `docs/superpowers/plans/2026-07-05-ukiel-v1-roadmap.md`

**Interfaces:**
- Adds `[query] cache_write_through`, `cache_large_object_threshold`, `cache_chunk_size` — all optional, defaulting to `CacheConfig::default()` values (single source of truth: `QueryConfig::default()` reads them from `ukiel_query::cache::CacheConfig::default()`), threaded into `CachingObjectStore::with_config` in `run.rs`. `ukield.example.toml` mentions them as comments only, since the defaults are right.

- [ ] **Step 1: Write the failing test**

In `crates/ukield/src/config.rs`, in the `parses_config_with_defaults` test, replace:

```rust
        assert_eq!(cfg.query.listen, "127.0.0.1:8080");
```

with:

```rust
        assert_eq!(cfg.query.listen, "127.0.0.1:8080");
        assert!(cfg.query.cache_write_through);
        assert_eq!(cfg.query.cache_large_object_threshold, 64 * 1024 * 1024);
        assert_eq!(cfg.query.cache_chunk_size, 8 * 1024 * 1024);
```

- [ ] **Step 2: Verify it fails**

Run: `cargo test -p ukield --lib`
Expected: **compile error** — `QueryConfig` has no such fields yet.

- [ ] **Step 3: Implement**

In `crates/ukield/src/config.rs`, replace the `QueryConfig` struct and its `Default` impl:

```rust
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(default)]
pub struct QueryConfig {
    pub listen: String,
    /// Non-empty = wrap the store in the local cache (read-through +
    /// write-through prewarm; chunked for large objects).
    pub cache_dir: String,
    /// Prewarm the cache with written objects (compactor/ingest output).
    pub cache_write_through: bool,
    /// Objects larger than this many bytes are cached as chunks.
    pub cache_large_object_threshold: u64,
    /// Chunk size in bytes for large-object caching.
    pub cache_chunk_size: u64,
}

impl Default for QueryConfig {
    fn default() -> Self {
        // Cache defaults come from one place: the cache's own config.
        let cache = ukiel_query::cache::CacheConfig::default();
        Self {
            listen: "127.0.0.1:8080".to_string(),
            cache_dir: String::new(),
            cache_write_through: cache.write_through,
            cache_large_object_threshold: cache.large_object_threshold,
            cache_chunk_size: cache.chunk_size,
        }
    }
}
```

In `crates/ukield/src/run.rs`, replace the import:

```rust
use ukiel_query::cache::CachingObjectStore;
```

with:

```rust
use ukiel_query::cache::{CacheConfig, CachingObjectStore};
```

and replace the store-wrapping block:

```rust
    let store: Arc<dyn ObjectStore> = if cfg.query.cache_dir.is_empty() {
        base_store
    } else {
        Arc::new(CachingObjectStore::new(
            base_store,
            PathBuf::from(&cfg.query.cache_dir),
        ))
    };
```

with:

```rust
    let store: Arc<dyn ObjectStore> = if cfg.query.cache_dir.is_empty() {
        base_store
    } else {
        Arc::new(CachingObjectStore::with_config(
            base_store,
            PathBuf::from(&cfg.query.cache_dir),
            CacheConfig {
                large_object_threshold: cfg.query.cache_large_object_threshold,
                chunk_size: cfg.query.cache_chunk_size,
                write_through: cfg.query.cache_write_through,
            },
        ))
    };
```

- [ ] **Step 4: Verify**

Run: `cargo test -p ukield -- --test-threads=2`
Expected: all pass (config unit tests without Docker; `bootstrap_test`/`server_test` need Docker, same as before).

- [ ] **Step 5: Example config mention**

In `ukield.example.toml`, replace:

```toml
[query]
listen = "127.0.0.1:8080"
cache_dir = "" # non-empty = local read-through cache directory
```

with:

```toml
[query]
listen = "127.0.0.1:8080"
cache_dir = "" # non-empty = local disk cache (read-through + write-through prewarm)
# Cache tuning (defaults shown; uncomment only to override):
# cache_write_through = true              # prewarm the cache with every written object
# cache_large_object_threshold = 67108864 # objects over 64 MiB cache as aligned chunks
# cache_chunk_size = 8388608              # 8 MiB chunk files for large objects
```

- [ ] **Step 6: Docs — perf-note status + roadmap**

In `docs/notes/2026-07-06-parquet-read-performance.md`, replace items 12 and 13 of Tier 4:

```markdown
12. **Write-through cache prewarm**: compactor L1 outputs are the hottest
    files in the system; push them into the NVMe cache at write time.
13. **Range-granular caching** for large dedicated files (whole-file caching
    is right for small packed files, wasteful at 1GB).
```

with:

```markdown
12. **Write-through cache prewarm** *(plan 15 — executed)*: `put_opts` on the
    caching store prewarms the local cache after a successful upload, so
    compactor L1 output and ingest L0 files land hot at write time — no
    first-query miss. Multipart uploads are not prewarmed (streaming; unused).
13. **Range-granular caching** *(plan 15 — executed)*: objects over
    `large_object_threshold` (64 MiB default) cache as aligned `chunk_size`
    (8 MiB) chunk files fetched on demand; small objects keep the whole-file
    path. One memoized HEAD per object discovers the size.

Cache growth after plan 15: the cache dir grows with every write (prewarm)
and every large-object read (chunks); eviction remains future work.
GC-deleted objects leave stale cache files — harmless (part paths are
UUID-fresh, the catalog never reuses them) but they count toward disk
usage; a `cache-gc` sweep is the natural follow-up.
```

In `docs/superpowers/plans/2026-07-05-ukiel-v1-roadmap.md`, set plan 15's row status from **Ready to execute** to **Executed** (keep the rest of the row unchanged).

- [ ] **Step 7: Full verification**

Run: `cargo fmt --check && cargo clippy --all-targets -- -D warnings && make test`
Expected: all green. Optionally `make e2e` — every role's object-store traffic now flows through the two-mode cache in cache-enabled deployments, so it is cheap insurance.

- [ ] **Step 8: Commit**

```bash
git add crates/ukield ukield.example.toml docs/
git commit -m "feat: thread cache tiering config through ukield"
```

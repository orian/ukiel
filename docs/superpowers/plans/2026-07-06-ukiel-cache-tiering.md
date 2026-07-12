# Ukiel Plan 15: Cache Tiering (Tier-4 write-through prewarm + range-granular chunks) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

> Re-authored 2026-07-12 against the post-29 tree. The original (2026-07-06) predated plans 13/14/20/27/29; its premise "nothing in ukiel writes multipart today" was retired by plan 29's streaming merge — compactor output now streams through `ParquetObjectWriter`/`BufWriter`, which is exactly `put_multipart_opts` for files over 10 MiB. This version adds the multipart prewarm tee, fixes the HEAD-downloads-the-object hazard that plan 29 made hot, and preserves the plan-20 cache counters.

**Goal:** Tier 4 items #12 and #13 of `docs/notes/2026-07-06-parquet-read-performance.md`, plus one latent-bug fix the re-author surfaced:

- **(A) Write-through prewarm on `put_opts`** — after a successful upload the payload also lands in the local cache. Covers ingest L0 (`flusher.rs:91`), deletion rewrites (`deletion.rs:84`), and small compactor outputs (≤ 10 MiB, which `BufWriter` flushes via `put_opts`).
- **(B) Write-through prewarm on multipart uploads** — a tee `MultipartUpload` that mirrors parts into a local file as they upload, finalized on `complete()`. Covers compactor streaming outputs > 10 MiB — the hottest files in the system, and the reason the old plan's `put_opts`-only design no longer suffices (plan 29 moved `merge_parts` to `AsyncArrowWriter<ParquetObjectWriter>`, `rewrite.rs:280`).
- **(C) HEAD passthrough** — `get_opts` with `options.head == true` must never download data. Today it calls `ensure_cached` and pulls the whole object. Latent until plan 29: `OpenFile::close` HEADs every output file right after streaming it (`rewrite.rs:319`), so a cache-enabled ukield currently re-downloads every merge output it just uploaded. With (B), that HEAD is served from the prewarmed local copy instead.
- **(D) Range-granular caching for large objects** (#13) — whole-file download is wrong for e.g. a 1 GB dedicated-tenant file (or a plan-29 whale slice). Objects larger than a configurable threshold (default 64 MiB) are cached as fixed-size aligned chunk files (default 8 MiB, `<mangled-path>.chunk-<index>`) fetched on demand via `inner.get_range`. Small objects keep the existing whole-file path.

**Architecture:** Everything lands in `crates/ukiel-query/src/cache.rs` (read it first; it is small) plus config plumbing in `crates/ukield`. Invariants that must survive every task:

- **`CachingObjectStore::new(inner, dir)` keeps its signature** — a new `with_config(inner, dir, CacheConfig)` carries the knobs; `new` = `with_config` with defaults. Callers today: `ukield/src/run.rs:86`, `ukiel-query/tests/query_test.rs:297`.
- **Every read routes through `get_opts`** (in object_store 0.13 `get`/`get_range`/`get_ranges`/`head` on `ObjectStoreExt` all funnel there). Corollary: **`head` cannot be overridden** — it is an ext-trait default (`object_store-0.13.2/src/lib.rs:1363`, `get_opts` with `with_head(true)`), so the head branch must live inside `get_opts`.
- **The plan-20 counters stay**: `cache_hits_total`/`cache_misses_total` with `tier => "file"` in `ensure_cached` (`cache.rs:40-43`). The chunked path emits the same counters with `tier => "chunk"` (per chunk); prewarms emit `cache_prewarms_total{kind}`. Do not drop them in any rewrite.
- **Objects are immutable**, so nothing ever invalidates — prewarm files, memoized sizes, and chunk files are correct forever.
- **A failed upload must never poison the cache**: `put_opts` delegates to `inner` first and prewarms only on success; the multipart tee renames its tmp file into place only after `inner.complete()` succeeds, and a tee write error marks the tmp poisoned (skipped at complete) without failing the upload — the prewarm is always best-effort, the upload is always authoritative.
- Correctness note carried into the docs: the cache dir grows with every write and every large-object read — eviction remains future work; the monitoring spec's `ukield_cache_dir_free_ratio`/`_bytes` gauges (plan 21) are the guardrail, and the collector's byte walk counts chunk files automatically (it walks the dir). GC-deleted objects leave stale cache files, which are harmless (part paths are UUID-fresh, the catalog never reuses them) but count toward disk usage.

**Tech Stack:** object_store 0.13.2 (pinned — the version DataFusion 54 links against; never bump). Verified API shapes this plan relies on: `ObjectMeta.size: u64`; `Range<u64>` byte ranges with `GetRange::as_range(u64)`; `GetOptions.head: bool` (`lib.rs:1488`); `pub type UploadPart = BoxFuture<'static, Result<()>>` and `MultipartUpload { fn put_part(&mut self, data: PutPayload) -> UploadPart; async fn complete(&mut self) -> Result<PutResult>; async fn abort(&mut self) -> Result<()> }` (`upload.rs:30,43-94`); `impl From<PutPayload> for Bytes` (`payload.rs:165`, concatenates ref-counted chunks — zero-copy for a single chunk); `BufWriter` buffers up to 10 MiB then switches to `put_multipart_opts` (`buffered.rs:213-260`). On any signature mismatch check the vendored source under `~/.cargo/registry/src/*/object_store-0.13.2/` and adapt minimally, keeping the plan shape. `bytes`, `uuid`, `metrics` are already dependencies of `ukiel-query`.

**Prerequisites:** Plans 1–7, 10, 13, 14, 20, and 29 executed (all are — this is what the re-author verified: `cache.rs` with plan-20 counters, `metadata_cache.rs`, the plan-29 streaming writer). Independent of plan 32 (bench suites) — no overlapping code; if 32 is in flight, merge doc edits textually.

**Measured motivation (2026-07-12, `docs/notes/2026-07-12-official-suite-gap.md`):** transport — reading MinIO over HTTP with no data-cache tier — was **52% of the official-suite gap** (53.4 s → 38.6 s on the 43 ClickBench hot minima just by serving local bytes). This plan is the production form of that experiment: after it, a cache-enabled deployment's hot path serves from local disk.

**How to see the payoff (executor note):** the effect only shows on an object-store-backed run. A `bench clickbench run` with `UKIEL_E2E_STORE_DIR` set reads local files already and will NOT move; the before/after window is the plan-32 MinIO configuration (env unset). The bench `Stack` does not wire `CachingObjectStore` — either verify via `ukield` with `cache_dir` set (`make play` config) against MinIO, or do a bench-only follow-up wiring the cache into the e2e `Stack` behind an env var (out of scope for this plan's tasks; note it in the results if skipped). At minimum, the `verify` skill run should observe: a compactor merge output appearing in the cache dir at write time (prewarm), and `cache_prewarms_total{kind="multipart"}` advancing on `/metrics`.

## Global Constraints

- Rust edition 2024, toolchain ≥ 1.96; workspace dep pins per the roadmap (arrow/parquet 58.3, object_store 0.13 — never bump independently).
- Component tests via `make test` (`--test-threads=2`).
- **Every existing test must stay green after every task** — especially `cache_serves_reads_after_inner_store_loses_the_object` (`crates/ukiel-query/tests/query_test.rs:294`) and the isolation tests, which exercise the read path through the (now two-mode) cache. Its files are far below the 64 MiB default threshold, so defaults keep it on the whole-file path unchanged.
- The new `crates/ukiel-query/tests/cache_test.rs` is pure object-store level (in-memory inner store, tempdir cache) — it must pass **without Docker**.
- Commit messages: conventional style, no Claude/AI attribution.
- Every task ends with `cargo fmt && cargo clippy --all-targets -- -D warnings` clean.

---

### Task 1: `CacheConfig`, prewarm on `put_opts`, HEAD passthrough

**Files:**
- Create: `crates/ukiel-query/tests/cache_test.rs`
- Modify: `crates/ukiel-query/src/cache.rs`

**Interfaces:**
- Produces: `pub struct CacheConfig { pub large_object_threshold: u64, pub chunk_size: u64, pub write_through: bool }` with `Default` (64 MiB / 8 MiB / true), and `CachingObjectStore::with_config(inner: Arc<dyn ObjectStore>, dir: PathBuf, config: CacheConfig) -> Self`. `new` keeps its signature and forwards defaults. (`large_object_threshold`/`chunk_size` are consumed in Task 3; they ride along now so `CacheConfig` is authored once.)
- Changes `put_opts`: delegate to `inner` first, then on success write the payload into the cache with the same atomic tmp+rename pattern `ensure_cached` uses (extract a `write_atomic(&self, path, bytes)` helper; `ensure_cached` calls it too). Cache-write failure is non-fatal (the upload is durable; the first read just misses like before). Emit `cache_prewarms_total{kind => "put"}` on success.
- Changes `get_opts`: **before** `ensure_cached`, branch on the whole-file copy and on `options.head`:
  1. whole-file cache copy exists → serve locally (any options, head included — the local file's metadata answers a HEAD);
  2. `options.head` → `return self.inner.get_opts(location, options).await` — a HEAD must never fetch data (this is what `OpenFile::close`'s post-upload `store.head` hits when the file was not prewarmed, e.g. `write_through = false`);
  3. otherwise the existing `ensure_cached` whole-file path (Task 3 splits this by size).

- [ ] **Step 1: Write the failing tests**

Create `crates/ukiel-query/tests/cache_test.rs`:

```rust
//! Cache-tier tests: write-through prewarm (put + multipart), HEAD
//! passthrough, and (from Task 3) range-granular chunk caching. Pure
//! object-store level — in-memory inner store, tempdir cache dir, no
//! catalog, no Docker.

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
    // HEAD is served from the local copy too (the compactor HEADs every
    // output right after writing it — rewrite.rs OpenFile::close).
    assert_eq!(cached.head(&location).await.unwrap().size, 4096);
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

#[tokio::test]
async fn head_never_downloads_the_object() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = tempfile::tempdir().unwrap();
    let cached = CachingObjectStore::new(inner.clone(), dir.path().to_path_buf());

    let location = Path::from("ht/9/L1/head-only.parquet");
    inner.put(&location, vec![7u8; 2048].into()).await.unwrap();

    let meta = cached.head(&location).await.unwrap();
    assert_eq!(meta.size, 2048);
    assert!(
        cached_file_names(dir.path()).is_empty(),
        "a HEAD must not populate the cache; dir had {:?}",
        cached_file_names(dir.path())
    );
}
```

- [ ] **Step 2: Verify the tests fail**

Run: `cargo test -p ukiel-query --test cache_test`
Expected: **compile error** — `CacheConfig` and `with_config` do not exist yet. (Once they compile, `head_never_downloads_the_object` fails on the empty-dir assertion — today's `get_opts` downloads on HEAD. That is the bug being fixed; do not weaken the tests.)

- [ ] **Step 3: Implement**

In `crates/ukiel-query/src/cache.rs`:

1. Add `CacheConfig` (doc comments per the Interfaces above) and its `Default` (`64 * 1024 * 1024` / `8 * 1024 * 1024` / `true`); add a `config: CacheConfig` field; `new` forwards to `with_config`.
2. Extract the tmp+rename block of `ensure_cached` (`cache.rs:45-47`) into:

```rust
    /// Atomic tmp+rename write. Concurrent racers may both write; the rename
    /// makes that harmless.
    async fn write_atomic(&self, path: &std::path::Path, bytes: &[u8]) -> Result<()> {
        let tmp = path.with_extension(format!("tmp-{}", uuid::Uuid::new_v4()));
        tokio::fs::write(&tmp, bytes).await.map_err(io_err)?;
        tokio::fs::rename(&tmp, path).await.map_err(io_err)?;
        Ok(())
    }
```

3. Replace `put_opts` (keep the existing counters and doc style):

```rust
    /// Delegate to `inner` first — a failed upload must not poison the cache —
    /// then prewarm the local cache with the accepted payload (write-through):
    /// ukield shares one wrapped store across roles, so ingest L0 files,
    /// deletion rewrites, and small compactor outputs land hot at write time.
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
            if self.write_atomic(&self.cache_path(location), &bytes).await.is_ok() {
                metrics::counter!("cache_prewarms_total", "kind" => "put").increment(1);
            }
        }
        Ok(result)
    }
```

4. In `get_opts`, before the `ensure_cached` call, insert the whole-file/head dispatch (extract today's file-serving tail of `get_opts` into a `serve_local_file(&self, location, path: PathBuf, options: &GetOptions) -> Result<GetResult>` helper so both branches share it):

```rust
        // A whole-file copy always wins — prewarmed writes and previously
        // read objects, HEADs included (local metadata answers them).
        let whole = self.cache_path(location);
        if tokio::fs::try_exists(&whole).await.unwrap_or(false) {
            metrics::counter!("cache_hits_total", "tier" => "file").increment(1);
            return self.serve_local_file(location, whole, &options);
        }
        // A HEAD must never fetch data (the compactor HEADs every output
        // file right after writing it; head routes through get_opts because
        // ObjectStoreExt::head is a get_opts default — it cannot be
        // overridden here).
        if options.head {
            return self.inner.get_opts(location, options).await;
        }
```

Move the `tier => "file"` hit counter out of `ensure_cached` into this branch (miss counter stays in `ensure_cached`, incremented just before the download) so hit/miss accounting is unchanged: one hit per locally-served read, one miss per download.

- [ ] **Step 4: Verify the tests pass**

Run: `cargo test -p ukiel-query --test cache_test`
Expected: all three pass.

Run: `cargo test -p ukiel-query -- --test-threads=2`
Expected: all pass, including `cache_serves_reads_after_inner_store_loses_the_object` (its `CachingObjectStore::new` call is untouched by design; note its writes now also prewarm — the test's "files were cached locally" assertion only gets easier).

- [ ] **Step 5: Lint and commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add crates/ukiel-query
git commit -m "feat: write-through prewarm on put + HEAD passthrough in the caching store"
```

---

### Task 2: Multipart prewarm tee (compactor streaming output lands hot)

**Files:**
- Modify: `crates/ukiel-query/src/cache.rs`
- Modify: `crates/ukiel-query/tests/cache_test.rs`

**Interfaces:**
- Produces (private to `cache.rs`): `struct PrewarmingUpload` implementing `MultipartUpload`, wrapping `Box<dyn MultipartUpload>` from `inner.put_multipart_opts`. Semantics:
  - `put_part(data)`: record the byte offset at **call time** (call order = byte order — `WriteMultipart` hands parts over in order even though their futures may complete out of order), then return a future that first best-effort writes the bytes to the tee's tmp file at that offset (positioned write, `std::os::unix::fs::FileExt::write_all_at`, so out-of-order completion is harmless) and then awaits the inner part upload. A tee write error sets a `poisoned` flag and is otherwise swallowed — **the upload must never fail because the prewarm did**.
  - `complete()`: `inner.complete().await?` **first** (the upload is authoritative); then, if not poisoned, rename tmp → final cache path and count `cache_prewarms_total{kind => "multipart"}`; if poisoned, remove the tmp. Rename/remove failures are swallowed.
  - `abort()`: delegate to inner, then best-effort remove the tmp.
  - `Drop` without `complete`: best-effort remove the tmp (`std::fs::remove_file`) so an errored merge leaves no tmp litter.
- Changes `put_multipart_opts`: when `write_through`, wrap the inner upload in `PrewarmingUpload` (create the tmp file up front; on tmp-create failure, fall back to the unwrapped inner upload — same best-effort rule). This is the path plan-29 compactor outputs > 10 MiB take (`ParquetObjectWriter` → `BufWriter`, 10 MiB buffer, `buffered.rs:346`); with Task 1, outputs ≤ 10 MiB take `put_opts`. Between the two, **every merge output is prewarmed**, and `OpenFile::close`'s HEAD (`rewrite.rs:319`) is served from the local copy — no object-store round trip after upload.

- [ ] **Step 1: Write the failing tests**

Append to `crates/ukiel-query/tests/cache_test.rs`:

```rust
#[tokio::test]
async fn multipart_upload_prewarms_on_complete() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = tempfile::tempdir().unwrap();
    let cached = CachingObjectStore::new(inner.clone(), dir.path().to_path_buf());

    let location = Path::from("ht/9/L2/streamed.parquet");
    let part_a: Vec<u8> = (0..6000u32).map(|i| (i % 251) as u8).collect();
    let part_b: Vec<u8> = (0..2500u32).map(|i| (i % 241) as u8).collect();

    let mut upload = cached.put_multipart(&location).await.unwrap();
    upload.put_part(part_a.clone().into()).await.unwrap();
    upload.put_part(part_b.clone().into()).await.unwrap();
    upload.complete().await.unwrap();

    let mut want = part_a;
    want.extend_from_slice(&part_b);

    // The object is in the inner store...
    let inner_bytes = inner.get(&location).await.unwrap().bytes().await.unwrap();
    assert_eq!(inner_bytes.as_ref(), want.as_slice());
    // ...and the completed upload prewarmed one whole-file cache entry
    // (no tmp litter).
    assert_eq!(
        cached_file_names(dir.path()),
        vec!["ht__9__L2__streamed.parquet".to_string()]
    );
    let cached_bytes = std::fs::read(dir.path().join("ht__9__L2__streamed.parquet")).unwrap();
    assert_eq!(cached_bytes, want);

    // The post-upload HEAD + reads are local: lose the inner object.
    inner.delete(&location).await.unwrap();
    assert_eq!(cached.head(&location).await.unwrap().size, want.len() as u64);
    let got = cached.get_range(&location, 5990..6010).await.unwrap();
    assert_eq!(got.as_ref(), &want[5990..6010]);
}

#[tokio::test]
async fn aborted_multipart_upload_leaves_no_cache_entry() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = tempfile::tempdir().unwrap();
    let cached = CachingObjectStore::new(inner.clone(), dir.path().to_path_buf());

    let location = Path::from("ht/9/L2/aborted.parquet");
    let mut upload = cached.put_multipart(&location).await.unwrap();
    upload.put_part(vec![3u8; 1024].into()).await.unwrap();
    upload.abort().await.unwrap();

    assert!(
        cached_file_names(dir.path()).is_empty(),
        "an aborted upload must leave neither a cache entry nor tmp litter; dir had {:?}",
        cached_file_names(dir.path())
    );
}

#[tokio::test]
async fn dropped_multipart_upload_leaves_no_tmp_litter() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = tempfile::tempdir().unwrap();
    let cached = CachingObjectStore::new(inner.clone(), dir.path().to_path_buf());

    let location = Path::from("ht/9/L2/dropped.parquet");
    let mut upload = cached.put_multipart(&location).await.unwrap();
    upload.put_part(vec![5u8; 512].into()).await.unwrap();
    drop(upload); // errored merge path: writer dropped without complete/abort

    assert!(
        cached_file_names(dir.path()).is_empty(),
        "a dropped upload must leave no tmp litter; dir had {:?}",
        cached_file_names(dir.path())
    );
}
```

- [ ] **Step 2: Verify the tests fail**

Run: `cargo test -p ukiel-query --test cache_test`
Expected: `multipart_upload_prewarms_on_complete` fails on the `cached_file_names` assertion (empty dir — `put_multipart_opts` still delegates without a tee). The abort/drop tests pass vacuously today (nothing writes tmp files yet); they are the regression pins for the tee's cleanup semantics.

- [ ] **Step 3: Implement**

In `crates/ukiel-query/src/cache.rs` (sketch; adapt names to the file):

```rust
/// Tees a multipart upload into a local tmp file (positioned writes, so
/// out-of-order part completion is harmless), renamed into the cache only
/// after the inner upload completes. Best-effort throughout: a tee failure
/// poisons the prewarm (tmp removed at complete), never the upload.
#[derive(Debug)]
struct PrewarmingUpload {
    inner: Box<dyn MultipartUpload>,
    file: Arc<std::fs::File>,
    tmp: PathBuf,
    dest: PathBuf,
    /// Next part's byte offset — parts arrive in call order.
    offset: u64,
    poisoned: Arc<std::sync::atomic::AtomicBool>,
    finished: bool,
}

#[async_trait]
impl MultipartUpload for PrewarmingUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        let bytes = Bytes::from(data.clone());
        let offset = self.offset;
        self.offset += bytes.len() as u64;
        let file = self.file.clone();
        let poisoned = self.poisoned.clone();
        let inner_part = self.inner.put_part(data);
        Box::pin(async move {
            use std::os::unix::fs::FileExt;
            if file.write_all_at(&bytes, offset).is_err() {
                poisoned.store(true, std::sync::atomic::Ordering::Relaxed);
            }
            inner_part.await
        })
    }

    async fn complete(&mut self) -> Result<PutResult> {
        // Inner first: the upload is authoritative, the prewarm is a bonus.
        let result = self.inner.complete().await?;
        self.finished = true;
        if self.poisoned.load(std::sync::atomic::Ordering::Relaxed) {
            let _ = std::fs::remove_file(&self.tmp);
        } else if std::fs::rename(&self.tmp, &self.dest).is_ok() {
            metrics::counter!("cache_prewarms_total", "kind" => "multipart").increment(1);
        }
        Ok(result)
    }

    async fn abort(&mut self) -> Result<()> {
        self.finished = true;
        let _ = std::fs::remove_file(&self.tmp);
        self.inner.abort().await
    }
}

impl Drop for PrewarmingUpload {
    fn drop(&mut self) {
        // Errored merge: writer dropped mid-stream. No tmp litter.
        if !self.finished {
            let _ = std::fs::remove_file(&self.tmp);
        }
    }
}
```

and replace `put_multipart_opts`:

```rust
    /// Streaming uploads (plan-29 compactor outputs over BufWriter's 10 MiB
    /// buffer) are teed into the cache; smaller streamed files flush through
    /// `put_opts` and are prewarmed there — between the two, every merge
    /// output lands hot and the compactor's post-close HEAD is local.
    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        let inner = self.inner.put_multipart_opts(location, opts).await?;
        if !self.config.write_through {
            return Ok(inner);
        }
        let dest = self.cache_path(location);
        let tmp = dest.with_extension(format!("tmp-{}", uuid::Uuid::new_v4()));
        match std::fs::File::create(&tmp) {
            Ok(file) => Ok(Box::new(PrewarmingUpload {
                inner,
                file: Arc::new(file),
                tmp,
                dest,
                offset: 0,
                poisoned: Arc::new(std::sync::atomic::AtomicBool::new(false)),
                finished: false,
            })),
            // Best-effort: no tee, upload unchanged.
            Err(_) => Ok(inner),
        }
    }
```

(`std::os::unix::fs::FileExt::write_all_at` is unix-only, matching the platform; if portability ever matters, a `Mutex<File>` + seek/write is the fallback. Blocking ~few-MiB local-disk writes inside the part future are acceptable — they overlap the network upload they tee.)

- [ ] **Step 4: Verify the tests pass**

Run: `cargo test -p ukiel-query --test cache_test`
Expected: all six pass.

Run: `cargo test -p ukiel-query -- --test-threads=2` — all pass.

- [ ] **Step 5: Lint and commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add crates/ukiel-query
git commit -m "feat: multipart prewarm tee - streamed compactor output lands hot"
```

---

### Task 3: Range-granular chunk caching for large objects

**Files:**
- Modify: `crates/ukiel-query/src/cache.rs`
- Modify: `crates/ukiel-query/tests/cache_test.rs`

**Interfaces:**
- Changes `get_opts` into a four-way dispatch (first two shipped in Task 1):
  1. a whole-file cache copy always wins (small objects, and write-through prewarms of any size — a prewarmed large object is served from its whole-file copy, never re-chunked);
  2. `options.head` → delegate to `inner.get_opts` (unchanged; additionally memoize `meta.size` from the response);
  3. otherwise discover the object size — one `inner.head` per path, memoized in a `Mutex<HashMap<String, u64>>` (immutable objects: never stale, and the entry outlives the object in the inner store, which keeps cached chunks readable after GC deletes it); size ≤ threshold → the existing `ensure_cached` whole-file path;
  4. size > threshold → chunked: compute covering chunk indices `range.start/chunk_size ..= (range.end-1)/chunk_size`, fetch each missing chunk via `inner.get_range(location, chunk_start..min(chunk_start+chunk_size, size))` into `<mangled-path>.chunk-<index>` (atomic `write_atomic` tmp+rename; concurrent racers may fetch a chunk twice — harmless, same as `ensure_cached`), then assemble the requested byte range from the chunk files (seek + read_exact per chunk, concatenate) and return it as a one-shot `Stream` payload whose bytes exactly match the returned `range`. Per chunk: `cache_hits_total`/`cache_misses_total` with `tier => "chunk"`.
- `get_ranges` keeps the default ext-trait impl (loops `get_range`, each of which lands here).
- An un-ranged `get` of a large object buffers it in memory — the Parquet read path always sends ranges (and footers come from plan-13's metadata cache after first read), so that stays unused; note it in a comment rather than engineering around it.

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

#[tokio::test]
async fn prewarmed_large_object_serves_whole_file_never_chunks() {
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

    // Written through the cache: prewarmed whole, despite being > threshold.
    let source: Vec<u8> = (0..3000u32).map(|i| (i % 253) as u8).collect();
    let location = Path::from("ht/9/L2/prewarmed-big.parquet");
    cached.put(&location, source.clone().into()).await.unwrap();
    inner.delete(&location).await.unwrap();

    // Any range serves from the whole-file copy; no chunk files appear, and
    // no HEAD to the (now empty) inner store is needed.
    let got = cached.get_range(&location, 700..2900).await.unwrap();
    assert_eq!(got.as_ref(), &source[700..2900]);
    assert_eq!(
        cached_file_names(dir.path()),
        vec!["ht__9__L2__prewarmed-big.parquet".to_string()]
    );
}
```

- [ ] **Step 2: Verify the new test fails**

Run: `cargo test -p ukiel-query --test cache_test`
Expected: `large_objects_cache_aligned_chunks_and_serve_ranges` **fails** on the `cached_file_names` assertion (today the whole 4000-byte file is downloaded, so the dir holds `ht__9__L1__big.parquet` and no chunk files). `small_objects_keep_the_whole_file_path` and `prewarmed_large_object_serves_whole_file_never_chunks` already pass — they are the regression pins Task 3 must not break. All Task-1/2 tests stay green.

- [ ] **Step 3: Implement**

In `crates/ukiel-query/src/cache.rs`, add to `CachingObjectStore` (sketch):

```rust
    fn chunk_path(&self, location: &Path, index: u64) -> PathBuf {
        self.dir
            .join(format!("{}.chunk-{index}", location.as_ref().replace('/', "__")))
    }

    /// Object size from the in-memory map, or one `head` on the inner store.
    /// Immutable objects: entries never go stale, and an entry outlives its
    /// object in the inner store — cached chunks stay readable after GC.
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
        let chunk_size = self.config.chunk_size.max(1);
        let mut out = Vec::with_capacity((range.end - range.start) as usize);
        for index in range.start / chunk_size..=(range.end - 1) / chunk_size {
            let chunk_start = index * chunk_size;
            let chunk_end = (chunk_start + chunk_size).min(size);
            let path = self.chunk_path(location, index);
            if tokio::fs::try_exists(&path).await.unwrap_or(false) {
                metrics::counter!("cache_hits_total", "tier" => "chunk").increment(1);
            } else {
                metrics::counter!("cache_misses_total", "tier" => "chunk").increment(1);
                let bytes = self.inner.get_range(location, chunk_start..chunk_end).await?;
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
```

(`sizes: Mutex<HashMap<String, u64>>` joins the struct fields; `use std::io::SeekFrom`, `tokio::io::{AsyncReadExt, AsyncSeekExt}`, `std::ops::Range`, `futures::StreamExt` join the imports.)

Then extend `get_opts` after the Task-1 whole-file/head branches:

```rust
        let size = self.object_size(location).await?;
        if size <= self.config.large_object_threshold {
            let path = self.ensure_cached(location).await?;
            return self.serve_local_file(location, path, &options);
        }

        // Large object: chunk-granular. Note: an un-ranged `get` buffers the
        // object in memory; the Parquet read path always sends ranges, so
        // that stays unused.
        let range = match &options.range {
            Some(r) => r.as_range(size).map_err(generic_err)?,
            None => 0..size,
        };
        let bytes = self.read_range_chunked(location, size, range.clone()).await?;
        Ok(GetResult {
            payload: GetResultPayload::Stream(
                futures::stream::once(async move { Ok::<_, object_store::Error>(bytes) }).boxed(),
            ),
            meta: ObjectMeta {
                location: location.clone(),
                last_modified: Utc::now(),
                size,
                e_tag: None,
                version: None,
            },
            range,
            attributes: Attributes::default(),
        })
    }
```

(In the head branch from Task 1, add the size memoization: on a successful inner `get_opts`, insert `meta.size` into `sizes` before returning — a HEAD then saves the later lookup. For a `GetResultPayload::Stream`, the stream must yield exactly the bytes of the returned `range` — which `read_range_chunked` guarantees.)

- [ ] **Step 4: Verify the tests pass**

Run: `cargo test -p ukiel-query --test cache_test`
Expected: all nine pass.

Run: `cargo test -p ukiel-query -- --test-threads=2`
Expected: all pass. `cache_serves_reads_after_inner_store_loses_the_object` stays on the whole-file path (its files are far below the 64 MiB default threshold; the only new inner-store call is one memoized `head` per object on first non-prewarmed read).

- [ ] **Step 5: Lint and commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add crates/ukiel-query
git commit -m "feat: range-granular chunk caching for large objects"
```

---

### Task 4: ukield config plumbing, monitoring-spec rows, docs, status flips

**Files:**
- Modify: `crates/ukield/src/config.rs`
- Modify: `crates/ukield/src/run.rs`
- Modify: `ukield.example.toml`
- Modify: `docs/superpowers/specs/2026-07-06-ukiel-monitoring.md`
- Modify: `docs/notes/2026-07-06-parquet-read-performance.md`
- Modify: `docs/superpowers/plans/2026-07-05-ukiel-v1-roadmap.md`

**Interfaces:**
- Adds to `QueryConfig` (`crates/ukield/src/config.rs:83`): `cache_write_through: bool`, `cache_large_object_threshold: u64`, `cache_chunk_size: u64` — defaults sourced from `ukiel_query::cache::CacheConfig::default()` (single source of truth), threaded into `CachingObjectStore::with_config` in `run.rs:83-90`. `ukield.example.toml` mentions them as comments only, since the defaults are right.

- [ ] **Step 1: Write the failing test**

In `crates/ukield/src/config.rs`, in the `parses_config_with_defaults` test, after the existing `cfg.query.listen` assertion add:

```rust
        assert!(cfg.query.cache_write_through);
        assert_eq!(cfg.query.cache_large_object_threshold, 64 * 1024 * 1024);
        assert_eq!(cfg.query.cache_chunk_size, 8 * 1024 * 1024);
```

- [ ] **Step 2: Verify it fails**

Run: `cargo test -p ukield --lib`
Expected: **compile error** — `QueryConfig` has no such fields yet.

- [ ] **Step 3: Implement**

In `crates/ukield/src/config.rs`, extend `QueryConfig` and its `Default` (match the file's doc-comment style):

```rust
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(default)]
pub struct QueryConfig {
    pub listen: String,
    /// Non-empty = wrap the store in the local cache (read-through +
    /// write-through prewarm; chunked for large objects).
    pub cache_dir: String,
    /// Prewarm the cache with written objects (ingest/compactor output).
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

In `crates/ukield/src/run.rs`, extend the import (`run.rs:17`) to `use ukiel_query::cache::{CacheConfig, CachingObjectStore};` and replace the store-wrapping arm (`run.rs:86-89`) with:

```rust
        Arc::new(CachingObjectStore::with_config(
            base_store,
            PathBuf::from(&cfg.query.cache_dir),
            CacheConfig {
                large_object_threshold: cfg.query.cache_large_object_threshold,
                chunk_size: cfg.query.cache_chunk_size,
                write_through: cfg.query.cache_write_through,
            },
        ))
```

- [ ] **Step 4: Verify**

Run: `cargo test -p ukield -- --test-threads=2`
Expected: all pass (config unit tests without Docker; `bootstrap_test`/`server_test` need Docker, same as before).

- [ ] **Step 5: Example config + monitoring spec + perf note + roadmap**

In `ukield.example.toml`, replace the `[query]` section's cache line with:

```toml
cache_dir = "" # non-empty = local disk cache (read-through + write-through prewarm)
# Cache tuning (defaults shown; uncomment only to override):
# cache_write_through = true              # prewarm the cache with every written object
# cache_large_object_threshold = 67108864 # objects over 64 MiB cache as aligned chunks
# cache_chunk_size = 8388608              # 8 MiB chunk files for large objects
```

In `docs/superpowers/specs/2026-07-06-ukiel-monitoring.md`:
- On the `cache_hits_total` / `cache_misses_total{tier}` row, note the tier values now include `chunk` (per-chunk accounting on the large-object path; `file` unchanged).
- Add a row: `cache_prewarms_total{kind}` | counter | write-through prewarms (`put` and `multipart`) — a prewarm rate far below the commit rate means writers are bypassing the cache tier.
- In the plan-15 mention at the cache-dir gauge paragraph (line ~92, "the cache dir currently has **no eviction** (plan 15 notes it)"), keep the sentence — still true; prewarm makes the gauge more important, not less.

In `docs/notes/2026-07-06-parquet-read-performance.md`, replace Tier-4 items 12 and 13 (lines 99–102) with:

```markdown
12. **Write-through cache prewarm** *(plan 15 — executed)*: `put_opts`
    prewarms the local cache after a successful upload, and a multipart tee
    mirrors the compactor's streamed outputs (plan 29) into the cache as they
    upload — ingest L0, deletion rewrites, and every merge output land hot;
    the compactor's post-close HEAD is served locally. HEADs never download
    (`get_opts` head passthrough — pre-15 the cache downloaded on HEAD).
13. **Range-granular caching** *(plan 15 — executed)*: objects over
    `cache_large_object_threshold` (64 MiB default) cache as aligned
    `cache_chunk_size` (8 MiB) chunk files fetched on demand; small objects
    keep the whole-file path; prewarmed files of any size serve whole. One
    memoized HEAD per object discovers the size.

Cache growth after plan 15: the cache dir grows with every write (prewarm)
and every large-object read (chunks); eviction remains future work —
`ukield_cache_dir_free_ratio`/`_bytes` (plan 21) are the guardrail, and the
collector's byte walk counts chunk files since it walks the dir. GC-deleted
objects leave stale cache files — harmless (part paths are UUID-fresh, the
catalog never reuses them) but they count toward disk usage; a `cache-gc`
sweep is the natural follow-up.
```

In `docs/superpowers/plans/2026-07-05-ukiel-v1-roadmap.md`, set row 15's status from **Ready to execute** to **Executed** — list what landed (put + multipart prewarm, HEAD passthrough, chunked large objects, ukield knobs) — and add 15 to the executed list in the header paragraph / recommended-order note.

- [ ] **Step 6: Full verification**

Run: `cargo fmt --check && cargo clippy --all-targets -- -D warnings && make test`
Expected: all green. Optionally `make e2e` — the e2e stack doesn't enable `cache_dir`, so this is unchanged-path insurance only; the cache tests themselves are the Docker-free net.

- [ ] **Step 7: Commit**

```bash
git add crates/ukield ukield.example.toml docs/
git commit -m "feat: thread cache tiering config through ukield"
```

---

## Self-review notes

**Coverage mapping.** Perf-note Tier-4 #12 (prewarm) → Tasks 1 (put) + 2 (multipart; required because plan 29 moved big compactor outputs to `put_multipart_opts` via `BufWriter`, `buffered.rs:346` — the old plan's `put_opts`-only design would prewarm nothing the ladder writes above 10 MiB). Tier-4 #13 (range-granular) → Task 3. The HEAD hazard (surfaced by this re-author: `ObjectStoreExt::head` defaults to `get_opts(head=true)` at `object_store-0.13.2/src/lib.rs:1363`, and `OpenFile::close` HEADs every streamed output at `rewrite.rs:319`, so a cache-enabled ukield re-downloads each merge output today) → Task 1. Config/observability/docs → Task 4.

**Type consistency.** `CacheConfig` is produced in Task 1 and consumed by `with_config` (Tasks 1–3 internally, `run.rs` in Task 4) and by `QueryConfig::default()` (Task 4) — field names match across all four sites. `PrewarmingUpload` implements the verified `MultipartUpload` trait shape (`put_part(&mut self, PutPayload) -> UploadPart` where `UploadPart = BoxFuture<'static, Result<()>>`, `upload.rs:30`). `ObjectMeta.size: u64` matches the `sizes` map value type and the test's `head().size` assertions.

**Sequencing.** Task 1 before 2 (the tee renames into the whole-file namespace Task 1's dispatch checks first, and reuses `write_atomic`'s tmp convention); Tasks 1–2 before 3 (the whole-file-wins and head branches must precede the size lookup, or a prewarmed large object would be re-chunked and a HEAD would trigger a chunk fetch); Task 4 last (wiring + flips). Each task leaves all existing tests green: defaults (64 MiB threshold) keep every current caller on the whole-file path, `new()` keeps its signature, and write-through only adds cache entries (never changes read results — objects are immutable).

**Safety argument restated.** The upload is always authoritative: `put_opts` delegates first and prewarms only on success; the tee completes the inner upload before renaming, poisons itself on any local write error, and cleans its tmp on abort/drop — so no failure mode of the cache tier can fail, corrupt, or reorder a durable write, and a corrupt prewarm cannot be installed (poisoned ⇒ tmp removed, never renamed). Reads: chunk files hold aligned slices verified byte-for-byte in tests; a `GetResultPayload::Stream` yields exactly the returned `range`; the memoized size is safe because parts are immutable and paths are never reused (UUID-fresh), which also keeps post-GC chunk serving consistent with the existing whole-file behavior the `cache_serves_reads_after_inner_store_loses_the_object` test pins.

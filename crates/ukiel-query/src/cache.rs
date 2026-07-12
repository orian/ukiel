//! Whole-file read-through cache: the local-disk tier of the spec's 2-layer
//! cache. Reads (`get_opts`, which every `get`/`get_range`/`get_ranges` call
//! routes through in object_store 0.13) are served from a local copy
//! downloaded once per object; everything else delegates to `inner`.

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
    PutResult, Result, UploadPart,
};
use tokio::io::{AsyncReadExt, AsyncSeekExt};

/// Tuning knobs for [`CachingObjectStore`]; `Default` is the production
/// configuration (write-through on, 64 MiB large-object threshold, 8 MiB
/// chunks).
#[derive(Debug, Clone, PartialEq)]
pub struct CacheConfig {
    /// Objects strictly larger than this many bytes are cached as aligned
    /// chunk files instead of one whole-file copy.
    pub large_object_threshold: u64,
    /// Chunk size in bytes for large-object caching.
    pub chunk_size: u64,
    /// Prewarm the cache with every successfully written object.
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
    /// Memoized object sizes (one `head` per path). Objects are immutable,
    /// so entries never go stale.
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
        self.dir.join(format!(
            "{}.chunk-{index}",
            location.as_ref().replace('/', "__")
        ))
    }

    /// Object size from the in-memory map, or one `head` on the inner store.
    /// Immutable objects: entries never go stale, and an entry outlives its
    /// object in the inner store — cached chunks stay readable after GC.
    async fn object_size(&self, location: &Path) -> Result<u64> {
        if let Some(size) = self
            .sizes
            .lock()
            .expect("sizes lock")
            .get(location.as_ref())
        {
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
        metrics::counter!("cache_misses_total", "tier" => "file").increment(1);
        let bytes = self.inner.get(location).await?.bytes().await?;
        self.write_atomic(&path, &bytes).await?;
        Ok(path)
    }

    /// Serve a read from a local cache file. The `File` payload lets
    /// object_store seek/slice the requested range itself.
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
}

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
            if self
                .write_atomic(&self.cache_path(location), &bytes)
                .await
                .is_ok()
            {
                metrics::counter!("cache_prewarms_total", "kind" => "put").increment(1);
            }
        }
        Ok(result)
    }

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

    /// Serve reads from the local cache. Every `get`/`get_range`/`get_ranges`/
    /// `head` in object_store 0.13 routes through here, so caching this one
    /// method covers the whole Parquet read path.
    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
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
        // overridden here). Memoize the size it reports: it saves the later
        // lookup on the chunked path.
        if options.head {
            let result = self.inner.get_opts(location, options).await?;
            self.sizes
                .lock()
                .expect("sizes lock")
                .insert(location.as_ref().to_string(), result.meta.size);
            return Ok(result);
        }

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
        let bytes = self
            .read_range_chunked(location, size, range.clone())
            .await?;
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

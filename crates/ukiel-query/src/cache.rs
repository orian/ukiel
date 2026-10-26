//! Bounded, disposable local cache for immutable object paths. Published files,
//! active reads and temporary fills share one byte/entry budget. Admission/fill
//! failures bypass caching; cached bytes do not grant extra object-read lifetime.
mod disk;

use std::fmt;
use std::num::NonZeroUsize;
use std::ops::Range;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use chrono::Utc;
use futures::{StreamExt, stream::BoxStream};
use lru::LruCache;
use object_store::path::Path;
use object_store::{
    Attributes, CopyOptions, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload,
    PutResult, Result, UploadPart,
};

pub use disk::CacheStats;
use disk::{DiskCache, Fill, rejected};

/// Limits are per process/cache directory. Bytes include temporary reservations;
/// filesystem block rounding and directory/inode overhead are additional and
/// bounded by the entry cap. Use a dedicated directory, exclusively owned by one
/// store. Startup reindexes/trims existing files and removes interrupted fills.
#[derive(Debug, Clone, PartialEq)]
pub struct CacheConfig {
    pub large_object_threshold: u64,
    pub chunk_size: u64,
    pub write_through: bool,
    pub max_bytes: u64,
    /// Completed entries plus active fills (also bounds empty-file/inode churn).
    pub max_entries: usize,
    /// Independent LRU for object sizes; zero disables size memoization.
    pub size_entries: usize,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            large_object_threshold: 64 * 1024 * 1024,
            chunk_size: 8 * 1024 * 1024,
            write_through: true,
            max_bytes: 16 * 1024 * 1024 * 1024,
            max_entries: 100_000,
            size_entries: 10_000,
        }
    }
}

#[derive(Debug)]
pub struct CachingObjectStore {
    inner: Arc<dyn ObjectStore>,
    config: CacheConfig,
    disk: Option<Arc<DiskCache>>,
    sizes: Mutex<Option<LruCache<String, u64>>>,
}

impl CachingObjectStore {
    pub fn new(inner: Arc<dyn ObjectStore>, dir: PathBuf) -> Self {
        Self::with_config(inner, dir, CacheConfig::default())
    }

    /// An unavailable/locked cache directory disables data caching. Backing
    /// storage remains authoritative and usable; a warning and rejection metric
    /// expose the degraded cache. Daemon configuration validates positive limits.
    pub fn with_config(inner: Arc<dyn ObjectStore>, dir: PathBuf, config: CacheConfig) -> Self {
        let disk = match DiskCache::open(dir.clone(), &config) {
            Ok(disk) => Some(disk),
            Err(error) => {
                rejected("startup");
                tracing::warn!(path = %dir.display(), %error, "cache unavailable; using backing storage");
                None
            }
        };
        let sizes = NonZeroUsize::new(config.size_entries).map(LruCache::new);
        metrics::gauge!("cache_enabled").set(f64::from(disk.is_some()));
        metrics::gauge!("cache_capacity_bytes").set(config.max_bytes as f64);
        metrics::gauge!("cache_capacity_entries").set(config.max_entries as f64);
        metrics::gauge!("cache_size_capacity_entries").set(config.size_entries as f64);
        metrics::gauge!("cache_size_entries").set(0.0);
        Self {
            inner,
            disk,
            config,
            sizes: Mutex::new(sizes),
        }
    }

    pub fn stats(&self) -> CacheStats {
        let mut stats = self
            .disk
            .as_ref()
            .map_or_else(CacheStats::default, |d| d.stats());
        stats.size_entries = self
            .sizes
            .lock()
            .expect("size cache")
            .as_ref()
            .map_or(0, LruCache::len);
        stats
    }

    fn key(location: &Path) -> String {
        // Fixed-size keys bound key memory, avoid flattened-path aliases
        // (a/b versus a__b), and stay within filesystem name limits.
        blake3::hash(location.as_ref().as_bytes())
            .to_hex()
            .to_string()
    }
    fn whole_name(location: &Path) -> String {
        format!("{}.file", Self::key(location))
    }

    fn memoize_size(&self, location: &Path, size: u64) {
        if let Some(sizes) = self.sizes.lock().expect("size cache").as_mut() {
            let key = Self::key(location);
            if !sizes.contains(&key) && sizes.len() == sizes.cap().get() {
                metrics::counter!("cache_size_evictions_total").increment(1);
            }
            sizes.put(key, size);
            metrics::gauge!("cache_size_entries").set(sizes.len() as f64);
        }
    }
    async fn object_size(&self, location: &Path) -> Result<u64> {
        if let Some(size) = self
            .sizes
            .lock()
            .expect("size cache")
            .as_mut()
            .and_then(|sizes| sizes.get(&Self::key(location)).copied())
        {
            return Ok(size);
        }
        let size = self.inner.head(location).await?.size;
        self.memoize_size(location, size);
        Ok(size)
    }

    async fn save(fill: Fill, payload: PutPayload) -> bool {
        // Cancellation does not release the reservation before blocking I/O ends.
        let saved = tokio::task::spawn_blocking(move || {
            fill.write_at(&payload, 0)?;
            fill.publish()
        })
        .await
        .is_ok_and(|r| r.is_ok());
        if !saved {
            metrics::counter!("cache_errors_total", "operation" => "fill").increment(1);
            rejected("io");
        }
        saved
    }

    async fn read_range_chunked(
        &self,
        disk: &Arc<DiskCache>,
        location: &Path,
        size: u64,
        range: Range<u64>,
    ) -> Result<Bytes> {
        if range.start == range.end {
            return Ok(Bytes::new());
        }
        let chunk_size = self.config.chunk_size;
        let mut out = Vec::with_capacity((range.end - range.start) as usize);
        for index in range.start / chunk_size..=(range.end - 1) / chunk_size {
            let start = index * chunk_size;
            let end = start.saturating_add(chunk_size).min(size);
            let lo = range.start.max(start);
            let hi = range.end.min(end);
            let name = format!("{}.{}.chunk-{index}", Self::key(location), chunk_size);
            if let Some(read) = disk.read(&name) {
                if let Ok(bytes) = read.range(lo - start..hi - start).await {
                    metrics::counter!("cache_hits_total", "tier" => "chunk").increment(1);
                    out.extend_from_slice(&bytes);
                    continue;
                }
                rejected("io");
            }
            metrics::counter!("cache_misses_total", "tier" => "chunk").increment(1);
            if let Some(fill) = disk.reserve(&name, end - start) {
                let bytes = self.inner.get_range(location, start..end).await?;
                let slice = bytes.slice((lo - start) as usize..(hi - start) as usize);
                Self::save(fill, bytes.into()).await;
                out.extend_from_slice(&slice);
            } else {
                out.extend_from_slice(&self.inner.get_range(location, lo..hi).await?);
            }
        }
        Ok(out.into())
    }
}

/// All part tasks share the reservation, including cancelled blocking writes.
#[derive(Debug)]
struct Tee {
    fill: Fill,
    poisoned: bool,
}

#[derive(Debug)]
struct PrewarmingUpload {
    inner: Box<dyn MultipartUpload>,
    tee: Option<Arc<Mutex<Tee>>>,
    offset: u64,
}

#[async_trait]
impl MultipartUpload for PrewarmingUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        let offset = self.offset;
        self.offset += data.content_length() as u64;
        let admitted = self.tee.as_ref().is_some_and(|tee| {
            let mut tee = tee.lock().expect("upload tee");
            let admitted = tee.fill.grow(data.content_length() as u64);
            if !admitted {
                tee.poisoned = true;
            }
            admitted
        });
        if !admitted {
            self.tee.take();
        }
        let tee = self.tee.clone();
        let payload = tee.as_ref().map(|_| data.clone());
        let inner = self.inner.put_part(data);
        Box::pin(async move {
            let task = tee.zip(payload).map(|(tee, payload)| {
                tokio::task::spawn_blocking(move || {
                    let mut tee = tee.lock().expect("upload tee");
                    if !tee.poisoned && tee.fill.write_at(&payload, offset).is_err() {
                        tee.poisoned = true;
                        rejected("io");
                    }
                })
            });
            let result = inner.await;
            if let Some(task) = task {
                let _ = task.await;
            }
            result
        })
    }
    async fn complete(&mut self) -> Result<PutResult> {
        let result = self.inner.complete().await?;
        if let Some(tee) = self.tee.take()
            && let Ok(tee) = Arc::try_unwrap(tee)
        {
            let tee = tee.into_inner().expect("upload tee");
            if !tee.poisoned {
                let saved = tokio::task::spawn_blocking(move || tee.fill.publish())
                    .await
                    .is_ok_and(|r| r.is_ok());
                if saved {
                    metrics::counter!("cache_prewarms_total", "kind" => "multipart").increment(1);
                } else {
                    rejected("io");
                }
            }
        }
        Ok(result)
    }
    async fn abort(&mut self) -> Result<()> {
        self.tee.take();
        self.inner.abort().await
    }
}

fn io_err(e: std::io::Error) -> object_store::Error {
    metrics::counter!("cache_errors_total", "operation" => "read").increment(1);
    generic_err(e)
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
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        let prewarm = self.config.write_through.then(|| payload.clone());
        let result = self.inner.put_opts(location, payload, opts).await?;
        if let Some(payload) = prewarm
            && let Some(disk) = &self.disk
            && let Some(fill) =
                disk.reserve(&Self::whole_name(location), payload.content_length() as u64)
            && Self::save(fill, payload).await
        {
            metrics::counter!("cache_prewarms_total", "kind" => "put").increment(1);
        }
        Ok(result)
    }
    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        let inner = self.inner.put_multipart_opts(location, opts).await?;
        if self.config.write_through
            && let Some(disk) = &self.disk
            && let Some(fill) = disk.reserve(&Self::whole_name(location), 0)
        {
            return Ok(Box::new(PrewarmingUpload {
                inner,
                tee: Some(Arc::new(Mutex::new(Tee {
                    fill,
                    poisoned: false,
                }))),
                offset: 0,
            }));
        }
        Ok(inner)
    }
    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let Some(disk) = &self.disk else {
            return self.inner.get_opts(location, options).await;
        };
        if options.version.is_some()
            || options.if_match.is_some()
            || options.if_none_match.is_some()
            || options.if_modified_since.is_some()
            || options.if_unmodified_since.is_some()
        {
            return self.inner.get_opts(location, options).await;
        }
        let name = Self::whole_name(location);
        if let Some(read) = disk.read(&name) {
            let range = options.range.as_ref().map_or(Ok(0..read.bytes), |r| {
                r.as_range(read.bytes).map_err(generic_err)
            })?;
            metrics::counter!("cache_hits_total", "tier" => "file").increment(1);
            return Ok(GetResult {
                meta: ObjectMeta {
                    location: location.clone(),
                    last_modified: Utc::now(),
                    size: read.bytes,
                    e_tag: None,
                    version: None,
                },
                payload: GetResultPayload::Stream(read.stream(range.clone())),
                range,
                attributes: Attributes::default(),
            });
        }
        if options.head {
            let result = self.inner.get_opts(location, options).await?;
            self.memoize_size(location, result.meta.size);
            return Ok(result);
        }
        let size = self.object_size(location).await?;
        if size <= self.config.large_object_threshold {
            metrics::counter!("cache_misses_total", "tier" => "file").increment(1);
            let Some(fill) = disk.reserve(&name, size) else {
                return self.inner.get_opts(location, options).await;
            };
            let result = self.inner.get(location).await?;
            let meta = result.meta.clone();
            let attributes = result.attributes.clone();
            let bytes = result.bytes().await?;
            if bytes.len() as u64 != size {
                drop(fill);
                return self.inner.get_opts(location, options).await;
            }
            let range = options
                .range
                .as_ref()
                .map_or(Ok(0..size), |r| r.as_range(size).map_err(generic_err))?;
            let slice = bytes.slice(range.start as usize..range.end as usize);
            Self::save(fill, bytes.into()).await;
            return Ok(GetResult {
                payload: GetResultPayload::Stream(
                    futures::stream::once(async move { Ok(slice) }).boxed(),
                ),
                meta,
                range,
                attributes,
            });
        }
        let Some(ref requested) = options.range else {
            return self.inner.get_opts(location, options).await;
        };
        let range = requested.as_range(size).map_err(generic_err)?;
        let bytes = self
            .read_range_chunked(disk, location, size, range.clone())
            .await?;
        Ok(GetResult {
            payload: GetResultPayload::Stream(
                futures::stream::once(async move { Ok(bytes) }).boxed(),
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
        // Keep disposable copies for already-planned queries; LRU capacity
        // reclamation is independent of authorization to delete backing data.
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

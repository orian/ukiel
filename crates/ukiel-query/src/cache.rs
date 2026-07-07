//! Whole-file read-through cache: the local-disk tier of the spec's 2-layer
//! cache. Reads (`get_opts`, which every `get`/`get_range`/`get_ranges` call
//! routes through in object_store 0.13) are served from a local copy
//! downloaded once per object; everything else delegates to `inner`.

use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    Attributes, CopyOptions, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload,
    PutResult, Result,
};

#[derive(Debug)]
pub struct CachingObjectStore {
    inner: Arc<dyn ObjectStore>,
    dir: PathBuf,
}

impl CachingObjectStore {
    pub fn new(inner: Arc<dyn ObjectStore>, dir: PathBuf) -> Self {
        Self { inner, dir }
    }

    fn cache_path(&self, location: &Path) -> PathBuf {
        self.dir.join(location.as_ref().replace('/', "__"))
    }

    /// Downloads the whole object once; concurrent racers may download twice,
    /// the atomic rename makes that harmless.
    async fn ensure_cached(&self, location: &Path) -> Result<PathBuf> {
        let path = self.cache_path(location);
        if tokio::fs::try_exists(&path).await.unwrap_or(false) {
            metrics::counter!("cache_hits_total", "tier" => "file").increment(1);
            return Ok(path);
        }
        metrics::counter!("cache_misses_total", "tier" => "file").increment(1);
        let bytes = self.inner.get(location).await?.bytes().await?;
        let tmp = path.with_extension(format!("tmp-{}", uuid::Uuid::new_v4()));
        tokio::fs::write(&tmp, &bytes).await.map_err(io_err)?;
        tokio::fs::rename(&tmp, &path).await.map_err(io_err)?;
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
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

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

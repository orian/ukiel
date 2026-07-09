//! Process-wide Parquet footer/metadata cache.
//!
//! Every query otherwise re-fetches and re-parses every file's footer (~2
//! object-store roundtrips per file). Ukiel parts are IMMUTABLE — a path's
//! bytes never change once committed (compaction writes new paths and
//! tombstones old ones) — so cached metadata never invalidates; the cache
//! only needs a size bound (LRU on entry count).

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

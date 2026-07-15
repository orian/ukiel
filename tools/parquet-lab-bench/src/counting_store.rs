//! An `ObjectStore` wrapper that accounts for every read the benchmark issues.
//!
//! Object-store confirmation must not infer I/O from output file size. This wrapper counts
//! the requests and requested bytes DataFusion actually issues — HEADs for object size,
//! `get_opts` reads (footer and page-index fetches ride here), and coalesced range reads —
//! by request kind, so a report can price the metadata and data phases separately. It never
//! writes: `put`/`delete`/`copy` delegate unchanged, because the benchmark is read-only.

use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result,
};

/// The counters, snapshotted before and after each query to attribute I/O to it.
#[derive(Debug, Default)]
pub struct Counters {
    pub head_requests: AtomicU64,
    pub get_requests: AtomicU64,
    pub get_range_requests: AtomicU64,
    /// Total individual ranges across all `get_ranges` calls (a coalesced call may carry
    /// several).
    pub ranges: AtomicU64,
    pub requested_bytes: AtomicU64,
}

/// A plain-data snapshot of the counters, for a report.
#[derive(Debug, Clone, Copy, serde::Serialize, PartialEq, Eq)]
pub struct CounterSnapshot {
    pub head_requests: u64,
    pub get_requests: u64,
    pub get_range_requests: u64,
    pub ranges: u64,
    pub requested_bytes: u64,
}

impl Counters {
    pub fn snapshot(&self) -> CounterSnapshot {
        CounterSnapshot {
            head_requests: self.head_requests.load(Ordering::Relaxed),
            get_requests: self.get_requests.load(Ordering::Relaxed),
            get_range_requests: self.get_range_requests.load(Ordering::Relaxed),
            ranges: self.ranges.load(Ordering::Relaxed),
            requested_bytes: self.requested_bytes.load(Ordering::Relaxed),
        }
    }
}

impl CounterSnapshot {
    /// The I/O attributable to the window between two snapshots (after − before).
    pub fn delta(after: &CounterSnapshot, before: &CounterSnapshot) -> CounterSnapshot {
        CounterSnapshot {
            head_requests: after.head_requests - before.head_requests,
            get_requests: after.get_requests - before.get_requests,
            get_range_requests: after.get_range_requests - before.get_range_requests,
            ranges: after.ranges - before.ranges,
            requested_bytes: after.requested_bytes - before.requested_bytes,
        }
    }
}

/// An `ObjectStore` that counts reads and delegates everything to an inner store.
#[derive(Debug)]
pub struct CountingObjectStore {
    inner: Arc<dyn ObjectStore>,
    counters: Arc<Counters>,
}

impl std::fmt::Display for CountingObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CountingObjectStore({})", self.inner)
    }
}

impl CountingObjectStore {
    pub fn new(inner: Arc<dyn ObjectStore>) -> (Arc<Self>, Arc<Counters>) {
        let counters = Arc::new(Counters::default());
        let store = Arc::new(CountingObjectStore {
            inner,
            counters: counters.clone(),
        });
        (store, counters)
    }
}

#[async_trait]
impl ObjectStore for CountingObjectStore {
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

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        // A metadata HEAD arrives here as `options.head`; a footer/page-index/data read as a
        // bounded range. Classify them so the report can price the phases separately.
        if options.head {
            self.counters.head_requests.fetch_add(1, Ordering::Relaxed);
        } else {
            self.counters.get_requests.fetch_add(1, Ordering::Relaxed);
        }
        if let Some(object_store::GetRange::Bounded(r)) = &options.range {
            self.counters
                .requested_bytes
                .fetch_add(r.end.saturating_sub(r.start), Ordering::Relaxed);
        }
        self.inner.get_opts(location, options).await
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        self.counters
            .get_range_requests
            .fetch_add(1, Ordering::Relaxed);
        self.counters
            .ranges
            .fetch_add(ranges.len() as u64, Ordering::Relaxed);
        let bytes: u64 = ranges.iter().map(|r| r.end.saturating_sub(r.start)).sum();
        self.counters
            .requested_bytes
            .fetch_add(bytes, Ordering::Relaxed);
        self.inner.get_ranges(location, ranges).await
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

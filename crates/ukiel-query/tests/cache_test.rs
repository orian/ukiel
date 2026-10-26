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
        .filter(|name| name != ".ukiel-cache.lock")
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
        cached_file_names(dir.path()).len() == 1,
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
    assert_eq!(cached_file_names(dir.path()).len(), 1);
    let cached_bytes = std::fs::read(dir.path().join(&cached_file_names(dir.path())[0])).unwrap();
    assert_eq!(cached_bytes, want);

    // The post-upload HEAD + reads are local: lose the inner object.
    inner.delete(&location).await.unwrap();
    assert_eq!(
        cached.head(&location).await.unwrap().size,
        want.len() as u64
    );
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
            ..CacheConfig::default()
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
        cached_file_names(dir.path())
            .iter()
            .map(|n| n.rsplit('.').next().unwrap())
            .collect::<Vec<_>>(),
        vec!["chunk-1", "chunk-2", "chunk-7"],
        "expected only the covering chunk files"
    );

    // Chunk files hold aligned slices: chunk 1 is exactly source[512..1024).
    let names = cached_file_names(dir.path());
    let chunk1 = std::fs::read(
        dir.path()
            .join(names.iter().find(|n| n.ends_with(".chunk-1")).unwrap()),
    )
    .unwrap();
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
            ..CacheConfig::default()
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
        cached_file_names(dir.path()).len(),
        1,
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
            ..CacheConfig::default()
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
    assert_eq!(cached_file_names(dir.path()).len(), 1);
}

fn bounded(bytes: u64, entries: usize) -> CacheConfig {
    CacheConfig {
        max_bytes: bytes,
        max_entries: entries,
        size_entries: 2,
        ..CacheConfig::default()
    }
}

fn assert_bound(cached: &CachingObjectStore, dir: &std::path::Path, cfg: &CacheConfig) {
    let stats = cached.stats();
    assert!(
        stats.resident_bytes + stats.reserved_bytes <= cfg.max_bytes,
        "{stats:?}"
    );
    assert!(
        stats.entries + stats.inflight_entries <= cfg.max_entries,
        "{stats:?}"
    );
    assert!(stats.size_entries <= cfg.size_entries, "{stats:?}");
    let names = cached_file_names(dir);
    assert!(names.len() <= cfg.max_entries, "{names:?}");
    let physical_len: u64 = names
        .iter()
        .map(|n| std::fs::metadata(dir.join(n)).unwrap().len())
        .sum();
    assert!(
        physical_len <= cfg.max_bytes,
        "{physical_len} bytes, {stats:?}"
    );
}

#[tokio::test]
async fn whole_file_lru_reclaims_cold_entries_and_preserves_recent_reads() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = tempfile::tempdir().unwrap();
    let cfg = bounded(16, 2);
    let cached = CachingObjectStore::with_config(inner.clone(), dir.path().into(), cfg.clone());
    let a = Path::from("a");
    let b = Path::from("b");
    let c = Path::from("c");
    for (p, value) in [(&a, 1), (&b, 2)] {
        cached.put(p, vec![value; 8].into()).await.unwrap();
    }
    assert_eq!(
        cached
            .get(&a)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
            .as_ref(),
        &[1; 8]
    );
    cached.put(&c, vec![3; 8].into()).await.unwrap();
    for p in [&a, &b, &c] {
        inner.delete(p).await.unwrap();
    }
    assert_eq!(
        cached
            .get(&a)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
            .as_ref(),
        &[1; 8]
    );
    assert_eq!(
        cached
            .get(&c)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
            .as_ref(),
        &[3; 8]
    );
    assert!(
        cached.get(&b).await.is_err(),
        "cold b must have been evicted"
    );
    assert_bound(&cached, dir.path(), &cfg);
}

#[tokio::test]
async fn chunk_working_set_larger_than_budget_reads_correctly() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = tempfile::tempdir().unwrap();
    let cfg = CacheConfig {
        large_object_threshold: 1,
        chunk_size: 512,
        ..bounded(1024, 2)
    };
    let cached = CachingObjectStore::with_config(inner.clone(), dir.path().into(), cfg.clone());
    let path = Path::from("chunks");
    let source: Vec<u8> = (0..4096).map(|i| (i % 251) as u8).collect();
    inner.put(&path, source.clone().into()).await.unwrap();
    assert_eq!(
        cached.get_range(&path, 0..4096).await.unwrap().as_ref(),
        &source
    );
    assert_bound(&cached, dir.path(), &cfg);
    inner.delete(&path).await.unwrap();
    assert_eq!(
        cached.get_range(&path, 3072..4096).await.unwrap().as_ref(),
        &source[3072..]
    );
    assert!(cached.get_range(&path, 0..512).await.is_err());
}

#[tokio::test]
async fn pinned_response_survives_pressure_and_cancellation_releases_capacity() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = tempfile::tempdir().unwrap();
    let cfg = bounded(8, 1);
    let cached = CachingObjectStore::with_config(inner.clone(), dir.path().into(), cfg.clone());
    let a = Path::from("pinned");
    let b = Path::from("bypassed");
    cached.put(&a, vec![1; 8].into()).await.unwrap();
    let held = cached.get(&a).await.unwrap();
    cached.put(&b, vec![2; 8].into()).await.unwrap();
    assert_eq!(
        cached
            .get(&b)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
            .as_ref(),
        &[2; 8]
    );
    assert_bound(&cached, dir.path(), &cfg);
    inner.delete(&a).await.unwrap();
    assert_eq!(held.bytes().await.unwrap().as_ref(), &[1; 8]);
    let cancelled = cached.get(&a).await.unwrap();
    drop(cancelled);
    cached.get(&b).await.unwrap().bytes().await.unwrap();
    inner.delete(&b).await.unwrap();
    assert_eq!(
        cached
            .get(&b)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
            .as_ref(),
        &[2; 8]
    );
    assert!(
        cached.get(&a).await.is_err(),
        "a was reclaimable after its response dropped"
    );
}

#[tokio::test]
async fn oversized_puts_reads_and_multipart_prewarming_bypass_without_losing_data() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = tempfile::tempdir().unwrap();
    let cfg = bounded(8, 2);
    let cached = CachingObjectStore::with_config(inner.clone(), dir.path().into(), cfg.clone());
    let path = Path::from("oversized");
    cached.put(&path, vec![7; 32].into()).await.unwrap();
    assert_eq!(
        cached.get_range(&path, 5..9).await.unwrap().as_ref(),
        &[7; 4]
    );
    assert_eq!(cached.stats().resident_bytes, 0);
    let multipart = Path::from("multipart");
    let mut upload = cached.put_multipart(&multipart).await.unwrap();
    upload.put_part(vec![1; 6].into()).await.unwrap();
    assert_eq!(cached.stats().reserved_bytes, 6);
    assert_bound(&cached, dir.path(), &cfg);
    upload.put_part(vec![2; 6].into()).await.unwrap();
    assert_eq!(
        cached.stats().reserved_bytes,
        0,
        "overflow abandons only the cache tee"
    );
    upload.complete().await.unwrap();
    let got = inner.get(&multipart).await.unwrap().bytes().await.unwrap();
    assert_eq!(got.as_ref(), &[1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2]);
    assert_bound(&cached, dir.path(), &cfg);
}

#[tokio::test]
async fn out_of_order_part_futures_share_one_reservation() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = tempfile::tempdir().unwrap();
    let cfg = bounded(8, 1);
    let cached = CachingObjectStore::with_config(inner.clone(), dir.path().into(), cfg.clone());
    let path = Path::from("parts");
    let mut upload = cached.put_multipart(&path).await.unwrap();
    let first = upload.put_part(vec![1; 3].into());
    let last = upload.put_part(vec![2; 5].into());
    assert_eq!(cached.stats().reserved_bytes, 8);
    let (last, first) = tokio::join!(last, first);
    last.unwrap();
    first.unwrap();
    upload.complete().await.unwrap();
    inner.delete(&path).await.unwrap();
    assert_eq!(
        cached
            .get(&path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
            .as_ref(),
        &[1, 1, 1, 2, 2, 2, 2, 2]
    );
    assert_bound(&cached, dir.path(), &cfg);
}

#[tokio::test]
async fn unpolled_part_retains_capacity_after_upload_drop_until_it_is_cancelled() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = tempfile::tempdir().unwrap();
    let cfg = bounded(8, 1);
    let cached = CachingObjectStore::with_config(inner, dir.path().into(), cfg.clone());
    let mut upload = cached
        .put_multipart(&Path::from("abandoned"))
        .await
        .unwrap();
    let part = upload.put_part(vec![7; 8].into());
    drop(upload);
    assert_eq!(cached.stats().reserved_bytes, 8);
    assert_bound(&cached, dir.path(), &cfg);
    drop(part);
    assert_eq!(cached.stats().reserved_bytes, 0);
    assert!(cached_file_names(dir.path()).is_empty());
}

#[tokio::test]
async fn a_partly_consumed_stream_stays_pinned_across_pressure() {
    use futures::StreamExt;
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = tempfile::tempdir().unwrap();
    let len = 2 * 1024 * 1024 + 17;
    let cfg = bounded(len as u64, 1);
    let cached = CachingObjectStore::with_config(inner.clone(), dir.path().into(), cfg.clone());
    let a = Path::from("streaming");
    let b = Path::from("pressure");
    cached.put(&a, vec![1; len].into()).await.unwrap();
    let mut stream = cached.get(&a).await.unwrap().into_stream();
    let first = stream.next().await.unwrap().unwrap();
    assert_eq!(first.len(), 1024 * 1024);
    assert!(first.iter().all(|v| *v == 1));
    cached.put(&b, vec![2; len].into()).await.unwrap();
    inner.delete(&a).await.unwrap();
    let mut observed = first.len();
    while let Some(bytes) = stream.next().await {
        let bytes = bytes.unwrap();
        assert!(bytes.iter().all(|v| *v == 1));
        observed += bytes.len();
    }
    assert_eq!(observed, len);
    assert_bound(&cached, dir.path(), &cfg);
    drop(stream);
    cached.put(&b, vec![2; len].into()).await.unwrap();
    inner.delete(&b).await.unwrap();
    assert_eq!(
        cached.get_range(&b, 0..17).await.unwrap().as_ref(),
        &[2; 17]
    );
}

#[tokio::test]
async fn restart_trims_existing_files_and_removes_interrupted_temporary_files() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = tempfile::tempdir().unwrap();
    let cached = CachingObjectStore::with_config(inner.clone(), dir.path().into(), bounded(24, 3));
    for i in 0..3 {
        cached
            .put(&Path::from(format!("obj-{i}")), vec![i; 8].into())
            .await
            .unwrap();
    }
    drop(cached);
    let interrupted = dir.path().join(format!(
        "{}.tmp-{}",
        cached_file_names(dir.path())[0],
        uuid::Uuid::new_v4()
    ));
    std::fs::write(&interrupted, vec![0; 100]).unwrap();
    let cfg = bounded(8, 1);
    let cached = CachingObjectStore::with_config(inner, dir.path().into(), cfg.clone());
    assert!(!interrupted.exists());
    assert_eq!(cached.stats().entries, 1);
    assert_bound(&cached, dir.path(), &cfg);
    for i in 0..3 {
        assert_eq!(
            cached
                .get(&Path::from(format!("obj-{i}")))
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap()
                .as_ref(),
            &[i; 8]
        );
        assert_bound(&cached, dir.path(), &cfg);
    }
}

#[tokio::test]
async fn a_second_owner_bypasses_until_the_last_old_read_is_dropped() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = tempfile::tempdir().unwrap();
    let cfg = bounded(8, 1);
    let cached = CachingObjectStore::with_config(inner.clone(), dir.path().into(), cfg.clone());
    let a = Path::from("old");
    let b = Path::from("new");
    cached.put(&a, vec![1; 8].into()).await.unwrap();
    let read = cached.get(&a).await.unwrap();
    drop(cached);
    let other = CachingObjectStore::with_config(inner.clone(), dir.path().into(), cfg.clone());
    other.put(&b, vec![2; 8].into()).await.unwrap();
    assert_eq!(
        other.stats().entries,
        0,
        "the still-live owner prevents a second cache budget"
    );
    assert_eq!(
        other.get(&b).await.unwrap().bytes().await.unwrap().as_ref(),
        &[2; 8]
    );
    drop(other);
    drop(read);
    let restarted = CachingObjectStore::with_config(inner, dir.path().into(), cfg.clone());
    assert_eq!(restarted.stats().entries, 1);
    assert_bound(&restarted, dir.path(), &cfg);
}

#[tokio::test]
async fn cache_io_failure_bypasses_and_path_aliases_do_not_collide() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = tempfile::tempdir().unwrap();
    let cached = CachingObjectStore::new(inner.clone(), dir.path().into());
    let a = Path::from("a/b");
    let b = Path::from("a__b");
    cached.put(&a, vec![1; 8].into()).await.unwrap();
    cached.put(&b, vec![2; 8].into()).await.unwrap();
    assert_eq!(
        cached
            .get(&a)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
            .as_ref(),
        &[1; 8]
    );
    assert_eq!(
        cached
            .get(&b)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
            .as_ref(),
        &[2; 8]
    );
    std::fs::remove_dir_all(dir.path()).unwrap();
    let c = Path::from("uncacheable");
    cached.put(&c, vec![3; 8].into()).await.unwrap();
    assert_eq!(
        cached
            .get(&c)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
            .as_ref(),
        &[3; 8]
    );
}

#[tokio::test]
async fn empty_files_and_head_only_metadata_are_bounded() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = tempfile::tempdir().unwrap();
    let cfg = bounded(1024, 4);
    let cached = CachingObjectStore::with_config(inner.clone(), dir.path().into(), cfg.clone());
    for i in 0..128 {
        let empty = Path::from(format!("empty-{i}"));
        cached
            .put(&empty, bytes::Bytes::new().into())
            .await
            .unwrap();
        let head = Path::from(format!("head-{i}"));
        inner.put(&head, vec![1; 100].into()).await.unwrap();
        assert_eq!(cached.head(&head).await.unwrap().size, 100);
        assert_bound(&cached, dir.path(), &cfg);
        cached.delete(&empty).await.unwrap();
        cached.delete(&head).await.unwrap();
    }
    assert_eq!(cached.stats().entries, 4);
    assert_eq!(cached.stats().size_entries, 2);
}

#[tokio::test]
async fn changed_chunk_geometry_after_restart_never_reuses_wrong_ranges() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = tempfile::tempdir().unwrap();
    let path = Path::from("geometry");
    let source: Vec<u8> = (0..4096).map(|i| (i % 251) as u8).collect();
    inner.put(&path, source.clone().into()).await.unwrap();
    for chunk_size in [512, 256, 1024] {
        let cfg = CacheConfig {
            large_object_threshold: 1,
            chunk_size,
            ..bounded(4096, 16)
        };
        let cached = CachingObjectStore::with_config(inner.clone(), dir.path().into(), cfg);
        assert_eq!(
            cached.get_range(&path, 400..1300).await.unwrap().as_ref(),
            &source[400..1300]
        );
    }
}

#[tokio::test]
async fn legacy_production_files_and_interrupted_fills_are_accounted_on_upgrade() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = tempfile::tempdir().unwrap();
    let stem = format!("ht__7__L1__{}", uuid::Uuid::new_v4());
    std::fs::write(dir.path().join(format!("{stem}.parquet")), [1; 8]).unwrap();
    std::fs::write(dir.path().join(format!("{stem}.parquet.chunk-0")), [1; 4]).unwrap();
    let interrupted = dir
        .path()
        .join(format!("{stem}.tmp-{}", uuid::Uuid::new_v4()));
    std::fs::write(&interrupted, [2; 100]).unwrap();
    let cfg = bounded(4, 1);
    let cached = CachingObjectStore::with_config(inner, dir.path().into(), cfg.clone());
    assert!(!interrupted.exists());
    assert_eq!(cached.stats().entries, 1);
    assert_bound(&cached, dir.path(), &cfg);
}

#[tokio::test]
async fn unexpected_directory_contents_are_preserved_before_any_eviction() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = tempfile::tempdir().unwrap();
    let cached_part = dir
        .path()
        .join(format!("ht__7__L1__{}.parquet", uuid::Uuid::new_v4()));
    let other = dir.path().join("application-state");
    std::fs::write(&cached_part, [1; 100]).unwrap();
    std::fs::write(&other, [2; 100]).unwrap();
    let cached = CachingObjectStore::with_config(inner, dir.path().into(), bounded(4, 1));
    cached
        .put(&Path::from("backing-only"), vec![3; 16].into())
        .await
        .unwrap();
    assert_eq!(cached.stats().entries, 0);
    assert_eq!(std::fs::read(cached_part).unwrap(), [1; 100]);
    assert_eq!(std::fs::read(other).unwrap(), [2; 100]);
    assert_eq!(
        cached
            .get_range(&Path::from("backing-only"), 0..4)
            .await
            .unwrap()
            .as_ref(),
        &[3; 4]
    );
}

async fn churn(paths_per_worker: usize) {
    use std::os::unix::fs::MetadataExt;
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = tempfile::tempdir().unwrap();
    let cfg = CacheConfig {
        large_object_threshold: 8192,
        chunk_size: 4096,
        max_bytes: 64 * 1024,
        max_entries: 8,
        size_entries: 4,
        ..CacheConfig::default()
    };
    let start = std::time::Instant::now();
    let mut peak_reservation = 0;
    for epoch in 0..2 {
        let cached = Arc::new(CachingObjectStore::with_config(
            inner.clone(),
            dir.path().into(),
            cfg.clone(),
        ));
        assert_bound(&cached, dir.path(), &cfg);
        let mut workers = tokio::task::JoinSet::new();
        for worker in 0..4 {
            let cached = cached.clone();
            let inner = inner.clone();
            let cfg = cfg.clone();
            workers.spawn(async move {
                let mut peak = 0;
                for i in 0..paths_per_worker {
                    let path = Path::from(format!("churn/{epoch}/{worker}/{i}"));
                    let value = ((worker * 17 + i) % 251) as u8;
                    match i % 3 {
                        0 => {
                            cached.put(&path, vec![value; 8192].into()).await.unwrap();
                        }
                        1 => {
                            inner.put(&path, vec![value; 16384].into()).await.unwrap();
                        }
                        _ => {
                            let mut upload = cached.put_multipart(&path).await.unwrap();
                            let a = upload.put_part(vec![value; 4096].into());
                            let b = upload.put_part(vec![value; 4096].into());
                            let (a, b) = tokio::join!(a, b);
                            a.unwrap();
                            b.unwrap();
                            upload.complete().await.unwrap();
                        }
                    }
                    assert_eq!(
                        cached.get_range(&path, 900..7000).await.unwrap().as_ref(),
                        vec![value; 6100]
                    );
                    let stats = cached.stats();
                    assert!(
                        stats.resident_bytes + stats.reserved_bytes <= cfg.max_bytes,
                        "{stats:?}"
                    );
                    assert!(
                        stats.entries + stats.inflight_entries <= cfg.max_entries,
                        "{stats:?}"
                    );
                    assert!(stats.size_entries <= cfg.size_entries);
                    peak = peak.max(stats.reserved_bytes);
                    cached.delete(&path).await.unwrap();
                }
                peak
            });
        }
        while let Some(result) = workers.join_next().await {
            peak_reservation = peak_reservation.max(result.unwrap());
        }
        assert_bound(&cached, dir.path(), &cfg);
        assert_eq!(cached.stats().inflight_entries, 0);
        let mut whole = 0;
        let mut chunks = 0;
        let mut allocated = 0;
        for name in cached_file_names(dir.path()) {
            let metadata = std::fs::metadata(dir.path().join(&name)).unwrap();
            let len = metadata.len();
            allocated += metadata.blocks() * 512;
            if name.ends_with(".file") {
                whole += len;
            } else {
                chunks += len;
            }
        }
        println!(
            "CACHE_CHURN epoch={epoch} paths={} whole_bytes={whole} chunk_bytes={chunks} allocated_bytes={allocated} reserved_bytes={} entries={} size_entries={}",
            4 * paths_per_worker,
            cached.stats().reserved_bytes,
            cached.stats().entries,
            cached.stats().size_entries
        );
        // Drop/reopen the owner before the next epoch, retaining disk contents.
    }
    let rss = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    let peak_rss = rss
        .lines()
        .find(|s| s.starts_with("VmHWM:"))
        .unwrap_or("VmHWM: unavailable");
    println!(
        "CACHE_CHURN total_paths={} elapsed_secs={:.3} peak_reserved_bytes={peak_reservation} {peak_rss}",
        8 * paths_per_worker,
        start.elapsed().as_secs_f64()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_churn_stays_bounded_through_restart() {
    churn(64).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "manual sustained cache churn measurement"]
async fn sustained_cache_churn_measurement() {
    churn(5000).await;
}

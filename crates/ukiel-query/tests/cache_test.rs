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

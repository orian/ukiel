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

#![allow(dead_code)]

use std::sync::Arc;

use futures::{StreamExt, TryStreamExt};
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use serde_json::json;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::ContainerAsync;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use ukiel_catalog::PostgresCatalog;
use ukiel_core::{CommitId, CommitOp, HypertableId, PartMeta};
use ukiel_gc::{Gc, GcConfig};

pub struct Env {
    pub _pg: ContainerAsync<Postgres>,
    pub catalog: PostgresCatalog,
    pub store: Arc<dyn ObjectStore>,
    pub ht: HypertableId,
    /// So a test can give GC its own, independently killable catalog pool.
    pub url: String,
}

pub async fn setup() -> Env {
    let pg = Postgres::default().start().await.expect("postgres");
    let port = pg.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
    let catalog = PostgresCatalog::connect(&url).await.unwrap();
    catalog.migrate().await.unwrap();
    let ht = catalog
        .create_hypertable(
            "events",
            &json!({"fields": [{"name": "tenant_id", "type": "int64"}]}),
            &json!({"columns": []}),
            &["tenant_id".to_string()],
            "tenant_id",
        )
        .await
        .unwrap();
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    Env {
        _pg: pg,
        catalog,
        store,
        ht,
        url,
    }
}

/// Uploads a dummy object and returns its PartMeta (GC never reads contents).
pub async fn upload(env: &Env, name: &str) -> PartMeta {
    let path = format!("ht/{}/L0/{name}.parquet", env.ht);
    env.store
        .put(&Path::from(path.clone()), b"data".to_vec().into())
        .await
        .unwrap();
    PartMeta {
        path,
        partition_values: json!({}),
        packing_key_min: 1,
        packing_key_max: 1,
        row_count: 1,
        size_bytes: 4,
        level: 0,
        column_stats: None,
    }
}

pub async fn object_paths(store: &Arc<dyn ObjectStore>) -> Vec<String> {
    let mut paths: Vec<String> = store
        .list(None)
        .try_collect::<Vec<_>>()
        .await
        .unwrap()
        .into_iter()
        .map(|m| m.location.to_string())
        .collect();
    paths.sort();
    paths
}

/// Commits the parts, then tombstones every one of them with a REPLACE that
/// introduces one surviving part — so each input becomes a reap candidate.
pub async fn tombstone_all(env: &Env, parts: Vec<PartMeta>) {
    env.catalog
        .commit(env.ht, CommitOp::Add { parts }, None)
        .await
        .unwrap();
    let old: Vec<_> = env
        .catalog
        .live_parts(env.ht, None)
        .await
        .unwrap()
        .iter()
        .map(|p| p.id)
        .collect();
    let survivor = upload(env, "survivor").await;
    env.catalog
        .commit(
            env.ht,
            CommitOp::Replace {
                old,
                new: vec![survivor],
            },
            None,
        )
        .await
        .unwrap();
}

fn gc(env: &Env, tombstone_grace_secs: f64, orphan_grace_secs: i64) -> Gc {
    Gc::new(
        env.catalog.clone(),
        env.store.clone(),
        GcConfig {
            tombstone_grace_secs,
            orphan_grace_secs,
            poll_interval_ms: 50,
            ..GcConfig::default()
        },
    )
}

#[tokio::test]
async fn reaps_tombstoned_parts_after_grace() {
    let env = setup().await;
    let p1 = upload(&env, "a").await;
    let p2 = upload(&env, "b").await;
    env.catalog
        .commit(
            env.ht,
            CommitOp::Add {
                parts: vec![p1, p2],
            },
            None,
        )
        .await
        .unwrap();
    let old: Vec<_> = env
        .catalog
        .live_parts(env.ht, None)
        .await
        .unwrap()
        .iter()
        .map(|p| p.id)
        .collect();
    let merged = upload(&env, "merged").await;
    env.catalog
        .commit(
            env.ht,
            CommitOp::Replace {
                old,
                new: vec![merged],
            },
            None,
        )
        .await
        .unwrap();

    // Grace not elapsed: nothing happens.
    let stats = gc(&env, 3600.0, 3600).run_once().await.unwrap();
    assert_eq!(stats.reaped_parts, 0);
    assert_eq!(object_paths(&env.store).await.len(), 3);

    // Grace elapsed: tombstoned objects deleted, live object kept.
    let stats = gc(&env, 0.0, 3600).run_once().await.unwrap();
    assert_eq!(stats.reaped_parts, 2);
    let remaining = object_paths(&env.store).await;
    assert_eq!(remaining.len(), 1);
    assert!(remaining[0].ends_with("merged.parquet"));

    // Idempotent; catalog rows retained for feed replay.
    let stats = gc(&env, 0.0, 3600).run_once().await.unwrap();
    assert_eq!(stats.reaped_parts, 0);
    let events = env
        .catalog
        .changes_since(env.ht, CommitId(0), 100)
        .await
        .unwrap();
    assert_eq!(events.len(), 2, "add + replace events survive purging");
}

#[tokio::test]
async fn sweeps_orphaned_intent_after_grace() {
    let env = setup().await;
    // A committed object (has a live part) and an orphan (intent + object, no
    // commit — a crashed writer).
    let committed = upload(&env, "committed").await;
    env.catalog
        .commit(
            env.ht,
            CommitOp::Add {
                parts: vec![committed],
            },
            None,
        )
        .await
        .unwrap();

    let orphan_path = format!("ht/{}/L0/orphan.parquet", env.ht);
    env.catalog
        .register_pending_objects(env.ht, std::slice::from_ref(&orphan_path))
        .await
        .unwrap();
    env.store
        .put(&Path::from(orphan_path.clone()), b"junk".to_vec().into())
        .await
        .unwrap();

    // Grace not elapsed: the orphan is protected (its commit might still land).
    let stats = gc(&env, 3600.0, 3600).run_once().await.unwrap();
    assert_eq!(stats.swept_orphans, 0);
    assert_eq!(object_paths(&env.store).await.len(), 2);

    // Grace 0: the orphan object and its intent row go; the committed part stays.
    let stats = gc(&env, 3600.0, 0).run_once().await.unwrap();
    assert_eq!(stats.swept_orphans, 1);
    let remaining = object_paths(&env.store).await;
    assert_eq!(remaining.len(), 1);
    assert!(remaining[0].ends_with("committed.parquet"));
    assert!(
        env.catalog
            .orphaned_pending_objects(env.ht, 0.0)
            .await
            .unwrap()
            .is_empty(),
        "swept orphan's intent row must be cleared"
    );
}

#[tokio::test]
async fn lagging_consumer_cursor_blocks_reaping() {
    let env = setup().await;
    let p = upload(&env, "old").await;
    env.catalog
        .commit(env.ht, CommitOp::Add { parts: vec![p] }, None)
        .await
        .unwrap();
    let old: Vec<_> = env
        .catalog
        .live_parts(env.ht, None)
        .await
        .unwrap()
        .iter()
        .map(|p| p.id)
        .collect();
    let replacement = upload(&env, "new").await;
    let result = env
        .catalog
        .commit(
            env.ht,
            CommitOp::Replace {
                old,
                new: vec![replacement],
            },
            None,
        )
        .await
        .unwrap();
    let replace_id = result.commit_id();

    // A consumer that hasn't processed the replace yet fences the reap.
    env.catalog
        .set_cursor("mv", env.ht, CommitId(replace_id.0 - 1))
        .await
        .unwrap();
    let stats = gc(&env, 0.0, 3600).run_once().await.unwrap();
    assert_eq!(stats.reaped_parts, 0);
    assert_eq!(object_paths(&env.store).await.len(), 2);

    // Caught up: reap proceeds.
    env.catalog
        .set_cursor("mv", env.ht, replace_id)
        .await
        .unwrap();
    let stats = gc(&env, 0.0, 3600).run_once().await.unwrap();
    assert_eq!(stats.reaped_parts, 1);
    let remaining = object_paths(&env.store).await;
    assert_eq!(remaining.len(), 1);
    assert!(remaining[0].ends_with("new.parquet"));
}

#[tokio::test]
async fn reconcile_sweeps_untracked_objects_only() {
    let env = setup().await;
    // A committed object, a pending (registered, in-flight) object, and a
    // truly-untracked object (uploaded with NO intent row — the failure mode
    // reconcile exists to catch), plus one outside the hypertable prefix.
    let committed = upload(&env, "committed").await;
    env.catalog
        .commit(
            env.ht,
            CommitOp::Add {
                parts: vec![committed],
            },
            None,
        )
        .await
        .unwrap();

    let pending_path = format!("ht/{}/L0/pending.parquet", env.ht);
    env.catalog
        .register_pending_objects(env.ht, std::slice::from_ref(&pending_path))
        .await
        .unwrap();
    env.store
        .put(&Path::from(pending_path), b"x".to_vec().into())
        .await
        .unwrap();

    env.store
        .put(
            &Path::from(format!("ht/{}/L0/untracked.parquet", env.ht)),
            b"x".to_vec().into(),
        )
        .await
        .unwrap();
    env.store
        .put(&Path::from("unrelated/file"), b"keep".to_vec().into())
        .await
        .unwrap();

    // Grace 0: only the untracked object is removed. Committed part, in-flight
    // pending object, and out-of-prefix object all survive.
    let gc = Gc::new(
        env.catalog.clone(),
        env.store.clone(),
        GcConfig {
            tombstone_grace_secs: 3600.0,
            orphan_grace_secs: 0,
            ..GcConfig::default()
        },
    );
    let stats = gc.reconcile_once().await.unwrap();
    assert_eq!(stats.reconciled_orphans, 1);
    let remaining = object_paths(&env.store).await;
    assert!(remaining.iter().any(|p| p.ends_with("committed.parquet")));
    assert!(remaining.iter().any(|p| p.ends_with("pending.parquet")));
    assert!(remaining.iter().any(|p| p == "unrelated/file"));
    assert!(!remaining.iter().any(|p| p.ends_with("untracked.parquet")));
}

// ---------------------------------------------------------------------------
// Plan 42: GC fails CLOSED. When the catalog goes away mid-pass, GC must not
// keep deleting objects on the word of a candidate list nothing can still
// vouch for. Delayed reclamation is cheap; deleting referenced data is not.
// ---------------------------------------------------------------------------

/// An object store that lets the test act on the first deletion — the moment
/// where "keep working through the list" would become "delete data we can no
/// longer prove is dead". `ObjectStoreExt::delete` routes through
/// `delete_stream`, so that is the hook.
struct DeleteHook {
    inner: Arc<dyn ObjectStore>,
    deleted: Arc<std::sync::Mutex<Vec<String>>>,
    on_first_delete: Arc<dyn Fn() + Send + Sync>,
}

impl std::fmt::Debug for DeleteHook {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("DeleteHook")
    }
}

impl std::fmt::Display for DeleteHook {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("DeleteHook")
    }
}

#[async_trait::async_trait]
impl ObjectStore for DeleteHook {
    async fn put_opts(
        &self,
        location: &Path,
        payload: object_store::PutPayload,
        opts: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }
    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }
    async fn get_opts(
        &self,
        location: &Path,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        self.inner.get_opts(location, options).await
    }
    fn delete_stream(
        &self,
        locations: futures::stream::BoxStream<'static, object_store::Result<Path>>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<Path>> {
        let deleted = self.deleted.clone();
        let hook = self.on_first_delete.clone();
        let inner = self.inner.clone();
        Box::pin(async_stream::stream! {
            futures::pin_mut!(locations);
            while let Some(next) = locations.next().await {
                let path = match next {
                    Ok(p) => p,
                    Err(e) => { yield Err(e); continue }
                };
                let first = {
                    let mut d = deleted.lock().unwrap();
                    d.push(path.to_string());
                    d.len() == 1
                };
                let result = inner.delete(&path).await;
                if first {
                    hook();
                }
                match result {
                    Ok(()) => yield Ok(path),
                    Err(e) => yield Err(e),
                }
            }
        })
    }
    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_catalog_lost_mid_reap_never_deletes_a_second_object() {
    let env = setup().await;
    let a = upload(&env, "a").await;
    let b = upload(&env, "b").await;
    let c = upload(&env, "c").await;
    // Tombstone all three: every one is a reapable candidate.
    tombstone_all(&env, vec![a, b, c]).await;

    // GC gets its own catalog pool, which we cut the instant its first object
    // deletion lands.
    let gc_catalog = PostgresCatalog::connect(&env.url).await.unwrap();
    let deleted = Arc::new(std::sync::Mutex::new(Vec::new()));
    let killer = gc_catalog.clone();
    let store: Arc<dyn ObjectStore> = Arc::new(DeleteHook {
        inner: env.store.clone(),
        deleted: deleted.clone(),
        on_first_delete: Arc::new(move || {
            // The database goes away exactly between candidate 1 and candidate 2.
            // Fire-and-forget: `close()` is async and we are in a sync hook.
            let pool = killer.pool_for_tests().clone();
            tokio::spawn(async move { pool.close().await });
        }),
    });

    let gc = Gc::new(
        gc_catalog,
        store,
        GcConfig {
            tombstone_grace_secs: 0.0,
            orphan_grace_secs: 0,
            poll_interval_ms: 50,
            ..GcConfig::default()
        },
    );
    let result = gc.run_once().await;
    assert!(result.is_err(), "a lost catalog must stop the pass");

    // THE assertion: exactly one object was deleted. GC did not work its way
    // through a list it could no longer justify — after a failover the new
    // primary might not even hold the commit that tombstoned these parts, and
    // then those "reapable" objects are live data.
    let deleted = deleted.lock().unwrap().clone();
    assert_eq!(
        deleted.len(),
        1,
        "GC continued deleting from a stale candidate list: {deleted:?}"
    );

    // The other two objects are still there, still reapable, and a recovered GC
    // will handle them from freshly-read authority.
    // 3 tombstoned + 1 survivor uploaded, minus the single deletion.
    let remaining = object_paths(&env.store).await;
    assert_eq!(remaining.len(), 3, "{remaining:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_recovered_gc_stamps_the_half_finished_candidate_and_continues() {
    // The state the previous test leaves behind: one object deleted, its row not
    // stamped (the stamp is what the outage interrupted). A recovered GC must
    // treat that as done — the store says NotFound, which is idempotent — and
    // then finish the rest. Otherwise the pass wedges on a candidate forever.
    let env = setup().await;
    let a = upload(&env, "a").await;
    let b = upload(&env, "b").await;
    tombstone_all(&env, vec![a, b]).await;

    // Simulate the interrupted candidate: its object is gone, its row is not
    // stamped.
    let first = env
        .catalog
        .next_reapable_part(env.ht, 0.0)
        .await
        .unwrap()
        .expect("a candidate");
    env.store
        .delete(&Path::from(first.meta.path.clone()))
        .await
        .unwrap();

    let stats = gc(&env, 0.0, 0).run_once().await.expect("recovered pass");
    assert_eq!(
        stats.reaped_parts, 2,
        "the already-deleted object is stamped, and the rest is finished"
    );
    assert_eq!(
        object_paths(&env.store).await.len(),
        1,
        "both tombstoned objects are gone; the live survivor is untouched"
    );
    // Idempotent: nothing left to do.
    assert_eq!(gc(&env, 0.0, 0).run_once().await.unwrap().reaped_parts, 0);
}

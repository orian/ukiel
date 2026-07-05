#![allow(dead_code)]

use std::sync::Arc;

use futures::TryStreamExt;
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
}

pub async fn setup() -> Env {
    let pg = Postgres::default().start().await.expect("postgres");
    let port = pg.get_host_port_ipv4(5432).await.unwrap();
    let catalog = PostgresCatalog::connect(&format!(
        "postgres://postgres:postgres@127.0.0.1:{port}/postgres"
    ))
    .await
    .unwrap();
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

fn gc(env: &Env, tombstone_grace_secs: f64, orphan_grace_secs: i64) -> Gc {
    Gc::new(
        env.catalog.clone(),
        env.store.clone(),
        GcConfig {
            tombstone_grace_secs,
            orphan_grace_secs,
            poll_interval_ms: 50,
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

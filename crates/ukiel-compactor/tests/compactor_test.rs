#![allow(dead_code)]

use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::Array;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use serde_json::json;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::ContainerAsync;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use ukiel_catalog::PostgresCatalog;
use ukiel_compactor::compactor::{Compactor, CompactorConfig};
use ukiel_compactor::rewrite;
use ukiel_core::{CommitOp, HypertableId, PartMeta, Placement, arrow_schema_from_json};
use ukiel_ingest::rows_to_parquet;

fn events_schema() -> serde_json::Value {
    json!({"fields": [
        {"name": "tenant_id", "type": "int64"},
        {"name": "ts", "type": "timestamp_ms"},
        {"name": "payload", "type": "utf8"}
    ]})
}

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
            &events_schema(),
            &json!({"columns": ["day"]}),
            &["tenant_id".to_string(), "ts".to_string()],
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

/// Writes one L0 part with the given rows and commits it.
pub async fn add_l0(env: &Env, day: &str, rows: Vec<serde_json::Value>) {
    let schema = arrow_schema_from_json(&events_schema()).unwrap();
    let encoded = rows_to_parquet(&schema, "tenant_id", "ts", rows).unwrap();
    let path = format!("ht/{}/L0/{}.parquet", env.ht, uuid::Uuid::new_v4());
    let size = encoded.bytes.len() as i64;
    env.store
        .put(&Path::from(path.clone()), encoded.bytes.into())
        .await
        .unwrap();
    env.catalog
        .commit(
            env.ht,
            CommitOp::Add {
                parts: vec![PartMeta {
                    path,
                    partition_values: json!({ "day": day }),
                    packing_key_min: encoded.key_min,
                    packing_key_max: encoded.key_max,
                    row_count: encoded.row_count,
                    size_bytes: size,
                    level: 0,
                    column_stats: None,
                }],
            },
            None,
        )
        .await
        .unwrap();
}

/// All payloads currently readable from live parts (the ground truth).
pub async fn live_payloads(env: &Env) -> HashSet<String> {
    let schema = Arc::new(arrow_schema_from_json(&events_schema()).unwrap());
    let parts = env.catalog.live_parts(env.ht, None).await.unwrap();
    let mut out = HashSet::new();
    for part in &parts {
        let batch =
            rewrite::read_parts_to_batch(&env.store, schema.clone(), std::slice::from_ref(part))
                .await
                .unwrap();
        let idx = batch.schema().index_of("payload").unwrap();
        let arr = batch
            .column(idx)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        for i in 0..arr.len() {
            out.insert(arr.value(i).to_string());
        }
    }
    out
}

#[tokio::test]
async fn packed_compaction_merges_within_partition() {
    let env = setup().await;
    // Three L0 files on day1, one on day2.
    add_l0(
        &env,
        "d1",
        vec![json!({"tenant_id": 2, "ts": 1, "payload": "p1"})],
    )
    .await;
    add_l0(
        &env,
        "d1",
        vec![json!({"tenant_id": 1, "ts": 2, "payload": "p2"})],
    )
    .await;
    add_l0(
        &env,
        "d1",
        vec![json!({"tenant_id": 2, "ts": 3, "payload": "p3"})],
    )
    .await;
    add_l0(
        &env,
        "d2",
        vec![json!({"tenant_id": 9, "ts": 4, "payload": "p4"})],
    )
    .await;
    let before = live_payloads(&env).await;

    let compactor = Compactor::new(
        env.catalog.clone(),
        env.store.clone(),
        CompactorConfig::default(),
    );
    let stats = compactor.run_once().await.unwrap();
    assert_eq!(stats.merged_groups, 1, "only day1 has >= 2 L0 files");
    assert_eq!(stats.parts_in, 3);
    assert_eq!(stats.parts_out, 1);
    assert_eq!(stats.conflicts, 0);

    // Data equivalence (invariant I4) and layout expectations.
    assert_eq!(live_payloads(&env).await, before);
    let parts = env.catalog.live_parts(env.ht, None).await.unwrap();
    assert_eq!(parts.len(), 2); // 1 merged L1 (day1) + 1 untouched L0 (day2)
    let l1 = parts.iter().find(|p| p.meta.level == 1).unwrap();
    assert_eq!(l1.meta.row_count, 3);
    assert_eq!((l1.meta.packing_key_min, l1.meta.packing_key_max), (1, 2));
    assert_eq!(l1.meta.partition_values, json!({"day": "d1"}));

    // Idempotence / convergence: a second pass finds nothing to do.
    let stats = compactor.run_once().await.unwrap();
    assert_eq!(stats.merged_groups, 0);
}

#[tokio::test]
async fn separated_compaction_gives_each_key_its_own_file() {
    let env = setup().await;
    env.catalog
        .set_placement(env.ht, Placement::Separated)
        .await
        .unwrap();
    add_l0(
        &env,
        "d1",
        vec![
            json!({"tenant_id": 5, "ts": 1, "payload": "x1"}),
            json!({"tenant_id": 6, "ts": 2, "payload": "y1"}),
        ],
    )
    .await;
    add_l0(
        &env,
        "d1",
        vec![json!({"tenant_id": 5, "ts": 3, "payload": "x2"})],
    )
    .await;
    let before = live_payloads(&env).await;

    let compactor = Compactor::new(
        env.catalog.clone(),
        env.store.clone(),
        CompactorConfig::default(),
    );
    let stats = compactor.run_once().await.unwrap();
    assert_eq!(stats.parts_in, 2);
    assert_eq!(stats.parts_out, 2, "keys 5 and 6 each get a dedicated file");

    assert_eq!(live_payloads(&env).await, before);
    let parts = env.catalog.live_parts(env.ht, None).await.unwrap();
    assert_eq!(parts.len(), 2);
    for p in &parts {
        assert_eq!(p.meta.level, 1);
        assert_eq!(
            p.meta.packing_key_min, p.meta.packing_key_max,
            "separated = one key per file"
        );
    }
}

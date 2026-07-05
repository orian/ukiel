use std::sync::Arc;

use futures::TryStreamExt;
use object_store::ObjectStore;
use object_store::memory::InMemory;
use serde_json::json;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use ukiel_catalog::{OffsetRange, PostgresCatalog};
use ukiel_core::arrow_schema_from_json;
use ukiel_ingest::flusher::{FlushItem, Flusher};
use ukiel_ingest::rows_to_parquet;

fn events_schema() -> serde_json::Value {
    json!({"fields": [
        {"name": "tenant_id", "type": "int64"},
        {"name": "ts", "type": "timestamp_ms"},
        {"name": "payload", "type": "utf8"}
    ]})
}

#[tokio::test]
async fn flush_writes_objects_and_commits_atomically() {
    let container = Postgres::default().start().await.expect("start postgres");
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
    let catalog = PostgresCatalog::connect(&url).await.unwrap();
    catalog.migrate().await.unwrap();

    let ht_id = catalog
        .create_hypertable(
            "events",
            &events_schema(),
            &json!({"columns": ["day"]}),
            &["tenant_id".to_string(), "ts".to_string()],
            "tenant_id",
        )
        .await
        .unwrap();
    let ht = catalog.get_hypertable("events").await.unwrap();

    let arrow_schema = arrow_schema_from_json(&ht.table_schema).unwrap();
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let flusher = Flusher::new(catalog.clone(), store.clone());

    let day1 = rows_to_parquet(
        &arrow_schema,
        "tenant_id",
        "ts",
        vec![
            json!({"tenant_id": 1, "ts": 1000, "payload": "a"}),
            json!({"tenant_id": 2, "ts": 2000, "payload": "b"}),
        ],
    )
    .unwrap();
    let day2 = rows_to_parquet(
        &arrow_schema,
        "tenant_id",
        "ts",
        vec![json!({"tenant_id": 1, "ts": 90_000_000, "payload": "c"})],
    )
    .unwrap();

    flusher
        .flush(
            &ht,
            vec![
                FlushItem {
                    partition_values: json!({"day": "1970-01-01"}),
                    part: day1,
                },
                FlushItem {
                    partition_values: json!({"day": "1970-01-02"}),
                    part: day2,
                },
            ],
            vec![OffsetRange {
                topic: "events".into(),
                partition: 0,
                first: 0,
                last: 2,
            }],
        )
        .await
        .unwrap();

    // Catalog sees both parts with correct metadata.
    let parts = catalog.live_parts(ht_id, None).await.unwrap();
    assert_eq!(parts.len(), 2);
    assert!(parts.iter().all(|p| p.meta.level == 0));
    assert!(
        parts
            .iter()
            .all(|p| p.meta.path.starts_with(&format!("ht/{}/L0/", ht_id)))
    );
    let total_rows: i64 = parts.iter().map(|p| p.meta.row_count).sum();
    assert_eq!(total_rows, 3);
    assert!(parts.iter().all(|p| p.meta.size_bytes > 0));

    // Offsets advanced with the same commit.
    let offsets = catalog.ingest_offsets(ht_id, "events").await.unwrap();
    assert_eq!(offsets.get(&0), Some(&3));

    // The objects actually exist in the store.
    let listed: Vec<_> = store.list(None).try_collect().await.unwrap();
    assert_eq!(listed.len(), 2);
}

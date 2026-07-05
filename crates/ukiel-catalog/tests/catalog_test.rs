mod common;

use serde_json::json;
use ukiel_core::{CommitOp, CommitResult, HypertableId, NamespaceId, PartMeta};

fn part_meta(path: &str, packing_key_min: i64, packing_key_max: i64) -> PartMeta {
    PartMeta {
        path: path.to_string(),
        partition_values: json!({"day": "2026-07-05"}),
        packing_key_min,
        packing_key_max,
        row_count: 100,
        size_bytes: 4096,
        level: 0,
        column_stats: None,
    }
}

async fn setup_hypertable(catalog: &ukiel_catalog::PostgresCatalog) -> HypertableId {
    catalog
        .create_hypertable(
            "events",
            &events_schema(),
            &json!({"columns": ["day"]}),
            &["tenant_id".to_string()],
            "tenant_id",
        )
        .await
        .unwrap()
}

fn events_schema() -> serde_json::Value {
    json!({"fields": [
        {"name": "tenant_id", "type": "int64"},
        {"name": "ts", "type": "timestamp_ms"},
        {"name": "payload", "type": "utf8"}
    ]})
}

#[tokio::test]
async fn connect_and_migrate() {
    let (_pg, _catalog) = common::setup().await;
    // setup() panics if connect or migrate fails; reaching here is the assertion.
}

#[tokio::test]
async fn hypertable_create_and_get() {
    let (_pg, catalog) = common::setup().await;
    let id = catalog
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
    assert_eq!(ht.id, id);
    assert_eq!(ht.name, "events");
    assert_eq!(ht.sort_key, vec!["tenant_id", "ts"]);
    assert_eq!(ht.packing_key, "tenant_id");
    assert_eq!(ht.table_schema, events_schema());

    let err = catalog.get_hypertable("missing").await.unwrap_err();
    assert!(matches!(err, ukiel_catalog::CatalogError::NotFound(_)));
}

#[tokio::test]
async fn logical_table_create_and_get() {
    let (_pg, catalog) = common::setup().await;
    let ht = catalog
        .create_hypertable(
            "events",
            &events_schema(),
            &json!({"columns": []}),
            &["tenant_id".to_string()],
            "tenant_id",
        )
        .await
        .unwrap();

    let lt_id = catalog
        .create_logical_table(NamespaceId(42), "my_events", ht)
        .await
        .unwrap();

    let lt = catalog
        .get_logical_table(NamespaceId(42), "my_events")
        .await
        .unwrap();
    assert_eq!(lt.id, lt_id);
    assert_eq!(lt.namespace_id, NamespaceId(42));
    assert_eq!(lt.hypertable_id, ht);
    assert_eq!(lt.column_mapping, None);

    // Same table name in a different namespace is a different logical table.
    let err = catalog
        .get_logical_table(NamespaceId(7), "my_events")
        .await
        .unwrap_err();
    assert!(matches!(err, ukiel_catalog::CatalogError::NotFound(_)));
}

#[tokio::test]
async fn commit_add_makes_parts_live() {
    let (_pg, catalog) = common::setup().await;
    let ht = setup_hypertable(&catalog).await;

    let result = catalog
        .commit(
            ht,
            CommitOp::Add {
                parts: vec![
                    part_meta("p1.parquet", 1, 50),
                    part_meta("p2.parquet", 51, 99),
                ],
            },
            None,
        )
        .await
        .unwrap();
    assert!(matches!(result, CommitResult::Committed(_)));

    let parts = catalog.live_parts(ht, None).await.unwrap();
    assert_eq!(parts.len(), 2);
    assert_eq!(parts[0].meta.path, "p1.parquet");
    assert_eq!(parts[0].created_by_commit, result.commit_id());

    // Packing-key pruning: key 60 only overlaps p2.
    let parts = catalog.live_parts(ht, Some(60)).await.unwrap();
    assert_eq!(parts.len(), 1);
    assert_eq!(parts[0].meta.path, "p2.parquet");
}

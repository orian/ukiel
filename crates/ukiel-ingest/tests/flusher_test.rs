use std::fmt;
use std::sync::Arc;

use futures::TryStreamExt;
use futures::stream::BoxStream;
use object_store::memory::InMemory;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as OsResult,
};
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
        &["tenant_id".to_string(), "ts".to_string()],
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
        &["tenant_id".to_string(), "ts".to_string()],
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

    // Column stats populated for the two-row part (Int64 columns tenant_id + ts).
    let with_stats = parts.iter().find(|p| p.meta.row_count == 2).unwrap();
    let stats = with_stats
        .meta
        .column_stats
        .as_ref()
        .expect("stats populated");
    assert_eq!(stats["ts"]["min"], serde_json::json!(1000));
    assert_eq!(stats["ts"]["max"], serde_json::json!(2000));
    assert_eq!(stats["tenant_id"]["min"], serde_json::json!(1));
    assert_eq!(stats["tenant_id"]["max"], serde_json::json!(2));

    // Offsets advanced with the same commit.
    let offsets = catalog.ingest_offsets(ht_id, "events").await.unwrap();
    assert_eq!(offsets.get(&0), Some(&3));

    // The objects actually exist in the store.
    let listed: Vec<_> = store.list(None).try_collect().await.unwrap();
    assert_eq!(listed.len(), 2);
}

/// Object store that fails every upload but delegates reads/lists — lets a test
/// exercise the "registered, upload failed, never committed" window.
#[derive(Debug)]
struct FailingPutStore {
    inner: Arc<dyn ObjectStore>,
}

#[async_trait::async_trait]
impl ObjectStore for FailingPutStore {
    async fn put_opts(
        &self,
        _location: &object_store::path::Path,
        _payload: PutPayload,
        _opts: PutOptions,
    ) -> OsResult<PutResult> {
        Err(object_store::Error::Generic {
            store: "failing",
            source: "put disabled".into(),
        })
    }
    async fn put_multipart_opts(
        &self,
        location: &object_store::path::Path,
        opts: PutMultipartOptions,
    ) -> OsResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }
    async fn get_opts(
        &self,
        location: &object_store::path::Path,
        options: GetOptions,
    ) -> OsResult<GetResult> {
        self.inner.get_opts(location, options).await
    }
    fn delete_stream(
        &self,
        locations: BoxStream<'static, OsResult<object_store::path::Path>>,
    ) -> BoxStream<'static, OsResult<object_store::path::Path>> {
        self.inner.delete_stream(locations)
    }
    fn list(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> BoxStream<'static, OsResult<ObjectMeta>> {
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> OsResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(
        &self,
        from: &object_store::path::Path,
        to: &object_store::path::Path,
        options: object_store::CopyOptions,
    ) -> OsResult<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

impl fmt::Display for FailingPutStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FailingPutStore")
    }
}

#[tokio::test]
async fn successful_flush_leaves_no_pending_intent() {
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

    let part = rows_to_parquet(
        &arrow_schema,
        "tenant_id",
        "ts",
        &["tenant_id".to_string(), "ts".to_string()],
        vec![json!({"tenant_id": 1, "ts": 1, "payload": "a"})],
    )
    .unwrap();
    flusher
        .flush(
            &ht,
            vec![FlushItem {
                partition_values: json!({"day": "1970-01-01"}),
                part,
            }],
            vec![OffsetRange {
                topic: "events".into(),
                partition: 0,
                first: 0,
                last: 0,
            }],
        )
        .await
        .unwrap();

    assert_eq!(catalog.live_parts(ht_id, None).await.unwrap().len(), 1);
    assert!(
        catalog
            .orphaned_pending_objects(ht_id, 0.0)
            .await
            .unwrap()
            .is_empty(),
        "commit must clear the upload-intent row"
    );
}

#[tokio::test]
async fn failed_upload_leaves_discoverable_intent() {
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
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let store: Arc<dyn ObjectStore> = Arc::new(FailingPutStore { inner });
    let flusher = Flusher::new(catalog.clone(), store);

    let part = rows_to_parquet(
        &arrow_schema,
        "tenant_id",
        "ts",
        &["tenant_id".to_string(), "ts".to_string()],
        vec![json!({"tenant_id": 1, "ts": 1, "payload": "a"})],
    )
    .unwrap();
    let result = flusher
        .flush(
            &ht,
            vec![FlushItem {
                partition_values: json!({"day": "1970-01-01"}),
                part,
            }],
            vec![OffsetRange {
                topic: "events".into(),
                partition: 0,
                first: 0,
                last: 0,
            }],
        )
        .await;

    assert!(result.is_err(), "upload failure must abort the flush");
    assert_eq!(catalog.live_parts(ht_id, None).await.unwrap().len(), 0);
    // Intent was registered BEFORE the (failing) upload, so the orphan is
    // discoverable even though the commit never ran.
    assert_eq!(
        catalog
            .orphaned_pending_objects(ht_id, 0.0)
            .await
            .unwrap()
            .len(),
        1,
        "register must happen before upload"
    );
}

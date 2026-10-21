use arrow::{
    array::{Int64Array, RecordBatch},
    datatypes::{DataType, Field, Schema},
};
use futures::stream::BoxStream;
use object_store::{ObjectStore, ObjectStoreExt, memory::InMemory, path::Path};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::json;
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use testcontainers_modules::{
    postgres::Postgres,
    testcontainers::{ContainerAsync, runners::AsyncRunner},
};
use tokio::sync::Notify;
use ukiel_catalog::{CommitFault, PostgresCatalog};
use ukiel_core::{
    CommitId, CommitOp, Delivery, Endpoint, ErrorPolicy, Hypertable, PartMeta, Pipeline,
    PipelineDefinition,
};
use ukiel_mv::{MvConfig, MvError, MvFleet};
use ukiel_write::batches_to_l0_items;

async fn setup() -> (
    ContainerAsync<Postgres>,
    PostgresCatalog,
    Arc<dyn ObjectStore>,
) {
    let pg = Postgres::default().start().await.unwrap();
    let port = pg.get_host_port_ipv4(5432).await.unwrap();
    let catalog = PostgresCatalog::connect(&format!(
        "postgres://postgres:postgres@127.0.0.1:{port}/postgres"
    ))
    .await
    .unwrap();
    catalog.migrate().await.unwrap();
    (pg, catalog, Arc::new(InMemory::new()))
}

async fn table(catalog: &PostgresCatalog, name: &str, clock: bool) -> Hypertable {
    let mut fields = vec![
        json!({"name":"tenant_id","type":"int64","nullable":false}),
        json!({"name":"value","type":"int64"}),
    ];
    if clock {
        fields.push(
            json!({"name":"inserted_at","type":"timestamp_ms","nullable":false,"default":"now()"}),
        );
    }
    let id = catalog
        .create_hypertable(
            name,
            &json!({"fields":fields}),
            &json!({"columns":[]}),
            &["tenant_id".into()],
            "tenant_id",
        )
        .await
        .unwrap();
    catalog.get_hypertable_by_id(id).await.unwrap()
}

async fn pipeline(
    catalog: &PostgresCatalog,
    source: &Hypertable,
    target: &Hypertable,
    name: &str,
    sql: &str,
    policy: ErrorPolicy,
) -> Pipeline {
    catalog
        .ensure_pipeline(&PipelineDefinition {
            name: name.into(),
            source: Endpoint::Parquet(source.id),
            target: target.id,
            sql: sql.into(),
            delivery: Delivery::ExactlyOnce,
            error_policy: policy,
            event_time_column: None,
        })
        .await
        .unwrap()
}

async fn fleet(catalog: &PostgresCatalog, store: &Arc<dyn ObjectStore>, names: &[&str]) -> MvFleet {
    MvFleet::new(
        catalog.clone(),
        store.clone(),
        MvConfig {
            pipelines: names.iter().map(|n| n.to_string()).collect(),
            ..MvConfig::default()
        },
    )
    .await
    .unwrap()
}

async fn append(
    catalog: &PostgresCatalog,
    store: &Arc<dyn ObjectStore>,
    source: &Hypertable,
    values: &[Vec<i64>],
) -> CommitId {
    let mut parts = Vec::new();
    for (i, values) in values.iter().enumerate() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("tenant_id", DataType::Int64, false),
                Field::new("value", DataType::Int64, true),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![1; values.len()])),
                Arc::new(Int64Array::from(values.clone())),
            ],
        )
        .unwrap();
        for item in batches_to_l0_items(&[batch], source, 1024 * 1024).unwrap() {
            let path = format!(
                "ht/{}/source-{}-{i}.parquet",
                source.id,
                uuid::Uuid::new_v4()
            );
            let size_bytes = item.part.bytes.len() as i64;
            store
                .put(&Path::from(path.clone()), item.part.bytes.into())
                .await
                .unwrap();
            parts.push(PartMeta {
                path,
                partition_values: item.partition_values,
                packing_key_min: item.part.key_min,
                packing_key_max: item.part.key_max,
                row_count: item.part.row_count,
                size_bytes,
                level: 0,
                column_stats: item.part.column_stats,
            });
        }
    }
    catalog
        .commit(source.id, CommitOp::Add { parts }, None)
        .await
        .unwrap()
        .commit_id()
}

async fn output(
    catalog: &PostgresCatalog,
    store: &Arc<dyn ObjectStore>,
    target: &Hypertable,
) -> Vec<RecordBatch> {
    let mut batches = vec![];
    for part in catalog.live_parts(target.id, None).await.unwrap() {
        let bytes = store
            .get(&Path::from(part.meta.path))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        batches.extend(
            ParquetRecordBatchReaderBuilder::try_new(bytes)
                .unwrap()
                .build()
                .unwrap()
                .map(Result::unwrap),
        );
    }
    batches
}

#[tokio::test]
async fn whole_add_executes_once_with_native_clock_and_storage_rewrites_do_not_reapply() {
    let (_pg, catalog, store) = setup().await;
    let source = table(&catalog, "source", false).await;
    let target = table(&catalog, "target", true).await;
    let pipe = pipeline(
        &catalog,
        &source,
        &target,
        "sum",
        "SELECT tenant_id,SUM(value) AS value FROM source GROUP BY tenant_id HAVING SUM(value)>2",
        ErrorPolicy::Stall,
    )
    .await;
    let worker = fleet(&catalog, &store, &["sum"]).await;
    assert_eq!(catalog.read_mv_cursor(pipe.id).await.unwrap(), CommitId(0));
    let commit = append(&catalog, &store, &source, &[vec![1], vec![2]]).await;
    assert_eq!(worker.process_once().await.unwrap(), 1);
    let batches = output(&catalog, &store, &target).await;
    assert_eq!(batches.len(), 1);
    assert_eq!(
        batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        3
    );
    let clock = batches[0]
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert!(clock > 1_700_000_000_000 && clock < 10_000_000_000_000);
    assert_eq!(catalog.read_mv_cursor(pipe.id).await.unwrap(), commit);
    assert_eq!(worker.process_once().await.unwrap(), 0);
    let old = catalog.live_parts(source.id, None).await.unwrap();
    catalog
        .commit(
            source.id,
            CommitOp::Replace {
                old: old.iter().map(|p| p.id).collect(),
                new: vec![],
            },
            None,
        )
        .await
        .unwrap();
    worker.process_once().await.unwrap();
    catalog
        .commit(source.id, CommitOp::Delete { parts: vec![] }, None)
        .await
        .unwrap();
    worker.process_once().await.unwrap();
    assert_eq!(
        catalog
            .changes_since(target.id, CommitId(0), 100)
            .await
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn committed_output_reconciles_after_lost_ack_without_reading_source_again() {
    let (_pg, catalog, store) = setup().await;
    let source = table(&catalog, "source", false).await;
    let target = table(&catalog, "target", true).await;
    let pipe = pipeline(
        &catalog,
        &source,
        &target,
        "copy",
        "SELECT * FROM source",
        ErrorPolicy::Stall,
    )
    .await;
    let worker = fleet(&catalog, &store, &["copy"]).await;
    let commit = append(&catalog, &store, &source, &[vec![7]]).await;
    catalog.arm_commit_fault(CommitFault::AfterCommitBeforeReturn);
    let error = worker.process_once().await.unwrap_err();
    assert!(matches!(
        error.catalog_error(),
        Some(ukiel_catalog::CatalogError::AmbiguousMutation { .. })
    ));
    assert_eq!(catalog.read_mv_cursor(pipe.id).await.unwrap(), CommitId(0));
    let before = catalog.live_parts(target.id, None).await.unwrap();
    assert_eq!(before.len(), 1);
    for part in catalog.live_parts(source.id, None).await.unwrap() {
        store.delete(&Path::from(part.meta.path)).await.unwrap();
    }
    let recovered = fleet(&catalog, &store, &["copy"]).await;
    recovered.process_once().await.unwrap();
    assert_eq!(catalog.read_mv_cursor(pipe.id).await.unwrap(), commit);
    assert_eq!(catalog.live_parts(target.id, None).await.unwrap(), before);
}

#[tokio::test]
async fn empty_and_skip_are_durable_markers_and_downstream_count_does_not_manufacture_rows() {
    let (_pg, catalog, store) = setup().await;
    let source = table(&catalog, "source", false).await;
    let empty = table(&catalog, "empty", false).await;
    let skipped = table(&catalog, "skipped", false).await;
    let downstream = table(&catalog, "downstream", false).await;
    let empty_pipe = pipeline(
        &catalog,
        &source,
        &empty,
        "empty",
        "SELECT * FROM source WHERE value<0",
        ErrorPolicy::Stall,
    )
    .await;
    let skip_pipe = pipeline(
        &catalog,
        &source,
        &skipped,
        "skip",
        "SELECT tenant_id,100/value AS value FROM source",
        ErrorPolicy::Skip,
    )
    .await;
    pipeline(
        &catalog,
        &empty,
        &downstream,
        "count",
        "SELECT 1 AS tenant_id,COUNT(*) AS value FROM empty",
        ErrorPolicy::Stall,
    )
    .await;
    let worker = fleet(&catalog, &store, &["empty", "skip", "count"]).await;
    let commit = append(&catalog, &store, &source, &[vec![0]]).await;
    assert_eq!(worker.process_once().await.unwrap(), 3);
    for target in [&empty, &skipped, &downstream] {
        let events = catalog
            .changes_since(target.id, CommitId(0), 10)
            .await
            .unwrap();
        assert_eq!(events.len(), 1);
        assert!(events[0].added.is_empty());
    }
    for id in [empty_pipe.id, skip_pipe.id] {
        assert_eq!(catalog.read_mv_cursor(id).await.unwrap(), commit);
    }
    assert_eq!(worker.process_once().await.unwrap(), 0);
}

#[tokio::test]
async fn required_missing_input_and_budget_failure_never_follow_skip_policy() {
    let (_pg, catalog, store) = setup().await;
    let source = table(&catalog, "source", false).await;
    let target = table(&catalog, "target", false).await;
    let pipe = pipeline(
        &catalog,
        &source,
        &target,
        "copy",
        "SELECT * FROM source",
        ErrorPolicy::Skip,
    )
    .await;
    let worker = fleet(&catalog, &store, &["copy"]).await;
    append(&catalog, &store, &source, &[vec![1]]).await;
    let limited = MvFleet::new(
        catalog.clone(),
        store.clone(),
        MvConfig {
            pipelines: vec!["copy".into()],
            limits: ukiel_pipeline::ExecutionLimits {
                input_bytes: 1,
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert!(matches!(
        limited.process_once().await,
        Err(MvError::InputBudget(1))
    ));
    for part in catalog.live_parts(source.id, None).await.unwrap() {
        store.delete(&Path::from(part.meta.path)).await.unwrap();
    }
    assert!(matches!(
        worker.process_once().await,
        Err(MvError::SourceInputUnavailable { .. })
    ));
    assert_eq!(catalog.read_mv_cursor(pipe.id).await.unwrap(), CommitId(0));
    assert!(
        catalog
            .changes_since(target.id, CommitId(0), 10)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn all_selections_are_validated_before_any_consumer_is_registered() {
    let (_pg, catalog, store) = setup().await;
    let source = table(&catalog, "source", false).await;
    let target = table(&catalog, "target", false).await;
    let valid = pipeline(
        &catalog,
        &source,
        &target,
        "valid",
        "SELECT * FROM source",
        ErrorPolicy::Stall,
    )
    .await;
    pipeline(
        &catalog,
        &source,
        &target,
        "invalid",
        "SELECT value FROM source",
        ErrorPolicy::Stall,
    )
    .await;
    let result = MvFleet::new(
        catalog.clone(),
        store,
        MvConfig {
            pipelines: vec!["valid".into(), "invalid".into()],
            ..Default::default()
        },
    )
    .await;
    assert!(result.is_err());
    assert!(matches!(
        catalog.read_mv_cursor(valid.id).await,
        Err(ukiel_catalog::CatalogError::Invariant(_))
    ));
}

#[tokio::test]
async fn stalled_pipeline_keeps_its_cursor_without_starving_other_selections() {
    let (_pg, catalog, store) = setup().await;
    let source = table(&catalog, "source", false).await;
    let stalled = table(&catalog, "stalled", false).await;
    let healthy = table(&catalog, "healthy", false).await;
    let bad = pipeline(
        &catalog,
        &source,
        &stalled,
        "stall",
        "SELECT tenant_id,100/value AS value FROM source",
        ErrorPolicy::Stall,
    )
    .await;
    let good = pipeline(
        &catalog,
        &source,
        &healthy,
        "copy",
        "SELECT * FROM source",
        ErrorPolicy::Stall,
    )
    .await;
    let worker = fleet(&catalog, &store, &["stall", "copy"]).await;
    let commit = append(&catalog, &store, &source, &[vec![0]]).await;
    assert!(matches!(
        worker.process_once().await,
        Err(MvError::Pipeline(_))
    ));
    assert_eq!(catalog.read_mv_cursor(bad.id).await.unwrap(), CommitId(0));
    assert_eq!(catalog.read_mv_cursor(good.id).await.unwrap(), commit);
    assert!(
        catalog
            .changes_since(stalled.id, CommitId(0), 10)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(output(&catalog, &store, &healthy).await[0].num_rows(), 1);
}

#[tokio::test]
async fn rollback_leaves_gc_covered_upload_and_retry_publishes_one_outcome() {
    let (_pg, catalog, store) = setup().await;
    let source = table(&catalog, "source", false).await;
    let target = table(&catalog, "target", true).await;
    let pipe = pipeline(
        &catalog,
        &source,
        &target,
        "copy",
        "SELECT * FROM source",
        ErrorPolicy::Stall,
    )
    .await;
    let worker = fleet(&catalog, &store, &["copy"]).await;
    let commit = append(&catalog, &store, &source, &[vec![1]]).await;
    catalog.arm_commit_fault(CommitFault::BeforeCommit);
    assert!(
        worker
            .process_once()
            .await
            .unwrap_err()
            .catalog_error()
            .is_some()
    );
    assert_eq!(catalog.read_mv_cursor(pipe.id).await.unwrap(), CommitId(0));
    assert!(
        catalog
            .live_parts(target.id, None)
            .await
            .unwrap()
            .is_empty()
    );
    let pending = catalog.all_pending_paths(target.id).await.unwrap();
    assert_eq!(pending.len(), 1);
    store.head(&Path::from(pending[0].clone())).await.unwrap();
    fleet(&catalog, &store, &["copy"])
        .await
        .process_once()
        .await
        .unwrap();
    assert_eq!(catalog.read_mv_cursor(pipe.id).await.unwrap(), commit);
    assert_eq!(
        catalog
            .changes_since(target.id, CommitId(0), 10)
            .await
            .unwrap()
            .len(),
        1
    );
    let gc = ukiel_gc::Gc::new(
        catalog.clone(),
        store.clone(),
        ukiel_gc::GcConfig {
            orphan_grace_secs: 0,
            ..Default::default()
        },
    );
    assert_eq!(gc.run_once().await.unwrap().swept_orphans, 1);
    assert!(
        catalog
            .all_pending_paths(target.id)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(output(&catalog, &store, &target).await[0].num_rows(), 1);
}

#[tokio::test]
async fn lagging_registered_cursor_holds_tombstoned_input_until_replacement_is_consumed() {
    let (_pg, catalog, store) = setup().await;
    let source = table(&catalog, "source", false).await;
    let target = table(&catalog, "target", false).await;
    pipeline(
        &catalog,
        &source,
        &target,
        "copy",
        "SELECT * FROM source",
        ErrorPolicy::Stall,
    )
    .await;
    let worker = fleet(&catalog, &store, &["copy"]).await;
    append(&catalog, &store, &source, &[vec![1]]).await;
    let parts = catalog.live_parts(source.id, None).await.unwrap();
    catalog
        .commit(
            source.id,
            CommitOp::Replace {
                old: parts.iter().map(|p| p.id).collect(),
                new: vec![],
            },
            None,
        )
        .await
        .unwrap();
    let gc = ukiel_gc::Gc::new(
        catalog.clone(),
        store.clone(),
        ukiel_gc::GcConfig {
            tombstone_grace_secs: 0.0,
            ..Default::default()
        },
    );
    assert_eq!(gc.run_once().await.unwrap().reaped_parts, 0);
    worker.process_once().await.unwrap();
    assert_eq!(gc.run_once().await.unwrap().reaped_parts, 0);
    worker.process_once().await.unwrap();
    assert_eq!(gc.run_once().await.unwrap().reaped_parts, 1);
    assert_eq!(output(&catalog, &store, &target).await[0].num_rows(), 1);
}

#[tokio::test]
async fn lost_ack_for_empty_outcome_reconciles_without_creating_an_object() {
    let (_pg, catalog, store) = setup().await;
    let source = table(&catalog, "source", false).await;
    let target = table(&catalog, "target", false).await;
    let pipe = pipeline(
        &catalog,
        &source,
        &target,
        "empty",
        "SELECT * FROM source WHERE value<0",
        ErrorPolicy::Stall,
    )
    .await;
    let worker = fleet(&catalog, &store, &["empty"]).await;
    let commit = append(&catalog, &store, &source, &[vec![1]]).await;
    catalog.arm_commit_fault(CommitFault::AfterCommitBeforeReturn);
    assert!(
        worker
            .process_once()
            .await
            .unwrap_err()
            .catalog_error()
            .is_some()
    );
    assert_eq!(catalog.read_mv_cursor(pipe.id).await.unwrap(), CommitId(0));
    assert!(
        catalog
            .all_pending_paths(target.id)
            .await
            .unwrap()
            .is_empty()
    );
    fleet(&catalog, &store, &["empty"])
        .await
        .process_once()
        .await
        .unwrap();
    assert_eq!(catalog.read_mv_cursor(pipe.id).await.unwrap(), commit);
    let events = catalog
        .changes_since(target.id, CommitId(0), 10)
        .await
        .unwrap();
    assert_eq!(events.len(), 1);
    assert!(events[0].added.is_empty());
}

#[tokio::test]
async fn replacement_handoff_registers_new_definition_after_old_worker_drains() {
    let (_pg, catalog, store) = setup().await;
    let source = table(&catalog, "source", false).await;
    let target = table(&catalog, "target", false).await;
    pipeline(
        &catalog,
        &source,
        &target,
        "old",
        "SELECT * FROM source",
        ErrorPolicy::Stall,
    )
    .await;
    let old = fleet(&catalog, &store, &["old"]).await;
    append(&catalog, &store, &source, &[vec![1]]).await;
    old.process_once().await.unwrap();
    // Source writes are paused for the handoff. Drain and drop every old
    // worker before selecting the replacement; the catalog has no fleet fence.
    assert_eq!(old.process_once().await.unwrap(), 0);
    drop(old);
    let new = pipeline(
        &catalog,
        &source,
        &target,
        "new",
        "SELECT tenant_id,value*10 AS value FROM source",
        ErrorPolicy::Stall,
    )
    .await;
    let worker = fleet(&catalog, &store, &["new"]).await;
    assert_eq!(
        catalog.read_mv_cursor(new.id).await.unwrap(),
        catalog.feed_head(source.id).await.unwrap()
    );
    assert_eq!(worker.process_once().await.unwrap(), 0);
    append(&catalog, &store, &source, &[vec![2]]).await;
    worker.process_once().await.unwrap();
    let mut values = output(&catalog, &store, &target)
        .await
        .iter()
        .flat_map(|b| {
            b.column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect::<Vec<_>>();
    values.sort();
    assert_eq!(values, vec![1, 20]);
}

#[derive(Debug)]
struct PausedRead {
    inner: Arc<dyn ObjectStore>,
    path: String,
    armed: AtomicBool,
    entered: Arc<Notify>,
    release: Arc<Notify>,
}
impl std::fmt::Display for PausedRead {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PausedRead")
    }
}
#[async_trait::async_trait]
impl ObjectStore for PausedRead {
    async fn put_opts(
        &self,
        p: &Path,
        b: object_store::PutPayload,
        o: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        self.inner.put_opts(p, b, o).await
    }
    async fn put_multipart_opts(
        &self,
        p: &Path,
        o: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(p, o).await
    }
    async fn get_opts(
        &self,
        p: &Path,
        o: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        if p.as_ref() == self.path && self.armed.swap(false, Ordering::SeqCst) {
            self.entered.notify_one();
            self.release.notified().await;
        }
        self.inner.get_opts(p, o).await
    }
    fn delete_stream(
        &self,
        paths: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(paths)
    }
    fn list(
        &self,
        p: Option<&Path>,
    ) -> BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        self.inner.list(p)
    }
    async fn list_with_delimiter(
        &self,
        p: Option<&Path>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(p).await
    }
    async fn copy_opts(
        &self,
        a: &Path,
        b: &Path,
        o: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(a, b, o).await
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn shutdown_drains_owned_read_and_publication_before_returning() {
    let (_pg, catalog, store) = setup().await;
    let source = table(&catalog, "source", false).await;
    let target = table(&catalog, "target", false).await;
    let pipe = pipeline(
        &catalog,
        &source,
        &target,
        "copy",
        "SELECT * FROM source",
        ErrorPolicy::Stall,
    )
    .await;
    let initial = fleet(&catalog, &store, &["copy"]).await;
    drop(initial);
    let commit = append(&catalog, &store, &source, &[vec![1]]).await;
    let part = catalog.live_parts(source.id, None).await.unwrap().remove(0);
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let paused: Arc<dyn ObjectStore> = Arc::new(PausedRead {
        inner: store.clone(),
        path: part.meta.path,
        armed: AtomicBool::new(true),
        entered: entered.clone(),
        release: release.clone(),
    });
    let worker = fleet(&catalog, &paused, &["copy"]).await;
    let shutdown = tokio_util::sync::CancellationToken::new();
    let token = shutdown.clone();
    let task = tokio::spawn(async move { worker.run(token).await });
    tokio::time::timeout(Duration::from_secs(5), entered.notified())
        .await
        .unwrap();
    shutdown.cancel();
    assert!(!task.is_finished());
    release.notify_one();
    tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(catalog.read_mv_cursor(pipe.id).await.unwrap(), commit);
    assert_eq!(output(&catalog, &store, &target).await[0].num_rows(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn slow_replica_reconciles_when_fast_peer_and_real_gc_remove_its_input() {
    let (_pg, catalog, store) = setup().await;
    let source = table(&catalog, "source", false).await;
    // Exercise nonempty, natural-empty, and configured-skip completion proofs.
    for (index, sql, policy) in [
        (0, "SELECT * FROM source", ErrorPolicy::Stall),
        (1, "SELECT * FROM source WHERE value<0", ErrorPolicy::Stall),
        (
            2,
            "SELECT tenant_id,100/value AS value FROM source",
            ErrorPolicy::Skip,
        ),
    ] {
        let target = table(&catalog, &format!("target_{index}"), false).await;
        let name = format!("pipe_{index}");
        let pipe = pipeline(&catalog, &source, &target, &name, sql, policy).await;
        let fast = fleet(&catalog, &store, &[&name]).await;
        let commit = append(&catalog, &store, &source, &[vec![0]]).await;
        let parts = catalog.live_parts(source.id, None).await.unwrap();
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let paused: Arc<dyn ObjectStore> = Arc::new(PausedRead {
            inner: store.clone(),
            path: parts[0].meta.path.clone(),
            armed: AtomicBool::new(true),
            entered: entered.clone(),
            release: release.clone(),
        });
        let slow = fleet(&catalog, &paused, &[&name]).await;
        let task = tokio::spawn(async move { slow.process_once().await });
        tokio::time::timeout(Duration::from_secs(5), entered.notified())
            .await
            .unwrap();
        fast.process_once().await.unwrap();
        assert_eq!(catalog.read_mv_cursor(pipe.id).await.unwrap(), commit);
        let replacement = catalog
            .commit(
                source.id,
                CommitOp::Replace {
                    old: parts.iter().map(|p| p.id).collect(),
                    new: vec![],
                },
                None,
            )
            .await
            .unwrap()
            .commit_id();
        fast.process_once().await.unwrap();
        // Earlier pipeline cursors also need to consume this replacement before
        // GC may reclaim it; use an explicit, fully caught-up fleet.
        let previous = (0..index).map(|i| format!("pipe_{i}")).collect::<Vec<_>>();
        if !previous.is_empty() {
            let prior = MvFleet::new(
                catalog.clone(),
                store.clone(),
                MvConfig {
                    pipelines: previous,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
            while prior.process_once().await.unwrap() > 0 {}
        }
        let gc = ukiel_gc::Gc::new(
            catalog.clone(),
            store.clone(),
            ukiel_gc::GcConfig {
                tombstone_grace_secs: 0.0,
                ..Default::default()
            },
        );
        assert_eq!(gc.run_once().await.unwrap().reaped_parts, 1);
        assert!(matches!(
            store.head(&Path::from(parts[0].meta.path.clone())).await,
            Err(object_store::Error::NotFound { .. })
        ));
        release.notify_one();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(5), task)
                .await
                .unwrap()
                .unwrap()
                .unwrap(),
            1
        );
        assert_eq!(catalog.read_mv_cursor(pipe.id).await.unwrap(), replacement);
        let events = catalog
            .changes_since(target.id, CommitId(0), 10)
            .await
            .unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].added.is_empty(), index != 0);
    }
}

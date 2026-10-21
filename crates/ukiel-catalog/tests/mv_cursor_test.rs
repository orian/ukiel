mod common;

use serde_json::json;
use std::time::Duration;
use ukiel_catalog::{CatalogError, CatalogErrorClass, CatalogPoolConfig, PostgresCatalog};
use ukiel_core::{
    CommitId, CommitOp, Delivery, Endpoint, ErrorPolicy, HypertableId, Pipeline,
    PipelineDefinition, StreamTableDefinition,
};

async fn table(catalog: &PostgresCatalog, name: &str) -> HypertableId {
    catalog
        .create_hypertable(
            name,
            &json!({"fields":[{"name":"tenant_id","type":"int64"}]}),
            &json!({"columns":[]}),
            &["tenant_id".into()],
            "tenant_id",
        )
        .await
        .unwrap()
}

fn definition(name: &str, source: HypertableId, target: HypertableId) -> PipelineDefinition {
    PipelineDefinition {
        name: name.into(),
        source: Endpoint::Parquet(source),
        target,
        sql: "SELECT tenant_id FROM source".into(),
        delivery: Delivery::ExactlyOnce,
        error_policy: ErrorPolicy::Stall,
        event_time_column: None,
    }
}

async fn mv(catalog: &PostgresCatalog) -> (Pipeline, HypertableId, HypertableId) {
    let source = table(catalog, "source").await;
    let target = table(catalog, "target").await;
    let pipeline = catalog
        .ensure_pipeline(&definition("mv", source, target))
        .await
        .unwrap();
    (pipeline, source, target)
}

async fn add(catalog: &PostgresCatalog, ht: HypertableId) -> CommitId {
    catalog
        .commit(ht, CommitOp::Add { parts: vec![] }, None)
        .await
        .unwrap()
        .commit_id()
}

async fn blocked_by(catalog: &PostgresCatalog, blocker: i32, except: Option<i32>) -> i32 {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let pid: Option<i32> = sqlx::query_scalar("SELECT pid FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid)) AND ($2::int IS NULL OR pid<>$2) LIMIT 1")
                .bind(blocker).bind(except).fetch_optional(catalog.pool_for_tests()).await.unwrap();
            if let Some(pid) = pid { return pid; }
            tokio::task::yield_now().await;
        }
    }).await.expect("lock wait was not observed")
}

#[tokio::test]
async fn pipeline_bootstrap_is_immutable_dormant_and_cycle_safe() {
    let (_pg, catalog) = common::setup().await;
    let (pipeline, source, target) = mv(&catalog).await;
    assert_eq!(
        catalog.ensure_pipeline(&pipeline.definition).await.unwrap(),
        pipeline
    );
    assert!(matches!(
        catalog.read_mv_cursor(pipeline.id).await,
        Err(CatalogError::Invariant(_))
    ));
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM worker_cursors")
        .fetch_one(catalog.pool_for_tests())
        .await
        .unwrap();
    assert_eq!(count, 0);
    let mut changed = pipeline.definition.clone();
    changed.sql.push_str(" WHERE tenant_id > 1");
    assert!(matches!(
        catalog.ensure_pipeline(&changed).await,
        Err(CatalogError::InvalidPipeline(_))
    ));
    assert!(
        catalog
            .ensure_pipeline(&definition("self", source, source))
            .await
            .is_err()
    );
    assert!(
        catalog
            .ensure_pipeline(&definition("reverse", target, source))
            .await
            .is_err()
    );
    let replacement = catalog
        .ensure_pipeline(&definition("replacement", source, target))
        .await
        .unwrap();
    assert_ne!(replacement.id, pipeline.id);
    assert_eq!(catalog.list_pipelines().await.unwrap().len(), 2);
    assert!(
        catalog
            .ensure_pipeline(&definition("missing", HypertableId(999_999), target))
            .await
            .is_err()
    );

    let a = table(&catalog, "a").await;
    let b = table(&catalog, "b").await;
    let forward = definition("ab", a, b);
    let reverse = definition("ba", b, a);
    let (a, b) = tokio::join!(
        catalog.ensure_pipeline(&forward),
        catalog.ensure_pipeline(&reverse)
    );
    assert_eq!(
        usize::from(a.is_ok()) + usize::from(b.is_ok()),
        1,
        "opposite edges serialize under the graph lock"
    );
}

#[tokio::test]
async fn stream_bootstrap_adopts_legacy_offsets_and_rejects_reserved_fields() {
    let (_pg, catalog) = common::setup().await;
    let target = table(&catalog, "target").await;
    sqlx::query("INSERT INTO ingest_offsets(hypertable_id,topic,kafka_partition,next_offset) VALUES($1,'events',0,123)").bind(target.0).execute(catalog.pool_for_tests()).await.unwrap();
    let def = StreamTableDefinition {
        name: "events".into(),
        topic: "events".into(),
        format: "json".into(),
        schema: json!({"fields":[{"name":"tenant_id","type":"int64"}]}),
    };
    let stream = catalog.ensure_stream_table(&def).await.unwrap();
    assert_eq!(catalog.ensure_stream_table(&def).await.unwrap(), stream);
    let adopted: (Option<i64>, i64) = sqlx::query_as(
        "SELECT stream_table_id,next_offset FROM ingest_offsets WHERE hypertable_id=$1",
    )
    .bind(target.0)
    .fetch_one(catalog.pool_for_tests())
    .await
    .unwrap();
    assert_eq!(adopted, (Some(stream.id.0), 123));
    let mut invalid = def.clone();
    invalid.schema = json!({"fields":[{"name":"__ukiel_clock","type":"int64"}]});
    assert!(catalog.ensure_stream_table(&invalid).await.is_err());
    let mut pipeline_def = definition("kafka", HypertableId(1), target);
    pipeline_def.source = Endpoint::Kafka(stream.id);
    let pipeline = catalog.ensure_pipeline(&pipeline_def).await.unwrap();
    assert!(catalog.ensure_mv_cursor(pipeline.id).await.is_err());
}

#[tokio::test]
async fn registration_holds_the_feed_barrier_and_preserves_the_first_cursor() {
    let (_pg, catalog) = common::setup().await;
    let (pipeline, source, target) = mv(&catalog).await;
    let initial = add(&catalog, source).await;
    sqlx::raw_sql("CREATE FUNCTION pause_mv_cursor() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_advisory_xact_lock(20123015); RETURN NEW; END $$;
        CREATE TRIGGER pause_mv_cursor BEFORE INSERT ON worker_cursors FOR EACH ROW EXECUTE FUNCTION pause_mv_cursor();")
        .execute(catalog.pool_for_tests()).await.unwrap();
    let mut controller = catalog.pool_for_tests().begin().await.unwrap();
    let controller_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *controller)
        .await
        .unwrap();
    sqlx::query("SELECT pg_advisory_xact_lock(20123015)")
        .execute(&mut *controller)
        .await
        .unwrap();
    let first_catalog = catalog.clone();
    let first = tokio::spawn(async move { first_catalog.ensure_mv_cursor(pipeline.id).await });
    let initializer_pid = blocked_by(&catalog, controller_pid, None).await;
    let writer_catalog = catalog.clone();
    let writer = tokio::spawn(async move { add(&writer_catalog, source).await });
    let writer_pid = blocked_by(&catalog, initializer_pid, None).await;
    let second_catalog = catalog.clone();
    let second = tokio::spawn(async move { second_catalog.ensure_mv_cursor(pipeline.id).await });
    blocked_by(&catalog, initializer_pid, Some(writer_pid)).await;
    // Another hypertable is independent even while source writers are queued.
    tokio::time::timeout(Duration::from_secs(1), add(&catalog, target))
        .await
        .unwrap();
    controller.commit().await.unwrap();
    assert_eq!(first.await.unwrap().unwrap(), initial);
    let later = writer.await.unwrap();
    assert!(later > initial);
    assert_eq!(second.await.unwrap().unwrap(), initial);
    assert_eq!(catalog.read_mv_cursor(pipeline.id).await.unwrap(), initial);
    assert_eq!(catalog.feed_head(source).await.unwrap(), later);
    // Simulate a lost acknowledgement: retry after newer source commits.
    assert_eq!(
        catalog.ensure_mv_cursor(pipeline.id).await.unwrap(),
        initial
    );
}

#[tokio::test]
async fn registration_timeouts_leave_no_partial_cursor_and_retry_from_authority() {
    let (_pg, catalog, url) = common::setup_with_url().await;
    let (pipeline, source, _target) = mv(&catalog).await;
    let fast_timeout = PostgresCatalog::connect_with_config(
        &url,
        &CatalogPoolConfig {
            lock_timeout: Duration::from_millis(100),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let mut holder = catalog.pool_for_tests().begin().await.unwrap();
    sqlx::query("SELECT pg_advisory_xact_lock_shared(~$1::bigint)")
        .bind(source.0)
        .execute(&mut *holder)
        .await
        .unwrap();
    let error = fast_timeout
        .ensure_mv_cursor(pipeline.id)
        .await
        .unwrap_err();
    assert_eq!(error.class(), CatalogErrorClass::RetryableDatabase);
    holder.rollback().await.unwrap();
    assert!(matches!(
        catalog.read_mv_cursor(pipeline.id).await,
        Err(CatalogError::Invariant(_))
    ));
    // A separate uncommitted cursor row exercises the uniqueness-lock timeout.
    let mut row_holder = catalog.pool_for_tests().begin().await.unwrap();
    sqlx::query("INSERT INTO worker_cursors(worker,hypertable_id,last_commit_id) VALUES($1,$2,0)")
        .bind(format!("mv/{}", pipeline.id))
        .bind(source.0)
        .execute(&mut *row_holder)
        .await
        .unwrap();
    let error = fast_timeout
        .ensure_mv_cursor(pipeline.id)
        .await
        .unwrap_err();
    assert_eq!(error.class(), CatalogErrorClass::RetryableDatabase);
    row_holder.rollback().await.unwrap();
    assert_eq!(
        fast_timeout.ensure_mv_cursor(pipeline.id).await.unwrap(),
        CommitId(0)
    );
    add(&catalog, source).await;
    assert_eq!(
        fast_timeout.ensure_mv_cursor(pipeline.id).await.unwrap(),
        CommitId(0),
        "a physical zero cursor must never be reinitialized"
    );
}

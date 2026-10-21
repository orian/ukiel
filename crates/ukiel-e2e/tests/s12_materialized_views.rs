//! S12: Kafka -> Parquet ADD -> active-active SQL MV -> S3 Parquet -> HTTP.
//! Compaction changes file layout without retriggering source append intent.
use serde_json::json;
use std::time::Duration;
use ukiel_core::{CommitId, Delivery, Endpoint, ErrorPolicy, PipelineDefinition};
use ukiel_e2e::Stack;
use ukiel_mv::{MvConfig, MvFleet};

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires the docker-compose stack (make e2e)"]
async fn materialized_views_publish_once_and_preserve_results_through_compaction() {
    let stack = Stack::start().await;
    let source = stack.create_events_table().await;
    let target_name = format!("counts_{}", stack.nonce);
    let target = stack
        .catalog
        .create_hypertable(
            &target_name,
            &json!({"fields":[
                {"name":"tenant_id","type":"int64"},
                {"name":"ts","type":"timestamp_ms"},
                {"name":"n","type":"int64"},
                {"name":"inserted_at","type":"timestamp_ms","nullable":false,"default":"now()"}
            ]}),
            &json!({"columns":[]}),
            &["tenant_id".into(), "ts".into()],
            "tenant_id",
        )
        .await
        .unwrap();
    stack
        .catalog
        .create_logical_table(source.namespace, "event_counts", target)
        .await
        .unwrap();
    let pipeline=stack.catalog.ensure_pipeline(&PipelineDefinition {
        name:format!("counts_v1_{}",stack.nonce),source:Endpoint::Parquet(source.hypertable_id),target,
        sql:format!("SELECT tenant_id,MIN(ts) AS ts,COUNT(*) AS n FROM {} WHERE now() IS NOT NULL GROUP BY tenant_id",source.hypertable_name),
        delivery:Delivery::ExactlyOnce,error_policy:ErrorPolicy::Stall,event_time_column:None,
    }).await.unwrap();
    let config = MvConfig {
        pipelines: vec![pipeline.definition.name],
        ..Default::default()
    };
    let a = MvFleet::new(stack.catalog.clone(), stack.store.clone(), config.clone())
        .await
        .unwrap();
    let b = MvFleet::new(stack.catalog.clone(), stack.store.clone(), config)
        .await
        .unwrap();
    let ingest = stack.spawn_ingest(&source).await;
    // Separate rounds produce multiple source ADDs and target partial groups.
    for round in 0..3 {
        stack
            .produce_events_for(&source, source.namespace, &format!("mv-{round}"), 10)
            .await;
        assert_eq!(
            stack
                .wait_for_row_count(source.namespace, (round + 1) * 10, Duration::from_secs(30))
                .await,
            (round + 1) * 10
        );
        let (left, right) = tokio::join!(a.process_once(), b.process_once());
        left.unwrap();
        right.unwrap();
    }
    ingest.stop().await;
    loop {
        let (left, right) = tokio::join!(a.process_once(), b.process_once());
        if left.unwrap() + right.unwrap() == 0 {
            break;
        }
    }
    let sql = "SELECT ts,n,inserted_at FROM event_counts ORDER BY ts,n,inserted_at";
    let before = stack.query(source.namespace, sql).await.unwrap();
    assert_eq!(
        stack
            .query(source.namespace, "SELECT SUM(n) AS n FROM event_counts")
            .await
            .unwrap()[0]["n"],
        30
    );
    for row in before.as_array().unwrap() {
        assert!(row["inserted_at"].as_i64().unwrap() > 1_700_000_000_000);
    }
    let source_adds = stack
        .catalog
        .changes_since(source.hypertable_id, CommitId(0), 1000)
        .await
        .unwrap()
        .into_iter()
        .filter(|event| event.kind == "add")
        .count();
    let count_adds = || async {
        stack
            .catalog
            .changes_since(target, CommitId(0), 1000)
            .await
            .unwrap()
            .into_iter()
            .filter(|event| event.kind == "add")
            .count()
    };
    assert_eq!(count_adds().await, source_adds);
    assert!(stack.run_compaction_to_quiescence().await > 0);
    while a.process_once().await.unwrap() > 0 {}
    assert_eq!(
        count_adds().await,
        source_adds,
        "source REPLACE must not produce another MV ADD"
    );
    assert_eq!(
        stack.query(source.namespace, sql).await.unwrap(),
        before,
        "compaction preserves partials and stored clock values"
    );
}

use std::sync::Arc;
use std::time::Duration;

use object_store::ObjectStore;
use object_store::memory::InMemory;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde_json::json;
use testcontainers_modules::kafka::apache::{KAFKA_PORT, Kafka};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tokio_util::sync::CancellationToken;
use ukiel_catalog::PostgresCatalog;
use ukiel_core::HypertableId;
use ukiel_ingest::config::{IngestConfig, TableRoute};
use ukiel_ingest::consumer::IngestWorker;

const TOPIC: &str = "events";
const DAY1_TS: i64 = 1_000; // 1970-01-01
const DAY2_TS: i64 = 90_000_000; // 1970-01-02

async fn produce(producer: &FutureProducer, tenant: i64, ts: i64) {
    let payload = json!({"tenant_id": tenant, "ts": ts, "payload": "x"}).to_string();
    producer
        .send(
            FutureRecord::<(), _>::to(TOPIC).payload(&payload),
            Duration::from_secs(10),
        )
        .await
        .map_err(|(e, _)| e)
        .expect("produce");
}

async fn wait_for_parts(catalog: &PostgresCatalog, ht: HypertableId, min_parts: usize) -> usize {
    for _ in 0..120 {
        let n = catalog.live_parts(ht, None).await.unwrap().len();
        if n >= min_parts {
            return n;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    panic!("timed out waiting for {min_parts} parts");
}

#[tokio::test(flavor = "multi_thread")]
async fn consumes_flushes_and_resumes_exactly_once() {
    let pg = Postgres::default().start().await.expect("postgres");
    let pg_port = pg.get_host_port_ipv4(5432).await.unwrap();
    let kafka = Kafka::default().start().await.expect("kafka");
    let brokers = format!(
        "127.0.0.1:{}",
        kafka.get_host_port_ipv4(KAFKA_PORT).await.unwrap()
    );

    let catalog = PostgresCatalog::connect(&format!(
        "postgres://postgres:postgres@127.0.0.1:{pg_port}/postgres"
    ))
    .await
    .unwrap();
    catalog.migrate().await.unwrap();
    let ht_id = catalog
        .create_hypertable(
            "events",
            &json!({"fields": [
                {"name": "tenant_id", "type": "int64"},
                {"name": "ts", "type": "timestamp_ms"},
                {"name": "payload", "type": "utf8"}
            ]}),
            &json!({"columns": ["day"]}),
            &["tenant_id".to_string(), "ts".to_string()],
            "tenant_id",
        )
        .await
        .unwrap();

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("message.timeout.ms", "10000")
        .create()
        .unwrap();

    // Round 1: 6 messages over two days.
    for tenant in 1..=3 {
        produce(&producer, tenant, DAY1_TS).await;
        produce(&producer, tenant, DAY2_TS).await;
    }

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let config = IngestConfig {
        brokers: brokers.clone(),
        group_id: "ukiel-test".into(),
        flush_interval_ms: 500,
        max_buffer_rows: 10_000,
        // Fixtures use 1970 epoch-ms timestamps; widen the past bound so the
        // event-time rail (issue 0004) doesn't skip them as ancient.
        max_event_age_days: 30_000,
        max_event_future_secs: 3600,
        tables: vec![TableRoute {
            topic: TOPIC.into(),
            hypertable: "events".into(),
            ts_column: "ts".into(),
        }],
    };

    let worker = IngestWorker::new(catalog.clone(), store.clone(), config.clone());
    let shutdown = CancellationToken::new();
    let handle = {
        let token = shutdown.clone();
        tokio::spawn(async move { worker.run(token).await })
    };

    // Two day-partitions -> at least 2 parts.
    wait_for_parts(&catalog, ht_id, 2).await;
    shutdown.cancel();
    handle.await.unwrap().unwrap();

    let parts = catalog.live_parts(ht_id, None).await.unwrap();
    let rows_round1: i64 = parts.iter().map(|p| p.meta.row_count).sum();
    assert_eq!(rows_round1, 6);
    let offsets = catalog.ingest_offsets(ht_id, TOPIC).await.unwrap();
    assert_eq!(
        offsets.values().sum::<i64>(),
        6,
        "next_offset must equal produced count"
    );

    // Round 2: restart from catalog offsets; must not re-ingest old rows.
    for tenant in 4..=6 {
        produce(&producer, tenant, DAY1_TS).await;
    }
    let worker = IngestWorker::new(catalog.clone(), store.clone(), config);
    let shutdown = CancellationToken::new();
    let handle = {
        let token = shutdown.clone();
        tokio::spawn(async move { worker.run(token).await })
    };
    wait_for_parts(&catalog, ht_id, 3).await;
    shutdown.cancel();
    handle.await.unwrap().unwrap();

    let parts = catalog.live_parts(ht_id, None).await.unwrap();
    let total_rows: i64 = parts.iter().map(|p| p.meta.row_count).sum();
    assert_eq!(total_rows, 9, "restart must not duplicate rows");
    let offsets = catalog.ingest_offsets(ht_id, TOPIC).await.unwrap();
    assert_eq!(offsets.values().sum::<i64>(), 9);
}

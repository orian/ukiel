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
use ukiel_core::{CommitOp, HypertableId, PartMeta};
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
        l0_slowdown_parts: 30,
        l0_stop_parts: 200,
        warn_partitions_per_flush: 64,
        // Fixtures use 1970 epoch-ms timestamps; widen the past bound so the
        // event-time rail (issue 0004) doesn't skip them as ancient.
        max_event_age_days: 30_000,
        max_event_future_secs: 3600,
        tables: vec![TableRoute {
            topic: TOPIC.into(),
            hypertable: "events".into(),
            ts_column: "ts".into(),
        }],
        ready: None,
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

#[tokio::test(flavor = "multi_thread")]
async fn backpressure_defers_flushes_until_pressure_clears() {
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

    // Pre-seed the produced rows' partition with 3 live L0 parts, at the stop
    // threshold. (The count is what backpressure reads; no objects needed.)
    let partition = json!({ "day": "1970-01-01" }); // DAY1_TS = 1000ms
    catalog
        .commit(
            ht_id,
            CommitOp::Add {
                parts: (0..3)
                    .map(|i| PartMeta {
                        path: format!("seed-{i}.parquet"),
                        partition_values: partition.clone(),
                        packing_key_min: 1,
                        packing_key_max: 1,
                        row_count: 1,
                        size_bytes: 1,
                        level: 0,
                        column_stats: None,
                    })
                    .collect(),
            },
            None,
        )
        .await
        .unwrap();

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("message.timeout.ms", "10000")
        .create()
        .unwrap();
    for tenant in 1..=3 {
        produce(&producer, tenant, DAY1_TS).await;
    }

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let config = IngestConfig {
        brokers: brokers.clone(),
        group_id: "ukiel-bp-test".into(),
        flush_interval_ms: 300,
        max_buffer_rows: 10_000,
        l0_slowdown_parts: 3,
        l0_stop_parts: 3, // 3 seeds -> stop band -> defer every tick
        warn_partitions_per_flush: 64,
        max_event_age_days: 30_000,
        max_event_future_secs: 3600,
        tables: vec![TableRoute {
            topic: TOPIC.into(),
            hypertable: "events".into(),
            ts_column: "ts".into(),
        }],
        ready: None,
    };

    let worker = IngestWorker::new(catalog.clone(), store.clone(), config);
    let shutdown = CancellationToken::new();
    let handle = {
        let token = shutdown.clone();
        tokio::spawn(async move { worker.run(token).await })
    };

    // Deferred: several flush intervals pass with no new part and no offset
    // advance (exactly-once holds through backpressure).
    tokio::time::sleep(Duration::from_secs(3)).await;
    let live = catalog.live_parts(ht_id, None).await.unwrap();
    assert_eq!(
        live.len(),
        3,
        "under backpressure only the 3 seeds are live"
    );
    let offsets = catalog.ingest_offsets(ht_id, TOPIC).await.unwrap();
    assert_eq!(
        offsets.values().sum::<i64>(),
        0,
        "offsets must not advance while flushes are deferred"
    );

    // Clear pressure: tombstone the seeds. The next tick flushes the buffer.
    let seed_ids: Vec<_> = live.iter().map(|p| p.id).collect();
    catalog
        .commit(ht_id, CommitOp::Delete { parts: seed_ids }, None)
        .await
        .unwrap();

    wait_for_parts(&catalog, ht_id, 1).await;
    shutdown.cancel();
    handle.await.unwrap().unwrap();

    let parts = catalog.live_parts(ht_id, None).await.unwrap();
    let rows: i64 = parts.iter().map(|p| p.meta.row_count).sum();
    assert_eq!(rows, 3, "all deferred rows flush once pressure clears");
    let offsets = catalog.ingest_offsets(ht_id, TOPIC).await.unwrap();
    assert_eq!(offsets.values().sum::<i64>(), 3);
}

/// Plan 42: one route's catalog failure must take the WHOLE ingest worker down.
///
/// Routes share a catalog and a `group.id`. If a sibling kept polling and
/// committing while another route's role was degraded, the fleet would have a
/// half-degraded ingest that is still consuming — and the recovery contract
/// ("drop everything, reload offsets from authority, re-seek") would be a lie
/// for the half that never stopped.
#[tokio::test(flavor = "multi_thread")]
async fn one_routes_catalog_failure_stops_every_route() {
    let pg = Postgres::default().start().await.expect("postgres");
    let pg_port = pg.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgres://postgres:postgres@127.0.0.1:{pg_port}/postgres");
    let catalog = PostgresCatalog::connect(&url).await.unwrap();
    catalog.migrate().await.unwrap();

    let schema = json!({"fields": [
        {"name": "tenant_id", "type": "int64"},
        {"name": "ts", "type": "timestamp_ms"},
        {"name": "payload", "type": "utf8"}
    ]});
    // Only ONE of the two routes has a hypertable; the other's lookup fails.
    catalog
        .create_hypertable(
            "events",
            &schema,
            &json!({"columns": ["day"]}),
            &["tenant_id".to_string(), "ts".to_string()],
            "tenant_id",
        )
        .await
        .unwrap();

    let kafka = Kafka::default().start().await.expect("kafka");
    let brokers = format!(
        "127.0.0.1:{}",
        kafka.get_host_port_ipv4(KAFKA_PORT).await.unwrap()
    );

    let ready_fired = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counter = ready_fired.clone();
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let config = IngestConfig {
        brokers,
        group_id: "ukiel-multiroute".into(),
        flush_interval_ms: 500,
        max_buffer_rows: 10_000,
        l0_slowdown_parts: 30,
        l0_stop_parts: 200,
        warn_partitions_per_flush: 64,
        max_event_age_days: 30_000,
        max_event_future_secs: 3600,
        tables: vec![
            TableRoute {
                topic: TOPIC.into(),
                hypertable: "events".into(),
                ts_column: "ts".into(),
            },
            TableRoute {
                topic: "other".into(),
                hypertable: "missing_table".into(),
                ts_column: "ts".into(),
            },
        ],
        ready: Some(ukiel_core::ReadySignal::new(move || {
            counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        })),
    };

    let worker = IngestWorker::new(catalog.clone(), store, config);
    let result = tokio::time::timeout(
        Duration::from_secs(60),
        worker.run(CancellationToken::new()),
    )
    .await
    .expect("the worker must fail, not hang");

    assert!(
        result.is_err(),
        "a route whose hypertable cannot be read fails the whole worker"
    );
    // And the role never claimed to be ready: readiness needs EVERY route to
    // have positioned itself from the catalog, so a half-started ingest is
    // reported as what it is.
    assert_eq!(
        ready_fired.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "ingest must not report ready while a route never reconciled"
    );
}

// ---------------------------------------------------------------------------
// Plan 43: the ingest operation identity is the offsets, and nothing else.
// ---------------------------------------------------------------------------

use ukiel_catalog::{OffsetRange, OperationLookup};
use ukiel_core::{CommitResult, OperationError};
use ukiel_ingest::flusher::{INGEST_TRANSFORMATION_VERSION, ingest_identity};

fn range(topic: &str, partition: i32, first: i64, last: i64) -> OffsetRange {
    OffsetRange {
        topic: topic.into(),
        partition,
        first,
        last,
    }
}

#[test]
fn the_ingest_identity_is_stable_across_range_enumeration_order() {
    let ht = HypertableId(1);
    let a = ingest_identity(ht, &[range("events", 1, 10, 19), range("events", 0, 0, 9)]).unwrap();
    let b = ingest_identity(ht, &[range("events", 0, 0, 9), range("events", 1, 10, 19)]).unwrap();
    assert_eq!(
        a.key(),
        b.key(),
        "the order the flusher happened to collect its ranges in is not intent"
    );

    // Every field of the intent is load-bearing.
    for other in [
        ingest_identity(HypertableId(2), &[range("events", 0, 0, 9)]).unwrap(),
        ingest_identity(ht, &[range("other", 0, 0, 9)]).unwrap(),
        ingest_identity(ht, &[range("events", 1, 0, 9)]).unwrap(),
        ingest_identity(ht, &[range("events", 0, 0, 10)]).unwrap(),
    ] {
        assert_ne!(a.key(), other.key());
    }
    assert_eq!(INGEST_TRANSFORMATION_VERSION, 1);
}

#[test]
fn a_malformed_batch_fails_before_it_costs_an_upload() {
    // Overlapping ranges on one partition are a consumer bug. Hashing them would
    // hide it behind a stable-looking key; the flusher builds the identity
    // before it registers intent or uploads a byte, so the bug surfaces here.
    let err = ingest_identity(
        HypertableId(1),
        &[range("events", 0, 0, 9), range("events", 0, 9, 12)],
    )
    .unwrap_err();
    assert!(
        matches!(
            err,
            ukiel_ingest::IngestError::Operation(OperationError::OverlappingOffsetRanges { .. })
        ),
        "{err:?}"
    );
}

/// The lost-acknowledgement case, end to end at the catalog boundary: the same
/// offsets, re-flushed with *different* output objects, must reconcile to the
/// original commit rather than double-applying.
#[tokio::test]
async fn a_replayed_flush_reconciles_to_the_original_commit() {
    let pg = Postgres::default().start().await.unwrap();
    let port = pg.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
    let catalog = PostgresCatalog::connect(&url).await.unwrap();
    catalog.migrate().await.unwrap();
    let ht = catalog
        .create_hypertable(
            "events",
            &json!({"fields": [
                {"name": "tenant_id", "type": "int64"},
                {"name": "ts", "type": "timestamp_ms"}
            ]}),
            &json!({"columns": ["day"]}),
            &["tenant_id".to_string()],
            "tenant_id",
        )
        .await
        .unwrap();

    let offsets = [range("events", 0, 0, 99)];
    let identity = ingest_identity(ht, &offsets).unwrap();
    assert_eq!(
        catalog.lookup_operation(&identity).await.unwrap(),
        OperationLookup::Absent
    );

    let part = |path: &str| PartMeta {
        path: path.to_string(),
        partition_values: json!({"day": "2026-07-01"}),
        packing_key_min: 1,
        packing_key_max: 1,
        row_count: 100,
        size_bytes: 100,
        level: 0,
        column_stats: None,
    };

    let CommitResult::Committed(first) = catalog
        .commit_with_offsets(
            ht,
            CommitOp::Add {
                parts: vec![part("first.parquet")],
            },
            &offsets,
            &identity,
        )
        .await
        .unwrap()
    else {
        panic!("expected a commit")
    };
    assert_eq!(
        catalog.lookup_operation(&identity).await.unwrap(),
        OperationLookup::Committed(first)
    );

    // The retry re-read the same Kafka offsets, re-encoded them, and uploaded to
    // fresh paths — as any retry must, since paths carry a UUID.
    let replay = catalog
        .commit_with_offsets(
            ht,
            CommitOp::Add {
                parts: vec![part("retry.parquet")],
            },
            &offsets,
            &ingest_identity(ht, &offsets).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(replay, CommitResult::AlreadyApplied(first));

    let live = catalog.live_parts(ht, None).await.unwrap();
    assert_eq!(live.len(), 1, "the replay applied nothing");
    assert_eq!(live[0].meta.path, "first.parquet");
    assert_eq!(
        catalog.ingest_offsets(ht, "events").await.unwrap().get(&0),
        Some(&100),
        "offsets advanced exactly once"
    );

    // And a *genuinely different* operation that overlaps an already-consumed
    // range is still an OffsetRace — a second ingest worker on one topic, not a
    // retry. Idempotency answers duplicate intent; it does not replace the CAS.
    let overlapping = [range("events", 0, 50, 149)];
    let err = catalog
        .commit_with_offsets(
            ht,
            CommitOp::Add {
                parts: vec![part("duplicate-worker.parquet")],
            },
            &overlapping,
            &ingest_identity(ht, &overlapping).unwrap(),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(err, ukiel_catalog::CatalogError::OffsetRace { .. }),
        "{err:?}"
    );
    assert_eq!(catalog.live_parts(ht, None).await.unwrap().len(), 1);
}

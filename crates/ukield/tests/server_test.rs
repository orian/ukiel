use std::time::Duration;

use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde_json::json;
use testcontainers_modules::kafka::apache::{KAFKA_PORT, Kafka};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tokio_util::sync::CancellationToken;
use ukield::config::UkieldConfig;

#[tokio::test(flavor = "multi_thread")]
async fn full_stack_ingest_to_query_over_http() {
    let pg = Postgres::default().start().await.expect("postgres");
    let pg_port = pg.get_host_port_ipv4(5432).await.unwrap();
    let kafka = Kafka::default().start().await.expect("kafka");
    let brokers = format!(
        "127.0.0.1:{}",
        kafka.get_host_port_ipv4(KAFKA_PORT).await.unwrap()
    );

    let config_toml = format!(
        r#"
        [catalog]
        url = "postgres://postgres:postgres@127.0.0.1:{pg_port}/postgres"
        [object_store]
        kind = "memory"
        [query]
        listen = "127.0.0.1:0"
        [ingest]
        brokers = "{brokers}"
        group_id = "ukield-test"
        flush_interval_ms = 500
        max_buffer_rows = 10000
        max_event_age_days = 30000

        [[tables]]
        name = "events"
        topic = "events"
        ts_column = "ts"
        packing_key = "tenant_id"
        sort_key = ["tenant_id", "ts"]
        partition_columns = ["day"]
        namespaces = [7]
        [[tables.columns]]
        name = "tenant_id"
        type = "int64"
        [[tables.columns]]
        name = "ts"
        type = "timestamp_ms"
        [[tables.columns]]
        name = "payload"
        type = "utf8"
        "#
    );
    let cfg: UkieldConfig = toml::from_str(&config_toml).unwrap();

    let shutdown = CancellationToken::new();
    let (bound_tx, bound_rx) = tokio::sync::oneshot::channel();
    let server = {
        let cfg = cfg.clone();
        let token = shutdown.clone();
        tokio::spawn(
            async move { ukield::run::run_with_bound_addr(cfg, token, Some(bound_tx)).await },
        )
    };
    let addr = tokio::time::timeout(Duration::from_secs(60), bound_rx)
        .await
        .expect("server did not report its address in time")
        .expect("server failed before binding");
    let base = format!("http://{addr}");
    let client = reqwest::Client::new();

    // Health first (server is up), then produce and poll for visibility.
    let health = client.get(format!("{base}/healthz")).send().await.unwrap();
    assert_eq!(health.status(), 200);

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("message.timeout.ms", "10000")
        .create()
        .unwrap();
    for i in 0..5 {
        let payload =
            json!({"tenant_id": 7, "ts": 1000 + i, "payload": format!("row-{i}")}).to_string();
        producer
            .send(
                FutureRecord::<(), _>::to("events").payload(&payload),
                Duration::from_secs(10),
            )
            .await
            .map_err(|(e, _)| e)
            .unwrap();
    }

    let mut rows = 0i64;
    for _ in 0..60 {
        let resp = client
            .post(format!("{base}/api/query"))
            .json(&json!({"namespace_id": 7, "sql": "SELECT count(*) AS n FROM events", "format": "rows"}))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let body: serde_json::Value = resp.json().await.unwrap();
        rows = body["rows"][0]["n"].as_i64().unwrap_or(0);
        if rows == 5 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert_eq!(rows, 5, "all produced rows must become queryable");

    // Readiness probe: catalog + object store reachable.
    let ready = client.get(format!("{base}/readyz")).send().await.unwrap();
    assert_eq!(ready.status(), 200);

    // Prometheus endpoint: the produce+query round trip has exercised every
    // async emission site (metrics P1) — this is their end-to-end proof.
    let metrics = client.get(format!("{base}/metrics")).send().await.unwrap();
    assert_eq!(metrics.status(), 200);
    let body = metrics.text().await.unwrap();
    for family in [
        "ingest_flush_duration_seconds",
        "ingest_last_flush_event_ts_seconds",
        "ingest_rows_total",
        "catalog_commit_duration_seconds",
        "query_requests_total",
        "ukield_worker_up",
    ] {
        assert!(
            body.contains(family),
            "missing metric family {family}:\n{body}"
        );
    }
    // Duration histograms render as buckets (aggregatable), not summary quantiles.
    assert!(
        body.contains("ingest_flush_duration_seconds_bucket"),
        "duration metric must use buckets, not summary quantiles"
    );

    // Graceful shutdown: cancel and the run future returns Ok.
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(30), server)
        .await
        .expect("shutdown timed out")
        .unwrap()
        .unwrap();
}

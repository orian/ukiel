use std::time::Duration;

use serde_json::json;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tokio_util::sync::CancellationToken;
use ukiel_catalog::PostgresCatalog;
use ukiel_core::{CommitOp, PartMeta};
use ukield::config::UkieldConfig;

/// The periodic collector's gauge families render on `/metrics` after a couple
/// of ticks, driven purely by catalog state (no Kafka: the consumer-lag family
/// is disabled by design when no table declares a topic).
#[tokio::test(flavor = "multi_thread")]
async fn collector_gauges_render_on_metrics() {
    let pg = Postgres::default().start().await.expect("postgres");
    let pg_port = pg.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgres://postgres:postgres@127.0.0.1:{pg_port}/postgres");

    // Seed one hypertable with a live part before the server starts, so the
    // first collector tick already has a live_parts label set to render.
    let seed = PostgresCatalog::connect(&url).await.unwrap();
    seed.migrate().await.unwrap();
    let ht = seed
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
    seed.commit(
        ht,
        CommitOp::Add {
            parts: vec![PartMeta {
                path: "l0-seed.parquet".to_string(),
                partition_values: json!({ "day": "2026-07-08" }),
                packing_key_min: 1,
                packing_key_max: 1,
                row_count: 1,
                size_bytes: 1,
                level: 0,
                column_stats: None,
            }],
        },
        None,
    )
    .await
    .unwrap();

    let config_toml = format!(
        r#"
        roles = ["query", "compactor", "gc"]
        [catalog]
        url = "{url}"
        [object_store]
        kind = "memory"
        [query]
        listen = "127.0.0.1:0"
        [collector]
        interval_ms = 100
        "#
    );
    let cfg: UkieldConfig = toml::from_str(&config_toml).unwrap();

    let shutdown = CancellationToken::new();
    let (bound_tx, bound_rx) = tokio::sync::oneshot::channel();
    let server = {
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

    // Poll /metrics until the collector has ticked and rendered live_parts.
    let mut body = String::new();
    for _ in 0..100 {
        let resp = client.get(format!("{base}/metrics")).send().await.unwrap();
        assert_eq!(resp.status(), 200);
        body = resp.text().await.unwrap();
        if body.contains("catalog_live_parts") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Families guaranteed by the seeded state + set-every-tick gauges.
    for family in [
        "catalog_live_parts",
        "compactor_backlog_groups",
        "compactor_unfinalized_partitions",
        "catalog_pending_objects",
        "catalog_unpurged_tombstones",
        "catalog_pool_connections",
        // Plan 41: partitions under compaction now, and the wedged-fleet signal
        // (an expired lease nobody is reclaiming).
        "catalog_compaction_leases_active",
        "catalog_compaction_lease_oldest_expired_age_seconds",
    ] {
        assert!(
            body.contains(family),
            "missing collector family {family}:\n{body}"
        );
    }
    // The seeded L0 part shows under its hypertable/level labels.
    assert!(
        body.contains("catalog_live_parts") && body.contains("hypertable=\"events\""),
        "live_parts must carry the hypertable label:\n{body}"
    );

    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(30), server)
        .await
        .expect("shutdown timed out")
        .unwrap()
        .unwrap();
}

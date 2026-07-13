//! S10 — a catalog outage degrades ukield and it recovers, without a restart
//! (plan 42).
//!
//! The failure being modelled is a managed writer failover: every connection
//! drops, nothing answers for a while, then the same database is back with its
//! data intact. Before this plan, that killed the process — the first worker
//! error cancelled every role — and the fleet's restarts then hammered the
//! primary exactly while it was being promoted.
//!
//! What this proves, in one process:
//!
//! 1. liveness stays 200 throughout (an orchestrator must not restart the fleet);
//! 2. readiness turns 503, and a new query is refused with a bounded, retryable
//!    503 rather than answered from a stale idea of which parts are live;
//! 3. ingest stops advancing catalog offsets while degraded;
//! 4. no role and no process exits;
//! 5. after the catalog returns, the roles pass Degraded → Reconciling → Healthy
//!    on their own, and every event produced *during* the outage becomes visible
//!    exactly once — because the rebuilt ingest reloads offsets from the catalog
//!    and re-seeks Kafka, rather than trusting anything it was holding.
//!
//! What it does NOT prove: a commit that was acknowledged and then lost. That
//! needs a fault point *inside* the transaction response path, and the machinery
//! to reconcile it (deterministic operation keys) is plan 43 — which remains the
//! production gate for active-active compaction.
//!
//! Run: `make e2e-ha` (adds the opt-in Toxiproxy compose profile).

use std::collections::HashSet;
use std::time::{Duration, Instant};

use tokio_util::sync::CancellationToken;
use ukiel_e2e::fault_proxy::FaultProxy;
use ukiel_e2e::{Stack, env_or};

/// Polls until `f` holds, or panics with the last observed value.
async fn eventually<T, F, Fut>(what: &str, timeout: Duration, mut f: F) -> T
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Option<T>>,
{
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(v) = f().await {
            return v;
        }
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires the compose stack with the `ha` profile (make e2e-ha)"]
async fn a_catalog_outage_degrades_and_recovers_ukield_without_a_restart() {
    // `make e2e` runs every --ignored test, but it does not start the opt-in
    // Toxiproxy profile. Skip rather than fail: this scenario is `make e2e-ha`'s,
    // and a normal e2e run must be untouched by the HA profile's existence.
    if env_or("UKIEL_E2E_HA", "0") != "1" {
        eprintln!("skipping S10: set UKIEL_E2E_HA=1 (see `make e2e-ha`)");
        return;
    }

    let api = format!(
        "http://127.0.0.1:{}",
        env_or("UKIEL_TOXIPROXY_API_PORT", "8474")
    );
    let proxied_port = env_or("UKIEL_TOXIPROXY_PG_PORT", "15432");
    // `listen` is inside the container network; the same port is published.
    let proxy = FaultProxy::create(
        &api,
        "catalog",
        &format!("0.0.0.0:{proxied_port}"),
        "postgres:5432",
    )
    .await;

    // A full ukield — every role — whose catalog runs through the proxy.
    let stack = Stack::start().await;
    let table = stack.create_events_table().await;
    let catalog_url = format!("postgres://postgres:postgres@127.0.0.1:{proxied_port}/postgres");
    let shutdown = CancellationToken::new();
    let (base, server) = stack
        .spawn_ukield_with_catalog(&table, &catalog_url, shutdown.clone())
        .await;
    let client = reqwest::Client::new();

    // --- 1. Healthy baseline -------------------------------------------------
    let produced_before = stack
        .produce_events_for(&table, table.namespace, "pre", 5)
        .await;
    eventually(
        "the baseline to be queryable",
        Duration::from_secs(60),
        || async {
            let resp = client.get(format!("{base}/readyz")).send().await.ok()?;
            (resp.status() == 200).then_some(())
        },
    )
    .await;

    // --- 2. The catalog goes away -------------------------------------------
    proxy.cut().await;

    // Liveness must not flinch. If it did, every process in the fleet would be
    // restarted at once — turning a 30-second failover into a real outage.
    let mut server = server;
    for _ in 0..5 {
        match client.get(format!("{base}/healthz")).send().await {
            Ok(resp) => assert_eq!(
                resp.status(),
                200,
                "liveness must not depend on the catalog"
            ),
            // The server is gone. Say *why*: a bare "connection refused" would
            // send the next person hunting the wrong bug.
            Err(e) => {
                let why = match tokio::time::timeout(Duration::from_secs(5), &mut server).await {
                    Ok(Ok(Err(err))) => format!("ukield exited: {err:#}"),
                    Ok(Ok(Ok(()))) => "ukield exited cleanly".to_string(),
                    Ok(Err(join)) => format!("ukield panicked: {join}"),
                    Err(_) => format!("ukield is alive but unreachable: {e}"),
                };
                panic!("liveness must survive a catalog outage — {why}");
            }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert!(!server.is_finished(), "the process must not exit");

    // Readiness tells the truth, and a new query is refused — not answered from
    // a stale plan.
    let body = eventually(
        "readiness to report the outage",
        Duration::from_secs(30),
        || async {
            let resp = client.get(format!("{base}/readyz")).send().await.ok()?;
            if resp.status() != 503 {
                return None;
            }
            resp.json::<serde_json::Value>().await.ok()
        },
    )
    .await;
    assert_eq!(body["ready"], serde_json::json!(false), "{body}");

    let resp = client
        .post(format!("{base}/api/query"))
        .json(&serde_json::json!({
            "namespace_id": table.namespace.0,
            "sql": "SELECT payload FROM events",
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        503,
        "a new query must be refused, not guessed"
    );
    assert!(
        resp.headers().contains_key("retry-after"),
        "a 503 must tell the client when to come back"
    );
    let err: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(err["retryable"], serde_json::json!(true));

    // Ingest is degraded: it stops advancing catalog offsets. (It cannot even
    // reach the catalog to advance them — which is the point: exactly-once is a
    // property of the catalog transaction, not of the consumer's memory.)
    let offsets_during = stack
        .ingest_offset_sum(table.hypertable_id, &table.topic)
        .await;

    // Events keep arriving throughout the outage. They must not be lost.
    let produced_during = stack
        .produce_events_for(&table, table.namespace, "during", 5)
        .await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(
        stack
            .ingest_offset_sum(table.hypertable_id, &table.topic)
            .await,
        offsets_during,
        "ingest must not advance catalog offsets while the catalog is unreachable"
    );
    assert!(!server.is_finished(), "still no process exit");

    // --- 3. The catalog comes back ------------------------------------------
    let restored = Instant::now();
    proxy.restore().await;

    // The roles recover *themselves*: no restart, no new process, no operator.
    eventually("readiness to recover", Duration::from_secs(120), || async {
        let resp = client.get(format!("{base}/readyz")).send().await.ok()?;
        (resp.status() == 200).then_some(())
    })
    .await;
    let recovery = restored.elapsed();

    // Every event — before and during the outage — is visible exactly once. The
    // rebuilt ingest reloaded offsets from the catalog and re-seeked Kafka, so
    // whatever it was holding when the connection died is irrelevant.
    let expected: HashSet<String> = produced_before.into_iter().chain(produced_during).collect();
    let want = expected.len();
    let got = eventually(
        "every event to be queryable",
        Duration::from_secs(120),
        || {
            let stack = &stack;
            async move {
                let rows = stack
                    .query(table.namespace, "SELECT payload FROM events")
                    .await
                    .ok()?;
                let got: HashSet<String> = rows
                    .as_array()?
                    .iter()
                    .map(|r| r["payload"].as_str().unwrap().to_string())
                    .collect();
                (got.len() >= want).then_some(got)
            }
        },
    )
    .await;
    assert_eq!(
        got, expected,
        "exactly once: nothing lost, nothing duplicated"
    );

    // Compaction converges from current live state after the outage.
    stack.run_compaction_to_quiescence().await;
    assert_eq!(
        stack
            .query(table.namespace, "SELECT count(*) AS n FROM events")
            .await
            .unwrap()[0]["n"]
            .as_i64()
            .unwrap() as usize,
        expected.len(),
        "compaction after recovery changed the data"
    );

    // The retry load was bounded, and the process is the one we started.
    let metrics = client.get(format!("{base}/metrics")).send().await.unwrap();
    let body = metrics.text().await.unwrap();
    assert!(
        body.contains("catalog_retry_total"),
        "the recovery must be observable"
    );
    assert!(
        body.contains("ukield_role_state"),
        "role state must be observable"
    );
    assert!(!server.is_finished(), "the process never exited");
    println!("S10: recovered to ready in {recovery:?}");

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(30), server).await;
    proxy.restore().await;
}

//! S3 — Freshness (invariant I3).
//!
//! With the spec's real 10s flush interval, an event must be queryable within
//! `flush_interval + grace`. We timestamp each produced marker, watch the query
//! endpoint for its first appearance, and assert the p95 produce→visible
//! latency is under 20s.
//!
//! Ignored by default; run against the compose stack with `make e2e` or
//! `cargo test -p ukiel-e2e -- --ignored`.

use std::collections::HashMap;
use std::collections::HashSet;
use std::time::{Duration, Instant};

use serde_json::json;
use ukiel_core::NamespaceId;
use ukiel_e2e::Stack;

/// Query the currently-visible payloads; for any produced marker seen for the
/// first time, record its produce→visible latency.
async fn record_visible(
    stack: &Stack,
    ns: NamespaceId,
    produced: &HashMap<String, Instant>,
    latency: &mut HashMap<String, Duration>,
) {
    let rows = stack
        .query(ns, "SELECT payload FROM events")
        .await
        .expect("payload query");
    let visible: HashSet<String> = rows
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["payload"].as_str().unwrap().to_string())
        .collect();
    let now = Instant::now();
    for (payload, produced_at) in produced {
        if visible.contains(payload) && !latency.contains_key(payload) {
            latency.insert(payload.clone(), now.duration_since(*produced_at));
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires the docker-compose stack (make e2e)"]
async fn events_are_queryable_within_the_freshness_sla() {
    let stack = Stack::start().await;
    let table = stack.create_events_table_multi(1).await;
    let ns = table.namespace;
    // The spec's real freshness knob: 10s flush interval, SLA <= 20s.
    let ingest = stack.spawn_ingest_with(&table, 10_000).await;

    const N: usize = 12;
    let mut produced: HashMap<String, Instant> = HashMap::new();
    let mut latency: HashMap<String, Duration> = HashMap::new();

    // Produce a marker per second, spanning more than one flush window, while
    // polling for first-visibility in between.
    for i in 0..N {
        let payload = format!("mark-{i}");
        let msg =
            json!({"tenant_id": ns.0, "ts": 1_000 + i as i64, "payload": payload}).to_string();
        stack.produce_raw(&table, &msg).await;
        produced.insert(payload, Instant::now());

        for _ in 0..4 {
            record_visible(&stack, ns, &produced, &mut latency).await;
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }

    // Drain: keep polling until every marker has been seen (or a hard deadline).
    let drain_deadline = Instant::now() + Duration::from_secs(25);
    while latency.len() < produced.len() && Instant::now() < drain_deadline {
        record_visible(&stack, ns, &produced, &mut latency).await;
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    ingest.stop().await;

    assert_eq!(
        latency.len(),
        N,
        "every marker must become visible ({}/{N})",
        latency.len()
    );

    let mut lats: Vec<Duration> = latency.values().copied().collect();
    lats.sort();
    let p95 = lats[((lats.len() * 95).div_ceil(100)).min(lats.len()) - 1];
    assert!(
        p95 <= Duration::from_secs(20),
        "p95 visible-latency {p95:?} exceeds the 20s freshness SLA (all: {lats:?})"
    );
}

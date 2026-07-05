//! S2 — Crash & resume (invariant I1: exactly-once).
//!
//! Produce continuously; abort the ingest worker mid-stream (a crash — no
//! graceful final flush); start a fresh worker; keep producing. Because Kafka
//! offsets live in the catalog and advance only inside the part commit, the
//! fresh worker resumes from exactly the last committed position: every
//! produced message ends up queryable exactly once — no loss, no duplication —
//! regardless of where the crash fell relative to a flush.
//!
//! Ignored by default; run against the compose stack with `make e2e` or
//! `cargo test -p ukiel-e2e -- --ignored`.

use std::collections::HashSet;
use std::time::Duration;

use ukiel_e2e::Stack;

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires the docker-compose stack (make e2e)"]
async fn crash_mid_stream_then_resume_is_exactly_once() {
    let stack = Stack::start().await;
    let table = stack.create_events_table_multi(1).await;
    let ns = table.namespace;

    let mut expected: HashSet<String> = HashSet::new();

    // Round 1: produce, let some of it flush, then CRASH the worker abruptly.
    let ingest = stack.spawn_ingest(&table).await;
    for got in stack.produce_events_for(&table, ns, "r1", 40).await {
        expected.insert(got);
    }
    // Wait until at least some rows have been committed (a flush happened), so
    // the crash lands after a real commit — the interesting exactly-once case.
    let seen = stack
        .wait_for_row_count(ns, 1, Duration::from_secs(30))
        .await;
    assert!(
        seen >= 1,
        "expected at least one committed row before crash"
    );
    ingest.abort(); // no graceful flush — simulate a process kill

    // More rows arrive while no worker is running; they wait in Kafka.
    for got in stack.produce_events_for(&table, ns, "r2", 40).await {
        expected.insert(got);
    }

    // Round 2: a fresh worker resumes from the catalog's stored offset.
    let ingest = stack.spawn_ingest(&table).await;
    for got in stack.produce_events_for(&table, ns, "r3", 20).await {
        expected.insert(got);
    }

    // Everything produced across the crash converges exactly once.
    let want = expected.len() as i64; // 100 distinct payloads
    let count = stack
        .wait_for_row_count(ns, want, Duration::from_secs(60))
        .await;
    ingest.stop().await;

    assert_eq!(
        count, want,
        "every produced row must be queryable after resume (no loss)"
    );

    // Exact-set check: no duplicates, no losses — the row count equalling the
    // distinct-payload count already rules out duplicates, but assert the set.
    let rows = stack
        .query(ns, "SELECT payload FROM events")
        .await
        .expect("payload query");
    let got: HashSet<String> = rows
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["payload"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(
        got, expected,
        "resumed result must equal the produced set exactly"
    );

    // And the raw row count equals the distinct count: zero duplication.
    let total = stack
        .query(ns, "SELECT count(*) AS n FROM events")
        .await
        .expect("count query");
    assert_eq!(total[0]["n"].as_i64().unwrap(), want, "no duplicate rows");
}

//! S0 — the simplest end-to-end path: create a table, ingest 100 rows through
//! real Kafka → MinIO → catalog, and query them back over HTTP.
//!
//! Ignored by default; run against the compose stack with `make e2e` or
//! `cargo test -p ukiel-e2e -- --ignored`.

use std::collections::HashSet;
use std::time::Duration;

use ukiel_e2e::Stack;

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires the docker-compose stack (make e2e)"]
async fn create_ingest_100_and_query() {
    let stack = Stack::start().await;
    let table = stack.create_events_table().await;
    let ingest = stack.spawn_ingest(&table);

    // 1. + 2. Ingest 100 rows.
    let produced = stack.produce_events(&table, 100).await;
    assert_eq!(produced.len(), 100);

    // 3. Query them back — poll until all 100 are visible (freshness ≤ flush + grace).
    let count = stack
        .wait_for_row_count(table.namespace, 100, Duration::from_secs(30))
        .await;
    assert_eq!(count, 100, "expected all 100 ingested rows to be queryable");

    // The payloads match exactly what we produced — no loss, no duplication.
    let rows = stack
        .query(table.namespace, "SELECT payload FROM events")
        .await
        .expect("payload query");
    let got: HashSet<String> = rows
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["payload"].as_str().unwrap().to_string())
        .collect();
    let expected: HashSet<String> = produced.into_iter().collect();
    assert_eq!(got, expected, "queried payloads must equal produced set");

    ingest.stop().await;
}

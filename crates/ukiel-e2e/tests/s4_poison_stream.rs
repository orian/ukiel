//! S4 — Poison stream (invariant I6: progress under poison).
//!
//! Interleave malformed messages (non-JSON, missing timestamp, missing or
//! wrong-typed packing key, empty) with valid events. The valid events must all
//! become queryable, no poison message may turn into a row, and ingest offsets
//! must advance *past* the garbage — a poison message can never stall the stream
//! or be re-read forever.
//!
//! Ignored by default; run against the compose stack with `make e2e` or
//! `cargo test -p ukiel-e2e -- --ignored`.

use std::collections::HashSet;
use std::time::Duration;

use ukiel_e2e::Stack;

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires the docker-compose stack (make e2e)"]
async fn poison_messages_are_skipped_and_never_stall_the_stream() {
    let stack = Stack::start().await;
    let table = stack.create_events_table_multi(1).await;
    let ns = table.namespace;
    let ingest = stack.spawn_ingest(&table).await;

    // Poison at the head of the stream (offsets 0..2).
    stack.produce_raw(&table, "this is not json").await;
    stack
        .produce_raw(&table, r#"{"tenant_id": 1, "payload": "no-ts"}"#)
        .await;
    stack.produce_raw(&table, "").await;

    // 30 valid rows (offsets 3..32).
    let expected: HashSet<String> = stack
        .produce_events_for(&table, ns, "ok", 30)
        .await
        .into_iter()
        .collect();

    // More poison at the tail (offsets 33..35): missing packing key, wrong-typed
    // packing key, wrong-typed ts. All parse as JSON but can't be encoded.
    stack
        .produce_raw(&table, r#"{"ts": 5, "payload": "no-key"}"#)
        .await;
    stack
        .produce_raw(
            &table,
            r#"{"tenant_id": "abc", "ts": 5, "payload": "bad-key"}"#,
        )
        .await;
    stack
        .produce_raw(
            &table,
            r#"{"tenant_id": 1, "ts": "nope", "payload": "bad-ts"}"#,
        )
        .await;

    let total_messages = 3 + 30 + 3; // valid + poison, all on one partition

    // Valid rows converge...
    let count = stack
        .wait_for_row_count(ns, 30, Duration::from_secs(30))
        .await;
    assert_eq!(count, 30, "all valid rows must be queryable");

    // ...and offsets advance past every poison message (no stall, no re-read).
    let mut offset_sum = 0;
    for _ in 0..120 {
        offset_sum = stack
            .ingest_offset_sum(table.hypertable_id, &table.topic)
            .await;
        if offset_sum >= total_messages as i64 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    ingest.stop().await;

    assert_eq!(
        offset_sum, total_messages as i64,
        "ingest offset must advance past all {total_messages} messages including poison"
    );

    // Exactly the valid payloads are present — no poison leaked in as a row.
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
    assert_eq!(got, expected, "only valid rows may appear");
}

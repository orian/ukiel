//! S7 — Key deletion (invariant I7).
//!
//! Several namespaces share one packed hypertable (their rows co-locate in the
//! same Parquet files). We delete one namespace's packing key and assert: that
//! namespace's queries return nothing, every neighbor sharing those packed
//! files is untouched, and no live part still claims the deleted key. A second
//! delete is a no-op (idempotent).
//!
//! Ignored by default; run against the compose stack with `make e2e` or
//! `cargo test -p ukiel-e2e -- --ignored`.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use ukiel_core::NamespaceId;
use ukiel_e2e::Stack;

async fn payloads(stack: &Stack, ns: NamespaceId) -> HashSet<String> {
    let rows = stack
        .query(ns, "SELECT payload FROM events")
        .await
        .expect("payload query");
    rows.as_array()
        .unwrap()
        .iter()
        .map(|r| r["payload"].as_str().unwrap().to_string())
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires the docker-compose stack (make e2e)"]
async fn deleting_a_key_erases_it_and_leaves_neighbors_intact() {
    let stack = Stack::start().await;
    let table = stack.create_events_table_multi(3).await;
    let namespaces = table.namespaces.clone();
    let ingest = stack.spawn_ingest(&table).await;

    // Interleaved load: all three namespaces' rows land in the same packed L0s.
    let mut model: HashMap<NamespaceId, HashSet<String>> = HashMap::new();
    for &ns in &namespaces {
        let got = stack
            .produce_events_for(&table, ns, &format!("ns{}", ns.0), 15)
            .await;
        model.insert(ns, got.into_iter().collect());
    }
    for &ns in &namespaces {
        let want = model[&ns].len() as i64;
        let got = stack
            .wait_for_row_count(ns, want, Duration::from_secs(30))
            .await;
        assert_eq!(got, want, "all rows for {ns} must be visible before delete");
    }
    ingest.stop().await;

    // Erase the first namespace's key.
    let victim = namespaces[0];
    let survivors = &namespaces[1..];
    let stats = stack.delete_key(table.hypertable_id, victim.0).await;
    assert_eq!(
        stats.rows_deleted,
        model[&victim].len() as i64,
        "every one of the victim's rows must be counted deleted"
    );
    assert!(
        !stack
            .any_live_part_claims(table.hypertable_id, victim.0)
            .await,
        "no live part may still claim the deleted key {victim}"
    );

    // I7: the victim sees nothing; neighbors in the same packed files are intact.
    assert!(
        payloads(&stack, victim).await.is_empty(),
        "deleted namespace {victim} must return no rows"
    );
    for &ns in survivors {
        assert_eq!(
            payloads(&stack, ns).await,
            model[&ns],
            "neighbor {ns} must be untouched by the deletion"
        );
    }

    // Idempotent: deleting again removes nothing.
    let stats = stack.delete_key(table.hypertable_id, victim.0).await;
    assert_eq!(stats.dropped_parts + stats.rewritten_parts, 0);
    assert_eq!(stats.rows_deleted, 0);
}

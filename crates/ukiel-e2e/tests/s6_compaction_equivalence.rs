//! S6 — Compaction equivalence (invariants I4 rewrite-equivalence, I5 race-safety).
//!
//! Multiple namespaces share one packed hypertable. We ingest an interleaved
//! load over several flush rounds (so many L0 files accumulate, each holding
//! rows for several tenants), then run the compactor to quiescence *while
//! ingest continues*. The assertion: every namespace's query result equals the
//! exact set of rows produced for it — compaction changed only file layout
//! (L0 count down, L1 files created), never the queryable data, and never lost
//! or duplicated a row under the ingest/compaction race.
//!
//! Ignored by default; run against the compose stack with `make e2e` or
//! `cargo test -p ukiel-e2e -- --ignored`.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use ukiel_core::NamespaceId;
use ukiel_e2e::Stack;

/// Payload set a namespace can currently see via SQL.
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
async fn compaction_preserves_results_while_ingest_continues() {
    let stack = Stack::start().await;
    let table = stack.create_events_table_multi(3).await;
    let namespaces = table.namespaces.clone();
    let ingest = stack.spawn_ingest(&table).await;

    // Oracle: per-namespace set of every payload we've produced for it.
    let mut model: HashMap<NamespaceId, HashSet<String>> = HashMap::new();
    let produce_round = |round: usize| {
        // returns the (ns, tag, count) plan for this round
        namespaces
            .iter()
            .map(move |&ns| (ns, format!("ns{}-r{round}", ns.0)))
            .collect::<Vec<_>>()
    };

    // Phase 1: several rounds with pauses so ingest flushes a distinct L0 each
    // round — building up the small-file pile compaction exists to collapse.
    for round in 0..4 {
        for (ns, tag) in produce_round(round) {
            let got = stack.produce_events_for(&table, ns, &tag, 10).await;
            model.entry(ns).or_default().extend(got);
        }
        tokio::time::sleep(Duration::from_millis(700)).await;
    }
    for &ns in &namespaces {
        let want = model[&ns].len() as i64;
        let got = stack
            .wait_for_row_count(ns, want, Duration::from_secs(30))
            .await;
        assert_eq!(got, want, "phase-1 rows for {ns} must all be visible");
    }
    let l0_before = stack.count_parts_at_level(table.hypertable_id, 0).await;
    assert!(
        l0_before >= 2,
        "need multiple L0 files to compact, got {l0_before}"
    );

    // Phase 2: compactor runs concurrently with more ingest (the race).
    let compactor = stack.spawn_compactor();
    for round in 4..7 {
        for (ns, tag) in produce_round(round) {
            let got = stack.produce_events_for(&table, ns, &tag, 10).await;
            model.entry(ns).or_default().extend(got);
        }
        tokio::time::sleep(Duration::from_millis(400)).await;
    }

    // Everything produced must become visible even as compaction rewrites files.
    for &ns in &namespaces {
        let want = model[&ns].len() as i64;
        let got = stack
            .wait_for_row_count(ns, want, Duration::from_secs(30))
            .await;
        assert_eq!(got, want, "phase-2 rows for {ns} must all be visible");
    }

    // Stop the racing writers, then drain compaction deterministically.
    ingest.stop().await;
    compactor.stop().await;
    stack.run_compaction_to_quiescence().await;

    // I4/I5: exact per-namespace equivalence — nothing lost, duplicated, or
    // leaked across namespaces despite living in shared, then compacted, files.
    for &ns in &namespaces {
        assert_eq!(
            payloads(&stack, ns).await,
            model[&ns],
            "namespace {ns} query result must equal exactly its produced rows"
        );
    }

    // Compaction actually happened: L1 files exist and the single-partition L0
    // pile collapsed (drain merges any group of >= 2, leaving at most a lone
    // trailing L0 that never found a pair).
    let l1 = stack.count_parts_at_level(table.hypertable_id, 1).await;
    let l0_after = stack.count_parts_at_level(table.hypertable_id, 0).await;
    assert!(
        l1 >= 1,
        "compaction should have produced at least one L1 file"
    );
    assert!(
        l0_after <= 1,
        "L0 files should be collapsed (was {l0_before}, now {l0_after})"
    );
}

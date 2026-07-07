//! S8 — GC hygiene (invariant I8, storage lifecycle).
//!
//! After an S6-style load-plus-compaction run and a key deletion, run GC to
//! quiescence with **zero graces** and assert the storage-lifecycle bijection:
//! every object in the bucket is the path of a live part, and every live part's
//! object exists — no more, no less. And no *referenced* object was removed:
//! every namespace's query still returns its full model.
//!
//! The reaper is fenced by the minimum worker cursor (a lagging feed consumer
//! must never have its file reaped). So this scenario keeps a single compactor
//! cursor and advances it past the deleting commit (a final drain pass) before
//! running GC — otherwise the delete's tombstones would be un-reapable and the
//! bucket would carry orphaned objects the bijection forbids.
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
async fn gc_leaves_bucket_and_live_parts_in_bijection() {
    let stack = Stack::start().await;
    let table = stack.create_events_table_multi(3).await;
    let namespaces = table.namespaces.clone();
    let ingest = stack.spawn_ingest(&table).await;

    // Several rounds of interleaved ingest so many L0 files accumulate.
    let mut model: HashMap<NamespaceId, HashSet<String>> = HashMap::new();
    for round in 0..4 {
        for &ns in &namespaces {
            let got = stack
                .produce_events_for(&table, ns, &format!("ns{}-r{round}", ns.0), 10)
                .await;
            model.entry(ns).or_default().extend(got);
        }
        tokio::time::sleep(Duration::from_millis(400)).await;
    }
    for &ns in &namespaces {
        let want = model[&ns].len() as i64;
        let got = stack
            .wait_for_row_count(ns, want, Duration::from_secs(30))
            .await;
        assert_eq!(got, want, "all rows for {ns} must be visible before GC");
    }
    ingest.stop().await;

    // Compact the L0 pile into L1+ runs (creates tombstones for GC to reap).
    // Single worker cursor keeps the reaper fence deterministic.
    stack.run_compaction_to_quiescence().await;

    // Erase one namespace's key (a REPLACE rewrite: more tombstones).
    let victim = namespaces[0];
    stack.delete_key(table.hypertable_id, victim.0).await;
    model.insert(victim, HashSet::new());

    // Advance the compactor cursor past the deleting commit so its tombstones
    // become reapable (the fence is min worker cursor, not wall-clock).
    stack.run_compaction_to_quiescence().await;

    // GC with zero graces until nothing is reaped or swept.
    stack.run_gc_to_quiescence().await;

    // I8 bijection: the bucket and the live-part object set are identical.
    let live_paths: HashSet<String> = stack
        .catalog
        .live_parts(table.hypertable_id, None)
        .await
        .expect("live_parts")
        .into_iter()
        .map(|p| p.meta.path)
        .collect();
    let bucket = stack.object_paths(table.hypertable_id).await;
    assert_eq!(
        bucket,
        live_paths,
        "every bucket object must be a live part and every live part its object\n\
         bucket-only: {:?}\n live-only: {:?}",
        bucket.difference(&live_paths).collect::<Vec<_>>(),
        live_paths.difference(&bucket).collect::<Vec<_>>(),
    );
    assert!(
        !live_paths.is_empty(),
        "the run left live parts to reference"
    );

    // No referenced object was removed: every namespace still sees its model.
    for &ns in &namespaces {
        assert_eq!(
            payloads(&stack, ns).await,
            model[&ns],
            "namespace {ns} query result after GC must equal its oracle model"
        );
    }
}

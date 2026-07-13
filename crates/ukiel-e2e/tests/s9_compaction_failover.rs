//! S9 — Active-active compaction failover (plan 41).
//!
//! Two compactor replicas share one fleet cursor. One of them claims a partition
//! and dies mid-merge, without releasing anything and without emitting a feed
//! event. The assertions are the HA acceptance criteria:
//!
//! 1. While the dead owner's lease is still live, the peer does **no** work on
//!    that partition — it does not duplicate the Parquet read/merge/upload.
//! 2. Once the lease expires, the peer claims the partition and the backlog
//!    converges **without a new feed event**: the cursor is already at head, so
//!    only live state can reveal the abandoned work.
//! 3. Data is intact throughout, and convergence lands inside `ttl + poll`.
//!
//! Ignored by default; run against the compose stack with `make e2e`.

use std::collections::HashSet;
use std::time::{Duration, Instant};

use ukiel_compactor::compactor::{Compactor, CompactorConfig};
use ukiel_e2e::Stack;

const TTL: Duration = Duration::from_secs(4);

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires the docker-compose stack (make e2e)"]
async fn a_dead_compactor_replica_hands_its_partition_to_a_peer() {
    let stack = Stack::start().await;
    let table = stack.create_events_table().await;
    let ingest = stack.spawn_ingest_with(&table, 300).await;

    // Four flush rounds -> four L0 files in one partition: a real backlog.
    let mut expected: HashSet<String> = HashSet::new();
    for round in 0..4 {
        expected.extend(
            stack
                .produce_events_for(&table, table.namespace, &format!("r{round}"), 5)
                .await,
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let seen = stack
        .wait_for_row_count(
            table.namespace,
            expected.len() as i64,
            Duration::from_secs(30),
        )
        .await;
    assert_eq!(seen, expected.len() as i64, "ingest did not land");
    ingest.stop().await;

    let ht = table.hypertable_id;
    let partitions: Vec<_> = stack
        .catalog
        .compaction_candidates(ht, 2, 2, 64)
        .await
        .expect("candidates");
    assert!(
        !partitions.is_empty(),
        "the ingested L0 files must form a compaction backlog"
    );
    let partition = partitions[0].clone();

    // Replica A claims the partition and dies: no release, no commit, and — the
    // dangerous part — the fleet cursor is already at feed head, so nothing
    // will ever wake anyone about this partition again.
    let dead_owner = uuid::Uuid::new_v4();
    stack
        .catalog
        .try_acquire_compaction_lease(ht, &partition, dead_owner, TTL)
        .await
        .expect("acquire")
        .expect("partition is free");
    let head = stack.catalog.feed_head(ht).await.expect("feed head");
    stack
        .catalog
        .set_cursor("fleet", ht, head)
        .await
        .expect("cursor");

    // Replica B, sharing the cursor name.
    let b = Compactor::new(
        stack.catalog.clone(),
        stack.store.clone(),
        CompactorConfig {
            worker_name: "fleet".to_string(),
            lease_ttl_ms: TTL.as_millis() as u64,
            lease_renew_interval_ms: 1_000,
            poll_interval_ms: 200,
            ..CompactorConfig::default()
        },
    );

    // (1) A's lease is live: B must not touch the partition, however much
    // backlog it can see.
    let stats = b.run_once().await.expect("pass");
    assert_eq!(
        stats.merged_groups, 0,
        "a partition owned by a live peer is not compacted twice"
    );
    assert_eq!(stats.conflicts, 0);

    // (2) A never comes back. Within ttl + one poll, B claims the partition and
    // converges — with no new feed event to wake it.
    let started = Instant::now();
    let deadline = TTL + Duration::from_secs(2);
    let mut merged = 0;
    while started.elapsed() < deadline {
        merged += b.run_once().await.expect("pass").merged_groups;
        if merged > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert!(
        merged > 0,
        "the peer must reclaim the expired lease within ttl + poll ({:?})",
        started.elapsed()
    );
    assert_eq!(
        stack.catalog.feed_head(ht).await.expect("head").0,
        head.0 + merged as i64,
        "the only new commits are the peer's own merges — nothing woke it"
    );

    // (3) The partition converged and no row was lost or duplicated.
    stack.run_compaction_to_quiescence().await;
    let rows = stack
        .query(table.namespace, "SELECT payload FROM events")
        .await
        .expect("query");
    let got: HashSet<String> = rows
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["payload"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(got, expected, "compaction after failover changed the data");
    assert_eq!(
        stack.count_parts_at_level(ht, 0).await,
        0,
        "the abandoned L0 backlog is gone"
    );
}

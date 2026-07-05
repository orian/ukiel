//! S5 — Scale smoke (invariant I2 isolation + catalog planning).
//!
//! 1,000 namespaces, a couple of rows each, all packed into one hypertable.
//! This is the 1M-tenant math in miniature: packing must keep the *file* count
//! tiny and independent of the *tenant* count, and a per-namespace query must
//! still return exactly that namespace's rows via a bounded catalog part list —
//! never a per-tenant file explosion, never an object-store listing.
//!
//! Not a perf benchmark — it asserts correctness and bounded fan-out.
//!
//! Ignored by default; run against the compose stack with `make e2e` or
//! `cargo test -p ukiel-e2e -- --ignored`.

use std::collections::HashSet;
use std::time::Duration;

use ukiel_e2e::Stack;

const TENANTS: usize = 1_000;
const ROWS_EACH: usize = 2;

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires the docker-compose stack (make e2e)"]
async fn a_thousand_tenants_stay_isolated_with_bounded_pruning() {
    let stack = Stack::start().await;
    let table = stack.create_events_table_multi(TENANTS).await;
    let namespaces = table.namespaces.clone();
    let ingest = stack.spawn_ingest(&table).await;

    // Round-robin produce so each flush window mixes many tenants into shared,
    // heavily-packed files.
    for round in 0..ROWS_EACH {
        for &ns in &namespaces {
            let msg = serde_json::json!({
                "tenant_id": ns.0,
                "ts": 1_000 + round as i64,
                "payload": format!("ns{}-{round}", ns.0),
            })
            .to_string();
            stack.produce_raw(&table, &msg).await;
        }
    }

    // The last-produced tenant carries the highest Kafka offset on the single
    // partition, so once it is fully visible, everything before it is committed.
    let last = *namespaces.last().unwrap();
    let seen = stack
        .wait_for_row_count(last, ROWS_EACH as i64, Duration::from_secs(60))
        .await;
    assert_eq!(seen, ROWS_EACH as i64, "ingest must drain all tenants");
    ingest.stop().await;

    // Packing kept the physical file count tiny despite 1,000 tenants — the
    // whole point of shared hypertables over per-tenant tables.
    let total_parts = stack.live_part_count(table.hypertable_id, None).await;
    assert!(
        total_parts < 100,
        "1,000 tenants must not explode into files: {total_parts} live parts"
    );

    // Sample 50 tenants deterministically (every 20th) and check each in full.
    for &ns in namespaces.iter().step_by(TENANTS / 50) {
        // Correctness + isolation: exactly this tenant's rows, nothing else.
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
        let want: HashSet<String> = (0..ROWS_EACH).map(|r| format!("ns{}-{r}", ns.0)).collect();
        assert_eq!(got, want, "tenant {ns} must see exactly its own rows");

        // Bounded fan-out: the catalog returns a small, file-count-bounded set
        // of parts for this key — not one file per tenant.
        let pruned = stack.live_part_count(table.hypertable_id, Some(ns.0)).await;
        assert!(
            pruned <= total_parts && pruned < 100,
            "per-key part list must stay bounded: {pruned} for tenant {ns}"
        );
    }
}

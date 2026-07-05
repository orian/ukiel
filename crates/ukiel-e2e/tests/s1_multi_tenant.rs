//! S1 — Multi-tenant happy path (invariants I1 exactly-once, I2 isolation).
//!
//! Ten namespaces with skewed row counts all stream into one packed hypertable,
//! so small tenants share Parquet files. Each namespace must converge to
//! exactly the rows produced for it, and — the isolation check — one namespace's
//! SQL must see zero of another's rows even though they live in the same files.
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
async fn ten_tenants_converge_and_stay_isolated() {
    let stack = Stack::start().await;
    let table = stack.create_events_table_multi(10).await;
    let namespaces = table.namespaces.clone();
    let ingest = stack.spawn_ingest(&table).await;

    // Skewed sizes (2, 5, 8, ... rows) so several tenants are small and share
    // files with larger ones. Interleave rounds so a single flush window mixes
    // many tenants into the same L0 file.
    let counts: HashMap<NamespaceId, usize> = namespaces
        .iter()
        .enumerate()
        .map(|(i, &ns)| (ns, 2 + i * 3))
        .collect();
    let max_count = *counts.values().max().unwrap();

    let mut model: HashMap<NamespaceId, HashSet<String>> = HashMap::new();
    for round in 0..max_count {
        for &ns in &namespaces {
            if round < counts[&ns] {
                let payload = format!("ns{}-{round}", ns.0);
                let msg = serde_json::json!({
                    "tenant_id": ns.0, "ts": 1_000 + round as i64, "payload": payload,
                })
                .to_string();
                stack.produce_raw(&table, &msg).await;
                model.entry(ns).or_default().insert(payload);
            }
        }
    }

    // I1: every tenant converges to exactly its produced set.
    for &ns in &namespaces {
        let want = model[&ns].len() as i64;
        let got = stack
            .wait_for_row_count(ns, want, Duration::from_secs(30))
            .await;
        assert_eq!(got, want, "tenant {ns} must see all its rows");
    }
    ingest.stop().await;

    for &ns in &namespaces {
        assert_eq!(
            payloads(&stack, ns).await,
            model[&ns],
            "tenant {ns} result must equal exactly its produced rows"
        );
    }

    // I2: explicit cross-tenant isolation. Every pair is disjoint, and a
    // specific marker row from one tenant is never visible to another.
    let a = namespaces[0];
    let b = namespaces[9];
    let seen_by_a = payloads(&stack, a).await;
    let b_marker = format!("ns{}-0", b.0);
    assert!(model[&b].contains(&b_marker));
    assert!(
        !seen_by_a.contains(&b_marker),
        "tenant {a} must not see tenant {b}'s marker row {b_marker}"
    );
    assert!(
        seen_by_a.is_disjoint(&payloads(&stack, b).await),
        "tenants {a} and {b} must share no rows"
    );
}

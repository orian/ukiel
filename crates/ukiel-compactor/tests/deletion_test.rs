mod compactor_test;

use compactor_test::{add_l0, live_payloads, setup};
use serde_json::json;
use std::collections::HashSet;
use ukiel_compactor::deletion::delete_key;

#[tokio::test]
async fn delete_key_drops_dedicated_and_rewrites_packed() {
    let env = setup().await;
    // A packed file where key 7 shares with key 8, and a file wholly owned by 7.
    add_l0(
        &env,
        "d1",
        vec![
            json!({"tenant_id": 7, "ts": 1, "payload": "seven-a"}),
            json!({"tenant_id": 8, "ts": 2, "payload": "eight-a"}),
        ],
    )
    .await;
    add_l0(
        &env,
        "d1",
        vec![json!({"tenant_id": 7, "ts": 3, "payload": "seven-b"})],
    )
    .await;
    // An untouched neighbor that doesn't overlap key 7 at all.
    add_l0(
        &env,
        "d2",
        vec![json!({"tenant_id": 9, "ts": 4, "payload": "nine-a"})],
    )
    .await;

    let ht = env.catalog.get_hypertable("events").await.unwrap();
    let stats = delete_key(&env.catalog, &env.store, &ht, 7).await.unwrap();
    assert_eq!(
        stats.dropped_parts, 1,
        "the wholly-owned file is a metadata-only drop"
    );
    assert_eq!(
        stats.rewritten_parts, 1,
        "the shared file is rewritten without key 7"
    );
    assert_eq!(stats.rows_deleted, 2);

    // Key 7 is gone; neighbors are intact (invariant I7).
    let expected: HashSet<String> = ["eight-a", "nine-a"]
        .into_iter()
        .map(String::from)
        .collect();
    assert_eq!(live_payloads(&env).await, expected);
    for part in env.catalog.live_parts(env.ht, None).await.unwrap() {
        assert!(
            !(part.meta.packing_key_min <= 7 && part.meta.packing_key_max >= 7),
            "no live part may still claim key 7: {part:?}"
        );
    }

    // Idempotent: deleting again is a no-op.
    let stats = delete_key(&env.catalog, &env.store, &ht, 7).await.unwrap();
    assert_eq!(stats.dropped_parts + stats.rewritten_parts, 0);
}

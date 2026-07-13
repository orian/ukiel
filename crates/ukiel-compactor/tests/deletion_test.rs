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

/// Plan 16: a deletion rewrite rebuilds the key index from the *kept* rows, so
/// the deleted key falls out of the bitmap. Without this, the rewritten part's
/// min/max could still span the deleted key and every future query for it would
/// open the file to find nothing — the exact false positive the bitmap exists to
/// kill.
#[tokio::test]
async fn deletion_rewrite_drops_the_key_from_the_bitmap() {
    use ukiel_core::stats::{PACKING_KEYS_STAT, bitmap_contains};

    let env = setup().await;
    // One packed file spanning keys 3..90: deleting 7 leaves the range intact
    // (3..90 still "contains" 7), so only the bitmap can prove 7 is gone.
    add_l0(
        &env,
        "d1",
        vec![
            json!({"tenant_id": 3, "ts": 1, "payload": "three"}),
            json!({"tenant_id": 7, "ts": 2, "payload": "seven"}),
            json!({"tenant_id": 90, "ts": 3, "payload": "ninety"}),
        ],
    )
    .await;

    let ht = env.catalog.get_hypertable("events").await.unwrap();
    delete_key(&env.catalog, &env.store, &ht, 7).await.unwrap();

    let parts = env.catalog.live_parts(env.ht, None).await.unwrap();
    assert_eq!(parts.len(), 1, "the packed file was rewritten in place");
    let part = &parts[0];
    // The range still spans 7 — this is precisely why range pruning is not enough.
    assert!(part.meta.packing_key_min <= 7 && part.meta.packing_key_max >= 7);

    let stats = part.meta.column_stats.as_ref().expect("stats rebuilt");
    let encoded = stats[PACKING_KEYS_STAT].as_str().expect("bitmap rebuilt");
    assert_eq!(
        bitmap_contains(encoded, 7),
        Some(false),
        "the deleted key must be gone from the bitmap"
    );
    assert_eq!(bitmap_contains(encoded, 3), Some(true));
    assert_eq!(bitmap_contains(encoded, 90), Some(true));
}

// ---------------------------------------------------------------------------
// Plan 43: deletion operation identity.
// ---------------------------------------------------------------------------

use ukiel_catalog::OperationLookup;
use ukiel_compactor::deletion::DELETION_TRANSFORMATION_VERSION;
use ukiel_core::{CommitOp, CommitResult, DeleteKeyIntent, OperationIdentity, PartMeta};

/// The deletion identity is "erase this key from exactly these live parts".
///
/// Getting this wrong is worse than for a merge: a replayed *deletion* that is
/// not recognised would tombstone parts a peer has since rebuilt, so the retry
/// must reconcile rather than re-erase.
#[tokio::test]
async fn a_replayed_deletion_reconciles_instead_of_erasing_twice() {
    let env = compactor_test::setup().await;
    add_l0(
        &env,
        "d1",
        vec![
            json!({"tenant_id": 7, "ts": 1, "payload": "seven"}),
            json!({"tenant_id": 8, "ts": 2, "payload": "eight"}),
        ],
    )
    .await;
    let ht = env.catalog.get_hypertable("events").await.unwrap();
    let inputs = env.catalog.live_parts(env.ht, Some(7)).await.unwrap();
    let input_parts: Vec<_> = inputs.iter().map(|p| p.id).collect();

    let identity = |parts: &[ukiel_core::PartId], key: i64| {
        OperationIdentity::delete_key(
            env.ht,
            DeleteKeyIntent {
                packing_key: key,
                input_parts: parts,
                table_schema: &ht.table_schema,
                sort_key: &ht.sort_key,
                placement: ht.placement,
                transformation_version: DELETION_TRANSFORMATION_VERSION,
            },
        )
        .unwrap()
    };
    let erase_seven = identity(&input_parts, 7);

    // A changed target key, or a changed input set, is different intent.
    assert_ne!(erase_seven.key(), identity(&input_parts, 8).key());
    let mut wider = input_parts.clone();
    wider.push(ukiel_core::PartId(999_999));
    assert_ne!(erase_seven.key(), identity(&wider, 7).key());

    let stats = delete_key(&env.catalog, &env.store, &ht, 7).await.unwrap();
    assert_eq!(stats.rows_deleted, 1);
    assert_eq!(
        env.catalog.lookup_operation(&erase_seven).await.unwrap(),
        OperationLookup::Committed(
            match env.catalog.lookup_operation(&erase_seven).await.unwrap() {
                OperationLookup::Committed(id) => id,
                other => panic!("the deletion must be recorded under its identity: {other:?}"),
            }
        )
    );
    let after = live_payloads(&env).await;
    assert_eq!(after, HashSet::from(["eight".to_string()]));

    // The acknowledgement was lost: the same intent is submitted again, with a
    // different rewrite path (as any second attempt would have). It reconciles,
    // and tombstones nothing — the inputs it names are already tombstoned, so
    // without the identity this would have been a Conflict at best and a
    // double-erasure of rebuilt parts at worst.
    let replay = env
        .catalog
        .commit(
            env.ht,
            CommitOp::Replace {
                old: input_parts.clone(),
                new: vec![PartMeta {
                    path: "second-attempt.parquet".into(),
                    partition_values: json!({"day": "d1"}),
                    packing_key_min: 8,
                    packing_key_max: 8,
                    row_count: 1,
                    size_bytes: 1,
                    level: 0,
                    column_stats: None,
                }],
            },
            Some(&erase_seven),
        )
        .await
        .unwrap();
    assert!(
        matches!(replay, CommitResult::AlreadyApplied(_)),
        "{replay:?}"
    );
    assert_eq!(
        live_payloads(&env).await,
        after,
        "the replay changed nothing"
    );
}

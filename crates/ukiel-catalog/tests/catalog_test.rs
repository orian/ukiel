mod common;

use serde_json::json;
use ukiel_core::{
    CommitOp, CommitResult, HypertableId, IngestRange, NamespaceId, OperationIdentity, PartMeta,
};

fn part_meta(path: &str, packing_key_min: i64, packing_key_max: i64) -> PartMeta {
    PartMeta {
        path: path.to_string(),
        partition_values: json!({"day": "2026-07-05"}),
        packing_key_min,
        packing_key_max,
        row_count: 100,
        size_bytes: 4096,
        level: 0,
        column_stats: None,
    }
}

async fn setup_hypertable(catalog: &ukiel_catalog::PostgresCatalog) -> HypertableId {
    setup_hypertable_named(catalog, "events").await
}

async fn setup_hypertable_named(
    catalog: &ukiel_catalog::PostgresCatalog,
    name: &str,
) -> HypertableId {
    catalog
        .create_hypertable(
            name,
            &events_schema(),
            &json!({"columns": ["day"]}),
            &["tenant_id".to_string()],
            "tenant_id",
        )
        .await
        .unwrap()
}

fn events_schema() -> serde_json::Value {
    json!({"fields": [
        {"name": "tenant_id", "type": "int64"},
        {"name": "ts", "type": "timestamp_ms"},
        {"name": "payload", "type": "utf8"}
    ]})
}

#[tokio::test]
async fn connect_and_migrate() {
    let (_pg, _catalog) = common::setup().await;
    // setup() panics if connect or migrate fails; reaching here is the assertion.
}

#[tokio::test]
async fn hypertable_create_and_get() {
    let (_pg, catalog) = common::setup().await;
    let id = catalog
        .create_hypertable(
            "events",
            &events_schema(),
            &json!({"columns": ["day"]}),
            &["tenant_id".to_string(), "ts".to_string()],
            "tenant_id",
        )
        .await
        .unwrap();

    let ht = catalog.get_hypertable("events").await.unwrap();
    assert_eq!(ht.id, id);
    assert_eq!(ht.name, "events");
    assert_eq!(ht.sort_key, vec!["tenant_id", "ts"]);
    assert_eq!(ht.packing_key, "tenant_id");
    assert_eq!(ht.table_schema, events_schema());

    let err = catalog.get_hypertable("missing").await.unwrap_err();
    assert!(matches!(err, ukiel_catalog::CatalogError::NotFound(_)));
}

#[tokio::test]
async fn logical_table_create_and_get() {
    let (_pg, catalog) = common::setup().await;
    let ht = catalog
        .create_hypertable(
            "events",
            &events_schema(),
            &json!({"columns": []}),
            &["tenant_id".to_string()],
            "tenant_id",
        )
        .await
        .unwrap();

    let lt_id = catalog
        .create_logical_table(NamespaceId(42), "my_events", ht)
        .await
        .unwrap();

    let lt = catalog
        .get_logical_table(NamespaceId(42), "my_events")
        .await
        .unwrap();
    assert_eq!(lt.id, lt_id);
    assert_eq!(lt.namespace_id, NamespaceId(42));
    assert_eq!(lt.hypertable_id, ht);
    assert_eq!(lt.column_mapping, None);

    // Same table name in a different namespace is a different logical table.
    let err = catalog
        .get_logical_table(NamespaceId(7), "my_events")
        .await
        .unwrap_err();
    assert!(matches!(err, ukiel_catalog::CatalogError::NotFound(_)));
}

#[tokio::test]
async fn commit_add_makes_parts_live() {
    let (_pg, catalog) = common::setup().await;
    let ht = setup_hypertable(&catalog).await;

    let result = catalog
        .commit(
            ht,
            CommitOp::Add {
                parts: vec![
                    part_meta("p1.parquet", 1, 50),
                    part_meta("p2.parquet", 51, 99),
                ],
            },
            None,
        )
        .await
        .unwrap();
    assert!(matches!(result, CommitResult::Committed(_)));

    let parts = catalog.live_parts(ht, None).await.unwrap();
    assert_eq!(parts.len(), 2);
    assert_eq!(parts[0].meta.path, "p1.parquet");
    assert_eq!(parts[0].created_by_commit, result.commit_id());

    // Packing-key pruning: key 60 only overlaps p2.
    let parts = catalog.live_parts(ht, Some(60)).await.unwrap();
    assert_eq!(parts.len(), 1);
    assert_eq!(parts[0].meta.path, "p2.parquet");
}

use ukiel_catalog::CatalogError;

#[tokio::test]
async fn replace_swaps_parts_atomically() {
    let (_pg, catalog) = common::setup().await;
    let ht = setup_hypertable(&catalog).await;

    catalog
        .commit(
            ht,
            CommitOp::Add {
                parts: vec![
                    part_meta("l0-1.parquet", 1, 10),
                    part_meta("l0-2.parquet", 1, 10),
                ],
            },
            None,
        )
        .await
        .unwrap();
    let old_ids: Vec<_> = catalog
        .live_parts(ht, None)
        .await
        .unwrap()
        .iter()
        .map(|p| p.id)
        .collect();

    // Compaction: two L0 files -> one L1 file.
    let mut compacted = part_meta("l1-1.parquet", 1, 10);
    compacted.level = 1;
    catalog
        .commit(
            ht,
            CommitOp::Replace {
                old: old_ids.clone(),
                new: vec![compacted],
            },
            None,
        )
        .await
        .unwrap();

    let live = catalog.live_parts(ht, None).await.unwrap();
    assert_eq!(live.len(), 1);
    assert_eq!(live[0].meta.path, "l1-1.parquet");
    assert_eq!(live[0].meta.level, 1);

    // Replacing already-tombstoned parts conflicts.
    let err = catalog
        .commit(
            ht,
            CommitOp::Replace {
                old: old_ids,
                new: vec![part_meta("x.parquet", 1, 10)],
            },
            None,
        )
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        CatalogError::Conflict {
            expected: 2,
            live_matched: 0
        }
    ));

    // The failed commit must not have added parts.
    assert_eq!(catalog.live_parts(ht, None).await.unwrap().len(), 1);
}

#[tokio::test]
async fn delete_tombstones_parts() {
    let (_pg, catalog) = common::setup().await;
    let ht = setup_hypertable(&catalog).await;

    catalog
        .commit(
            ht,
            CommitOp::Add {
                parts: vec![part_meta("p1.parquet", 1, 10)],
            },
            None,
        )
        .await
        .unwrap();
    let ids: Vec<_> = catalog
        .live_parts(ht, None)
        .await
        .unwrap()
        .iter()
        .map(|p| p.id)
        .collect();

    catalog
        .commit(ht, CommitOp::Delete { parts: ids }, None)
        .await
        .unwrap();
    assert!(catalog.live_parts(ht, None).await.unwrap().is_empty());
}

#[tokio::test]
async fn concurrent_replace_exactly_one_wins() {
    let (_pg, catalog) = common::setup().await;
    let ht = setup_hypertable(&catalog).await;

    catalog
        .commit(
            ht,
            CommitOp::Add {
                parts: vec![part_meta("l0.parquet", 1, 10)],
            },
            None,
        )
        .await
        .unwrap();
    let old_ids: Vec<_> = catalog
        .live_parts(ht, None)
        .await
        .unwrap()
        .iter()
        .map(|p| p.id)
        .collect();

    let (c1, c2) = (catalog.clone(), catalog.clone());
    let (o1, o2) = (old_ids.clone(), old_ids.clone());
    let t1 = tokio::spawn(async move {
        c1.commit(
            ht,
            CommitOp::Replace {
                old: o1,
                new: vec![part_meta("winner-a.parquet", 1, 10)],
            },
            None,
        )
        .await
    });
    let t2 = tokio::spawn(async move {
        c2.commit(
            ht,
            CommitOp::Replace {
                old: o2,
                new: vec![part_meta("winner-b.parquet", 1, 10)],
            },
            None,
        )
        .await
    });

    let r1 = t1.await.unwrap();
    let r2 = t2.await.unwrap();

    let ok_count = [&r1, &r2].iter().filter(|r| r.is_ok()).count();
    let conflict_count = [&r1, &r2]
        .iter()
        .filter(|r| matches!(r, Err(CatalogError::Conflict { .. })))
        .count();
    assert_eq!(
        ok_count, 1,
        "exactly one replace must win, got {r1:?} / {r2:?}"
    );
    assert_eq!(conflict_count, 1);

    // Exactly the winner's part is live.
    let live = catalog.live_parts(ht, None).await.unwrap();
    assert_eq!(live.len(), 1);
    assert!(live[0].meta.path.starts_with("winner-"));
}

#[tokio::test]
async fn idempotency_key_dedups_commit() {
    let (_pg, catalog) = common::setup().await;
    let ht = setup_hypertable(&catalog).await;

    let key = op(ht, 0, 0, 99); // kafka partition 0, offsets 0..=99
    let first = catalog
        .commit(
            ht,
            CommitOp::Add {
                parts: vec![part_meta("p1.parquet", 1, 10)],
            },
            Some(&key),
        )
        .await
        .unwrap();
    let CommitResult::Committed(first_id) = first else {
        panic!("first commit must be Committed, got {first:?}");
    };

    // Ingest worker crashed after commit, re-flushed the same offset range.
    let second = catalog
        .commit(
            ht,
            CommitOp::Add {
                parts: vec![part_meta("p1-retry.parquet", 1, 10)],
            },
            Some(&key),
        )
        .await
        .unwrap();
    assert_eq!(second, CommitResult::AlreadyApplied(first_id));

    // The retry's parts were NOT inserted.
    let live = catalog.live_parts(ht, None).await.unwrap();
    assert_eq!(live.len(), 1);
    assert_eq!(live[0].meta.path, "p1.parquet");

    // A different key commits normally.
    let third = catalog
        .commit(
            ht,
            CommitOp::Add {
                parts: vec![part_meta("p2.parquet", 1, 10)],
            },
            Some(&op(ht, 0, 100, 199)),
        )
        .await
        .unwrap();
    assert!(matches!(third, CommitResult::Committed(_)));
    assert_eq!(catalog.live_parts(ht, None).await.unwrap().len(), 2);
}

use ukiel_core::CommitId;

#[tokio::test]
async fn change_feed_replays_commits_in_order() {
    let (_pg, catalog) = common::setup().await;
    let ht = setup_hypertable(&catalog).await;

    catalog
        .commit(
            ht,
            CommitOp::Add {
                parts: vec![part_meta("a.parquet", 1, 10)],
            },
            None,
        )
        .await
        .unwrap();
    let old_ids: Vec<_> = catalog
        .live_parts(ht, None)
        .await
        .unwrap()
        .iter()
        .map(|p| p.id)
        .collect();
    catalog
        .commit(
            ht,
            CommitOp::Replace {
                old: old_ids.clone(),
                new: vec![part_meta("b.parquet", 1, 10)],
            },
            None,
        )
        .await
        .unwrap();

    let events = catalog.changes_since(ht, CommitId(0), 100).await.unwrap();
    assert_eq!(events.len(), 2);

    assert_eq!(events[0].kind, "add");
    assert_eq!(events[0].added.len(), 1);
    assert_eq!(events[0].added[0].meta.path, "a.parquet");
    assert!(events[0].removed.is_empty());

    assert_eq!(events[1].kind, "replace");
    assert_eq!(events[1].added[0].meta.path, "b.parquet");
    assert_eq!(events[1].removed, old_ids);
    assert!(events[0].commit_id < events[1].commit_id);

    // Cursor semantics: resuming after event 0 yields only event 1.
    let tail = catalog
        .changes_since(ht, events[0].commit_id, 100)
        .await
        .unwrap();
    assert_eq!(tail.len(), 1);
    assert_eq!(tail[0].commit_id, events[1].commit_id);

    // Limit is respected.
    let page = catalog.changes_since(ht, CommitId(0), 1).await.unwrap();
    assert_eq!(page.len(), 1);
}

use ukiel_catalog::OffsetRange;

#[tokio::test]
async fn duplicate_offset_commit_is_rejected_and_rolled_back() {
    let (_pg, catalog) = common::setup().await;
    let ht = setup_hypertable(&catalog).await;
    let range = |first, last| OffsetRange {
        topic: "events".to_string(),
        partition: 0,
        first,
        last,
    };
    let commit_add =
        |catalog: ukiel_catalog::PostgresCatalog, path: &'static str, r: OffsetRange| async move {
            let ranges = [r];
            let identity = offsets_op(ht, &ranges);
            catalog
                .commit_with_offsets(
                    ht,
                    CommitOp::Add {
                        parts: vec![part_meta(path, 1, 10)],
                    },
                    &ranges,
                    &identity,
                )
                .await
        };

    // Writer A commits rows for offsets 0..=9.
    commit_add(catalog.clone(), "p-a.parquet", range(0, 9))
        .await
        .unwrap();
    let live_before = catalog.live_parts(ht, None).await.unwrap().len();

    // Another attempt at *exactly* those offsets — a retry whose acknowledgement
    // was lost, or a duplicate consumer that read the same records — is the same
    // logical operation: the same Kafka offsets under the same transformation
    // produce the same rows. Plan 43 reconciles it instead of failing it, which
    // is what stops an honest retry from being told it is a duplicate worker.
    // Issue 0003's guarantee is unchanged and is what the assertion below pins:
    // nothing is duplicated.
    let replay = commit_add(catalog.clone(), "p-b.parquet", range(0, 9))
        .await
        .unwrap();
    assert!(
        matches!(replay, CommitResult::AlreadyApplied(_)),
        "{replay:?}"
    );
    assert_eq!(
        catalog.live_parts(ht, None).await.unwrap().len(),
        live_before,
        "the replay applied nothing"
    );

    // A *genuinely different* operation that overlaps an already-consumed range
    // is the duplicate-consumer case issue 0003 exists for, and it is still a
    // loud OffsetRace: idempotency answers duplicate intent, it does not replace
    // the offset CAS.
    let err = commit_add(catalog.clone(), "p-overlap.parquet", range(5, 14))
        .await
        .unwrap_err();
    assert!(
        matches!(err, CatalogError::OffsetRace { ref topic, partition: 0 } if topic == "events"),
        "{err:?}"
    );

    // The whole transaction rolled back: no part appeared.
    assert_eq!(
        catalog.live_parts(ht, None).await.unwrap().len(),
        live_before
    );

    // The real writer continues from 10 — sequential progress still works.
    commit_add(catalog.clone(), "p-c.parquet", range(10, 19))
        .await
        .unwrap();

    // Offset gaps stay legal (compacted topics): next stored is 20, a range
    // starting at 25 commits fine.
    commit_add(catalog.clone(), "p-d.parquet", range(25, 30))
        .await
        .unwrap();
}

#[tokio::test]
async fn commit_with_offsets_is_atomic() {
    let (_pg, catalog) = common::setup().await;
    let ht = setup_hypertable(&catalog).await;

    // Fresh hypertable: no offsets stored.
    assert!(
        catalog
            .ingest_offsets(ht, "events")
            .await
            .unwrap()
            .is_empty()
    );

    // Successful commit advances offsets.
    let ranges = [
        OffsetRange {
            topic: "events".into(),
            partition: 0,
            first: 0,
            last: 41,
        },
        OffsetRange {
            topic: "events".into(),
            partition: 1,
            first: 0,
            last: 9,
        },
    ];
    catalog
        .commit_with_offsets(
            ht,
            CommitOp::Add {
                parts: vec![part_meta("p1.parquet", 1, 10)],
            },
            &ranges,
            &offsets_op(ht, &ranges),
        )
        .await
        .unwrap();

    let offsets = catalog.ingest_offsets(ht, "events").await.unwrap();
    assert_eq!(offsets.get(&0), Some(&42));
    assert_eq!(offsets.get(&1), Some(&10));

    // A conflicting commit must NOT advance offsets (same transaction).
    let bogus_old = vec![ukiel_core::PartId(999_999)];
    let next = [OffsetRange {
        topic: "events".into(),
        partition: 0,
        first: 42,
        last: 99,
    }];
    let err = catalog
        .commit_with_offsets(
            ht,
            CommitOp::Replace {
                old: bogus_old,
                new: vec![part_meta("p2.parquet", 1, 10)],
            },
            &next,
            &offsets_op(ht, &next),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, CatalogError::Conflict { .. }));

    let offsets = catalog.ingest_offsets(ht, "events").await.unwrap();
    assert_eq!(
        offsets.get(&0),
        Some(&42),
        "failed commit must not move offsets"
    );

    // A later flush upserts over the existing row. It is a *different* operation
    // from the conflicting one above even though it consumes the same offsets —
    // the failed one added no parts, so its intent never landed and its identity
    // is free to be reused by the attempt that does the work.
    catalog
        .commit_with_offsets(
            ht,
            CommitOp::Add {
                parts: vec![part_meta("p3.parquet", 1, 10)],
            },
            &next,
            &offsets_op(ht, &next),
        )
        .await
        .unwrap();
    let offsets = catalog.ingest_offsets(ht, "events").await.unwrap();
    assert_eq!(offsets.get(&0), Some(&100));
}

#[tokio::test]
async fn hypertable_by_id_and_namespace_table_listing() {
    let (_pg, catalog) = common::setup().await;
    let ht = setup_hypertable(&catalog).await;

    let fetched = catalog.get_hypertable_by_id(ht).await.unwrap();
    assert_eq!(fetched.id, ht);
    assert_eq!(fetched.name, "events");

    let err = catalog
        .get_hypertable_by_id(ukiel_core::HypertableId(999_999))
        .await
        .unwrap_err();
    assert!(matches!(err, CatalogError::NotFound(_)));

    catalog
        .create_logical_table(NamespaceId(1), "events", ht)
        .await
        .unwrap();
    catalog
        .create_logical_table(NamespaceId(1), "clicks", ht)
        .await
        .unwrap();
    catalog
        .create_logical_table(NamespaceId(2), "events", ht)
        .await
        .unwrap();

    let tables = catalog.list_logical_tables(NamespaceId(1)).await.unwrap();
    let names: Vec<_> = tables.iter().map(|t| t.name.as_str()).collect();
    assert_eq!(names, vec!["clicks", "events"]);
    assert!(
        catalog
            .list_logical_tables(NamespaceId(99))
            .await
            .unwrap()
            .is_empty()
    );
}

use ukiel_core::Placement;

#[tokio::test]
async fn placement_cursors_and_listing() {
    let (_pg, catalog) = common::setup().await;
    let ht = setup_hypertable(&catalog).await;

    // Placement defaults to packed and is settable.
    assert_eq!(
        catalog.get_hypertable("events").await.unwrap().placement,
        Placement::Packed
    );
    catalog
        .set_placement(ht, Placement::Separated)
        .await
        .unwrap();
    assert_eq!(
        catalog.get_hypertable("events").await.unwrap().placement,
        Placement::Separated
    );

    // Listing returns every hypertable.
    let all = catalog.list_hypertables().await.unwrap();
    assert_eq!(all.len(), 1);
    assert_eq!(all[0].id, ht);

    // Cursors: 0 when absent, then upsert.
    assert_eq!(
        catalog.get_cursor("compactor", ht).await.unwrap(),
        CommitId(0)
    );
    catalog
        .set_cursor("compactor", ht, CommitId(7))
        .await
        .unwrap();
    catalog
        .set_cursor("compactor", ht, CommitId(9))
        .await
        .unwrap();
    assert_eq!(
        catalog.get_cursor("compactor", ht).await.unwrap(),
        CommitId(9)
    );
    // Cursors are per worker name.
    assert_eq!(catalog.get_cursor("mv", ht).await.unwrap(), CommitId(0));
}

#[tokio::test]
async fn reapable_parts_respect_grace_and_cursors() {
    let (_pg, catalog) = common::setup().await;
    let ht = setup_hypertable(&catalog).await;

    catalog
        .commit(
            ht,
            CommitOp::Add {
                parts: vec![part_meta("old.parquet", 1, 10)],
            },
            None,
        )
        .await
        .unwrap();
    let old_ids: Vec<_> = catalog
        .live_parts(ht, None)
        .await
        .unwrap()
        .iter()
        .map(|p| p.id)
        .collect();
    let replace = catalog
        .commit(
            ht,
            CommitOp::Replace {
                old: old_ids.clone(),
                new: vec![part_meta("new.parquet", 1, 10)],
            },
            None,
        )
        .await
        .unwrap();
    let CommitResult::Committed(replace_id) = replace else {
        panic!("expected commit")
    };

    // Grace not elapsed -> nothing reapable.
    assert!(catalog.reapable_parts(ht, 3600.0).await.unwrap().is_empty());

    // Grace elapsed (0s), no cursors registered -> the tombstoned part is reapable.
    let reapable = catalog.reapable_parts(ht, 0.0).await.unwrap();
    assert_eq!(reapable.len(), 1);
    assert_eq!(reapable[0].meta.path, "old.parquet");

    // A lagging consumer fences reaping: cursor before the replace commit.
    catalog
        .set_cursor("mv", ht, CommitId(replace_id.0 - 1))
        .await
        .unwrap();
    assert!(catalog.reapable_parts(ht, 0.0).await.unwrap().is_empty());

    // Consumer catches up -> reapable again.
    catalog.set_cursor("mv", ht, replace_id).await.unwrap();
    let reapable = catalog.reapable_parts(ht, 0.0).await.unwrap();
    assert_eq!(reapable.len(), 1);

    // Purging stamps the row: no longer reapable. Its path drops out of
    // all_part_paths (the object is already deleted), but the catalog row and
    // the change feed still replay the original add.
    catalog.mark_purged(&[reapable[0].id]).await.unwrap();
    assert!(catalog.reapable_parts(ht, 0.0).await.unwrap().is_empty());
    let paths = catalog.all_part_paths(ht).await.unwrap();
    assert!(
        !paths.contains(&"old.parquet".to_string()),
        "purged path is excluded from the known set"
    );
    assert!(paths.contains(&"new.parquet".to_string()));
    let events = catalog.changes_since(ht, CommitId(0), 100).await.unwrap();
    assert_eq!(
        events[0].added.len(),
        1,
        "purged part must still appear in feed replay"
    );
}

#[tokio::test]
async fn pending_objects_track_orphans_and_clear_on_commit() {
    let (_pg, catalog) = common::setup().await;
    let ht = setup_hypertable(&catalog).await;

    // A writer records intent for two objects it is about to upload.
    catalog
        .register_pending_objects(ht, &["a.parquet".to_string(), "b.parquet".to_string()])
        .await
        .unwrap();

    // Both are orphan candidates at grace 0 (rows are in the past), none at a
    // large grace (protects in-flight commits).
    let orphans = catalog.orphaned_pending_objects(ht, 0.0).await.unwrap();
    assert_eq!(
        orphans,
        vec!["a.parquet".to_string(), "b.parquet".to_string()]
    );
    assert!(
        catalog
            .orphaned_pending_objects(ht, 3600.0)
            .await
            .unwrap()
            .is_empty()
    );

    // Re-registering the same path is a no-op (idempotent retry).
    catalog
        .register_pending_objects(ht, &["a.parquet".to_string()])
        .await
        .unwrap();
    assert_eq!(
        catalog
            .orphaned_pending_objects(ht, 0.0)
            .await
            .unwrap()
            .len(),
        2
    );

    // Committing a part with path "a.parquet" clears its intent row.
    catalog
        .commit(
            ht,
            CommitOp::Add {
                parts: vec![part_meta("a.parquet", 1, 10)],
            },
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        catalog.orphaned_pending_objects(ht, 0.0).await.unwrap(),
        vec!["b.parquet".to_string()]
    );

    // The sweeper clears the row after deleting the object.
    catalog
        .clear_pending_objects(&["b.parquet".to_string()])
        .await
        .unwrap();
    assert!(
        catalog
            .orphaned_pending_objects(ht, 0.0)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn placement_round_trips_all_variants() {
    let (_pg, catalog) = common::setup().await;
    let ht = setup_hypertable(&catalog).await;

    for placement in [
        ukiel_core::Placement::SizeTargeted(268_435_456),
        ukiel_core::Placement::Separated,
        ukiel_core::Placement::Packed,
    ] {
        catalog.set_placement(ht, placement).await.unwrap();
        let hypertable = catalog.get_hypertable_by_id(ht).await.unwrap();
        assert_eq!(hypertable.placement, placement);
    }
}

/// One `CommitOp::Add` of a single L0 part in the given partition.
async fn add_l0_part(
    catalog: &ukiel_catalog::PostgresCatalog,
    ht: HypertableId,
    partition: &serde_json::Value,
) {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let n = SEQ.fetch_add(1, Ordering::Relaxed);
    catalog
        .commit(
            ht,
            CommitOp::Add {
                parts: vec![PartMeta {
                    path: format!("l0-{n}.parquet"),
                    partition_values: partition.clone(),
                    packing_key_min: 1,
                    packing_key_max: 1,
                    row_count: 1,
                    size_bytes: 1,
                    level: 0,
                    column_stats: None,
                }],
            },
            None,
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn partition_l0_quiet_since_tracks_l0_commits() {
    let (_pg, catalog) = common::setup().await;
    let ht = setup_hypertable(&catalog).await;
    let partition = json!({ "day": "2026-07-01" });

    // No L0 parts ever: quiet at any window.
    assert!(
        catalog
            .partition_l0_quiet_since(ht, &partition, 3600.0)
            .await
            .unwrap()
    );

    add_l0_part(&catalog, ht, &partition).await;

    // Fresh L0: not quiet for a 1h window, quiet for a 0s window.
    assert!(
        !catalog
            .partition_l0_quiet_since(ht, &partition, 3600.0)
            .await
            .unwrap()
    );
    assert!(
        catalog
            .partition_l0_quiet_since(ht, &partition, 0.0)
            .await
            .unwrap()
    );

    // A different partition is unaffected.
    assert!(
        catalog
            .partition_l0_quiet_since(ht, &json!({ "day": "2026-07-02" }), 3600.0)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn capacity_probes_count_l0_runs_and_candidates() {
    let (_pg, catalog) = common::setup().await;
    let ht = setup_hypertable(&catalog).await;
    let day1 = json!({ "day": "2026-07-01" });
    let day2 = json!({ "day": "2026-07-02" });

    // Empty: no pressure, no candidates.
    assert_eq!(catalog.max_live_l0_parts(ht, &[]).await.unwrap(), 0);
    assert_eq!(
        catalog
            .max_live_l0_parts(ht, std::slice::from_ref(&day1))
            .await
            .unwrap(),
        0
    );
    assert!(
        catalog
            .finalize_candidates(ht, Uuid::nil(), 0.0, 64, None)
            .await
            .unwrap()
            .partitions
            .is_empty()
    );

    // Two L0 commits on day1, one on day2.
    add_l0_part(&catalog, ht, &day1).await;
    add_l0_part(&catalog, ht, &day1).await;
    add_l0_part(&catalog, ht, &day2).await;

    // Grouped probe: max live-L0 count across the probed partitions.
    assert_eq!(
        catalog
            .max_live_l0_parts(ht, std::slice::from_ref(&day1))
            .await
            .unwrap(),
        2
    );
    assert_eq!(
        catalog
            .max_live_l0_parts(ht, std::slice::from_ref(&day2))
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        catalog
            .max_live_l0_parts(ht, &[day1.clone(), day2.clone()])
            .await
            .unwrap(),
        2
    );

    // Only day1 has >= 2 live runs.
    let candidates = catalog
        .finalize_candidates(ht, Uuid::nil(), 0.0, 64, None)
        .await
        .unwrap();
    assert_eq!(candidates.partitions, vec![day1.clone()]);

    // live_partition_parts scopes to one partition.
    let parts = catalog.live_partition_parts(ht, &day1).await.unwrap();
    assert_eq!(parts.len(), 2);
    assert!(parts.iter().all(|p| p.meta.partition_values == day1));

    // Tombstoned parts stop counting everywhere: REPLACE day1's two parts
    // into one L1 part, then recheck.
    let new = PartMeta {
        path: "day1-merged.parquet".to_string(),
        partition_values: day1.clone(),
        packing_key_min: 1,
        packing_key_max: 1,
        row_count: 2,
        size_bytes: 2,
        level: 1,
        column_stats: None,
    };
    catalog
        .commit(
            ht,
            CommitOp::Replace {
                old: parts.iter().map(|p| p.id).collect(),
                new: vec![new],
            },
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        catalog
            .max_live_l0_parts(ht, std::slice::from_ref(&day1))
            .await
            .unwrap(),
        0
    );
    // day1 now has a single run (the L1 merge); day2 still has one L0 run.
    assert!(
        catalog
            .finalize_candidates(ht, Uuid::nil(), 0.0, 64, None)
            .await
            .unwrap()
            .partitions
            .is_empty()
    );
}

use ukiel_core::ColumnRange;

fn part_meta_with_ts(path: &str, ts_min: i64, ts_max: i64) -> PartMeta {
    let mut meta = part_meta(path, 1, 10);
    meta.column_stats = Some(json!({"ts": {"min": ts_min, "max": ts_max}}));
    meta
}

#[tokio::test]
async fn live_parts_pruned_by_column_stats() {
    let (_pg, catalog) = common::setup().await;
    let ht = setup_hypertable(&catalog).await;
    catalog
        .commit(
            ht,
            CommitOp::Add {
                parts: vec![
                    part_meta_with_ts("early.parquet", 0, 100),
                    part_meta_with_ts("late.parquet", 1000, 2000),
                    part_meta("no-stats.parquet", 1, 10), // must always survive
                ],
            },
            None,
        )
        .await
        .unwrap();

    let range = |min, max| ColumnRange {
        column: "ts".to_string(),
        min,
        max,
    };

    // ts <= 150: excludes 'late'.
    let parts = catalog
        .live_parts_pruned(ht, None, &[range(None, Some(150))])
        .await
        .unwrap();
    let paths: Vec<_> = parts.iter().map(|p| p.meta.path.as_str()).collect();
    assert_eq!(paths, vec!["early.parquet", "no-stats.parquet"]);

    // ts >= 500: excludes 'early'.
    let parts = catalog
        .live_parts_pruned(ht, None, &[range(Some(500), None)])
        .await
        .unwrap();
    let paths: Vec<_> = parts.iter().map(|p| p.meta.path.as_str()).collect();
    assert_eq!(paths, vec!["late.parquet", "no-stats.parquet"]);

    // 200..=500: only the stats-less part survives.
    let parts = catalog
        .live_parts_pruned(ht, None, &[range(Some(200), Some(500))])
        .await
        .unwrap();
    assert_eq!(parts.len(), 1);
    assert_eq!(parts[0].meta.path, "no-stats.parquet");

    // A range on a column with no stats anywhere prunes nothing.
    let parts = catalog
        .live_parts_pruned(
            ht,
            None,
            &[ColumnRange {
                column: "nope".to_string(),
                min: Some(1),
                max: Some(2),
            }],
        )
        .await
        .unwrap();
    assert_eq!(parts.len(), 3);

    // Empty ranges == live_parts.
    assert_eq!(
        catalog
            .live_parts_pruned(ht, None, &[])
            .await
            .unwrap()
            .len(),
        3
    );
}

#[tokio::test]
async fn collector_probes_report_live_counts_lag_and_backlogs() {
    let (_pg, catalog) = common::setup().await;
    let ht = setup_hypertable(&catalog).await;
    let day1 = json!({ "day": "2026-07-01" });
    let day2 = json!({ "day": "2026-07-02" });

    // day1: two L0 commits (2 runs) + one L1 commit; day2: one L0 commit.
    add_l0_part(&catalog, ht, &day1).await;
    add_l0_part(&catalog, ht, &day1).await;
    add_l0_part(&catalog, ht, &day2).await;
    let l1 = PartMeta {
        path: "day1-l1.parquet".to_string(),
        partition_values: day1.clone(),
        packing_key_min: 1,
        packing_key_max: 1,
        row_count: 1,
        size_bytes: 1,
        level: 1,
        column_stats: None,
    };
    catalog
        .commit(ht, CommitOp::Add { parts: vec![l1] }, None)
        .await
        .unwrap();

    // live_part_counts: 3 L0 + 1 L1 for this hypertable.
    let mut counts = catalog.live_part_counts().await.unwrap();
    counts.retain(|(h, _, _)| *h == ht);
    counts.sort_by_key(|(_, level, _)| *level);
    assert_eq!(counts, vec![(ht, 0, 3), (ht, 1, 1)]);

    // backlog_groups(2, 10): only day1's level-0 group (2 runs >= 2) qualifies.
    assert_eq!(catalog.backlog_groups(2, 10).await.unwrap(), 1);

    // unfinalized: day1 has 3 distinct commits (>1 run) and is quiet at 0s.
    let (unfin, oldest) = catalog.unfinalized_partitions(0.0).await.unwrap();
    assert_eq!(unfin, 1);
    assert!(oldest >= 0.0);
    // A huge quiet window means nothing is quiet yet.
    assert_eq!(catalog.unfinalized_partitions(3600.0).await.unwrap().0, 0);

    // feed_lags: a lagging worker sees head-0; an ahead worker floors at 0.
    catalog
        .set_cursor("lagging", ht, CommitId(0))
        .await
        .unwrap();
    catalog
        .set_cursor("ahead", ht, CommitId(1_000_000))
        .await
        .unwrap();
    let lags = catalog.feed_lags().await.unwrap();
    let lagging = lags.iter().find(|(w, _, _)| w == "lagging").unwrap();
    assert!(lagging.2 > 0, "lagging worker has positive lag");
    let ahead = lags.iter().find(|(w, _, _)| w == "ahead").unwrap();
    assert_eq!(ahead.2, 0, "greatest() floors lag at 0");

    // gc_backlogs: one pending object; one tombstone after deleting the L1.
    catalog
        .register_pending_objects(ht, &["orphan.parquet".to_string()])
        .await
        .unwrap();
    let live = catalog.live_partition_parts(ht, &day1).await.unwrap();
    let l1_id = live.iter().find(|p| p.meta.level == 1).unwrap().id;
    catalog
        .commit(ht, CommitOp::Delete { parts: vec![l1_id] }, None)
        .await
        .unwrap();
    let gc = catalog.gc_backlogs().await.unwrap();
    assert_eq!(gc.pending, 1);
    assert!(gc.pending_oldest_secs >= 0.0);
    assert_eq!(gc.unpurged_tombstones, 1);
    assert!(gc.oldest_tombstone_secs >= 0.0);

    // pool_stats: at least one connection exists; idle never exceeds size.
    let (size, idle) = catalog.pool_stats();
    assert!(size >= 1);
    assert!(idle <= size as usize);
}

#[tokio::test]
async fn create_hypertable_rejects_unsound_sort_key() {
    let (_pg, catalog) = common::setup().await;
    // Packing key must lead the sort key (plan 27): ts-first is rejected.
    let err = catalog
        .create_hypertable(
            "bad",
            &events_schema(),
            &json!({"columns": ["day"]}),
            &["ts".to_string(), "tenant_id".to_string()],
            "tenant_id",
        )
        .await
        .unwrap_err();
    assert!(matches!(err, CatalogError::SortKey(_)), "got {err:?}");

    // A sort column absent from the schema is rejected too.
    let err = catalog
        .create_hypertable(
            "bad2",
            &events_schema(),
            &json!({"columns": ["day"]}),
            &["tenant_id".to_string(), "nope".to_string()],
            "tenant_id",
        )
        .await
        .unwrap_err();
    assert!(matches!(err, CatalogError::SortKey(_)), "got {err:?}");

    // The valid shape still works.
    catalog
        .create_hypertable(
            "good",
            &events_schema(),
            &json!({"columns": ["day"]}),
            &["tenant_id".to_string(), "ts".to_string()],
            "tenant_id",
        )
        .await
        .unwrap();
}

/// `updated_at` of a cursor row — the progress timestamp a stale writer must
/// not be able to refresh.
async fn cursor_updated_at(
    catalog: &ukiel_catalog::PostgresCatalog,
    worker: &str,
    ht: HypertableId,
) -> chrono::DateTime<chrono::Utc> {
    sqlx::query_scalar(
        "SELECT updated_at FROM worker_cursors WHERE worker = $1 AND hypertable_id = $2",
    )
    .bind(worker)
    .bind(ht.0)
    .fetch_one(catalog.pool_for_tests())
    .await
    .unwrap()
}

#[tokio::test]
async fn cursors_never_retreat() {
    let (_pg, catalog) = common::setup().await;
    let ht = setup_hypertable(&catalog).await;

    catalog.set_cursor("c", ht, CommitId(7)).await.unwrap();
    catalog.set_cursor("c", ht, CommitId(9)).await.unwrap();
    // A delayed replica replays an older position: a successful no-op.
    catalog.set_cursor("c", ht, CommitId(8)).await.unwrap();
    assert_eq!(catalog.get_cursor("c", ht).await.unwrap(), CommitId(9));

    // A stale (or equal) write must not refresh the progress timestamp:
    // otherwise a stuck worker looks alive to feed-lag alerting.
    let stamped = cursor_updated_at(&catalog, "c", ht).await;
    catalog.set_cursor("c", ht, CommitId(3)).await.unwrap();
    catalog.set_cursor("c", ht, CommitId(9)).await.unwrap();
    assert_eq!(cursor_updated_at(&catalog, "c", ht).await, stamped);

    // A real advance does refresh it.
    catalog.set_cursor("c", ht, CommitId(10)).await.unwrap();
    assert!(cursor_updated_at(&catalog, "c", ht).await > stamped);
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_cursor_writers_leave_the_maximum() {
    let (_pg, catalog) = common::setup().await;
    let ht = setup_hypertable(&catalog).await;

    // Three workers race on one shared cursor name (the compactor fleet).
    let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(3));
    let mut handles = Vec::new();
    for pos in [7i64, 11, 9] {
        let (catalog, barrier) = (catalog.clone(), barrier.clone());
        handles.push(tokio::spawn(async move {
            barrier.wait().await;
            catalog
                .set_cursor("fleet", ht, CommitId(pos))
                .await
                .unwrap();
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
    assert_eq!(catalog.get_cursor("fleet", ht).await.unwrap(), CommitId(11));
}

#[tokio::test]
async fn gc_fence_survives_a_stale_cursor_write() {
    let (_pg, catalog) = common::setup().await;
    let ht = setup_hypertable(&catalog).await;

    catalog
        .commit(
            ht,
            CommitOp::Add {
                parts: vec![part_meta("old.parquet", 1, 10)],
            },
            None,
        )
        .await
        .unwrap();
    let old: Vec<_> = catalog
        .live_parts(ht, None)
        .await
        .unwrap()
        .iter()
        .map(|p| p.id)
        .collect();
    let CommitResult::Committed(replace_id) = catalog
        .commit(
            ht,
            CommitOp::Replace {
                old,
                new: vec![part_meta("new.parquet", 1, 10)],
            },
            None,
        )
        .await
        .unwrap()
    else {
        panic!("expected commit")
    };

    // The consumer passes the replace, so its tombstone is reapable...
    catalog.set_cursor("mv", ht, replace_id).await.unwrap();
    assert_eq!(catalog.reapable_parts(ht, 0.0).await.unwrap().len(), 1);

    // ... and a delayed replica of that same consumer replaying an old
    // position cannot re-fence it (monotonicity is what makes the GC fence
    // a one-way gate).
    catalog.set_cursor("mv", ht, CommitId(0)).await.unwrap();
    assert_eq!(catalog.reapable_parts(ht, 0.0).await.unwrap().len(), 1);
}

#[tokio::test]
async fn compaction_candidates_come_from_live_state() {
    let (_pg, catalog) = common::setup().await;
    let ht = setup_hypertable(&catalog).await;
    let day1 = json!({ "day": "2026-07-01" });
    let day2 = json!({ "day": "2026-07-02" });
    let day3 = json!({ "day": "2026-07-03" });

    // day1: two L0 runs (over an l0_fanout of 2). day2: one L0 run (under).
    add_l0_part(&catalog, ht, &day1).await;
    add_l0_part(&catalog, ht, &day1).await;
    add_l0_part(&catalog, ht, &day2).await;

    let candidates = catalog
        .compaction_candidates(ht, Uuid::nil(), 2, 2, 64, None)
        .await
        .unwrap();
    assert_eq!(candidates.partitions, vec![day1.clone()]);

    // The trigger is per level: raising l0_fanout drops day1 out.
    assert!(
        catalog
            .compaction_candidates(ht, Uuid::nil(), 3, 2, 64, None)
            .await
            .unwrap()
            .partitions
            .is_empty()
    );

    // Tombstoned parts do not count: merging day1's two L0s into one L1 run
    // leaves nothing above the trigger.
    let l0: Vec<_> = catalog
        .live_partition_parts(ht, &day1)
        .await
        .unwrap()
        .iter()
        .map(|p| p.id)
        .collect();
    catalog
        .commit(
            ht,
            CommitOp::Replace {
                old: l0,
                new: vec![PartMeta {
                    path: "l1-a.parquet".into(),
                    partition_values: day1.clone(),
                    packing_key_min: 1,
                    packing_key_max: 1,
                    row_count: 2,
                    size_bytes: 2,
                    level: 1,
                    column_stats: None,
                }],
            },
            None,
        )
        .await
        .unwrap();
    assert!(
        catalog
            .compaction_candidates(ht, Uuid::nil(), 2, 2, 64, None)
            .await
            .unwrap()
            .partitions
            .is_empty()
    );

    // A partition qualifying at two levels at once is returned exactly once.
    add_l0_part(&catalog, ht, &day1).await;
    add_l0_part(&catalog, ht, &day1).await;
    catalog
        .commit(
            ht,
            CommitOp::Add {
                parts: vec![PartMeta {
                    path: "l1-b.parquet".into(),
                    partition_values: day1.clone(),
                    packing_key_min: 1,
                    packing_key_max: 1,
                    row_count: 2,
                    size_bytes: 2,
                    level: 1,
                    column_stats: None,
                }],
            },
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        catalog
            .compaction_candidates(ht, Uuid::nil(), 2, 2, 64, None)
            .await
            .unwrap()
            .partitions,
        vec![day1.clone()],
        "two qualifying levels, one partition"
    );

    // Several qualifying partitions: deterministic order, bounded by `limit`.
    add_l0_part(&catalog, ht, &day2).await;
    add_l0_part(&catalog, ht, &day3).await;
    add_l0_part(&catalog, ht, &day3).await;
    assert_eq!(
        catalog
            .compaction_candidates(ht, Uuid::nil(), 2, 2, 64, None)
            .await
            .unwrap()
            .partitions,
        vec![day1.clone(), day2.clone(), day3.clone()]
    );
    assert_eq!(
        catalog
            .compaction_candidates(ht, Uuid::nil(), 2, 2, 2, None)
            .await
            .unwrap()
            .partitions,
        vec![day1, day2],
        "limit is honored"
    );
}

use std::time::Duration;
use uuid::Uuid;

/// Backdates a lease so it is expired for the next statement — stands in for a
/// worker that stalled past its TTL, without sleeping through one.
async fn expire_lease(
    catalog: &ukiel_catalog::PostgresCatalog,
    ht: HypertableId,
    partition: &serde_json::Value,
) {
    sqlx::query(
        "UPDATE compaction_leases SET expires_at = clock_timestamp() - INTERVAL '1 second'
         WHERE hypertable_id = $1 AND partition_values = $2",
    )
    .bind(ht.0)
    .bind(partition)
    .execute(catalog.pool_for_tests())
    .await
    .unwrap();
}

const TTL: Duration = Duration::from_secs(60);

#[tokio::test(flavor = "multi_thread")]
async fn one_lease_winner_under_contention() {
    let (_pg, catalog) = common::setup().await;
    let ht = setup_hypertable(&catalog).await;
    let day1 = json!({ "day": "2026-07-01" });

    // 32 contenders (plan 40's worst same-partition shape) hit one partition.
    let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(32));
    let mut handles = Vec::new();
    for _ in 0..32 {
        let (catalog, barrier, partition) = (catalog.clone(), barrier.clone(), day1.clone());
        handles.push(tokio::spawn(async move {
            barrier.wait().await;
            catalog
                .try_acquire_compaction_lease(ht, &partition, Uuid::new_v4(), TTL)
                .await
                .unwrap()
        }));
    }
    let mut won = Vec::new();
    for h in handles {
        if let Some(lease) = h.await.unwrap() {
            won.push(lease);
        }
    }
    assert_eq!(won.len(), 1, "exactly one owner: {won:?}");
    // The generation is a sequence draw, not a count: the 31 losers each
    // evaluated the VALUES list on their way to losing, so the winner's token
    // says nothing about how many contenders there were.
    assert!(won[0].generation > 0);
    assert_eq!(won[0].partition_values, day1);

    // The 31 losers saw Ok(None) — contention is scheduling, not an error.
    // A fresh contender still sees None while the owner is live.
    assert!(
        catalog
            .try_acquire_compaction_lease(ht, &day1, Uuid::new_v4(), TTL)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn lease_reacquire_renew_and_release() {
    let (_pg, catalog) = common::setup().await;
    let ht = setup_hypertable(&catalog).await;
    let day1 = json!({ "day": "2026-07-01" });
    let day2 = json!({ "day": "2026-07-02" });
    let owner = Uuid::new_v4();

    let lease = catalog
        .try_acquire_compaction_lease(ht, &day1, owner, TTL)
        .await
        .unwrap()
        .expect("free partition");

    // The same owner reacquiring is idempotent: no generation churn (a retry
    // must not invalidate the fence its own in-flight merge is holding).
    let again = catalog
        .try_acquire_compaction_lease(ht, &day1, owner, TTL)
        .await
        .unwrap()
        .expect("own lease");
    assert_eq!(again.generation, lease.generation);

    // Renewal extends expiry and preserves the generation.
    let renewed = catalog
        .renew_compaction_lease(&lease, Duration::from_secs(120))
        .await
        .unwrap()
        .expect("still ours");
    assert_eq!(renewed.generation, lease.generation);
    assert!(renewed.expires_at > again.expires_at);

    // Partitions are independent: a different one is free to another owner.
    let other = catalog
        .try_acquire_compaction_lease(ht, &day2, Uuid::new_v4(), TTL)
        .await
        .unwrap()
        .expect("different partition");
    assert!(other.generation > 0);

    // Release is scoped to the exact owner/generation.
    assert!(catalog.release_compaction_lease(&renewed).await.unwrap());
    assert!(
        !catalog.release_compaction_lease(&renewed).await.unwrap(),
        "releasing twice is a no-op, not a lie"
    );
    // Released -> immediately claimable by anyone, and at a generation that has
    // never been used before (issue 0013 — the row is gone, but the token
    // sequence is not).
    let next = catalog
        .try_acquire_compaction_lease(ht, &day1, Uuid::new_v4(), TTL)
        .await
        .unwrap()
        .expect("released partition");
    assert!(
        next.generation > renewed.generation,
        "a released generation is not recycled"
    );
}

#[tokio::test]
async fn expired_lease_is_taken_over_and_stale_owner_is_fenced_out() {
    let (_pg, catalog) = common::setup().await;
    let ht = setup_hypertable(&catalog).await;
    let day1 = json!({ "day": "2026-07-01" });
    let (a, b) = (Uuid::new_v4(), Uuid::new_v4());

    let stale = catalog
        .try_acquire_compaction_lease(ht, &day1, a, TTL)
        .await
        .unwrap()
        .expect("free");
    // A stalls past its TTL.
    expire_lease(&catalog, ht, &day1).await;

    // B takes over; reclamation bumps the generation.
    let fresh = catalog
        .try_acquire_compaction_lease(ht, &day1, b, TTL)
        .await
        .unwrap()
        .expect("expired lease is reclaimable");
    assert!(fresh.generation > stale.generation);
    assert_eq!(fresh.owner_id, b);

    // A wakes up: it can neither renew nor release B's tenancy, and its renewal
    // failure is a *proven* loss, not an error to retry.
    assert!(
        catalog
            .renew_compaction_lease(&stale, TTL)
            .await
            .unwrap()
            .is_none()
    );
    assert!(!catalog.release_compaction_lease(&stale).await.unwrap());
    let held = catalog
        .renew_compaction_lease(&fresh, TTL)
        .await
        .unwrap()
        .expect("B still owns it");
    assert_eq!(held.owner_id, b);

    // An expired lease cannot renew itself back to life: only a fresh
    // acquisition (which bumps the generation) restores tenancy.
    expire_lease(&catalog, ht, &day1).await;
    assert!(
        catalog
            .renew_compaction_lease(&fresh, TTL)
            .await
            .unwrap()
            .is_none()
    );
    let regained = catalog
        .try_acquire_compaction_lease(ht, &day1, b, TTL)
        .await
        .unwrap()
        .expect("same owner reclaims");
    assert!(
        regained.generation > fresh.generation,
        "reclaim after expiry takes a new generation even for the same owner"
    );
}

#[tokio::test]
async fn lease_ttl_is_validated_in_rust() {
    let (_pg, catalog) = common::setup().await;
    let ht = setup_hypertable(&catalog).await;
    let err = catalog
        .try_acquire_compaction_lease(ht, &json!({"day": "d"}), Uuid::new_v4(), Duration::ZERO)
        .await
        .unwrap_err();
    assert!(matches!(err, CatalogError::InvalidLeaseTtl(_)), "{err:?}");
}

#[tokio::test]
async fn lease_stats_separate_active_from_abandoned() {
    let (_pg, catalog) = common::setup().await;
    let ht = setup_hypertable(&catalog).await;
    let day1 = json!({ "day": "2026-07-01" });
    let day2 = json!({ "day": "2026-07-02" });

    assert_eq!(catalog.compaction_lease_stats().await.unwrap(), (0, 0.0));

    catalog
        .try_acquire_compaction_lease(ht, &day1, Uuid::new_v4(), TTL)
        .await
        .unwrap()
        .unwrap();
    catalog
        .try_acquire_compaction_lease(ht, &day2, Uuid::new_v4(), TTL)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(catalog.compaction_lease_stats().await.unwrap(), (2, 0.0));

    // An expired, unreclaimed lease is the wedged-fleet signal: it drops out of
    // the active count and shows up as age.
    expire_lease(&catalog, ht, &day1).await;
    let (active, oldest) = catalog.compaction_lease_stats().await.unwrap();
    assert_eq!(active, 1);
    assert!(oldest >= 1.0, "expired ~1s ago, got {oldest}");
}

#[tokio::test]
async fn lease_migration_applies_to_a_populated_pre_0008_catalog() {
    let (_pg, catalog) = common::setup().await;
    let ht = setup_hypertable(&catalog).await;
    add_l0_part(&catalog, ht, &json!({ "day": "2026-07-01" })).await;

    // Rewind to a pre-0008 catalog that already holds data, then upgrade — the
    // path a running deployment takes, not the fresh bootstrap setup() covers.
    let pool = catalog.pool_for_tests();
    sqlx::query("DROP TABLE compaction_leases")
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("DELETE FROM _sqlx_migrations WHERE version = 8")
        .execute(pool)
        .await
        .unwrap();

    catalog.migrate().await.unwrap();

    let day = json!({ "day": "2026-07-01" });
    let lease = catalog
        .try_acquire_compaction_lease(ht, &day, Uuid::new_v4(), TTL)
        .await
        .unwrap()
        .expect("leases work after the upgrade");
    assert!(lease.generation > 0);
    // The pre-existing data is untouched by the upgrade.
    assert_eq!(catalog.live_parts(ht, None).await.unwrap().len(), 1);
}

/// The identity an ingest flush would build for these ranges. Derived, never
/// hand-assembled, so the offset tests exercise the real reconcilable path.
fn offsets_op(ht: HypertableId, ranges: &[OffsetRange]) -> OperationIdentity {
    let ranges: Vec<IngestRange> = ranges
        .iter()
        .map(|r| IngestRange {
            topic: r.topic.clone(),
            partition: r.partition,
            first: r.first,
            last: r.last,
        })
        .collect();
    OperationIdentity::ingest(ht, &ranges, 1).unwrap()
}

/// A factory-built identity. The catalog boundary only ever sees a (key,
/// fingerprint) pair, so the domain is immaterial here — what matters is that
/// the pair is *derived*, never hand-assembled.
fn op(ht: HypertableId, partition: i32, first: i64, last: i64) -> OperationIdentity {
    OperationIdentity::ingest(
        ht,
        &[IngestRange {
            topic: "events".into(),
            partition,
            first,
            last,
        }],
        1,
    )
    .unwrap()
}

/// Live part ids of one partition.
async fn live_ids(
    catalog: &ukiel_catalog::PostgresCatalog,
    ht: HypertableId,
    partition: &serde_json::Value,
) -> Vec<ukiel_core::PartId> {
    catalog
        .live_partition_parts(ht, partition)
        .await
        .unwrap()
        .iter()
        .map(|p| p.id)
        .collect()
}

fn l1_meta(path: &str, partition: &serde_json::Value) -> PartMeta {
    PartMeta {
        path: path.to_string(),
        partition_values: partition.clone(),
        packing_key_min: 1,
        packing_key_max: 1,
        row_count: 2,
        size_bytes: 2,
        level: 1,
        column_stats: None,
    }
}

#[tokio::test]
async fn leased_replace_commits_only_for_the_current_generation() {
    let (_pg, catalog) = common::setup().await;
    let ht = setup_hypertable(&catalog).await;
    let day1 = json!({ "day": "2026-07-01" });
    add_l0_part(&catalog, ht, &day1).await;
    add_l0_part(&catalog, ht, &day1).await;

    let owner = Uuid::new_v4();
    let lease = catalog
        .try_acquire_compaction_lease(ht, &day1, owner, TTL)
        .await
        .unwrap()
        .unwrap();

    // The current owner commits.
    let old = live_ids(&catalog, ht, &day1).await;
    catalog
        .commit_compaction_replace(
            ht,
            &lease,
            old,
            vec![l1_meta("l1-a.parquet", &day1)],
            &op(ht, 101, 0, 0),
        )
        .await
        .unwrap();
    let live = catalog.live_partition_parts(ht, &day1).await.unwrap();
    assert_eq!(live.len(), 1);
    assert_eq!(live[0].meta.level, 1);

    // A stale generation cannot: B took the partition over after A expired, so
    // A's REPLACE is refused and nothing is tombstoned.
    add_l0_part(&catalog, ht, &day1).await;
    expire_lease(&catalog, ht, &day1).await;
    let b = catalog
        .try_acquire_compaction_lease(ht, &day1, Uuid::new_v4(), TTL)
        .await
        .unwrap()
        .unwrap();
    assert!(b.generation > lease.generation);

    let old = live_ids(&catalog, ht, &day1).await;
    let err = catalog
        .commit_compaction_replace(
            ht,
            &lease,
            old.clone(),
            vec![l1_meta("zombie.parquet", &day1)],
            &op(ht, 102, 0, 0),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, CatalogError::LeaseLost { .. }), "{err:?}");
    assert_eq!(
        catalog.live_partition_parts(ht, &day1).await.unwrap().len(),
        2,
        "the fenced-out commit tombstoned nothing"
    );
    assert!(
        !catalog
            .all_part_paths(ht)
            .await
            .unwrap()
            .contains(&"zombie.parquet".to_string()),
        "and inserted nothing"
    );

    // The current owner still commits fine.
    catalog
        .commit_compaction_replace(
            ht,
            &b,
            old,
            vec![l1_meta("l1-b.parquet", &day1)],
            &op(ht, 103, 0, 0),
        )
        .await
        .unwrap();
    assert_eq!(
        catalog.live_partition_parts(ht, &day1).await.unwrap().len(),
        1
    );
}

#[tokio::test]
async fn leased_replace_rejects_wrong_owner_expired_and_missing_leases() {
    let (_pg, catalog) = common::setup().await;
    let ht = setup_hypertable(&catalog).await;
    let day1 = json!({ "day": "2026-07-01" });
    add_l0_part(&catalog, ht, &day1).await;
    add_l0_part(&catalog, ht, &day1).await;
    let old = live_ids(&catalog, ht, &day1).await;

    let lease = catalog
        .try_acquire_compaction_lease(ht, &day1, Uuid::new_v4(), TTL)
        .await
        .unwrap()
        .unwrap();

    // Same generation, forged owner.
    let impostor = ukiel_catalog::CompactionLease {
        owner_id: Uuid::new_v4(),
        ..lease.clone()
    };
    let err = catalog
        .commit_compaction_replace(
            ht,
            &impostor,
            old.clone(),
            vec![l1_meta("x.parquet", &day1)],
            &op(ht, 104, 0, 0),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, CatalogError::LeaseLost { .. }), "{err:?}");

    // Right owner and generation, but the tenancy has expired: PostgreSQL's
    // clock decides, not the worker's belief that it is still inside its TTL.
    expire_lease(&catalog, ht, &day1).await;
    let err = catalog
        .commit_compaction_replace(
            ht,
            &lease,
            old.clone(),
            vec![l1_meta("x.parquet", &day1)],
            &op(ht, 105, 0, 0),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, CatalogError::LeaseLost { .. }), "{err:?}");

    // Released outright: no row at all is also no ownership.
    catalog.release_compaction_lease(&lease).await.unwrap();
    let err = catalog
        .commit_compaction_replace(
            ht,
            &lease,
            old.clone(),
            vec![l1_meta("x.parquet", &day1)],
            &op(ht, 106, 0, 0),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, CatalogError::LeaseLost { .. }), "{err:?}");
    assert_eq!(
        catalog.live_partition_parts(ht, &day1).await.unwrap().len(),
        2
    );
}

#[tokio::test]
async fn a_lease_on_one_partition_cannot_rewrite_another() {
    let (_pg, catalog) = common::setup().await;
    let ht = setup_hypertable(&catalog).await;
    let day1 = json!({ "day": "2026-07-01" });
    let day2 = json!({ "day": "2026-07-02" });
    add_l0_part(&catalog, ht, &day1).await;
    add_l0_part(&catalog, ht, &day2).await;

    let lease = catalog
        .try_acquire_compaction_lease(ht, &day1, Uuid::new_v4(), TTL)
        .await
        .unwrap()
        .unwrap();

    // Inputs from the unleased partition.
    let err = catalog
        .commit_compaction_replace(
            ht,
            &lease,
            live_ids(&catalog, ht, &day2).await,
            vec![l1_meta("cross.parquet", &day1)],
            &op(ht, 107, 0, 0),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(err, CatalogError::LeasePartitionMismatch { .. }),
        "{err:?}"
    );

    // Outputs aimed at the unleased partition.
    let err = catalog
        .commit_compaction_replace(
            ht,
            &lease,
            live_ids(&catalog, ht, &day1).await,
            vec![l1_meta("cross.parquet", &day2)],
            &op(ht, 108, 0, 0),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(err, CatalogError::LeasePartitionMismatch { .. }),
        "{err:?}"
    );
    assert_eq!(
        catalog.live_partition_parts(ht, &day2).await.unwrap().len(),
        1,
        "day2 is untouched"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn stale_and_current_leased_commits_serialize_on_the_lease_row() {
    let (_pg, catalog) = common::setup().await;
    let ht = setup_hypertable(&catalog).await;
    let day1 = json!({ "day": "2026-07-01" });
    add_l0_part(&catalog, ht, &day1).await;
    add_l0_part(&catalog, ht, &day1).await;
    let old = live_ids(&catalog, ht, &day1).await;

    let stale = catalog
        .try_acquire_compaction_lease(ht, &day1, Uuid::new_v4(), TTL)
        .await
        .unwrap()
        .unwrap();
    expire_lease(&catalog, ht, &day1).await;
    let current = catalog
        .try_acquire_compaction_lease(ht, &day1, Uuid::new_v4(), TTL)
        .await
        .unwrap()
        .unwrap();

    // Both fire at once at the same live inputs; the FOR UPDATE lock inside the
    // commit orders them and only the current generation survives.
    let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(2));
    let mut handles = Vec::new();
    for (lease, path) in [(stale, "stale.parquet"), (current, "current.parquet")] {
        let (catalog, barrier, old, partition) =
            (catalog.clone(), barrier.clone(), old.clone(), day1.clone());
        handles.push(tokio::spawn(async move {
            barrier.wait().await;
            catalog
                .commit_compaction_replace(
                    ht,
                    &lease,
                    old,
                    vec![l1_meta(path, &partition)],
                    &op(ht, 109, 0, 0),
                )
                .await
        }));
    }
    let mut committed = 0;
    let mut fenced = 0;
    for h in handles {
        match h.await.unwrap() {
            Ok(_) => committed += 1,
            Err(CatalogError::LeaseLost { .. }) => fenced += 1,
            Err(e) => panic!("unexpected {e:?}"),
        }
    }
    assert_eq!((committed, fenced), (1, 1));

    let live = catalog.live_partition_parts(ht, &day1).await.unwrap();
    assert_eq!(live.len(), 1);
    assert_eq!(
        live[0].meta.path, "current.parquet",
        "the zombie's output never became live"
    );
}

#[tokio::test]
async fn ingest_and_deletion_still_race_the_lease_holder_safely() {
    let (_pg, catalog) = common::setup().await;
    let ht = setup_hypertable(&catalog).await;
    let day1 = json!({ "day": "2026-07-01" });
    add_l0_part(&catalog, ht, &day1).await;
    add_l0_part(&catalog, ht, &day1).await;

    let lease = catalog
        .try_acquire_compaction_lease(ht, &day1, Uuid::new_v4(), TTL)
        .await
        .unwrap()
        .unwrap();
    let planned = live_ids(&catalog, ht, &day1).await;

    // Ingest takes no lease and needs none: an ADD lands while the compactor
    // holds the partition, and the leased REPLACE of the older inputs still
    // commits. The new L0 simply stays live.
    add_l0_part(&catalog, ht, &day1).await;
    catalog
        .commit_compaction_replace(
            ht,
            &lease,
            planned.clone(),
            vec![l1_meta("l1.parquet", &day1)],
            &op(ht, 110, 0, 0),
        )
        .await
        .unwrap();
    let live = catalog.live_partition_parts(ht, &day1).await.unwrap();
    assert_eq!(live.len(), 2, "merged L1 + the L0 that arrived mid-merge");

    // Deletion takes no lease either, and it still wins the optimistic race:
    // the lease fence passes, then the conflict check refuses a REPLACE whose
    // input was tombstoned under it. Scheduling ownership is not data
    // integrity — REPLACE remains the backstop.
    let planned = live_ids(&catalog, ht, &day1).await;
    catalog
        .commit(
            ht,
            CommitOp::Delete {
                parts: vec![planned[0]],
            },
            None,
        )
        .await
        .unwrap();
    let err = catalog
        .commit_compaction_replace(
            ht,
            &lease,
            planned,
            vec![l1_meta("l2.parquet", &day1)],
            &op(ht, 111, 0, 0),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, CatalogError::Conflict { .. }), "{err:?}");
}

#[tokio::test]
async fn an_idempotent_replay_is_reconciled_before_the_lease_is_judged() {
    let (_pg, catalog) = common::setup().await;
    let ht = setup_hypertable(&catalog).await;
    let day1 = json!({ "day": "2026-07-01" });
    add_l0_part(&catalog, ht, &day1).await;
    add_l0_part(&catalog, ht, &day1).await;
    let old = live_ids(&catalog, ht, &day1).await;

    let lease = catalog
        .try_acquire_compaction_lease(ht, &day1, Uuid::new_v4(), TTL)
        .await
        .unwrap()
        .unwrap();
    let CommitResult::Committed(id) = catalog
        .commit_compaction_replace(
            ht,
            &lease,
            old.clone(),
            vec![l1_meta("l1.parquet", &day1)],
            &op(ht, 1, 0, 0),
        )
        .await
        .unwrap()
    else {
        panic!("expected a commit")
    };

    // The acknowledgement was lost and the worker's lease has since been taken
    // over. Replaying the *same operation* must report the durable truth ("it
    // already landed"), not LeaseLost — a worker that is told it lost would redo
    // work that is already committed. This ordering is what HA phase 2's
    // canonical compactor operation identities splice into.
    expire_lease(&catalog, ht, &day1).await;
    catalog
        .try_acquire_compaction_lease(ht, &day1, Uuid::new_v4(), TTL)
        .await
        .unwrap()
        .unwrap();
    let replay = catalog
        .commit_compaction_replace(
            ht,
            &lease,
            old,
            vec![l1_meta("l1.parquet", &day1)],
            &op(ht, 1, 0, 0),
        )
        .await
        .unwrap();
    assert_eq!(replay, CommitResult::AlreadyApplied(id));
    assert_eq!(
        catalog.live_partition_parts(ht, &day1).await.unwrap().len(),
        1,
        "the replay changed nothing"
    );
}

#[tokio::test]
async fn the_same_owner_is_fenced_by_generation_alone() {
    // A restarted (or simply slow) worker can hold the *right* owner id and
    // still be a zombie: it is the generation, freshly drawn by every
    // reclamation, that separates its old tenancy from its new one.
    let (_pg, catalog) = common::setup().await;
    let ht = setup_hypertable(&catalog).await;
    let day1 = json!({ "day": "2026-07-01" });
    add_l0_part(&catalog, ht, &day1).await;
    add_l0_part(&catalog, ht, &day1).await;
    let old = live_ids(&catalog, ht, &day1).await;

    let owner = Uuid::new_v4();
    let first = catalog
        .try_acquire_compaction_lease(ht, &day1, owner, TTL)
        .await
        .unwrap()
        .unwrap();
    expire_lease(&catalog, ht, &day1).await;
    let second = catalog
        .try_acquire_compaction_lease(ht, &day1, owner, TTL)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(second.owner_id, first.owner_id);
    assert!(second.generation > first.generation);

    let err = catalog
        .commit_compaction_replace(
            ht,
            &first,
            old.clone(),
            vec![l1_meta("stale.parquet", &day1)],
            &op(ht, 112, 0, 0),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, CatalogError::LeaseLost { .. }), "{err:?}");

    catalog
        .commit_compaction_replace(
            ht,
            &second,
            old,
            vec![l1_meta("fresh.parquet", &day1)],
            &op(ht, 113, 0, 0),
        )
        .await
        .unwrap();
    let live = catalog.live_partition_parts(ht, &day1).await.unwrap();
    assert_eq!(live.len(), 1);
    assert_eq!(live[0].meta.path, "fresh.parquet");
}

#[tokio::test]
async fn a_lazy_pool_starts_without_a_database_and_fails_classifiably() {
    // Plan 42: ukield must be able to start, bind its listeners and answer
    // liveness while the writer is still being promoted. So building the pool
    // must not touch the database...
    let cfg = ukiel_catalog::CatalogPoolConfig {
        acquire_timeout: Duration::from_millis(250),
        ..Default::default()
    };
    // Port 1 is reserved and nothing listens there.
    let catalog = ukiel_catalog::PostgresCatalog::connect_lazy_with_config(
        "postgres://u:p@127.0.0.1:1/db",
        &cfg,
    )
    .expect("a lazy pool is built, not connected");

    // ...and the failure must arrive at the call site, classified as transport,
    // so the recovery supervisor can tell "come back later" from "you are wrong".
    let err = catalog.ping().await.unwrap_err();
    assert!(
        err.is_recoverable_transport(),
        "an unreachable database must be recoverable transport, got {:?}",
        err.class()
    );
}

#[tokio::test]
async fn a_lazy_pool_reaches_a_database_that_appears_later() {
    // The point of lazy: the pool outlives the outage. It is built before the
    // database is usable and still reaches it afterwards — no new process, no
    // reconstructed pool.
    let (_pg, _seeded, url) = common::setup_with_url().await;
    let catalog = ukiel_catalog::PostgresCatalog::connect_lazy_with_config(
        &url,
        &ukiel_catalog::CatalogPoolConfig::default(),
    )
    .unwrap();
    catalog.ping().await.expect("first use connects");
}

#[tokio::test]
async fn an_invalid_url_is_a_permanent_error_at_build_time() {
    let built = ukiel_catalog::PostgresCatalog::connect_lazy_with_config(
        "not-a-postgres-url",
        &ukiel_catalog::CatalogPoolConfig::default(),
    );
    match built {
        Ok(_) => panic!("a malformed URL must not build a pool"),
        Err(e) => assert!(
            !e.is_recoverable_transport(),
            "a malformed URL will not fix itself: {e:?}"
        ),
    }
}

/// Issue 0012: a bounded candidate window must be a *moving* window. The old
/// selector always returned the same lexical prefix and counted peer-held
/// partitions against it, so a hot head could keep the tail unreachable and a
/// busy fleet could spend a whole window on work nobody asking for it could
/// take.
#[tokio::test]
async fn candidate_windows_are_lease_aware_and_resumable() {
    let (_pg, catalog) = common::setup().await;
    let ht = setup_hypertable(&catalog).await;
    let days: Vec<serde_json::Value> = (1..=4).map(|d| json!({ "day": format!("d{d}") })).collect();
    for day in &days {
        add_l0_part(&catalog, ht, day).await;
        add_l0_part(&catalog, ht, day).await;
    }

    // A window of two, then the next two: the cursor resumes strictly after the
    // partition it names, so the tail is reached without widening the bound.
    let first = catalog
        .compaction_candidates(ht, Uuid::nil(), 2, 2, 2, None)
        .await
        .unwrap();
    assert_eq!(first.partitions, days[..2]);
    assert_eq!((first.eligible, first.leased, first.claimable()), (4, 0, 4));

    let second = catalog
        .compaction_candidates(ht, Uuid::nil(), 2, 2, 2, first.next_cursor.as_deref())
        .await
        .unwrap();
    assert_eq!(second.partitions, days[2..]);
    assert_eq!(second.eligible, 4, "the backlog is the set, not the window");

    // Past the tail: no partitions, but the backlog is still reported — "swept
    // out" and "nothing to do" are different states, and only one of them means
    // the cursor should wrap.
    let past_tail = catalog
        .compaction_candidates(ht, Uuid::nil(), 2, 2, 2, second.next_cursor.as_deref())
        .await
        .unwrap();
    assert!(past_tail.partitions.is_empty());
    assert_eq!(past_tail.eligible, 4);
    assert_eq!(past_tail.next_cursor, None);

    // A peer's unexpired lease on the head no longer costs a window slot: the
    // window fills with work this caller can actually claim.
    let peer = Uuid::new_v4();
    catalog
        .try_acquire_compaction_lease(ht, &days[0], peer, TTL)
        .await
        .unwrap()
        .expect("d1 is free");
    let held = catalog
        .compaction_candidates(ht, Uuid::nil(), 2, 2, 2, None)
        .await
        .unwrap();
    assert_eq!(held.partitions, days[1..3]);
    assert_eq!(
        (held.eligible, held.leased, held.claimable()),
        (4, 1, 3),
        "the leased partition stays visible as backlog, just not as a candidate"
    );

    // ...but the *holder* still sees its own partition. A process rebuilt after
    // a blip keeps its owner identity precisely so it can take back the work it
    // already owns; hiding its own lease from it would lock it out until its own
    // TTL ran down — a self-inflicted failover on every restart.
    let mine = catalog
        .compaction_candidates(ht, peer, 2, 2, 2, None)
        .await
        .unwrap();
    assert_eq!(mine.partitions, days[..2]);
    assert_eq!(mine.leased, 0, "a lease I hold is not a lease against me");

    // Expiry — Postgres time, not the caller's — puts it back in everyone's
    // window.
    expire_lease(&catalog, ht, &days[0]).await;
    let reclaimed = catalog
        .compaction_candidates(ht, Uuid::nil(), 2, 2, 2, None)
        .await
        .unwrap();
    assert_eq!(reclaimed.partitions, days[..2]);
    assert_eq!(reclaimed.leased, 0);
}

/// Finalization shares the fair selector (issue 0012). Its sweep used to be
/// unbounded and lease-blind: every replica walked the whole cold set each tick
/// and offered each other partitions they already held.
#[tokio::test]
async fn finalize_windows_are_lease_aware_and_resumable() {
    let (_pg, catalog) = common::setup().await;
    let ht = setup_hypertable(&catalog).await;
    let days: Vec<serde_json::Value> = (1..=3).map(|d| json!({ "day": format!("d{d}") })).collect();
    for day in &days {
        add_l0_part(&catalog, ht, day).await;
        add_l0_part(&catalog, ht, day).await;
    }

    let first = catalog
        .finalize_candidates(ht, Uuid::nil(), 0.0, 2, None)
        .await
        .unwrap();
    assert_eq!(first.partitions, days[..2]);
    assert_eq!((first.eligible, first.leased), (3, 0));

    let rest = catalog
        .finalize_candidates(ht, Uuid::nil(), 0.0, 2, first.next_cursor.as_deref())
        .await
        .unwrap();
    assert_eq!(rest.partitions, days[2..]);

    catalog
        .try_acquire_compaction_lease(ht, &days[0], Uuid::new_v4(), TTL)
        .await
        .unwrap()
        .expect("d1 is free");
    let held = catalog
        .finalize_candidates(ht, Uuid::nil(), 0.0, 2, None)
        .await
        .unwrap();
    assert_eq!(held.partitions, days[1..]);
    assert_eq!((held.eligible, held.leased), (3, 1));
}

/// Quietness is part of eligibility, not a post-claim filter (issue 0012).
///
/// ">= 2 live runs" describes nearly every partition that has been written to
/// and not yet finalized. Bounding the window on *that* set — and only then
/// claiming each partition to discover it is still hot — would spend the window
/// on partitions that cannot be finalized and make a genuinely cold one wait for
/// the cursor to rotate past all of them. The old sweep got away with it by
/// being unbounded; a bounded one cannot.
#[tokio::test]
async fn a_hot_partition_never_occupies_a_finalize_window_slot() {
    let (_pg, catalog) = common::setup().await;
    let ht = setup_hypertable(&catalog).await;
    let hot = json!({ "day": "d1" });
    let cold = json!({ "day": "d2" });
    for partition in [&hot, &cold] {
        add_l0_part(&catalog, ht, partition).await;
        add_l0_part(&catalog, ht, partition).await;
    }

    // Both partitions have two live runs; both had L0 arrivals just now, so with
    // any real quiet period neither is finalizable.
    let none_quiet = catalog
        .finalize_candidates(ht, Uuid::nil(), 3600.0, 64, None)
        .await
        .unwrap();
    assert_eq!(none_quiet.partitions, Vec::<serde_json::Value>::new());
    assert_eq!(
        none_quiet.eligible, 0,
        "a hot partition is not backlog the finalizer can drain"
    );

    // Backdate d2's arrivals past the quiet period. d1 stays hot — and stays out
    // of the window even though it sorts first and would have taken the only
    // slot.
    sqlx::query(
        "UPDATE commits SET created_at = clock_timestamp() - INTERVAL '2 hours'
         WHERE id IN (SELECT created_by_commit FROM parts
                      WHERE hypertable_id = $1 AND partition_values = $2)",
    )
    .bind(ht.0)
    .bind(&cold)
    .execute(catalog.pool_for_tests())
    .await
    .unwrap();

    let quiet = catalog
        .finalize_candidates(ht, Uuid::nil(), 3600.0, 1, None)
        .await
        .unwrap();
    assert_eq!(
        quiet.partitions,
        vec![cold],
        "the one window slot went to the partition that can actually finalize"
    );
    assert_eq!(quiet.eligible, 1);
}

/// Issue 0013 — the ABA the old per-row counter allowed.
///
/// Clean release *deletes* the lease row, so the next acquisition used to insert
/// a fresh row at generation 1. Combined with a stable process owner id, that
/// made `(partition, owner A, generation 1)` reachable twice — and those three
/// fields are exactly what renew, release and the fenced REPLACE compare. A
/// caller that held its old token across the release (a delayed cleanup task,
/// in-process concurrency, or a reconciliation lifecycle carrying an operation
/// across a worker rebuild) could act on a *later* tenancy of its own process.
#[tokio::test]
async fn a_released_lease_generation_is_never_reissued() {
    let (_pg, catalog) = common::setup().await;
    let ht = setup_hypertable(&catalog).await;
    let day1 = json!({ "day": "2026-07-01" });
    add_l0_part(&catalog, ht, &day1).await;
    add_l0_part(&catalog, ht, &day1).await;

    // One process, one owner id — the case the row counter could not separate.
    let owner = Uuid::new_v4();
    let stale = catalog
        .try_acquire_compaction_lease(ht, &day1, owner, TTL)
        .await
        .unwrap()
        .expect("free");
    assert!(catalog.release_compaction_lease(&stale).await.unwrap());

    let current = catalog
        .try_acquire_compaction_lease(ht, &day1, owner, TTL)
        .await
        .unwrap()
        .expect("released partition is immediately claimable");
    assert_eq!(current.owner_id, stale.owner_id);
    assert!(
        current.generation > stale.generation,
        "a released generation must not be reissued: {} -> {}",
        stale.generation,
        current.generation
    );

    // The retained token is inert against the new tenancy on all three paths.
    assert!(
        catalog
            .renew_compaction_lease(&stale, TTL)
            .await
            .unwrap()
            .is_none(),
        "a pre-release token must not renew its successor"
    );
    assert!(
        !catalog.release_compaction_lease(&stale).await.unwrap(),
        "a pre-release token must not release its successor"
    );
    let old = live_ids(&catalog, ht, &day1).await;
    let err = catalog
        .commit_compaction_replace(
            ht,
            &stale,
            old,
            vec![l1_meta("aba.parquet", &day1)],
            &op(ht, 114, 0, 0),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, CatalogError::LeaseLost { .. }), "{err:?}");
    assert_eq!(
        catalog.live_partition_parts(ht, &day1).await.unwrap().len(),
        2,
        "the fenced-out commit tombstoned nothing"
    );

    // The current tenancy still works, and renewal still preserves its fence.
    let renewed = catalog
        .renew_compaction_lease(&current, TTL)
        .await
        .unwrap()
        .expect("the live tenancy");
    assert_eq!(renewed.generation, current.generation);

    // A *different* owner taking the partition after a release gets a fresh
    // token too — the same invariant, with the owner id no longer masking it.
    assert!(catalog.release_compaction_lease(&current).await.unwrap());
    let third = catalog
        .try_acquire_compaction_lease(ht, &day1, Uuid::new_v4(), TTL)
        .await
        .unwrap()
        .expect("free");
    assert!(third.generation > current.generation);
}

/// The sequence is a *catalog* token, not a per-partition one: two partitions
/// never share a generation, so a token is unambiguous on its own.
#[tokio::test]
async fn lease_generations_are_unique_across_partitions() {
    let (_pg, catalog) = common::setup().await;
    let ht = setup_hypertable(&catalog).await;
    let owner = Uuid::new_v4();
    let mut seen = std::collections::HashSet::new();
    for d in 1..=5 {
        let lease = catalog
            .try_acquire_compaction_lease(ht, &json!({ "day": format!("d{d}") }), owner, TTL)
            .await
            .unwrap()
            .expect("free");
        assert!(
            seen.insert(lease.generation),
            "generation {} was handed out twice",
            lease.generation
        );
    }
}

/// An upgrade, not a fresh install: a catalog already carrying generations must
/// start the sequence *above* every one of them, or a live tenancy could be
/// re-issued as a new one — the very bug, reintroduced by the fix.
#[tokio::test]
async fn generation_sequence_starts_above_existing_leases() {
    let (_pg, catalog) = common::setup().await;
    let ht = setup_hypertable(&catalog).await;
    let pool = catalog.pool_for_tests();

    // Rewind to a pre-0010 catalog that already holds a high-generation lease
    // (plan 41's row counter could produce any value on a long-lived partition).
    sqlx::query("DROP SEQUENCE compaction_lease_generation_seq")
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("DELETE FROM _sqlx_migrations WHERE version = 10")
        .execute(pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO compaction_leases
             (hypertable_id, partition_values, owner_id, generation, expires_at)
         VALUES ($1, $2, $3, 4242, clock_timestamp() - INTERVAL '1 second')",
    )
    .bind(ht.0)
    .bind(json!({ "day": "legacy" }))
    .bind(Uuid::new_v4())
    .execute(pool)
    .await
    .unwrap();

    catalog.migrate().await.unwrap();

    // Reclaiming the legacy partition — the exact case that must not collide.
    let reclaimed = catalog
        .try_acquire_compaction_lease(ht, &json!({ "day": "legacy" }), Uuid::new_v4(), TTL)
        .await
        .unwrap()
        .expect("the expired legacy lease is reclaimable");
    assert!(
        reclaimed.generation > 4242,
        "the sequence must start above every generation the old counter handed out, got {}",
        reclaimed.generation
    );
}

// ---------------------------------------------------------------------------
// Plan 43: operation identity, fingerprint storage, authoritative lookup.
// ---------------------------------------------------------------------------

use ukiel_catalog::{CatalogErrorClass, OperationLookup};

#[tokio::test]
async fn lookup_answers_absent_then_committed() {
    let (_pg, catalog) = common::setup().await;
    let ht = setup_hypertable(&catalog).await;
    let identity = op(ht, 0, 0, 99);

    assert_eq!(
        catalog.lookup_operation(&identity).await.unwrap(),
        OperationLookup::Absent,
        "nothing has been committed under this intent"
    );

    let CommitResult::Committed(id) = catalog
        .commit(
            ht,
            CommitOp::Add {
                parts: vec![part_meta("p1.parquet", 1, 10)],
            },
            Some(&identity),
        )
        .await
        .unwrap()
    else {
        panic!("expected a commit")
    };
    assert_eq!(
        catalog.lookup_operation(&identity).await.unwrap(),
        OperationLookup::Committed(id)
    );

    // Scoped to its hypertable: the same key under another table is not this
    // operation. (Two hypertables cannot in practice produce the same key —
    // the id is inside the fingerprint — but the query must be scoped anyway,
    // or the uniqueness the catalog enforces and the one the caller assumes
    // would differ.)
    let other = setup_hypertable_named(&catalog, "events2").await;
    assert_eq!(
        catalog
            .lookup_operation(
                &OperationIdentity::from_stored(
                    other,
                    identity.key().to_string(),
                    *identity.fingerprint()
                )
                .unwrap()
            )
            .await
            .unwrap(),
        OperationLookup::Absent
    );
}

#[tokio::test]
async fn a_replayed_operation_applies_nothing_and_returns_the_original_commit() {
    let (_pg, catalog) = common::setup().await;
    let ht = setup_hypertable(&catalog).await;
    let identity = op(ht, 0, 0, 99);

    let CommitResult::Committed(first) = catalog
        .commit(
            ht,
            CommitOp::Add {
                parts: vec![part_meta("first.parquet", 1, 10)],
            },
            Some(&identity),
        )
        .await
        .unwrap()
    else {
        panic!("expected a commit")
    };

    // The retry uploaded *different objects* — a fresh attempt at the same
    // intent always does, because paths carry a UUID. It must still reconcile.
    let replay = catalog
        .commit(
            ht,
            CommitOp::Add {
                parts: vec![part_meta("retry.parquet", 1, 10)],
            },
            Some(&identity),
        )
        .await
        .unwrap();
    assert_eq!(replay, CommitResult::AlreadyApplied(first));

    let live = catalog.live_parts(ht, None).await.unwrap();
    assert_eq!(live.len(), 1, "the replay inserted nothing");
    assert_eq!(live[0].meta.path, "first.parquet", "the first outputs win");
}

#[tokio::test]
async fn a_key_reused_under_different_intent_is_a_loud_collision() {
    let (_pg, catalog) = common::setup().await;
    let ht = setup_hypertable(&catalog).await;
    let identity = op(ht, 0, 0, 99);
    catalog
        .commit(
            ht,
            CommitOp::Add {
                parts: vec![part_meta("p1.parquet", 1, 10)],
            },
            Some(&identity),
        )
        .await
        .unwrap();

    // Same key, different fingerprint. Only a bug or direct SQL can produce
    // this — which is exactly why the fingerprint is stored beside the key
    // rather than trusted to be derivable from it.
    let forged =
        OperationIdentity::from_stored(ht, identity.key().to_string(), [0xAB; 32]).unwrap();
    for err in [
        catalog.lookup_operation(&forged).await.unwrap_err(),
        catalog
            .commit(
                ht,
                CommitOp::Add {
                    parts: vec![part_meta("forged.parquet", 1, 10)],
                },
                Some(&forged),
            )
            .await
            .unwrap_err(),
    ] {
        assert!(
            matches!(err, CatalogError::OperationKeyCollision { .. }),
            "must never be AlreadyApplied: {err:?}"
        );
        assert_eq!(err.class(), CatalogErrorClass::PermanentInput);
    }
    assert_eq!(
        catalog.live_parts(ht, None).await.unwrap().len(),
        1,
        "a collision mutates nothing"
    );
}

#[tokio::test]
async fn a_historical_key_without_a_fingerprint_collides_rather_than_guessing() {
    let (_pg, catalog) = common::setup().await;
    let ht = setup_hypertable(&catalog).await;
    let identity = op(ht, 0, 0, 99);

    // A pre-plan-43 keyed commit: its intent is not recoverable, and migration
    // 0011 deliberately does not invent one. "Probably the same thing" is not
    // an answer a reconciliation protocol may give.
    sqlx::query(
        "INSERT INTO commits (hypertable_id, kind, idempotency_key) VALUES ($1, 'add', $2)",
    )
    .bind(ht.0)
    .bind(identity.key())
    .execute(catalog.pool_for_tests())
    .await
    .unwrap();

    let err = catalog.lookup_operation(&identity).await.unwrap_err();
    assert!(
        matches!(err, CatalogError::OperationKeyCollision { .. }),
        "{err:?}"
    );
}

#[tokio::test]
async fn an_identity_from_another_hypertable_is_refused_before_any_work() {
    let (_pg, catalog) = common::setup().await;
    let ht = setup_hypertable(&catalog).await;
    let other = setup_hypertable_named(&catalog, "events2").await;

    let err = catalog
        .commit(
            ht,
            CommitOp::Add {
                parts: vec![part_meta("p1.parquet", 1, 10)],
            },
            Some(&op(other, 0, 0, 99)),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(err, CatalogError::OperationHypertableMismatch { .. }),
        "{err:?}"
    );
    assert!(catalog.live_parts(ht, None).await.unwrap().is_empty());
}

/// The ordering that makes reconciliation worth having: durable work beats a
/// lease that expired *after* the commit landed.
#[tokio::test]
async fn a_committed_leased_replace_is_reconciled_before_the_lease_is_judged() {
    let (_pg, catalog) = common::setup().await;
    let ht = setup_hypertable(&catalog).await;
    let day1 = json!({ "day": "2026-07-01" });
    add_l0_part(&catalog, ht, &day1).await;
    add_l0_part(&catalog, ht, &day1).await;
    let old = live_ids(&catalog, ht, &day1).await;
    let identity = op(ht, 1, 0, 0);

    let owner = Uuid::new_v4();
    let lease = catalog
        .try_acquire_compaction_lease(ht, &day1, owner, TTL)
        .await
        .unwrap()
        .unwrap();
    let CommitResult::Committed(id) = catalog
        .commit_compaction_replace(
            ht,
            &lease,
            old.clone(),
            vec![l1_meta("l1.parquet", &day1)],
            &identity,
        )
        .await
        .unwrap()
    else {
        panic!("expected a commit")
    };

    // The acknowledgement was lost. By the time the worker asks again, its lease
    // is long gone — released on the way down, then reissued to a peer at a new
    // generation (issue 0013: a *new* generation, never the old one back).
    assert!(catalog.release_compaction_lease(&lease).await.unwrap());
    let peer = catalog
        .try_acquire_compaction_lease(ht, &day1, owner, TTL)
        .await
        .unwrap()
        .unwrap();
    assert!(peer.generation > lease.generation);

    // Replaying the same intent under the *stale* lease reports the durable
    // truth. Judging the lease first would say LeaseLost and send a second
    // worker to redo a merge that is already committed.
    let replay = catalog
        .commit_compaction_replace(
            ht,
            &lease,
            old,
            vec![l1_meta("redo.parquet", &day1)],
            &identity,
        )
        .await
        .unwrap();
    assert_eq!(replay, CommitResult::AlreadyApplied(id));

    let live = catalog.live_partition_parts(ht, &day1).await.unwrap();
    assert_eq!(live.len(), 1);
    assert_eq!(
        live[0].meta.path, "l1.parquet",
        "the replay changed nothing"
    );
}

/// ...but reconciliation is not a lease bypass. A *new* operation under a stale
/// lease is still fenced out — the replay path only forgives work that is
/// already durable.
#[tokio::test]
async fn a_new_operation_under_a_stale_lease_is_still_fenced_out() {
    let (_pg, catalog) = common::setup().await;
    let ht = setup_hypertable(&catalog).await;
    let day1 = json!({ "day": "2026-07-01" });
    add_l0_part(&catalog, ht, &day1).await;
    add_l0_part(&catalog, ht, &day1).await;
    let old = live_ids(&catalog, ht, &day1).await;

    let stale = catalog
        .try_acquire_compaction_lease(ht, &day1, Uuid::new_v4(), TTL)
        .await
        .unwrap()
        .unwrap();
    expire_lease(&catalog, ht, &day1).await;
    catalog
        .try_acquire_compaction_lease(ht, &day1, Uuid::new_v4(), TTL)
        .await
        .unwrap()
        .expect("a peer takes over");

    let err = catalog
        .commit_compaction_replace(
            ht,
            &stale,
            old,
            vec![l1_meta("zombie.parquet", &day1)],
            &op(ht, 9, 0, 0),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, CatalogError::LeaseLost { .. }), "{err:?}");
    assert_eq!(
        catalog.live_partition_parts(ht, &day1).await.unwrap().len(),
        2,
        "nothing was tombstoned"
    );
}

/// A collision is reported before the lease is even read, and mutates nothing.
#[tokio::test]
async fn a_collision_is_reported_before_lease_state() {
    let (_pg, catalog) = common::setup().await;
    let ht = setup_hypertable(&catalog).await;
    let day1 = json!({ "day": "2026-07-01" });
    add_l0_part(&catalog, ht, &day1).await;
    add_l0_part(&catalog, ht, &day1).await;
    let old = live_ids(&catalog, ht, &day1).await;

    let identity = op(ht, 1, 0, 0);
    catalog
        .commit(
            ht,
            CommitOp::Add {
                parts: vec![part_meta("unrelated.parquet", 1, 10)],
            },
            Some(&identity),
        )
        .await
        .unwrap();

    // Same key, different intent, and a lease that is not ours either. The
    // collision is the answer: a permanent input error outranks a scheduling
    // one, and the caller must not be told to retry.
    let forged =
        OperationIdentity::from_stored(ht, identity.key().to_string(), [0x11; 32]).unwrap();
    let stale = catalog
        .try_acquire_compaction_lease(ht, &day1, Uuid::new_v4(), TTL)
        .await
        .unwrap()
        .unwrap();
    expire_lease(&catalog, ht, &day1).await;
    catalog
        .try_acquire_compaction_lease(ht, &day1, Uuid::new_v4(), TTL)
        .await
        .unwrap()
        .unwrap();

    let err = catalog
        .commit_compaction_replace(ht, &stale, old, vec![l1_meta("x.parquet", &day1)], &forged)
        .await
        .unwrap_err();
    assert!(
        matches!(err, CatalogError::OperationKeyCollision { .. }),
        "{err:?}"
    );
    assert_eq!(
        catalog.live_partition_parts(ht, &day1).await.unwrap().len(),
        2
    );
}

#[tokio::test]
async fn the_fingerprint_column_rejects_a_wrong_width_digest() {
    let (_pg, catalog) = common::setup().await;
    let ht = setup_hypertable(&catalog).await;
    let err = sqlx::query(
        "INSERT INTO commits (hypertable_id, kind, idempotency_key, operation_fingerprint)
         VALUES ($1, 'add', 'k', $2)",
    )
    .bind(ht.0)
    .bind(vec![0u8; 31])
    .execute(catalog.pool_for_tests())
    .await
    .unwrap_err();
    assert!(
        err.to_string()
            .contains("commits_operation_fingerprint_check"),
        "the 32-byte width is a database invariant, not a convention: {err}"
    );
}

/// Migration 0011 lands on a populated catalog: unkeyed and historical keyed
/// commits both survive, and neither acquires an invented fingerprint.
#[tokio::test]
async fn fingerprint_migration_applies_to_a_populated_catalog() {
    let (_pg, catalog) = common::setup().await;
    let ht = setup_hypertable(&catalog).await;
    catalog
        .commit(
            ht,
            CommitOp::Add {
                parts: vec![part_meta("unkeyed.parquet", 1, 10)],
            },
            None,
        )
        .await
        .unwrap();

    let pool = catalog.pool_for_tests();
    sqlx::query("ALTER TABLE commits DROP COLUMN operation_fingerprint")
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("DELETE FROM _sqlx_migrations WHERE version = 11")
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO commits (hypertable_id, kind, idempotency_key) VALUES ($1, 'add', 'legacy-key')")
        .bind(ht.0)
        .execute(pool)
        .await
        .unwrap();

    catalog.migrate().await.unwrap();

    let fingerprints: Vec<Option<Vec<u8>>> =
        sqlx::query_scalar("SELECT operation_fingerprint FROM commits ORDER BY id")
            .fetch_all(pool)
            .await
            .unwrap();
    assert!(
        fingerprints.iter().all(|f| f.is_none()),
        "history is not backfilled: a made-up digest is false certainty"
    );
    assert_eq!(catalog.live_parts(ht, None).await.unwrap().len(), 1);

    // And the catalog still works afterwards.
    let identity = op(ht, 0, 0, 9);
    assert!(matches!(
        catalog
            .commit(
                ht,
                CommitOp::Add {
                    parts: vec![part_meta("after.parquet", 1, 10)],
                },
                Some(&identity),
            )
            .await
            .unwrap(),
        CommitResult::Committed(_)
    ));
    assert!(matches!(
        catalog.lookup_operation(&identity).await.unwrap(),
        OperationLookup::Committed(_)
    ));
}

// ---------------------------------------------------------------------------
// Plan 43, task 7: the commit-boundary fault seam (test builds only).
// ---------------------------------------------------------------------------

/// The seam has to be *exact*, or S11 proves nothing: `BeforeCommit` must leave
/// the operation absent, `AfterCommitBeforeReturn` must leave it durable, and
/// the caller must not be able to tell them apart.
#[cfg(feature = "fault-injection")]
#[tokio::test]
async fn the_commit_fault_seam_is_one_shot_instance_scoped_and_exact() {
    use ukiel_catalog::CommitFault;

    let (_pg, catalog) = common::setup().await;
    let ht = setup_hypertable(&catalog).await;

    let add = |catalog: ukiel_catalog::PostgresCatalog,
               path: &'static str,
               id: OperationIdentity| async move {
        catalog
            .commit(
                ht,
                CommitOp::Add {
                    parts: vec![part_meta(path, 1, 10)],
                },
                Some(&id),
            )
            .await
    };

    // --- Before the commit: rolled back, and the caller cannot tell. ---------
    let absent = op(ht, 0, 0, 9);
    catalog.arm_commit_fault(CommitFault::BeforeCommit);
    let err = add(catalog.clone(), "never.parquet", absent.clone())
        .await
        .unwrap_err();
    assert!(
        matches!(err, CatalogError::AmbiguousMutation { .. }),
        "the caller sees an unknown outcome, not a rollback: {err:?}"
    );
    assert_eq!(err.class(), CatalogErrorClass::Transport);
    assert_eq!(
        catalog.lookup_operation(&absent).await.unwrap(),
        OperationLookup::Absent,
        "it really did roll back"
    );
    assert!(catalog.live_parts(ht, None).await.unwrap().is_empty());

    // One-shot: the next commit is unaffected. A fault that fired every time
    // would model a broken database, not a lost acknowledgement.
    assert!(matches!(
        add(catalog.clone(), "fine.parquet", op(ht, 0, 10, 19))
            .await
            .unwrap(),
        CommitResult::Committed(_)
    ));

    // --- After the commit: durable, and the caller *still* cannot tell. ------
    let durable = op(ht, 0, 20, 29);
    catalog.arm_commit_fault(CommitFault::AfterCommitBeforeReturn);
    let err = add(catalog.clone(), "landed.parquet", durable.clone())
        .await
        .unwrap_err();
    assert!(
        matches!(err, CatalogError::AmbiguousMutation { .. }),
        "byte-for-byte the same failure as the rolled-back one: {err:?}"
    );
    assert!(
        matches!(
            catalog.lookup_operation(&durable).await.unwrap(),
            OperationLookup::Committed(_)
        ),
        "the transaction is durable — only the catalog knows"
    );
    let live = catalog.live_parts(ht, None).await.unwrap();
    assert_eq!(live.len(), 2);
    assert!(live.iter().any(|p| p.meta.path == "landed.parquet"));

    // Instance-scoped: arming one handle does not arm an unrelated catalog.
    let (_pg2, other) = common::setup().await;
    let other_ht = setup_hypertable(&other).await;
    catalog.arm_commit_fault(CommitFault::BeforeCommit);
    assert!(matches!(
        other
            .commit(
                other_ht,
                CommitOp::Add {
                    parts: vec![part_meta("other.parquet", 1, 10)],
                },
                Some(&op(other_ht, 0, 0, 9)),
            )
            .await
            .unwrap(),
        CommitResult::Committed(_),
        // (and the arming above is still pending on `catalog`, unspent)
    ));
}

/// An unidentified mutation at the same boundary is undecidable, and says so.
#[cfg(feature = "fault-injection")]
#[tokio::test]
async fn an_unidentified_mutation_at_the_boundary_is_loud_and_permanent() {
    use ukiel_catalog::CommitFault;

    let (_pg, catalog) = common::setup().await;
    let ht = setup_hypertable(&catalog).await;
    catalog.arm_commit_fault(CommitFault::AfterCommitBeforeReturn);
    let err = catalog
        .commit(
            ht,
            CommitOp::Add {
                parts: vec![part_meta("orphan.parquet", 1, 10)],
            },
            None,
        )
        .await
        .unwrap_err();
    assert!(
        matches!(err, CatalogError::UnidentifiedAmbiguousMutation { .. }),
        "{err:?}"
    );
    assert_eq!(err.class(), CatalogErrorClass::PermanentDatabase);
    assert!(
        !err.is_recoverable_transport(),
        "there is nothing to reconcile, so retrying is guessing"
    );
    // It did commit — the point is that nobody can prove it from here.
    assert_eq!(catalog.live_parts(ht, None).await.unwrap().len(), 1);
}

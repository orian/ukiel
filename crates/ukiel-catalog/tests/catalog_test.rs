mod common;

use serde_json::json;
use ukiel_core::{CommitOp, CommitResult, HypertableId, NamespaceId, PartMeta};

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
    catalog
        .create_hypertable(
            "events",
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

    let key = "0:0-99"; // kafka partition 0, offsets 0..=99
    let first = catalog
        .commit(
            ht,
            CommitOp::Add {
                parts: vec![part_meta("p1.parquet", 1, 10)],
            },
            Some(key),
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
            Some(key),
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
            Some("0:100-199"),
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
    catalog
        .commit_with_offsets(
            ht,
            CommitOp::Add {
                parts: vec![part_meta("p1.parquet", 1, 10)],
            },
            &[
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
            ],
        )
        .await
        .unwrap();

    let offsets = catalog.ingest_offsets(ht, "events").await.unwrap();
    assert_eq!(offsets.get(&0), Some(&42));
    assert_eq!(offsets.get(&1), Some(&10));

    // A conflicting commit must NOT advance offsets (same transaction).
    let bogus_old = vec![ukiel_core::PartId(999_999)];
    let err = catalog
        .commit_with_offsets(
            ht,
            CommitOp::Replace {
                old: bogus_old,
                new: vec![part_meta("p2.parquet", 1, 10)],
            },
            &[OffsetRange {
                topic: "events".into(),
                partition: 0,
                first: 42,
                last: 99,
            }],
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

    // A later flush upserts over the existing row.
    catalog
        .commit_with_offsets(
            ht,
            CommitOp::Add {
                parts: vec![part_meta("p3.parquet", 1, 10)],
            },
            &[OffsetRange {
                topic: "events".into(),
                partition: 0,
                first: 42,
                last: 99,
            }],
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

    // Purging stamps the row: no longer reapable, but path stays known and
    // the change feed still replays the original add.
    catalog.mark_purged(&[reapable[0].id]).await.unwrap();
    assert!(catalog.reapable_parts(ht, 0.0).await.unwrap().is_empty());
    let paths = catalog.all_part_paths(ht).await.unwrap();
    assert!(paths.contains(&"old.parquet".to_string()));
    assert!(paths.contains(&"new.parquet".to_string()));
    let events = catalog.changes_since(ht, CommitId(0), 100).await.unwrap();
    assert_eq!(
        events[0].added.len(),
        1,
        "purged part must still appear in feed replay"
    );
}

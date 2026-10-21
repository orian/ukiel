//! MV outputs may differ across attempts. The source identity selects one
//! durable outcome, including an empty result or configured skip.
mod common;

use serde_json::json;
use ukiel_catalog::{OperationLookup, PostgresCatalog};
use ukiel_core::{
    CommitId, CommitOp, CommitResult, HypertableId, MvIntent, OperationIdentity, PartMeta,
    PipelineId,
};

async fn table(catalog: &PostgresCatalog, name: &str) -> HypertableId {
    catalog
        .create_hypertable(
            name,
            &json!({"fields": [{"name": "tenant_id", "type": "int64"}]}),
            &json!({"columns": []}),
            &["tenant_id".into()],
            "tenant_id",
        )
        .await
        .unwrap()
}

fn part(path: &str, rows: i64) -> PartMeta {
    PartMeta {
        path: path.into(),
        partition_values: json!({}),
        packing_key_min: 1,
        packing_key_max: 1,
        row_count: rows,
        size_bytes: rows * 100,
        level: 0,
        column_stats: None,
    }
}

fn identity(target: HypertableId, source: HypertableId, event: CommitId) -> OperationIdentity {
    OperationIdentity::mv(
        target,
        MvIntent {
            pipeline_id: PipelineId(7),
            source_hypertable_id: source,
            source_commit_id: event,
            transformation_version: 1,
        },
    )
    .unwrap()
}

async fn source_event(catalog: &PostgresCatalog, source: HypertableId) -> CommitId {
    catalog
        .commit(source, CommitOp::Add { parts: vec![] }, None)
        .await
        .unwrap()
        .commit_id()
}

#[tokio::test]
async fn first_mv_outcome_survives_different_replays_and_cursor_advancement() {
    let (_pg, catalog) = common::setup().await;
    let source = table(&catalog, "source").await;
    for (name, original, replacement) in [
        ("empty_first", vec![], vec![part("late.parquet", 2)]),
        ("rows_first", vec![part("winner.parquet", 3)], vec![]),
        (
            "different_rows",
            vec![part("first.parquet", 4)],
            vec![part("different_rows_and_clock.parquet", 5)],
        ),
    ] {
        let target = table(&catalog, name).await;
        let event = source_event(&catalog, source).await;
        let id = identity(target, source, event);
        let first = catalog
            .commit(
                target,
                CommitOp::Add {
                    parts: original.clone(),
                },
                Some(&id),
            )
            .await
            .unwrap();
        assert!(matches!(first, CommitResult::Committed(_)));
        catalog.set_cursor("mv/7", source, event).await.unwrap();
        let late = catalog
            .commit(target, CommitOp::Add { parts: replacement }, Some(&id))
            .await
            .unwrap();
        assert_eq!(late, CommitResult::AlreadyApplied(first.commit_id()));
        assert_eq!(
            catalog.lookup_operation(&id).await.unwrap(),
            OperationLookup::Committed(first.commit_id())
        );
        let live = catalog.live_parts(target, None).await.unwrap();
        assert_eq!(
            live.iter().map(|p| p.meta.clone()).collect::<Vec<_>>(),
            original
        );
        let feed = catalog
            .changes_since(target, CommitId(0), 10)
            .await
            .unwrap();
        assert_eq!(
            feed.len(),
            1,
            "empty completion is also one durable feed event"
        );
        assert_eq!(feed[0].added.len(), original.len());
        assert_eq!(catalog.get_cursor("mv/7", source).await.unwrap(), event);
    }
}

#[tokio::test]
async fn concurrent_empty_and_nonempty_mv_attempts_publish_one_outcome() {
    let (_pg, catalog) = common::setup().await;
    let source = table(&catalog, "source").await;
    let target = table(&catalog, "target").await;
    let event = source_event(&catalog, source).await;
    let id = identity(target, source, event);
    let (empty, rows, other_rows) = tokio::join!(
        catalog.commit(target, CommitOp::Add { parts: vec![] }, Some(&id)),
        catalog.commit(
            target,
            CommitOp::Add {
                parts: vec![part("a.parquet", 2)]
            },
            Some(&id)
        ),
        catalog.commit(
            target,
            CommitOp::Add {
                parts: vec![part("b.parquet", 3)]
            },
            Some(&id)
        ),
    );
    let outcomes = [empty.unwrap(), rows.unwrap(), other_rows.unwrap()];
    assert_eq!(
        outcomes
            .iter()
            .filter(|x| matches!(x, CommitResult::Committed(_)))
            .count(),
        1
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|x| matches!(x, CommitResult::AlreadyApplied(_)))
            .count(),
        2
    );
    let commit = outcomes[0].commit_id();
    assert!(outcomes.iter().all(|x| x.commit_id() == commit));
    let live = catalog.live_parts(target, None).await.unwrap();
    let winner = outcomes
        .iter()
        .position(|x| matches!(x, CommitResult::Committed(_)))
        .unwrap();
    if winner == 0 {
        assert!(live.is_empty());
    } else {
        assert_eq!(live.len(), 1);
        assert_eq!(
            live[0].meta.path,
            if winner == 1 {
                "a.parquet"
            } else {
                "b.parquet"
            }
        );
    }
    assert_eq!(
        catalog
            .changes_since(target, CommitId(0), 10)
            .await
            .unwrap()
            .len(),
        1
    );
}

#[cfg(feature = "fault-injection")]
#[tokio::test]
async fn lost_mv_acknowledgements_preserve_the_actual_empty_or_nonempty_outcome() {
    use ukiel_catalog::{CatalogError, CommitFault};
    let (_pg, catalog) = common::setup().await;
    let source = table(&catalog, "source").await;
    let target = table(&catalog, "target").await;
    for (fault, original_empty) in [
        (CommitFault::BeforeCommit, true),
        (CommitFault::BeforeCommit, false),
        (CommitFault::AfterCommitBeforeReturn, true),
        (CommitFault::AfterCommitBeforeReturn, false),
    ] {
        let event = source_event(&catalog, source).await;
        let id = identity(target, source, event);
        let first_parts = if original_empty {
            vec![]
        } else {
            vec![part(&format!("original-{}.parquet", event.0), 2)]
        };
        catalog.arm_commit_fault(fault);
        let err = catalog
            .commit(
                target,
                CommitOp::Add {
                    parts: first_parts.clone(),
                },
                Some(&id),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, CatalogError::AmbiguousMutation { .. }));
        let lookup = catalog.lookup_operation(&id).await.unwrap();
        let retry = catalog
            .commit(
                target,
                CommitOp::Add {
                    parts: vec![part(&format!("retry-{}.parquet", event.0), 9)],
                },
                Some(&id),
            )
            .await
            .unwrap();
        match lookup {
            OperationLookup::Absent => assert!(matches!(retry, CommitResult::Committed(_))),
            OperationLookup::Committed(commit) => {
                assert_eq!(retry, CommitResult::AlreadyApplied(commit));
                let feed = catalog
                    .changes_since(target, CommitId(0), 100)
                    .await
                    .unwrap();
                let outcome = feed.iter().find(|x| x.commit_id == commit).unwrap();
                assert_eq!(
                    outcome
                        .added
                        .iter()
                        .map(|p| p.meta.clone())
                        .collect::<Vec<_>>(),
                    first_parts
                );
            }
        }
    }
}

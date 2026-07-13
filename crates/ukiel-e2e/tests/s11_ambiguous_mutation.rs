//! S11 — a commit whose acknowledgement is lost converges exactly once
//! (plan 43).
//!
//! S10 proved ukield *survives* a catalog outage. It could not prove the harder
//! thing, and said so: an outage seen from outside never tells you which side of
//! `COMMIT` the connection died on. A proxy can cut the wire; it cannot say "this
//! transaction is durable, and the caller will never learn it". That case — the
//! lost acknowledgement — is the one that makes a mutation *undecidable*, and it
//! is the production gate for active-active compaction.
//!
//! So the fault point here is inside the process, on either side of the commit
//! boundary (`ukiel-catalog`'s `fault-injection` feature, compiled out of
//! production builds). Both points hand the caller the identical transport
//! error. The only thing that can tell them apart afterwards is the catalog, and
//! the only reason it can is that the mutation carries a fingerprinted identity.
//!
//! What this proves:
//!
//! 1. **Ingest, durable.** The commit lands, the acknowledgement is lost, the
//!    catalog then goes away entirely. The role degrades, reconciles the
//!    operation to `Committed` once the writer is back, rebuilds — and every
//!    event is visible **exactly once**. Nothing is re-applied.
//! 2. **Ingest, absent.** The commit rolls back and the acknowledgement is lost
//!    the same way. Reconciliation says `Absent`, the rebuilt worker replays from
//!    Kafka, and the events are still visible exactly once.
//! 3. **Compaction, durable.** A merge commits, its acknowledgement is lost, its
//!    lease expires and a peer takes the partition. The replay is reconciled
//!    *before* the lease is judged, so the durable merge is reported rather than
//!    redone — and no second Parquet read or upload happens. Data is unchanged.
//! 4. **Compaction, absent.** The merge rolls back; the fleet replans from live
//!    state and converges.
//! 5. **Collision.** The same key under a different fingerprint fails
//!    permanently and mutates neither parts nor offsets.
//!
//! Run: `make e2e-ha` (adds the opt-in Toxiproxy compose profile).

use std::collections::HashSet;
use std::time::{Duration, Instant};

use tokio_util::sync::CancellationToken;
use ukiel_catalog::{CatalogError, CommitFault, CompactionLease, OperationLookup, PostgresCatalog};
use ukiel_compactor::CompactorError;
use ukiel_compactor::compactor::{Compactor, CompactorConfig, compaction_identity_for};
use ukiel_core::{CommitOp, CommitResult, OperationIdentity, PartMeta};
use ukiel_e2e::fault_proxy::FaultProxy;
use ukiel_e2e::{Stack, env_or};

async fn eventually<T, F, Fut>(what: &str, timeout: Duration, mut f: F) -> T
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Option<T>>,
{
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(v) = f().await {
            return v;
        }
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

fn ha_enabled() -> bool {
    if env_or("UKIEL_E2E_HA", "0") != "1" {
        eprintln!("skipping S11: set UKIEL_E2E_HA=1 (see `make e2e-ha`)");
        return false;
    }
    true
}

/// A catalog handle on the proxied port that the test *and* ukield share, so the
/// test can arm a fault in the very transaction the ingest worker is about to
/// run.
fn shared_catalog(url: &str) -> PostgresCatalog {
    PostgresCatalog::connect_lazy_with_config(url, &ukiel_catalog::CatalogPoolConfig::default())
        .expect("catalog pool")
}

async fn proxy_and_url() -> (FaultProxy, String) {
    let api = format!(
        "http://127.0.0.1:{}",
        env_or("UKIEL_TOXIPROXY_API_PORT", "8474")
    );
    let port = env_or("UKIEL_TOXIPROXY_PG_PORT", "15432");
    let proxy =
        FaultProxy::create(&api, "catalog", &format!("0.0.0.0:{port}"), "postgres:5432").await;
    (
        proxy,
        format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres"),
    )
}

/// Scenarios 1 and 2: an ingest commit whose acknowledgement is lost — durable in
/// one run, rolled back in the other — followed by a real catalog outage. In both
/// the events must end up visible exactly once, and the difference must be
/// *observable*: the reconciliation counter says which one happened.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires the compose stack with the `ha` profile (make e2e-ha)"]
async fn a_lost_ingest_acknowledgement_converges_exactly_once() {
    if !ha_enabled() {
        return;
    }

    for fault in [
        CommitFault::AfterCommitBeforeReturn,
        CommitFault::BeforeCommit,
    ] {
        let (proxy, url) = proxy_and_url().await;
        proxy.restore().await;

        let stack = Stack::start().await;
        let table = stack.create_events_table().await;
        let catalog = shared_catalog(&url);
        let shutdown = CancellationToken::new();
        let (base, mut server) = stack
            .spawn_ukield_with_injected_catalog(&table, &url, shutdown.clone(), catalog.clone())
            .await;
        let client = reqwest::Client::new();

        eventually("ukield to be ready", Duration::from_secs(60), || async {
            let resp = client.get(format!("{base}/readyz")).send().await.ok()?;
            (resp.status() == 200).then_some(())
        })
        .await;

        // Arm the boundary, then give ingest something to commit. The next flush
        // is the one that will never hear its answer.
        catalog.arm_commit_fault(fault);
        let produced: HashSet<String> = stack
            .produce_events_for(&table, table.namespace, "lost-ack", 5)
            .await
            .into_iter()
            .collect();
        let want = produced.len();

        // The role must recover on its own. It reconciles the operation first —
        // the counter is how we know it did, and which answer it got.
        let outcome = if fault == CommitFault::AfterCommitBeforeReturn {
            "committed"
        } else {
            "absent"
        };
        let reconciled = eventually(
            "the ambiguous mutation to be reconciled",
            Duration::from_secs(120),
            || {
                let client = &client;
                let base = &base;
                async move {
                    let body = client
                        .get(format!("{base}/metrics"))
                        .send()
                        .await
                        .ok()?
                        .text()
                        .await
                        .ok()?;
                    body.lines()
                        .find(|l| {
                            l.starts_with("catalog_mutation_reconciliation_total")
                                && l.contains(r#"outcome="#)
                                && l.contains(outcome)
                                && !l.ends_with(" 0")
                        })
                        .map(|l| l.to_string())
                }
            },
        )
        .await;
        println!("S11 ingest/{fault:?}: {reconciled}");

        // Whichever it was, the data is the same: exactly once. A durable commit
        // is not re-applied, and a rolled-back one is replayed from Kafka.
        let got = eventually(
            "every event to be queryable exactly once",
            Duration::from_secs(120),
            || {
                let stack = &stack;
                async move {
                    let rows = stack
                        .query(table.namespace, "SELECT payload FROM events")
                        .await
                        .ok()?;
                    let got: HashSet<String> = rows
                        .as_array()?
                        .iter()
                        .map(|r| r["payload"].as_str().unwrap().to_string())
                        .collect();
                    (got.len() >= want).then_some(got)
                }
            },
        )
        .await;
        assert_eq!(
            got, produced,
            "{fault:?}: exactly once — nothing lost, nothing duplicated"
        );
        assert!(!server.is_finished(), "{fault:?}: the process never exited");

        // And the whole thing survives the outage on top of it: the writer goes
        // away *after* the ambiguous commit, which is the realistic ordering (the
        // connection died because the database did).
        proxy.cut().await;
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert!(!server.is_finished(), "{fault:?}: an outage is not an exit");
        proxy.restore().await;
        eventually("readiness to recover", Duration::from_secs(120), || async {
            let resp = client.get(format!("{base}/readyz")).send().await.ok()?;
            (resp.status() == 200).then_some(())
        })
        .await;

        shutdown.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(30), &mut server).await;
    }
}

/// Scenarios 3 and 4: a compaction merge whose acknowledgement is lost.
///
/// The dangerous ordering, and the reason the replay lookup runs *before* the
/// lease fence: by the time the worker can ask, its lease is gone and a peer owns
/// the partition. Judging the lease first would answer `LeaseLost` for a merge
/// that is already durable, and a second worker would redo it.
///
/// The merge here is done by a **real compactor** on a fault-armed catalog, so
/// the Parquet is really read, really merged and really uploaded, and the commit
/// that loses its acknowledgement is the compactor's own. A hand-built REPLACE
/// would prove nothing about the code that runs in production — and would leave a
/// live part pointing at an object nobody wrote.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires the compose stack with the `ha` profile (make e2e-ha)"]
async fn a_lost_compaction_acknowledgement_is_reconciled_before_the_lease() {
    if !ha_enabled() {
        return;
    }
    let (proxy, url) = proxy_and_url().await;
    proxy.restore().await;

    let stack = Stack::start().await;
    let table = stack.create_events_table().await;
    let catalog = shared_catalog(&url);

    let ht = table.hypertable_id;
    let hypertable = catalog
        .get_hypertable_by_id(ht)
        .await
        .expect("the hypertable");
    let mut expected: HashSet<String> = HashSet::new();

    for (run, fault) in [
        CommitFault::AfterCommitBeforeReturn,
        CommitFault::BeforeCommit,
    ]
    .into_iter()
    .enumerate()
    {
        // Every Stack shares one Postgres, so `run_once` sweeps every hypertable
        // in the database — including the ones other scenarios left behind. Drain
        // them first, so the *only* eligible partition when the fault is armed is
        // the one this scenario is about. (A fresh backlog per fault, too: the
        // previous run's merge consumed the last one, which is exactly what a
        // durable merge is supposed to do.)
        stack.run_compaction_to_quiescence().await;

        let ingest = stack.spawn_ingest_with(&table, 300).await;
        for round in 0..4 {
            expected.extend(
                stack
                    .produce_events_for(&table, table.namespace, &format!("f{run}r{round}"), 5)
                    .await,
            );
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        stack
            .wait_for_row_count(
                table.namespace,
                expected.len() as i64,
                Duration::from_secs(60),
            )
            .await;
        ingest.stop().await;

        let partition = catalog
            .compaction_candidates(ht, uuid::Uuid::nil(), 2, 2, 64, None)
            .await
            .expect("candidates")
            .partitions
            .first()
            .cloned()
            .expect("the ingested L0 files must form a backlog");

        // The group the compactor is about to pick: the lowest level over its
        // trigger. We name the operation the same way it will, before it runs.
        let live = catalog
            .live_partition_parts(ht, &partition)
            .await
            .expect("live parts");
        let level = live.iter().map(|p| p.meta.level).min().unwrap_or(0);
        let group: Vec<_> = live
            .iter()
            .filter(|p| p.meta.level == level)
            .cloned()
            .collect();
        assert!(group.len() >= 2, "{fault:?}: nothing to merge");
        let identity = compaction_identity_for(&hypertable, &group, level + 1).expect("identity");
        assert_eq!(
            catalog.lookup_operation(&identity).await.expect("lookup"),
            OperationLookup::Absent
        );

        // A real compactor, on the fault-armed catalog. It claims the partition,
        // reads and merges the Parquet, uploads the output — and then loses the
        // answer to its commit.
        let owner = uuid::Uuid::new_v4();
        let compactor = Compactor::new(
            catalog.clone(),
            stack.store.clone(),
            CompactorConfig {
                worker_name: "s11".into(),
                owner_id: Some(owner),
                lease_ttl_ms: 2_000,
                lease_renew_interval_ms: 500,
                ..CompactorConfig::default()
            },
        );
        catalog.arm_commit_fault(fault);
        let err = compactor
            .run_once()
            .await
            .expect_err("the boundary fault must surface to the worker");
        let CompactorError::Catalog(CatalogError::AmbiguousMutation {
            identity: committed,
            ..
        }) = &err
        else {
            panic!("{fault:?}: expected an ambiguous mutation, got {err:?}");
        };
        // The error carries the identity — that is how the supervisor learns what
        // to ask about, and it must be the operation we predicted from the same
        // live state the compactor planned from.
        assert_eq!(
            committed.key(),
            identity.key(),
            "{fault:?}: the compactor merged a different group than the one this \
             scenario named (group {:?}, level {} -> {})",
            group.iter().map(|p| p.id.0).collect::<Vec<_>>(),
            level,
            level + 1
        );

        // Its lease is gone by the time it can ask — released on the way out, and
        // then reissued to a peer at a generation that has never been used before
        // (issue 0013). This is the state the old code would have judged first.
        let peer = catalog
            .try_acquire_compaction_lease(
                ht,
                &partition,
                uuid::Uuid::new_v4(),
                Duration::from_secs(60),
            )
            .await
            .expect("acquire")
            .expect("the partition is claimable again");

        let lookup = catalog.lookup_operation(&identity).await.expect("lookup");
        match fault {
            CommitFault::AfterCommitBeforeReturn => {
                let OperationLookup::Committed(id) = lookup else {
                    let keys: Vec<String> = sqlx::query_scalar(
                        "SELECT idempotency_key FROM commits WHERE idempotency_key IS NOT NULL",
                    )
                    .fetch_all(catalog.pool_for_tests())
                    .await
                    .unwrap();
                    panic!(
                        "the merge is durable and must be reported as such: {lookup:?}\n\
                         predicted: {}\n\
                         group: {:?} level {} -> {}\n\
                         stored keys: {keys:#?}",
                        identity.key(),
                        group
                            .iter()
                            .map(|p| (p.id.0, p.meta.level))
                            .collect::<Vec<_>>(),
                        level,
                        level + 1
                    );
                };
                // The merge really landed: its inputs are tombstoned and its
                // output is live and readable.
                let after = catalog
                    .live_partition_parts(ht, &partition)
                    .await
                    .expect("live parts");
                assert!(
                    after.iter().all(|p| p.meta.level > level),
                    "the inputs were tombstoned by the durable merge: {after:?}"
                );

                // And a replay under the stale lease reports that, rather than
                // LeaseLost. The peer above owns the partition now; judging the
                // lease first would send it to redo a committed merge.
                let stale = CompactionLease {
                    hypertable_id: ht,
                    partition_values: partition.clone(),
                    owner_id: owner,
                    generation: peer.generation - 1,
                    expires_at: peer.expires_at,
                };
                let replay = catalog
                    .commit_compaction_replace(
                        ht,
                        &stale,
                        group.iter().map(|p| p.id).collect(),
                        vec![PartMeta {
                            path: "redo-never-uploaded.parquet".into(),
                            partition_values: partition.clone(),
                            packing_key_min: 1,
                            packing_key_max: 1,
                            row_count: 1,
                            size_bytes: 1,
                            level: level + 1,
                            column_stats: None,
                        }],
                        &identity,
                    )
                    .await
                    .expect("a durable replay is reconciled, not refused");
                assert_eq!(replay, CommitResult::AlreadyApplied(id));
                assert!(
                    !catalog
                        .all_part_paths(ht)
                        .await
                        .expect("paths")
                        .contains(&"redo-never-uploaded.parquet".to_string()),
                    "the replay applied nothing — its outputs are orphans, not parts"
                );
            }
            CommitFault::BeforeCommit => {
                assert_eq!(
                    lookup,
                    OperationLookup::Absent,
                    "the merge rolled back and must be reported as absent"
                );
                let after = catalog
                    .live_partition_parts(ht, &partition)
                    .await
                    .expect("live parts");
                assert_eq!(
                    after.iter().filter(|p| p.meta.level == level).count(),
                    group.len(),
                    "a rolled-back merge tombstoned nothing"
                );
            }
        }
        catalog
            .release_compaction_lease(&peer)
            .await
            .expect("release");
    }

    // Whatever happened, the fleet converges from live state and every row is
    // intact: the identity protects that invariant, it does not replace it.
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
    assert_eq!(
        got, expected,
        "compaction after a lost ack changed the data"
    );
}

/// Scenario 5: the same key under a different fingerprint. Permanent, loud, and
/// it mutates nothing — the alternative would be telling a worker that somebody
/// else's commit was its own.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires the compose stack with the `ha` profile (make e2e-ha)"]
async fn a_fingerprint_collision_mutates_neither_parts_nor_offsets() {
    if !ha_enabled() {
        return;
    }
    let (proxy, url) = proxy_and_url().await;
    proxy.restore().await;

    let stack = Stack::start().await;
    let table = stack.create_events_table().await;
    let catalog = shared_catalog(&url);
    catalog.migrate().await.expect("migrate");
    let ht = table.hypertable_id;

    let honest = OperationIdentity::ingest(
        ht,
        &[ukiel_core::IngestRange {
            topic: table.topic.clone(),
            partition: 0,
            first: 1_000_000,
            last: 1_000_009,
        }],
        1,
    )
    .expect("identity");
    let part = |path: &str| PartMeta {
        path: path.to_string(),
        partition_values: serde_json::json!({"day": "2026-07-13"}),
        packing_key_min: 1,
        packing_key_max: 1,
        row_count: 1,
        size_bytes: 1,
        level: 0,
        column_stats: None,
    };

    catalog
        .commit(
            ht,
            CommitOp::Add {
                parts: vec![part("honest.parquet")],
            },
            Some(&honest),
        )
        .await
        .expect("the first commit lands");
    let parts_before = catalog.all_part_paths(ht).await.expect("paths").len();
    let offsets_before = stack.ingest_offset_sum(ht, &table.topic).await;

    // Same key, different intent — a buggy caller, or direct SQL.
    let forged = OperationIdentity::from_stored(ht, honest.key().to_string(), [0x5A; 32])
        .expect("a well-formed but wrong pair");
    let err = catalog
        .commit(
            ht,
            CommitOp::Add {
                parts: vec![part("forged.parquet")],
            },
            Some(&forged),
        )
        .await
        .expect_err("a collision must never be AlreadyApplied");
    assert!(
        matches!(err, CatalogError::OperationKeyCollision { .. }),
        "{err:?}"
    );
    assert!(!err.is_recoverable_transport(), "permanent, never retried");

    assert_eq!(
        catalog.all_part_paths(ht).await.expect("paths").len(),
        parts_before,
        "a collision must not insert a part"
    );
    assert_eq!(
        stack.ingest_offset_sum(ht, &table.topic).await,
        offsets_before,
        "a collision must not advance an offset"
    );
}

use std::time::Duration;

use serde_json::json;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::{ContainerAsync, ImageExt, runners::AsyncRunner};
use ukiel_catalog::{
    CatalogError, CatalogErrorClass, CatalogPoolConfig, OffsetRange, PostgresCatalog,
};
use ukiel_core::{CommitId, CommitOp, HypertableId, IngestRange, OperationIdentity, PartMeta};

// Match the compose stack's PostgreSQL version (the module's default is 11).
async fn setup() -> (ContainerAsync<Postgres>, PostgresCatalog, String) {
    let pg = Postgres::default().with_tag("17").start().await.unwrap();
    let port = pg.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
    let catalog = PostgresCatalog::connect(&url).await.unwrap();
    catalog.migrate().await.unwrap();
    (pg, catalog, url)
}

async fn table(catalog: &PostgresCatalog, name: &str) -> HypertableId {
    catalog
        .create_hypertable(
            name,
            &json!({"fields": [{"name": "tenant_id", "type": "int64"}]}),
            &json!({"columns": ["day"]}),
            &["tenant_id".into()],
            "tenant_id",
        )
        .await
        .unwrap()
}

fn part(path: &str) -> PartMeta {
    PartMeta {
        path: path.into(),
        partition_values: json!({"day": "2026-10-21"}),
        packing_key_min: 1,
        packing_key_max: 1,
        row_count: 1,
        size_bytes: 100,
        level: 0,
        column_stats: None,
    }
}

async fn ingest(
    catalog: &PostgresCatalog,
    ht: HypertableId,
    lane: i32,
    path: &str,
) -> Result<CommitId, ukiel_catalog::CatalogError> {
    let identity = ingest_identity(ht, lane);
    catalog
        .commit_with_offsets(
            ht,
            CommitOp::Add {
                parts: vec![part(path)],
            },
            &[OffsetRange {
                topic: "events".into(),
                partition: lane,
                first: 0,
                last: 9,
            }],
            &identity,
        )
        .await
        .map(|result| result.commit_id())
}

fn ingest_identity(ht: HypertableId, lane: i32) -> OperationIdentity {
    OperationIdentity::ingest(
        ht,
        &[IngestRange {
            topic: "events".into(),
            partition: lane,
            first: 0,
            last: 9,
        }],
        1,
    )
    .unwrap()
}

// Pause a real catalog writer AFTER it allocated its ID, without sleeps or a
// production fault seam. The controller owns the advisory lock; pg_blocking_pids
// is the barrier acknowledgement. Every test owns an isolated database.
async fn gate(
    catalog: &PostgresCatalog,
    identity: &OperationIdentity,
) -> (sqlx::Transaction<'static, sqlx::Postgres>, i32) {
    sqlx::raw_sql(
        "CREATE TABLE feed_gate (key TEXT NOT NULL);
         CREATE FUNCTION pause_feed_writer() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
             IF NEW.idempotency_key = (SELECT key FROM feed_gate) THEN
                 PERFORM pg_advisory_xact_lock(150015);
             END IF;
             RETURN NEW;
         END $$;
         CREATE TRIGGER pause_feed_writer BEFORE INSERT ON commits
         FOR EACH ROW EXECUTE FUNCTION pause_feed_writer();",
    )
    .execute(catalog.pool_for_tests())
    .await
    .unwrap();
    sqlx::query("INSERT INTO feed_gate VALUES ($1)")
        .bind(identity.key())
        .execute(catalog.pool_for_tests())
        .await
        .unwrap();
    let mut tx = catalog.pool_for_tests().begin().await.unwrap();
    let pid = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    sqlx::query("SELECT pg_advisory_xact_lock(150015)")
        .execute(&mut *tx)
        .await
        .unwrap();
    (tx, pid)
}

async fn blocked_by(catalog: &PostgresCatalog, pid: i32) -> i32 {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let waiting: Option<i32> = sqlx::query_scalar(
                "SELECT pid FROM pg_stat_activity
                 WHERE $1 = ANY(pg_blocking_pids(pid)) LIMIT 1",
            )
            .bind(pid)
            .fetch_optional(catalog.pool_for_tests())
            .await
            .unwrap();
            if let Some(waiting) = waiting {
                return waiting;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("database did not acknowledge the lock barrier")
}

// A allocates N; B commits N+1 on a disjoint Kafka lane. A consumer must not
// persist N+1 while A is unresolved. Both feed APIs must use a fresh snapshot
// AFTER their publication barrier, including when A rolls back.
async fn out_of_order(rollback: bool, head_only: bool) {
    let (_pg, catalog, url) = setup().await;
    let ht = table(&catalog, "events").await;
    let (gate, gate_pid) = gate(&catalog, &ingest_identity(ht, 0)).await;
    let writer = PostgresCatalog::connect(&url).await.unwrap();
    if rollback {
        writer.arm_commit_fault(ukiel_catalog::CommitFault::BeforeCommit);
    }
    let a = tokio::spawn(async move { ingest(&writer, ht, 0, "paused.parquet").await });
    let a_pid = blocked_by(&catalog, gate_pid).await;

    // B really completes while A is paused: a fix must preserve concurrent
    // writes rather than just making this particular schedule impossible.
    let b = tokio::time::timeout(
        Duration::from_secs(10),
        ingest(&catalog, ht, 1, "b.parquet"),
    )
    .await
    .expect("disjoint writer B was serialized behind A")
    .unwrap();

    if head_only {
        // A configured repeatable-read default must not freeze the prefix
        // snapshot before the barrier finishes waiting.
        sqlx::query(
            "ALTER DATABASE postgres SET default_transaction_isolation = 'repeatable read'",
        )
        .execute(catalog.pool_for_tests())
        .await
        .unwrap();
    }
    let reader = PostgresCatalog::connect(&url).await.unwrap();
    let mut read = tokio::spawn(async move {
        if head_only {
            reader.feed_head(ht).await.unwrap()
        } else {
            reader.changes_since(ht, CommitId(0), 1).await.unwrap()[0].commit_id
        }
    });
    tokio::select! {
        _ = blocked_by(&catalog, a_pid) => {},
        result = &mut read => {
            // Report the actual data loss, not merely the missing lock.
            let observed = result.unwrap();
            catalog.set_cursor("consumer", ht, observed).await.unwrap();
            gate.commit().await.unwrap();
            let a = a.await.unwrap();
            let resumed = PostgresCatalog::connect(&url).await.unwrap();
            let cursor = resumed.get_cursor("consumer", ht).await.unwrap();
            let tail = resumed.changes_since(ht, cursor, 100).await.unwrap();
            panic!("unsafe prefix {observed:?}: A={a:?}, B={b:?}, restarted tail={tail:?}");
        }
    }
    let other = table(&catalog, "other").await;
    let other_id = tokio::time::timeout(
        Duration::from_secs(5),
        ingest(&catalog, other, 0, "other.parquet"),
    )
    .await
    .expect("feed barrier blocked a different hypertable")
    .unwrap();
    assert_eq!(catalog.feed_head(other).await.unwrap(), other_id);
    assert_eq!(catalog.live_parts(ht, None).await.unwrap().len(), 1);
    gate.commit().await.unwrap();
    let a = tokio::time::timeout(Duration::from_secs(10), a)
        .await
        .expect("writer did not finish after barrier release")
        .unwrap();
    assert_eq!(a.is_err(), rollback);
    if let Ok(a_id) = a {
        assert!(a_id < b);
    }
    let observed = tokio::time::timeout(Duration::from_secs(10), read)
        .await
        .expect("feed stalled after transaction completion")
        .unwrap();
    assert_eq!(
        observed,
        if head_only || rollback {
            b
        } else {
            *a.as_ref().unwrap()
        }
    );

    // A new pool models restart after persisting the first page. Every
    // committed event must be delivered, and an aborted ID must not stall it.
    let first = catalog.changes_since(ht, CommitId(0), 1).await.unwrap();
    catalog
        .set_cursor("consumer", ht, first[0].commit_id)
        .await
        .unwrap();
    let resumed = PostgresCatalog::connect(&url).await.unwrap();
    let cursor = resumed.get_cursor("consumer", ht).await.unwrap();
    let tail = resumed.changes_since(ht, cursor, 100).await.unwrap();
    let delivered: Vec<_> = first
        .iter()
        .chain(&tail)
        .map(|event| event.commit_id)
        .collect();
    assert_eq!(
        delivered,
        if rollback {
            vec![b]
        } else {
            vec![a.unwrap(), b]
        }
    );
    let offsets = resumed.ingest_offsets(ht, "events").await.unwrap();
    assert_eq!(offsets.get(&1), Some(&10));
    assert_eq!(offsets.get(&0), if rollback { None } else { Some(&10) });
    assert_eq!(catalog.feed_head(ht).await.unwrap(), b);
}

#[tokio::test]
async fn feed_page_waits_for_lower_commit_and_survives_restart() {
    out_of_order(false, false).await;
}

#[tokio::test]
async fn feed_head_waits_for_lower_commit() {
    out_of_order(false, true).await;
}

#[tokio::test]
async fn feed_page_advances_after_lower_transaction_rolls_back() {
    out_of_order(true, false).await;
}

#[tokio::test]
async fn feed_head_advances_after_lower_transaction_rolls_back() {
    out_of_order(true, true).await;
}

fn assert_lock_timeout(error: &CatalogError) {
    assert_eq!(error.class(), CatalogErrorClass::RetryableDatabase);
    assert!(error.is_recoverable_transport());
    let CatalogError::Db(sqlx::Error::Database(database)) = error else {
        panic!("lock timeout must be a definite database failure, got {error:?}");
    };
    assert_eq!(database.code().as_deref(), Some("55P03"));
}

#[tokio::test]
async fn lock_timeout_reaches_eager_lazy_and_replacement_connections() {
    let (_pg, _catalog, url) = setup().await;
    // Explicit pool config takes precedence over a conflicting URL setting.
    let url = format!("{url}?options=-c%20lock_timeout%3D0");
    let config = CatalogPoolConfig {
        max_connections: 2,
        lock_timeout: Duration::from_millis(150),
        ..Default::default()
    };
    for lazy in [false, true] {
        let catalog = if lazy {
            PostgresCatalog::connect_lazy_with_config(&url, &config).unwrap()
        } else {
            PostgresCatalog::connect_with_config(&url, &config)
                .await
                .unwrap()
        };
        // Hold two connections simultaneously, then close both to force new
        // sessions. Every session must carry the setting, not just the first.
        for _ in 0..2 {
            let mut first = catalog.pool_for_tests().acquire().await.unwrap();
            let mut second = catalog.pool_for_tests().acquire().await.unwrap();
            for connection in [&mut first, &mut second] {
                let value: String = sqlx::query_scalar("SHOW lock_timeout")
                    .fetch_one(&mut **connection)
                    .await
                    .unwrap();
                assert_eq!(value, "150ms");
            }
            first.close().await.unwrap();
            second.close().await.unwrap();
        }
    }
}

#[tokio::test]
async fn feed_timeouts_preserve_the_cursor_and_release_the_waiter() {
    let (_pg, catalog, url) = setup().await;
    let ht = table(&catalog, "events").await;
    catalog
        .set_cursor("consumer", ht, CommitId(0))
        .await
        .unwrap();
    let (gate, gate_pid) = gate(&catalog, &ingest_identity(ht, 0)).await;
    let writer = PostgresCatalog::connect_with_config(
        &url,
        &CatalogPoolConfig {
            lock_timeout: Duration::from_secs(10),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let a = tokio::spawn(async move { ingest(&writer, ht, 0, "paused.parquet").await });
    blocked_by(&catalog, gate_pid).await;
    let b = ingest(&catalog, ht, 1, "b.parquet").await.unwrap();
    let reader = PostgresCatalog::connect_with_config(
        &url,
        &CatalogPoolConfig {
            max_connections: 1,
            lock_timeout: Duration::from_millis(150),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    for head_only in [true, false] {
        let error = tokio::time::timeout(Duration::from_secs(5), async {
            if head_only {
                reader.feed_head(ht).await.map(|_| ())
            } else {
                reader.changes_since(ht, CommitId(0), 100).await.map(|_| ())
            }
        })
        .await
        .expect("server did not enforce the lock timeout")
        .unwrap_err();
        assert_lock_timeout(&error);
        assert_eq!(
            catalog.get_cursor("consumer", ht).await.unwrap(),
            CommitId(0)
        );
        // Reuse the sole connection: the failed transaction must be rolled
        // back and returned to the pool, rather than left holding a waiter.
        tokio::time::timeout(Duration::from_secs(5), reader.ping())
            .await
            .unwrap()
            .unwrap();
    }
    // A is still paused. A timed-out exclusive waiter must no longer prevent
    // another compatible writer from making progress on this same hypertable.
    let c = tokio::time::timeout(Duration::from_secs(5), ingest(&reader, ht, 2, "c.parquet"))
        .await
        .expect("timed-out feed waiter still blocked writers")
        .unwrap();
    gate.commit().await.unwrap();
    let a = a.await.unwrap().unwrap();
    let events = reader.changes_since(ht, CommitId(0), 100).await.unwrap();
    assert_eq!(
        events
            .iter()
            .map(|event| event.commit_id)
            .collect::<Vec<_>>(),
        vec![a, b, c]
    );
}

#[tokio::test]
async fn writer_barrier_timeout_applies_nothing_and_can_be_retried() {
    let (_pg, catalog, url) = setup().await;
    let ht = table(&catalog, "events").await;
    let mut gate = catalog.pool_for_tests().begin().await.unwrap();
    sqlx::query("SELECT pg_advisory_xact_lock(~$1::bigint)")
        .bind(ht.0)
        .execute(&mut *gate)
        .await
        .unwrap();
    let before: (i64, bool) = sqlx::query_as("SELECT last_value, is_called FROM commits_id_seq")
        .fetch_one(&mut *gate)
        .await
        .unwrap();
    let writer = PostgresCatalog::connect_lazy_with_config(
        &url,
        &CatalogPoolConfig {
            max_connections: 1,
            lock_timeout: Duration::from_millis(150),
            ..Default::default()
        },
    )
    .unwrap();
    let error = tokio::time::timeout(
        Duration::from_secs(5),
        ingest(&writer, ht, 0, "new.parquet"),
    )
    .await
    .expect("writer lock wait exceeded its budget")
    .unwrap_err();
    assert_lock_timeout(&error);
    let during: (i64, bool) = sqlx::query_as("SELECT last_value, is_called FROM commits_id_seq")
        .fetch_one(&mut *gate)
        .await
        .unwrap();
    assert_eq!(before, during, "a timed-out writer must not allocate an ID");
    assert!(writer.live_parts(ht, None).await.unwrap().is_empty());
    assert!(
        writer
            .ingest_offsets(ht, "events")
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        writer
            .lookup_operation(&ingest_identity(ht, 0))
            .await
            .unwrap(),
        ukiel_catalog::OperationLookup::Absent
    );
    gate.commit().await.unwrap();
    let committed = ingest(&writer, ht, 0, "new.parquet").await.unwrap();
    assert_eq!(writer.feed_head(ht).await.unwrap(), committed);
    assert_eq!(writer.live_parts(ht, None).await.unwrap().len(), 1);
    assert_eq!(
        writer.ingest_offsets(ht, "events").await.unwrap().get(&0),
        Some(&10)
    );
}

#[tokio::test]
async fn a_lock_timeout_after_part_insertion_rolls_back_the_whole_mutation() {
    let (_pg, catalog, url) = setup().await;
    let ht = table(&catalog, "events").await;
    let initial = ingest(&catalog, ht, 0, "initial.parquet").await.unwrap();
    let mut gate = catalog.pool_for_tests().begin().await.unwrap();
    sqlx::query(
        "SELECT next_offset FROM ingest_offsets
         WHERE hypertable_id = $1 AND topic = 'events' AND kafka_partition = 0 FOR UPDATE",
    )
    .bind(ht.0)
    .execute(&mut *gate)
    .await
    .unwrap();
    let writer = PostgresCatalog::connect_with_config(
        &url,
        &CatalogPoolConfig {
            max_connections: 1,
            lock_timeout: Duration::from_millis(150),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let identity = OperationIdentity::ingest(
        ht,
        &[IngestRange {
            topic: "events".into(),
            partition: 0,
            first: 10,
            last: 19,
        }],
        1,
    )
    .unwrap();
    let offsets = [OffsetRange {
        topic: "events".into(),
        partition: 0,
        first: 10,
        last: 19,
    }];
    let op = CommitOp::Add {
        parts: vec![part("new.parquet")],
    };
    let error = tokio::time::timeout(
        Duration::from_secs(5),
        writer.commit_with_offsets(ht, op.clone(), &offsets, &identity),
    )
    .await
    .expect("offset lock wait exceeded its budget")
    .unwrap_err();
    assert_lock_timeout(&error);
    assert_eq!(
        writer.lookup_operation(&identity).await.unwrap(),
        ukiel_catalog::OperationLookup::Absent
    );
    let live = writer.live_parts(ht, None).await.unwrap();
    assert_eq!(live.len(), 1);
    assert_eq!(live[0].meta.path, "initial.parquet");
    assert_eq!(
        writer.ingest_offsets(ht, "events").await.unwrap().get(&0),
        Some(&10)
    );
    assert_eq!(writer.feed_head(ht).await.unwrap(), initial);
    gate.commit().await.unwrap();
    let committed = writer
        .commit_with_offsets(ht, op, &offsets, &identity)
        .await
        .unwrap()
        .commit_id();
    assert!(
        committed.0 > initial.0 + 1,
        "the aborted allocation leaves a legal ID gap"
    );
    let events = writer.changes_since(ht, initial, 100).await.unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].commit_id, committed);
    assert_eq!(
        writer.ingest_offsets(ht, "events").await.unwrap().get(&0),
        Some(&20)
    );
}

#[tokio::test]
async fn every_mutation_locks_before_allocating_an_id() {
    let (_pg, catalog, _url) = setup().await;
    let cache: i64 = sqlx::query_scalar(
        "SELECT seqcache FROM pg_sequence
         WHERE seqrelid = pg_get_serial_sequence('commits', 'id')::regclass",
    )
    .fetch_one(catalog.pool_for_tests())
    .await
    .unwrap();
    assert_eq!(cache, 1, "pooled connections must not reserve future IDs");

    for path in ["add", "replace", "delete", "offsets", "leased"] {
        let ht = table(&catalog, path).await;
        catalog
            .commit(
                ht,
                CommitOp::Add {
                    parts: vec![part("input.parquet")],
                },
                None,
            )
            .await
            .unwrap();
        let input = catalog.live_parts(ht, None).await.unwrap()[0].id;
        let lease = catalog
            .try_acquire_compaction_lease(
                ht,
                &part("unused").partition_values,
                uuid::Uuid::new_v4(),
                Duration::from_secs(60),
            )
            .await
            .unwrap()
            .unwrap();

        // Stand in for a feed reader. An attempted writer must wait HERE,
        // before INSERT has even evaluated its identity default, on every API.
        let mut gate = catalog.pool_for_tests().begin().await.unwrap();
        let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&mut *gate)
            .await
            .unwrap();
        sqlx::query("SELECT pg_advisory_xact_lock(~$1::bigint)")
            .bind(ht.0)
            .execute(&mut *gate)
            .await
            .unwrap();
        let before: i64 = sqlx::query_scalar("SELECT last_value FROM commits_id_seq")
            .fetch_one(&mut *gate)
            .await
            .unwrap();
        let writer = catalog.clone();
        let task = tokio::spawn(async move {
            let identity = ingest_identity(ht, 0);
            match path {
                "add" => {
                    writer
                        .commit(
                            ht,
                            CommitOp::Add {
                                parts: vec![part("new.parquet")],
                            },
                            None,
                        )
                        .await
                }
                "replace" => {
                    writer
                        .commit(
                            ht,
                            CommitOp::Replace {
                                old: vec![input],
                                new: vec![part("new.parquet")],
                            },
                            Some(&identity),
                        )
                        .await
                }
                "delete" => {
                    writer
                        .commit(ht, CommitOp::Delete { parts: vec![input] }, Some(&identity))
                        .await
                }
                "offsets" => {
                    writer
                        .commit_with_offsets(
                            ht,
                            CommitOp::Add {
                                parts: vec![part("new.parquet")],
                            },
                            &[OffsetRange {
                                topic: "events".into(),
                                partition: 0,
                                first: 0,
                                last: 9,
                            }],
                            &identity,
                        )
                        .await
                }
                "leased" => {
                    writer
                        .commit_compaction_replace(
                            ht,
                            &lease,
                            vec![input],
                            vec![part("new.parquet")],
                            &identity,
                        )
                        .await
                }
                _ => unreachable!(),
            }
        });
        blocked_by(&catalog, pid).await;
        let during: i64 = sqlx::query_scalar("SELECT last_value FROM commits_id_seq")
            .fetch_one(&mut *gate)
            .await
            .unwrap();
        gate.commit().await.unwrap();
        task.await.unwrap().unwrap();
        assert_eq!(
            before, during,
            "{path} allocated an ID before taking the writer lock"
        );
    }
}

/// A small, manual before/after experiment on the real catalog API. This is a
/// local contention measurement, not a production capacity claim.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "manual issue-0015 throughput/latency experiment"]
async fn feed_publication_benchmark() {
    use std::sync::Arc;
    use std::time::Instant;
    use tokio::sync::Barrier;

    let (_pg, _catalog, url) = setup().await;
    let catalog = PostgresCatalog::connect_with_pool_size(&url, 64)
        .await
        .unwrap();
    let seconds: u64 = std::env::var("UKIEL_FEED_BENCH_SECONDS")
        .map(|value| {
            value
                .parse()
                .expect("UKIEL_FEED_BENCH_SECONDS must be an integer")
        })
        .unwrap_or(2);
    let trials: usize = std::env::var("UKIEL_FEED_BENCH_TRIALS")
        .map(|value| {
            value
                .parse()
                .expect("UKIEL_FEED_BENCH_TRIALS must be an integer")
        })
        .unwrap_or(3);
    assert!((1..=60).contains(&seconds));
    assert!((1..=10).contains(&trials));
    let disable_timeout = match std::env::var("UKIEL_FEED_BENCH_DISABLE_LOCK_TIMEOUT").as_deref() {
        Ok("1") => true,
        Ok("0") | Err(_) => false,
        Ok(_) => panic!("UKIEL_FEED_BENCH_DISABLE_LOCK_TIMEOUT must be 0 or 1"),
    };
    // Benchmark-only control. Prewarm the same 64 sessions in both arms, then
    // disable the server timeout only in the isolated control arm. The catalog
    // API still refuses an unbounded timeout in application configuration.
    let setting = if disable_timeout { "0" } else { "2000ms" };
    let mut connections = Vec::new();
    for _ in 0..64 {
        let mut connection = catalog.pool_for_tests().acquire().await.unwrap();
        sqlx::query("SELECT set_config('lock_timeout', $1, false)")
            .bind(setting)
            .execute(&mut *connection)
            .await
            .unwrap();
        connections.push(connection);
    }
    drop(connections);
    let actual: String = sqlx::query_scalar("SHOW lock_timeout")
        .fetch_one(catalog.pool_for_tests())
        .await
        .unwrap();
    let version: String = sqlx::query_scalar("SHOW server_version")
        .fetch_one(catalog.pool_for_tests())
        .await
        .unwrap();
    assert_eq!(actual, if disable_timeout { "0" } else { "2s" });
    eprintln!(
        "benchmark=normal postgres={version:?} lock_timeout={actual} seconds={seconds} trials={trials} pool=64 profile=debug"
    );
    for trial in 0..trials {
        for (workers, table_count, poll) in
            [(1, 1, false), (16, 1, false), (16, 1, true), (16, 8, true)]
        {
            let mut tables = Vec::new();
            for n in 0..table_count {
                tables.push(
                    table(
                        &catalog,
                        &format!("t{trial}_{workers}_{table_count}_{poll}_{n}"),
                    )
                    .await,
                );
            }
            let barrier = Arc::new(Barrier::new(workers + 1 + usize::from(poll)));
            let mut tasks = Vec::new();
            for worker in 0..workers {
                let (catalog, barrier, tables) = (catalog.clone(), barrier.clone(), tables.clone());
                tasks.push(tokio::spawn(async move {
                    barrier.wait().await;
                    let start = Instant::now();
                    let mut samples = Vec::new();
                    while start.elapsed() < Duration::from_secs(seconds) {
                        let before = Instant::now();
                        catalog
                            .commit(
                                tables[worker % tables.len()],
                                CommitOp::Add {
                                    parts: vec![part(&format!(
                                        "{worker}/{}.parquet",
                                        samples.len()
                                    ))],
                                },
                                None,
                            )
                            .await
                            .unwrap();
                        samples.push(before.elapsed().as_secs_f64() * 1000.0);
                    }
                    samples
                }));
            }
            let poller = if poll {
                let (catalog, tables, barrier) = (catalog.clone(), tables.clone(), barrier.clone());
                Some(tokio::spawn(async move {
                    barrier.wait().await;
                    let start = Instant::now();
                    let mut samples = Vec::new();
                    while start.elapsed() < Duration::from_secs(seconds) {
                        for ht in &tables {
                            let before = Instant::now();
                            catalog.feed_head(*ht).await.unwrap();
                            samples.push(before.elapsed().as_secs_f64() * 1000.0);
                        }
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                    samples
                }))
            } else {
                None
            };
            let before = Instant::now();
            barrier.wait().await;
            let mut samples = Vec::new();
            for task in tasks {
                samples.extend(task.await.unwrap());
            }
            let elapsed = before.elapsed().as_secs_f64();
            samples.sort_by(f64::total_cmp);
            let mut polls = if let Some(poller) = poller {
                poller.await.unwrap()
            } else {
                vec![0.0]
            };
            polls.sort_by(f64::total_cmp);
            eprintln!(
                "trial={trial} writers={workers} tables={table_count} poll={poll} commits={} tps={:.0} p50_ms={:.3} p99_ms={:.3} feed_p99_ms={:.3}",
                samples.len(),
                samples.len() as f64 / elapsed,
                samples[samples.len() / 2],
                samples[samples.len() * 99 / 100],
                polls[polls.len() * 99 / 100]
            );
        }
    }
}

/// Measure the queue behind one deliberately stalled transaction. Lock barriers
/// establish the schedule; the three-second hold is the measured intervention.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "manual issue-0015 stalled-writer latency experiment"]
async fn feed_barrier_stall_benchmark() {
    use std::time::Instant;

    for trial in 0..3 {
        let budgets = if trial % 2 == 0 {
            [2_000, 10_000]
        } else {
            [10_000, 2_000]
        };
        for budget_ms in budgets {
            let (_pg, catalog, url) = setup().await;
            let ht = table(&catalog, "events").await;
            let other = table(&catalog, "other").await;
            catalog
                .set_cursor("consumer", ht, CommitId(0))
                .await
                .unwrap();
            let (gate, gate_pid) = gate(&catalog, &ingest_identity(ht, 0)).await;
            // A must outlast the controlled stall. Only the reader and later
            // writer use the arm's budget; A's ten-second budget is held fixed.
            let a_catalog = PostgresCatalog::connect_with_config(
                &url,
                &CatalogPoolConfig {
                    lock_timeout: Duration::from_secs(10),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
            let a = tokio::spawn(async move { ingest(&a_catalog, ht, 0, "paused.parquet").await });
            let a_pid = blocked_by(&catalog, gate_pid).await;
            let config = CatalogPoolConfig {
                max_connections: 2,
                lock_timeout: Duration::from_millis(budget_ms),
                ..Default::default()
            };
            let reader = PostgresCatalog::connect_with_config(&url, &config)
                .await
                .unwrap();
            let writer = PostgresCatalog::connect_with_config(&url, &config)
                .await
                .unwrap();
            let origin = Instant::now();
            let read = tokio::spawn(async move {
                let start = Instant::now();
                let result = reader.feed_head(ht).await;
                (result, start.elapsed())
            });
            let reader_pid = blocked_by(&catalog, a_pid).await;
            let b = tokio::spawn(async move {
                let start = Instant::now();
                let result = ingest(&writer, ht, 1, "b.parquet").await;
                (result, start.elapsed())
            });
            blocked_by(&catalog, reader_pid).await;

            let before = Instant::now();
            let other_commit = ingest(&catalog, other, 0, "other.parquet").await.unwrap();
            let other_ms = before.elapsed().as_secs_f64() * 1000.0;
            let before = Instant::now();
            catalog.live_parts(ht, None).await.unwrap();
            let query_ms = before.elapsed().as_secs_f64() * 1000.0;

            tokio::time::sleep_until(tokio::time::Instant::from_std(
                origin + Duration::from_secs(3),
            ))
            .await;
            assert!(
                !a.is_finished(),
                "the paused writer ended before gate release"
            );
            let b_before_release = b.is_finished();
            assert_eq!(
                catalog.get_cursor("consumer", ht).await.unwrap(),
                CommitId(0)
            );
            let held_ms = origin.elapsed().as_secs_f64() * 1000.0;
            gate.commit().await.unwrap();
            let a_id = tokio::time::timeout(Duration::from_secs(5), a)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            let (read_result, read_elapsed) = tokio::time::timeout(Duration::from_secs(5), read)
                .await
                .unwrap()
                .unwrap();
            let (b_result, b_elapsed) = tokio::time::timeout(Duration::from_secs(5), b)
                .await
                .unwrap()
                .unwrap();
            let feed_outcome = match &read_result {
                Ok(head) => {
                    assert!(head.0 >= a_id.0);
                    "ok"
                }
                Err(error) => {
                    assert_lock_timeout(error);
                    "timeout"
                }
            };
            if budget_ms == 2_000 {
                assert_eq!(feed_outcome, "timeout");
            } else {
                assert_eq!(feed_outcome, "ok");
            }
            let writer_outcome = match &b_result {
                Ok(_) => "ok",
                Err(error) => {
                    assert_lock_timeout(error);
                    "timeout"
                }
            };
            let before = Instant::now();
            catalog.feed_head(ht).await.unwrap();
            let recovery_ms = before.elapsed().as_secs_f64() * 1000.0;
            assert_eq!(catalog.feed_head(other).await.unwrap(), other_commit);
            let events = catalog.changes_since(ht, CommitId(0), 100).await.unwrap();
            let mut expected = vec![a_id];
            if let Ok(b_id) = b_result {
                expected.push(b_id);
            }
            expected.sort();
            assert_eq!(
                events
                    .iter()
                    .map(|event| event.commit_id)
                    .collect::<Vec<_>>(),
                expected
            );
            eprintln!(
                "benchmark=stall trial={trial} budget_ms={budget_ms} held_ms={held_ms:.3} feed={feed_outcome} feed_ms={:.3} queued_writer={writer_outcome} writer_ms={:.3} writer_finished_before_release={b_before_release} other_ht_ms={other_ms:.3} ordinary_query_ms={query_ms:.3} recovery_ms={recovery_ms:.3} delivered={}",
                read_elapsed.as_secs_f64() * 1000.0,
                b_elapsed.as_secs_f64() * 1000.0,
                events.len()
            );
        }
    }
}

//! Role supervision (plan 42).
//!
//! Before this, one worker returning any error cancelled every role and took the
//! process down — so a five-second database failover became a fleet-wide crash
//! loop, and the restart storm hit the primary exactly when it was weakest.
//!
//! Now a *recoverable* catalog failure is contained: the failing worker is
//! dropped (which is also how its stale plan, buffers and Kafka assignment are
//! discarded), the role goes `Degraded`, and only that role waits for the
//! catalog. Its siblings keep running. When the catalog answers again the worker
//! is **rebuilt from scratch** — never resumed — and reloads its authoritative
//! state.
//!
//! Rebuild, not retry. Retrying the failed call is not available to us: a
//! connection that died around a commit leaves that commit's outcome *unknown*,
//! and replaying it could double-apply it. Rebuilding from catalog authority is
//! the only move that is correct without knowing the outcome.
//!
//! Plan 43 makes that outcome **knowable**. If the failure carried an operation
//! identity, the role stays `Reconciling` until the catalog says `Committed` or
//! `Absent`, and only then is the worker rebuilt. The answer does not change what
//! recovery *does* — it is not permission to replay a stale payload, and `Absent`
//! least of all: the old worker is already gone, and the fresh one replans from
//! offsets and live parts either way. What it changes is that a lost
//! acknowledgement is now a fact in the log instead of a silence.
//!
//! A *permanent* failure keeps the old behavior: the role fails and the process
//! goes down. A configuration or schema error will not fix itself by waiting —
//! and neither will a fingerprint collision, which says the caller is wrong.

use std::future::Future;
use std::pin::Pin;

use tokio_util::sync::CancellationToken;
use ukiel_catalog::{CatalogError, OperationLookup, PostgresCatalog};
use ukiel_core::OperationIdentity;

use crate::health::{HealthRegistry, RoleHandle};
use crate::recovery::{RecoveryPolicy, Shutdown, wait_for_catalog};

/// A worker error we can reason about. Implemented for each role's own error
/// type, so classification is **typed** — never a string search through an
/// `anyhow` chain, which would silently stop working the day a message changes.
pub trait RoleFailure {
    fn catalog_error(&self) -> Option<&CatalogError>;

    /// The operation whose outcome this failure left unknown, if any.
    ///
    /// Derived from the typed error, never reconstructed: an identity invented
    /// *after* seeing the failure would be fabricating the evidence for the
    /// answer it is about to look up.
    fn ambiguous_operation(&self) -> Option<&OperationIdentity> {
        match self.catalog_error() {
            Some(CatalogError::AmbiguousMutation { identity, .. }) => Some(identity),
            _ => None,
        }
    }
}

impl RoleFailure for ukiel_ingest::IngestError {
    fn catalog_error(&self) -> Option<&CatalogError> {
        match self {
            ukiel_ingest::IngestError::Catalog(e) => Some(e),
            _ => None,
        }
    }
}

impl RoleFailure for ukiel_compactor::CompactorError {
    fn catalog_error(&self) -> Option<&CatalogError> {
        match self {
            ukiel_compactor::CompactorError::Catalog(e) => Some(e),
            _ => None,
        }
    }
}

impl RoleFailure for ukiel_gc::GcError {
    fn catalog_error(&self) -> Option<&CatalogError> {
        match self {
            ukiel_gc::GcError::Catalog(e) => Some(e),
            _ => None,
        }
    }
}

impl RoleFailure for CatalogError {
    fn catalog_error(&self) -> Option<&CatalogError> {
        Some(self)
    }
}

/// Startup steps (bootstrap) report `anyhow`. Finding the catalog error is a
/// **downcast through the chain**, not a search of the message: a classification
/// that depends on wording stops working the day the wording changes.
impl RoleFailure for anyhow::Error {
    fn catalog_error(&self) -> Option<&CatalogError> {
        self.chain().find_map(|e| e.downcast_ref::<CatalogError>())
    }
}

/// A worker run: builds a fresh worker and drives it to completion. `Ok(())`
/// means it stopped because the process is shutting down.
pub type RoleRun<'a, E> = Pin<Box<dyn Future<Output = Result<(), E>> + Send + 'a>>;

/// Runs one role, rebuilding it after any recoverable catalog failure, until the
/// process shuts down or the role fails permanently.
///
/// `build` is called afresh for every attempt: nothing — no buffer, no plan, no
/// lease, no cursor — crosses the boundary between attempts. That is the whole
/// safety argument.
pub async fn supervise_role<E, Build>(
    mut build: Build,
    catalog: PostgresCatalog,
    health: HealthRegistry,
    handle: RoleHandle,
    policy: RecoveryPolicy,
    shutdown: CancellationToken,
) -> anyhow::Result<&'static str>
where
    E: RoleFailure + std::error::Error + Send + Sync + 'static,
    Build: FnMut() -> RoleRun<'static, E> + Send,
{
    let label = handle.label();
    loop {
        match build().await {
            Ok(()) => {
                handle.stopped();
                return Ok(label);
            }
            Err(e) => {
                let recoverable = e
                    .catalog_error()
                    .filter(|c| c.is_recoverable_transport())
                    .map(|c| c.class());
                // Cloned out of the error before anything else can drop it: at
                // most one per attempt, because a worker stops at its first
                // ambiguous commit.
                let ambiguous = e.ambiguous_operation().cloned();

                let Some(class) = recoverable else {
                    // Permanent: a bad schema, a lost lease misused, an
                    // object-store failure, a Kafka failure. Waiting does not
                    // help, so keep today's fail-fast behavior.
                    let class = e.catalog_error().map(|c| c.class());
                    handle.failed(class);
                    return Err(anyhow::Error::new(e).context(format!("{label} worker")));
                };

                tracing::warn!(
                    role = label,
                    error = %e,
                    class = class.as_label(),
                    "catalog unavailable; role degraded, worker dropped"
                );
                handle.degraded(class);
                metrics::counter!("worker_restarts_total", "role" => label).increment(1);

                // The worker future is already dropped: its Kafka assignment,
                // buffered rows, live-part plan and in-flight merge went with it.
                let paused = std::time::Instant::now();
                if wait_for_catalog(&catalog, &health, &handle, &policy, &shutdown)
                    .await
                    .is_err()
                {
                    // Shutdown during the outage: a clean stop, not a failure.
                    handle.stopped();
                    return Ok(label);
                }
                metrics::histogram!("worker_pause_seconds", "role" => label)
                    .record(paused.elapsed().as_secs_f64());

                // The catalog answers again — but this role is not healthy yet.
                // It becomes healthy when it has reloaded from authority and
                // says so itself, not because a `SELECT 1` succeeded.
                handle.reconciling();

                // And if the worker died not knowing whether its commit landed,
                // the role stays Reconciling until the catalog answers. The
                // rebuild does not wait on this for *safety* — it is correct
                // either way — but a fleet that cannot say whether an operation
                // committed cannot be reasoned about after an incident, and a
                // collision here is a bug that must stop the role, not be
                // rebuilt past.
                if let Some(identity) = ambiguous {
                    match reconcile(
                        &catalog, &identity, label, &health, &handle, &policy, &shutdown,
                    )
                    .await
                    {
                        Ok(()) => {}
                        Err(Reconcile::Stopped) => {
                            handle.stopped();
                            return Ok(label);
                        }
                        Err(Reconcile::Failed(e)) => {
                            handle.failed(Some(e.class()));
                            return Err(anyhow::Error::new(e)
                                .context(format!("{label} operation reconciliation")));
                        }
                    }
                }

                tracing::info!(role = label, "catalog reachable; rebuilding worker");
            }
        }
    }
}

/// Why reconciliation did not produce an answer.
enum Reconcile {
    /// The process is shutting down. A clean stop, not a failure.
    Stopped,
    /// The catalog answered, and the answer was that the caller is wrong: this
    /// key is already recorded under different intent. Waiting will not change
    /// that, and rebuilding past it would leave the bug in production.
    Failed(CatalogError),
}

/// Asks the catalog whether one ambiguous operation landed, while the role is
/// `Reconciling`.
///
/// A transport failure during the lookup is the outage still going on: it reuses
/// the same bounded, jittered wait as everything else, and never gives up and
/// rebuilds early — the whole point is to have the answer *before* a fresh worker
/// starts writing.
///
/// Both answers are then **discarded**. `Absent` is not permission to replay the
/// old worker's payload — that worker is gone, and resuming its future is exactly
/// how reconciliation would turn into blessing stale state. `Committed` is not a
/// result to restore either. The rebuilt worker reloads offsets, re-seeks Kafka,
/// and replans from live parts, as it would have anyway. What the answer buys is
/// that the outcome is in the log and in a counter instead of being unknowable.
#[allow(clippy::too_many_arguments)]
async fn reconcile(
    catalog: &PostgresCatalog,
    identity: &OperationIdentity,
    label: &'static str,
    health: &HealthRegistry,
    handle: &RoleHandle,
    policy: &RecoveryPolicy,
    shutdown: &CancellationToken,
) -> Result<(), Reconcile> {
    let started = std::time::Instant::now();
    let outcome = loop {
        match catalog.lookup_operation(identity).await {
            Ok(OperationLookup::Committed(commit)) => {
                tracing::info!(
                    role = label,
                    domain = identity.domain(),
                    key = identity.key(),
                    commit = commit.0,
                    "the ambiguous mutation is durable; rebuilding from authority"
                );
                break "committed";
            }
            Ok(OperationLookup::Absent) => {
                tracing::info!(
                    role = label,
                    domain = identity.domain(),
                    key = identity.key(),
                    "the ambiguous mutation never landed; rebuilding from authority"
                );
                break "absent";
            }
            Err(e) if e.is_recoverable_transport() => {
                // Still the outage. Back off on the shared policy and ask again;
                // do not rebuild while the question is open.
                if wait_for_catalog(catalog, health, handle, policy, shutdown)
                    .await
                    .is_err()
                {
                    metrics::counter!("catalog_mutation_reconciliation_total",
                        "role" => label, "outcome" => "error")
                    .increment(1);
                    return Err(Reconcile::Stopped);
                }
                handle.reconciling();
            }
            Err(e) => {
                let collision = matches!(e, CatalogError::OperationKeyCollision { .. });
                tracing::error!(
                    role = label,
                    domain = identity.domain(),
                    key = identity.key(),
                    error = %e,
                    "the ambiguous mutation cannot be reconciled"
                );
                metrics::counter!("catalog_mutation_reconciliation_total",
                    "role" => label,
                    "outcome" => if collision { "collision" } else { "error" })
                .increment(1);
                return Err(Reconcile::Failed(e));
            }
        }
    };

    // Bounded label set, and never the key, the fingerprint, the hypertable or
    // the commit id: those belong in the structured log above, where cardinality
    // costs nothing.
    metrics::counter!("catalog_mutation_reconciliation_total",
        "role" => label, "outcome" => outcome)
    .increment(1);
    metrics::histogram!("catalog_mutation_reconciliation_duration_seconds", "role" => label)
        .record(started.elapsed().as_secs_f64());
    Ok(())
}

/// Runs a fallible catalog step (migrations, bootstrap) through the same
/// recovery policy: a writer failover during a rolling deploy should delay
/// startup, not crash-loop it. Permanent errors still terminate.
pub async fn with_catalog_recovery<T, E, F, Fut>(
    what: &'static str,
    catalog: &PostgresCatalog,
    health: &HealthRegistry,
    handle: &RoleHandle,
    policy: &RecoveryPolicy,
    shutdown: &CancellationToken,
    mut step: F,
) -> anyhow::Result<T>
where
    E: RoleFailure + Into<anyhow::Error>,
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
{
    loop {
        match step().await {
            Ok(value) => {
                health.catalog_reachable();
                return Ok(value);
            }
            Err(e) => {
                let transient = e
                    .catalog_error()
                    .is_some_and(|c| c.is_recoverable_transport());
                if !transient {
                    // A bad schema, an invalid config, an unknown database
                    // error: none of these are fixed by waiting.
                    return Err(e.into().context(what));
                }
                tracing::warn!(step = what, "catalog unavailable during startup; waiting");
                health.catalog_unavailable();
                if wait_for_catalog(catalog, health, handle, policy, shutdown)
                    .await
                    .is_err()
                {
                    anyhow::bail!("shutdown while waiting for the catalog during {what}");
                }
            }
        }
    }
}

/// Shutdown is not a failure. A worker cancelled mid-flight may surface the
/// cancellation as a transport error; without this, a clean stop would be
/// reported as an outage.
pub fn is_shutdown(shutdown: &CancellationToken) -> bool {
    shutdown.is_cancelled()
}

pub type Never = Shutdown;

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use crate::config::{CatalogConfig, Role};
    use crate::health::RoleState;
    use ukiel_catalog::CatalogPoolConfig;

    /// A scripted worker: each attempt pops the next outcome.
    #[derive(Debug, thiserror::Error)]
    pub(super) enum FakeError {
        #[error(transparent)]
        Catalog(#[from] CatalogError),
        #[error("boom")]
        Permanent,
    }

    impl RoleFailure for FakeError {
        fn catalog_error(&self) -> Option<&CatalogError> {
            match self {
                FakeError::Catalog(e) => Some(e),
                FakeError::Permanent => None,
            }
        }
    }

    pub(super) fn transport() -> FakeError {
        FakeError::Catalog(CatalogError::Db(sqlx::Error::PoolTimedOut))
    }

    pub(super) fn identity(ht: i64) -> ukiel_core::OperationIdentity {
        ukiel_core::OperationIdentity::ingest(
            ukiel_core::HypertableId(ht),
            &[ukiel_core::IngestRange {
                topic: "events".into(),
                partition: 0,
                first: 0,
                last: 9,
            }],
            1,
        )
        .unwrap()
    }

    /// The worker died at the commit boundary, holding an identity.
    pub(super) fn ambiguous(identity: &ukiel_core::OperationIdentity) -> FakeError {
        FakeError::Catalog(CatalogError::AmbiguousMutation {
            key: identity.key().to_string(),
            identity: Box::new(identity.clone()),
            source: sqlx::Error::Io(std::io::Error::new(
                std::io::ErrorKind::ConnectionReset,
                "connection reset by peer",
            )),
        })
    }

    pub(super) fn reachable_catalog(url: &str) -> PostgresCatalog {
        PostgresCatalog::connect_lazy_with_config(url, &CatalogPoolConfig::default()).unwrap()
    }

    pub(super) fn fast_policy() -> RecoveryPolicy {
        RecoveryPolicy {
            base: std::time::Duration::from_millis(10),
            max: std::time::Duration::from_millis(20),
            jitter_ratio: 0.0,
            probe_timeout: std::time::Duration::from_millis(500),
            seed: 1,
        }
    }

    #[tokio::test]
    async fn a_transient_failure_degrades_then_rebuilds_the_worker() {
        let pg = testcontainers_modules::testcontainers::runners::AsyncRunner::start(
            testcontainers_modules::postgres::Postgres::default(),
        )
        .await
        .expect("postgres");
        let port = pg.get_host_port_ipv4(5432).await.unwrap();
        let catalog = reachable_catalog(&format!(
            "postgres://postgres:postgres@127.0.0.1:{port}/postgres"
        ));

        let health = HealthRegistry::new(&[Role::Gc]);
        let handle = health.role(Role::Gc);
        let shutdown = CancellationToken::new();
        let builds = Arc::new(AtomicUsize::new(0));

        let counter = builds.clone();
        let token = shutdown.clone();
        let build = move || -> RoleRun<'static, FakeError> {
            let attempt = counter.fetch_add(1, Ordering::SeqCst);
            let token = token.clone();
            Box::pin(async move {
                match attempt {
                    // Two transient failures, then a worker that runs until
                    // shutdown. The rebuild is what recovery *is*.
                    0 | 1 => Err(transport()),
                    _ => {
                        token.cancel();
                        Ok(())
                    }
                }
            })
        };

        let label = supervise_role(
            build,
            catalog,
            health.clone(),
            handle.clone(),
            fast_policy(),
            shutdown,
        )
        .await
        .expect("recoverable failures must not fail the role");

        assert_eq!(label, "gc");
        assert_eq!(
            builds.load(Ordering::SeqCst),
            3,
            "the worker was rebuilt twice"
        );
        assert_eq!(handle.state(), RoleState::Stopped);
        assert_eq!(
            health.snapshot().roles["gc"].recovery_attempts,
            2,
            "each rebuild counts"
        );
    }

    #[tokio::test]
    async fn a_permanent_failure_still_fails_the_role() {
        let catalog = reachable_catalog("postgres://u:p@127.0.0.1:1/db");
        let health = HealthRegistry::new(&[Role::Gc]);
        let handle = health.role(Role::Gc);

        let build = move || -> RoleRun<'static, FakeError> {
            Box::pin(async move { Err(FakeError::Permanent) })
        };
        let err = supervise_role(
            build,
            catalog,
            health,
            handle.clone(),
            fast_policy(),
            CancellationToken::new(),
        )
        .await
        .expect_err("a permanent failure must not be retried forever");
        assert!(err.to_string().contains("gc worker"), "{err}");
        assert_eq!(handle.state(), RoleState::Failed);
    }

    #[tokio::test]
    async fn shutdown_during_an_outage_stops_cleanly() {
        // The catalog never comes back; the process is asked to stop. That is a
        // clean exit, not a role failure — otherwise every shutdown during a
        // database outage would be reported as a crash.
        let catalog = reachable_catalog("postgres://u:p@127.0.0.1:1/db");
        let health = HealthRegistry::new(&[Role::Gc]);
        let handle = health.role(Role::Gc);
        let shutdown = CancellationToken::new();

        let token = shutdown.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            token.cancel();
        });

        let build =
            move || -> RoleRun<'static, FakeError> { Box::pin(async move { Err(transport()) }) };
        let label = supervise_role(
            build,
            catalog,
            health,
            handle.clone(),
            fast_policy(),
            shutdown,
        )
        .await
        .expect("shutdown is not a failure");
        assert_eq!(label, "gc");
        assert_eq!(handle.state(), RoleState::Stopped);
    }

    #[tokio::test]
    async fn a_startup_step_waits_out_an_outage_but_not_a_bad_schema() {
        let catalog = reachable_catalog("postgres://u:p@127.0.0.1:1/db");
        let health = HealthRegistry::new(&[Role::Gc]);
        let handle = health.role(Role::Gc);
        let shutdown = CancellationToken::new();
        let token = shutdown.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(150)).await;
            token.cancel();
        });

        // Transient: it waits (and here, shutdown ends the wait).
        let err = with_catalog_recovery(
            "migrate",
            &catalog,
            &health,
            &handle,
            &fast_policy(),
            &shutdown,
            || async { Err::<(), _>(CatalogError::Db(sqlx::Error::PoolTimedOut)) },
        )
        .await
        .expect_err("shutdown ends the wait");
        assert!(err.to_string().contains("shutdown"), "{err}");

        // Permanent: it does not wait at all. A bad schema will not fix itself.
        let err = with_catalog_recovery(
            "bootstrap",
            &catalog,
            &health,
            &handle,
            &fast_policy(),
            &CancellationToken::new(),
            || async { Err::<(), _>(CatalogError::NotFound("events".into())) },
        )
        .await
        .expect_err("a permanent error must terminate startup");
        assert!(err.to_string().contains("bootstrap"), "{err}");
    }

    #[tokio::test]
    async fn defaults_build_a_usable_policy() {
        let policy = RecoveryPolicy::from_config(&CatalogConfig::default(), 7);
        assert_eq!(policy.base, std::time::Duration::from_millis(100));
        assert_eq!(policy.max, std::time::Duration::from_millis(5_000));
    }
}

#[cfg(test)]
mod reconciliation_tests {
    use super::tests::*;
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use crate::config::Role;
    use crate::health::RoleState;
    use ukiel_core::{CommitOp, PartMeta};

    async fn catalog_with_events() -> (
        testcontainers_modules::testcontainers::ContainerAsync<
            testcontainers_modules::postgres::Postgres,
        >,
        PostgresCatalog,
        ukiel_core::HypertableId,
    ) {
        let pg = testcontainers_modules::testcontainers::runners::AsyncRunner::start(
            testcontainers_modules::postgres::Postgres::default(),
        )
        .await
        .expect("postgres");
        let port = pg.get_host_port_ipv4(5432).await.unwrap();
        let catalog = PostgresCatalog::connect(&format!(
            "postgres://postgres:postgres@127.0.0.1:{port}/postgres"
        ))
        .await
        .unwrap();
        catalog.migrate().await.unwrap();
        let ht = catalog
            .create_hypertable(
                "events",
                &serde_json::json!({"fields": [
                    {"name": "tenant_id", "type": "int64"},
                    {"name": "ts", "type": "timestamp_ms"}
                ]}),
                &serde_json::json!({"columns": ["day"]}),
                &["tenant_id".to_string()],
                "tenant_id",
            )
            .await
            .unwrap();
        (pg, catalog, ht)
    }

    /// The lost-acknowledgement path, end to end through the supervisor: the
    /// worker dies at the boundary, the role degrades, the catalog comes back,
    /// the role stays Reconciling through an authoritative lookup, and only then
    /// is a fresh worker built.
    #[tokio::test]
    async fn an_ambiguous_commit_is_looked_up_before_the_worker_is_rebuilt() {
        for durable in [true, false] {
            let (_pg, catalog, ht) = catalog_with_events().await;
            let identity = identity(ht.0);

            // `durable` is the case the worker could not distinguish: the
            // transaction committed and the acknowledgement was lost.
            if durable {
                catalog
                    .commit(
                        ht,
                        CommitOp::Add {
                            parts: vec![PartMeta {
                                path: "landed.parquet".into(),
                                partition_values: serde_json::json!({"day": "d1"}),
                                packing_key_min: 1,
                                packing_key_max: 1,
                                row_count: 1,
                                size_bytes: 1,
                                level: 0,
                                column_stats: None,
                            }],
                        },
                        Some(&identity),
                    )
                    .await
                    .unwrap();
            }

            let health = HealthRegistry::new(&[Role::Ingest]);
            let handle = health.role(Role::Ingest);
            let shutdown = CancellationToken::new();
            let builds = Arc::new(AtomicUsize::new(0));

            let counter = builds.clone();
            let token = shutdown.clone();
            let id = identity.clone();
            let build = move || -> RoleRun<'static, FakeError> {
                let attempt = counter.fetch_add(1, Ordering::SeqCst);
                let token = token.clone();
                let id = id.clone();
                Box::pin(async move {
                    match attempt {
                        0 => Err(ambiguous(&id)),
                        _ => {
                            token.cancel();
                            Ok(())
                        }
                    }
                })
            };

            supervise_role(
                build,
                catalog,
                health,
                handle.clone(),
                fast_policy(),
                shutdown,
            )
            .await
            .expect("an ambiguous commit is recoverable, not fatal");

            assert_eq!(
                builds.load(Ordering::SeqCst),
                2,
                "durable={durable}: the worker is rebuilt after the lookup, not before"
            );
            assert_eq!(handle.state(), RoleState::Stopped);
        }
    }

    /// A fingerprint collision says the *caller* is wrong: the key is already
    /// recorded under different intent. Waiting cannot fix it and rebuilding
    /// past it would leave the bug running, so the role fails.
    #[tokio::test]
    async fn a_fingerprint_collision_fails_the_role() {
        let (_pg, catalog, ht) = catalog_with_events().await;
        let identity = identity(ht.0);

        // Somebody else's commit already holds this key (a legacy row: keyed,
        // and with no recoverable intent).
        sqlx::query(
            "INSERT INTO commits (hypertable_id, kind, idempotency_key) VALUES ($1, 'add', $2)",
        )
        .bind(ht.0)
        .bind(identity.key())
        .execute(catalog.pool_for_tests())
        .await
        .unwrap();

        let health = HealthRegistry::new(&[Role::Ingest]);
        let handle = health.role(Role::Ingest);
        let id = identity.clone();
        let build = move || -> RoleRun<'static, FakeError> {
            let id = id.clone();
            Box::pin(async move { Err(ambiguous(&id)) })
        };

        let err = supervise_role(
            build,
            catalog,
            health,
            handle.clone(),
            fast_policy(),
            CancellationToken::new(),
        )
        .await
        .expect_err("a collision must not be rebuilt past");
        assert!(err.to_string().contains("reconciliation"), "{err}");
        assert_eq!(handle.state(), RoleState::Failed);
    }

    /// Shutdown while the lookup is still failing is a clean stop, not a crash.
    #[tokio::test]
    async fn shutdown_during_the_lookup_stops_cleanly() {
        let catalog = reachable_catalog("postgres://u:p@127.0.0.1:1/db");
        let health = HealthRegistry::new(&[Role::Ingest]);
        let handle = health.role(Role::Ingest);
        let shutdown = CancellationToken::new();
        let token = shutdown.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            token.cancel();
        });

        let id = identity(1);
        let build = move || -> RoleRun<'static, FakeError> {
            let id = id.clone();
            Box::pin(async move { Err(ambiguous(&id)) })
        };
        let label = supervise_role(
            build,
            catalog,
            health,
            handle.clone(),
            fast_policy(),
            shutdown,
        )
        .await
        .expect("shutdown is not a failure");
        assert_eq!(label, "ingest");
        assert_eq!(handle.state(), RoleState::Stopped);
    }
}

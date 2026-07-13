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
//! the only move that is correct without knowing the outcome. (Plan 43's
//! operation identities are what will make the outcome knowable.)
//!
//! A *permanent* failure keeps the old behavior: the role fails and the process
//! goes down. A configuration or schema error will not fix itself by waiting.

use std::future::Future;
use std::pin::Pin;

use tokio_util::sync::CancellationToken;
use ukiel_catalog::{CatalogError, PostgresCatalog};

use crate::health::{HealthRegistry, RoleHandle};
use crate::recovery::{RecoveryPolicy, Shutdown, wait_for_catalog};

/// A worker error we can reason about. Implemented for each role's own error
/// type, so classification is **typed** — never a string search through an
/// `anyhow` chain, which would silently stop working the day a message changes.
pub trait RoleFailure {
    fn catalog_error(&self) -> Option<&CatalogError>;
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
                tracing::info!(role = label, "catalog reachable; rebuilding worker");
            }
        }
    }
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
    enum FakeError {
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

    fn transport() -> FakeError {
        FakeError::Catalog(CatalogError::Db(sqlx::Error::PoolTimedOut))
    }

    fn reachable_catalog(url: &str) -> PostgresCatalog {
        PostgresCatalog::connect_lazy_with_config(url, &CatalogPoolConfig::default()).unwrap()
    }

    fn fast_policy() -> RecoveryPolicy {
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

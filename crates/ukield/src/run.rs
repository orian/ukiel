//! Assembles and runs the enabled roles under one CancellationToken.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context;
use object_store::ObjectStore;
use object_store::aws::AmazonS3Builder;
use object_store::memory::InMemory;
use tokio_util::sync::CancellationToken;
use ukiel_catalog::PostgresCatalog;
use ukiel_compactor::compactor::{Compactor, CompactorConfig};
use ukiel_gc::{Gc, GcConfig};
use ukiel_ingest::config::{IngestConfig, TableRoute};
use ukiel_ingest::consumer::IngestWorker;
use ukiel_query::cache::{CacheConfig, CachingObjectStore};
use ukiel_query::metadata_cache::ParquetMetadataCache;
use ukiel_query::server::{AppState, router};

use crate::config::{ObjectStoreConfig, Role, UkieldConfig};
use crate::health::HealthRegistry;
use crate::recovery::RecoveryPolicy;
use crate::supervisor::{self, RoleFailure, RoleRun};

/// Sets `ukield_worker_up{role}` to 1 while alive and 0 on drop — so any exit
/// path (clean, error, or cancellation) flips the gauge (metrics P1).
struct WorkerUp(&'static str);

impl WorkerUp {
    fn new(role: &'static str) -> Self {
        metrics::gauge!("ukield_worker_up", "role" => role).set(1.0);
        WorkerUp(role)
    }
}

impl Drop for WorkerUp {
    fn drop(&mut self) {
        metrics::gauge!("ukield_worker_up", "role" => self.0).set(0.0);
    }
}

/// Spawns one role under the recovery supervisor. A recoverable catalog failure
/// rebuilds just this worker; its siblings never notice. Only a *permanent*
/// failure escapes to the JoinSet and takes the process down.
fn spawn_supervised<E, Build>(
    tasks: &mut tokio::task::JoinSet<anyhow::Result<&'static str>>,
    role: Role,
    build: Build,
    catalog: &PostgresCatalog,
    health: &HealthRegistry,
    policy: &RecoveryPolicy,
    shutdown: &CancellationToken,
) where
    E: RoleFailure + std::error::Error + Send + Sync + 'static,
    Build: FnMut() -> RoleRun<'static, E> + Send + 'static,
{
    let label = crate::health::role_label(role);
    let (catalog, health, handle, policy, shutdown) = (
        catalog.clone(),
        health.clone(),
        health.role(role),
        policy.clone(),
        shutdown.clone(),
    );
    tasks.spawn(async move {
        let _up = WorkerUp::new(label);
        supervisor::supervise_role(role, build, catalog, health, handle, policy, shutdown).await
    });
}

pub fn build_store(cfg: &ObjectStoreConfig) -> anyhow::Result<(Arc<dyn ObjectStore>, url::Url)> {
    match cfg.kind.as_str() {
        "memory" => Ok((Arc::new(InMemory::new()), url::Url::parse("mem://ukiel/")?)),
        "s3" => {
            let s3 = AmazonS3Builder::new()
                .with_endpoint(&cfg.endpoint)
                .with_bucket_name(&cfg.bucket)
                .with_access_key_id(&cfg.access_key_id)
                .with_secret_access_key(&cfg.secret_access_key)
                .with_region(&cfg.region)
                .with_allow_http(cfg.allow_http)
                .with_virtual_hosted_style_request(false)
                .build()
                .context("building s3 object store")?;
            let url = url::Url::parse(&format!("s3://{}/", cfg.bucket))?;
            Ok((Arc::new(s3), url))
        }
        other => anyhow::bail!("unknown object_store.kind '{other}' (expected 's3' or 'memory')"),
    }
}

pub async fn run(cfg: UkieldConfig, shutdown: CancellationToken) -> anyhow::Result<()> {
    run_with_bound_addr(cfg, shutdown, None).await
}

/// Like `run`, reporting the query server's bound address (tests bind port 0).
pub async fn run_with_bound_addr(
    cfg: UkieldConfig,
    shutdown: CancellationToken,
    bound: Option<tokio::sync::oneshot::Sender<SocketAddr>>,
) -> anyhow::Result<()> {
    crate::config::validate(&cfg)?;

    // Install the process-wide Prometheus recorder before any worker emits;
    // idempotent so repeated calls (tests) don't double-install (metrics P1).
    let prom = crate::metrics::handle();

    // Health exists before the catalog does. A process that starts during a
    // writer failover must be able to say "starting, catalog unavailable" —
    // not exit before it has an endpoint to say it on (plan 42).
    let health = HealthRegistry::new(&cfg.roles);

    // One identity per process: it seeds the recovery jitter (so the fleet does
    // not retry in lockstep) and owns the compactor's partition leases (plan 41,
    // where a reused identity would let a fresh process inherit a zombie's
    // tenancy).
    let instance_id = uuid::Uuid::new_v4();
    tracing::info!(%instance_id, "ukield instance");
    let policy = RecoveryPolicy::from_config(&cfg.catalog, instance_id.as_u128() as u64);

    // Lazy: building the pool contacts nothing. The process comes up, binds its
    // listeners, and reports Starting; the first real call is what discovers
    // whether the writer is there. And because the pool outlives the outage, it
    // reaches the *promoted* writer without a restart.
    let catalog =
        PostgresCatalog::connect_lazy_with_config(&cfg.catalog.url, &cfg.catalog.pool_config())
            .context("building the catalog pool")?;

    let catalog_for_roles = catalog.clone();

    let (base_store, store_url) = build_store(&cfg.object_store)?;
    let store: Arc<dyn ObjectStore> = if cfg.query.cache_dir.is_empty() {
        base_store
    } else {
        Arc::new(CachingObjectStore::with_config(
            base_store,
            PathBuf::from(&cfg.query.cache_dir),
            CacheConfig {
                large_object_threshold: cfg.query.cache_large_object_threshold,
                chunk_size: cfg.query.cache_chunk_size,
                write_through: cfg.query.cache_write_through,
            },
        ))
    };

    // Process-wide Parquet footer cache: parts are immutable, so entries
    // never invalidate — the LRU bound is the only policy.
    let metadata_cache = Arc::new(ParquetMetadataCache::default());

    let mut tasks: tokio::task::JoinSet<anyhow::Result<&'static str>> = tokio::task::JoinSet::new();

    // Listeners come up BEFORE the catalog is touched (plan 42). A process that
    // starts during a writer failover must be able to answer liveness and report
    // "starting, catalog unavailable" — not die before it has an endpoint to say
    // it on, which is how a rolling deploy turns a failover into a crash loop.
    if cfg.roles.contains(&Role::Query) {
        let state = AppState {
            catalog: catalog.clone(),
            store: store.clone(),
            store_url: store_url.clone(),
            statement_timeout: std::time::Duration::from_secs(cfg.query.statement_timeout_secs),
            metadata_cache: metadata_cache.clone(),
        };
        let listener = tokio::net::TcpListener::bind(&cfg.query.listen)
            .await
            .with_context(|| format!("binding query listener on {}", cfg.query.listen))?;
        let addr = listener.local_addr()?;
        tracing::info!(%addr, "query server listening");
        if let Some(tx) = bound {
            let _ = tx.send(addr);
        }
        let served = crate::metrics::route(router(state), prom.clone());
        let token = shutdown.clone();
        // The query role holds no catalog worker state to reconcile: once it is
        // serving, it is doing its job. Whether it can *admit* a new query during
        // an outage is a separate question, answered by readiness (Task 4).
        health.role(Role::Query).healthy();
        tasks.spawn(async move {
            let _up = WorkerUp::new("query");
            axum::serve(listener, served)
                .with_graceful_shutdown(async move { token.cancelled().await })
                .await
                .context("query server")?;
            Ok("query")
        });
    }

    // Standalone /metrics listener (metrics P2): serves the P1 recorder for
    // processes without the query role. Bound only when explicitly set; an
    // unconfigured query-role-less process makes the P1 gap loud instead of
    // silently emitting metrics nothing serves.
    if !cfg.metrics_listen.is_empty() {
        let listener = tokio::net::TcpListener::bind(&cfg.metrics_listen)
            .await
            .with_context(|| format!("binding metrics listener on {}", cfg.metrics_listen))?;
        tracing::info!(addr = %listener.local_addr()?, "standalone metrics listener");
        let served = crate::metrics::route(
            axum::Router::new().route("/healthz", axum::routing::get(|| async { "ok" })),
            prom.clone(),
        );
        let token = shutdown.clone();
        tasks.spawn(async move {
            axum::serve(listener, served)
                .with_graceful_shutdown(async move { token.cancelled().await })
                .await
                .context("metrics listener")?;
            Ok("metrics-listener")
        });
    } else if !cfg.roles.contains(&Role::Query) {
        tracing::warn!(
            "metrics have no listener: no query role and metrics_listen unset — set metrics_listen"
        );
    }

    // Migrations and bootstrap go through the same bounded, jittered recovery as
    // everything else: a failover during a rolling deploy should *delay* startup,
    // not crash-loop it against the primary that is trying to come back. A
    // permanent error (bad schema, bad config) still terminates immediately.
    //
    // Ordinary replicas still run migrations here. A dedicated migration job is
    // HA phase 3 and remains a documented follow-up.
    let startup = health.role(*cfg.roles.first().unwrap_or(&Role::Query));
    supervisor::with_catalog_recovery(
        "running migrations",
        &catalog,
        &health,
        &startup,
        &policy,
        &shutdown,
        || catalog.migrate(),
    )
    .await?;
    supervisor::with_catalog_recovery(
        "bootstrapping tables",
        &catalog,
        &health,
        &startup,
        &policy,
        &shutdown,
        || crate::bootstrap::apply(&catalog, &cfg.tables),
    )
    .await?;

    if cfg.roles.contains(&Role::Ingest) {
        let routes: Vec<TableRoute> = cfg
            .tables
            .iter()
            .filter_map(|t| {
                t.topic.as_ref().map(|topic| TableRoute {
                    topic: topic.clone(),
                    hypertable: t.name.clone(),
                    ts_column: t.ts_column.clone(),
                })
            })
            .collect();
        if routes.is_empty() {
            tracing::warn!("ingest role enabled but no [[tables]] declares a topic");
        } else {
            let ingest_cfg = IngestConfig {
                brokers: cfg.ingest.brokers.clone(),
                group_id: cfg.ingest.group_id.clone(),
                flush_interval_ms: cfg.ingest.flush_interval_ms,
                max_buffer_rows: cfg.ingest.max_buffer_rows,
                l0_slowdown_parts: cfg.ingest.l0_slowdown_parts,
                l0_stop_parts: cfg.ingest.l0_stop_parts,
                warn_partitions_per_flush: cfg.ingest.warn_partitions_per_flush,
                max_event_age_days: cfg.ingest.max_event_age_days,
                max_event_future_secs: cfg.ingest.max_event_future_secs,
                tables: routes,
            };
            // Rebuilt from scratch on every attempt — a fresh consumer, empty
            // buffers, and offsets reloaded from the catalog. Nothing a dropped
            // worker was holding is allowed to survive into the next one.
            let (catalog, store, token) = (catalog.clone(), store.clone(), shutdown.clone());
            let build = move || -> supervisor::RoleRun<'static, ukiel_ingest::IngestError> {
                let worker = IngestWorker::new(catalog.clone(), store.clone(), ingest_cfg.clone());
                let token = token.clone();
                Box::pin(async move { worker.run(token).await })
            };
            spawn_supervised(
                &mut tasks,
                Role::Ingest,
                build,
                &catalog_for_roles,
                &health,
                &policy,
                &shutdown,
            );
        }
    }

    if cfg.roles.contains(&Role::Compactor) {
        // The lease identity is the *process*, allocated once outside the
        // rebuild closure: a compactor that is reconstructed after an outage is
        // still the same owner, so it can reclaim its own partitions rather than
        // wait out its own leases (plan 41).
        tracing::info!(owner_id = %instance_id, "compactor lease owner");
        let compactor_cfg = CompactorConfig {
            poll_interval_ms: cfg.compactor.poll_interval_ms,
            l0_fanout: cfg.compactor.l0_fanout,
            fanout: cfg.compactor.fanout,
            finalize_after_secs: cfg.compactor.finalize_after_secs,
            finalize_poll_interval_ms: cfg.compactor.finalize_poll_interval_ms,
            candidate_limit: cfg.compactor.candidate_limit,
            lease_ttl_ms: cfg.compactor.lease_ttl_secs * 1_000,
            lease_renew_interval_ms: cfg.compactor.lease_renew_interval_secs * 1_000,
            owner_id: Some(instance_id),
            ..CompactorConfig::default()
        };
        let (catalog, store, token) = (catalog.clone(), store.clone(), shutdown.clone());
        let build = move || -> supervisor::RoleRun<'static, ukiel_compactor::CompactorError> {
            let compactor = Compactor::new(catalog.clone(), store.clone(), compactor_cfg.clone());
            let token = token.clone();
            Box::pin(async move { compactor.run(token).await })
        };
        spawn_supervised(
            &mut tasks,
            Role::Compactor,
            build,
            &catalog_for_roles,
            &health,
            &policy,
            &shutdown,
        );
    }

    if cfg.roles.contains(&Role::Gc) {
        let gc_cfg = GcConfig {
            poll_interval_ms: cfg.gc.poll_interval_ms,
            tombstone_grace_secs: cfg.gc.tombstone_grace_secs,
            orphan_grace_secs: cfg.gc.orphan_grace_secs,
            ..GcConfig::default()
        };
        let (catalog, store, token) = (catalog.clone(), store.clone(), shutdown.clone());
        let build = move || -> supervisor::RoleRun<'static, ukiel_gc::GcError> {
            let gc = Gc::new(catalog.clone(), store.clone(), gc_cfg.clone());
            let token = token.clone();
            Box::pin(async move { gc.run(token).await })
        };
        spawn_supervised(
            &mut tasks,
            Role::Gc,
            build,
            &catalog_for_roles,
            &health,
            &policy,
            &shutdown,
        );
    }

    // Periodic gauge collector (metrics P2): lag, backlog, pool, and disk
    // families. Role-independent, like metrics-upkeep below.
    crate::collector::spawn(&mut tasks, &cfg, catalog.clone(), shutdown.clone());

    // Drain histogram buffers between scrapes; without upkeep they grow
    // unboundedly (metrics P1). Runs regardless of the query role.
    {
        let prom = prom.clone();
        let token = shutdown.clone();
        tasks.spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(5));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    _ = token.cancelled() => return Ok("metrics-upkeep"),
                    _ = tick.tick() => prom.run_upkeep(),
                }
            }
        });
    }

    // First failure cancels everything; clean exits are logged as they land.
    let mut result = Ok(());
    while let Some(joined) = tasks.join_next().await {
        match joined.expect("worker task panicked") {
            Ok(role) => tracing::info!(role, "worker stopped"),
            Err(e) => {
                tracing::error!(error = %e, "worker failed; shutting down");
                shutdown.cancel();
                if result.is_ok() {
                    result = Err(e);
                }
            }
        }
    }
    result
}

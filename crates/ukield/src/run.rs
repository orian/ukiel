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
use ukiel_query::cache::CachingObjectStore;
use ukiel_query::server::{AppState, router};

use crate::config::{ObjectStoreConfig, Role, UkieldConfig};

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

    let catalog = PostgresCatalog::connect(&cfg.catalog.url)
        .await
        .context("connecting to catalog")?;
    catalog.migrate().await.context("running migrations")?;

    let (base_store, store_url) = build_store(&cfg.object_store)?;
    let store: Arc<dyn ObjectStore> = if cfg.query.cache_dir.is_empty() {
        base_store
    } else {
        Arc::new(CachingObjectStore::new(
            base_store,
            PathBuf::from(&cfg.query.cache_dir),
        ))
    };

    crate::bootstrap::apply(&catalog, &cfg.tables).await?;

    let mut tasks: tokio::task::JoinSet<anyhow::Result<&'static str>> = tokio::task::JoinSet::new();

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
            let worker = IngestWorker::new(
                catalog.clone(),
                store.clone(),
                IngestConfig {
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
                },
            );
            let token = shutdown.clone();
            tasks.spawn(async move {
                let _up = WorkerUp::new("ingest");
                worker.run(token).await.context("ingest worker")?;
                Ok("ingest")
            });
        }
    }

    if cfg.roles.contains(&Role::Query) {
        let state = AppState {
            catalog: catalog.clone(),
            store: store.clone(),
            store_url: store_url.clone(),
            statement_timeout: std::time::Duration::from_secs(cfg.query.statement_timeout_secs),
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
        tasks.spawn(async move {
            let _up = WorkerUp::new("query");
            axum::serve(listener, served)
                .with_graceful_shutdown(async move { token.cancelled().await })
                .await
                .context("query server")?;
            Ok("query")
        });
    }

    if cfg.roles.contains(&Role::Compactor) {
        let compactor = Compactor::new(
            catalog.clone(),
            store.clone(),
            CompactorConfig {
                poll_interval_ms: cfg.compactor.poll_interval_ms,
                l0_fanout: cfg.compactor.l0_fanout,
                fanout: cfg.compactor.fanout,
                finalize_after_secs: cfg.compactor.finalize_after_secs,
                finalize_poll_interval_ms: cfg.compactor.finalize_poll_interval_ms,
                max_merge_input_bytes: (cfg.compactor.max_merge_input_mb as i64) * 1024 * 1024,
                ..CompactorConfig::default()
            },
        );
        let token = shutdown.clone();
        tasks.spawn(async move {
            let _up = WorkerUp::new("compactor");
            compactor.run(token).await.context("compactor")?;
            Ok("compactor")
        });
    }

    if cfg.roles.contains(&Role::Gc) {
        let gc = Gc::new(
            catalog.clone(),
            store.clone(),
            GcConfig {
                poll_interval_ms: cfg.gc.poll_interval_ms,
                tombstone_grace_secs: cfg.gc.tombstone_grace_secs,
                orphan_grace_secs: cfg.gc.orphan_grace_secs,
                ..GcConfig::default()
            },
        );
        let token = shutdown.clone();
        tasks.spawn(async move {
            let _up = WorkerUp::new("gc");
            gc.run(token).await.context("gc")?;
            Ok("gc")
        });
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

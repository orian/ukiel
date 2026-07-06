//! Object garbage collection: reap tombstoned parts, sweep orphaned objects.
//!
//! Safety model (see the plan/spec): grace periods protect in-flight readers
//! and in-flight commits; the worker-cursor fence protects lagging change-feed
//! consumers. Catalog rows of purged parts are kept (stamped `purged_at`) so
//! feed replay works and the sweeper can distinguish purged from orphaned.

use std::sync::Arc;

use object_store::ObjectStore;
use object_store::ObjectStoreExt;
use object_store::path::Path;
use ukiel_catalog::PostgresCatalog;
use ukiel_core::Hypertable;

#[derive(Debug, thiserror::Error)]
pub enum GcError {
    #[error(transparent)]
    Catalog(#[from] ukiel_catalog::CatalogError),
    #[error(transparent)]
    ObjectStore(#[from] object_store::Error),
}

#[derive(Debug, Clone)]
pub struct GcConfig {
    /// Seconds after the deleting commit before a tombstoned part's object
    /// may be removed (protects queries planned just before the REPLACE).
    pub tombstone_grace_secs: f64,
    /// Minimum object age before an uncommitted object counts as an orphan
    /// (protects uploads whose commit is still in flight).
    pub orphan_grace_secs: i64,
    /// Loop tick for `run`.
    pub poll_interval_ms: u64,
}

impl Default for GcConfig {
    fn default() -> Self {
        Self {
            tombstone_grace_secs: 900.0,
            orphan_grace_secs: 3600,
            poll_interval_ms: 60_000,
        }
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct GcStats {
    pub reaped_parts: usize,
    pub swept_orphans: usize,
}

pub struct Gc {
    catalog: PostgresCatalog,
    store: Arc<dyn ObjectStore>,
    config: GcConfig,
}

impl Gc {
    pub fn new(catalog: PostgresCatalog, store: Arc<dyn ObjectStore>, config: GcConfig) -> Self {
        Self {
            catalog,
            store,
            config,
        }
    }

    /// One pass over every hypertable: reap, then sweep.
    pub async fn run_once(&self) -> Result<GcStats, GcError> {
        let mut stats = GcStats::default();
        for hypertable in self.catalog.list_hypertables().await? {
            self.reap(&hypertable, &mut stats).await?;
            self.sweep(&hypertable, &mut stats).await?;
        }
        Ok(stats)
    }

    /// Deletes objects of safely-reapable tombstoned parts, then stamps them
    /// purged. Delete-then-stamp order: a crash in between re-deletes on the
    /// next pass, and a missing object counts as already deleted.
    async fn reap(&self, hypertable: &Hypertable, stats: &mut GcStats) -> Result<(), GcError> {
        let parts = self
            .catalog
            .reapable_parts(hypertable.id, self.config.tombstone_grace_secs)
            .await?;
        if parts.is_empty() {
            return Ok(());
        }
        for part in &parts {
            match self.store.delete(&Path::from(part.meta.path.clone())).await {
                Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
                Err(e) => return Err(e.into()),
            }
        }
        self.catalog
            .mark_purged(&parts.iter().map(|p| p.id).collect::<Vec<_>>())
            .await?;
        stats.reaped_parts += parts.len();
        Ok(())
    }

    /// Deletes objects for orphaned upload-intents past the grace, then clears
    /// their intent rows. No object-store listing — orphans are a catalog query
    /// (see `reconcile` for the listing-based backstop).
    async fn sweep(&self, hypertable: &Hypertable, stats: &mut GcStats) -> Result<(), GcError> {
        let orphans = self
            .catalog
            .orphaned_pending_objects(hypertable.id, self.config.orphan_grace_secs as f64)
            .await?;
        if orphans.is_empty() {
            return Ok(());
        }
        for path in &orphans {
            match self.store.delete(&Path::from(path.clone())).await {
                Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
                Err(e) => return Err(e.into()),
            }
        }
        self.catalog.clear_pending_objects(&orphans).await?;
        stats.swept_orphans += orphans.len();
        Ok(())
    }

    /// Ticks `run_once` until cancelled.
    pub async fn run(&self, shutdown: tokio_util::sync::CancellationToken) -> Result<(), GcError> {
        let mut ticker = tokio::time::interval(std::time::Duration::from_millis(
            self.config.poll_interval_ms,
        ));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => return Ok(()),
                _ = ticker.tick() => {
                    let stats = self.run_once().await?;
                    if stats != GcStats::default() {
                        tracing::info!(?stats, "gc pass");
                    }
                }
            }
        }
    }
}

//! Object garbage collection: reap tombstoned parts, sweep orphaned objects.
//!
//! Safety model (see the plan/spec): grace periods protect in-flight readers
//! and in-flight commits; the worker-cursor fence protects lagging change-feed
//! consumers. Catalog rows of purged parts are kept (stamped `purged_at`) so
//! feed replay works and the sweeper can distinguish purged from orphaned.

use std::sync::Arc;

use futures::TryStreamExt;
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
    /// Run the listing-based `reconcile` backstop every Nth `run` tick
    /// (0 = never). Reconcile catches objects uploaded without an intent row.
    pub reconcile_every_n_passes: u64,
    /// Fired after the first pass that successfully read authoritative
    /// candidates and fences (plan 42). GC is ready when it has re-derived what
    /// is safe to delete — never on a ping, because a ping authorizes nothing.
    pub ready: Option<ukiel_core::ReadySignal>,
}

impl Default for GcConfig {
    fn default() -> Self {
        Self {
            tombstone_grace_secs: 900.0,
            orphan_grace_secs: 3600,
            poll_interval_ms: 60_000,
            reconcile_every_n_passes: 60,
            ready: None,
        }
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct GcStats {
    pub reaped_parts: usize,
    pub swept_orphans: usize,
    pub reconciled_orphans: usize,
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
        // Metrics P1: normal-operation counters.
        metrics::counter!("gc_reaped_parts_total").increment(stats.reaped_parts as u64);
        metrics::counter!("gc_swept_orphans_total").increment(stats.swept_orphans as u64);
        Ok(stats)
    }

    /// Deletes objects of safely-reapable tombstoned parts, then stamps them
    /// purged — **one authorized candidate at a time** (plan 42).
    ///
    /// Each iteration re-asks the catalog for the next candidate, so a catalog
    /// that dies mid-pass stops the pass. It cannot authorize the deletion of a
    /// second object from a list nothing can still vouch for: after a failover
    /// the new primary may not even hold the commit that tombstoned these parts,
    /// and then "safe to delete" would describe objects that are still live.
    /// Delayed reclamation is cheap; deleting referenced data is not (HA7).
    ///
    /// Delete-then-stamp order is unchanged: a crash in between re-deletes next
    /// pass, and a missing object counts as already deleted.
    async fn reap(&self, hypertable: &Hypertable, stats: &mut GcStats) -> Result<(), GcError> {
        loop {
            let next = self
                .catalog
                .next_reapable_part(hypertable.id, self.config.tombstone_grace_secs)
                .await
                .inspect_err(|_| {
                    metrics::counter!("gc_destructive_pass_aborted_total", "phase" => "reap")
                        .increment(1);
                })?;
            let Some(part) = next else { return Ok(()) };

            match self.store.delete(&Path::from(part.meta.path.clone())).await {
                Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
                Err(e) => return Err(e.into()),
            }
            // If this stamp fails, the object is already gone and the row is not
            // marked — the next recovered pass sees the same candidate, gets
            // `NotFound` from the store (idempotent), and stamps it then.
            self.catalog
                .mark_purged(&[part.id])
                .await
                .inspect_err(|_| {
                    metrics::counter!("gc_destructive_pass_aborted_total", "phase" => "reap")
                        .increment(1);
                })?;
            stats.reaped_parts += 1;
        }
    }

    /// Deletes objects for orphaned upload-intents past the grace, then clears
    /// their intent rows — one authorized candidate at a time, for the same
    /// reason as `reap`. No object-store listing: orphans are a catalog query
    /// (see `reconcile` for the listing-based backstop).
    async fn sweep(&self, hypertable: &Hypertable, stats: &mut GcStats) -> Result<(), GcError> {
        loop {
            let next = self
                .catalog
                .next_orphaned_pending_object(hypertable.id, self.config.orphan_grace_secs as f64)
                .await
                .inspect_err(|_| {
                    metrics::counter!("gc_destructive_pass_aborted_total", "phase" => "sweep")
                        .increment(1);
                })?;
            let Some(path) = next else { return Ok(()) };

            match self.store.delete(&Path::from(path.clone())).await {
                Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
                Err(e) => return Err(e.into()),
            }
            self.catalog
                .clear_pending_objects(std::slice::from_ref(&path))
                .await
                .inspect_err(|_| {
                    metrics::counter!("gc_destructive_pass_aborted_total", "phase" => "sweep")
                        .increment(1);
                })?;
            stats.swept_orphans += 1;
        }
    }

    /// Defense-in-depth: lists objects under each hypertable's prefix and
    /// deletes any older than the orphan grace that the catalog knows nothing
    /// about — neither a non-purged part nor a pending intent. Costs one
    /// object-store LIST per hypertable, so it runs rarely (see
    /// `reconcile_every_n_passes`).
    pub async fn reconcile_once(&self) -> Result<GcStats, GcError> {
        let mut stats = GcStats::default();
        for hypertable in self.catalog.list_hypertables().await? {
            let mut known: std::collections::HashSet<String> = self
                .catalog
                .all_part_paths(hypertable.id)
                .await?
                .into_iter()
                .collect();
            known.extend(self.catalog.all_pending_paths(hypertable.id).await?);

            let prefix = Path::from(format!("ht/{}", hypertable.id));
            let objects: Vec<_> = self.store.list(Some(&prefix)).try_collect().await?;
            let now = chrono::Utc::now();
            let mut deleted_any = false;
            for meta in objects {
                if known.contains(meta.location.as_ref()) {
                    continue;
                }
                if (now - meta.last_modified).num_seconds() < self.config.orphan_grace_secs {
                    continue;
                }
                // The known-set was derived from the catalog before this listing.
                // Before every *subsequent* deletion, re-confirm the catalog is
                // still there: if it is not, that set may no longer describe the
                // truth, and this loop would keep deleting objects on its word.
                // Aborting costs a delayed sweep; continuing could cost data.
                if deleted_any {
                    self.catalog.ping().await.inspect_err(|_| {
                        metrics::counter!(
                            "gc_destructive_pass_aborted_total", "phase" => "reconcile"
                        )
                        .increment(1);
                    })?;
                }
                match self.store.delete(&meta.location).await {
                    Ok(()) | Err(object_store::Error::NotFound { .. }) => {
                        stats.reconciled_orphans += 1;
                        deleted_any = true;
                    }
                    Err(e) => return Err(e.into()),
                }
            }
        }
        // Metrics P1: should stay ~zero forever — nonzero means a writer
        // uploaded without registering intent (plan-9 invariant violated).
        metrics::counter!("gc_reconcile_orphans_total").increment(stats.reconciled_orphans as u64);
        Ok(stats)
    }

    /// Ticks `run_once` until cancelled.
    pub async fn run(&self, shutdown: tokio_util::sync::CancellationToken) -> Result<(), GcError> {
        let mut ticker = tokio::time::interval(std::time::Duration::from_millis(
            self.config.poll_interval_ms,
        ));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut pass: u64 = 0;
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => return Ok(()),
                _ = ticker.tick() => {
                    let mut stats = self.run_once().await?;
                    // A completed pass means candidates and fences were read
                    // from authority: GC's definition of reconciled (plan 42).
                    ukiel_core::signal_ready(&self.config.ready);
                    pass += 1;
                    if self.config.reconcile_every_n_passes > 0
                        && pass.is_multiple_of(self.config.reconcile_every_n_passes)
                    {
                        stats.reconciled_orphans += self.reconcile_once().await?.reconciled_orphans;
                    }
                    if stats != GcStats::default() {
                        tracing::info!(?stats, "gc pass");
                    }
                }
            }
        }
    }
}

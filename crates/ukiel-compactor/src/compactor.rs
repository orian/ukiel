//! Fanout-driven level-ladder compaction. The change feed (via the durable
//! cursor) is only a wake-up signal; every pass plans from current
//! live-parts state, so passes are idempotent and REPLACE conflicts are
//! safely retried next pass. A run = one commit's key-disjoint output set;
//! `fanout` runs at a level merge into one run a level up.

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use futures::FutureExt;
use object_store::ObjectStore;
use ukiel_catalog::{CatalogError, PostgresCatalog};
use ukiel_core::{CommitOp, Hypertable, Part, PartMeta};

use crate::CompactorError;
use crate::rewrite;

/// Seconds since the Unix epoch for liveness gauges (metrics P1). The
/// workflow-script `Date` ban does not apply to crate runtime code.
fn unix_secs() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

#[derive(Debug, Clone)]
pub struct CompactorConfig {
    /// Cursor identity in `worker_cursors`.
    pub worker_name: String,
    /// Merge a partition's L0 files once at least this many exist. L0 files
    /// span ~all tenants — no key pruning applies, so every query reads
    /// every live L0 file: keep this low. L0 files are tiny; the merges
    /// are cheap.
    pub l0_fanout: usize,
    /// Run-count merge trigger for L1 and above. A run = the key-disjoint
    /// output of one commit (distinct `created_by_commit` among live
    /// parts); a query touches <= 1 file per run, so extra runs are cheap
    /// to leave lying around while their merges move real bytes — keep this
    /// higher than `l0_fanout`. Read amp per key <= l0_fanout + fanout x
    /// depth while a partition is hot.
    pub fanout: usize,
    /// A partition with no L0 arrivals for this long and more than one live
    /// run is folded into a single run one level above its maximum
    /// (finalization; see `finalize_once`).
    pub finalize_after_secs: u64,
    /// Cadence of the finalization sweep inside `run`.
    pub finalize_poll_interval_ms: u64,
    /// Loop tick for the merge pass in `run`.
    pub poll_interval_ms: u64,
    /// Most candidate partitions one pass considers per hypertable. Bounds the
    /// pass, not the backlog: the rest come back next tick.
    pub candidate_limit: i64,
}

impl Default for CompactorConfig {
    fn default() -> Self {
        Self {
            worker_name: "compactor".to_string(),
            // Both 2: preserves the old `min_l0_files = 2` test behavior.
            // Production tuning lives in the ukield config defaults (4/10).
            l0_fanout: 2,
            fanout: 2,
            finalize_after_secs: 3600,
            finalize_poll_interval_ms: 60_000,
            poll_interval_ms: 5000,
            candidate_limit: 64,
        }
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct CompactionStats {
    pub merged_groups: usize,
    pub parts_in: usize,
    pub parts_out: usize,
    pub conflicts: usize,
}

pub struct Compactor {
    catalog: PostgresCatalog,
    store: Arc<dyn ObjectStore>,
    config: CompactorConfig,
}

impl Compactor {
    pub fn new(
        catalog: PostgresCatalog,
        store: Arc<dyn ObjectStore>,
        config: CompactorConfig,
    ) -> Self {
        Self {
            catalog,
            store,
            config,
        }
    }

    /// Ticks `run_once` until cancelled. Conflicts are expected under
    /// concurrency and are never fatal; real errors stop the worker.
    pub async fn run(
        &self,
        shutdown: tokio_util::sync::CancellationToken,
    ) -> Result<(), CompactorError> {
        let mut ticker = tokio::time::interval(std::time::Duration::from_millis(
            self.config.poll_interval_ms,
        ));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut finalize_ticker = tokio::time::interval(std::time::Duration::from_millis(
            self.config.finalize_poll_interval_ms,
        ));
        finalize_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => return Ok(()),
                _ = ticker.tick() => {
                    let stats = self.run_once().await?;
                    if stats.merged_groups > 0 || stats.conflicts > 0 {
                        tracing::info!(?stats, "compaction pass");
                    }
                }
                _ = finalize_ticker.tick() => {
                    let partitions = self.finalize_once().await?;
                    if partitions > 0 {
                        tracing::info!(partitions, "finalized cold partitions");
                    }
                }
            }
        }
    }

    /// One idempotent pass over every hypertable.
    pub async fn run_once(&self) -> Result<CompactionStats, CompactorError> {
        let started = std::time::Instant::now();
        let mut stats = CompactionStats::default();
        let mut result = Ok(());
        for hypertable in self.catalog.list_hypertables().await? {
            if let Err(e) = self.compact_hypertable(&hypertable, &mut stats).await {
                result = Err(e);
                break;
            }
        }

        // Metrics P1: pass-level throughput/latency and a liveness timestamp
        // that survives error-looping (only bumped on a clean pass).
        metrics::histogram!("compactor_pass_duration_seconds")
            .record(started.elapsed().as_secs_f64());
        metrics::counter!("compactor_merges_total").increment(stats.merged_groups as u64);
        metrics::counter!("compactor_parts_in_total").increment(stats.parts_in as u64);
        metrics::counter!("compactor_parts_out_total").increment(stats.parts_out as u64);
        metrics::counter!("compactor_conflicts_total").increment(stats.conflicts as u64);
        if result.is_ok() {
            metrics::gauge!("compactor_last_success_timestamp").set(unix_secs());
        }

        result.map(|()| stats)
    }

    /// One finalization sweep: every partition with no L0 arrivals for
    /// `finalize_after_secs` and more than one live run is folded into a
    /// single run one level above its current maximum. After it, all of the
    /// partition's files are pairwise key-disjoint: one file chain per key,
    /// exact range pruning, metadata-only deletion for dedicated keys.
    /// Not gated on the change feed — a cold partition emits no events.
    /// Returns the number of partitions finalized.
    pub async fn finalize_once(&self) -> Result<usize, CompactorError> {
        let mut finalized = 0;
        for hypertable in self.catalog.list_hypertables().await? {
            // Aggregated in SQL: only partitions with >1 live run come back,
            // as bare partition values — no part rows cross the wire here.
            for partition in self.catalog.finalize_candidates(hypertable.id).await? {
                let quiet = self
                    .catalog
                    .partition_l0_quiet_since(
                        hypertable.id,
                        &partition,
                        self.config.finalize_after_secs as f64,
                    )
                    .await?;
                if !quiet {
                    continue;
                }
                let parts = self
                    .catalog
                    .live_partition_parts(hypertable.id, &partition)
                    .await?;
                // Raced since the candidate query: recheck before merging.
                let runs: HashSet<i64> = parts.iter().map(|p| p.created_by_commit.0).collect();
                if runs.len() < 2 {
                    continue;
                }
                let top = parts.iter().map(|p| p.meta.level).max().unwrap_or(0);
                match self.merge_parts(&hypertable, &parts, top + 1).await {
                    Ok(_) => finalized += 1,
                    // Raced by a merge/deletion: replan next sweep.
                    Err(CompactorError::Catalog(CatalogError::Conflict { .. })) => {}
                    Err(e) => return Err(e),
                }
            }
        }
        Ok(finalized)
    }

    async fn compact_hypertable(
        &self,
        hypertable: &Hypertable,
        stats: &mut CompactionStats,
    ) -> Result<(), CompactorError> {
        // The work list is current live state, not the feed. A shared fleet
        // cursor can pass an event whose compaction a dying worker abandoned;
        // gating on "unread events exist" would then hide that backlog forever.
        let candidates = self
            .catalog
            .compaction_candidates(
                hypertable.id,
                self.config.l0_fanout as i64,
                self.config.fanout as i64,
                self.config.candidate_limit,
            )
            .await?;

        for partition in candidates {
            let parts = self
                .catalog
                .live_partition_parts(hypertable.id, &partition)
                .await?;
            // Raced since the candidate query: recheck against this snapshot.
            let Some((level, group)) = self.eligible_level(parts) else {
                continue;
            };
            match self.merge_parts(hypertable, &group, level + 1).await {
                Ok(parts_out) => {
                    stats.merged_groups += 1;
                    stats.parts_in += group.len();
                    stats.parts_out += parts_out;
                }
                // Another writer got here first: replan next pass.
                Err(CompactorError::Catalog(CatalogError::Conflict { .. })) => {
                    stats.conflicts += 1;
                }
                Err(e) => return Err(e),
            }
        }

        // Cursor advancement is wake/observability progress, never the work
        // ledger — so it happens whatever the merges did, and monotonically
        // (a lagging peer sharing this name cannot pull it back).
        let head = self.catalog.feed_head(hypertable.id).await?;
        self.catalog
            .set_cursor(&self.config.worker_name, hypertable.id, head)
            .await?;
        Ok(())
    }

    /// The lowest level of this partition whose live runs have reached its
    /// merge trigger, with that level's parts. One merge per partition per
    /// pass: bounded commits; the ladder cascades over subsequent passes.
    fn eligible_level(&self, parts: Vec<Part>) -> Option<(i16, Vec<Part>)> {
        // Runs per level: parts committed together form one key-disjoint run,
        // so distinct commits = distinct runs (each L0 file is its own ingest
        // commit).
        let mut by_level: BTreeMap<i16, Vec<Part>> = BTreeMap::new();
        for part in parts {
            by_level.entry(part.meta.level).or_default().push(part);
        }
        by_level.into_iter().find(|(level, group)| {
            let runs: HashSet<i64> = group.iter().map(|p| p.created_by_commit.0).collect();
            // Two-tier trigger: L0 files are unpruned by key (merge eagerly),
            // L1+ runs are key-disjoint (merge lazily).
            let trigger = if *level == 0 {
                self.config.l0_fanout
            } else {
                self.config.fanout
            };
            runs.len() >= trigger
        })
    }

    /// Streams the given live parts (all one partition) through a bounded-memory
    /// k-way merge into placement-shaped output files at `output_level`, then
    /// commits the swap (plan 29). Memory is O(K·batch + row-group), not
    /// O(decoded partition) — so an arbitrarily large cold partition no longer
    /// needs the plan-28 cap. Returns the number of output parts.
    async fn merge_parts(
        &self,
        hypertable: &Hypertable,
        group: &[Part],
        output_level: i16,
    ) -> Result<usize, CompactorError> {
        let cols = ukiel_core::TableColumns::parse(&hypertable.table_schema)?;
        let schema = Arc::new(cols.physical_schema());
        let partition_values = group[0].meta.partition_values.clone();

        // Async per-part streams -> k-way streaming merge over the already-sorted
        // inputs (never re-sorts; plan-27 ordering is the trust anchor).
        let streams = rewrite::part_streams(&self.store, schema.clone(), &cols, group).await?;
        let merged = crate::merge::merge_streams(streams, schema.clone(), &hypertable.sort_key);

        // Streaming writer: placement cuts on actual encoded size, whale-splits
        // big keys, and registers upload intent per file before its first byte.
        let opts = ukiel_core::WriteOpts::from_columns(&cols, &hypertable.sort_key, output_level);
        let ht_id = hypertable.id;
        let next_path: rewrite::NextPath = Box::new(move || {
            format!(
                "ht/{}/L{}/{}.parquet",
                ht_id.0,
                output_level,
                uuid::Uuid::new_v4()
            )
        });
        let catalog = self.catalog.clone();
        let on_open: rewrite::OnOpen = Box::new(move |path| {
            let catalog = catalog.clone();
            async move {
                catalog
                    .register_pending_objects(ht_id, &[path])
                    .await
                    .map_err(CompactorError::from)
            }
            .boxed()
        });
        let writer = rewrite::StreamingChunkWriter::new(
            self.store.clone(),
            schema,
            opts,
            hypertable.placement,
            hypertable.packing_key.clone(),
            hypertable.sort_key.clone(),
            next_path,
            on_open,
        );
        let outputs = writer.write(merged).await?;

        let new_parts: Vec<PartMeta> = outputs
            .into_iter()
            .map(|c| PartMeta {
                path: c.path,
                partition_values: partition_values.clone(),
                packing_key_min: c.key_min,
                packing_key_max: c.key_max,
                row_count: c.row_count,
                size_bytes: c.size_bytes,
                level: output_level,
                column_stats: c.column_stats,
            })
            .collect();

        let old = group.iter().map(|p| p.id).collect();
        let count = new_parts.len();
        // Conflict propagates as CatalogError::Conflict; uploaded objects are
        // then orphans — harmless, cleaned by GC's orphan sweep.
        self.catalog
            .commit(
                hypertable.id,
                CommitOp::Replace {
                    old,
                    new: new_parts,
                },
                None,
            )
            .await?;
        Ok(count)
    }
}

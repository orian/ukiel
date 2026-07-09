//! Fanout-driven level-ladder compaction. The change feed (via the durable
//! cursor) is only a wake-up signal; every pass plans from current
//! live-parts state, so passes are idempotent and REPLACE conflicts are
//! safely retried next pass. A run = one commit's key-disjoint output set;
//! `fanout` runs at a level merge into one run a level up.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use object_store::ObjectStore;
use object_store::ObjectStoreExt;
use object_store::path::Path;
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
    /// Skip any merge whose inputs' total size_bytes exceeds this (issue
    /// 0005: merges are in-memory until streaming merge lands; finalization
    /// would otherwise pull an arbitrarily large cold partition into RAM
    /// and OOM-crash-loop, since the sweep is stateless). Skipped merges
    /// warn and count `compactor_capped_merges_total`; the partition stays
    /// multi-run. `0` = unlimited.
    pub max_merge_input_bytes: i64,
    /// A partition with no L0 arrivals for this long and more than one live
    /// run is folded into a single run one level above its maximum
    /// (finalization; see `finalize_once`).
    pub finalize_after_secs: u64,
    /// Cadence of the finalization sweep inside `run`.
    pub finalize_poll_interval_ms: u64,
    /// Loop tick for the merge pass in `run`.
    pub poll_interval_ms: u64,
}

impl Default for CompactorConfig {
    fn default() -> Self {
        Self {
            worker_name: "compactor".to_string(),
            // Both 2: preserves the old `min_l0_files = 2` test behavior.
            // Production tuning lives in the ukield config defaults (4/10).
            l0_fanout: 2,
            fanout: 2,
            max_merge_input_bytes: 1 << 30,
            finalize_after_secs: 3600,
            finalize_poll_interval_ms: 60_000,
            poll_interval_ms: 5000,
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
                if self.merge_capped(&hypertable, &parts) {
                    continue; // stays multi-run until the cap is raised
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
        // Wake-up signal: skip hypertables with no feed activity since our cursor.
        let cursor = self
            .catalog
            .get_cursor(&self.config.worker_name, hypertable.id)
            .await?;
        let events = self
            .catalog
            .changes_since(hypertable.id, cursor, 1000)
            .await?;
        if events.is_empty() {
            return Ok(());
        }

        // Plan from live state, never from the events themselves.
        let live = self.catalog.live_parts(hypertable.id, None).await?;
        let mut partitions: HashMap<String, Vec<Part>> = HashMap::new();
        for part in live {
            partitions
                .entry(part.meta.partition_values.to_string())
                .or_default()
                .push(part);
        }

        for (_, parts) in partitions {
            // Runs per level: parts committed together form one key-disjoint
            // run, so distinct commits = distinct runs (each L0 file is its
            // own ingest commit).
            let mut by_level: BTreeMap<i16, Vec<Part>> = BTreeMap::new();
            for part in parts {
                by_level.entry(part.meta.level).or_default().push(part);
            }
            for (level, group) in by_level {
                let runs: HashSet<i64> = group.iter().map(|p| p.created_by_commit.0).collect();
                // Two-tier trigger: L0 files are unpruned by key (merge
                // eagerly), L1+ runs are key-disjoint (merge lazily).
                let trigger = if level == 0 {
                    self.config.l0_fanout
                } else {
                    self.config.fanout
                };
                if runs.len() < trigger {
                    continue;
                }
                if self.merge_capped(hypertable, &group) {
                    continue; // next level; don't break the ladder for this partition
                }
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
                // One merge per partition per pass: bounded commits; the
                // ladder cascades over subsequent passes.
                break;
            }
        }

        // Advance past everything we saw. Crash before this line only means
        // re-scanning: replanning from live state finds no L0s left to merge.
        let last = events.last().expect("non-empty").commit_id;
        self.catalog
            .set_cursor(&self.config.worker_name, hypertable.id, last)
            .await?;
        Ok(())
    }

    /// Issue 0005 interim guardrail (real fix: streaming merge, post-v1).
    /// True when the group's inputs exceed `max_merge_input_bytes` — the
    /// merge is skipped (warn + counter), never attempted.
    fn merge_capped(&self, hypertable: &Hypertable, group: &[Part]) -> bool {
        let cap = self.config.max_merge_input_bytes;
        if cap <= 0 {
            return false;
        }
        let total: i64 = group.iter().map(|p| p.meta.size_bytes).sum();
        if total <= cap {
            return false;
        }
        tracing::warn!(
            hypertable = %hypertable.name,
            partition = %group[0].meta.partition_values,
            input_bytes = total,
            cap_bytes = cap,
            parts = group.len(),
            "merge skipped: input exceeds max_merge_input_bytes (issue 0005)"
        );
        metrics::counter!("compactor_capped_merges_total").increment(1);
        true
    }

    /// Merges the given live parts (all one partition) into one run at
    /// `output_level` per the placement policy and commits the swap. The
    /// run's files are key-disjoint by construction (`plan_chunks`).
    /// Returns the number of output parts.
    async fn merge_parts(
        &self,
        hypertable: &Hypertable,
        group: &[Part],
        output_level: i16,
    ) -> Result<usize, CompactorError> {
        let cols = ukiel_core::TableColumns::parse(&hypertable.table_schema)?;
        let schema = Arc::new(cols.physical_schema());
        let merged = rewrite::read_parts_to_batch(&self.store, schema, group).await?;
        // Rewrites recompute write-time columns: this is what backfills a
        // materialized column added after these files were written.
        let merged = ukiel_expr::apply_defaults_and_materialized(merged, &cols)?;
        let sorted = rewrite::sort_batch(&merged, &hypertable.sort_key)?;
        let partition_values = group[0].meta.partition_values.clone();

        // Encoded-size estimate for SizeTargeted, from the inputs' stats.
        let total_bytes: i64 = group.iter().map(|p| p.meta.size_bytes).sum();
        let total_rows: i64 = group.iter().map(|p| p.meta.row_count).sum();
        let bytes_per_row = (total_bytes as f64 / total_rows.max(1) as f64).max(1.0);

        let outputs = rewrite::plan_chunks(
            &sorted,
            &hypertable.packing_key,
            &hypertable.placement,
            bytes_per_row,
        )?;
        // Layout for the output level (delta-ts, blooms, ZSTD at L1+, cap).
        let opts = ukiel_core::WriteOpts::from_columns(&cols, &hypertable.sort_key, output_level);

        // Assign paths and record upload intent before any upload: GC orphan
        // discovery is a catalog query over pending_objects (plan 9).
        let paths: Vec<String> = outputs
            .iter()
            .map(|_| {
                format!(
                    "ht/{}/L{}/{}.parquet",
                    hypertable.id,
                    output_level,
                    uuid::Uuid::new_v4()
                )
            })
            .collect();
        self.catalog
            .register_pending_objects(hypertable.id, &paths)
            .await?;

        let mut new_parts = Vec::with_capacity(outputs.len());
        for ((key_min, key_max, batch), path) in outputs.iter().zip(paths) {
            // Multi-key chunks (packed / SizeTargeted shared) get row groups
            // aligned to key boundaries so per-group key stats prune exactly;
            // single-key chunks (separated / whale) need no alignment.
            let bytes = if key_min != key_max {
                rewrite::batch_to_parquet_key_aligned(
                    batch,
                    &hypertable.packing_key,
                    &hypertable.sort_key,
                    &opts,
                )?
            } else {
                rewrite::batch_to_parquet_opts(batch, &hypertable.sort_key, &opts)?
            };
            let size_bytes = bytes.len() as i64;
            self.store
                .put(&Path::from(path.clone()), bytes.into())
                .await?;
            new_parts.push(PartMeta {
                path,
                partition_values: partition_values.clone(),
                packing_key_min: *key_min,
                packing_key_max: *key_max,
                row_count: batch.num_rows() as i64,
                size_bytes,
                level: output_level,
                column_stats: ukiel_core::stats::int64_column_stats(batch),
            });
        }

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

//! L0 -> L1 compaction. The change feed (via the durable cursor) is only a
//! wake-up signal; every pass plans from current live-parts state, so passes
//! are idempotent and REPLACE conflicts are safely retried next pass.

use std::collections::HashMap;
use std::sync::Arc;

use object_store::ObjectStore;
use object_store::ObjectStoreExt;
use object_store::path::Path;
use ukiel_catalog::{CatalogError, PostgresCatalog};
use ukiel_core::{CommitOp, Hypertable, Part, PartMeta, Placement};

use crate::CompactorError;
use crate::rewrite;

#[derive(Debug, Clone)]
pub struct CompactorConfig {
    /// Cursor identity in `worker_cursors`.
    pub worker_name: String,
    /// Merge a partition's L0 files once at least this many exist.
    pub min_l0_files: usize,
    /// Loop tick for `run` (Task 4).
    pub poll_interval_ms: u64,
}

impl Default for CompactorConfig {
    fn default() -> Self {
        Self {
            worker_name: "compactor".to_string(),
            min_l0_files: 2,
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
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => return Ok(()),
                _ = ticker.tick() => {
                    let stats = self.run_once().await?;
                    if stats.merged_groups > 0 || stats.conflicts > 0 {
                        tracing::info!(?stats, "compaction pass");
                    }
                }
            }
        }
    }

    /// One idempotent pass over every hypertable.
    pub async fn run_once(&self) -> Result<CompactionStats, CompactorError> {
        let mut stats = CompactionStats::default();
        for hypertable in self.catalog.list_hypertables().await? {
            self.compact_hypertable(&hypertable, &mut stats).await?;
        }
        Ok(stats)
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
        let mut groups: HashMap<String, Vec<Part>> = HashMap::new();
        for part in live.into_iter().filter(|p| p.meta.level == 0) {
            groups
                .entry(part.meta.partition_values.to_string())
                .or_default()
                .push(part);
        }

        for (_, group) in groups {
            if group.len() < self.config.min_l0_files {
                continue;
            }
            match self.merge_group(hypertable, &group).await {
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

        // Advance past everything we saw. Crash before this line only means
        // re-scanning: replanning from live state finds no L0s left to merge.
        let last = events.last().expect("non-empty").commit_id;
        self.catalog
            .set_cursor(&self.config.worker_name, hypertable.id, last)
            .await?;
        Ok(())
    }

    /// Merges one partition's L0 files into L1 output per the placement
    /// policy and commits the swap. Returns the number of output parts.
    async fn merge_group(
        &self,
        hypertable: &Hypertable,
        group: &[Part],
    ) -> Result<usize, CompactorError> {
        let cols = ukiel_core::TableColumns::parse(&hypertable.table_schema)?;
        let schema = Arc::new(cols.physical_schema());
        let merged = rewrite::read_parts_to_batch(&self.store, schema, group).await?;
        // Rewrites recompute write-time columns: this is what backfills a
        // materialized column added after these files were written.
        let merged = ukiel_expr::apply_defaults_and_materialized(merged, &cols)?;
        let sorted = rewrite::sort_batch(&merged, &hypertable.sort_key)?;
        let partition_values = group[0].meta.partition_values.clone();

        let outputs: Vec<(i64, i64, arrow::array::RecordBatch)> = match hypertable.placement {
            Placement::Packed => {
                let (min, max) = rewrite::key_range(&sorted, &hypertable.packing_key)?;
                vec![(min, max, sorted)]
            }
            Placement::Separated => rewrite::split_by_key(&sorted, &hypertable.packing_key)?
                .into_iter()
                .map(|(key, batch)| (key, key, batch))
                .collect(),
        };

        let mut new_parts = Vec::with_capacity(outputs.len());
        for (key_min, key_max, batch) in &outputs {
            let bytes = rewrite::batch_to_parquet(batch)?;
            let path = format!("ht/{}/L1/{}.parquet", hypertable.id, uuid::Uuid::new_v4());
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
                level: 1,
                column_stats: None,
            });
        }

        let old = group.iter().map(|p| p.id).collect();
        let count = new_parts.len();
        // Conflict propagates as CatalogError::Conflict; uploaded objects are
        // then orphans — harmless, cleaned by a future GC worker.
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

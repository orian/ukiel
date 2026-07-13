//! Fanout-driven level-ladder compaction. Every pass plans from current
//! live-parts state — the change feed is a wake-up signal, never the work
//! ledger — so passes are idempotent and REPLACE conflicts are safely retried
//! next pass. A run = one commit's key-disjoint output set; `fanout` runs at a
//! level merge into one run a level up.
//!
//! Under an active-active fleet, a worker claims a partition lease (plan 41)
//! *before* it opens a single Parquet file, renews it while it works, and has
//! that generation fenced inside the REPLACE transaction. Two layers, two jobs:
//! the lease stops peers duplicating expensive object work, and the optimistic
//! REPLACE — which ingest, deletion and operators race without any lease — stays
//! the correctness backstop.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::future::Future;
use std::sync::{Arc, Mutex};

use futures::FutureExt;
use object_store::ObjectStore;
use ukiel_catalog::{CandidateWindow, CatalogError, CompactionLease, PostgresCatalog};
use ukiel_core::{CompactionIntent, Hypertable, HypertableId, OperationIdentity, Part, PartMeta};
use uuid::Uuid;

use crate::CompactorError;
use crate::rewrite;

/// The version of "what merging these parts means" (plan 43). Inside the
/// operation fingerprint, so bumping it makes the same input group a *different*
/// operation. Bump it when a change would make the same inputs produce
/// materially different output — a new placement rule, a changed sort or
/// materialized-column semantic. Not for an ordinary release: a version that
/// moves on every deploy orphans every in-flight identity.
pub const COMPACTION_TRANSFORMATION_VERSION: u32 = 1;

/// The identity of "merge exactly these live parts into one run at
/// `output_level`". Built before a single Parquet file is opened, and free of
/// everything that belongs to the *attempt* rather than the intent: the output
/// paths (fresh UUIDs every time), the lease owner and generation, the sizes and
/// statistics only a completed merge knows.
pub fn compaction_identity_for(
    hypertable: &Hypertable,
    group: &[Part],
    output_level: i16,
) -> Result<OperationIdentity, CompactorError> {
    let input_parts: Vec<ukiel_core::PartId> = group.iter().map(|p| p.id).collect();
    Ok(OperationIdentity::compaction(
        hypertable.id,
        CompactionIntent {
            output_level,
            input_parts: &input_parts,
            partition_values: &group[0].meta.partition_values,
            table_schema: &hypertable.table_schema,
            sort_key: &hypertable.sort_key,
            placement: hypertable.placement,
            transformation_version: COMPACTION_TRANSFORMATION_VERSION,
        },
    )?)
}

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
    /// pass, not the backlog: a sweep cursor rotates through the rest on the
    /// following ticks, so no partition is stuck behind a hot prefix.
    pub candidate_limit: i64,
    /// How long a partition claim survives without renewal. It bounds failover
    /// delay (a dead owner's partitions idle this long) and duplicate work, not
    /// correctness: the REPLACE fence is what makes a late owner harmless.
    pub lease_ttl_ms: u64,
    /// Renewal cadence. Well under the TTL, so a transient scheduler or network
    /// stall costs a retry rather than the partition.
    pub lease_renew_interval_ms: u64,
    /// Process-instance identity for leases. `None` = generate one per
    /// `Compactor` (production); tests inject a stable id.
    pub owner_id: Option<uuid::Uuid>,
    /// Fired after the first pass that successfully read current live state
    /// (plan 42). *Discovery*, not a ping: this worker is ready when it has
    /// replanned from authority, not when the database merely answered.
    pub ready: Option<ukiel_core::ReadySignal>,
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
            lease_ttl_ms: 60_000,
            lease_renew_interval_ms: 20_000,
            owner_id: None,
            ready: None,
        }
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct CompactionStats {
    pub merged_groups: usize,
    pub parts_in: usize,
    pub parts_out: usize,
    pub conflicts: usize,
    /// Partitions this pass's windows offered — how far the sweep got, not how
    /// much work exists.
    pub candidates: usize,
    /// Partitions over their merge trigger, window or no window: the backlog
    /// the sweep is rotating through.
    pub eligible: usize,
    /// Eligible partitions a peer's unexpired lease kept out of those windows.
    pub leased_skips: usize,
    /// Sweeps that reached the tail of their eligible set and rotated back to
    /// the head (issue 0012). Zero for many consecutive passes over a large
    /// backlog means the rotation is not completing.
    pub sweep_wraps: usize,
}

/// Where each hypertable's sweep left off, keyed by hypertable id. The value is
/// Postgres's `partition_values::text` of the last partition offered, so the
/// next window resumes strictly after it.
///
/// Deliberately in memory: this is scheduling fairness, not durable state. A
/// restart resumes at the head of the sweep — the head is exactly what a fresh
/// process has no reason to skip — and the tail stays reachable because every
/// pass advances the cursor whether or not it managed to claim anything.
type SweepCursors = Mutex<HashMap<i64, String>>;

pub struct Compactor {
    catalog: PostgresCatalog,
    store: Arc<dyn ObjectStore>,
    config: CompactorConfig,
    /// This process instance's lease identity, stable for the compactor's life.
    /// Not a pod name: names are reused across restarts, and a reused identity
    /// would let a fresh process inherit a zombie's tenancy.
    owner_id: Uuid,
    ladder_cursors: SweepCursors,
    finalize_cursors: SweepCursors,
}

impl Compactor {
    pub fn new(
        catalog: PostgresCatalog,
        store: Arc<dyn ObjectStore>,
        config: CompactorConfig,
    ) -> Self {
        let owner_id = config.owner_id.unwrap_or_else(Uuid::new_v4);
        Self {
            catalog,
            store,
            config,
            owner_id,
            ladder_cursors: SweepCursors::default(),
            finalize_cursors: SweepCursors::default(),
        }
    }

    pub fn owner_id(&self) -> Uuid {
        self.owner_id
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
        // Fairness (issue 0012): eligible is the backlog, claimable is what this
        // fleet could take right now, and the gap between them is peer-held
        // work. A backlog that stays high while claimable stays near zero is a
        // fleet that has run out of parallelism, not a compactor that is stuck.
        set_candidate_gauges("ladder", &stats);
        if result.is_ok() {
            metrics::gauge!("compactor_last_success_timestamp").set(unix_secs());
            // A clean pass means candidates were discovered from current live
            // state — the compactor's definition of "reconciled" (plan 42).
            ukiel_core::signal_ready(&self.config.ready);
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
        let mut stats = CompactionStats::default();
        for hypertable in self.catalog.list_hypertables().await? {
            // Aggregated in SQL: only cold partitions with >1 live run come
            // back, as bare partition values — no part rows cross the wire here.
            // Same bounded rotating window as the ladder (issue 0012), and
            // quietness is *inside* the eligibility query: a window bounded on
            // ">1 live run" alone would be filled with hot partitions it can only
            // claim, probe and drop, and a cold one would wait for the cursor to
            // rotate past every one of them.
            let limit = self.config.candidate_limit;
            let quiet_secs = self.config.finalize_after_secs as f64;
            let window = self
                .sweep(
                    &self.finalize_cursors,
                    hypertable.id,
                    "finalize",
                    &mut stats,
                    |after: Option<String>| async move {
                        self.catalog
                            .finalize_candidates(
                                hypertable.id,
                                self.owner_id,
                                quiet_secs,
                                limit,
                                after.as_deref(),
                            )
                            .await
                    },
                )
                .await?;
            for partition in window.partitions {
                // The same partition lease the ladder takes: finalization and a
                // level merge consume the same live parts, so they must not
                // overlap. Claimed before the quiet/live-state rechecks, so the
                // snapshot those rechecks see cannot be rewritten under them.
                let Some(lease) = self.claim(hypertable.id, &partition, "finalize").await? else {
                    continue;
                };
                let result = self.finalize_partition(&hypertable, &lease).await;
                self.release(&lease).await;
                match result {
                    Ok(true) => finalized += 1,
                    Ok(false) => {}
                    // Raced by a merge/deletion, or handed off mid-merge:
                    // replan next sweep.
                    Err(CompactorError::Catalog(
                        CatalogError::Conflict { .. } | CatalogError::LeaseLost { .. },
                    )) => {}
                    Err(e) => return Err(e),
                }
            }
        }
        set_candidate_gauges("finalize", &stats);
        Ok(finalized)
    }

    /// Finalizes one claimed partition. `false` = it stopped being a candidate
    /// between the sweep query and the claim.
    async fn finalize_partition(
        &self,
        hypertable: &Hypertable,
        lease: &CompactionLease,
    ) -> Result<bool, CompactorError> {
        let partition = &lease.partition_values;
        let quiet = self
            .catalog
            .partition_l0_quiet_since(
                hypertable.id,
                partition,
                self.config.finalize_after_secs as f64,
            )
            .await?;
        if !quiet {
            return Ok(false);
        }
        let parts = self
            .catalog
            .live_partition_parts(hypertable.id, partition)
            .await?;
        // Raced since the candidate query: recheck before merging.
        let runs: HashSet<i64> = parts.iter().map(|p| p.created_by_commit.0).collect();
        if runs.len() < 2 {
            return Ok(false);
        }
        let top = parts.iter().map(|p| p.meta.level).max().unwrap_or(0);
        self.merge_under_lease(hypertable, lease, &parts, top + 1)
            .await?;
        Ok(true)
    }

    async fn compact_hypertable(
        &self,
        hypertable: &Hypertable,
        stats: &mut CompactionStats,
    ) -> Result<(), CompactorError> {
        // The work list is current live state, not the feed. A shared fleet
        // cursor can pass an event whose compaction a dying worker abandoned;
        // gating on "unread events exist" would then hide that backlog forever.
        let (l0_fanout, fanout) = (self.config.l0_fanout as i64, self.config.fanout as i64);
        let limit = self.config.candidate_limit;
        let window = self
            .sweep(
                &self.ladder_cursors,
                hypertable.id,
                "ladder",
                stats,
                |after: Option<String>| async move {
                    self.catalog
                        .compaction_candidates(
                            hypertable.id,
                            self.owner_id,
                            l0_fanout,
                            fanout,
                            limit,
                            after.as_deref(),
                        )
                        .await
                },
            )
            .await?;

        for partition in window.partitions {
            // Claim before any object work. Plan 40's finding: 32 same-partition
            // contenders land the same 58 useful swaps as 2, and in the product
            // every loser has already read, merged and uploaded Parquet by the
            // time REPLACE tells it so. The window already dropped the
            // partitions a peer held when it was built, but the claim — not the
            // query — is the authority: a peer can take one in between.
            let Some(lease) = self.claim(hypertable.id, &partition, "ladder").await? else {
                continue;
            };
            let result = self.compact_partition(hypertable, &lease, stats).await;
            self.release(&lease).await;
            result?;
        }

        // Cursor advancement is wake/observability progress, never the work
        // ledger — so it happens whatever the merges did, and monotonically
        // (a lagging peer sharing this name cannot pull it back). The sweep
        // cursor above is a different thing entirely: in-memory scheduling
        // state, private to this process, never consulted to decide whether
        // work exists.
        let head = self.catalog.feed_head(hypertable.id).await?;
        self.catalog
            .set_cursor(&self.config.worker_name, hypertable.id, head)
            .await?;
        Ok(())
    }

    /// The next window of one hypertable's sweep, with its cursor rotated past
    /// everything the window offered (issue 0012).
    ///
    /// Rotation is what makes the bound fair. A deterministic prefix re-serves
    /// the same head every pass, so a partition that stays eligible forever —
    /// deeply backlogged, or simply hot — can pin the window and starve the
    /// tail behind it. Advancing past the offered partitions, claimed or not,
    /// gives every eligible partition its turn within one rotation.
    ///
    /// A window the query could not fill means the eligible set is spent: the
    /// cursor resets and the next pass starts again at the head. Running off the
    /// tail outright is retried from the head immediately, so a wrap costs no
    /// idle tick.
    async fn sweep<F, Fut>(
        &self,
        cursors: &SweepCursors,
        hypertable_id: HypertableId,
        kind: &'static str,
        stats: &mut CompactionStats,
        window_at: F,
    ) -> Result<CandidateWindow, CompactorError>
    where
        F: Fn(Option<String>) -> Fut,
        Fut: Future<Output = Result<CandidateWindow, CatalogError>>,
    {
        let cursor = cursors
            .lock()
            .expect("sweep cursors")
            .get(&hypertable_id.0)
            .cloned();
        let resumed = cursor.is_some();
        let mut window = window_at(cursor).await?;

        let ran_off_tail = resumed && window.partitions.is_empty();
        if ran_off_tail {
            window = window_at(None).await?;
        }
        let exhausted = window.partitions.len() < self.config.candidate_limit as usize;

        let next = window.next_cursor.clone().filter(|_| !exhausted);
        match next {
            Some(next) => cursors
                .lock()
                .expect("sweep cursors")
                .insert(hypertable_id.0, next),
            None => cursors
                .lock()
                .expect("sweep cursors")
                .remove(&hypertable_id.0),
        };

        // A rotation completed. Not counted on an empty backlog, where every
        // tick would trivially "wrap" and drown the signal that matters: a large
        // eligible set whose rotation never finishes.
        let wrapped = window.eligible > 0 && (ran_off_tail || exhausted);
        stats.candidates += window.partitions.len();
        stats.eligible += window.eligible as usize;
        stats.leased_skips += window.leased as usize;
        stats.sweep_wraps += usize::from(wrapped);
        metrics::counter!("compactor_candidates_offered_total", "kind" => kind)
            .increment(window.partitions.len() as u64);
        metrics::counter!("compactor_sweep_wraps_total", "kind" => kind)
            .increment(u64::from(wrapped));
        Ok(window)
    }

    /// One ladder merge on a claimed partition, planned from the live state as
    /// of the claim (never from the candidate query, which may have raced).
    async fn compact_partition(
        &self,
        hypertable: &Hypertable,
        lease: &CompactionLease,
        stats: &mut CompactionStats,
    ) -> Result<(), CompactorError> {
        let parts = self
            .catalog
            .live_partition_parts(hypertable.id, &lease.partition_values)
            .await?;
        let Some((level, group)) = self.eligible_level(parts) else {
            return Ok(());
        };
        match self
            .merge_under_lease(hypertable, lease, &group, level + 1)
            .await
        {
            Ok(parts_out) => {
                stats.merged_groups += 1;
                stats.parts_in += group.len();
                stats.parts_out += parts_out;
            }
            // An ingest ADD cannot conflict, but a deletion rewrite can: it
            // takes no lease and tombstones live inputs under us. Replan.
            Err(CompactorError::Catalog(CatalogError::Conflict { .. })) => {
                stats.conflicts += 1;
            }
            // Handed off mid-merge (expiry, takeover, or a catalog we could not
            // reach to prove we still hold it). Expected churn under an
            // active-active fleet, not a worker fault: the new owner replans
            // from live state and redoes the work.
            Err(CompactorError::Catalog(CatalogError::LeaseLost { .. })) => {}
            Err(e) => return Err(e),
        }
        Ok(())
    }

    fn lease_ttl(&self) -> std::time::Duration {
        std::time::Duration::from_millis(self.config.lease_ttl_ms)
    }

    /// Tries to claim a partition. `None` = a live peer owns it: ordinary
    /// scheduling contention, not a failure.
    async fn claim(
        &self,
        hypertable_id: ukiel_core::HypertableId,
        partition: &serde_json::Value,
        kind: &'static str,
    ) -> Result<Option<CompactionLease>, CompactorError> {
        let lease = self
            .catalog
            .try_acquire_compaction_lease(hypertable_id, partition, self.owner_id, self.lease_ttl())
            .await?;
        let outcome = if lease.is_some() { "acquired" } else { "held" };
        metrics::counter!("compactor_lease_acquire_total", "outcome" => outcome).increment(1);
        if lease.is_some() {
            metrics::counter!("compactor_partitions_claimed_total", "kind" => kind).increment(1);
        }
        Ok(lease)
    }

    /// Hands a partition back so a peer can take it immediately. Best-effort:
    /// expiry is the crash fallback, so a failed release costs latency, never
    /// correctness — never the pass.
    async fn release(&self, lease: &CompactionLease) {
        let outcome = match self.catalog.release_compaction_lease(lease).await {
            Ok(true) => "released",
            // Already taken over (we were fenced out): nothing of ours to drop.
            Ok(false) => "stale",
            Err(e) => {
                tracing::warn!(error = %e, partition = %lease.partition_values,
                    "releasing compaction lease failed; it will expire");
                "error"
            }
        };
        metrics::counter!("compactor_lease_release_total", "outcome" => outcome).increment(1);
    }

    /// Runs a merge with a renewal loop beside it. The loop ticks on its own
    /// cadence — never gated on merge or commit progress — and the first proven
    /// loss (or a catalog it cannot reach to prove ownership) drops the merge
    /// future, stopping reads and uploads before REPLACE.
    ///
    /// Uploads that did complete stay registered as upload intent and become
    /// ordinary GC orphans. Losing a lease must never delete an object or clear
    /// an intent: the new owner's outputs are different files, and the old
    /// ones' only claim to existence is that intent row.
    async fn merge_under_lease(
        &self,
        hypertable: &Hypertable,
        lease: &CompactionLease,
        group: &[Part],
        output_level: i16,
    ) -> Result<usize, CompactorError> {
        // Named before any object work, from the live inputs and the output
        // level alone. If the commit's acknowledgement is lost, this is what the
        // supervisor asks the catalog about — and a second attempt at the same
        // merge, with different output files, reconciles to it.
        let identity = compaction_identity_for(hypertable, group, output_level)?;

        let cancel = tokio_util::sync::CancellationToken::new();
        let renewals = tokio::spawn(renew_until_cancelled(
            self.catalog.clone(),
            lease.clone(),
            self.lease_ttl(),
            std::time::Duration::from_millis(self.config.lease_renew_interval_ms),
            cancel.clone(),
        ));

        // `biased`: poll the merge first, so a merge that has just finished is
        // never discarded by a renewal that fails in the same instant.
        let merged = tokio::select! {
            biased;
            result = self.merge_parts(hypertable, lease, &identity, group, output_level) => Some(result),
            () = cancel.cancelled() => None,
        };
        cancel.cancel();
        let renewal = renewals.await.expect("renewal task does not panic");

        match merged {
            Some(result) => {
                // The fence refused us inside the REPLACE transaction: the
                // handoff happened between our last renewal and our commit.
                if let Err(CompactorError::Catalog(CatalogError::LeaseLost { .. })) = &result {
                    metrics::counter!("compactor_lease_lost_total", "stage" => "commit")
                        .increment(1);
                }
                result
            }
            None => {
                metrics::counter!("compactor_lease_lost_total", "stage" => "merge").increment(1);
                tracing::info!(partition = %lease.partition_values, generation = lease.generation,
                    "compaction lease lost mid-merge; abandoning object work");
                Err(renewal
                    .err()
                    .unwrap_or(CatalogError::LeaseLost {
                        partition: lease.partition_values.to_string(),
                    })
                    .into())
            }
        }
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
        lease: &CompactionLease,
        identity: &OperationIdentity,
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
        // Fenced on the lease generation we have held since before the first
        // Parquet byte: a zombie that stalled past its TTL cannot tombstone the
        // inputs its successor is already merging. Conflict still propagates as
        // CatalogError::Conflict (ingest and deletion take no lease); uploaded
        // objects are then orphans — harmless, cleaned by GC's orphan sweep.
        self.catalog
            .commit_compaction_replace(hypertable.id, lease, old, new_parts, identity)
            .await
            .inspect_err(|e| {
                // A transport failure around the REPLACE leaves its outcome
                // unknown: it may have committed and lost the acknowledgement.
                // Still never replayed in place — the next pass replans from
                // live state, which is correct either way. What plan 43 adds is
                // that the error now *carries the identity*, so the supervisor
                // can ask the catalog which of the two happened instead of
                // merely surviving not knowing.
                if e.is_recoverable_transport() {
                    metrics::counter!("compactor_ambiguous_commit_total").increment(1);
                }
            })?;
        Ok(count)
    }
}

/// Publishes what one pass's sweeps saw across every hypertable (issue 0012):
/// the backlog, the part of it this fleet may take, and the part a peer already
/// holds. Set per pass rather than per hypertable — a labelled gauge would grow
/// with the tenant's hypertable count for a fleet-level question.
fn set_candidate_gauges(kind: &'static str, stats: &CompactionStats) {
    let leased = stats.leased_skips as f64;
    let eligible = stats.eligible as f64;
    metrics::gauge!("compactor_candidates_eligible", "kind" => kind).set(eligible);
    metrics::gauge!("compactor_candidates_leased", "kind" => kind).set(leased);
    metrics::gauge!("compactor_candidates_claimable", "kind" => kind).set(eligible - leased);
}

/// Renews `lease` until cancelled. Independent of merge/commit progress: a
/// merge that takes ten TTLs stays alive, and a lease that is gone is noticed
/// within one renewal interval instead of at commit time, after the object work
/// has already been paid for.
///
/// `Ok(None)` from the catalog is a *proven* loss; an `Err` leaves ownership
/// unknown. Neither authorizes continuing — both cancel the merge.
async fn renew_until_cancelled(
    catalog: PostgresCatalog,
    lease: CompactionLease,
    ttl: std::time::Duration,
    interval: std::time::Duration,
    cancel: tokio_util::sync::CancellationToken,
) -> Result<(), CatalogError> {
    let mut tick = tokio::time::interval(interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    tick.tick().await; // the first tick completes immediately
    loop {
        tokio::select! {
            () = cancel.cancelled() => return Ok(()),
            _ = tick.tick() => {
                let outcome = catalog.renew_compaction_lease(&lease, ttl).await;
                let label = match &outcome {
                    Ok(Some(_)) => "ok",
                    Ok(None) => "lost",
                    Err(_) => "error",
                };
                metrics::counter!("compactor_lease_renew_total", "outcome" => label).increment(1);
                match outcome {
                    Ok(Some(_)) => {}
                    Ok(None) => {
                        cancel.cancel();
                        return Err(CatalogError::LeaseLost {
                            partition: lease.partition_values.to_string(),
                        });
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, partition = %lease.partition_values,
                            "compaction lease renewal failed; ownership unknown, stopping work");
                        cancel.cancel();
                        return Err(e);
                    }
                }
            }
        }
    }
}

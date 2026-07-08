//! Periodic metrics collector (metrics P2): the gauge families that need a
//! query or an API call each tick, run on a ticker independent of any role
//! (like the metrics-upkeep task). Each family is isolated — a failing family
//! logs, increments `collector_errors_total{family}`, and never stops the
//! others or the process: a monitoring outage must not become a data-plane
//! outage. Gauges are set-only (a vanished label set keeps its last value);
//! cardinality is bounded (hypertables and workers, never namespaces and
//! never partitions-as-an-unbounded-set), so stale zeros are acceptable.
//!
//! The compactor backlog/unfinalized gauges reflect *this process's*
//! configured triggers (`[compactor]`), so a compactor-role-less process still
//! reports them against the same config source.

use std::collections::HashMap;
use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use metrics::{counter, gauge};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use tokio_util::sync::CancellationToken;
use ukiel_catalog::PostgresCatalog;
use ukiel_core::HypertableId;

use crate::config::UkieldConfig;

type BoxFuture<'a> = Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + 'a>>;

/// Spawns the collector as one task in the existing `JoinSet`, unconditionally
/// (role-independent). Cancelled via `shutdown` like every other worker.
pub fn spawn(
    tasks: &mut tokio::task::JoinSet<anyhow::Result<&'static str>>,
    cfg: &UkieldConfig,
    catalog: PostgresCatalog,
    shutdown: CancellationToken,
) {
    let collector = Collector::from_config(cfg, catalog);
    tasks.spawn(async move { collector.run(shutdown).await });
}

pub struct Collector {
    catalog: PostgresCatalog,
    interval: Duration,
    cache_walk_every: u32,
    l0_fanout: i64,
    fanout: i64,
    finalize_after_secs: f64,
    cache_dir: String,
    /// (topic, hypertable name) for every table declaring a topic.
    routes: Vec<(String, String)>,
    kafka: Option<Arc<BaseConsumer>>,
}

impl Collector {
    fn from_config(cfg: &UkieldConfig, catalog: PostgresCatalog) -> Self {
        let routes: Vec<(String, String)> = cfg
            .tables
            .iter()
            .filter_map(|t| {
                t.topic
                    .as_ref()
                    .map(|topic| (topic.clone(), t.name.clone()))
            })
            .collect();
        // Consumer-lag family is disabled (quietly) when nothing declares a
        // topic — a query-only node must not build a Kafka client nor warn.
        let kafka = if routes.is_empty() {
            tracing::info!("collector: no kafka topics configured; consumer-lag family disabled");
            None
        } else {
            match ClientConfig::new()
                .set("bootstrap.servers", &cfg.ingest.brokers)
                .set("group.id", format!("{}-collector", cfg.ingest.group_id))
                .create::<BaseConsumer>()
            {
                Ok(c) => Some(Arc::new(c)),
                Err(e) => {
                    tracing::warn!(error = %e, "collector: kafka client build failed; lag family disabled");
                    None
                }
            }
        };
        Self {
            catalog,
            interval: Duration::from_millis(cfg.collector.interval_ms),
            cache_walk_every: cfg.collector.cache_walk_every,
            l0_fanout: cfg.compactor.l0_fanout as i64,
            fanout: cfg.compactor.fanout as i64,
            finalize_after_secs: cfg.compactor.finalize_after_secs as f64,
            cache_dir: cfg.query.cache_dir.clone(),
            routes,
            kafka,
        }
    }

    async fn run(self, token: CancellationToken) -> anyhow::Result<&'static str> {
        let mut tick = tokio::time::interval(self.interval);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut n: u64 = 0;
        loop {
            tokio::select! {
                _ = token.cancelled() => return Ok("collector"),
                _ = tick.tick() => {
                    self.tick(n).await;
                    n = n.wrapping_add(1);
                }
            }
        }
    }

    async fn tick(&self, n: u64) {
        let mut families: Vec<(&'static str, BoxFuture<'_>)> = vec![
            ("catalog", Box::pin(self.catalog_family())),
            ("compactor", Box::pin(self.compactor_family())),
            ("gc", Box::pin(self.gc_family())),
            ("cache", Box::pin(self.cache_family(n))),
        ];
        if self.kafka.is_some() {
            families.push(("consumer_lag", Box::pin(self.kafka_family())));
        }
        run_families(families).await;
    }

    async fn catalog_family(&self) -> anyhow::Result<()> {
        let names: HashMap<HypertableId, String> = self
            .catalog
            .list_hypertables()
            .await?
            .into_iter()
            .map(|h| (h.id, h.name))
            .collect();
        let name_of = |ht: HypertableId| names.get(&ht).cloned().unwrap_or_else(|| ht.to_string());

        for (ht, level, count) in self.catalog.live_part_counts().await? {
            gauge!("catalog_live_parts", "hypertable" => name_of(ht), "level" => level.to_string())
                .set(count as f64);
        }
        for (worker, ht, lag) in self.catalog.feed_lags().await? {
            gauge!("catalog_feed_lag", "worker" => worker, "hypertable" => name_of(ht))
                .set(lag as f64);
        }
        let (size, idle) = self.catalog.pool_stats();
        gauge!("catalog_pool_connections", "state" => "size").set(size as f64);
        gauge!("catalog_pool_connections", "state" => "idle").set(idle as f64);
        Ok(())
    }

    async fn compactor_family(&self) -> anyhow::Result<()> {
        let backlog = self
            .catalog
            .backlog_groups(self.l0_fanout, self.fanout)
            .await?;
        gauge!("compactor_backlog_groups").set(backlog as f64);
        let (unfinalized, oldest) = self
            .catalog
            .unfinalized_partitions(self.finalize_after_secs)
            .await?;
        gauge!("compactor_unfinalized_partitions").set(unfinalized as f64);
        gauge!("compactor_oldest_unfinalized_age_seconds").set(oldest);
        Ok(())
    }

    async fn gc_family(&self) -> anyhow::Result<()> {
        let b = self.catalog.gc_backlogs().await?;
        gauge!("catalog_pending_objects").set(b.pending as f64);
        gauge!("gc_pending_object_age_seconds").set(b.pending_oldest_secs);
        gauge!("catalog_unpurged_tombstones").set(b.unpurged_tombstones as f64);
        gauge!("gc_reap_fence_age_seconds").set(b.oldest_tombstone_secs);
        Ok(())
    }

    async fn cache_family(&self, n: u64) -> anyhow::Result<()> {
        if self.cache_dir.is_empty() {
            return Ok(());
        }
        gauge!("ukield_cache_dir_free_ratio").set(free_ratio(&self.cache_dir)?);
        // The byte walk is the expensive one: only every Nth tick.
        if self.cache_walk_every > 0 && n.is_multiple_of(self.cache_walk_every as u64) {
            let dir = self.cache_dir.clone();
            let bytes = tokio::task::spawn_blocking(move || dir_bytes(Path::new(&dir))).await??;
            gauge!("ukield_cache_dir_bytes").set(bytes as f64);
        }
        Ok(())
    }

    async fn kafka_family(&self) -> anyhow::Result<()> {
        let consumer = self
            .kafka
            .clone()
            .expect("kafka family runs only when built");
        for (topic, ht_name) in &self.routes {
            let ht = self.catalog.get_hypertable(ht_name).await?;
            let offsets = self.catalog.ingest_offsets(ht.id, topic).await?;
            // Blocking rdkafka metadata + watermark calls off the async runtime.
            let (c, t) = (consumer.clone(), topic.clone());
            let highs = tokio::task::spawn_blocking(move || {
                let md = c.fetch_metadata(Some(&t), Duration::from_secs(5))?;
                let mut out: Vec<(i32, i64)> = Vec::new();
                for topic_md in md.topics() {
                    for p in topic_md.partitions() {
                        let (_low, high) =
                            c.fetch_watermarks(&t, p.id(), Duration::from_secs(5))?;
                        out.push((p.id(), high));
                    }
                }
                Ok::<_, rdkafka::error::KafkaError>(out)
            })
            .await??;
            for (partition, high) in highs {
                let next = offsets.get(&partition).copied().unwrap_or(0);
                gauge!("ingest_consumer_lag", "topic" => topic.clone(), "partition" => partition.to_string())
                    .set((high - next).max(0) as f64);
            }
        }
        Ok(())
    }
}

/// Awaits each named family; per family an `Err` is logged and counted
/// (`collector_errors_total{family}`), never propagated. Families run in
/// sequence but independently — one erroring never skips the rest.
async fn run_families(families: Vec<(&'static str, BoxFuture<'_>)>) {
    for (name, fut) in families {
        if let Err(e) = fut.await {
            tracing::warn!(family = name, error = %e, "collector family failed");
            counter!("collector_errors_total", "family" => name).increment(1);
        }
    }
}

/// Free-space ratio (available blocks / total blocks) of the filesystem
/// backing `dir`, via statvfs. Unitless, so the block size cancels.
fn free_ratio(dir: &str) -> std::io::Result<f64> {
    let c = std::ffi::CString::new(dir)
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidInput, "nul byte in path"))?;
    // SAFETY: statvfs writes into a zeroed POD struct we own; `c` is a valid
    // NUL-terminated C string that outlives the call.
    let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };
    let rc = unsafe { libc::statvfs(c.as_ptr(), &mut stat) };
    if rc != 0 {
        return Err(std::io::Error::last_os_error());
    }
    let total = stat.f_blocks as f64;
    if total == 0.0 {
        return Ok(0.0);
    }
    Ok(stat.f_bavail as f64 / total)
}

/// Recursive byte sum of a directory tree (the read-through cache dir). A
/// missing dir is 0. Bounded by the tree size; the caller runs it only every
/// Nth tick.
fn dir_bytes(dir: &Path) -> std::io::Result<u64> {
    if !dir.exists() {
        return Ok(0);
    }
    let mut total = 0u64;
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        for entry in std::fs::read_dir(&d)? {
            let entry = entry?;
            let ft = entry.file_type()?;
            if ft.is_dir() {
                stack.push(entry.path());
            } else if ft.is_file() {
                total += entry.metadata()?.len();
            }
        }
    }
    Ok(total)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn free_ratio_is_a_fraction_for_existing_dir() {
        let r = free_ratio("/tmp").unwrap();
        assert!((0.0..=1.0).contains(&r), "ratio out of range: {r}");
    }

    #[test]
    fn free_ratio_errors_for_missing_dir() {
        assert!(free_ratio("/nonexistent-ukiel-collector-xyz-98765").is_err());
    }

    #[test]
    fn dir_bytes_sums_files_and_is_zero_when_missing() {
        let dir = std::env::temp_dir().join(format!("ukiel-collector-{}", std::process::id()));
        std::fs::create_dir_all(dir.join("sub")).unwrap();
        std::fs::write(dir.join("a.bin"), vec![0u8; 128]).unwrap();
        std::fs::write(dir.join("sub/b.bin"), vec![0u8; 72]).unwrap();
        assert_eq!(dir_bytes(&dir).unwrap(), 200);
        std::fs::remove_dir_all(&dir).ok();
        assert_eq!(dir_bytes(&dir).unwrap(), 0);
    }

    #[tokio::test]
    async fn family_errors_are_isolated() {
        let ran = Arc::new(AtomicUsize::new(0));
        let (r1, r2) = (ran.clone(), ran.clone());
        let families: Vec<(&'static str, BoxFuture<'_>)> = vec![
            (
                "bad",
                Box::pin(async move {
                    r1.fetch_add(1, Ordering::SeqCst);
                    anyhow::bail!("boom")
                }),
            ),
            (
                "good",
                Box::pin(async move {
                    r2.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }),
            ),
        ];
        run_families(families).await;
        assert_eq!(
            ran.load(Ordering::SeqCst),
            2,
            "both families ran despite the first erroring"
        );
    }
}

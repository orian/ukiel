//! The ingest worker: Kafka -> per-day buffers -> Flusher.
//!
//! Offsets live in the catalog (`ingest_offsets`), committed atomically with
//! parts. Kafka group offsets are never committed; on startup we assign all
//! partitions explicitly and seek to the catalog's stored positions.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use object_store::ObjectStore;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;
use ukiel_catalog::{OffsetRange, PostgresCatalog};
use ukiel_core::Hypertable;

use crate::IngestError;
use crate::config::{IngestConfig, TableRoute};
use crate::flusher::{FlushItem, Flusher};

pub struct IngestWorker {
    catalog: PostgresCatalog,
    store: Arc<dyn ObjectStore>,
    config: IngestConfig,
}

impl IngestWorker {
    pub fn new(
        catalog: PostgresCatalog,
        store: Arc<dyn ObjectStore>,
        config: IngestConfig,
    ) -> Self {
        Self {
            catalog,
            store,
            config,
        }
    }

    /// Consumes all configured routes until `shutdown` is cancelled.
    /// Buffered rows are flushed one final time on shutdown.
    ///
    /// Route tasks are owned by a `JoinSet` so that dropping this future
    /// (a crash / cancellation) aborts them — otherwise a detached route task
    /// would keep consuming Kafka and committing after the worker is gone.
    pub async fn run(&self, shutdown: CancellationToken) -> Result<(), IngestError> {
        let mut set = tokio::task::JoinSet::new();
        for route in &self.config.tables {
            let task = RouteIngest {
                catalog: self.catalog.clone(),
                flusher: Flusher::new(self.catalog.clone(), self.store.clone()),
                config: self.config.clone(),
                route: route.clone(),
            };
            let token = shutdown.clone();
            set.spawn(async move { task.run(token).await });
        }
        while let Some(res) = set.join_next().await {
            res.expect("route task panicked")?;
        }
        Ok(())
    }
}

struct RouteIngest {
    catalog: PostgresCatalog,
    flusher: Flusher,
    config: IngestConfig,
    route: TableRoute,
}

/// Buffered state between flushes.
#[derive(Default)]
struct Buffers {
    /// day (YYYY-MM-DD) -> rows
    rows_by_day: HashMap<String, Vec<serde_json::Value>>,
    /// kafka partition -> (first, last) consumed offset since the last flush
    ranges: HashMap<i32, (i64, i64)>,
    total_rows: usize,
}

/// Issue 0004: bounds event time so bad producer timestamps can't mint junk
/// partitions (or the old permanent "unknown" day). Pure for testability;
/// saturating arithmetic keeps i64::MIN/MAX out of bounds by construction.
pub(crate) fn event_time_in_bounds(ts_ms: i64, now_ms: i64, config: &IngestConfig) -> bool {
    let max_age_ms = (config.max_event_age_days as i64).saturating_mul(24 * 3600 * 1000);
    let max_future_ms = (config.max_event_future_secs as i64).saturating_mul(1000);
    ts_ms >= now_ms.saturating_sub(max_age_ms) && ts_ms <= now_ms.saturating_add(max_future_ms)
}

impl RouteIngest {
    async fn run(&self, shutdown: CancellationToken) -> Result<(), IngestError> {
        let hypertable = self.catalog.get_hypertable(&self.route.hypertable).await?;
        let consumer = self.connect(&hypertable).await?;

        let mut buffers = Buffers::default();
        let mut ticker =
            tokio::time::interval(Duration::from_millis(self.config.flush_interval_ms));
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    self.flush(&hypertable, &mut buffers).await?;
                    return Ok(());
                }
                _ = ticker.tick() => {
                    self.flush(&hypertable, &mut buffers).await?;
                }
                msg = consumer.recv() => {
                    let msg = msg?;
                    self.buffer_message(
                        &mut buffers,
                        &hypertable.packing_key,
                        msg.payload(),
                        msg.partition(),
                        msg.offset(),
                    );
                    if buffers.total_rows >= self.config.max_buffer_rows {
                        self.flush(&hypertable, &mut buffers).await?;
                    }
                }
            }
        }
    }

    /// Explicit assignment of every partition, positioned from the catalog.
    async fn connect(&self, hypertable: &Hypertable) -> Result<StreamConsumer, IngestError> {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", &self.config.brokers)
            .set("group.id", &self.config.group_id)
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .set("allow.auto.create.topics", "true")
            .create()?;

        let stored = self
            .catalog
            .ingest_offsets(hypertable.id, &self.route.topic)
            .await?;

        // Partitions are assigned once at startup, so we must wait for the
        // topic to actually exist with partitions. A freshly-declared topic is
        // auto-created lazily; a metadata fetch that races that creation can
        // report zero partitions, which would silently consume nothing.
        let mut tpl = TopicPartitionList::new();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            let metadata =
                consumer.fetch_metadata(Some(&self.route.topic), Duration::from_secs(10))?;
            let partitions = metadata
                .topics()
                .iter()
                .find(|t| t.name() == self.route.topic)
                .map(|t| t.partitions())
                .unwrap_or(&[]);
            if !partitions.is_empty() {
                for p in partitions {
                    let next = stored.get(&p.id()).copied().unwrap_or(0);
                    tpl.add_partition_offset(&self.route.topic, p.id(), Offset::Offset(next))?;
                }
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(IngestError::Config(format!(
                    "topic '{}' has no partitions after 30s",
                    self.route.topic
                )));
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        consumer.assign(&tpl)?;
        Ok(consumer)
    }

    fn buffer_message(
        &self,
        buffers: &mut Buffers,
        packing_key: &str,
        payload: Option<&[u8]>,
        partition: i32,
        offset: i64,
    ) {
        // Track the offset even for rows we skip: a poison message must not
        // stall the stream or be re-read forever.
        let range = buffers.ranges.entry(partition).or_insert((offset, offset));
        range.1 = offset;

        let Some(payload) = payload else { return };
        let Ok(row) = serde_json::from_slice::<serde_json::Value>(payload) else {
            return;
        };
        let Some(ts) = row.get(&self.route.ts_column).and_then(|v| v.as_i64()) else {
            return;
        };
        // Every buffered row must also carry a valid i64 packing key, or the
        // Parquet encoder would reject the whole flush at commit time. Skip
        // (but still advance past) rows that can't be encoded.
        if row.get(packing_key).and_then(|v| v.as_i64()).is_none() {
            return;
        }
        // Out-of-bounds event time: skip like poison — the offset above has
        // already advanced, the row is dropped visibly (warn), and no junk
        // partition is minted (issue 0004).
        if !event_time_in_bounds(ts, chrono::Utc::now().timestamp_millis(), &self.config) {
            tracing::warn!(ts, "event time out of bounds; row skipped");
            return;
        }
        let Some(day) =
            chrono::DateTime::from_timestamp_millis(ts).map(|d| d.date_naive().to_string())
        else {
            return; // unreachable given the bounds check; never mint "unknown"
        };

        buffers.rows_by_day.entry(day).or_default().push(row);
        buffers.total_rows += 1;
    }

    async fn flush(
        &self,
        hypertable: &Hypertable,
        buffers: &mut Buffers,
    ) -> Result<(), IngestError> {
        if buffers.ranges.is_empty() {
            return Ok(());
        }
        let cols = ukiel_core::TableColumns::parse(&hypertable.table_schema)?;
        let mut items = Vec::new();
        for (day, rows) in buffers.rows_by_day.drain() {
            let part = crate::writer::encode_rows(
                &cols,
                &hypertable.packing_key,
                &self.route.ts_column,
                rows,
            )?;
            items.push(FlushItem {
                partition_values: serde_json::json!({ "day": day }),
                part,
            });
        }
        let offsets: Vec<OffsetRange> = buffers
            .ranges
            .drain()
            .map(|(partition, (first, last))| OffsetRange {
                topic: self.route.topic.clone(),
                partition,
                first,
                last,
            })
            .collect();
        buffers.total_rows = 0;

        // Offset-only flush (all rows were skipped): commit offsets with no parts.
        self.flusher.flush(hypertable, items, offsets).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Minimal IngestConfig for pure-function tests (no Kafka/store touched).
    fn cfg() -> IngestConfig {
        IngestConfig {
            brokers: String::new(),
            group_id: String::new(),
            flush_interval_ms: 1000,
            max_buffer_rows: 1000,
            max_event_age_days: 3650,
            max_event_future_secs: 3600,
            tables: vec![],
        }
    }

    #[test]
    fn event_time_bounds() {
        let c = cfg(); // max_event_age_days: 3650, max_event_future_secs: 3600
        let now_ms: i64 = 1_780_000_000_000; // fixed "now" for determinism

        assert!(event_time_in_bounds(now_ms, now_ms, &c));
        assert!(event_time_in_bounds(now_ms - 24 * 3600 * 1000, now_ms, &c)); // yesterday
        assert!(event_time_in_bounds(now_ms + 3_599_000, now_ms, &c)); // 59m59s ahead

        // Too far future: year-3000-style producer bugs.
        assert!(!event_time_in_bounds(now_ms + 3_601_000, now_ms, &c));
        // Too old: beyond the age window.
        let age_ms = 3650i64 * 24 * 3600 * 1000;
        assert!(!event_time_in_bounds(now_ms - age_ms - 1000, now_ms, &c));
        // Unrepresentable timestamps are out of bounds by construction.
        assert!(!event_time_in_bounds(i64::MAX, now_ms, &c));
        assert!(!event_time_in_bounds(i64::MIN, now_ms, &c));
    }
}

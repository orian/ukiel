//! The ingest worker: Kafka -> per-day buffers -> Flusher.
//!
//! Offsets live in the catalog (`ingest_offsets`), committed atomically with
//! parts. Kafka group offsets are never committed; on startup we assign all
//! partitions explicitly and seek to the catalog's stored positions.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use metrics::{counter, gauge};
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
    /// Max event ts (ms) of rows accepted since the last flush; drives the
    /// `ingest_last_flush_event_ts_seconds` freshness gauge (metrics P1).
    max_event_ts_ms: Option<i64>,
    /// Consecutive flush ticks deferred by backpressure (plan 18).
    deferred_flushes: u32,
}

/// Running max of accepted event timestamps (metrics P1 freshness tracker).
fn track_event_ts(current: Option<i64>, ts: i64) -> i64 {
    current.map_or(ts, |m| m.max(ts))
}

/// Backpressure verdict for one flush tick (plan 18). Pure so it is
/// testable without Kafka or Postgres.
#[derive(Debug)]
pub(crate) enum FlushDecision {
    Flush,
    Defer,
}

pub(crate) fn flush_decision(
    max_live_l0: i64,
    deferred: u32,
    total_rows: usize,
    config: &IngestConfig,
) -> FlushDecision {
    // Memory valve: never hold more than 2x the buffer cap, whatever the
    // catalog says — a full stop must degrade to big flushes, not OOM.
    if total_rows >= config.max_buffer_rows.saturating_mul(2) {
        return FlushDecision::Flush;
    }
    if max_live_l0 >= config.l0_stop_parts as i64 {
        return FlushDecision::Defer;
    }
    if max_live_l0 >= config.l0_slowdown_parts as i64 && deferred == 0 {
        return FlushDecision::Defer; // every other tick in the slowdown band
    }
    FlushDecision::Flush
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
                    // Shutdown bypasses backpressure: a clean stop must not
                    // strand buffered rows (they would replay on restart).
                    self.flush(&hypertable, &mut buffers, true).await?;
                    return Ok(());
                }
                _ = ticker.tick() => {
                    self.flush(&hypertable, &mut buffers, false).await?;
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
                        self.flush(&hypertable, &mut buffers, false).await?;
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

        let Some(payload) = payload else {
            counter!("ingest_poison_messages_total", "topic" => self.route.topic.clone())
                .increment(1);
            return;
        };
        let Ok(row) = serde_json::from_slice::<serde_json::Value>(payload) else {
            counter!("ingest_poison_messages_total", "topic" => self.route.topic.clone())
                .increment(1);
            return;
        };
        let Some(ts) = row.get(&self.route.ts_column).and_then(|v| v.as_i64()) else {
            counter!("ingest_poison_messages_total", "topic" => self.route.topic.clone())
                .increment(1);
            return;
        };
        // Every buffered row must also carry a valid i64 packing key, or the
        // Parquet encoder would reject the whole flush at commit time. Skip
        // (but still advance past) rows that can't be encoded.
        if row.get(packing_key).and_then(|v| v.as_i64()).is_none() {
            counter!("ingest_poison_messages_total", "topic" => self.route.topic.clone())
                .increment(1);
            return;
        }
        // Out-of-bounds event time: skip like poison — the offset above has
        // already advanced, the row is dropped visibly (warn), and no junk
        // partition is minted (issue 0004).
        if !event_time_in_bounds(ts, chrono::Utc::now().timestamp_millis(), &self.config) {
            counter!("ingest_out_of_bounds_total", "hypertable" => self.route.hypertable.clone())
                .increment(1);
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
        buffers.max_event_ts_ms = Some(track_event_ts(buffers.max_event_ts_ms, ts));
    }

    async fn flush(
        &self,
        hypertable: &Hypertable,
        buffers: &mut Buffers,
        force: bool,
    ) -> Result<(), IngestError> {
        if buffers.ranges.is_empty() {
            return Ok(());
        }
        if !force && !buffers.rows_by_day.is_empty() {
            // Backpressure: probe live-L0 pressure on the partitions this
            // flush would touch. Deferring is exactly-once-safe — offsets
            // only advance with a flush.
            let mut max_live = 0i64;
            for day in buffers.rows_by_day.keys() {
                let pv = serde_json::json!({ "day": day });
                max_live = max_live.max(self.catalog.live_l0_parts(hypertable.id, &pv).await?);
            }
            if let FlushDecision::Defer = flush_decision(
                max_live,
                buffers.deferred_flushes,
                buffers.total_rows,
                &self.config,
            ) {
                buffers.deferred_flushes += 1;
                tracing::warn!(
                    hypertable = %hypertable.name,
                    max_live_l0 = max_live,
                    deferred = buffers.deferred_flushes,
                    buffered_rows = buffers.total_rows,
                    "backpressure: flush deferred (compaction behind)"
                );
                return Ok(());
            }
        }
        buffers.deferred_flushes = 0;
        if buffers.rows_by_day.len() > self.config.warn_partitions_per_flush {
            tracing::warn!(
                hypertable = %hypertable.name,
                partitions = buffers.rows_by_day.len(),
                "flush spans many partitions (backfill?) — L0 file fan-out"
            );
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
        // Freshness (metrics P1): max event ts of this flush's rows, reset each
        // flush. `None` on an offset-only flush leaves the gauge untouched.
        let max_event_ts_ms = buffers.max_event_ts_ms.take();

        // Offset-only flush (all rows were skipped): commit offsets with no parts.
        self.flusher.flush(hypertable, items, offsets).await?;

        if let Some(ts_ms) = max_event_ts_ms {
            gauge!("ingest_last_flush_event_ts_seconds", "hypertable" => self.route.hypertable.clone())
                .set(ts_ms as f64 / 1000.0);
        }
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
            l0_slowdown_parts: 30,
            l0_stop_parts: 200,
            warn_partitions_per_flush: 64,
            max_event_age_days: 3650,
            max_event_future_secs: 3600,
            tables: vec![],
        }
    }

    #[test]
    fn flush_decision_thresholds() {
        // Use explicit round thresholds so the bands read clearly.
        let c = IngestConfig {
            max_buffer_rows: 100_000,
            l0_slowdown_parts: 30,
            l0_stop_parts: 200,
            ..cfg()
        };
        // Healthy: always flush.
        assert!(matches!(flush_decision(0, 0, 10, &c), FlushDecision::Flush));
        assert!(matches!(
            flush_decision(29, 5, 10, &c),
            FlushDecision::Flush
        ));

        // Slowdown band: defer, then flush (every other tick).
        assert!(matches!(
            flush_decision(30, 0, 10, &c),
            FlushDecision::Defer
        ));
        assert!(matches!(
            flush_decision(30, 1, 10, &c),
            FlushDecision::Flush
        ));
        assert!(matches!(
            flush_decision(199, 0, 10, &c),
            FlushDecision::Defer
        ));
        assert!(matches!(
            flush_decision(199, 1, 10, &c),
            FlushDecision::Flush
        ));

        // Stop band: defer regardless of how long we've deferred...
        assert!(matches!(
            flush_decision(200, 7, 10, &c),
            FlushDecision::Defer
        ));
        // ...until the memory valve (2x max_buffer_rows) forces a flush.
        assert!(matches!(
            flush_decision(200, 7, 200_000, &c),
            FlushDecision::Flush
        ));
    }

    #[test]
    fn max_event_ts_tracks_max_out_of_order() {
        // The freshness tracker holds the max ts regardless of arrival order.
        let mut cur: Option<i64> = None;
        for ts in [100, 50, 300, 200] {
            cur = Some(track_event_ts(cur, ts));
        }
        assert_eq!(cur, Some(300));
        // Reset-on-flush contract: taking the field yields the max, then clears.
        assert_eq!(cur.take(), Some(300));
        assert_eq!(cur, None);
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

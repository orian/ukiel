#[derive(Debug, Clone, serde::Deserialize)]
pub struct IngestConfig {
    pub brokers: String,
    pub group_id: String,
    /// Freshness knob: how often buffers are flushed to Parquet (spec: ~10s).
    pub flush_interval_ms: u64,
    /// Flush early when this many rows are buffered across all days.
    pub max_buffer_rows: usize,
    /// Backpressure (plan 18): once a target partition holds this many live
    /// L0 parts, flush only every other tick (bigger, fewer L0 files).
    #[serde(default = "default_l0_slowdown_parts")]
    pub l0_slowdown_parts: usize,
    /// Hard stop: at this many live L0 parts, defer flushes entirely until
    /// the memory valve (2x max_buffer_rows) forces one.
    #[serde(default = "default_l0_stop_parts")]
    pub l0_stop_parts: usize,
    /// Warn when one flush spans more than this many partitions (backfill
    /// detector). No hard cap: offsets cover all consumed rows, so a flush
    /// cannot be split by partition without breaking exactly-once.
    #[serde(default = "default_warn_partitions_per_flush")]
    pub warn_partitions_per_flush: usize,
    /// Reject (skip like poison) events older than this many days (issue 0004).
    /// Raise temporarily for historical backfills; `0` = unbounded past.
    #[serde(default = "default_max_event_age_days")]
    pub max_event_age_days: u64,
    /// Reject (skip like poison) events with timestamps further in the future
    /// than this (clock-skew allowance; catches sec-vs-ms unit bugs).
    #[serde(default = "default_max_event_future_secs")]
    pub max_event_future_secs: u64,
    pub tables: Vec<TableRoute>,
    /// Fired once EVERY route has validated its hypertable, reloaded catalog
    /// offsets and seeked Kafka (plan 42). All of them, not the first: while any
    /// route is still positioning, the ingest role has not reconciled, and a
    /// process that claimed otherwise would be advertising a consumer that is
    /// not yet reading from the right place.
    #[serde(skip)]
    pub ready: Option<ukiel_core::ReadySignal>,
}

/// Guardrail defaults (issue 0009: one source of truth — ukield's
/// `IngestSection` and the e2e fixture reference these, so the production
/// defaults cannot drift between the crate and its embedders).
pub mod defaults {
    pub const L0_SLOWDOWN_PARTS: usize = 30;
    pub const L0_STOP_PARTS: usize = 200;
    pub const WARN_PARTITIONS_PER_FLUSH: usize = 64;
}

fn default_l0_slowdown_parts() -> usize {
    defaults::L0_SLOWDOWN_PARTS
}

fn default_l0_stop_parts() -> usize {
    defaults::L0_STOP_PARTS
}

fn default_warn_partitions_per_flush() -> usize {
    defaults::WARN_PARTITIONS_PER_FLUSH
}

fn default_max_event_age_days() -> u64 {
    3650
}

fn default_max_event_future_secs() -> u64 {
    3600
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct TableRoute {
    pub topic: String,
    /// Catalog hypertable name this topic feeds.
    pub hypertable: String,
    /// Column holding epoch-milliseconds; used for sorting and to derive the
    /// `day` partition value.
    pub ts_column: String,
}

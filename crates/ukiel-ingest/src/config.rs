#[derive(Debug, Clone, serde::Deserialize)]
pub struct IngestConfig {
    pub brokers: String,
    pub group_id: String,
    /// Freshness knob: how often buffers are flushed to Parquet (spec: ~10s).
    pub flush_interval_ms: u64,
    /// Flush early when this many rows are buffered across all days.
    pub max_buffer_rows: usize,
    /// Reject (skip like poison) events older than this many days (issue 0004).
    /// Raise temporarily for historical backfills; `0` = unbounded past.
    #[serde(default = "default_max_event_age_days")]
    pub max_event_age_days: u64,
    /// Reject (skip like poison) events with timestamps further in the future
    /// than this (clock-skew allowance; catches sec-vs-ms unit bugs).
    #[serde(default = "default_max_event_future_secs")]
    pub max_event_future_secs: u64,
    pub tables: Vec<TableRoute>,
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

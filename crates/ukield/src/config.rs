//! TOML configuration for ukield. See ukield.example.toml for the annotated
//! reference. Secrets can be overridden by env vars (UKIELD_CATALOG_URL,
//! UKIELD_S3_ACCESS_KEY_ID, UKIELD_S3_SECRET_ACCESS_KEY).

use serde::Deserialize;

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct UkieldConfig {
    /// Which workers this process runs. Default: all.
    #[serde(default = "default_roles")]
    pub roles: Vec<Role>,
    pub catalog: CatalogConfig,
    pub object_store: ObjectStoreConfig,
    #[serde(default)]
    pub query: QueryConfig,
    #[serde(default)]
    pub ingest: IngestSection,
    #[serde(default)]
    pub compactor: CompactorSection,
    #[serde(default)]
    pub gc: GcSection,
    #[serde(default)]
    pub collector: CollectorSection,
    /// Standalone `/metrics` listener address (e.g. `"127.0.0.1:9187"`).
    /// Empty = disabled; a query-role-less process then has no metrics
    /// listener and logs a startup warning (metrics P2).
    #[serde(default)]
    pub metrics_listen: String,
    /// Tables bootstrapped idempotently at startup.
    #[serde(default)]
    pub tables: Vec<TableConfig>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    Ingest,
    Query,
    Compactor,
    Gc,
}

fn default_roles() -> Vec<Role> {
    vec![Role::Ingest, Role::Query, Role::Compactor, Role::Gc]
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct CatalogConfig {
    pub url: String,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct ObjectStoreConfig {
    /// "s3" (default) or "memory" (dev/tests: no MinIO, data lost on exit).
    #[serde(default = "default_store_kind")]
    pub kind: String,
    #[serde(default)]
    pub endpoint: String,
    #[serde(default = "default_bucket")]
    pub bucket: String,
    #[serde(default)]
    pub access_key_id: String,
    #[serde(default)]
    pub secret_access_key: String,
    #[serde(default = "default_region")]
    pub region: String,
    #[serde(default)]
    pub allow_http: bool,
}

fn default_store_kind() -> String {
    "s3".to_string()
}
fn default_bucket() -> String {
    "ukiel".to_string()
}
fn default_region() -> String {
    "us-east-1".to_string()
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(default)]
pub struct QueryConfig {
    pub listen: String,
    /// Non-empty = wrap the store in the local read-through cache.
    pub cache_dir: String,
    /// Statement timeout in seconds (issue 0002: must stay below
    /// gc.tombstone_grace_secs — validated at startup by `validate`).
    pub statement_timeout_secs: u64,
}

impl Default for QueryConfig {
    fn default() -> Self {
        Self {
            listen: "127.0.0.1:8080".to_string(),
            cache_dir: String::new(),
            statement_timeout_secs: 300,
        }
    }
}

/// Issue 0002 invariant: a query that outlives the GC tombstone grace can
/// observe deleted objects. Refuse the configuration outright when one process
/// runs both roles (a split deployment can't see both sides — the invariant
/// note stays in the config comment).
pub fn validate(cfg: &UkieldConfig) -> anyhow::Result<()> {
    if cfg.roles.contains(&Role::Query)
        && cfg.roles.contains(&Role::Gc)
        && cfg.query.statement_timeout_secs as f64 >= cfg.gc.tombstone_grace_secs
    {
        anyhow::bail!(
            "query.statement_timeout_secs ({}) must be < gc.tombstone_grace_secs ({}): \
             a query outliving the grace can read reaped objects (issue 0002)",
            cfg.query.statement_timeout_secs,
            cfg.gc.tombstone_grace_secs
        );
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(default)]
pub struct IngestSection {
    pub brokers: String,
    pub group_id: String,
    pub flush_interval_ms: u64,
    pub max_buffer_rows: usize,
    /// Backpressure (plan 18): halve flush rate at this live-L0 count.
    pub l0_slowdown_parts: usize,
    /// Backpressure (plan 18): stop flushing at this live-L0 count (memory
    /// valve at 2x max_buffer_rows).
    pub l0_stop_parts: usize,
    /// Warn when one flush spans more than this many partitions (backfill
    /// fan-out detector; no hard cap).
    pub warn_partitions_per_flush: usize,
    /// Issue 0004: skip events older than this many days (raise for backfills).
    pub max_event_age_days: u64,
    /// Issue 0004: skip events from further in the future than this (unit-bug
    /// detector / clock-skew allowance).
    pub max_event_future_secs: u64,
}

impl Default for IngestSection {
    fn default() -> Self {
        Self {
            brokers: "127.0.0.1:9092".to_string(),
            group_id: "ukield".to_string(),
            flush_interval_ms: 10_000,
            max_buffer_rows: 100_000,
            l0_slowdown_parts: ukiel_ingest::config::defaults::L0_SLOWDOWN_PARTS,
            l0_stop_parts: ukiel_ingest::config::defaults::L0_STOP_PARTS,
            warn_partitions_per_flush: ukiel_ingest::config::defaults::WARN_PARTITIONS_PER_FLUSH,
            max_event_age_days: 3650,
            max_event_future_secs: 3600,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(default)]
pub struct CompactorSection {
    pub poll_interval_ms: u64,
    /// L0 merge trigger. L0 files are unpruned by key (every query reads
    /// all of them): keep low.
    pub l0_fanout: usize,
    /// Run-count merge trigger for L1 and above. Runs are key-disjoint
    /// (<= 1 file per query each), merges move real bytes: keep higher.
    pub fanout: usize,
    /// Fold a partition into a single run once it has had no L0 arrivals
    /// for this long.
    pub finalize_after_secs: u64,
    /// Cadence of the finalization sweep.
    pub finalize_poll_interval_ms: u64,
    /// Skip merges whose inputs exceed this (issue 0005: guards against
    /// pulling an oversized cold partition into RAM; `0` = unlimited).
    pub max_merge_input_mb: u64,
}

impl Default for CompactorSection {
    fn default() -> Self {
        Self {
            poll_interval_ms: 5_000,
            // Production shape (unlike the test-compat library defaults):
            // eager L0, lazy L1+ — bounds L0 read amp at 4 while total
            // write amp stays ~depth = log10(runs).
            l0_fanout: 4,
            fanout: 10,
            finalize_after_secs: 3_600,
            finalize_poll_interval_ms: 60_000,
            max_merge_input_mb: 1024,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(default)]
pub struct GcSection {
    pub poll_interval_ms: u64,
    pub tombstone_grace_secs: f64,
    pub orphan_grace_secs: i64,
}

impl Default for GcSection {
    fn default() -> Self {
        Self {
            poll_interval_ms: 60_000,
            tombstone_grace_secs: 900.0,
            orphan_grace_secs: 3600,
        }
    }
}

/// Periodic metrics collector (metrics P2): the gauge families that need a
/// query or API call each tick. Role-independent; runs in every process.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(default)]
pub struct CollectorSection {
    /// Tick cadence. Every family runs each tick except the cache-dir walk.
    pub interval_ms: u64,
    /// Run the (expensive) cache-dir byte walk every Nth tick; the cheap
    /// free-ratio statvfs still runs every tick.
    pub cache_walk_every: u32,
}

impl Default for CollectorSection {
    fn default() -> Self {
        Self {
            interval_ms: 30_000,
            cache_walk_every: 10,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct TableConfig {
    /// Hypertable name; bootstrapped logical tables share it.
    pub name: String,
    /// Kafka topic feeding this table; omit for query-only tables.
    /// (Pre-plan-8 routing — becomes a pipeline definition later.)
    #[serde(default)]
    pub topic: Option<String>,
    /// Epoch-millis column used for sorting and the `day` partition.
    pub ts_column: String,
    pub packing_key: String,
    pub sort_key: Vec<String>,
    #[serde(default)]
    pub partition_columns: Vec<String>,
    /// "packed" (default) or "separated".
    #[serde(default)]
    pub placement: Option<String>,
    /// Size-targeted placement: compaction cuts merge outputs at key
    /// boundaries into files of roughly this many megabytes; keys bigger
    /// than the target get dedicated files. Mutually exclusive with
    /// `placement`.
    #[serde(default)]
    pub target_file_mb: Option<u64>,
    /// Namespaces that get a logical table named `name`.
    pub namespaces: Vec<i64>,
    pub columns: Vec<ColumnConfig>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct ColumnConfig {
    pub name: String,
    #[serde(rename = "type")]
    pub type_name: String,
    #[serde(default)]
    pub nullable: Option<bool>,
    #[serde(default)]
    pub default: Option<String>,
    #[serde(default)]
    pub materialized: Option<String>,
    #[serde(default)]
    pub alias: Option<String>,
}

impl UkieldConfig {
    pub fn load(path: &str) -> anyhow::Result<UkieldConfig> {
        let raw = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("reading config '{path}': {e}"))?;
        let mut cfg: UkieldConfig =
            toml::from_str(&raw).map_err(|e| anyhow::anyhow!("parsing config '{path}': {e}"))?;

        if let Ok(url) = std::env::var("UKIELD_CATALOG_URL") {
            cfg.catalog.url = url;
        }
        if let Ok(key) = std::env::var("UKIELD_S3_ACCESS_KEY_ID") {
            cfg.object_store.access_key_id = key;
        }
        if let Ok(secret) = std::env::var("UKIELD_S3_SECRET_ACCESS_KEY") {
            cfg.object_store.secret_access_key = secret;
        }
        Ok(cfg)
    }
}

impl TableConfig {
    /// The catalog schema JSON (`TableColumns` format) for this table.
    pub fn schema_json(&self) -> serde_json::Value {
        let fields: Vec<serde_json::Value> = self
            .columns
            .iter()
            .map(|c| {
                let mut field = serde_json::json!({ "name": c.name, "type": c.type_name });
                let obj = field.as_object_mut().expect("object literal");
                if let Some(n) = c.nullable {
                    obj.insert("nullable".to_string(), serde_json::json!(n));
                }
                if let Some(e) = &c.default {
                    obj.insert("default".to_string(), serde_json::json!(e));
                }
                if let Some(e) = &c.materialized {
                    obj.insert("materialized".to_string(), serde_json::json!(e));
                }
                if let Some(e) = &c.alias {
                    obj.insert("alias".to_string(), serde_json::json!(e));
                }
                field
            })
            .collect();
        serde_json::json!({ "fields": fields })
    }

    pub fn partition_spec(&self) -> serde_json::Value {
        serde_json::json!({ "columns": self.partition_columns })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const EXAMPLE: &str = r#"
        [catalog]
        url = "postgres://postgres:postgres@127.0.0.1:5432/postgres"

        [object_store]
        endpoint = "http://127.0.0.1:19000"
        access_key_id = "minioadmin"
        secret_access_key = "minioadmin"
        allow_http = true

        [[tables]]
        name = "events"
        topic = "events"
        ts_column = "ts"
        packing_key = "tenant_id"
        sort_key = ["tenant_id", "ts"]
        partition_columns = ["day"]
        namespaces = [1, 2]

        [[tables.columns]]
        name = "tenant_id"
        type = "int64"

        [[tables.columns]]
        name = "ts"
        type = "timestamp_ms"

        [[tables.columns]]
        name = "amount"
        type = "float64"
        default = "0.0"

        [[tables.columns]]
        name = "doubled"
        type = "float64"
        materialized = "amount * 2.0"
    "#;

    fn write_tmp(content: &str) -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!("ukield-test-{}.toml", uuid_like()));
        std::fs::write(&path, content).unwrap();
        path
    }

    // Avoid a uuid dependency for a test tmp name.
    fn uuid_like() -> String {
        format!(
            "{:?}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        )
    }

    #[test]
    fn parses_config_with_defaults() {
        let path = write_tmp(EXAMPLE);
        let cfg = UkieldConfig::load(path.to_str().unwrap()).unwrap();

        assert_eq!(
            cfg.roles,
            vec![Role::Ingest, Role::Query, Role::Compactor, Role::Gc]
        );
        assert_eq!(cfg.query.listen, "127.0.0.1:8080");
        assert_eq!(cfg.ingest.flush_interval_ms, 10_000);
        assert_eq!(cfg.gc.tombstone_grace_secs, 900.0);
        assert_eq!(cfg.object_store.kind, "s3");
        assert_eq!(cfg.object_store.bucket, "ukiel");
        // Metrics P2 defaults: collector on at 30s, standalone listener off.
        assert_eq!(cfg.collector.interval_ms, 30_000);
        assert_eq!(cfg.collector.cache_walk_every, 10);
        assert!(cfg.metrics_listen.is_empty());

        let table = &cfg.tables[0];
        assert_eq!(table.topic.as_deref(), Some("events"));
        assert_eq!(table.namespaces, vec![1, 2]);
        assert_eq!(
            table.schema_json(),
            json!({"fields": [
                {"name": "tenant_id", "type": "int64"},
                {"name": "ts", "type": "timestamp_ms"},
                {"name": "amount", "type": "float64", "default": "0.0"},
                {"name": "doubled", "type": "float64", "materialized": "amount * 2.0"}
            ]})
        );
        std::fs::remove_file(path).ok();
    }

    #[test]
    fn valid_default_timeout_passes_invariant() {
        // Defaults: statement_timeout 300 < gc grace 900, all roles present.
        let cfg: UkieldConfig = toml::from_str(EXAMPLE).unwrap();
        assert_eq!(cfg.query.statement_timeout_secs, 300);
        validate(&cfg).unwrap();
    }

    #[test]
    fn timeout_at_or_above_grace_is_refused() {
        let raw = format!("{EXAMPLE}\n[query]\nstatement_timeout_secs = 900\n");
        let cfg: UkieldConfig = toml::from_str(&raw).unwrap();
        let err = validate(&cfg).unwrap_err().to_string();
        assert!(err.contains("issue 0002"), "{err}");
    }

    #[test]
    fn env_overrides_win() {
        let path = write_tmp(EXAMPLE);
        // Serialize env mutation risk: this is the only test touching these vars.
        unsafe {
            std::env::set_var("UKIELD_CATALOG_URL", "postgres://env-wins/db");
        }
        let cfg = UkieldConfig::load(path.to_str().unwrap()).unwrap();
        assert_eq!(cfg.catalog.url, "postgres://env-wins/db");
        unsafe {
            std::env::remove_var("UKIELD_CATALOG_URL");
        }
        std::fs::remove_file(path).ok();
    }
}

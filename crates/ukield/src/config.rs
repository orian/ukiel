//! TOML configuration for ukield. See ukield.example.toml for the annotated
//! reference. Secrets can be overridden by env vars (UKIELD_CATALOG_URL,
//! UKIELD_S3_ACCESS_KEY_ID, UKIELD_S3_SECRET_ACCESS_KEY).

use std::time::Duration;

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

/// Catalog connection, pool lifecycle, and recovery policy (plan 42).
///
/// The pool defaults preserve today's capacity. What is new is the *recovery*
/// half: a managed writer failover must degrade this process, not crash-loop the
/// fleet, so the pool is built lazily, acquire failures time out promptly enough
/// to be classified, and retries are bounded and jittered.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(default)]
pub struct CatalogConfig {
    pub url: String,
    /// Per **process**, not per fleet: N processes open N × this many
    /// connections to the one primary, and `max_connections` has to survive the
    /// product (roadmap row 40's first wall). No single process can validate
    /// that budget; it is a deployment invariant.
    pub max_connections: u32,
    pub min_connections: u32,
    /// How long a caller waits for a connection before the pool gives up. Short
    /// on purpose: it is what turns "the database is gone" into a prompt,
    /// classifiable failure instead of a request hanging until its own deadline.
    pub acquire_timeout_ms: u64,
    pub idle_timeout_secs: u64,
    pub max_lifetime_secs: u64,
    /// Check a connection before handing it out. After a failover the pool holds
    /// connections to a primary that no longer exists; without this they are
    /// dealt to callers and fail one request each.
    pub test_before_acquire: bool,
    /// Recovery backoff: first delay, ceiling, and the ± fraction of jitter.
    /// Jitter is not decoration — a fleet that retries in lockstep is a
    /// thundering herd against a writer that has just come back.
    pub retry_base_ms: u64,
    pub retry_max_ms: u64,
    pub retry_jitter_ratio: f64,
    pub recovery_probe_timeout_ms: u64,
    /// How long a query may spend trying to obtain an authoritative plan before
    /// it is refused. Exceeded → 503 + `Retry-After`, never a stale plan.
    pub query_admission_timeout_ms: u64,
    pub retry_after_secs: u64,
}

impl Default for CatalogConfig {
    fn default() -> Self {
        Self {
            url: String::new(),
            max_connections: ukiel_catalog::PostgresCatalog::DEFAULT_POOL_SIZE,
            min_connections: 0,
            acquire_timeout_ms: 2_000,
            idle_timeout_secs: 600,
            max_lifetime_secs: 1_800,
            test_before_acquire: true,
            retry_base_ms: 100,
            retry_max_ms: 5_000,
            retry_jitter_ratio: 0.20,
            recovery_probe_timeout_ms: 2_000,
            query_admission_timeout_ms: 1_000,
            retry_after_secs: 1,
        }
    }
}

impl CatalogConfig {
    pub fn pool_config(&self) -> ukiel_catalog::CatalogPoolConfig {
        ukiel_catalog::CatalogPoolConfig {
            max_connections: self.max_connections,
            min_connections: self.min_connections,
            acquire_timeout: Duration::from_millis(self.acquire_timeout_ms),
            idle_timeout: Some(Duration::from_secs(self.idle_timeout_secs)),
            max_lifetime: Some(Duration::from_secs(self.max_lifetime_secs)),
            test_before_acquire: self.test_before_acquire,
        }
    }
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
    /// Non-empty = wrap the store in the local cache (read-through +
    /// write-through prewarm; chunked for large objects).
    pub cache_dir: String,
    /// Prewarm the cache with written objects (ingest/compactor output).
    pub cache_write_through: bool,
    /// Objects larger than this many bytes are cached as chunks.
    pub cache_large_object_threshold: u64,
    /// Chunk size in bytes for large-object caching.
    pub cache_chunk_size: u64,
    /// Statement timeout in seconds (issue 0002: must stay below
    /// gc.tombstone_grace_secs — validated at startup by `validate`).
    pub statement_timeout_secs: u64,
}

impl Default for QueryConfig {
    fn default() -> Self {
        // Cache defaults come from one place: the cache's own config.
        let cache = ukiel_query::cache::CacheConfig::default();
        Self {
            listen: "127.0.0.1:8080".to_string(),
            cache_dir: String::new(),
            cache_write_through: cache.write_through,
            cache_large_object_threshold: cache.large_object_threshold,
            cache_chunk_size: cache.chunk_size,
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
    let (ttl, renew) = (
        cfg.compactor.lease_ttl_secs,
        cfg.compactor.lease_renew_interval_secs,
    );
    if ttl == 0 || renew == 0 {
        anyhow::bail!(
            "compactor.lease_ttl_secs ({ttl}) and lease_renew_interval_secs ({renew}) must be > 0"
        );
    }
    // Three attempts inside one TTL. A renewal interval close to the TTL turns
    // one slow catalog round-trip into a lost partition and a discarded merge
    // (plan 41).
    if renew * 3 > ttl {
        anyhow::bail!(
            "compactor.lease_renew_interval_secs ({renew}) must be <= lease_ttl_secs/3 ({}): \
             a single stalled renewal would otherwise drop the partition mid-merge",
            ttl / 3
        );
    }
    if cfg.compactor.candidate_limit <= 0 {
        anyhow::bail!(
            "compactor.candidate_limit ({}) must be > 0",
            cfg.compactor.candidate_limit
        );
    }
    validate_catalog(&cfg.catalog)?;
    Ok(())
}

fn validate_catalog(cat: &CatalogConfig) -> anyhow::Result<()> {
    if cat.url.is_empty() {
        anyhow::bail!("catalog.url is required (or set UKIELD_CATALOG_URL)");
    }
    if cat.max_connections == 0 {
        anyhow::bail!("catalog.max_connections must be > 0");
    }
    if cat.min_connections > cat.max_connections {
        anyhow::bail!(
            "catalog.min_connections ({}) must be <= max_connections ({})",
            cat.min_connections,
            cat.max_connections
        );
    }
    for (name, value) in [
        ("acquire_timeout_ms", cat.acquire_timeout_ms),
        ("retry_base_ms", cat.retry_base_ms),
        ("retry_max_ms", cat.retry_max_ms),
        ("recovery_probe_timeout_ms", cat.recovery_probe_timeout_ms),
        ("query_admission_timeout_ms", cat.query_admission_timeout_ms),
    ] {
        if value == 0 {
            anyhow::bail!("catalog.{name} must be > 0");
        }
    }
    if cat.retry_base_ms > cat.retry_max_ms {
        anyhow::bail!(
            "catalog.retry_base_ms ({}) must be <= retry_max_ms ({})",
            cat.retry_base_ms,
            cat.retry_max_ms
        );
    }
    // Jitter above 1.0 could produce a negative delay; below 0 is meaningless.
    // Zero is allowed but discouraged: a fleet retrying in lockstep is a
    // thundering herd against a writer that has only just come back.
    if !(0.0..=1.0).contains(&cat.retry_jitter_ratio) {
        anyhow::bail!(
            "catalog.retry_jitter_ratio ({}) must be within [0, 1]",
            cat.retry_jitter_ratio
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
    /// How long a partition claim survives without renewal (plan 41). It bounds
    /// failover delay — a dead owner's partitions idle for at most this plus a
    /// poll interval before a peer reclaims them — and duplicate object work.
    /// It is not a correctness knob: the REPLACE fence is what makes a late
    /// owner harmless at any TTL.
    pub lease_ttl_secs: u64,
    /// Renewal cadence. Kept well under the TTL so a transient scheduler or
    /// network stall costs a retry, not the partition.
    pub lease_renew_interval_secs: u64,
    /// Candidate partitions considered per hypertable per pass.
    pub candidate_limit: i64,
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
            // 60s/20s: three renewal attempts inside one TTL, so a partition is
            // lost only after a stall long enough that the merge was doomed
            // anyway, while failover still lands inside a minute.
            lease_ttl_secs: 60,
            lease_renew_interval_secs: 20,
            candidate_limit: 64,
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
        assert!(cfg.query.cache_write_through);
        assert_eq!(cfg.query.cache_large_object_threshold, 64 * 1024 * 1024);
        assert_eq!(cfg.query.cache_chunk_size, 8 * 1024 * 1024);
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
    fn lease_defaults_leave_room_for_three_renewals() {
        let cfg: UkieldConfig = toml::from_str(EXAMPLE).unwrap();
        assert_eq!(cfg.compactor.lease_ttl_secs, 60);
        assert_eq!(cfg.compactor.lease_renew_interval_secs, 20);
        assert_eq!(cfg.compactor.candidate_limit, 64);
        validate(&cfg).unwrap();
    }

    #[test]
    fn a_renewal_interval_too_close_to_the_ttl_is_refused() {
        // 30s renewals inside a 60s TTL: one stalled round-trip and the merge
        // in flight loses its partition.
        let raw = format!(
            "{EXAMPLE}\n[compactor]\nlease_ttl_secs = 60\nlease_renew_interval_secs = 30\n"
        );
        let cfg: UkieldConfig = toml::from_str(&raw).unwrap();
        let err = validate(&cfg).unwrap_err().to_string();
        assert!(err.contains("lease_renew_interval_secs"), "{err}");
    }

    #[test]
    fn zero_lease_timings_are_refused() {
        for section in [
            "lease_ttl_secs = 0",
            "lease_renew_interval_secs = 0",
            "candidate_limit = 0",
        ] {
            let raw = format!("{EXAMPLE}\n[compactor]\n{section}\n");
            let cfg: UkieldConfig = toml::from_str(&raw).unwrap();
            assert!(validate(&cfg).is_err(), "{section} must be rejected");
        }
    }

    #[test]
    fn catalog_pool_and_recovery_defaults() {
        let cfg: UkieldConfig = toml::from_str(EXAMPLE).unwrap();
        let c = &cfg.catalog;
        assert_eq!(c.max_connections, 16);
        assert_eq!(c.min_connections, 0);
        assert_eq!(c.acquire_timeout_ms, 2_000);
        assert!(
            c.test_before_acquire,
            "a failover leaves dead connections in the pool"
        );
        assert_eq!(c.retry_base_ms, 100);
        assert_eq!(c.retry_max_ms, 5_000);
        assert_eq!(c.retry_jitter_ratio, 0.20);
        assert_eq!(c.query_admission_timeout_ms, 1_000);
        assert_eq!(c.retry_after_secs, 1);
        validate(&cfg).unwrap();

        let pool = c.pool_config();
        assert_eq!(pool.max_connections, 16);
        assert_eq!(pool.acquire_timeout, Duration::from_millis(2_000));
        assert_eq!(pool.max_lifetime, Some(Duration::from_secs(1_800)));
    }

    #[test]
    fn invalid_catalog_settings_are_refused() {
        let bad = [
            ("min_connections = 32", "min > max"),
            ("max_connections = 0", "zero pool"),
            ("acquire_timeout_ms = 0", "zero acquire timeout"),
            ("retry_base_ms = 9000", "base > max"),
            ("retry_jitter_ratio = 1.5", "jitter out of range"),
            ("retry_jitter_ratio = -0.1", "negative jitter"),
            ("query_admission_timeout_ms = 0", "zero admission budget"),
        ];
        for (line, why) in bad {
            let raw = EXAMPLE.replace(
                "[catalog]\n        url =",
                &format!("[catalog]\n        {line}\n        url ="),
            );
            let cfg: UkieldConfig = toml::from_str(&raw).unwrap();
            assert!(validate(&cfg).is_err(), "{why} must be refused");
        }
    }

    #[test]
    fn a_missing_catalog_url_is_refused_rather_than_defaulted() {
        // The URL has no sane default: silently connecting to localhost is how a
        // staging process quietly writes to the wrong catalog.
        let raw = EXAMPLE.replace(
            r#"url = "postgres://postgres:postgres@127.0.0.1:5432/postgres""#,
            "",
        );
        let cfg: UkieldConfig = toml::from_str(&raw).unwrap();
        let err = validate(&cfg).unwrap_err().to_string();
        assert!(err.contains("catalog.url"), "{err}");
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

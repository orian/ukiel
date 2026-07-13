//! End-to-end test harness: connects to the docker-compose stack
//! (Kafka + Postgres + MinIO) and wires real Ukiel components in-process.
//!
//! Unlike the component tests, this harness does NOT manage containers — it
//! connects to whatever the compose stack exposes, configured via env vars
//! with compose-matching defaults. See
//! `docs/superpowers/specs/2026-07-05-ukiel-testing-design.md`.

use std::sync::Arc;
use std::time::{Duration, Instant};

use object_store::ObjectStore;
use object_store::aws::AmazonS3Builder;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;
use ukiel_catalog::PostgresCatalog;
use ukiel_compactor::CompactorError;
use ukiel_compactor::compactor::{Compactor, CompactorConfig};
use ukiel_core::{HypertableId, NamespaceId};
use ukiel_ingest::config::{IngestConfig, TableRoute};
use ukiel_ingest::consumer::IngestWorker;
use ukiel_query::server::{AppState, router};

pub mod fault_proxy;

/// Env with a compose-matching default.
pub fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

/// A table wired for one run: unique hypertable/topic so a dirty stack never
/// causes false failures. The logical table is always named `events` (what the
/// SQL references); the namespace id is the packing-key value (v1 convention).
#[derive(Debug, Clone)]
pub struct Table {
    pub hypertable_id: HypertableId,
    pub hypertable_name: String,
    pub topic: String,
    /// Primary namespace (the first one created); handy for single-tenant tests.
    pub namespace: NamespaceId,
    /// Every namespace with a logical `events` table over this hypertable.
    pub namespaces: Vec<NamespaceId>,
    pub ts_column: String,
}

/// The running system under test.
pub struct Stack {
    pub catalog: PostgresCatalog,
    pub store: Arc<dyn ObjectStore>,
    pub store_url: url::Url,
    pub brokers: String,
    pub producer: FutureProducer,
    pub query_base: String,
    pub nonce: String,
    client: reqwest::Client,
}

impl Stack {
    /// Connects to the env-configured stack, runs migrations, builds the S3
    /// (MinIO) object store and a Kafka producer, and spawns the query HTTP
    /// server in-process on an ephemeral port.
    pub async fn start() -> Stack {
        let brokers = env_or("UKIEL_E2E_KAFKA", "127.0.0.1:9092");
        let pg = env_or(
            "UKIEL_E2E_PG",
            "postgres://postgres:postgres@127.0.0.1:5432/postgres",
        );
        let s3_endpoint = env_or("UKIEL_E2E_S3", "http://127.0.0.1:19000");

        let catalog = PostgresCatalog::connect(&pg)
            .await
            .expect("connect catalog");
        catalog.migrate().await.expect("migrate");

        // UKIEL_E2E_STORE_DIR swaps MinIO for a local-filesystem store (bench
        // attribution: reads skip the HTTP object-store layer entirely; part
        // paths are store-relative, so the catalog is oblivious). Unset =
        // compose MinIO, the e2e default.
        let (store, store_url): (Arc<dyn ObjectStore>, url::Url) =
            match std::env::var("UKIEL_E2E_STORE_DIR") {
                Ok(dir) if !dir.is_empty() => {
                    std::fs::create_dir_all(&dir).expect("create store dir");
                    let local = object_store::local::LocalFileSystem::new_with_prefix(&dir)
                        .expect("build local store");
                    (Arc::new(local), url::Url::parse("file:///").unwrap())
                }
                _ => {
                    let s3 = AmazonS3Builder::new()
                        .with_endpoint(&s3_endpoint)
                        .with_bucket_name("ukiel")
                        .with_access_key_id("minioadmin")
                        .with_secret_access_key("minioadmin")
                        .with_region("us-east-1")
                        .with_allow_http(true)
                        .with_virtual_hosted_style_request(false)
                        .build()
                        .expect("build s3 store");
                    (Arc::new(s3), url::Url::parse("s3://ukiel/").unwrap())
                }
            };

        // UKIEL_E2E_CACHE_DIR wraps the store in the plan-15 cache tier
        // (read-through + write-through prewarm, chunked large objects) —
        // bench attribution: prices the MinIO transport share with the
        // production cache in front. Unset = bare store, the e2e default.
        let store: Arc<dyn ObjectStore> = match std::env::var("UKIEL_E2E_CACHE_DIR") {
            Ok(dir) if !dir.is_empty() => {
                std::fs::create_dir_all(&dir).expect("create cache dir");
                Arc::new(ukiel_query::cache::CachingObjectStore::new(
                    store,
                    std::path::PathBuf::from(dir),
                ))
            }
            _ => store,
        };

        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", &brokers)
            .set("message.timeout.ms", "10000")
            .create()
            .expect("build producer");

        // In-process query server on an ephemeral port.
        let state = AppState::with_defaults(
            catalog.clone(),
            store.clone(),
            store_url.clone(),
            std::time::Duration::from_secs(300),
            Arc::new(ukiel_query::metadata_cache::ParquetMetadataCache::default()),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, router(state)).await.unwrap();
        });

        let nonce = uuid::Uuid::new_v4().simple().to_string()[..8].to_string();

        Stack {
            catalog,
            store,
            store_url,
            brokers,
            producer,
            query_base: format!("http://{addr}"),
            nonce,
            client: reqwest::Client::new(),
        }
    }

    /// The standard `(tenant_id, ts, payload)` events schema used by scenarios.
    fn events_schema() -> Value {
        json!({"fields": [
            {"name": "tenant_id", "type": "int64"},
            {"name": "ts", "type": "timestamp_ms"},
            {"name": "payload", "type": "utf8"}
        ]})
    }

    /// Creates a fresh hypertable + a logical table named `events` in a fresh
    /// namespace, all keyed off this run's nonce so runs never collide.
    pub async fn create_events_table(&self) -> Table {
        let hypertable_name = format!("events_{}", self.nonce);
        let topic = format!("events_{}", self.nonce);
        // A stable-per-run positive namespace id (also the packing-key value).
        let namespace = NamespaceId((uuid::Uuid::new_v4().as_u128() as i64).abs() % 1_000_000 + 1);

        let ht = self
            .catalog
            .create_hypertable(
                &hypertable_name,
                &Self::events_schema(),
                &json!({"columns": ["day"]}),
                &["tenant_id".to_string(), "ts".to_string()],
                "tenant_id",
            )
            .await
            .expect("create hypertable");
        self.catalog
            .create_logical_table(namespace, "events", ht)
            .await
            .expect("create logical table");

        Table {
            hypertable_id: ht,
            hypertable_name,
            topic,
            namespace,
            namespaces: vec![namespace],
            ts_column: "ts".to_string(),
        }
    }

    /// Like `create_events_table`, but registers `count` distinct namespaces —
    /// all logical `events` tables over one shared (packed) hypertable, so their
    /// rows land in the same Parquet files. Namespace ids are per-run unique.
    pub async fn create_events_table_multi(&self, count: usize) -> Table {
        let hypertable_name = format!("events_{}", self.nonce);
        let topic = format!("events_{}", self.nonce);
        let base = (uuid::Uuid::new_v4().as_u128() as i64).abs() % 1_000_000 + 1;
        let namespaces: Vec<NamespaceId> =
            (0..count as i64).map(|i| NamespaceId(base + i)).collect();

        let ht = self
            .catalog
            .create_hypertable(
                &hypertable_name,
                &Self::events_schema(),
                &json!({"columns": ["day"]}),
                &["tenant_id".to_string(), "ts".to_string()],
                "tenant_id",
            )
            .await
            .expect("create hypertable");
        for ns in &namespaces {
            self.catalog
                .create_logical_table(*ns, "events", ht)
                .await
                .expect("create logical table");
        }

        Table {
            hypertable_id: ht,
            hypertable_name,
            topic,
            namespace: namespaces[0],
            namespaces,
            ts_column: "ts".to_string(),
        }
    }

    /// Blocks until `topic` exists with at least one partition (broker
    /// auto-create is enabled, so a metadata fetch materializes it). The ingest
    /// worker assigns partitions once at startup; without this it could race a
    /// cold topic, see zero partitions, and silently consume nothing.
    pub async fn ensure_topic(&self, topic: &str) {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            if let Ok(md) = self
                .producer
                .client()
                .fetch_metadata(Some(topic), Duration::from_secs(5))
                && let Some(t) = md.topics().iter().find(|t| t.name() == topic)
                && t.error().is_none()
                && !t.partitions().is_empty()
            {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "topic '{topic}' never became ready"
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    /// Spawns an in-process ingest worker with a fast (1s) flush interval —
    /// the default for most scenarios where test speed matters.
    pub async fn spawn_ingest(&self, table: &Table) -> IngestHandle {
        self.spawn_ingest_with(table, 1_000).await
    }

    /// Spawns an in-process ingest worker with an explicit flush interval —
    /// e.g. the freshness scenario runs the spec's real 10s interval.
    pub async fn spawn_ingest_with(&self, table: &Table, flush_interval_ms: u64) -> IngestHandle {
        self.ensure_topic(&table.topic).await;
        let config = IngestConfig {
            brokers: self.brokers.clone(),
            group_id: format!("ukiel-e2e-{}", self.nonce),
            flush_interval_ms,
            max_buffer_rows: 100_000,
            l0_slowdown_parts: ukiel_ingest::config::defaults::L0_SLOWDOWN_PARTS,
            l0_stop_parts: ukiel_ingest::config::defaults::L0_STOP_PARTS,
            warn_partitions_per_flush: ukiel_ingest::config::defaults::WARN_PARTITIONS_PER_FLUSH,
            // Scenario fixtures use 1970 epoch-ms timestamps; widen the past
            // bound so the event-time rail (issue 0004) keeps them.
            max_event_age_days: 30_000,
            max_event_future_secs: 3600,
            tables: vec![TableRoute {
                topic: table.topic.clone(),
                hypertable: table.hypertable_name.clone(),
                ts_column: table.ts_column.clone(),
            }],
            ready: None,
        };
        let worker = IngestWorker::new(self.catalog.clone(), self.store.clone(), config);
        let shutdown = CancellationToken::new();
        let token = shutdown.clone();
        let handle = tokio::spawn(async move { worker.run(token).await });
        IngestHandle { shutdown, handle }
    }

    /// Produces `count` events for `table`'s namespace, one Kafka message each,
    /// all in the same day partition. Returns the payloads produced.
    pub async fn produce_events(&self, table: &Table, count: usize) -> Vec<String> {
        let mut payloads = Vec::with_capacity(count);
        for i in 0..count {
            let payload = format!("row-{i}");
            let ts = 1_000 + i as i64; // all within 1970-01-01
            let msg = json!({
                "tenant_id": table.namespace.0,
                "ts": ts,
                "payload": payload,
            })
            .to_string();
            self.producer
                .send(
                    FutureRecord::<(), _>::to(&table.topic).payload(&msg),
                    Duration::from_secs(10),
                )
                .await
                .map_err(|(e, _)| e)
                .expect("produce");
            payloads.push(payload);
        }
        payloads
    }

    /// Produces `count` events for an explicit `tenant` (packing-key value),
    /// tagging payloads `"{tag}-{i}"` so callers can build a per-namespace model.
    /// Returns the payloads produced.
    pub async fn produce_events_for(
        &self,
        table: &Table,
        tenant: NamespaceId,
        tag: &str,
        count: usize,
    ) -> Vec<String> {
        let mut payloads = Vec::with_capacity(count);
        for i in 0..count {
            let payload = format!("{tag}-{i}");
            let ts = 1_000 + i as i64; // all within 1970-01-01
            let msg = json!({
                "tenant_id": tenant.0,
                "ts": ts,
                "payload": payload,
            })
            .to_string();
            self.producer
                .send(
                    FutureRecord::<(), _>::to(&table.topic).payload(&msg),
                    Duration::from_secs(10),
                )
                .await
                .map_err(|(e, _)| e)
                .expect("produce");
            payloads.push(payload);
        }
        payloads
    }

    /// Sends a single raw (possibly malformed) payload to `table`'s topic —
    /// for poison-stream tests. `payload` is written to Kafka verbatim.
    pub async fn produce_raw(&self, table: &Table, payload: &str) {
        self.producer
            .send(
                FutureRecord::<(), _>::to(&table.topic).payload(payload),
                Duration::from_secs(10),
            )
            .await
            .map_err(|(e, _)| e)
            .expect("produce raw");
    }

    /// Sum of stored next-offsets across all partitions of `table`'s topic for a
    /// hypertable — i.e. how many messages ingest has durably accounted for.
    pub async fn ingest_offset_sum(&self, ht: HypertableId, topic: &str) -> i64 {
        self.catalog
            .ingest_offsets(ht, topic)
            .await
            .expect("ingest_offsets")
            .values()
            .sum()
    }

    /// Spawns an in-process compactor loop (fast poll) over all hypertables.
    /// Runs a **real ukield** — every role, its own HTTP server — with its
    /// catalog pointed at `catalog_url` (plan 42's S10 points it at the fault
    /// proxy). Returns the base URL and the process's join handle, so a test can
    /// assert the process never exits.
    pub async fn spawn_ukield_with_catalog(
        &self,
        table: &Table,
        catalog_url: &str,
        shutdown: CancellationToken,
    ) -> (String, tokio::task::JoinHandle<anyhow::Result<()>>) {
        self.spawn_ukield_inner(table, catalog_url, shutdown, None)
            .await
    }

    /// `spawn_ukield_with_catalog`, with ukield running on a catalog handle the
    /// caller already owns — the only way to arm a commit-boundary fault in the
    /// process under test (S11, plan 43). The handle must point at the same
    /// database as `catalog_url`; the URL still configures the pool.
    pub async fn spawn_ukield_with_injected_catalog(
        &self,
        table: &Table,
        catalog_url: &str,
        shutdown: CancellationToken,
        catalog: PostgresCatalog,
    ) -> (String, tokio::task::JoinHandle<anyhow::Result<()>>) {
        self.spawn_ukield_inner(table, catalog_url, shutdown, Some(catalog))
            .await
    }

    async fn spawn_ukield_inner(
        &self,
        table: &Table,
        catalog_url: &str,
        shutdown: CancellationToken,
        injected: Option<PostgresCatalog>,
    ) -> (String, tokio::task::JoinHandle<anyhow::Result<()>>) {
        let cfg_toml = format!(
            r#"
            roles = ["ingest", "query", "compactor", "gc"]

            [catalog]
            url = "{catalog_url}"
            # Short and jittered: the outage must be *survived*, and the retry
            # load it generates must stay bounded (that is half the point).
            acquire_timeout_ms = 1000
            retry_base_ms = 200
            retry_max_ms = 2000
            recovery_probe_timeout_ms = 1000
            query_admission_timeout_ms = 1000
            retry_after_secs = 1

            [object_store]
            kind = "s3"
            endpoint = "{s3}"
            bucket = "ukiel"
            access_key_id = "minioadmin"
            secret_access_key = "minioadmin"
            allow_http = true

            [query]
            listen = "127.0.0.1:0"

            [ingest]
            brokers = "{brokers}"
            group_id = "s10-{nonce}"
            flush_interval_ms = 500
            max_event_age_days = 30000

            [compactor]
            poll_interval_ms = 500

            [gc]
            poll_interval_ms = 1000

            [collector]
            interval_ms = 500

            [[tables]]
            name = "{ht}"
            topic = "{topic}"
            ts_column = "ts"
            packing_key = "tenant_id"
            sort_key = ["tenant_id", "ts"]
            partition_columns = ["day"]
            namespaces = [{ns}]

            [[tables.columns]]
            name = "tenant_id"
            type = "int64"

            [[tables.columns]]
            name = "ts"
            type = "timestamp_ms"

            [[tables.columns]]
            name = "payload"
            type = "utf8"
            "#,
            s3 = env_or("UKIEL_E2E_S3", "http://127.0.0.1:19000"),
            brokers = self.brokers,
            nonce = self.nonce,
            ht = table.hypertable_name,
            topic = table.topic,
            ns = table.namespace.0,
        );
        let cfg: ukield::config::UkieldConfig =
            toml::from_str(&cfg_toml).expect("s10 ukield config");

        let (bound_tx, bound_rx) = tokio::sync::oneshot::channel();
        let handle = tokio::spawn(async move {
            ukield::run::run_with_catalog(cfg, shutdown, Some(bound_tx), injected).await
        });
        let addr = tokio::time::timeout(Duration::from_secs(60), bound_rx)
            .await
            .expect("ukield must bind its listener even if the catalog is unreachable")
            .expect("ukield died before binding");
        (format!("http://{addr}"), handle)
    }

    pub fn spawn_compactor(&self) -> CompactorHandle {
        let compactor = Compactor::new(
            self.catalog.clone(),
            self.store.clone(),
            CompactorConfig {
                worker_name: "e2e-loop".to_string(),
                poll_interval_ms: 200,
                ..CompactorConfig::default()
            },
        );
        let shutdown = CancellationToken::new();
        let token = shutdown.clone();
        let handle = tokio::spawn(async move { compactor.run(token).await });
        CompactorHandle { shutdown, handle }
    }

    /// Runs `run_once` (with a fresh cursor) until a pass merges nothing —
    /// deterministic quiescence for assertions. Returns total groups merged.
    pub async fn run_compaction_to_quiescence(&self) -> usize {
        let compactor = Compactor::new(
            self.catalog.clone(),
            self.store.clone(),
            CompactorConfig {
                worker_name: "e2e-drain".to_string(),
                ..CompactorConfig::default()
            },
        );
        let mut total = 0;
        loop {
            let stats = compactor.run_once().await.expect("compaction pass");
            total += stats.merged_groups;
            if stats.merged_groups == 0 {
                break;
            }
        }
        total
    }

    /// Number of live parts the catalog returns for a query — all parts when
    /// `key` is `None`, or those whose packing-key range contains `key`. This is
    /// exactly the file set a scan would open (catalog planning, never a store
    /// listing), so it bounds per-query fan-out.
    pub async fn live_part_count(&self, ht: HypertableId, key: Option<i64>) -> usize {
        self.catalog
            .live_parts(ht, key)
            .await
            .expect("live_parts")
            .len()
    }

    /// Count of live parts at the given compaction level for a hypertable.
    pub async fn count_parts_at_level(&self, ht: HypertableId, level: i16) -> usize {
        self.catalog
            .live_parts(ht, None)
            .await
            .expect("live_parts")
            .iter()
            .filter(|p| p.meta.level == level)
            .count()
    }

    /// Deletes all of one packing key's data from a hypertable (GDPR-style),
    /// via the compactor's atomic REPLACE deletion path. Returns the stats.
    pub async fn delete_key(
        &self,
        ht: HypertableId,
        key: i64,
    ) -> ukiel_compactor::deletion::DeletionStats {
        let hypertable = self
            .catalog
            .get_hypertable_by_id(ht)
            .await
            .expect("get hypertable");
        ukiel_compactor::deletion::delete_key(&self.catalog, &self.store, &hypertable, key)
            .await
            .expect("delete_key")
    }

    /// Whether any live part still claims `key` in its packing-key range.
    pub async fn any_live_part_claims(&self, ht: HypertableId, key: i64) -> bool {
        self.catalog
            .live_parts(ht, Some(key))
            .await
            .expect("live_parts")
            .iter()
            .any(|p| p.meta.packing_key_min <= key && p.meta.packing_key_max >= key)
    }

    /// Runs GC (reaper + orphan sweep) with **zero graces** until a full pass
    /// reaps and sweeps nothing — deterministic quiescence for S8. Reconcile is
    /// disabled: the sweep works off the intent table, which is what we assert.
    pub async fn run_gc_to_quiescence(&self) {
        let gc = ukiel_gc::Gc::new(
            self.catalog.clone(),
            self.store.clone(),
            ukiel_gc::GcConfig {
                ready: None,
                tombstone_grace_secs: 0.0,
                orphan_grace_secs: 0,
                poll_interval_ms: 1000,
                reconcile_every_n_passes: 0,
            },
        );
        loop {
            let stats = gc.run_once().await.expect("gc pass");
            if stats.reaped_parts == 0 && stats.swept_orphans == 0 {
                break;
            }
        }
    }

    /// Every object path in the bucket under this hypertable's prefix. Listing
    /// is allowed here — this is a test, not a hot path.
    pub async fn object_paths(&self, ht: HypertableId) -> std::collections::HashSet<String> {
        use futures::StreamExt;
        let prefix = object_store::path::Path::from(format!("ht/{}", ht.0));
        let mut stream = self.store.list(Some(&prefix));
        let mut paths = std::collections::HashSet::new();
        while let Some(meta) = stream.next().await {
            paths.insert(meta.expect("list object").location.to_string());
        }
        paths
    }

    /// Runs a SQL query for `namespace` through the HTTP endpoint, returning the
    /// `rows` JSON array (or an Err with the HTTP body on non-200). Pins the
    /// `rows` format explicitly (the server default is `columns`).
    pub async fn query(&self, namespace: NamespaceId, sql: &str) -> Result<Value, String> {
        let resp = self
            .client
            .post(format!("{}/api/query", self.query_base))
            .json(&json!({"namespace_id": namespace.0, "sql": sql, "format": "rows"}))
            .send()
            .await
            .expect("query request");
        let status = resp.status();
        let body: Value = resp.json().await.expect("query response json");
        if status.is_success() {
            Ok(body["rows"].clone())
        } else {
            Err(body.to_string())
        }
    }

    /// Polls `SELECT count(*)` for `namespace` until it reaches `expected` or
    /// the deadline elapses. Returns the final observed count (panics on error).
    pub async fn wait_for_row_count(
        &self,
        namespace: NamespaceId,
        expected: i64,
        timeout: Duration,
    ) -> i64 {
        let deadline = Instant::now() + timeout;
        loop {
            let rows = self
                .query(namespace, "SELECT count(*) AS n FROM events")
                .await
                .expect("count query");
            let last = rows
                .get(0)
                .and_then(|r| r.get("n"))
                .and_then(|n| n.as_i64())
                .unwrap_or(0);
            if last >= expected || Instant::now() >= deadline {
                return last;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }
}

/// A running ingest worker; drop-safe via `stop()`.
pub struct IngestHandle {
    shutdown: CancellationToken,
    handle: tokio::task::JoinHandle<Result<(), ukiel_ingest::IngestError>>,
}

impl IngestHandle {
    /// Signals shutdown (final flush) and waits for the worker to finish.
    pub async fn stop(self) {
        self.shutdown.cancel();
        self.handle.await.expect("worker join").expect("worker run");
    }

    /// Aborts the worker abruptly, simulating a crash (no final flush).
    pub fn abort(self) {
        self.handle.abort();
    }
}

/// A running compactor loop; stop with `stop()`.
pub struct CompactorHandle {
    shutdown: CancellationToken,
    handle: tokio::task::JoinHandle<Result<(), CompactorError>>,
}

impl CompactorHandle {
    /// Signals shutdown and waits for the compactor loop to finish.
    pub async fn stop(self) {
        self.shutdown.cancel();
        self.handle
            .await
            .expect("compactor join")
            .expect("compactor run");
    }
}

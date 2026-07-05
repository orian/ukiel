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

/// Env with a compose-matching default.
fn env_or(key: &str, default: &str) -> String {
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
        let store: Arc<dyn ObjectStore> = Arc::new(s3);
        let store_url = url::Url::parse("s3://ukiel/").unwrap();

        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", &brokers)
            .set("message.timeout.ms", "10000")
            .create()
            .expect("build producer");

        // In-process query server on an ephemeral port.
        let state = AppState {
            catalog: catalog.clone(),
            store: store.clone(),
            store_url: store_url.clone(),
        };
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

    /// Spawns an in-process ingest worker for `table`, returning a shutdown
    /// token and the worker's join handle. Fast flush interval for test speed.
    pub async fn spawn_ingest(&self, table: &Table) -> IngestHandle {
        self.ensure_topic(&table.topic).await;
        let config = IngestConfig {
            brokers: self.brokers.clone(),
            group_id: format!("ukiel-e2e-{}", self.nonce),
            flush_interval_ms: 1_000,
            max_buffer_rows: 100_000,
            tables: vec![TableRoute {
                topic: table.topic.clone(),
                hypertable: table.hypertable_name.clone(),
                ts_column: table.ts_column.clone(),
            }],
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

    /// Spawns an in-process compactor loop (fast poll) over all hypertables.
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

    /// Runs a SQL query for `namespace` through the HTTP endpoint, returning the
    /// `rows` JSON array (or an Err with the HTTP body on non-200).
    pub async fn query(&self, namespace: NamespaceId, sql: &str) -> Result<Value, String> {
        let resp = self
            .client
            .post(format!("{}/api/query", self.query_base))
            .json(&json!({"namespace_id": namespace.0, "sql": sql}))
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

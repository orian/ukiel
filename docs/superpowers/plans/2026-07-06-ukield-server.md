# Ukiel Plan 10: `ukield` — the single deployable binary Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** One binary (`ukield`) that runs the whole system — ingest worker, query HTTP server, compactor, and GC — against Postgres/Kafka/object store, configured by a TOML file, with idempotent table bootstrap and graceful shutdown. `make play` gives a curl-able system in two commands.

**Architecture:** Every component already exists as a library with the same shape (constructor + `run(CancellationToken)`); the e2e harness proves the in-process assembly works. `ukield` is that assembly as a product: `main` loads config, connects the catalog (runs migrations), builds the object store, **bootstraps declared tables idempotently** (the system has no tenant DDL by design, so playability requires config-declared tables), then spawns the enabled roles as tokio tasks under one `CancellationToken`, cancelled by SIGTERM/ctrl-C. The same binary later becomes the per-role production deployment by narrowing `roles = [...]` — no new machinery, only wiring. Table routes here are the pre-plan-8 `TableRoute` shape; when pipelines land, the `[[tables]] topic/ts_column` fields migrate to pipeline definitions (noted in the config file).

**Tech Stack:** toml 1 (config), anyhow 1 (binary-only error handling), tokio signals; everything else is existing workspace crates.

**Prerequisites:** Plans 1–7 executed and green (`ukiel-expr` is used for schema validation at bootstrap).

## Global Constraints

- Rust edition 2024, toolchain ≥ 1.96; datafusion 54 / arrow 58.3 / object_store 0.13 pinned together.
- Component tests: testcontainers (Postgres, Kafka) + `object_store::memory::InMemory`; run via `make test` (`--test-threads=2`). Config-parsing tests must pass without Docker.
- `anyhow` is allowed **only** in the `ukield` crate (binary); library crates keep their typed errors.
- Commit messages: conventional style, no Claude/AI attribution.
- Every task ends with `cargo fmt && cargo clippy --all-targets -- -D warnings` clean.
- Where this plan calls existing constructors (`IngestWorker::new`, `Compactor::new`, `Gc::new`, `router`, `AppState`), match their current signatures in the codebase if they have drifted — do not redesign them.

---

### Task 1: Crate scaffold + config model + example config

**Files:**
- Modify: `Cargo.toml` (workspace root: member + deps)
- Create: `crates/ukield/Cargo.toml`
- Create: `crates/ukield/src/lib.rs`
- Create: `crates/ukield/src/config.rs`
- Create: `crates/ukield/src/main.rs` (placeholder, completed in Task 3)
- Create: `ukield.example.toml`

**Interfaces:**
- Produces: `config::UkieldConfig` (full model below) with `UkieldConfig::load(path: &str) -> anyhow::Result<UkieldConfig>` (TOML + env overrides `UKIELD_CATALOG_URL`, `UKIELD_S3_ACCESS_KEY_ID`, `UKIELD_S3_SECRET_ACCESS_KEY`), and `TableConfig::schema_json() -> serde_json::Value` (builds the catalog schema JSON from `[[tables.columns]]`, including plan 7's `default`/`materialized`/`alias` attributes).

- [ ] **Step 1: Workspace wiring**

Root `Cargo.toml`: add `"crates/ukield"` to `members` and to `[workspace.dependencies]`:

```toml
toml = "1"
anyhow = "1"
ukield = { path = "crates/ukield" }
```

Create `crates/ukield/Cargo.toml`:

```toml
[package]
name = "ukield"
version.workspace = true
edition.workspace = true

[dependencies]
ukiel-core = { workspace = true }
ukiel-catalog = { workspace = true }
ukiel-ingest = { workspace = true }
ukiel-query = { workspace = true }
ukiel-compactor = { workspace = true }
ukiel-gc = { workspace = true }
ukiel-expr = { workspace = true }
object_store = { workspace = true, features = ["aws"] }
axum = { workspace = true }
tokio = { workspace = true }
tokio-util = { workspace = true }
serde = { workspace = true }
serde_json = { workspace = true }
toml = { workspace = true }
anyhow = { workspace = true }
url = { workspace = true }
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter"] }

[dev-dependencies]
testcontainers-modules = { workspace = true, features = ["kafka"] }
rdkafka = { workspace = true }
reqwest = { workspace = true }
```

Create `crates/ukield/src/main.rs` (placeholder until Task 3):

```rust
fn main() {
    println!("ukield: completed in a later task");
}
```

Create `crates/ukield/src/lib.rs`:

```rust
//! ukield: the all-in-one Ukiel server binary. Library side holds the
//! testable pieces (config, bootstrap, run); main.rs is a thin shell.

pub mod config;
```

- [ ] **Step 2: Write the failing test**

Create `crates/ukield/src/config.rs` with the model stubbed and tests at the bottom:

```rust
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
}

impl Default for QueryConfig {
    fn default() -> Self {
        Self { listen: "127.0.0.1:8080".to_string(), cache_dir: String::new() }
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(default)]
pub struct IngestSection {
    pub brokers: String,
    pub group_id: String,
    pub flush_interval_ms: u64,
    pub max_buffer_rows: usize,
}

impl Default for IngestSection {
    fn default() -> Self {
        Self {
            brokers: "127.0.0.1:9092".to_string(),
            group_id: "ukield".to_string(),
            flush_interval_ms: 10_000,
            max_buffer_rows: 100_000,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(default)]
pub struct CompactorSection {
    pub poll_interval_ms: u64,
    pub min_l0_files: usize,
}

impl Default for CompactorSection {
    fn default() -> Self {
        Self { poll_interval_ms: 5_000, min_l0_files: 2 }
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
        Self { poll_interval_ms: 60_000, tombstone_grace_secs: 900.0, orphan_grace_secs: 3600 }
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
        todo!()
    }
}

impl TableConfig {
    /// The catalog schema JSON (`TableColumns` format) for this table.
    pub fn schema_json(&self) -> serde_json::Value {
        todo!()
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
        format!("{:?}", std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos())
    }

    #[test]
    fn parses_config_with_defaults() {
        let path = write_tmp(EXAMPLE);
        let cfg = UkieldConfig::load(path.to_str().unwrap()).unwrap();

        assert_eq!(cfg.roles, vec![Role::Ingest, Role::Query, Role::Compactor, Role::Gc]);
        assert_eq!(cfg.query.listen, "127.0.0.1:8080");
        assert_eq!(cfg.ingest.flush_interval_ms, 10_000);
        assert_eq!(cfg.gc.tombstone_grace_secs, 900.0);
        assert_eq!(cfg.object_store.kind, "s3");
        assert_eq!(cfg.object_store.bucket, "ukiel");

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
```

(Note: `std::env::set_var` is `unsafe` in edition 2024 — the blocks above are required.)

- [ ] **Step 3: Run test to verify it fails**

Run: `cargo test -p ukield`
Expected: panics at `todo!()`.

- [ ] **Step 4: Implement `load` and `schema_json`**

Replace the `todo!()` bodies:

```rust
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
```

```rust
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
```

- [ ] **Step 5: Write `ukield.example.toml`** (repo root — the annotated reference used by `make play`)

```toml
# ukield example configuration. Start the compose stack first:
#   docker compose up -d --wait
# then: cargo run -p ukield -- --config ukield.example.toml

# Which workers this process runs (default: all four).
roles = ["ingest", "query", "compactor", "gc"]

[catalog]
url = "postgres://postgres:postgres@127.0.0.1:5432/postgres"

[object_store]
kind = "s3" # or "memory" for a throwaway in-process store (no MinIO needed)
endpoint = "http://127.0.0.1:19000"
bucket = "ukiel"
access_key_id = "minioadmin"
secret_access_key = "minioadmin"
allow_http = true

[query]
listen = "127.0.0.1:8080"
cache_dir = "" # non-empty = local read-through cache directory

[ingest]
brokers = "127.0.0.1:9092"
group_id = "ukield"
flush_interval_ms = 2000 # demo-friendly freshness; production default 10000
max_buffer_rows = 100000

[compactor]
poll_interval_ms = 5000
min_l0_files = 2

[gc]
poll_interval_ms = 60000
tombstone_grace_secs = 900
orphan_grace_secs = 3600

# Tables are bootstrapped idempotently at startup. `topic`/`ts_column` are the
# pre-pipelines routing config (see docs/notes/2026-07-06-pipelines.md).
[[tables]]
name = "events"
topic = "events"
ts_column = "ts"
packing_key = "tenant_id"
sort_key = ["tenant_id", "ts"]
partition_columns = ["day"]
namespaces = [1, 2, 3]

[[tables.columns]]
name = "tenant_id"
type = "int64"

[[tables.columns]]
name = "ts"
type = "timestamp_ms"

[[tables.columns]]
name = "payload"
type = "utf8"
```

- [ ] **Step 6: Run tests, lint, commit**

Run: `cargo test -p ukield && cargo build -p ukield`
Expected: config tests pass; the placeholder binary builds.

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add Cargo.toml Cargo.lock crates/ukield ukield.example.toml
git commit -m "feat: ukield crate - config model and example config"
```

---

### Task 2: Idempotent bootstrap

**Files:**
- Create: `crates/ukield/src/bootstrap.rs`
- Modify: `crates/ukield/src/lib.rs`
- Create: `crates/ukield/tests/bootstrap_test.rs`

**Interfaces:**
- Produces: `bootstrap::apply(catalog: &PostgresCatalog, tables: &[TableConfig]) -> anyhow::Result<()>` — validates each table's schema (`ukiel_expr::validate_table_schema`), creates missing hypertables and logical tables, sets `separated` placement when configured. Safe to run on every boot; never mutates existing objects (drift between config and an existing hypertable's schema is logged as a warning, not an error — v1 has no ALTER).

- [ ] **Step 1: Write the failing test**

Create `crates/ukield/tests/bootstrap_test.rs`:

```rust
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use ukiel_catalog::PostgresCatalog;
use ukiel_core::{NamespaceId, Placement};
use ukield::bootstrap;
use ukield::config::UkieldConfig;

const CONFIG: &str = r#"
    [catalog]
    url = "unused"
    [object_store]
    [[tables]]
    name = "events"
    topic = "events"
    ts_column = "ts"
    packing_key = "tenant_id"
    sort_key = ["tenant_id", "ts"]
    partition_columns = ["day"]
    placement = "separated"
    namespaces = [1, 2]
    [[tables.columns]]
    name = "tenant_id"
    type = "int64"
    [[tables.columns]]
    name = "ts"
    type = "timestamp_ms"
    [[tables.columns]]
    name = "payload"
    type = "utf8"
"#;

#[tokio::test]
async fn bootstrap_is_idempotent() {
    let pg = Postgres::default().start().await.expect("postgres");
    let port = pg.get_host_port_ipv4(5432).await.unwrap();
    let catalog =
        PostgresCatalog::connect(&format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres"))
            .await
            .unwrap();
    catalog.migrate().await.unwrap();

    let cfg: UkieldConfig = toml::from_str(CONFIG).unwrap();

    // First boot creates everything.
    bootstrap::apply(&catalog, &cfg.tables).await.unwrap();
    let ht = catalog.get_hypertable("events").await.unwrap();
    assert_eq!(ht.packing_key, "tenant_id");
    assert_eq!(ht.placement, Placement::Separated);
    for ns in [1, 2] {
        catalog.get_logical_table(NamespaceId(ns), "events").await.unwrap();
    }

    // Second boot is a no-op (same hypertable id, no duplicates, no error).
    bootstrap::apply(&catalog, &cfg.tables).await.unwrap();
    assert_eq!(catalog.get_hypertable("events").await.unwrap().id, ht.id);
    assert_eq!(catalog.list_hypertables().await.unwrap().len(), 1);
    assert_eq!(catalog.list_logical_tables(NamespaceId(1)).await.unwrap().len(), 1);

    // A bad expression is rejected up front.
    let mut bad: UkieldConfig = toml::from_str(CONFIG).unwrap();
    bad.tables[0].name = "bad_events".to_string();
    bad.tables[0].columns.push(ukield::config::ColumnConfig {
        name: "boom".to_string(),
        type_name: "float64".to_string(),
        nullable: None,
        default: Some("random()".to_string()),
        materialized: None,
        alias: None,
    });
    assert!(bootstrap::apply(&catalog, &bad.tables).await.is_err(), "volatile default must fail");
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p ukield --test bootstrap_test -- --test-threads=2`
Expected: compile error (`bootstrap` module not found).

- [ ] **Step 3: Implement `crates/ukield/src/bootstrap.rs`**

```rust
//! Idempotent table bootstrap: applies the config's [[tables]] on every boot.
//! Creates what is missing; never mutates what exists (v1 has no ALTER).

use anyhow::Context;
use ukiel_catalog::{CatalogError, PostgresCatalog};
use ukiel_core::{NamespaceId, Placement};

use crate::config::TableConfig;

pub async fn apply(catalog: &PostgresCatalog, tables: &[TableConfig]) -> anyhow::Result<()> {
    for table in tables {
        let schema = table.schema_json();
        ukiel_expr::validate_table_schema(&schema)
            .with_context(|| format!("invalid schema for table '{}'", table.name))?;

        let hypertable = match catalog.get_hypertable(&table.name).await {
            Ok(existing) => {
                if existing.table_schema != schema {
                    tracing::warn!(
                        table = %table.name,
                        "config schema differs from existing hypertable; \
                         not altering (v1 has no ALTER) — using the existing schema"
                    );
                }
                existing
            }
            Err(CatalogError::NotFound(_)) => {
                let id = catalog
                    .create_hypertable(
                        &table.name,
                        &schema,
                        &table.partition_spec(),
                        &table.sort_key,
                        &table.packing_key,
                    )
                    .await
                    .with_context(|| format!("creating hypertable '{}'", table.name))?;
                if table.placement.as_deref() == Some("separated") {
                    catalog.set_placement(id, Placement::Separated).await?;
                }
                tracing::info!(table = %table.name, id = %id, "created hypertable");
                catalog.get_hypertable(&table.name).await?
            }
            Err(e) => return Err(e.into()),
        };

        for &ns in &table.namespaces {
            match catalog.get_logical_table(NamespaceId(ns), &table.name).await {
                Ok(_) => {}
                Err(CatalogError::NotFound(_)) => {
                    catalog
                        .create_logical_table(NamespaceId(ns), &table.name, hypertable.id)
                        .await
                        .with_context(|| {
                            format!("creating logical table '{}' for namespace {ns}", table.name)
                        })?;
                    tracing::info!(table = %table.name, namespace = ns, "created logical table");
                }
                Err(e) => return Err(e.into()),
            }
        }
    }
    Ok(())
}
```

Add to `crates/ukield/src/lib.rs`:

```rust
pub mod bootstrap;
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p ukield -- --test-threads=2`
Expected: config tests + `bootstrap_is_idempotent` pass.

- [ ] **Step 5: Lint and commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add crates/ukield
git commit -m "feat: idempotent table bootstrap from config"
```

---

### Task 3: Run wiring — roles, workers, HTTP server, graceful shutdown

**Files:**
- Create: `crates/ukield/src/run.rs`
- Modify: `crates/ukield/src/lib.rs`
- Modify: `crates/ukield/src/main.rs`
- Create: `crates/ukield/tests/server_test.rs`

**Interfaces:**
- Produces:
  - `run::build_store(cfg: &ObjectStoreConfig) -> anyhow::Result<(Arc<dyn ObjectStore>, url::Url)>` — `kind = "s3"` builds `AmazonS3Builder` (endpoint/bucket/creds/region/allow_http, path-style) with url `s3://{bucket}/`; `kind = "memory"` builds `InMemory` with url `mem://ukiel/`.
  - `run::run(cfg: UkieldConfig, shutdown: CancellationToken) -> anyhow::Result<()>` — connects + migrates the catalog, builds the store (wrapping in `CachingObjectStore` when `query.cache_dir` is non-empty), bootstraps tables, spawns the enabled roles, and returns when all tasks finish after cancellation (first worker error cancels the rest and propagates).
  - `main`: parses `--config <path>` (default `ukield.toml`) and `--bootstrap-only`; installs tracing; cancels on ctrl-C/SIGTERM.

- [ ] **Step 1: Write the failing test**

Create `crates/ukield/tests/server_test.rs` — the whole binary wiring as a component test: real Postgres + Kafka (testcontainers), in-memory object store, produce over Kafka, read back over HTTP:

```rust
use std::time::Duration;

use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde_json::json;
use testcontainers_modules::kafka::apache::{Kafka, KAFKA_PORT};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tokio_util::sync::CancellationToken;
use ukield::config::UkieldConfig;

#[tokio::test(flavor = "multi_thread")]
async fn full_stack_ingest_to_query_over_http() {
    let pg = Postgres::default().start().await.expect("postgres");
    let pg_port = pg.get_host_port_ipv4(5432).await.unwrap();
    let kafka = Kafka::default().start().await.expect("kafka");
    let brokers = format!("127.0.0.1:{}", kafka.get_host_port_ipv4(KAFKA_PORT).await.unwrap());

    let config_toml = format!(
        r#"
        [catalog]
        url = "postgres://postgres:postgres@127.0.0.1:{pg_port}/postgres"
        [object_store]
        kind = "memory"
        [query]
        listen = "127.0.0.1:0"
        [ingest]
        brokers = "{brokers}"
        group_id = "ukield-test"
        flush_interval_ms = 500
        max_buffer_rows = 10000
        [[tables]]
        name = "events"
        topic = "events"
        ts_column = "ts"
        packing_key = "tenant_id"
        sort_key = ["tenant_id", "ts"]
        partition_columns = ["day"]
        namespaces = [7]
        [[tables.columns]]
        name = "tenant_id"
        type = "int64"
        [[tables.columns]]
        name = "ts"
        type = "timestamp_ms"
        [[tables.columns]]
        name = "payload"
        type = "utf8"
        "#
    );
    let cfg: UkieldConfig = toml::from_str(&config_toml).unwrap();

    let shutdown = CancellationToken::new();
    let (bound_tx, bound_rx) = tokio::sync::oneshot::channel();
    let server = {
        let cfg = cfg.clone();
        let token = shutdown.clone();
        tokio::spawn(async move { ukield::run::run_with_bound_addr(cfg, token, Some(bound_tx)).await })
    };
    let addr = tokio::time::timeout(Duration::from_secs(60), bound_rx)
        .await
        .expect("server did not report its address in time")
        .expect("server failed before binding");
    let base = format!("http://{addr}");
    let client = reqwest::Client::new();

    // Health first (server is up), then produce and poll for visibility.
    let health = client.get(format!("{base}/healthz")).send().await.unwrap();
    assert_eq!(health.status(), 200);

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("message.timeout.ms", "10000")
        .create()
        .unwrap();
    for i in 0..5 {
        let payload = json!({"tenant_id": 7, "ts": 1000 + i, "payload": format!("row-{i}")}).to_string();
        producer
            .send(FutureRecord::<(), _>::to("events").payload(&payload), Duration::from_secs(10))
            .await
            .map_err(|(e, _)| e)
            .unwrap();
    }

    let mut rows = 0i64;
    for _ in 0..60 {
        let resp = client
            .post(format!("{base}/api/query"))
            .json(&json!({"namespace_id": 7, "sql": "SELECT count(*) AS n FROM events"}))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let body: serde_json::Value = resp.json().await.unwrap();
        rows = body["rows"][0]["n"].as_i64().unwrap_or(0);
        if rows == 5 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert_eq!(rows, 5, "all produced rows must become queryable");

    // Graceful shutdown: cancel and the run future returns Ok.
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(30), server)
        .await
        .expect("shutdown timed out")
        .unwrap()
        .unwrap();
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p ukield --test server_test -- --test-threads=1`
Expected: compile error (`run` module not found).

- [ ] **Step 3: Implement `crates/ukield/src/run.rs`**

```rust
//! Assembles and runs the enabled roles under one CancellationToken.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context;
use object_store::aws::AmazonS3Builder;
use object_store::memory::InMemory;
use object_store::ObjectStore;
use tokio_util::sync::CancellationToken;
use ukiel_catalog::PostgresCatalog;
use ukiel_compactor::compactor::{Compactor, CompactorConfig};
use ukiel_gc::{Gc, GcConfig};
use ukiel_ingest::config::{IngestConfig, TableRoute};
use ukiel_ingest::consumer::IngestWorker;
use ukiel_query::cache::CachingObjectStore;
use ukiel_query::server::{router, AppState};

use crate::config::{ObjectStoreConfig, Role, UkieldConfig};

pub fn build_store(
    cfg: &ObjectStoreConfig,
) -> anyhow::Result<(Arc<dyn ObjectStore>, url::Url)> {
    match cfg.kind.as_str() {
        "memory" => Ok((Arc::new(InMemory::new()), url::Url::parse("mem://ukiel/")?)),
        "s3" => {
            let s3 = AmazonS3Builder::new()
                .with_endpoint(&cfg.endpoint)
                .with_bucket_name(&cfg.bucket)
                .with_access_key_id(&cfg.access_key_id)
                .with_secret_access_key(&cfg.secret_access_key)
                .with_region(&cfg.region)
                .with_allow_http(cfg.allow_http)
                .with_virtual_hosted_style_request(false)
                .build()
                .context("building s3 object store")?;
            let url = url::Url::parse(&format!("s3://{}/", cfg.bucket))?;
            Ok((Arc::new(s3), url))
        }
        other => anyhow::bail!("unknown object_store.kind '{other}' (expected 's3' or 'memory')"),
    }
}

pub async fn run(cfg: UkieldConfig, shutdown: CancellationToken) -> anyhow::Result<()> {
    run_with_bound_addr(cfg, shutdown, None).await
}

/// Like `run`, reporting the query server's bound address (tests bind port 0).
pub async fn run_with_bound_addr(
    cfg: UkieldConfig,
    shutdown: CancellationToken,
    bound: Option<tokio::sync::oneshot::Sender<SocketAddr>>,
) -> anyhow::Result<()> {
    let catalog = PostgresCatalog::connect(&cfg.catalog.url)
        .await
        .context("connecting to catalog")?;
    catalog.migrate().await.context("running migrations")?;

    let (base_store, store_url) = build_store(&cfg.object_store)?;
    let store: Arc<dyn ObjectStore> = if cfg.query.cache_dir.is_empty() {
        base_store
    } else {
        Arc::new(CachingObjectStore::new(base_store, PathBuf::from(&cfg.query.cache_dir)))
    };

    crate::bootstrap::apply(&catalog, &cfg.tables).await?;

    let mut tasks: tokio::task::JoinSet<anyhow::Result<&'static str>> =
        tokio::task::JoinSet::new();

    if cfg.roles.contains(&Role::Ingest) {
        let routes: Vec<TableRoute> = cfg
            .tables
            .iter()
            .filter_map(|t| {
                t.topic.as_ref().map(|topic| TableRoute {
                    topic: topic.clone(),
                    hypertable: t.name.clone(),
                    ts_column: t.ts_column.clone(),
                })
            })
            .collect();
        if routes.is_empty() {
            tracing::warn!("ingest role enabled but no [[tables]] declares a topic");
        } else {
            let worker = IngestWorker::new(
                catalog.clone(),
                store.clone(),
                IngestConfig {
                    brokers: cfg.ingest.brokers.clone(),
                    group_id: cfg.ingest.group_id.clone(),
                    flush_interval_ms: cfg.ingest.flush_interval_ms,
                    max_buffer_rows: cfg.ingest.max_buffer_rows,
                    tables: routes,
                },
            );
            let token = shutdown.clone();
            tasks.spawn(async move {
                worker.run(token).await.context("ingest worker")?;
                Ok("ingest")
            });
        }
    }

    if cfg.roles.contains(&Role::Query) {
        let state = AppState {
            catalog: catalog.clone(),
            store: store.clone(),
            store_url: store_url.clone(),
        };
        let listener = tokio::net::TcpListener::bind(&cfg.query.listen)
            .await
            .with_context(|| format!("binding query listener on {}", cfg.query.listen))?;
        let addr = listener.local_addr()?;
        tracing::info!(%addr, "query server listening");
        if let Some(tx) = bound {
            let _ = tx.send(addr);
        }
        let token = shutdown.clone();
        tasks.spawn(async move {
            axum::serve(listener, router(state))
                .with_graceful_shutdown(async move { token.cancelled().await })
                .await
                .context("query server")?;
            Ok("query")
        });
    }

    if cfg.roles.contains(&Role::Compactor) {
        let compactor = Compactor::new(
            catalog.clone(),
            store.clone(),
            CompactorConfig {
                poll_interval_ms: cfg.compactor.poll_interval_ms,
                min_l0_files: cfg.compactor.min_l0_files,
                ..CompactorConfig::default()
            },
        );
        let token = shutdown.clone();
        tasks.spawn(async move {
            compactor.run(token).await.context("compactor")?;
            Ok("compactor")
        });
    }

    if cfg.roles.contains(&Role::Gc) {
        let gc = Gc::new(
            catalog.clone(),
            store.clone(),
            GcConfig {
                poll_interval_ms: cfg.gc.poll_interval_ms,
                tombstone_grace_secs: cfg.gc.tombstone_grace_secs,
                orphan_grace_secs: cfg.gc.orphan_grace_secs,
                ..GcConfig::default()
            },
        );
        let token = shutdown.clone();
        tasks.spawn(async move {
            gc.run(token).await.context("gc")?;
            Ok("gc")
        });
    }

    // First failure cancels everything; clean exits are logged as they land.
    let mut result = Ok(());
    while let Some(joined) = tasks.join_next().await {
        match joined.expect("worker task panicked") {
            Ok(role) => tracing::info!(role, "worker stopped"),
            Err(e) => {
                tracing::error!(error = %e, "worker failed; shutting down");
                shutdown.cancel();
                if result.is_ok() {
                    result = Err(e);
                }
            }
        }
    }
    result
}
```

(If `GcConfig` or `CompactorConfig` gained fields since their plans — e.g. plan 9's `reconcile_every_n_passes` — the `..Default::default()` spreads absorb them.)

Add to `crates/ukield/src/lib.rs`:

```rust
pub mod run;
```

Replace `crates/ukield/src/main.rs`:

```rust
use tokio_util::sync::CancellationToken;
use ukield::config::UkieldConfig;

fn parse_args() -> (String, bool) {
    let mut config_path = "ukield.toml".to_string();
    let mut bootstrap_only = false;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => {
                config_path = args.next().unwrap_or_else(|| {
                    eprintln!("--config requires a path");
                    std::process::exit(2);
                });
            }
            "--bootstrap-only" => bootstrap_only = true,
            other => {
                eprintln!("unknown argument '{other}'\nusage: ukield [--config <path>] [--bootstrap-only]");
                std::process::exit(2);
            }
        }
    }
    (config_path, bootstrap_only)
}

async fn shutdown_signal(token: CancellationToken) {
    let ctrl_c = tokio::signal::ctrl_c();
    #[cfg(unix)]
    {
        let mut term =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("install SIGTERM handler");
        tokio::select! {
            _ = ctrl_c => {},
            _ = term.recv() => {},
        }
    }
    #[cfg(not(unix))]
    {
        let _ = ctrl_c.await;
    }
    tracing::info!("shutdown signal received");
    token.cancel();
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let (config_path, bootstrap_only) = parse_args();
    let cfg = UkieldConfig::load(&config_path)?;

    if bootstrap_only {
        let catalog = ukiel_catalog::PostgresCatalog::connect(&cfg.catalog.url).await?;
        catalog.migrate().await?;
        ukield::bootstrap::apply(&catalog, &cfg.tables).await?;
        tracing::info!("bootstrap complete");
        return Ok(());
    }

    let shutdown = CancellationToken::new();
    tokio::spawn(shutdown_signal(shutdown.clone()));
    ukield::run::run(cfg, shutdown).await
}
```

(Add `ukiel-catalog` usage in main via the existing dependency; `tracing-subscriber` is already in Task 1's Cargo.toml.)

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p ukield --test server_test -- --test-threads=1`
Expected: `full_stack_ingest_to_query_over_http ... ok` (Kafka container cold-start can take ~30s).

- [ ] **Step 5: Run the whole crate's tests, lint, commit**

Run: `cargo test -p ukield -- --test-threads=1`

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add crates/ukield
git commit -m "feat: ukield binary - role wiring, http server, graceful shutdown"
```

---

### Task 4: `make play`, quickstart docs, roadmap

**Files:**
- Modify: `Makefile`
- Modify: `README.md`
- Modify: `docs/superpowers/plans/2026-07-05-ukiel-v1-roadmap.md`

- [ ] **Step 1: Add the Makefile target**

Add to `Makefile` (and `play` to the `.PHONY` line):

```make
# Run the all-in-one server against the compose stack with the example config.
play:
	docker compose up -d --wait
	cargo run -p ukield -- --config ukield.example.toml
```

- [ ] **Step 2: Manual end-to-end verification (required before committing)**

Terminal A:

```bash
make play
```

Expected: `query server listening` on `127.0.0.1:8080`, bootstrap logs for `events`.

Terminal B:

```bash
echo '{"tenant_id": 1, "ts": 1751800000000, "payload": "hello ukiel"}' | \
  docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server localhost:9092 --topic events

sleep 3

curl -s -X POST http://127.0.0.1:8080/api/query \
  -H 'content-type: application/json' \
  -d '{"namespace_id": 1, "sql": "SELECT payload FROM events"}'
```

Expected: `{"rows":[{"payload":"hello ukiel"}]}`.
Then ctrl-C in terminal A — expected: "shutdown signal received", workers stop, clean exit.

- [ ] **Step 3: README quickstart**

Add to `README.md`, directly after the `## Development` heading's intro paragraph (before the crate list):

````markdown
### Quickstart

```sh
make play           # compose stack (Kafka/Postgres/MinIO) + the ukield server
```

Then, in another terminal, produce an event and query it back:

```sh
echo '{"tenant_id": 1, "ts": 1751800000000, "payload": "hello"}' | \
  docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server localhost:9092 --topic events

curl -s -X POST http://127.0.0.1:8080/api/query \
  -H 'content-type: application/json' \
  -d '{"namespace_id": 1, "sql": "SELECT payload FROM events"}'
```

Tables, namespaces, and server roles are declared in `ukield.example.toml`.
````

And add to the crate list after `ukiel-e2e`:

```markdown
- `crates/ukield` — the all-in-one server binary: ingest + query + compactor
  + GC in one process (per-role deployment via `roles = [...]`), idempotent
  table bootstrap from TOML config, graceful shutdown.
```

- [ ] **Step 4: Roadmap row**

Add after plan 9's row (or after plan 8 if 9 has no row yet) in `docs/superpowers/plans/2026-07-05-ukiel-v1-roadmap.md`:

```markdown
| 10 | `2026-07-06-ukield-server.md` | `ukield`: single deployable binary — config-driven role wiring (ingest/query/compactor/gc), idempotent table bootstrap, graceful shutdown, `make play` quickstart | **Executed** |
```

- [ ] **Step 5: Full verification and commit**

Run: `cargo fmt --check && cargo clippy --all-targets -- -D warnings && make test`
Expected: all green.

```bash
git add Makefile README.md docs/superpowers/plans/2026-07-05-ukiel-v1-roadmap.md
git commit -m "docs: ukield quickstart, make play target, roadmap entry"
```

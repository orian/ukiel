# Ukiel V1 Plan 3: Query (DataFusion + SQL endpoint + cache) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `ukiel-query` crate: a DataFusion `TableProvider` that plans scans from the catalog (packing-key-pruned part list, never an object-store listing), enforces namespace isolation by injecting a filter at plan time, serves SQL over HTTP (axum), and reads Parquet through a local-disk read-through cache.

**Architecture:** Per request, a `SessionContext` is built for one namespace. Table resolution goes through a custom `SchemaProvider` (`(namespace, table name)` → catalog lookup → `UkielTableProvider`). The provider's `scan()` asks the catalog for live parts whose packing-key range contains the namespace's key, builds a Parquet `DataSourceExec` over exactly those files, and wraps it in a mandatory `FilterExec(packing_key = key)` **before** any projection — isolation is enforced in the physical plan, not by rewriting SQL. v1 convention: a logical table's slice is `packing_key == namespace_id` (richer key mappings come later). DDL/DML are rejected via `SQLOptions`.

**Tech Stack:** datafusion 54 (arrow/parquet 58.3, object_store 0.13 — already pinned), axum 0.8, url 2. Tests use testcontainers Postgres + the `object_store::memory::InMemory` store (no MinIO needed until the e2e plan).

**Prerequisites:** Plans 1 and 2 are implemented and green (`ukiel-core`, `ukiel-catalog`, `ukiel-ingest` — the query tests reuse `ukiel_ingest::rows_to_parquet` to fabricate Parquet files).

## Global Constraints

- Rust edition 2024, toolchain ≥ 1.96.
- datafusion 54 / arrow 58.3 / object_store 0.13 — pinned together; never bump one alone.
- Use `datafusion::arrow::...` re-exports inside `ukiel-query` (don't add a direct arrow dep there).
- Integration tests need Docker (Postgres) but NOT MinIO/Kafka — the object store is `InMemory` registered under `mem://ukiel/`.
- Commit messages: conventional style, no Claude/AI attribution.
- Every task ends with `cargo fmt && cargo clippy --all-targets -- -D warnings` clean.
- DataFusion's datasource APIs move between majors. The code below targets 54; if a compile error points at a moved type or changed signature (`FileScanConfigBuilder`, `FileGroup`, `ParquetSource`, `DataSourceExec`), check docs.rs/datafusion/54.0.0 and adapt minimally — keep the plan shape (DataSourceExec → FilterExec → ProjectionExec) exactly as designed.

---

### Task 1: Catalog lookups needed by the query path

**Files:**
- Modify: `crates/ukiel-catalog/src/tables.rs`
- Modify: `crates/ukiel-catalog/tests/catalog_test.rs`

**Interfaces:**
- Produces (on `PostgresCatalog`):
  - `get_hypertable_by_id(&self, id: HypertableId) -> Result<Hypertable, CatalogError>` (NotFound if missing)
  - `list_logical_tables(&self, namespace_id: NamespaceId) -> Result<Vec<LogicalTable>, CatalogError>` (ordered by name; empty vec when none)

- [ ] **Step 1: Write the failing test**

Append to `crates/ukiel-catalog/tests/catalog_test.rs`:

```rust
#[tokio::test]
async fn hypertable_by_id_and_namespace_table_listing() {
    let (_pg, catalog) = common::setup().await;
    let ht = setup_hypertable(&catalog).await;

    let fetched = catalog.get_hypertable_by_id(ht).await.unwrap();
    assert_eq!(fetched.id, ht);
    assert_eq!(fetched.name, "events");

    let err = catalog.get_hypertable_by_id(ukiel_core::HypertableId(999_999)).await.unwrap_err();
    assert!(matches!(err, CatalogError::NotFound(_)));

    catalog.create_logical_table(NamespaceId(1), "events", ht).await.unwrap();
    catalog.create_logical_table(NamespaceId(1), "clicks", ht).await.unwrap();
    catalog.create_logical_table(NamespaceId(2), "events", ht).await.unwrap();

    let tables = catalog.list_logical_tables(NamespaceId(1)).await.unwrap();
    let names: Vec<_> = tables.iter().map(|t| t.name.as_str()).collect();
    assert_eq!(names, vec!["clicks", "events"]);
    assert!(catalog.list_logical_tables(NamespaceId(99)).await.unwrap().is_empty());
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p ukiel-catalog hypertable_by_id`
Expected: compile error (`get_hypertable_by_id` not found).

- [ ] **Step 3: Implement**

Append to the `impl PostgresCatalog` block in `crates/ukiel-catalog/src/tables.rs`:

```rust
    pub async fn get_hypertable_by_id(
        &self,
        id: HypertableId,
    ) -> Result<Hypertable, CatalogError> {
        let row = sqlx::query(
            "SELECT id, name, table_schema, partition_spec, sort_key, packing_key
             FROM hypertables WHERE id = $1",
        )
        .bind(id.0)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| CatalogError::NotFound(format!("hypertable id {id}")))?;

        Ok(Hypertable {
            id: HypertableId(row.get("id")),
            name: row.get("name"),
            table_schema: row.get("table_schema"),
            partition_spec: row.get("partition_spec"),
            sort_key: row.get("sort_key"),
            packing_key: row.get("packing_key"),
        })
    }

    pub async fn list_logical_tables(
        &self,
        namespace_id: NamespaceId,
    ) -> Result<Vec<LogicalTable>, CatalogError> {
        let rows = sqlx::query(
            "SELECT id, namespace_id, name, hypertable_id, column_mapping
             FROM logical_tables WHERE namespace_id = $1 ORDER BY name",
        )
        .bind(namespace_id.0)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|row| LogicalTable {
                id: LogicalTableId(row.get("id")),
                namespace_id: NamespaceId(row.get("namespace_id")),
                name: row.get("name"),
                hypertable_id: HypertableId(row.get("hypertable_id")),
                column_mapping: row.get("column_mapping"),
            })
            .collect())
    }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p ukiel-catalog`
Expected: all pass.

- [ ] **Step 5: Lint and commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add crates/ukiel-catalog
git commit -m "feat: hypertable-by-id and per-namespace table listing"
```

---

### Task 2: ukiel-query crate + namespace-isolated TableProvider

**Files:**
- Modify: `Cargo.toml` (workspace root)
- Create: `crates/ukiel-query/Cargo.toml`
- Create: `crates/ukiel-query/src/lib.rs`
- Create: `crates/ukiel-query/src/error.rs`
- Create: `crates/ukiel-query/src/provider.rs`
- Create: `crates/ukiel-query/tests/query_test.rs` (first test)

**Interfaces:**
- Consumes: `live_parts` (plan 1), `arrow_schema_from_json` (plan 2 Task 1), `rows_to_parquet` (plan 2, test-only).
- Produces:
  - `QueryError` enum
  - `UkielTableProvider::try_new(catalog: PostgresCatalog, hypertable: Hypertable, packing_key_value: i64, store_url: ObjectStoreUrl) -> Result<UkielTableProvider, QueryError>` implementing `datafusion::datasource::TableProvider`. Scan plan shape: `DataSourceExec(parquet files from catalog)` → `FilterExec(packing_key = value)` → optional `ProjectionExec` → optional `GlobalLimitExec`.

- [ ] **Step 1: Add workspace deps**

Add to `[workspace.dependencies]` in the root `Cargo.toml`:

```toml
datafusion = "54"
axum = "0.8"
async-trait = "0.1"
url = "2"
tempfile = "3"
reqwest = { version = "0.12", default-features = false, features = ["json", "rustls-tls"] }
ukiel-query = { path = "crates/ukiel-query" }
```

Add `"crates/ukiel-query"` to `members`.

- [ ] **Step 2: Scaffold the crate**

Create `crates/ukiel-query/Cargo.toml`:

```toml
[package]
name = "ukiel-query"
version.workspace = true
edition.workspace = true

[dependencies]
ukiel-core = { workspace = true }
ukiel-catalog = { workspace = true }
datafusion = { workspace = true }
object_store = { workspace = true }
axum = { workspace = true }
tokio = { workspace = true }
serde = { workspace = true }
serde_json = { workspace = true }
thiserror = { workspace = true }
async-trait = { workspace = true }
futures = { workspace = true }
url = { workspace = true }
uuid = { workspace = true }
bytes = { workspace = true }

[dev-dependencies]
ukiel-ingest = { workspace = true }
testcontainers-modules = { workspace = true }
tempfile = { workspace = true }
reqwest = { workspace = true }
```

Create `crates/ukiel-query/src/error.rs`:

```rust
#[derive(Debug, thiserror::Error)]
pub enum QueryError {
    #[error(transparent)]
    Catalog(#[from] ukiel_catalog::CatalogError),
    #[error(transparent)]
    Schema(#[from] ukiel_core::SchemaError),
    #[error(transparent)]
    DataFusion(#[from] datafusion::error::DataFusionError),
    #[error("unknown table '{0}'")]
    UnknownTable(String),
}
```

Create `crates/ukiel-query/src/lib.rs`:

```rust
//! DataFusion query serving over the Ukiel catalog.

mod error;
pub mod provider;

pub use error::QueryError;
```

- [ ] **Step 3: Write the failing test**

Create `crates/ukiel-query/tests/query_test.rs`. This file also carries the shared test harness used by later tasks:

```rust
use std::sync::Arc;

use datafusion::arrow::array::{Array, Int64Array, StringArray};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::prelude::SessionContext;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::ObjectStore;
use serde_json::json;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::ContainerAsync;
use ukiel_catalog::PostgresCatalog;
use ukiel_core::{arrow_schema_from_json, CommitOp, HypertableId, NamespaceId, PartMeta};
use ukiel_ingest::rows_to_parquet;
use ukiel_query::provider::UkielTableProvider;

pub const STORE_URL: &str = "mem://ukiel/";

pub struct Harness {
    pub _pg: ContainerAsync<Postgres>,
    pub catalog: PostgresCatalog,
    pub store: Arc<dyn ObjectStore>,
    pub ht: HypertableId,
}

fn events_schema() -> serde_json::Value {
    json!({"fields": [
        {"name": "tenant_id", "type": "int64"},
        {"name": "ts", "type": "timestamp_ms"},
        {"name": "payload", "type": "utf8"}
    ]})
}

/// Seeds: one packed file with namespaces 1 and 2, one file with only
/// namespace 2. Namespace 1 rows: (1, 100, "a1"), (1, 200, "a2").
/// Namespace 2 rows: (2, 150, "b1") packed + (2, 300, "b2") dedicated.
pub async fn setup() -> Harness {
    let pg = Postgres::default().start().await.expect("postgres");
    let port = pg.get_host_port_ipv4(5432).await.unwrap();
    let catalog =
        PostgresCatalog::connect(&format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres"))
            .await
            .unwrap();
    catalog.migrate().await.unwrap();

    let ht = catalog
        .create_hypertable(
            "events",
            &events_schema(),
            &json!({"columns": ["day"]}),
            &["tenant_id".to_string(), "ts".to_string()],
            "tenant_id",
        )
        .await
        .unwrap();
    catalog.create_logical_table(NamespaceId(1), "events", ht).await.unwrap();
    catalog.create_logical_table(NamespaceId(2), "events", ht).await.unwrap();

    let schema = arrow_schema_from_json(&events_schema()).unwrap();
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    let packed = rows_to_parquet(
        &schema,
        "tenant_id",
        "ts",
        vec![
            json!({"tenant_id": 1, "ts": 100, "payload": "a1"}),
            json!({"tenant_id": 1, "ts": 200, "payload": "a2"}),
            json!({"tenant_id": 2, "ts": 150, "payload": "b1"}),
        ],
    )
    .unwrap();
    let dedicated = rows_to_parquet(
        &schema,
        "tenant_id",
        "ts",
        vec![json!({"tenant_id": 2, "ts": 300, "payload": "b2"})],
    )
    .unwrap();

    let mut parts = Vec::new();
    for (name, part) in [("packed", packed), ("dedicated", dedicated)] {
        let path = format!("ht/{ht}/L0/{name}.parquet");
        let size = part.bytes.len() as i64;
        store.put(&Path::from(path.clone()), part.bytes.into()).await.unwrap();
        parts.push(PartMeta {
            path,
            partition_values: json!({"day": "1970-01-01"}),
            packing_key_min: part.key_min,
            packing_key_max: part.key_max,
            row_count: part.row_count,
            size_bytes: size,
            level: 0,
            column_stats: None,
        });
    }
    catalog.commit(ht, CommitOp::Add { parts }, None).await.unwrap();

    Harness { _pg: pg, catalog, store, ht }
}

pub fn all_strings(batches: &[RecordBatch], column: &str) -> Vec<String> {
    let mut out = Vec::new();
    for b in batches {
        let idx = b.schema().index_of(column).unwrap();
        let arr: &StringArray = b.column(idx).as_any().downcast_ref().unwrap();
        for i in 0..arr.len() {
            out.push(arr.value(i).to_string());
        }
    }
    out
}

pub fn all_i64(batches: &[RecordBatch], column: &str) -> Vec<i64> {
    let mut out = Vec::new();
    for b in batches {
        let idx = b.schema().index_of(column).unwrap();
        let arr: &Int64Array = b.column(idx).as_any().downcast_ref().unwrap();
        for i in 0..arr.len() {
            out.push(arr.value(i));
        }
    }
    out
}

/// Registers the provider directly on a bare SessionContext (schema-provider
/// wiring is exercised separately in the context tests).
async fn ctx_with_provider(h: &Harness, namespace: i64) -> SessionContext {
    let ctx = SessionContext::new();
    ctx.register_object_store(&url::Url::parse(STORE_URL).unwrap(), h.store.clone());
    let ht = h.catalog.get_hypertable_by_id(h.ht).await.unwrap();
    let provider = UkielTableProvider::try_new(
        h.catalog.clone(),
        ht,
        namespace,
        ObjectStoreUrl::parse(STORE_URL).unwrap(),
    )
    .unwrap();
    ctx.register_table("events", Arc::new(provider)).unwrap();
    ctx
}

#[tokio::test]
async fn namespace_sees_only_its_rows_even_in_packed_files() {
    let h = setup().await;

    // Namespace 1: rows only from the packed file, and only its own.
    let ctx = ctx_with_provider(&h, 1).await;
    let batches =
        ctx.sql("SELECT payload FROM events ORDER BY ts").await.unwrap().collect().await.unwrap();
    assert_eq!(all_strings(&batches, "payload"), vec!["a1", "a2"]);

    // Namespace 2: one row from the shared file, one from its dedicated file.
    let ctx = ctx_with_provider(&h, 2).await;
    let batches =
        ctx.sql("SELECT payload FROM events ORDER BY ts").await.unwrap().collect().await.unwrap();
    assert_eq!(all_strings(&batches, "payload"), vec!["b1", "b2"]);

    // A namespace with no data gets an empty result, not an error.
    let ctx = ctx_with_provider(&h, 42).await;
    let batches = ctx.sql("SELECT * FROM events").await.unwrap().collect().await.unwrap();
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 0);
}

#[tokio::test]
async fn projection_excluding_packing_key_still_isolates() {
    let h = setup().await;
    let ctx = ctx_with_provider(&h, 1).await;
    // 'payload' only: the packing key is projected away AFTER the isolation
    // filter — this query must not see namespace 2's "b1" from the packed file.
    let batches = ctx
        .sql("SELECT payload FROM events WHERE ts < 1000 ORDER BY payload")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(all_strings(&batches, "payload"), vec!["a1", "a2"]);

    // Aggregates work through the provider.
    let ctx = ctx_with_provider(&h, 2).await;
    let batches =
        ctx.sql("SELECT count(*) AS n FROM events").await.unwrap().collect().await.unwrap();
    assert_eq!(all_i64(&batches, "n"), vec![2]);
}
```

- [ ] **Step 4: Run test to verify it fails**

Run: `cargo test -p ukiel-query`
Expected: compile error (`provider` module empty / `UkielTableProvider` not found).

- [ ] **Step 5: Implement `crates/ukiel-query/src/provider.rs`**

```rust
//! A DataFusion TableProvider that plans scans from the Ukiel catalog and
//! enforces namespace isolation by injecting `packing_key = value` into the
//! physical plan, upstream of any projection.

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::common::Result as DfResult;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::datasource::physical_plan::{FileGroup, FileScanConfigBuilder, ParquetSource};
use datafusion::datasource::source::DataSourceExec;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{Expr, Operator, TableProviderFilterPushDown};
use datafusion::physical_expr::expressions::{binary, col, lit};
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::limit::GlobalLimitExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::scalar::ScalarValue;
use ukiel_catalog::PostgresCatalog;
use ukiel_core::{arrow_schema_from_json, Hypertable};

use crate::QueryError;

pub struct UkielTableProvider {
    catalog: PostgresCatalog,
    hypertable: Hypertable,
    schema: SchemaRef,
    packing_key_value: i64,
    store_url: ObjectStoreUrl,
}

impl UkielTableProvider {
    pub fn try_new(
        catalog: PostgresCatalog,
        hypertable: Hypertable,
        packing_key_value: i64,
        store_url: ObjectStoreUrl,
    ) -> Result<Self, QueryError> {
        let schema = Arc::new(arrow_schema_from_json(&hypertable.table_schema)?);
        Ok(Self { catalog, hypertable, schema, packing_key_value, store_url })
    }
}

#[async_trait]
impl TableProvider for UkielTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DfResult<Vec<TableProviderFilterPushDown>> {
        // We don't evaluate user filters ourselves; DataFusion re-applies them.
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        // Catalog-pruned file list: only parts whose packing-key range
        // contains this namespace's key. Never lists the object store.
        let parts = self
            .catalog
            .live_parts(self.hypertable.id, Some(self.packing_key_value))
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let files: Vec<PartitionedFile> = parts
            .iter()
            .map(|p| PartitionedFile::new(p.meta.path.clone(), p.meta.size_bytes as u64))
            .collect();

        let source = Arc::new(ParquetSource::default());
        let config = FileScanConfigBuilder::new(self.store_url.clone(), self.schema.clone(), source)
            .with_file_group(FileGroup::new(files))
            .build();
        let scan: Arc<dyn ExecutionPlan> = DataSourceExec::from_data_source(config);

        // Mandatory namespace isolation, before projection can drop the column.
        let predicate = binary(
            col(&self.hypertable.packing_key, &self.schema)?,
            Operator::Eq,
            lit(ScalarValue::Int64(Some(self.packing_key_value))),
            &self.schema,
        )?;
        let mut plan: Arc<dyn ExecutionPlan> = Arc::new(FilterExec::try_new(predicate, scan)?);

        if let Some(indices) = projection {
            let exprs = indices
                .iter()
                .map(|&i| {
                    let field = self.schema.field(i);
                    Ok((col(field.name(), &self.schema)?, field.name().clone()))
                })
                .collect::<DfResult<Vec<_>>>()?;
            plan = Arc::new(ProjectionExec::try_new(exprs, plan)?);
        }
        if let Some(n) = limit {
            plan = Arc::new(GlobalLimitExec::new(plan, 0, Some(n)));
        }
        Ok(plan)
    }
}
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `cargo test -p ukiel-query`
Expected: both tests pass. (If DataFusion 54's `FileGroup`/builder API differs, see the Global Constraints note — the plan shape must stay DataSourceExec → FilterExec → ProjectionExec.)

- [ ] **Step 7: Lint and commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add Cargo.toml Cargo.lock crates/ukiel-query
git commit -m "feat: namespace-isolated datafusion table provider over the catalog"
```

---

### Task 3: Per-namespace SessionContext (schema + catalog providers)

**Files:**
- Create: `crates/ukiel-query/src/context.rs`
- Modify: `crates/ukiel-query/src/lib.rs`
- Modify: `crates/ukiel-query/tests/query_test.rs`

**Interfaces:**
- Consumes: `UkielTableProvider` (Task 2), `list_logical_tables` / `get_logical_table` / `get_hypertable_by_id` (Task 1).
- Produces:
  - `session_for_namespace(catalog: &PostgresCatalog, namespace: NamespaceId, store: Arc<dyn ObjectStore>, store_url: &url::Url) -> Result<SessionContext, QueryError>` — default catalog `ukiel`, schema `public`; tables resolve lazily by name; the object store is registered on the context.

- [ ] **Step 1: Write the failing test**

Append to `crates/ukiel-query/tests/query_test.rs`:

```rust
use ukiel_query::context::session_for_namespace;

#[tokio::test]
async fn session_resolves_tables_by_namespace() {
    let h = setup().await;
    let store_url = url::Url::parse(STORE_URL).unwrap();

    let ctx = session_for_namespace(&h.catalog, NamespaceId(1), h.store.clone(), &store_url)
        .await
        .unwrap();
    let batches =
        ctx.sql("SELECT payload FROM events ORDER BY ts").await.unwrap().collect().await.unwrap();
    assert_eq!(all_strings(&batches, "payload"), vec!["a1", "a2"]);

    // Unknown table -> planning error, not a panic.
    assert!(ctx.sql("SELECT * FROM nope").await.is_err());

    // A namespace without that logical table cannot see it at all.
    let ctx = session_for_namespace(&h.catalog, NamespaceId(42), h.store.clone(), &store_url)
        .await
        .unwrap();
    assert!(ctx.sql("SELECT * FROM events").await.is_err());
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p ukiel-query session_resolves`
Expected: compile error (`context` module not found).

- [ ] **Step 3: Implement `crates/ukiel-query/src/context.rs`**

```rust
//! Builds a per-namespace SessionContext: custom catalog/schema providers
//! resolve table names through the Ukiel catalog, scoped to one namespace.

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::{CatalogProvider, SchemaProvider};
use datafusion::common::Result as DfResult;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::execution::config::SessionConfig;
use datafusion::prelude::SessionContext;
use object_store::ObjectStore;
use ukiel_catalog::{CatalogError, PostgresCatalog};
use ukiel_core::NamespaceId;

use crate::provider::UkielTableProvider;
use crate::QueryError;

pub async fn session_for_namespace(
    catalog: &PostgresCatalog,
    namespace: NamespaceId,
    store: Arc<dyn ObjectStore>,
    store_url: &url::Url,
) -> Result<SessionContext, QueryError> {
    let table_names: Vec<String> = catalog
        .list_logical_tables(namespace)
        .await?
        .into_iter()
        .map(|t| t.name)
        .collect();

    let config = SessionConfig::new().with_default_catalog_and_schema("ukiel", "public");
    let ctx = SessionContext::new_with_config(config);
    ctx.register_object_store(store_url, store);

    let schema = Arc::new(UkielSchemaProvider {
        catalog: catalog.clone(),
        namespace,
        table_names,
        store_url: ObjectStoreUrl::parse(store_url.as_str())?,
    });
    ctx.register_catalog("ukiel", Arc::new(UkielCatalogProvider { schema }));
    Ok(ctx)
}

struct UkielCatalogProvider {
    schema: Arc<UkielSchemaProvider>,
}

impl CatalogProvider for UkielCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn schema_names(&self) -> Vec<String> {
        vec!["public".to_string()]
    }
    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        (name == "public").then(|| self.schema.clone() as Arc<dyn SchemaProvider>)
    }
}

struct UkielSchemaProvider {
    catalog: PostgresCatalog,
    namespace: NamespaceId,
    /// Names snapshot taken at session build (used for sync `table_exist`).
    table_names: Vec<String>,
    store_url: ObjectStoreUrl,
}

#[async_trait]
impl SchemaProvider for UkielSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.table_names.clone()
    }

    async fn table(&self, name: &str) -> DfResult<Option<Arc<dyn TableProvider>>> {
        let logical = match self.catalog.get_logical_table(self.namespace, name).await {
            Ok(t) => t,
            Err(CatalogError::NotFound(_)) => return Ok(None),
            Err(e) => return Err(DataFusionError::External(Box::new(e))),
        };
        let hypertable = self
            .catalog
            .get_hypertable_by_id(logical.hypertable_id)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let provider = UkielTableProvider::try_new(
            self.catalog.clone(),
            hypertable,
            self.namespace.0, // v1 convention: table slice = packing_key == namespace id
            self.store_url.clone(),
        )
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(Some(Arc::new(provider)))
    }

    fn table_exist(&self, name: &str) -> bool {
        self.table_names.iter().any(|n| n == name)
    }
}
```

Add to `crates/ukiel-query/src/lib.rs`:

```rust
pub mod context;
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p ukiel-query`
Expected: all pass.

- [ ] **Step 5: Lint and commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add crates/ukiel-query
git commit -m "feat: per-namespace session context with catalog-backed table resolution"
```

---

### Task 4: Read-through object-store cache

**Files:**
- Create: `crates/ukiel-query/src/cache.rs`
- Modify: `crates/ukiel-query/src/lib.rs`
- Modify: `crates/ukiel-query/tests/query_test.rs`

**Interfaces:**
- Produces: `CachingObjectStore::new(inner: Arc<dyn ObjectStore>, dir: PathBuf) -> CachingObjectStore` implementing `ObjectStore`. Behavior: `get_range`/`get_ranges` (the calls the Parquet reader makes) download the whole object once into `dir` and serve byte ranges from the local file; all other methods delegate to `inner`. This is the "local NVMe" tier; the distributed tier stays a future second `inner`.

- [ ] **Step 1: Write the failing test**

Append to `crates/ukiel-query/tests/query_test.rs`:

```rust
use ukiel_query::cache::CachingObjectStore;

#[tokio::test]
async fn cache_serves_reads_after_inner_store_loses_the_object() {
    let h = setup().await;
    let cache_dir = tempfile::tempdir().unwrap();
    let cached: Arc<dyn ObjectStore> =
        Arc::new(CachingObjectStore::new(h.store.clone(), cache_dir.path().to_path_buf()));
    let store_url = url::Url::parse(STORE_URL).unwrap();

    let ctx = session_for_namespace(&h.catalog, NamespaceId(1), cached.clone(), &store_url)
        .await
        .unwrap();
    let batches =
        ctx.sql("SELECT payload FROM events ORDER BY ts").await.unwrap().collect().await.unwrap();
    assert_eq!(all_strings(&batches, "payload"), vec!["a1", "a2"]);

    // Files were cached locally.
    let cached_files = std::fs::read_dir(cache_dir.path()).unwrap().count();
    assert!(cached_files >= 1, "expected at least one cached file");

    // Delete everything from the inner store; reads must now come from cache.
    use futures::TryStreamExt;
    let objects: Vec<_> = h.store.list(None).try_collect().await.unwrap();
    for obj in objects {
        h.store.delete(&obj.location).await.unwrap();
    }

    let ctx = session_for_namespace(&h.catalog, NamespaceId(1), cached, &store_url).await.unwrap();
    let batches =
        ctx.sql("SELECT payload FROM events ORDER BY ts").await.unwrap().collect().await.unwrap();
    assert_eq!(all_strings(&batches, "payload"), vec!["a1", "a2"]);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p ukiel-query cache_serves`
Expected: compile error (`cache` module not found).

- [ ] **Step 3: Implement `crates/ukiel-query/src/cache.rs`**

```rust
//! Whole-file read-through cache: the local-disk tier of the spec's 2-layer
//! cache. `get_range`/`get_ranges` (what the Parquet reader uses) are served
//! from a local copy downloaded once per object; everything else delegates.

use std::fmt;
use std::ops::Range;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result,
};
use tokio::io::{AsyncReadExt, AsyncSeekExt};

#[derive(Debug)]
pub struct CachingObjectStore {
    inner: Arc<dyn ObjectStore>,
    dir: PathBuf,
}

impl CachingObjectStore {
    pub fn new(inner: Arc<dyn ObjectStore>, dir: PathBuf) -> Self {
        Self { inner, dir }
    }

    fn cache_path(&self, location: &Path) -> PathBuf {
        self.dir.join(location.as_ref().replace('/', "__"))
    }

    /// Downloads the whole object once; concurrent racers may download twice,
    /// the atomic rename makes that harmless.
    async fn ensure_cached(&self, location: &Path) -> Result<PathBuf> {
        let path = self.cache_path(location);
        if tokio::fs::try_exists(&path).await.unwrap_or(false) {
            return Ok(path);
        }
        let bytes = self.inner.get(location).await?.bytes().await?;
        let tmp = path.with_extension(format!("tmp-{}", uuid::Uuid::new_v4()));
        tokio::fs::write(&tmp, &bytes).await.map_err(io_err)?;
        tokio::fs::rename(&tmp, &path).await.map_err(io_err)?;
        Ok(path)
    }
}

fn io_err(e: std::io::Error) -> object_store::Error {
    object_store::Error::Generic { store: "ukiel-cache", source: Box::new(e) }
}

impl fmt::Display for CachingObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CachingObjectStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for CachingObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    async fn get_range(&self, location: &Path, range: Range<u64>) -> Result<Bytes> {
        let path = self.ensure_cached(location).await?;
        let mut file = tokio::fs::File::open(&path).await.map_err(io_err)?;
        file.seek(std::io::SeekFrom::Start(range.start)).await.map_err(io_err)?;
        let len = (range.end - range.start) as usize;
        let mut buf = vec![0u8; len];
        file.read_exact(&mut buf).await.map_err(io_err)?;
        Ok(Bytes::from(buf))
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        self.inner.delete(location).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}
```

(`get_ranges` keeps its default implementation, which loops over `get_range`. If the `ObjectStore` trait in object_store 0.13 requires additional methods, delegate them to `self.inner` the same way.)

Add to `crates/ukiel-query/src/lib.rs`:

```rust
pub mod cache;
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p ukiel-query`
Expected: all pass. If the cache test fails on the second query, the Parquet reader used a non-`get_range` read path — check which method it called (add `dbg!` temporarily in the delegating methods) and route that method through `ensure_cached` too.

- [ ] **Step 5: Lint and commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add crates/ukiel-query
git commit -m "feat: local-disk read-through object store cache"
```

---

### Task 5: HTTP SQL endpoint

**Files:**
- Create: `crates/ukiel-query/src/server.rs`
- Modify: `crates/ukiel-query/src/lib.rs`
- Create: `crates/ukiel-query/tests/server_test.rs`

**Interfaces:**
- Consumes: `session_for_namespace` (Task 3).
- Produces:
  - `AppState { catalog: PostgresCatalog, store: Arc<dyn ObjectStore>, store_url: url::Url }`
  - `router(state: AppState) -> axum::Router` with `POST /api/query` (`{"namespace_id": 1, "sql": "SELECT ..."}` → `{"rows": [...]}`, 400 on SQL/plan errors) and `GET /healthz` → `ok`. DDL/DML/statements rejected via `SQLOptions`.

- [ ] **Step 1: Write the failing test**

Create `crates/ukiel-query/tests/server_test.rs`:

```rust
mod query_test;

use query_test::{setup, STORE_URL};
use serde_json::json;
use ukiel_query::server::{router, AppState};

async fn spawn_server() -> (String, query_test::Harness) {
    let h = setup().await;
    let state = AppState {
        catalog: h.catalog.clone(),
        store: h.store.clone(),
        store_url: url::Url::parse(STORE_URL).unwrap(),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, router(state)).await.unwrap();
    });
    (format!("http://{addr}"), h)
}

#[tokio::test]
async fn http_query_returns_namespace_scoped_rows() {
    let (base, _h) = spawn_server().await;
    let client = reqwest::Client::new();

    let health = client.get(format!("{base}/healthz")).send().await.unwrap();
    assert_eq!(health.status(), 200);

    let resp = client
        .post(format!("{base}/api/query"))
        .json(&json!({"namespace_id": 2, "sql": "SELECT payload FROM events ORDER BY ts"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["rows"], json!([{"payload": "b1"}, {"payload": "b2"}]));

    // Bad SQL -> 400 with a message, not a 500.
    let resp = client
        .post(format!("{base}/api/query"))
        .json(&json!({"namespace_id": 1, "sql": "SELEKT nope"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);

    // DDL is rejected.
    let resp = client
        .post(format!("{base}/api/query"))
        .json(&json!({"namespace_id": 1, "sql": "CREATE TABLE hack (x INT)"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
}
```

Note: `mod query_test;` reuses the harness file as a module; mark the helper functions in `query_test.rs` `pub` (they already are) and ignore the duplicate-test-compilation — each integration-test binary compiles independently. If the compiler complains about unused items, add `#![allow(dead_code)]` at the top of `query_test.rs`.

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p ukiel-query --test server_test`
Expected: compile error (`server` module not found).

- [ ] **Step 3: Implement `crates/ukiel-query/src/server.rs`**

```rust
//! Tenant-facing SQL over HTTP. Isolation comes from the session build
//! (namespace-scoped providers), not from anything in the SQL text.

use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use datafusion::execution::context::SQLOptions;
use object_store::ObjectStore;
use ukiel_catalog::PostgresCatalog;
use ukiel_core::NamespaceId;

use crate::context::session_for_namespace;

#[derive(Clone)]
pub struct AppState {
    pub catalog: PostgresCatalog,
    pub store: Arc<dyn ObjectStore>,
    pub store_url: url::Url,
}

#[derive(serde::Deserialize)]
pub struct QueryRequest {
    pub namespace_id: i64,
    pub sql: String,
}

#[derive(serde::Serialize)]
pub struct QueryResponse {
    pub rows: serde_json::Value,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/api/query", post(run_query))
        .route("/healthz", get(|| async { "ok" }))
        .with_state(state)
}

fn bad_request<E: std::fmt::Display>(e: E) -> (StatusCode, String) {
    (StatusCode::BAD_REQUEST, e.to_string())
}

async fn run_query(
    State(state): State<AppState>,
    Json(req): Json<QueryRequest>,
) -> Result<Json<QueryResponse>, (StatusCode, String)> {
    let ctx = session_for_namespace(
        &state.catalog,
        NamespaceId(req.namespace_id),
        state.store.clone(),
        &state.store_url,
    )
    .await
    .map_err(bad_request)?;

    let options =
        SQLOptions::new().with_allow_ddl(false).with_allow_dml(false).with_allow_statements(false);
    let df = ctx.sql_with_options(&req.sql, options).await.map_err(bad_request)?;
    let batches = df.collect().await.map_err(bad_request)?;

    let mut writer = datafusion::arrow::json::ArrayWriter::new(Vec::new());
    for batch in &batches {
        writer.write(batch).map_err(bad_request)?;
    }
    writer.finish().map_err(bad_request)?;
    let rows: serde_json::Value =
        serde_json::from_slice(&writer.into_inner()).unwrap_or_else(|_| serde_json::json!([]));

    Ok(Json(QueryResponse { rows }))
}
```

Add to `crates/ukiel-query/src/lib.rs`:

```rust
pub mod server;
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p ukiel-query`
Expected: all pass (provider, context, cache, server).

- [ ] **Step 5: Lint and commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add crates/ukiel-query
git commit -m "feat: http sql endpoint with ddl/dml rejection"
```

---

### Task 6: Full-workspace verification + docs

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Run the full suite**

Run: `cargo fmt --check && cargo clippy --all-targets -- -D warnings && cargo test`
Expected: all crates green (Docker running).

- [ ] **Step 2: Update `README.md`**

In the `## Development` crate list, add after the `ukiel-ingest` bullet:

```markdown
- `crates/ukiel-query` — DataFusion query serving: catalog-planned Parquet
  scans (no object-store listings), namespace isolation injected into the
  physical plan, HTTP SQL endpoint (`POST /api/query`), local-disk
  read-through cache in front of the object store.
```

- [ ] **Step 3: Commit**

```bash
git add README.md
git commit -m "docs: query crate overview"
```

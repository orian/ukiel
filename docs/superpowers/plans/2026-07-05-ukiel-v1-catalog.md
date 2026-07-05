# Ukiel V1 Plan 1: Workspace + Core Types + Catalog Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rust workspace with `ukiel-core` (shared types) and `ukiel-catalog` (Postgres-backed catalog: hypertables, logical tables, parts, transactional commits with optimistic REPLACE, change feed).

**Architecture:** The catalog is the transactional heart of Ukiel (spec: `docs/superpowers/specs/2026-07-05-ukiel-design.md`). Parquet files ("parts") are immutable; the catalog tracks which parts are live. All writers (ingest, compaction, mutation workers) commit through one protocol: `ADD` inserts parts, `REPLACE` atomically tombstones old parts and inserts new ones, failing if any old part was already tombstoned (optimistic concurrency — loser retries). Every commit is an ordered change-feed event that downstream workers (MVs, compaction) consume via cursors.

**Tech Stack:** Rust 2024, sqlx 0.9 (Postgres, runtime queries — NOT the compile-time `query!` macros), tokio 1.52, testcontainers-modules 0.15 for integration tests.

## Global Constraints

- Rust edition 2024, toolchain ≥ 1.96.
- Use plain `sqlx::query` / `query_as` / `query_scalar` string queries. Do NOT use `sqlx::query!` macros (they need DATABASE_URL at compile time).
- Integration tests live in `crates/ukiel-catalog/tests/` and require Docker running. `cargo build` and `ukiel-core` unit tests must work without Docker.
- Commit messages: conventional style (`feat:`, `test:`, `chore:`). Never add Claude/AI attribution ("Co-Authored-By: Claude", "Generated with Claude Code") to commits.
- Run commands from the repo root `/Users/orian/workspace/datainq/ukiel`.
- Every task ends with `cargo fmt && cargo clippy --all-targets -- -D warnings` clean.

---

### Task 1: Cargo workspace scaffold

**Files:**
- Create: `Cargo.toml` (workspace root)
- Create: `.gitignore`
- Create: `crates/ukiel-core/Cargo.toml`
- Create: `crates/ukiel-core/src/lib.rs` (placeholder, filled in Task 2)
- Create: `crates/ukiel-catalog/Cargo.toml`
- Create: `crates/ukiel-catalog/src/lib.rs` (placeholder, filled in Task 3)

**Interfaces:**
- Produces: workspace with two member crates; all later tasks add code inside these crates and use `workspace = true` deps.

- [ ] **Step 1: Write root `Cargo.toml`**

```toml
[workspace]
resolver = "2"
members = ["crates/ukiel-core", "crates/ukiel-catalog"]

[workspace.package]
version = "0.1.0"
edition = "2024"

[workspace.dependencies]
tokio = { version = "1.52", features = ["macros", "rt-multi-thread"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
thiserror = "2"
chrono = { version = "0.4", features = ["serde"] }
sqlx = { version = "0.9", default-features = false, features = ["runtime-tokio", "tls-rustls", "postgres", "chrono", "json", "migrate", "macros"] }
testcontainers-modules = { version = "0.15", features = ["postgres"] }
ukiel-core = { path = "crates/ukiel-core" }
```

(The sqlx `macros` feature is for `#[derive(sqlx::FromRow)]` and `sqlx::migrate!`, not the `query!` macros.)

- [ ] **Step 2: Write `.gitignore`**

```gitignore
/target
.idea/
```

- [ ] **Step 3: Write `crates/ukiel-core/Cargo.toml`**

```toml
[package]
name = "ukiel-core"
version.workspace = true
edition.workspace = true

[dependencies]
serde = { workspace = true }
serde_json = { workspace = true }
```

- [ ] **Step 4: Write `crates/ukiel-core/src/lib.rs`**

```rust
//! Shared types for the Ukiel data lake.
```

- [ ] **Step 5: Write `crates/ukiel-catalog/Cargo.toml`**

```toml
[package]
name = "ukiel-catalog"
version.workspace = true
edition.workspace = true

[dependencies]
ukiel-core = { workspace = true }
serde_json = { workspace = true }
sqlx = { workspace = true }
thiserror = { workspace = true }
chrono = { workspace = true }

[dev-dependencies]
tokio = { workspace = true }
testcontainers-modules = { workspace = true }
```

- [ ] **Step 6: Write `crates/ukiel-catalog/src/lib.rs`**

```rust
//! Postgres-backed catalog: parts, commits, change feed.
```

- [ ] **Step 7: Verify it builds**

Run: `cargo build`
Expected: `Finished` with no errors (downloads deps on first run).

- [ ] **Step 8: Commit**

```bash
git add Cargo.toml Cargo.lock .gitignore crates/
git commit -m "chore: scaffold cargo workspace with ukiel-core and ukiel-catalog"
```

---

### Task 2: ukiel-core shared types

**Files:**
- Create: `crates/ukiel-core/src/ids.rs`
- Create: `crates/ukiel-core/src/part.rs`
- Create: `crates/ukiel-core/src/commit.rs`
- Create: `crates/ukiel-core/src/table.rs`
- Modify: `crates/ukiel-core/src/lib.rs`

**Interfaces:**
- Produces (used by every later crate):
  - `HypertableId(pub i64)`, `LogicalTableId(pub i64)`, `PartId(pub i64)`, `CommitId(pub i64)`, `NamespaceId(pub i64)`
  - `PartMeta { path: String, partition_values: serde_json::Value, packing_key_min: i64, packing_key_max: i64, row_count: i64, size_bytes: i64, level: i16, column_stats: Option<serde_json::Value> }`
  - `Part { id: PartId, hypertable_id: HypertableId, meta: PartMeta, created_by_commit: CommitId }`
  - `CommitOp::{Add { parts }, Replace { old, new }, Delete { parts }}`, `CommitOp::kind() -> &'static str`
  - `CommitResult::{Committed(CommitId), AlreadyApplied(CommitId)}`
  - `ChangeEvent { commit_id: CommitId, kind: String, added: Vec<Part>, removed: Vec<PartId> }`
  - `Hypertable { id, name, table_schema, partition_spec, sort_key, packing_key }`, `LogicalTable { id, namespace_id, name, hypertable_id, column_mapping }`

- [ ] **Step 1: Write the failing test**

Replace `crates/ukiel-core/src/lib.rs` entirely with the version below — it declares the modules and contains tests that exercise the API before it exists:

```rust
//! Shared types for the Ukiel data lake.

pub mod commit;
pub mod ids;
pub mod part;
pub mod table;

pub use commit::{ChangeEvent, CommitOp, CommitResult};
pub use ids::{CommitId, HypertableId, LogicalTableId, PartId, NamespaceId};
pub use part::{Part, PartMeta};
pub use table::{Hypertable, LogicalTable};

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_meta() -> PartMeta {
        PartMeta {
            path: "ht/1/part-0001.parquet".to_string(),
            partition_values: serde_json::json!({"day": "2026-07-05"}),
            packing_key_min: 10,
            packing_key_max: 20,
            row_count: 100,
            size_bytes: 4096,
            level: 0,
            column_stats: None,
        }
    }

    #[test]
    fn part_meta_serde_round_trip() {
        let meta = sample_meta();
        let json = serde_json::to_string(&meta).unwrap();
        let back: PartMeta = serde_json::from_str(&json).unwrap();
        assert_eq!(meta, back);
    }

    #[test]
    fn commit_op_kind_strings() {
        assert_eq!(CommitOp::Add { parts: vec![sample_meta()] }.kind(), "add");
        assert_eq!(
            CommitOp::Replace { old: vec![PartId(1)], new: vec![sample_meta()] }.kind(),
            "replace"
        );
        assert_eq!(CommitOp::Delete { parts: vec![PartId(1)] }.kind(), "delete");
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p ukiel-core`
Expected: compile error `unresolved module` / `cannot find` (modules don't exist yet).

- [ ] **Step 3: Write the modules**

`crates/ukiel-core/src/ids.rs`:

```rust
use serde::{Deserialize, Serialize};

macro_rules! id_type {
    ($name:ident) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(pub i64);

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", self.0)
            }
        }
    };
}

id_type!(HypertableId);
id_type!(LogicalTableId);
id_type!(PartId);
id_type!(CommitId);
id_type!(NamespaceId);
```

`crates/ukiel-core/src/part.rs`:

```rust
use serde::{Deserialize, Serialize};

use crate::ids::{CommitId, HypertableId, PartId};

/// Metadata describing one immutable Parquet file, before it has a catalog identity.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PartMeta {
    /// Object-store key of the Parquet file.
    pub path: String,
    /// Partition values, e.g. `{"key_bucket": 17, "day": "2026-07-05"}`.
    pub partition_values: serde_json::Value,
    /// Smallest packing-key value contained in the file (packed files span a key range).
    pub packing_key_min: i64,
    /// Largest packing-key value contained in the file.
    pub packing_key_max: i64,
    pub row_count: i64,
    pub size_bytes: i64,
    /// Compaction level: 0 = fresh ingest, higher = more compacted.
    pub level: i16,
    /// Optional per-column min/max stats for pruning.
    pub column_stats: Option<serde_json::Value>,
}

/// A part as registered in the catalog.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Part {
    pub id: PartId,
    pub hypertable_id: HypertableId,
    pub meta: PartMeta,
    pub created_by_commit: CommitId,
}
```

`crates/ukiel-core/src/commit.rs`:

```rust
use serde::{Deserialize, Serialize};

use crate::ids::{CommitId, PartId};
use crate::part::{Part, PartMeta};

/// An atomic catalog mutation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum CommitOp {
    /// Register new parts (ingest).
    Add { parts: Vec<PartMeta> },
    /// Atomically tombstone `old` and register `new` (compaction, mutation rewrite).
    /// Fails with a conflict if any `old` part is no longer live.
    Replace { old: Vec<PartId>, new: Vec<PartMeta> },
    /// Tombstone parts without replacement (retention/GDPR).
    Delete { parts: Vec<PartId> },
}

impl CommitOp {
    pub fn kind(&self) -> &'static str {
        match self {
            CommitOp::Add { .. } => "add",
            CommitOp::Replace { .. } => "replace",
            CommitOp::Delete { .. } => "delete",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitResult {
    /// The commit was applied by this call.
    Committed(CommitId),
    /// A commit with the same idempotency key already existed; nothing was changed.
    AlreadyApplied(CommitId),
}

impl CommitResult {
    pub fn commit_id(&self) -> CommitId {
        match self {
            CommitResult::Committed(id) | CommitResult::AlreadyApplied(id) => *id,
        }
    }
}

/// One change-feed entry: what a commit added and removed.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChangeEvent {
    pub commit_id: CommitId,
    /// "add" | "replace" | "delete"
    pub kind: String,
    pub added: Vec<Part>,
    pub removed: Vec<PartId>,
}
```

`crates/ukiel-core/src/table.rs`:

```rust
use serde::{Deserialize, Serialize};

use crate::ids::{HypertableId, LogicalTableId, NamespaceId};

/// A physical table family: many logical tables' data packed into shared
/// Parquet files, ordered by the packing key.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Hypertable {
    pub id: HypertableId,
    pub name: String,
    /// JSON-serialized column schema, e.g. `{"fields":[{"name":"ts","type":"timestamp_ms"}, ...]}`.
    pub table_schema: serde_json::Value,
    /// e.g. `{"columns": ["key_bucket", "day"]}`.
    pub partition_spec: serde_json::Value,
    /// File sort order, e.g. `["tenant_id", "ts"]`.
    pub sort_key: Vec<String>,
    /// The i64 column whose min/max every part records for range pruning
    /// (e.g. `"tenant_id"` in a multitenant deployment).
    pub packing_key: String,
}

/// A namespaced view over a slice of a hypertable. In a multitenant
/// deployment, namespace = tenant.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LogicalTable {
    pub id: LogicalTableId,
    pub namespace_id: NamespaceId,
    pub name: String,
    pub hypertable_id: HypertableId,
    /// Optional per-table column mapping; `None` = identity.
    pub column_mapping: Option<serde_json::Value>,
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p ukiel-core`
Expected: `test result: ok. 2 passed`

- [ ] **Step 5: Lint and commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add crates/ukiel-core
git commit -m "feat: ukiel-core shared types (ids, parts, commit ops, tables)"
```

---

### Task 3: Catalog crate skeleton, migrations, connect + migrate

**Files:**
- Create: `crates/ukiel-catalog/migrations/0001_init.sql`
- Create: `crates/ukiel-catalog/src/error.rs`
- Modify: `crates/ukiel-catalog/src/lib.rs`
- Create: `crates/ukiel-catalog/tests/common/mod.rs`
- Create: `crates/ukiel-catalog/tests/catalog_test.rs`

**Interfaces:**
- Consumes: `ukiel-core` types (Task 2).
- Produces:
  - `PostgresCatalog::connect(url: &str) -> Result<PostgresCatalog, CatalogError>` (Clone-able)
  - `PostgresCatalog::migrate(&self) -> Result<(), CatalogError>`
  - `CatalogError::{Conflict { expected: usize, live_matched: usize }, NotFound(String), Db(sqlx::Error)}`
  - Test helper `common::setup() -> (ContainerAsync<Postgres>, PostgresCatalog)` — keep the container binding alive for the test's duration or Postgres shuts down.

- [ ] **Step 1: Write the migration**

`crates/ukiel-catalog/migrations/0001_init.sql`:

```sql
CREATE TABLE hypertables (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    table_schema JSONB NOT NULL,
    partition_spec JSONB NOT NULL,
    sort_key TEXT[] NOT NULL,
    -- The i64 column whose min/max each part records for range pruning
    -- (e.g. 'tenant_id' in a multitenant deployment).
    packing_key TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE logical_tables (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    namespace_id BIGINT NOT NULL,
    name TEXT NOT NULL,
    hypertable_id BIGINT NOT NULL REFERENCES hypertables(id),
    column_mapping JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (namespace_id, name)
);

CREATE TABLE commits (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    hypertable_id BIGINT NOT NULL REFERENCES hypertables(id),
    kind TEXT NOT NULL CHECK (kind IN ('add', 'replace', 'delete')),
    idempotency_key TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Idempotency: at most one commit per (hypertable, key). NULL keys are exempt.
CREATE UNIQUE INDEX commits_idempotency_idx
    ON commits (hypertable_id, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

-- Change feed ordering.
CREATE INDEX commits_feed_idx ON commits (hypertable_id, id);

CREATE TABLE parts (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    hypertable_id BIGINT NOT NULL REFERENCES hypertables(id),
    path TEXT NOT NULL,
    partition_values JSONB NOT NULL,
    packing_key_min BIGINT NOT NULL,
    packing_key_max BIGINT NOT NULL,
    row_count BIGINT NOT NULL,
    size_bytes BIGINT NOT NULL,
    level SMALLINT NOT NULL DEFAULT 0,
    column_stats JSONB,
    created_by_commit BIGINT NOT NULL REFERENCES commits(id),
    deleted_by_commit BIGINT REFERENCES commits(id)
);

-- Query planning: live parts of a hypertable, pruned by packing-key range.
CREATE INDEX parts_live_idx ON parts (hypertable_id, packing_key_min, packing_key_max)
    WHERE deleted_by_commit IS NULL;

-- Change feed: parts added / removed by a commit.
CREATE INDEX parts_created_by_idx ON parts (created_by_commit);
CREATE INDEX parts_deleted_by_idx ON parts (deleted_by_commit) WHERE deleted_by_commit IS NOT NULL;
```

- [ ] **Step 2: Write `crates/ukiel-catalog/src/error.rs`**

```rust
#[derive(Debug, thiserror::Error)]
pub enum CatalogError {
    /// A REPLACE/DELETE lost the optimistic-concurrency race: some input
    /// parts were already tombstoned by another commit. Retry on fresh state.
    #[error("commit conflict: only {live_matched} of {expected} input parts were still live")]
    Conflict { expected: usize, live_matched: usize },

    #[error("not found: {0}")]
    NotFound(String),

    #[error(transparent)]
    Db(#[from] sqlx::Error),

    #[error("migration failed: {0}")]
    Migrate(#[from] sqlx::migrate::MigrateError),
}
```

- [ ] **Step 3: Write the failing test**

`crates/ukiel-catalog/tests/common/mod.rs`:

```rust
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::ContainerAsync;
use ukiel_catalog::PostgresCatalog;

/// Starts a throwaway Postgres and returns a migrated catalog.
/// Keep the returned container binding alive for the whole test.
pub async fn setup() -> (ContainerAsync<Postgres>, PostgresCatalog) {
    let container = Postgres::default().start().await.expect("start postgres container");
    let port = container.get_host_port_ipv4(5432).await.expect("mapped port");
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
    let catalog = PostgresCatalog::connect(&url).await.expect("connect");
    catalog.migrate().await.expect("migrate");
    (container, catalog)
}
```

`crates/ukiel-catalog/tests/catalog_test.rs`:

```rust
mod common;

#[tokio::test]
async fn connect_and_migrate() {
    let (_pg, _catalog) = common::setup().await;
    // setup() panics if connect or migrate fails; reaching here is the assertion.
}
```

- [ ] **Step 4: Run test to verify it fails**

Run: `cargo test -p ukiel-catalog`
Expected: compile error (`PostgresCatalog` not found).

- [ ] **Step 5: Write `crates/ukiel-catalog/src/lib.rs`**

```rust
//! Postgres-backed catalog: parts, commits, change feed.

mod error;

pub use error::CatalogError;

use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;

/// The catalog service handle. Cheap to clone (wraps a connection pool).
#[derive(Clone)]
pub struct PostgresCatalog {
    pool: PgPool,
}

impl PostgresCatalog {
    pub async fn connect(url: &str) -> Result<Self, CatalogError> {
        let pool = PgPoolOptions::new().max_connections(16).connect(url).await?;
        Ok(Self { pool })
    }

    pub async fn migrate(&self) -> Result<(), CatalogError> {
        sqlx::migrate!("./migrations").run(&self.pool).await?;
        Ok(())
    }
}
```

- [ ] **Step 6: Run test to verify it passes**

Run: `cargo test -p ukiel-catalog` (Docker must be running)
Expected: `connect_and_migrate ... ok`

- [ ] **Step 7: Lint and commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add crates/ukiel-catalog
git commit -m "feat: catalog crate with postgres schema and migrations"
```

---

### Task 4: Hypertable and logical-table CRUD

**Files:**
- Create: `crates/ukiel-catalog/src/tables.rs`
- Modify: `crates/ukiel-catalog/src/lib.rs` (add `mod tables;`)
- Modify: `crates/ukiel-catalog/tests/catalog_test.rs`

**Interfaces:**
- Produces (on `PostgresCatalog`):
  - `create_hypertable(&self, name: &str, table_schema: &serde_json::Value, partition_spec: &serde_json::Value, sort_key: &[String], packing_key: &str) -> Result<HypertableId, CatalogError>`
  - `get_hypertable(&self, name: &str) -> Result<Hypertable, CatalogError>` (NotFound if missing)
  - `create_logical_table(&self, namespace_id: NamespaceId, name: &str, hypertable_id: HypertableId) -> Result<LogicalTableId, CatalogError>`
  - `get_logical_table(&self, namespace_id: NamespaceId, name: &str) -> Result<LogicalTable, CatalogError>` (NotFound if missing)

- [ ] **Step 1: Write the failing tests**

Append to `crates/ukiel-catalog/tests/catalog_test.rs`:

```rust
use serde_json::json;
use ukiel_core::NamespaceId;

fn events_schema() -> serde_json::Value {
    json!({"fields": [
        {"name": "tenant_id", "type": "int64"},
        {"name": "ts", "type": "timestamp_ms"},
        {"name": "payload", "type": "utf8"}
    ]})
}

#[tokio::test]
async fn hypertable_create_and_get() {
    let (_pg, catalog) = common::setup().await;
    let id = catalog
        .create_hypertable(
            "events",
            &events_schema(),
            &json!({"columns": ["day"]}),
            &["tenant_id".to_string(), "ts".to_string()],
            "tenant_id",
        )
        .await
        .unwrap();

    let ht = catalog.get_hypertable("events").await.unwrap();
    assert_eq!(ht.id, id);
    assert_eq!(ht.name, "events");
    assert_eq!(ht.sort_key, vec!["tenant_id", "ts"]);
    assert_eq!(ht.packing_key, "tenant_id");
    assert_eq!(ht.table_schema, events_schema());

    let err = catalog.get_hypertable("missing").await.unwrap_err();
    assert!(matches!(err, ukiel_catalog::CatalogError::NotFound(_)));
}

#[tokio::test]
async fn logical_table_create_and_get() {
    let (_pg, catalog) = common::setup().await;
    let ht = catalog
        .create_hypertable("events", &events_schema(), &json!({"columns": []}), &["tenant_id".to_string()], "tenant_id")
        .await
        .unwrap();

    let lt_id = catalog
        .create_logical_table(NamespaceId(42), "my_events", ht)
        .await
        .unwrap();

    let lt = catalog.get_logical_table(NamespaceId(42), "my_events").await.unwrap();
    assert_eq!(lt.id, lt_id);
    assert_eq!(lt.namespace_id, NamespaceId(42));
    assert_eq!(lt.hypertable_id, ht);
    assert_eq!(lt.column_mapping, None);

    // Same table name in a different namespace is a different logical table.
    let err = catalog.get_logical_table(NamespaceId(7), "my_events").await.unwrap_err();
    assert!(matches!(err, ukiel_catalog::CatalogError::NotFound(_)));
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p ukiel-catalog`
Expected: compile error (`create_hypertable` not found).

- [ ] **Step 3: Implement `crates/ukiel-catalog/src/tables.rs`**

```rust
use sqlx::Row;
use ukiel_core::{Hypertable, HypertableId, LogicalTable, LogicalTableId, NamespaceId};

use crate::{CatalogError, PostgresCatalog};

impl PostgresCatalog {
    pub async fn create_hypertable(
        &self,
        name: &str,
        table_schema: &serde_json::Value,
        partition_spec: &serde_json::Value,
        sort_key: &[String],
        packing_key: &str,
    ) -> Result<HypertableId, CatalogError> {
        let id: i64 = sqlx::query_scalar(
            "INSERT INTO hypertables (name, table_schema, partition_spec, sort_key, packing_key)
             VALUES ($1, $2, $3, $4, $5) RETURNING id",
        )
        .bind(name)
        .bind(table_schema)
        .bind(partition_spec)
        .bind(sort_key)
        .bind(packing_key)
        .fetch_one(&self.pool)
        .await?;
        Ok(HypertableId(id))
    }

    pub async fn get_hypertable(&self, name: &str) -> Result<Hypertable, CatalogError> {
        let row = sqlx::query(
            "SELECT id, name, table_schema, partition_spec, sort_key, packing_key
             FROM hypertables WHERE name = $1",
        )
        .bind(name)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| CatalogError::NotFound(format!("hypertable '{name}'")))?;

        Ok(Hypertable {
            id: HypertableId(row.get("id")),
            name: row.get("name"),
            table_schema: row.get("table_schema"),
            partition_spec: row.get("partition_spec"),
            sort_key: row.get("sort_key"),
            packing_key: row.get("packing_key"),
        })
    }

    pub async fn create_logical_table(
        &self,
        namespace_id: NamespaceId,
        name: &str,
        hypertable_id: HypertableId,
    ) -> Result<LogicalTableId, CatalogError> {
        let id: i64 = sqlx::query_scalar(
            "INSERT INTO logical_tables (namespace_id, name, hypertable_id)
             VALUES ($1, $2, $3) RETURNING id",
        )
        .bind(namespace_id.0)
        .bind(name)
        .bind(hypertable_id.0)
        .fetch_one(&self.pool)
        .await?;
        Ok(LogicalTableId(id))
    }

    pub async fn get_logical_table(
        &self,
        namespace_id: NamespaceId,
        name: &str,
    ) -> Result<LogicalTable, CatalogError> {
        let row = sqlx::query(
            "SELECT id, namespace_id, name, hypertable_id, column_mapping
             FROM logical_tables WHERE namespace_id = $1 AND name = $2",
        )
        .bind(namespace_id.0)
        .bind(name)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| {
            CatalogError::NotFound(format!("logical table '{name}' in namespace {namespace_id}"))
        })?;

        Ok(LogicalTable {
            id: LogicalTableId(row.get("id")),
            namespace_id: NamespaceId(row.get("namespace_id")),
            name: row.get("name"),
            hypertable_id: HypertableId(row.get("hypertable_id")),
            column_mapping: row.get("column_mapping"),
        })
    }
}
```

Add to `crates/ukiel-catalog/src/lib.rs` after `mod error;`:

```rust
mod tables;
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p ukiel-catalog`
Expected: 3 tests pass (`connect_and_migrate`, `hypertable_create_and_get`, `logical_table_create_and_get`).

- [ ] **Step 5: Lint and commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add crates/ukiel-catalog
git commit -m "feat: hypertable and logical table CRUD"
```

---

### Task 5: Commit protocol — ADD + live_parts

**Files:**
- Create: `crates/ukiel-catalog/src/commit.rs`
- Create: `crates/ukiel-catalog/src/parts.rs`
- Modify: `crates/ukiel-catalog/src/lib.rs` (add `mod commit; mod parts;`)
- Modify: `crates/ukiel-catalog/tests/catalog_test.rs`

**Interfaces:**
- Produces (on `PostgresCatalog`):
  - `commit(&self, hypertable_id: HypertableId, op: CommitOp, idempotency_key: Option<&str>) -> Result<CommitResult, CatalogError>` (ADD path in this task; REPLACE/DELETE in Task 6; idempotency in Task 7)
  - `live_parts(&self, hypertable_id: HypertableId, key: Option<i64>) -> Result<Vec<Part>, CatalogError>` — live = `deleted_by_commit IS NULL`; key filter = `packing_key_min <= k AND packing_key_max >= k`; ordered by part id.

- [ ] **Step 1: Write the failing test**

Append to `crates/ukiel-catalog/tests/catalog_test.rs`:

```rust
use ukiel_core::{CommitOp, CommitResult, HypertableId, PartMeta};

fn part_meta(path: &str, packing_key_min: i64, packing_key_max: i64) -> PartMeta {
    PartMeta {
        path: path.to_string(),
        partition_values: json!({"day": "2026-07-05"}),
        packing_key_min,
        packing_key_max,
        row_count: 100,
        size_bytes: 4096,
        level: 0,
        column_stats: None,
    }
}

async fn setup_hypertable(catalog: &ukiel_catalog::PostgresCatalog) -> HypertableId {
    catalog
        .create_hypertable("events", &events_schema(), &json!({"columns": ["day"]}), &["tenant_id".to_string()], "tenant_id")
        .await
        .unwrap()
}

#[tokio::test]
async fn commit_add_makes_parts_live() {
    let (_pg, catalog) = common::setup().await;
    let ht = setup_hypertable(&catalog).await;

    let result = catalog
        .commit(
            ht,
            CommitOp::Add { parts: vec![part_meta("p1.parquet", 1, 50), part_meta("p2.parquet", 51, 99)] },
            None,
        )
        .await
        .unwrap();
    assert!(matches!(result, CommitResult::Committed(_)));

    let parts = catalog.live_parts(ht, None).await.unwrap();
    assert_eq!(parts.len(), 2);
    assert_eq!(parts[0].meta.path, "p1.parquet");
    assert_eq!(parts[0].created_by_commit, result.commit_id());

    // Packing-key pruning: key 60 only overlaps p2.
    let parts = catalog.live_parts(ht, Some(60)).await.unwrap();
    assert_eq!(parts.len(), 1);
    assert_eq!(parts[0].meta.path, "p2.parquet");
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p ukiel-catalog`
Expected: compile error (`commit` not found).

- [ ] **Step 3: Implement commit (ADD only) and live_parts**

`crates/ukiel-catalog/src/commit.rs`:

```rust
use sqlx::{Postgres, Transaction};
use ukiel_core::{CommitId, CommitOp, CommitResult, HypertableId, PartId, PartMeta};

use crate::{CatalogError, PostgresCatalog};

impl PostgresCatalog {
    /// Atomically apply `op`. REPLACE/DELETE fail with `CatalogError::Conflict`
    /// if any input part is no longer live (another commit tombstoned it first);
    /// the caller should re-read live parts and retry.
    pub async fn commit(
        &self,
        hypertable_id: HypertableId,
        op: CommitOp,
        idempotency_key: Option<&str>,
    ) -> Result<CommitResult, CatalogError> {
        let mut tx = self.pool.begin().await?;

        let inserted: Option<i64> = sqlx::query_scalar(
            "INSERT INTO commits (hypertable_id, kind, idempotency_key)
             VALUES ($1, $2, $3)
             ON CONFLICT (hypertable_id, idempotency_key) WHERE idempotency_key IS NOT NULL
             DO NOTHING
             RETURNING id",
        )
        .bind(hypertable_id.0)
        .bind(op.kind())
        .bind(idempotency_key)
        .fetch_optional(&mut *tx)
        .await?;

        let commit_id = match inserted {
            Some(id) => CommitId(id),
            None => {
                // Idempotency-key collision: the work was already done.
                let existing: i64 = sqlx::query_scalar(
                    "SELECT id FROM commits WHERE hypertable_id = $1 AND idempotency_key = $2",
                )
                .bind(hypertable_id.0)
                .bind(idempotency_key)
                .fetch_one(&mut *tx)
                .await?;
                return Ok(CommitResult::AlreadyApplied(CommitId(existing)));
            }
        };

        match op {
            CommitOp::Add { parts } => {
                insert_parts(&mut tx, hypertable_id, commit_id, &parts).await?;
            }
            CommitOp::Replace { old, new } => {
                tombstone_parts(&mut tx, hypertable_id, commit_id, &old).await?;
                insert_parts(&mut tx, hypertable_id, commit_id, &new).await?;
            }
            CommitOp::Delete { parts } => {
                tombstone_parts(&mut tx, hypertable_id, commit_id, &parts).await?;
            }
        }

        tx.commit().await?;
        Ok(CommitResult::Committed(commit_id))
    }
}

async fn insert_parts(
    tx: &mut Transaction<'_, Postgres>,
    hypertable_id: HypertableId,
    commit_id: CommitId,
    parts: &[PartMeta],
) -> Result<(), CatalogError> {
    for p in parts {
        sqlx::query(
            "INSERT INTO parts (hypertable_id, path, partition_values, packing_key_min, packing_key_max,
                                row_count, size_bytes, level, column_stats, created_by_commit)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
        )
        .bind(hypertable_id.0)
        .bind(&p.path)
        .bind(&p.partition_values)
        .bind(p.packing_key_min)
        .bind(p.packing_key_max)
        .bind(p.row_count)
        .bind(p.size_bytes)
        .bind(p.level)
        .bind(&p.column_stats)
        .bind(commit_id.0)
        .execute(&mut **tx)
        .await?;
    }
    Ok(())
}

/// Tombstones exactly the given live parts. If another committed transaction
/// already tombstoned any of them, fewer rows match and we return `Conflict`
/// (the transaction is rolled back by drop).
async fn tombstone_parts(
    tx: &mut Transaction<'_, Postgres>,
    hypertable_id: HypertableId,
    commit_id: CommitId,
    parts: &[PartId],
) -> Result<(), CatalogError> {
    let ids: Vec<i64> = parts.iter().map(|p| p.0).collect();
    let updated = sqlx::query(
        "UPDATE parts SET deleted_by_commit = $1
         WHERE id = ANY($2) AND hypertable_id = $3 AND deleted_by_commit IS NULL",
    )
    .bind(commit_id.0)
    .bind(&ids)
    .bind(hypertable_id.0)
    .execute(&mut **tx)
    .await?
    .rows_affected();

    if updated as usize != ids.len() {
        return Err(CatalogError::Conflict { expected: ids.len(), live_matched: updated as usize });
    }
    Ok(())
}
```

`crates/ukiel-catalog/src/parts.rs`:

```rust
use ukiel_core::{CommitId, HypertableId, Part, PartId, PartMeta, NamespaceId};

use crate::{CatalogError, PostgresCatalog};

#[derive(sqlx::FromRow)]
pub(crate) struct PartRow {
    pub id: i64,
    pub hypertable_id: i64,
    pub path: String,
    pub partition_values: serde_json::Value,
    pub packing_key_min: i64,
    pub packing_key_max: i64,
    pub row_count: i64,
    pub size_bytes: i64,
    pub level: i16,
    pub column_stats: Option<serde_json::Value>,
    pub created_by_commit: i64,
}

impl From<PartRow> for Part {
    fn from(r: PartRow) -> Self {
        Part {
            id: PartId(r.id),
            hypertable_id: HypertableId(r.hypertable_id),
            meta: PartMeta {
                path: r.path,
                partition_values: r.partition_values,
                packing_key_min: r.packing_key_min,
                packing_key_max: r.packing_key_max,
                row_count: r.row_count,
                size_bytes: r.size_bytes,
                level: r.level,
                column_stats: r.column_stats,
            },
            created_by_commit: CommitId(r.created_by_commit),
        }
    }
}

pub(crate) const PART_COLUMNS: &str = "id, hypertable_id, path, partition_values, packing_key_min, \
     packing_key_max, row_count, size_bytes, level, column_stats, created_by_commit";

impl PostgresCatalog {
    /// Live parts of a hypertable, optionally pruned to files whose
    /// packing-key range contains `key`.
    pub async fn live_parts(
        &self,
        hypertable_id: HypertableId,
        key: Option<i64>,
    ) -> Result<Vec<Part>, CatalogError> {
        let sql = format!(
            "SELECT {PART_COLUMNS} FROM parts
             WHERE hypertable_id = $1 AND deleted_by_commit IS NULL
               AND ($2::BIGINT IS NULL OR (packing_key_min <= $2 AND packing_key_max >= $2))
             ORDER BY id"
        );
        let rows: Vec<PartRow> = sqlx::query_as(&sql)
            .bind(hypertable_id.0)
            .bind(key)
            .fetch_all(&self.pool)
            .await?;
        Ok(rows.into_iter().map(Part::from).collect())
    }
}
```

Add to `crates/ukiel-catalog/src/lib.rs` after `mod tables;`:

```rust
mod commit;
mod parts;
```

(`pool` is a private field; `commit.rs`/`parts.rs` access it because they are modules of the same crate — keep the field `pub(crate)`:)

In `lib.rs`, change the struct definition to:

```rust
#[derive(Clone)]
pub struct PostgresCatalog {
    pub(crate) pool: PgPool,
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p ukiel-catalog`
Expected: 4 tests pass.

- [ ] **Step 5: Lint and commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add crates/ukiel-catalog
git commit -m "feat: commit protocol (ADD) and live_parts with packing-key pruning"
```

---

### Task 6: REPLACE + DELETE with optimistic conflict detection

**Files:**
- Modify: `crates/ukiel-catalog/tests/catalog_test.rs` (implementation already written in Task 5's `commit.rs`; this task proves it)

**Interfaces:**
- Consumes: `commit()` with `CommitOp::Replace` / `CommitOp::Delete` (Task 5).
- Produces: verified conflict semantics that ingest/compactor plans rely on: concurrent REPLACEs of the same part → exactly one `Committed`, the other `CatalogError::Conflict`; failed commits leave no trace (no new parts, no tombstones, no change-feed entry).

- [ ] **Step 1: Write the failing tests**

Append to `crates/ukiel-catalog/tests/catalog_test.rs`:

```rust
use ukiel_catalog::CatalogError;

#[tokio::test]
async fn replace_swaps_parts_atomically() {
    let (_pg, catalog) = common::setup().await;
    let ht = setup_hypertable(&catalog).await;

    catalog
        .commit(ht, CommitOp::Add { parts: vec![part_meta("l0-1.parquet", 1, 10), part_meta("l0-2.parquet", 1, 10)] }, None)
        .await
        .unwrap();
    let old_ids: Vec<_> = catalog.live_parts(ht, None).await.unwrap().iter().map(|p| p.id).collect();

    // Compaction: two L0 files -> one L1 file.
    let mut compacted = part_meta("l1-1.parquet", 1, 10);
    compacted.level = 1;
    catalog
        .commit(ht, CommitOp::Replace { old: old_ids.clone(), new: vec![compacted] }, None)
        .await
        .unwrap();

    let live = catalog.live_parts(ht, None).await.unwrap();
    assert_eq!(live.len(), 1);
    assert_eq!(live[0].meta.path, "l1-1.parquet");
    assert_eq!(live[0].meta.level, 1);

    // Replacing already-tombstoned parts conflicts.
    let err = catalog
        .commit(ht, CommitOp::Replace { old: old_ids, new: vec![part_meta("x.parquet", 1, 10)] }, None)
        .await
        .unwrap_err();
    assert!(matches!(err, CatalogError::Conflict { expected: 2, live_matched: 0 }));

    // The failed commit must not have added parts.
    assert_eq!(catalog.live_parts(ht, None).await.unwrap().len(), 1);
}

#[tokio::test]
async fn delete_tombstones_parts() {
    let (_pg, catalog) = common::setup().await;
    let ht = setup_hypertable(&catalog).await;

    catalog
        .commit(ht, CommitOp::Add { parts: vec![part_meta("p1.parquet", 1, 10)] }, None)
        .await
        .unwrap();
    let ids: Vec<_> = catalog.live_parts(ht, None).await.unwrap().iter().map(|p| p.id).collect();

    catalog.commit(ht, CommitOp::Delete { parts: ids }, None).await.unwrap();
    assert!(catalog.live_parts(ht, None).await.unwrap().is_empty());
}

#[tokio::test]
async fn concurrent_replace_exactly_one_wins() {
    let (_pg, catalog) = common::setup().await;
    let ht = setup_hypertable(&catalog).await;

    catalog
        .commit(ht, CommitOp::Add { parts: vec![part_meta("l0.parquet", 1, 10)] }, None)
        .await
        .unwrap();
    let old_ids: Vec<_> = catalog.live_parts(ht, None).await.unwrap().iter().map(|p| p.id).collect();

    let (c1, c2) = (catalog.clone(), catalog.clone());
    let (o1, o2) = (old_ids.clone(), old_ids.clone());
    let t1 = tokio::spawn(async move {
        c1.commit(ht, CommitOp::Replace { old: o1, new: vec![part_meta("winner-a.parquet", 1, 10)] }, None).await
    });
    let t2 = tokio::spawn(async move {
        c2.commit(ht, CommitOp::Replace { old: o2, new: vec![part_meta("winner-b.parquet", 1, 10)] }, None).await
    });

    let r1 = t1.await.unwrap();
    let r2 = t2.await.unwrap();

    let ok_count = [&r1, &r2].iter().filter(|r| r.is_ok()).count();
    let conflict_count = [&r1, &r2]
        .iter()
        .filter(|r| matches!(r, Err(CatalogError::Conflict { .. })))
        .count();
    assert_eq!(ok_count, 1, "exactly one replace must win, got {r1:?} / {r2:?}");
    assert_eq!(conflict_count, 1);

    // Exactly the winner's part is live.
    let live = catalog.live_parts(ht, None).await.unwrap();
    assert_eq!(live.len(), 1);
    assert!(live[0].meta.path.starts_with("winner-"));
}
```

- [ ] **Step 2: Run tests to verify they pass**

Run: `cargo test -p ukiel-catalog`
Expected: all 7 tests pass. The REPLACE/DELETE code paths were written in Task 5; these tests prove the conflict semantics. If `concurrent_replace_exactly_one_wins` hangs or both succeed, the `AND deleted_by_commit IS NULL` predicate in `tombstone_parts` is wrong — under READ COMMITTED the second transaction blocks on the row lock, then re-evaluates the predicate and matches 0 rows.

- [ ] **Step 3: Lint and commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add crates/ukiel-catalog
git commit -m "test: prove REPLACE/DELETE optimistic conflict semantics"
```

---

### Task 7: Idempotent commits

**Files:**
- Modify: `crates/ukiel-catalog/tests/catalog_test.rs` (implementation already in Task 5's `commit.rs`; this proves dedup)

**Interfaces:**
- Consumes: `commit(..., idempotency_key: Some(key))` from Task 5.
- Produces: verified dedup semantics the ingest plan relies on — key format there will be `"{kafka_partition}:{first_offset}-{last_offset}"`.

- [ ] **Step 1: Write the failing test**

Append to `crates/ukiel-catalog/tests/catalog_test.rs`:

```rust
#[tokio::test]
async fn idempotency_key_dedups_commit() {
    let (_pg, catalog) = common::setup().await;
    let ht = setup_hypertable(&catalog).await;

    let key = "0:0-99"; // kafka partition 0, offsets 0..=99
    let first = catalog
        .commit(ht, CommitOp::Add { parts: vec![part_meta("p1.parquet", 1, 10)] }, Some(key))
        .await
        .unwrap();
    let CommitResult::Committed(first_id) = first else {
        panic!("first commit must be Committed, got {first:?}");
    };

    // Ingest worker crashed after commit, re-flushed the same offset range.
    let second = catalog
        .commit(ht, CommitOp::Add { parts: vec![part_meta("p1-retry.parquet", 1, 10)] }, Some(key))
        .await
        .unwrap();
    assert_eq!(second, CommitResult::AlreadyApplied(first_id));

    // The retry's parts were NOT inserted.
    let live = catalog.live_parts(ht, None).await.unwrap();
    assert_eq!(live.len(), 1);
    assert_eq!(live[0].meta.path, "p1.parquet");

    // A different key commits normally.
    let third = catalog
        .commit(ht, CommitOp::Add { parts: vec![part_meta("p2.parquet", 1, 10)] }, Some("0:100-199"))
        .await
        .unwrap();
    assert!(matches!(third, CommitResult::Committed(_)));
    assert_eq!(catalog.live_parts(ht, None).await.unwrap().len(), 2);
}
```

- [ ] **Step 2: Run test to verify it passes**

Run: `cargo test -p ukiel-catalog idempotency_key_dedups_commit`
Expected: PASS (logic exists since Task 5). If it fails with a unique-violation error instead of `AlreadyApplied`, the `ON CONFLICT ... WHERE idempotency_key IS NOT NULL DO NOTHING` clause in `commit()` doesn't match the partial index — the `WHERE` in `ON CONFLICT` must match the index predicate exactly.

- [ ] **Step 3: Lint and commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add crates/ukiel-catalog
git commit -m "test: prove idempotent commit dedup"
```

---

### Task 8: Change feed

**Files:**
- Create: `crates/ukiel-catalog/src/feed.rs`
- Modify: `crates/ukiel-catalog/src/lib.rs` (add `mod feed;`)
- Modify: `crates/ukiel-catalog/tests/catalog_test.rs`

**Interfaces:**
- Consumes: `PartRow` + `PART_COLUMNS` from `parts.rs` (Task 5).
- Produces (on `PostgresCatalog`):
  - `changes_since(&self, hypertable_id: HypertableId, after: CommitId, limit: i64) -> Result<Vec<ChangeEvent>, CatalogError>` — commits with `id > after`, ascending, at most `limit`. Consumers persist the last `commit_id` they processed as their cursor; `CommitId(0)` means "from the beginning".

- [ ] **Step 1: Write the failing test**

Append to `crates/ukiel-catalog/tests/catalog_test.rs`:

```rust
use ukiel_core::CommitId;

#[tokio::test]
async fn change_feed_replays_commits_in_order() {
    let (_pg, catalog) = common::setup().await;
    let ht = setup_hypertable(&catalog).await;

    catalog
        .commit(ht, CommitOp::Add { parts: vec![part_meta("a.parquet", 1, 10)] }, None)
        .await
        .unwrap();
    let old_ids: Vec<_> = catalog.live_parts(ht, None).await.unwrap().iter().map(|p| p.id).collect();
    catalog
        .commit(ht, CommitOp::Replace { old: old_ids.clone(), new: vec![part_meta("b.parquet", 1, 10)] }, None)
        .await
        .unwrap();

    let events = catalog.changes_since(ht, CommitId(0), 100).await.unwrap();
    assert_eq!(events.len(), 2);

    assert_eq!(events[0].kind, "add");
    assert_eq!(events[0].added.len(), 1);
    assert_eq!(events[0].added[0].meta.path, "a.parquet");
    assert!(events[0].removed.is_empty());

    assert_eq!(events[1].kind, "replace");
    assert_eq!(events[1].added[0].meta.path, "b.parquet");
    assert_eq!(events[1].removed, old_ids);
    assert!(events[0].commit_id < events[1].commit_id);

    // Cursor semantics: resuming after event 0 yields only event 1.
    let tail = catalog.changes_since(ht, events[0].commit_id, 100).await.unwrap();
    assert_eq!(tail.len(), 1);
    assert_eq!(tail[0].commit_id, events[1].commit_id);

    // Limit is respected.
    let page = catalog.changes_since(ht, CommitId(0), 1).await.unwrap();
    assert_eq!(page.len(), 1);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p ukiel-catalog`
Expected: compile error (`changes_since` not found).

- [ ] **Step 3: Implement `crates/ukiel-catalog/src/feed.rs`**

```rust
use ukiel_core::{ChangeEvent, CommitId, HypertableId, Part, PartId};

use crate::parts::{PartRow, PART_COLUMNS};
use crate::{CatalogError, PostgresCatalog};

impl PostgresCatalog {
    /// Ordered change feed: commits with id > `after`, oldest first.
    /// Consumers persist the last processed commit id as their cursor.
    pub async fn changes_since(
        &self,
        hypertable_id: HypertableId,
        after: CommitId,
        limit: i64,
    ) -> Result<Vec<ChangeEvent>, CatalogError> {
        let commits: Vec<(i64, String)> = sqlx::query_as(
            "SELECT id, kind FROM commits
             WHERE hypertable_id = $1 AND id > $2
             ORDER BY id
             LIMIT $3",
        )
        .bind(hypertable_id.0)
        .bind(after.0)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        let mut events = Vec::with_capacity(commits.len());
        for (commit_id, kind) in commits {
            let added_sql = format!("SELECT {PART_COLUMNS} FROM parts WHERE created_by_commit = $1 ORDER BY id");
            let added: Vec<PartRow> = sqlx::query_as(&added_sql).bind(commit_id).fetch_all(&self.pool).await?;

            let removed: Vec<i64> = sqlx::query_scalar(
                "SELECT id FROM parts WHERE deleted_by_commit = $1 ORDER BY id",
            )
            .bind(commit_id)
            .fetch_all(&self.pool)
            .await?;

            events.push(ChangeEvent {
                commit_id: CommitId(commit_id),
                kind,
                added: added.into_iter().map(Part::from).collect(),
                removed: removed.into_iter().map(PartId).collect(),
            });
        }
        Ok(events)
    }
}
```

Add to `crates/ukiel-catalog/src/lib.rs` after `mod parts;`:

```rust
mod feed;
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p ukiel-catalog`
Expected: all 8 integration tests pass.

- [ ] **Step 5: Lint and commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add crates/ukiel-catalog
git commit -m "feat: change feed (changes_since with cursor semantics)"
```

---

### Task 9: Full-workspace verification + dev docs

**Files:**
- Modify: `README.md` (append development section)

**Interfaces:**
- Consumes: everything above.
- Produces: green workspace; README tells the next developer how to run tests.

- [ ] **Step 1: Run the full suite**

Run: `cargo fmt --check && cargo clippy --all-targets -- -D warnings && cargo test`
Expected: all crates pass (2 unit tests in ukiel-core, 8 integration tests in ukiel-catalog). Docker must be running for the catalog tests.

- [ ] **Step 2: Append to `README.md`**

Add at the end of the existing `README.md` (keep existing content):

```markdown
## Development

Rust workspace. Design spec: `docs/superpowers/specs/2026-07-05-ukiel-design.md`,
plans: `docs/superpowers/plans/`.

- `crates/ukiel-core` — shared types (ids, parts, commit ops).
- `crates/ukiel-catalog` — Postgres-backed catalog: transactional part commits
  (ADD / REPLACE / DELETE with optimistic concurrency), packing-key-pruned
  part listing, ordered change feed. The catalog is tenancy-agnostic:
  logical tables live in namespaces, parts cover packing-key ranges;
  multitenancy is a deployment mapping (namespace = tenant).

Tests: `cargo test` (integration tests spin up Postgres via testcontainers —
Docker must be running).
```

- [ ] **Step 3: Commit**

```bash
git add README.md
git commit -m "docs: development section for workspace and catalog"
```

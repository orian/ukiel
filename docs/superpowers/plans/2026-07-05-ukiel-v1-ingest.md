# Ukiel V1 Plan 2: Ingest (Kafka → Parquet L0 → Catalog) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `ukiel-ingest` crate: consume JSON records from Kafka, buffer per (hypertable, day), flush sorted ZSTD Parquet L0 files to an object store, and register them in the catalog with exactly-once semantics.

**Architecture:** Kafka is the source of truth (spec: `docs/superpowers/specs/2026-07-05-ukiel-design.md`). Exactly-once is achieved by storing Kafka offsets **in the catalog**, transactionally with the part commit: a new `ingest_offsets` table is updated in the same Postgres transaction that ADDs parts. On startup the worker seeks Kafka to the catalog's stored offsets (Kafka consumer-group offsets are never committed), so a crash between flush and anything else replays from exactly the right place and can never duplicate or lose rows. L0 files are always packed (many packing keys per file), sorted by (packing key, ts), so later compaction/deletion rewrites are cheap.

**Tech Stack:** rdkafka 0.39 (librdkafka via cmake), arrow/parquet 58.3 (the versions DataFusion 54 links against — do NOT bump), object_store 0.13, testcontainers (postgres + apache kafka) for integration tests.

**Prerequisite:** Plan 1 (`2026-07-05-ukiel-v1-catalog.md`) is implemented: the workspace exists with `ukiel-core` and `ukiel-catalog`, and all its tests pass.

## Global Constraints

- Rust edition 2024, toolchain ≥ 1.96.
- Pin arrow/parquet to `58.3` and object_store to `0.13` — these must match DataFusion 54 (plan 3). Never bump them independently.
- Use plain `sqlx::query`/`query_as`/`query_scalar` string queries. No `sqlx::query!` macros.
- `rdkafka` builds librdkafka from source: **cmake must be installed** (`brew install cmake` on macOS). Verify with `cmake --version` before starting.
- Integration tests need Docker running. Unit tests (schema mapping, parquet writer) must pass without Docker.
- Commit messages: conventional style, no Claude/AI attribution.
- Every task ends with `cargo fmt && cargo clippy --all-targets -- -D warnings` clean.
- External-crate APIs (rdkafka, arrow, object_store) are pinned but may differ in detail from the code below; if a compile error points at a changed signature, check the exact version on docs.rs and adapt minimally — do not redesign.

---

### Task 1: Workspace deps + arrow schema mapping in ukiel-core

**Files:**
- Modify: `Cargo.toml` (workspace root)
- Create: `crates/ukiel-core/src/schema.rs`
- Modify: `crates/ukiel-core/Cargo.toml`
- Modify: `crates/ukiel-core/src/lib.rs`

**Interfaces:**
- Consumes: `Hypertable.table_schema` JSON format from plan 1: `{"fields": [{"name": "...", "type": "..."}]}`.
- Produces: `ukiel_core::schema::arrow_schema_from_json(v: &serde_json::Value) -> Result<arrow::datatypes::Schema, SchemaError>` — used by ingest (plan 2) and query (plan 3). Supported type strings: `int64`, `float64`, `utf8`, `bool`, `timestamp_ms`. **v1 maps `timestamp_ms` to Arrow `Int64`** (epoch milliseconds); a real Timestamp type is a later refinement. Optional per-field `"nullable"` (default `true`).

- [ ] **Step 1: Add shared deps to the workspace root `Cargo.toml`**

Add to `[workspace.dependencies]` (keep existing entries), and change the existing `tokio` entry to `tokio = { version = "1.52", features = ["full"] }` (the consumer needs `time`, later crates need `fs`/`net`):

```toml
arrow = "58.3"
parquet = "58.3"
object_store = "0.13"
rdkafka = { version = "0.39", features = ["cmake-build", "tokio"] }
uuid = { version = "1", features = ["v4"] }
bytes = "1"
futures = "0.3"
tokio-util = "0.7"
ukiel-catalog = { path = "crates/ukiel-catalog" }
```

- [ ] **Step 2: Write the failing test**

Create `crates/ukiel-core/src/schema.rs`:

```rust
//! Mapping from the catalog's JSON table schema to an Arrow schema.
//!
//! Supported field types: `int64`, `float64`, `utf8`, `bool`, `timestamp_ms`.
//! v1 maps `timestamp_ms` to Arrow `Int64` (epoch milliseconds).

use arrow::datatypes::{DataType, Field, Schema};

#[derive(Debug, thiserror::Error)]
pub enum SchemaError {
    #[error("invalid table schema: {0}")]
    Invalid(String),
    #[error("unsupported field type '{0}'")]
    UnsupportedType(String),
}

pub fn arrow_schema_from_json(v: &serde_json::Value) -> Result<Schema, SchemaError> {
    todo!()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn maps_all_supported_types() {
        let schema = arrow_schema_from_json(&json!({"fields": [
            {"name": "tenant_id", "type": "int64"},
            {"name": "ts", "type": "timestamp_ms"},
            {"name": "value", "type": "float64"},
            {"name": "payload", "type": "utf8", "nullable": true},
            {"name": "flag", "type": "bool", "nullable": false}
        ]}))
        .unwrap();

        assert_eq!(schema.fields().len(), 5);
        assert_eq!(schema.field(0).data_type(), &DataType::Int64);
        assert_eq!(schema.field(1).data_type(), &DataType::Int64); // timestamp_ms -> Int64 in v1
        assert_eq!(schema.field(2).data_type(), &DataType::Float64);
        assert_eq!(schema.field(3).data_type(), &DataType::Utf8);
        assert_eq!(schema.field(4).data_type(), &DataType::Boolean);
        assert!(schema.field(0).is_nullable()); // default nullable
        assert!(!schema.field(4).is_nullable());
    }

    #[test]
    fn rejects_unknown_type_and_malformed_input() {
        let err = arrow_schema_from_json(&json!({"fields": [{"name": "x", "type": "decimal"}]}))
            .unwrap_err();
        assert!(matches!(err, SchemaError::UnsupportedType(_)));

        let err = arrow_schema_from_json(&json!({"no_fields": true})).unwrap_err();
        assert!(matches!(err, SchemaError::Invalid(_)));
    }
}
```

Add to `crates/ukiel-core/Cargo.toml` `[dependencies]`:

```toml
arrow = { workspace = true }
thiserror = { workspace = true }
```

Add to `crates/ukiel-core/src/lib.rs` (module list and re-export):

```rust
pub mod schema;
```

and to the `pub use` block:

```rust
pub use schema::{arrow_schema_from_json, SchemaError};
```

- [ ] **Step 3: Run test to verify it fails**

Run: `cargo test -p ukiel-core`
Expected: `maps_all_supported_types` panics at `todo!()`.

- [ ] **Step 4: Implement**

Replace the `todo!()` body of `arrow_schema_from_json`:

```rust
pub fn arrow_schema_from_json(v: &serde_json::Value) -> Result<Schema, SchemaError> {
    let fields = v
        .get("fields")
        .and_then(|f| f.as_array())
        .ok_or_else(|| SchemaError::Invalid("missing 'fields' array".to_string()))?;

    let mut arrow_fields = Vec::with_capacity(fields.len());
    for f in fields {
        let name = f
            .get("name")
            .and_then(|n| n.as_str())
            .ok_or_else(|| SchemaError::Invalid("field missing 'name'".to_string()))?;
        let type_name = f
            .get("type")
            .and_then(|t| t.as_str())
            .ok_or_else(|| SchemaError::Invalid(format!("field '{name}' missing 'type'")))?;
        let nullable = f.get("nullable").and_then(|n| n.as_bool()).unwrap_or(true);

        let data_type = match type_name {
            "int64" | "timestamp_ms" => DataType::Int64,
            "float64" => DataType::Float64,
            "utf8" => DataType::Utf8,
            "bool" => DataType::Boolean,
            other => return Err(SchemaError::UnsupportedType(other.to_string())),
        };
        arrow_fields.push(Field::new(name, data_type, nullable));
    }
    Ok(Schema::new(arrow_fields))
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test -p ukiel-core`
Expected: all ukiel-core tests pass (2 old + 2 new).

- [ ] **Step 6: Lint and commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add Cargo.toml Cargo.lock crates/ukiel-core
git commit -m "feat: arrow schema mapping from catalog table schema"
```

---

### Task 2: Catalog ingest offsets (exactly-once bookkeeping)

**Files:**
- Create: `crates/ukiel-catalog/migrations/0002_ingest_offsets.sql`
- Create: `crates/ukiel-catalog/src/offsets.rs`
- Modify: `crates/ukiel-catalog/src/commit.rs` (full replacement below)
- Modify: `crates/ukiel-catalog/src/lib.rs`
- Modify: `crates/ukiel-catalog/tests/catalog_test.rs`

**Interfaces:**
- Consumes: plan 1's `commit()` internals (`insert_parts`, `tombstone_parts`).
- Produces (on `PostgresCatalog`), used by the flusher in Task 4:
  - `OffsetRange { topic: String, partition: i32, first: i64, last: i64 }` (exported from `ukiel_catalog`)
  - `commit_with_offsets(&self, hypertable_id: HypertableId, op: CommitOp, offsets: &[OffsetRange]) -> Result<CommitResult, CatalogError>` — same semantics as `commit`, plus upserts `next_offset = last + 1` per (topic, partition) in the same transaction.
  - `ingest_offsets(&self, hypertable_id: HypertableId, topic: &str) -> Result<HashMap<i32, i64>, CatalogError>` — partition → next offset to consume (empty map when none stored).

- [ ] **Step 1: Write the migration**

`crates/ukiel-catalog/migrations/0002_ingest_offsets.sql`:

```sql
-- Kafka offsets stored transactionally with part commits: the source of truth
-- for where ingest resumes. Kafka consumer-group offsets are never used.
CREATE TABLE ingest_offsets (
    hypertable_id BIGINT NOT NULL REFERENCES hypertables(id),
    topic TEXT NOT NULL,
    kafka_partition INT NOT NULL,
    next_offset BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (hypertable_id, topic, kafka_partition)
);
```

- [ ] **Step 2: Write the failing test**

Append to `crates/ukiel-catalog/tests/catalog_test.rs`:

```rust
use ukiel_catalog::OffsetRange;

#[tokio::test]
async fn commit_with_offsets_is_atomic() {
    let (_pg, catalog) = common::setup().await;
    let ht = setup_hypertable(&catalog).await;

    // Fresh hypertable: no offsets stored.
    assert!(catalog.ingest_offsets(ht, "events").await.unwrap().is_empty());

    // Successful commit advances offsets.
    catalog
        .commit_with_offsets(
            ht,
            CommitOp::Add { parts: vec![part_meta("p1.parquet", 1, 10)] },
            &[
                OffsetRange { topic: "events".into(), partition: 0, first: 0, last: 41 },
                OffsetRange { topic: "events".into(), partition: 1, first: 0, last: 9 },
            ],
        )
        .await
        .unwrap();

    let offsets = catalog.ingest_offsets(ht, "events").await.unwrap();
    assert_eq!(offsets.get(&0), Some(&42));
    assert_eq!(offsets.get(&1), Some(&10));

    // A conflicting commit must NOT advance offsets (same transaction).
    let bogus_old = vec![ukiel_core::PartId(999_999)];
    let err = catalog
        .commit_with_offsets(
            ht,
            CommitOp::Replace { old: bogus_old, new: vec![part_meta("p2.parquet", 1, 10)] },
            &[OffsetRange { topic: "events".into(), partition: 0, first: 42, last: 99 }],
        )
        .await
        .unwrap_err();
    assert!(matches!(err, CatalogError::Conflict { .. }));

    let offsets = catalog.ingest_offsets(ht, "events").await.unwrap();
    assert_eq!(offsets.get(&0), Some(&42), "failed commit must not move offsets");

    // A later flush upserts over the existing row.
    catalog
        .commit_with_offsets(
            ht,
            CommitOp::Add { parts: vec![part_meta("p3.parquet", 1, 10)] },
            &[OffsetRange { topic: "events".into(), partition: 0, first: 42, last: 99 }],
        )
        .await
        .unwrap();
    let offsets = catalog.ingest_offsets(ht, "events").await.unwrap();
    assert_eq!(offsets.get(&0), Some(&100));
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `cargo test -p ukiel-catalog commit_with_offsets_is_atomic`
Expected: compile error (`OffsetRange` not found).

- [ ] **Step 4: Implement**

Create `crates/ukiel-catalog/src/offsets.rs`:

```rust
use std::collections::HashMap;

use ukiel_core::HypertableId;

use crate::{CatalogError, PostgresCatalog};

/// A consumed Kafka offset range, stored transactionally with the part commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetRange {
    pub topic: String,
    pub partition: i32,
    /// First consumed offset in this flush (inclusive).
    pub first: i64,
    /// Last consumed offset in this flush (inclusive).
    pub last: i64,
}

impl PostgresCatalog {
    /// Next offset to consume per Kafka partition. Empty when nothing stored.
    pub async fn ingest_offsets(
        &self,
        hypertable_id: HypertableId,
        topic: &str,
    ) -> Result<HashMap<i32, i64>, CatalogError> {
        let rows: Vec<(i32, i64)> = sqlx::query_as(
            "SELECT kafka_partition, next_offset FROM ingest_offsets
             WHERE hypertable_id = $1 AND topic = $2",
        )
        .bind(hypertable_id.0)
        .bind(topic)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().collect())
    }
}
```

Replace `crates/ukiel-catalog/src/commit.rs` entirely with:

```rust
use sqlx::{Postgres, Transaction};
use ukiel_core::{CommitId, CommitOp, CommitResult, HypertableId, PartId, PartMeta};

use crate::offsets::OffsetRange;
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
        self.commit_inner(hypertable_id, op, idempotency_key, &[]).await
    }

    /// Like `commit`, additionally advancing ingest offsets in the same
    /// transaction. Used by ingest workers for exactly-once delivery: on a
    /// failed commit, offsets do not move and the worker replays from the
    /// catalog's stored position.
    pub async fn commit_with_offsets(
        &self,
        hypertable_id: HypertableId,
        op: CommitOp,
        offsets: &[OffsetRange],
    ) -> Result<CommitResult, CatalogError> {
        self.commit_inner(hypertable_id, op, None, offsets).await
    }

    async fn commit_inner(
        &self,
        hypertable_id: HypertableId,
        op: CommitOp,
        idempotency_key: Option<&str>,
        offsets: &[OffsetRange],
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

        for range in offsets {
            sqlx::query(
                "INSERT INTO ingest_offsets (hypertable_id, topic, kafka_partition, next_offset)
                 VALUES ($1, $2, $3, $4)
                 ON CONFLICT (hypertable_id, topic, kafka_partition)
                 DO UPDATE SET next_offset = EXCLUDED.next_offset, updated_at = now()",
            )
            .bind(hypertable_id.0)
            .bind(&range.topic)
            .bind(range.partition)
            .bind(range.last + 1)
            .execute(&mut *tx)
            .await?;
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

Add to `crates/ukiel-catalog/src/lib.rs` after `mod feed;`:

```rust
mod offsets;
```

and next to `pub use error::CatalogError;`:

```rust
pub use offsets::OffsetRange;
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test -p ukiel-catalog`
Expected: all previous tests plus `commit_with_offsets_is_atomic` pass.

- [ ] **Step 6: Lint and commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add crates/ukiel-catalog
git commit -m "feat: transactional ingest offsets for exactly-once kafka ingest"
```

---

### Task 3: ukiel-ingest crate + Parquet writer

**Files:**
- Modify: `Cargo.toml` (workspace root: add member)
- Create: `crates/ukiel-ingest/Cargo.toml`
- Create: `crates/ukiel-ingest/src/lib.rs`
- Create: `crates/ukiel-ingest/src/error.rs`
- Create: `crates/ukiel-ingest/src/writer.rs`

**Interfaces:**
- Consumes: `ukiel_core::arrow_schema_from_json` (Task 1).
- Produces:
  - `EncodedPart { bytes: Vec<u8>, row_count: i64, key_min: i64, key_max: i64 }`
  - `writer::rows_to_parquet(schema: &arrow::datatypes::Schema, packing_key: &str, ts_column: &str, rows: Vec<serde_json::Value>) -> Result<EncodedPart, IngestError>` — sorts rows by (packing key, ts), encodes ZSTD Parquet. Also used by plan 3's tests to fabricate files.
  - `IngestError` enum.

- [ ] **Step 1: Scaffold the crate**

Add `"crates/ukiel-ingest"` to `members` in the root `Cargo.toml`, and add to `[workspace.dependencies]`:

```toml
ukiel-ingest = { path = "crates/ukiel-ingest" }
```

Create `crates/ukiel-ingest/Cargo.toml`:

```toml
[package]
name = "ukiel-ingest"
version.workspace = true
edition.workspace = true

[dependencies]
ukiel-core = { workspace = true }
ukiel-catalog = { workspace = true }
arrow = { workspace = true }
parquet = { workspace = true }
object_store = { workspace = true }
rdkafka = { workspace = true }
serde = { workspace = true }
serde_json = { workspace = true }
thiserror = { workspace = true }
chrono = { workspace = true }
tokio = { workspace = true }
tokio-util = { workspace = true }
uuid = { workspace = true }
bytes = { workspace = true }
futures = { workspace = true }

[dev-dependencies]
testcontainers-modules = { workspace = true, features = ["kafka"] }
```

(The `kafka` feature adds the Kafka container module on top of the `postgres` one already enabled in the workspace dependency.)

Create `crates/ukiel-ingest/src/error.rs`:

```rust
#[derive(Debug, thiserror::Error)]
pub enum IngestError {
    #[error(transparent)]
    Schema(#[from] ukiel_core::SchemaError),
    #[error(transparent)]
    Arrow(#[from] arrow::error::ArrowError),
    #[error(transparent)]
    Parquet(#[from] parquet::errors::ParquetError),
    #[error(transparent)]
    ObjectStore(#[from] object_store::Error),
    #[error(transparent)]
    Catalog(#[from] ukiel_catalog::CatalogError),
    #[error(transparent)]
    Kafka(#[from] rdkafka::error::KafkaError),
    #[error("row {row} is missing required i64 column '{column}'")]
    MissingColumn { row: usize, column: String },
    #[error("flush called with no rows")]
    EmptyFlush,
    #[error("config error: {0}")]
    Config(String),
}
```

Create `crates/ukiel-ingest/src/lib.rs`:

```rust
//! Kafka -> sorted Parquet L0 -> catalog commit.

mod error;
pub mod writer;

pub use error::IngestError;
pub use writer::{rows_to_parquet, EncodedPart};
```

- [ ] **Step 2: Write the failing test**

Create `crates/ukiel-ingest/src/writer.rs`:

```rust
//! Encodes buffered JSON rows into a sorted, ZSTD-compressed Parquet file.

use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::json::ReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;

use crate::IngestError;

/// One encoded L0 Parquet file, ready for the object store.
pub struct EncodedPart {
    pub bytes: Vec<u8>,
    pub row_count: i64,
    pub key_min: i64,
    pub key_max: i64,
}

/// Sorts `rows` by (packing key, ts) and encodes them as one Parquet file.
/// Every row must carry both columns as JSON integers.
pub fn rows_to_parquet(
    schema: &Schema,
    packing_key: &str,
    ts_column: &str,
    rows: Vec<serde_json::Value>,
) -> Result<EncodedPart, IngestError> {
    todo!()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int64Array, StringArray};
    use bytes::Bytes;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use serde_json::json;
    use ukiel_core::arrow_schema_from_json;

    fn test_schema() -> Schema {
        arrow_schema_from_json(&json!({"fields": [
            {"name": "tenant_id", "type": "int64"},
            {"name": "ts", "type": "timestamp_ms"},
            {"name": "payload", "type": "utf8"}
        ]}))
        .unwrap()
    }

    #[test]
    fn sorts_rows_and_reports_key_range() {
        let rows = vec![
            json!({"tenant_id": 7, "ts": 300, "payload": "c"}),
            json!({"tenant_id": 2, "ts": 200, "payload": "b"}),
            json!({"tenant_id": 7, "ts": 100, "payload": "a"}),
        ];
        let part = rows_to_parquet(&test_schema(), "tenant_id", "ts", rows).unwrap();
        assert_eq!(part.row_count, 3);
        assert_eq!(part.key_min, 2);
        assert_eq!(part.key_max, 7);

        // Read back and verify sort order (tenant_id asc, ts asc).
        let reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(part.bytes))
            .unwrap()
            .build()
            .unwrap();
        let batches: Vec<_> = reader.map(|b| b.unwrap()).collect();
        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        let tenants: &Int64Array = batch.column(0).as_any().downcast_ref().unwrap();
        let ts: &Int64Array = batch.column(1).as_any().downcast_ref().unwrap();
        let payloads: &StringArray = batch.column(2).as_any().downcast_ref().unwrap();
        assert_eq!(tenants.values().as_ref(), &[2, 7, 7]);
        assert_eq!(ts.values().as_ref(), &[200, 100, 300]);
        assert_eq!(payloads.value(0), "b");
        assert_eq!(payloads.value(1), "a");
        assert_eq!(payloads.value(2), "c");
    }

    #[test]
    fn rejects_missing_key_and_empty_input() {
        let err =
            rows_to_parquet(&test_schema(), "tenant_id", "ts", vec![json!({"ts": 1})]).unwrap_err();
        assert!(matches!(err, IngestError::MissingColumn { .. }));

        let err = rows_to_parquet(&test_schema(), "tenant_id", "ts", vec![]).unwrap_err();
        assert!(matches!(err, IngestError::EmptyFlush));
    }
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `cargo test -p ukiel-ingest`
Expected: panics at `todo!()`.

- [ ] **Step 4: Implement**

Replace the `todo!()` body of `rows_to_parquet`:

```rust
pub fn rows_to_parquet(
    schema: &Schema,
    packing_key: &str,
    ts_column: &str,
    mut rows: Vec<serde_json::Value>,
) -> Result<EncodedPart, IngestError> {
    if rows.is_empty() {
        return Err(IngestError::EmptyFlush);
    }

    // Validate up front so sorting can't panic on malformed rows.
    let get = |row: &serde_json::Value, col: &str| row.get(col).and_then(|v| v.as_i64());
    for (i, row) in rows.iter().enumerate() {
        for col in [packing_key, ts_column] {
            if get(row, col).is_none() {
                return Err(IngestError::MissingColumn { row: i, column: col.to_string() });
            }
        }
    }

    rows.sort_by_key(|r| (get(r, packing_key).unwrap(), get(r, ts_column).unwrap()));
    let key_min = get(&rows[0], packing_key).unwrap();
    let key_max = get(rows.last().unwrap(), packing_key).unwrap();

    let schema_ref = Arc::new(schema.clone());
    let mut decoder = ReaderBuilder::new(schema_ref.clone()).build_decoder()?;
    decoder.serialize(&rows)?;
    let batch = decoder.flush()?.ok_or(IngestError::EmptyFlush)?;

    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .build();
    let mut bytes = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut bytes, schema_ref, Some(props))?;
    writer.write(&batch)?;
    writer.close()?;

    Ok(EncodedPart { bytes, row_count: batch.num_rows() as i64, key_min, key_max })
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test -p ukiel-ingest`
Expected: 2 tests pass. (`cargo build` will compile librdkafka the first time — several minutes is normal.)

- [ ] **Step 6: Lint and commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add Cargo.toml Cargo.lock crates/ukiel-ingest
git commit -m "feat: ukiel-ingest crate with sorted zstd parquet writer"
```

---

### Task 4: Flusher — encoded parts → object store + catalog commit

**Files:**
- Create: `crates/ukiel-ingest/src/flusher.rs`
- Modify: `crates/ukiel-ingest/src/lib.rs`
- Create: `crates/ukiel-ingest/tests/flusher_test.rs`

**Interfaces:**
- Consumes: `EncodedPart` (Task 3), `commit_with_offsets`/`OffsetRange` (Task 2).
- Produces:
  - `FlushItem { partition_values: serde_json::Value, part: EncodedPart }`
  - `Flusher::new(catalog: PostgresCatalog, store: Arc<dyn ObjectStore>) -> Flusher`
  - `Flusher::flush(&self, hypertable: &Hypertable, items: Vec<FlushItem>, offsets: Vec<OffsetRange>) -> Result<CommitResult, IngestError>` — object paths follow `ht/{hypertable_id}/L0/{uuid}.parquet`.

- [ ] **Step 1: Write the failing test**

Create `crates/ukiel-ingest/tests/flusher_test.rs`:

```rust
use std::sync::Arc;

use futures::TryStreamExt;
use object_store::memory::InMemory;
use object_store::ObjectStore;
use serde_json::json;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use ukiel_catalog::{OffsetRange, PostgresCatalog};
use ukiel_core::arrow_schema_from_json;
use ukiel_ingest::flusher::{FlushItem, Flusher};
use ukiel_ingest::rows_to_parquet;

fn events_schema() -> serde_json::Value {
    json!({"fields": [
        {"name": "tenant_id", "type": "int64"},
        {"name": "ts", "type": "timestamp_ms"},
        {"name": "payload", "type": "utf8"}
    ]})
}

#[tokio::test]
async fn flush_writes_objects_and_commits_atomically() {
    let container = Postgres::default().start().await.expect("start postgres");
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
    let catalog = PostgresCatalog::connect(&url).await.unwrap();
    catalog.migrate().await.unwrap();

    let ht_id = catalog
        .create_hypertable("events", &events_schema(), &json!({"columns": ["day"]}), &["tenant_id".to_string(), "ts".to_string()], "tenant_id")
        .await
        .unwrap();
    let ht = catalog.get_hypertable("events").await.unwrap();

    let arrow_schema = arrow_schema_from_json(&ht.table_schema).unwrap();
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let flusher = Flusher::new(catalog.clone(), store.clone());

    let day1 = rows_to_parquet(
        &arrow_schema,
        "tenant_id",
        "ts",
        vec![
            json!({"tenant_id": 1, "ts": 1000, "payload": "a"}),
            json!({"tenant_id": 2, "ts": 2000, "payload": "b"}),
        ],
    )
    .unwrap();
    let day2 = rows_to_parquet(
        &arrow_schema,
        "tenant_id",
        "ts",
        vec![json!({"tenant_id": 1, "ts": 90_000_000, "payload": "c"})],
    )
    .unwrap();

    flusher
        .flush(
            &ht,
            vec![
                FlushItem { partition_values: json!({"day": "1970-01-01"}), part: day1 },
                FlushItem { partition_values: json!({"day": "1970-01-02"}), part: day2 },
            ],
            vec![OffsetRange { topic: "events".into(), partition: 0, first: 0, last: 2 }],
        )
        .await
        .unwrap();

    // Catalog sees both parts with correct metadata.
    let parts = catalog.live_parts(ht_id, None).await.unwrap();
    assert_eq!(parts.len(), 2);
    assert!(parts.iter().all(|p| p.meta.level == 0));
    assert!(parts.iter().all(|p| p.meta.path.starts_with(&format!("ht/{}/L0/", ht_id))));
    let total_rows: i64 = parts.iter().map(|p| p.meta.row_count).sum();
    assert_eq!(total_rows, 3);
    assert!(parts.iter().all(|p| p.meta.size_bytes > 0));

    // Offsets advanced with the same commit.
    let offsets = catalog.ingest_offsets(ht_id, "events").await.unwrap();
    assert_eq!(offsets.get(&0), Some(&3));

    // The objects actually exist in the store.
    let listed: Vec<_> = store.list(None).try_collect().await.unwrap();
    assert_eq!(listed.len(), 2);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p ukiel-ingest --test flusher_test`
Expected: compile error (`flusher` module not found).

- [ ] **Step 3: Implement `crates/ukiel-ingest/src/flusher.rs`**

```rust
//! Writes encoded parts to the object store and commits them (with offsets)
//! to the catalog in one transaction.

use std::sync::Arc;

use object_store::path::Path;
use object_store::ObjectStore;
use ukiel_catalog::{OffsetRange, PostgresCatalog};
use ukiel_core::{CommitOp, CommitResult, Hypertable, PartMeta};

use crate::writer::EncodedPart;
use crate::IngestError;

pub struct FlushItem {
    pub partition_values: serde_json::Value,
    pub part: EncodedPart,
}

pub struct Flusher {
    catalog: PostgresCatalog,
    store: Arc<dyn ObjectStore>,
}

impl Flusher {
    pub fn new(catalog: PostgresCatalog, store: Arc<dyn ObjectStore>) -> Self {
        Self { catalog, store }
    }

    /// Uploads every item, then commits all parts + offsets atomically.
    /// If the commit fails, uploaded objects are orphaned in the store —
    /// harmless (never referenced) and cleaned up by a later GC worker.
    pub async fn flush(
        &self,
        hypertable: &Hypertable,
        items: Vec<FlushItem>,
        offsets: Vec<OffsetRange>,
    ) -> Result<CommitResult, IngestError> {
        let mut parts = Vec::with_capacity(items.len());
        for item in items {
            let path_str =
                format!("ht/{}/L0/{}.parquet", hypertable.id, uuid::Uuid::new_v4());
            let size_bytes = item.part.bytes.len() as i64;
            self.store.put(&Path::from(path_str.clone()), item.part.bytes.into()).await?;
            parts.push(PartMeta {
                path: path_str,
                partition_values: item.partition_values,
                packing_key_min: item.part.key_min,
                packing_key_max: item.part.key_max,
                row_count: item.part.row_count,
                size_bytes,
                level: 0,
                column_stats: None,
            });
        }
        let result = self
            .catalog
            .commit_with_offsets(hypertable.id, CommitOp::Add { parts }, &offsets)
            .await?;
        Ok(result)
    }
}
```

Add to `crates/ukiel-ingest/src/lib.rs`:

```rust
pub mod flusher;
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p ukiel-ingest`
Expected: writer unit tests + `flush_writes_objects_and_commits_atomically` pass (Docker required).

- [ ] **Step 5: Lint and commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add crates/ukiel-ingest
git commit -m "feat: flusher uploads parts and commits with offsets"
```

---

### Task 5: Kafka consumer loop

**Files:**
- Create: `crates/ukiel-ingest/src/config.rs`
- Create: `crates/ukiel-ingest/src/consumer.rs`
- Modify: `crates/ukiel-ingest/src/lib.rs`
- Create: `crates/ukiel-ingest/tests/consumer_test.rs`

**Interfaces:**
- Consumes: `Flusher` (Task 4), `ingest_offsets` (Task 2), `rows_to_parquet` (Task 3).
- Produces:
  - `IngestConfig { brokers, group_id, flush_interval_ms, max_buffer_rows, tables: Vec<TableRoute> }`
  - `TableRoute { topic, hypertable, ts_column }`
  - `IngestWorker::new(catalog: PostgresCatalog, store: Arc<dyn ObjectStore>, config: IngestConfig) -> IngestWorker`
  - `IngestWorker::run(&self, shutdown: CancellationToken) -> Result<(), IngestError>` — consumes until cancelled, flushing on interval / row threshold / shutdown. One worker instance owns all partitions of its topics (v1; horizontal sharding of partitions across workers comes later).

- [ ] **Step 1: Write `crates/ukiel-ingest/src/config.rs`**

```rust
#[derive(Debug, Clone, serde::Deserialize)]
pub struct IngestConfig {
    pub brokers: String,
    pub group_id: String,
    /// Freshness knob: how often buffers are flushed to Parquet (spec: ~10s).
    pub flush_interval_ms: u64,
    /// Flush early when this many rows are buffered across all days.
    pub max_buffer_rows: usize,
    pub tables: Vec<TableRoute>,
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
```

- [ ] **Step 2: Write the failing test**

Create `crates/ukiel-ingest/tests/consumer_test.rs`:

```rust
use std::sync::Arc;
use std::time::Duration;

use object_store::memory::InMemory;
use object_store::ObjectStore;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde_json::json;
use testcontainers_modules::kafka::apache::{Kafka, KAFKA_PORT};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tokio_util::sync::CancellationToken;
use ukiel_catalog::PostgresCatalog;
use ukiel_core::HypertableId;
use ukiel_ingest::config::{IngestConfig, TableRoute};
use ukiel_ingest::consumer::IngestWorker;

const TOPIC: &str = "events";
const DAY1_TS: i64 = 1_000; // 1970-01-01
const DAY2_TS: i64 = 90_000_000; // 1970-01-02

async fn produce(producer: &FutureProducer, tenant: i64, ts: i64) {
    let payload = json!({"tenant_id": tenant, "ts": ts, "payload": "x"}).to_string();
    producer
        .send(
            FutureRecord::<(), _>::to(TOPIC).payload(&payload),
            Duration::from_secs(10),
        )
        .await
        .map_err(|(e, _)| e)
        .expect("produce");
}

async fn wait_for_parts(
    catalog: &PostgresCatalog,
    ht: HypertableId,
    min_parts: usize,
) -> usize {
    for _ in 0..120 {
        let n = catalog.live_parts(ht, None).await.unwrap().len();
        if n >= min_parts {
            return n;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    panic!("timed out waiting for {min_parts} parts");
}

#[tokio::test(flavor = "multi_thread")]
async fn consumes_flushes_and_resumes_exactly_once() {
    let pg = Postgres::default().start().await.expect("postgres");
    let pg_port = pg.get_host_port_ipv4(5432).await.unwrap();
    let kafka = Kafka::default().start().await.expect("kafka");
    let brokers = format!("127.0.0.1:{}", kafka.get_host_port_ipv4(KAFKA_PORT).await.unwrap());

    let catalog =
        PostgresCatalog::connect(&format!("postgres://postgres:postgres@127.0.0.1:{pg_port}/postgres"))
            .await
            .unwrap();
    catalog.migrate().await.unwrap();
    let ht_id = catalog
        .create_hypertable(
            "events",
            &json!({"fields": [
                {"name": "tenant_id", "type": "int64"},
                {"name": "ts", "type": "timestamp_ms"},
                {"name": "payload", "type": "utf8"}
            ]}),
            &json!({"columns": ["day"]}),
            &["tenant_id".to_string(), "ts".to_string()],
            "tenant_id",
        )
        .await
        .unwrap();

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("message.timeout.ms", "10000")
        .create()
        .unwrap();

    // Round 1: 6 messages over two days.
    for tenant in 1..=3 {
        produce(&producer, tenant, DAY1_TS).await;
        produce(&producer, tenant, DAY2_TS).await;
    }

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let config = IngestConfig {
        brokers: brokers.clone(),
        group_id: "ukiel-test".into(),
        flush_interval_ms: 500,
        max_buffer_rows: 10_000,
        tables: vec![TableRoute {
            topic: TOPIC.into(),
            hypertable: "events".into(),
            ts_column: "ts".into(),
        }],
    };

    let worker = IngestWorker::new(catalog.clone(), store.clone(), config.clone());
    let shutdown = CancellationToken::new();
    let handle = {
        let token = shutdown.clone();
        tokio::spawn(async move { worker.run(token).await })
    };

    // Two day-partitions -> at least 2 parts.
    wait_for_parts(&catalog, ht_id, 2).await;
    shutdown.cancel();
    handle.await.unwrap().unwrap();

    let parts = catalog.live_parts(ht_id, None).await.unwrap();
    let rows_round1: i64 = parts.iter().map(|p| p.meta.row_count).sum();
    assert_eq!(rows_round1, 6);
    let offsets = catalog.ingest_offsets(ht_id, TOPIC).await.unwrap();
    assert_eq!(offsets.values().sum::<i64>(), 6, "next_offset must equal produced count");

    // Round 2: restart from catalog offsets; must not re-ingest old rows.
    for tenant in 4..=6 {
        produce(&producer, tenant, DAY1_TS).await;
    }
    let worker = IngestWorker::new(catalog.clone(), store.clone(), config);
    let shutdown = CancellationToken::new();
    let handle = {
        let token = shutdown.clone();
        tokio::spawn(async move { worker.run(token).await })
    };
    wait_for_parts(&catalog, ht_id, 3).await;
    shutdown.cancel();
    handle.await.unwrap().unwrap();

    let parts = catalog.live_parts(ht_id, None).await.unwrap();
    let total_rows: i64 = parts.iter().map(|p| p.meta.row_count).sum();
    assert_eq!(total_rows, 9, "restart must not duplicate rows");
    let offsets = catalog.ingest_offsets(ht_id, TOPIC).await.unwrap();
    assert_eq!(offsets.values().sum::<i64>(), 9);
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `cargo test -p ukiel-ingest --test consumer_test`
Expected: compile error (`consumer` module not found).

- [ ] **Step 4: Implement `crates/ukiel-ingest/src/consumer.rs`**

```rust
//! The ingest worker: Kafka -> per-day buffers -> Flusher.
//!
//! Offsets live in the catalog (`ingest_offsets`), committed atomically with
//! parts. Kafka group offsets are never committed; on startup we assign all
//! partitions explicitly and seek to the catalog's stored positions.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use object_store::ObjectStore;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;
use ukiel_catalog::{OffsetRange, PostgresCatalog};
use ukiel_core::{arrow_schema_from_json, Hypertable};

use crate::config::{IngestConfig, TableRoute};
use crate::flusher::{FlushItem, Flusher};
use crate::writer::rows_to_parquet;
use crate::IngestError;

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
        Self { catalog, store, config }
    }

    /// Consumes all configured routes until `shutdown` is cancelled.
    /// Buffered rows are flushed one final time on shutdown.
    pub async fn run(&self, shutdown: CancellationToken) -> Result<(), IngestError> {
        let mut handles = Vec::new();
        for route in &self.config.tables {
            let task = RouteIngest {
                catalog: self.catalog.clone(),
                flusher: Flusher::new(self.catalog.clone(), self.store.clone()),
                config: self.config.clone(),
                route: route.clone(),
            };
            let token = shutdown.clone();
            handles.push(tokio::spawn(async move { task.run(token).await }));
        }
        for handle in handles {
            handle.await.expect("route task panicked")?;
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
                    self.flush(&hypertable, &mut buffers).await?;
                    return Ok(());
                }
                _ = ticker.tick() => {
                    self.flush(&hypertable, &mut buffers).await?;
                }
                msg = consumer.recv() => {
                    let msg = msg?;
                    self.buffer_message(&mut buffers, msg.payload(), msg.partition(), msg.offset());
                    if buffers.total_rows >= self.config.max_buffer_rows {
                        self.flush(&hypertable, &mut buffers).await?;
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

        let stored = self.catalog.ingest_offsets(hypertable.id, &self.route.topic).await?;
        let metadata =
            consumer.fetch_metadata(Some(&self.route.topic), Duration::from_secs(10))?;
        let topic_meta = metadata
            .topics()
            .iter()
            .find(|t| t.name() == self.route.topic)
            .ok_or_else(|| IngestError::Config(format!("topic '{}' not found", self.route.topic)))?;

        let mut tpl = TopicPartitionList::new();
        for p in topic_meta.partitions() {
            let next = stored.get(&p.id()).copied().unwrap_or(0);
            tpl.add_partition_offset(&self.route.topic, p.id(), Offset::Offset(next))?;
        }
        consumer.assign(&tpl)?;
        Ok(consumer)
    }

    fn buffer_message(
        &self,
        buffers: &mut Buffers,
        payload: Option<&[u8]>,
        partition: i32,
        offset: i64,
    ) {
        // Track the offset even for rows we skip: a poison message must not
        // stall the stream or be re-read forever.
        let range = buffers.ranges.entry(partition).or_insert((offset, offset));
        range.1 = offset;

        let Some(payload) = payload else { return };
        let Ok(row) = serde_json::from_slice::<serde_json::Value>(payload) else { return };
        let Some(ts) = row.get(&self.route.ts_column).and_then(|v| v.as_i64()) else { return };
        let day = chrono::DateTime::from_timestamp_millis(ts)
            .map(|d| d.date_naive().to_string())
            .unwrap_or_else(|| "unknown".to_string());

        buffers.rows_by_day.entry(day).or_default().push(row);
        buffers.total_rows += 1;
    }

    async fn flush(
        &self,
        hypertable: &Hypertable,
        buffers: &mut Buffers,
    ) -> Result<(), IngestError> {
        if buffers.ranges.is_empty() {
            return Ok(());
        }
        let schema = arrow_schema_from_json(&hypertable.table_schema)?;
        let mut items = Vec::new();
        for (day, rows) in buffers.rows_by_day.drain() {
            let part =
                rows_to_parquet(&schema, &hypertable.packing_key, &self.route.ts_column, rows)?;
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

        // Offset-only flush (all rows were skipped): commit offsets with no parts.
        self.flusher.flush(hypertable, items, offsets).await?;
        Ok(())
    }
}
```

Note: `Flusher::flush` with an empty `items` vec issues an ADD commit with zero parts, which still advances offsets — that is intentional (poison messages advance the stream).

Add to `crates/ukiel-ingest/src/lib.rs`:

```rust
pub mod config;
pub mod consumer;
```

- [ ] **Step 5: Run the integration test**

Run: `cargo test -p ukiel-ingest --test consumer_test -- --nocapture`
Expected: `consumes_flushes_and_resumes_exactly_once ... ok` (Kafka container startup can take ~30s; total test a couple of minutes).

- [ ] **Step 6: Run everything, lint, commit**

Run: `cargo test -p ukiel-ingest && cargo test -p ukiel-catalog`
Expected: all pass.

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add crates/ukiel-ingest
git commit -m "feat: kafka ingest worker with catalog-anchored exactly-once resume"
```

---

### Task 6: Full-workspace verification + docs

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Run the full suite**

Run: `cargo fmt --check && cargo clippy --all-targets -- -D warnings && cargo test`
Expected: all crates green (Docker running).

- [ ] **Step 2: Update `README.md`**

In the `## Development` section's crate list, add after the `ukiel-catalog` bullet:

```markdown
- `crates/ukiel-ingest` — Kafka → sorted ZSTD Parquet L0 files → catalog
  commits. Exactly-once: Kafka offsets are stored in the catalog in the same
  transaction as the part commit; workers resume from catalog offsets and
  never commit Kafka group offsets.
```

- [ ] **Step 3: Commit**

```bash
git add README.md
git commit -m "docs: ingest crate overview"
```

# Ukiel V1 Plan 4: Compactor (L0→L1, placement policy, key deletion) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `ukiel-compactor` crate: a change-feed-driven worker that merges small L0 Parquet files into sorted L1 files (respecting each hypertable's **placement policy**: packed vs separated-per-key), plus a **key deletion** worker that removes one packing key's data (metadata-only drops for dedicated files, rewrite for packed files) — both committing through REPLACE with optimistic-concurrency conflict handling.

**Architecture:** The compactor is a change-feed consumer (spec: `docs/superpowers/specs/2026-07-05-ukiel-design.md`, "Mutations & compaction" and "Placement policy" sections). It keeps a durable cursor per (worker, hypertable); feed events since the cursor are only a *wake-up signal* — the actual compaction plan is always computed from current `live_parts` state, which makes every run idempotent and crash-safe: a REPLACE that loses the optimistic race simply fails with `Conflict`, and the next run replans from fresh state. Deletion shares the same rewrite engine and commit path (testing design: scenarios S6/S7 in `2026-07-05-ukiel-testing-design.md` run against this crate).

**Tech Stack:** arrow/parquet 58.3 (concat + lexsort + filter kernels), object_store 0.13, sqlx 0.9, tokio.

**Prerequisites:** Plans 1–3 executed and green (`ukiel-core`, `ukiel-catalog`, `ukiel-ingest`, `ukiel-query`).

## Global Constraints

- Rust edition 2024, toolchain ≥ 1.96.
- arrow/parquet 58.3, object_store 0.13, datafusion 54 — pinned together, never bump one alone.
- Plain `sqlx::query`/`query_as`/`query_scalar` string queries, no `query!` macros.
- Component tests: testcontainers Postgres + `object_store::memory::InMemory`. Run with `make test` (caps `--test-threads=2` — unbounded container parallelism flakes). Pure rewrite-engine tests must pass without Docker.
- Commit messages: conventional style, no Claude/AI attribution.
- Every task ends with `cargo fmt && cargo clippy --all-targets -- -D warnings` clean.
- External-crate APIs are pinned but may differ in detail; on a signature mismatch check docs.rs for the exact version and adapt minimally — keep the plan's structure.

---

### Task 1: Catalog groundwork — placement policy, worker cursors, hypertable listing

**Files:**
- Create: `crates/ukiel-catalog/migrations/0003_placement_and_cursors.sql`
- Modify: `crates/ukiel-core/src/table.rs`
- Modify: `crates/ukiel-core/src/lib.rs`
- Modify: `crates/ukiel-catalog/src/tables.rs`
- Create: `crates/ukiel-catalog/src/cursors.rs`
- Modify: `crates/ukiel-catalog/src/lib.rs`
- Modify: `crates/ukiel-catalog/tests/catalog_test.rs`

**Interfaces:**
- Produces:
  - `ukiel_core::Placement::{Packed, Separated}` with `as_str() -> &'static str`; new field `Hypertable.placement: Placement` (DB default `packed`)
  - `PostgresCatalog::set_placement(&self, id: HypertableId, placement: Placement) -> Result<(), CatalogError>`
  - `PostgresCatalog::list_hypertables(&self) -> Result<Vec<Hypertable>, CatalogError>` (ordered by id)
  - `PostgresCatalog::get_cursor(&self, worker: &str, hypertable_id: HypertableId) -> Result<CommitId, CatalogError>` (`CommitId(0)` when absent)
  - `PostgresCatalog::set_cursor(&self, worker: &str, hypertable_id: HypertableId, cursor: CommitId) -> Result<(), CatalogError>` (upsert)

- [ ] **Step 1: Write the migration**

`crates/ukiel-catalog/migrations/0003_placement_and_cursors.sql`:

```sql
-- Placement policy (spec: "Placement policy: per-key deletion & retention").
-- 'packed': small keys share L1+ files, sorted by packing key.
-- 'separated': compaction splits every packing key into dedicated L1+ files,
-- making key deletion and retention metadata-only at rest.
ALTER TABLE hypertables
    ADD COLUMN placement TEXT NOT NULL DEFAULT 'packed'
    CHECK (placement IN ('packed', 'separated'));

-- Durable change-feed cursors for background workers (compactor, MV, ...).
CREATE TABLE worker_cursors (
    worker TEXT NOT NULL,
    hypertable_id BIGINT NOT NULL REFERENCES hypertables(id),
    last_commit_id BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (worker, hypertable_id)
);
```

- [ ] **Step 2: Write the failing test**

Append to `crates/ukiel-catalog/tests/catalog_test.rs`:

```rust
use ukiel_core::Placement;

#[tokio::test]
async fn placement_cursors_and_listing() {
    let (_pg, catalog) = common::setup().await;
    let ht = setup_hypertable(&catalog).await;

    // Placement defaults to packed and is settable.
    assert_eq!(catalog.get_hypertable("events").await.unwrap().placement, Placement::Packed);
    catalog.set_placement(ht, Placement::Separated).await.unwrap();
    assert_eq!(catalog.get_hypertable("events").await.unwrap().placement, Placement::Separated);

    // Listing returns every hypertable.
    let all = catalog.list_hypertables().await.unwrap();
    assert_eq!(all.len(), 1);
    assert_eq!(all[0].id, ht);

    // Cursors: 0 when absent, then upsert.
    assert_eq!(catalog.get_cursor("compactor", ht).await.unwrap(), CommitId(0));
    catalog.set_cursor("compactor", ht, CommitId(7)).await.unwrap();
    catalog.set_cursor("compactor", ht, CommitId(9)).await.unwrap();
    assert_eq!(catalog.get_cursor("compactor", ht).await.unwrap(), CommitId(9));
    // Cursors are per worker name.
    assert_eq!(catalog.get_cursor("mv", ht).await.unwrap(), CommitId(0));
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `cargo test -p ukiel-catalog placement_cursors -- --test-threads=2`
Expected: compile error (`Placement` not found).

- [ ] **Step 4: Implement**

In `crates/ukiel-core/src/table.rs`, add above `Hypertable`:

```rust
/// Where a hypertable's data lives at rest (L1+). L0 is always packed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Placement {
    /// Small keys share files, sorted by packing key (default).
    Packed,
    /// Compaction gives every packing key dedicated files; key deletion and
    /// retention become metadata-only at rest.
    Separated,
}

impl Placement {
    pub fn as_str(&self) -> &'static str {
        match self {
            Placement::Packed => "packed",
            Placement::Separated => "separated",
        }
    }
}
```

and add the field at the end of the `Hypertable` struct:

```rust
    /// Placement policy at rest (L1+); L0 is always packed.
    pub placement: Placement,
```

In `crates/ukiel-core/src/lib.rs`, extend the table re-export:

```rust
pub use table::{Hypertable, LogicalTable, Placement};
```

In `crates/ukiel-catalog/src/tables.rs`:
- Add `Placement` to the `ukiel_core` import list.
- In **both** `get_hypertable` and `get_hypertable_by_id`, add `placement` to the SELECT column list and this field to the `Hypertable` construction:

```rust
            placement: match row.get::<String, _>("placement").as_str() {
                "separated" => Placement::Separated,
                _ => Placement::Packed,
            },
```

- Append to the `impl PostgresCatalog` block:

```rust
    pub async fn set_placement(
        &self,
        id: HypertableId,
        placement: Placement,
    ) -> Result<(), CatalogError> {
        sqlx::query("UPDATE hypertables SET placement = $1 WHERE id = $2")
            .bind(placement.as_str())
            .bind(id.0)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn list_hypertables(&self) -> Result<Vec<Hypertable>, CatalogError> {
        let rows = sqlx::query(
            "SELECT id, name, table_schema, partition_spec, sort_key, packing_key, placement
             FROM hypertables ORDER BY id",
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| Hypertable {
                id: HypertableId(row.get("id")),
                name: row.get("name"),
                table_schema: row.get("table_schema"),
                partition_spec: row.get("partition_spec"),
                sort_key: row.get("sort_key"),
                packing_key: row.get("packing_key"),
                placement: match row.get::<String, _>("placement").as_str() {
                    "separated" => Placement::Separated,
                    _ => Placement::Packed,
                },
            })
            .collect())
    }
```

Create `crates/ukiel-catalog/src/cursors.rs`:

```rust
//! Durable change-feed cursors for background workers.

use ukiel_core::{CommitId, HypertableId};

use crate::{CatalogError, PostgresCatalog};

impl PostgresCatalog {
    /// The last commit id this worker processed for this hypertable
    /// (CommitId(0) = start of the feed).
    pub async fn get_cursor(
        &self,
        worker: &str,
        hypertable_id: HypertableId,
    ) -> Result<CommitId, CatalogError> {
        let id: Option<i64> = sqlx::query_scalar(
            "SELECT last_commit_id FROM worker_cursors WHERE worker = $1 AND hypertable_id = $2",
        )
        .bind(worker)
        .bind(hypertable_id.0)
        .fetch_optional(&self.pool)
        .await?;
        Ok(CommitId(id.unwrap_or(0)))
    }

    pub async fn set_cursor(
        &self,
        worker: &str,
        hypertable_id: HypertableId,
        cursor: CommitId,
    ) -> Result<(), CatalogError> {
        sqlx::query(
            "INSERT INTO worker_cursors (worker, hypertable_id, last_commit_id)
             VALUES ($1, $2, $3)
             ON CONFLICT (worker, hypertable_id)
             DO UPDATE SET last_commit_id = EXCLUDED.last_commit_id, updated_at = now()",
        )
        .bind(worker)
        .bind(hypertable_id.0)
        .bind(cursor.0)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}
```

Add to `crates/ukiel-catalog/src/lib.rs` after `mod offsets;`:

```rust
mod cursors;
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test -p ukiel-catalog -- --test-threads=2` and `cargo build --workspace`
Expected: all catalog tests pass; the whole workspace still compiles (the new `Hypertable.placement` field is only constructed inside ukiel-catalog).

- [ ] **Step 6: Lint and commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add crates/ukiel-core crates/ukiel-catalog
git commit -m "feat: placement policy, worker cursors, hypertable listing"
```

---

### Task 2: ukiel-compactor crate + rewrite engine

**Files:**
- Modify: `Cargo.toml` (workspace root: member + dep)
- Create: `crates/ukiel-compactor/Cargo.toml`
- Create: `crates/ukiel-compactor/src/lib.rs`
- Create: `crates/ukiel-compactor/src/error.rs`
- Create: `crates/ukiel-compactor/src/rewrite.rs`

**Interfaces:**
- Consumes: `Part` metadata (plan 1), Parquet files on an `ObjectStore`.
- Produces (module `rewrite`, shared by compaction and deletion):
  - `read_parts_to_batch(store, schema: SchemaRef, parts: &[Part]) -> Result<RecordBatch, CompactorError>` — downloads and concatenates all row batches of the given parts.
  - `sort_batch(batch, sort_key: &[String]) -> Result<RecordBatch, CompactorError>` — lexicographic sort by the named columns.
  - `split_by_key(batch, packing_key: &str) -> Result<Vec<(i64, RecordBatch)>, CompactorError>` — batch must already be sorted with the packing key as the primary sort column; returns one slice per distinct key.
  - `batch_to_parquet(batch) -> Result<Vec<u8>, CompactorError>` — ZSTD Parquet bytes.
  - `key_range(batch, packing_key: &str) -> Result<(i64, i64), CompactorError>` — min/max of the packing column.

- [ ] **Step 1: Scaffold**

Root `Cargo.toml`: add `"crates/ukiel-compactor"` to `members` and to `[workspace.dependencies]`:

```toml
ukiel-compactor = { path = "crates/ukiel-compactor" }
```

Create `crates/ukiel-compactor/Cargo.toml`:

```toml
[package]
name = "ukiel-compactor"
version.workspace = true
edition.workspace = true

[dependencies]
ukiel-core = { workspace = true }
ukiel-catalog = { workspace = true }
arrow = { workspace = true }
parquet = { workspace = true }
object_store = { workspace = true }
serde_json = { workspace = true }
thiserror = { workspace = true }
tokio = { workspace = true }
tokio-util = { workspace = true }
uuid = { workspace = true }
bytes = { workspace = true }
futures = { workspace = true }
tracing = "0.1"

[dev-dependencies]
ukiel-ingest = { workspace = true }
testcontainers-modules = { workspace = true }
```

(Add `tracing = "0.1"` to `[workspace.dependencies]` in the root and use `tracing = { workspace = true }` here instead if you prefer; either is acceptable.)

Create `crates/ukiel-compactor/src/error.rs`:

```rust
#[derive(Debug, thiserror::Error)]
pub enum CompactorError {
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
    #[error("column '{0}' not found in schema")]
    MissingColumn(String),
    #[error("column '{0}' is not Int64")]
    NotInt64(String),
}
```

Create `crates/ukiel-compactor/src/lib.rs`:

```rust
//! Background rewrite workers: L0→L1 compaction and key deletion.

mod error;
pub mod rewrite;

pub use error::CompactorError;
```

- [ ] **Step 2: Write the failing test**

Create `crates/ukiel-compactor/src/rewrite.rs` with the API stubbed and tests at the bottom:

```rust
//! The shared rewrite engine: read parts -> merge -> sort -> split -> encode.
//! Used by both compaction (merge small files) and deletion (rewrite without
//! one key). Pure functions over Arrow batches; no catalog access.

use std::sync::Arc;

use arrow::array::{Array, Int64Array, RecordBatch};
use arrow::compute::{concat_batches, lexsort_to_indices, take, SortColumn};
use arrow::datatypes::SchemaRef;
use object_store::path::Path;
use object_store::ObjectStore;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use ukiel_core::Part;

use crate::CompactorError;

/// Downloads every part and concatenates all row batches into one.
pub async fn read_parts_to_batch(
    store: &Arc<dyn ObjectStore>,
    schema: SchemaRef,
    parts: &[Part],
) -> Result<RecordBatch, CompactorError> {
    todo!()
}

/// Lexicographic sort by the named columns (in order).
pub fn sort_batch(batch: &RecordBatch, sort_key: &[String]) -> Result<RecordBatch, CompactorError> {
    todo!()
}

/// Splits a batch (already sorted with `packing_key` as primary sort column)
/// into one contiguous slice per distinct key value.
pub fn split_by_key(
    batch: &RecordBatch,
    packing_key: &str,
) -> Result<Vec<(i64, RecordBatch)>, CompactorError> {
    todo!()
}

/// Min/max of the packing column.
pub fn key_range(batch: &RecordBatch, packing_key: &str) -> Result<(i64, i64), CompactorError> {
    todo!()
}

/// ZSTD Parquet bytes for one batch.
pub fn batch_to_parquet(batch: &RecordBatch) -> Result<Vec<u8>, CompactorError> {
    todo!()
}

pub(crate) fn int64_column<'a>(
    batch: &'a RecordBatch,
    name: &str,
) -> Result<&'a Int64Array, CompactorError> {
    let idx = batch
        .schema()
        .index_of(name)
        .map_err(|_| CompactorError::MissingColumn(name.to_string()))?;
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| CompactorError::NotInt64(name.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use serde_json::json;
    use ukiel_core::{arrow_schema_from_json, CommitId, HypertableId, PartId, PartMeta};
    use ukiel_ingest::rows_to_parquet;

    fn schema_json() -> serde_json::Value {
        json!({"fields": [
            {"name": "tenant_id", "type": "int64"},
            {"name": "ts", "type": "timestamp_ms"},
            {"name": "payload", "type": "utf8"}
        ]})
    }

    fn part(path: &str, meta_src: &ukiel_ingest::EncodedPart) -> Part {
        Part {
            id: PartId(0),
            hypertable_id: HypertableId(1),
            meta: PartMeta {
                path: path.to_string(),
                partition_values: json!({}),
                packing_key_min: meta_src.key_min,
                packing_key_max: meta_src.key_max,
                row_count: meta_src.row_count,
                size_bytes: meta_src.bytes.len() as i64,
                level: 0,
                column_stats: None,
            },
            created_by_commit: CommitId(0),
        }
    }

    #[tokio::test]
    async fn merge_sort_split_round_trip() {
        let arrow_schema = Arc::new(arrow_schema_from_json(&schema_json()).unwrap());
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

        // Two unsorted-across-files L0 parts: keys interleaved.
        let f1 = rows_to_parquet(&arrow_schema, "tenant_id", "ts", vec![
            json!({"tenant_id": 2, "ts": 20, "payload": "b1"}),
            json!({"tenant_id": 7, "ts": 10, "payload": "c1"}),
        ]).unwrap();
        let f2 = rows_to_parquet(&arrow_schema, "tenant_id", "ts", vec![
            json!({"tenant_id": 1, "ts": 30, "payload": "a1"}),
            json!({"tenant_id": 7, "ts": 5,  "payload": "c0"}),
        ]).unwrap();
        store.put(&Path::from("p1"), f1.bytes.clone().into()).await.unwrap();
        store.put(&Path::from("p2"), f2.bytes.clone().into()).await.unwrap();
        let parts = vec![part("p1", &f1), part("p2", &f2)];

        let merged = read_parts_to_batch(&store, arrow_schema.clone(), &parts).await.unwrap();
        assert_eq!(merged.num_rows(), 4);

        let sorted = sort_batch(&merged, &["tenant_id".to_string(), "ts".to_string()]).unwrap();
        let keys = int64_column(&sorted, "tenant_id").unwrap();
        assert_eq!(keys.values().as_ref(), &[1, 2, 7, 7]);
        let ts = int64_column(&sorted, "ts").unwrap();
        assert_eq!(ts.values().as_ref(), &[30, 20, 5, 10]); // ts sorted within key 7

        assert_eq!(key_range(&sorted, "tenant_id").unwrap(), (1, 7));

        let splits = split_by_key(&sorted, "tenant_id").unwrap();
        let split_keys: Vec<i64> = splits.iter().map(|(k, _)| *k).collect();
        assert_eq!(split_keys, vec![1, 2, 7]);
        assert_eq!(splits[2].1.num_rows(), 2);

        // Encode a slice and read it back.
        let bytes = batch_to_parquet(&splits[2].1).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes))
            .unwrap()
            .build()
            .unwrap();
        let rows: usize = reader.map(|b| b.unwrap().num_rows()).sum();
        assert_eq!(rows, 2);
    }
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `cargo test -p ukiel-compactor`
Expected: panics at `todo!()`.

- [ ] **Step 4: Implement the five functions**

Replace the `todo!()` bodies in `rewrite.rs`:

```rust
pub async fn read_parts_to_batch(
    store: &Arc<dyn ObjectStore>,
    schema: SchemaRef,
    parts: &[Part],
) -> Result<RecordBatch, CompactorError> {
    let mut batches = Vec::new();
    for part in parts {
        let bytes = store.get(&Path::from(part.meta.path.clone())).await?.bytes().await?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)?.build()?;
        for batch in reader {
            batches.push(batch?);
        }
    }
    Ok(concat_batches(&schema, &batches)?)
}

pub fn sort_batch(batch: &RecordBatch, sort_key: &[String]) -> Result<RecordBatch, CompactorError> {
    let mut columns = Vec::with_capacity(sort_key.len());
    for name in sort_key {
        let idx = batch
            .schema()
            .index_of(name)
            .map_err(|_| CompactorError::MissingColumn(name.clone()))?;
        columns.push(SortColumn { values: batch.column(idx).clone(), options: None });
    }
    let indices = lexsort_to_indices(&columns, None)?;
    let sorted = batch
        .columns()
        .iter()
        .map(|c| take(c.as_ref(), &indices, None))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(RecordBatch::try_new(batch.schema(), sorted)?)
}

pub fn split_by_key(
    batch: &RecordBatch,
    packing_key: &str,
) -> Result<Vec<(i64, RecordBatch)>, CompactorError> {
    let keys = int64_column(batch, packing_key)?;
    let mut out = Vec::new();
    let mut start = 0usize;
    for i in 1..=keys.len() {
        if i == keys.len() || keys.value(i) != keys.value(start) {
            out.push((keys.value(start), batch.slice(start, i - start)));
            start = i;
        }
    }
    Ok(out)
}

pub fn key_range(batch: &RecordBatch, packing_key: &str) -> Result<(i64, i64), CompactorError> {
    let keys = int64_column(batch, packing_key)?;
    let min = arrow::compute::min(keys)
        .ok_or_else(|| CompactorError::MissingColumn(packing_key.to_string()))?;
    let max = arrow::compute::max(keys)
        .ok_or_else(|| CompactorError::MissingColumn(packing_key.to_string()))?;
    Ok((min, max))
}

pub fn batch_to_parquet(batch: &RecordBatch) -> Result<Vec<u8>, CompactorError> {
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .build();
    let mut bytes = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut bytes, batch.schema(), Some(props))?;
    writer.write(batch)?;
    writer.close()?;
    Ok(bytes)
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test -p ukiel-compactor`
Expected: `merge_sort_split_round_trip ... ok` (no Docker needed).

- [ ] **Step 6: Lint and commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add Cargo.toml Cargo.lock crates/ukiel-compactor
git commit -m "feat: ukiel-compactor crate with shared rewrite engine"
```

---

### Task 3: Compaction — plan from live state, honor placement, REPLACE commit

**Files:**
- Create: `crates/ukiel-compactor/src/compactor.rs`
- Modify: `crates/ukiel-compactor/src/lib.rs`
- Create: `crates/ukiel-compactor/tests/compactor_test.rs`

**Interfaces:**
- Consumes: rewrite engine (Task 2), `live_parts`/`commit`/`list_hypertables`/cursors (plan 1 + Task 1).
- Produces:
  - `CompactorConfig { worker_name: String, min_l0_files: usize, poll_interval_ms: u64 }` with `Default` (`"compactor"`, 2, 5000)
  - `Compactor::new(catalog: PostgresCatalog, store: Arc<dyn ObjectStore>, config: CompactorConfig) -> Compactor`
  - `Compactor::run_once(&self) -> Result<CompactionStats, CompactorError>` — one idempotent pass over all hypertables
  - `CompactionStats { merged_groups: usize, parts_in: usize, parts_out: usize, conflicts: usize }`
  - Output paths: `ht/{id}/L1/{uuid}.parquet`; output parts get `level = 1`, the group's `partition_values`, recomputed key min/max and row counts.

- [ ] **Step 1: Write the failing test**

Create `crates/ukiel-compactor/tests/compactor_test.rs`:

```rust
use std::collections::HashSet;
use std::sync::Arc;

use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::ObjectStore;
use serde_json::json;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::ContainerAsync;
use ukiel_catalog::PostgresCatalog;
use ukiel_compactor::compactor::{Compactor, CompactorConfig};
use ukiel_compactor::rewrite;
use ukiel_core::{arrow_schema_from_json, CommitOp, HypertableId, PartMeta, Placement};
use ukiel_ingest::rows_to_parquet;

fn events_schema() -> serde_json::Value {
    json!({"fields": [
        {"name": "tenant_id", "type": "int64"},
        {"name": "ts", "type": "timestamp_ms"},
        {"name": "payload", "type": "utf8"}
    ]})
}

pub struct Env {
    pub _pg: ContainerAsync<Postgres>,
    pub catalog: PostgresCatalog,
    pub store: Arc<dyn ObjectStore>,
    pub ht: HypertableId,
}

pub async fn setup() -> Env {
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
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    Env { _pg: pg, catalog, store, ht }
}

/// Writes one L0 part with the given rows and commits it.
pub async fn add_l0(env: &Env, day: &str, rows: Vec<serde_json::Value>) {
    let schema = arrow_schema_from_json(&events_schema()).unwrap();
    let encoded = rows_to_parquet(&schema, "tenant_id", "ts", rows).unwrap();
    let path = format!("ht/{}/L0/{}.parquet", env.ht, uuid::Uuid::new_v4());
    let size = encoded.bytes.len() as i64;
    env.store.put(&Path::from(path.clone()), encoded.bytes.into()).await.unwrap();
    env.catalog
        .commit(
            env.ht,
            CommitOp::Add {
                parts: vec![PartMeta {
                    path,
                    partition_values: json!({ "day": day }),
                    packing_key_min: encoded.key_min,
                    packing_key_max: encoded.key_max,
                    row_count: encoded.row_count,
                    size_bytes: size,
                    level: 0,
                    column_stats: None,
                }],
            },
            None,
        )
        .await
        .unwrap();
}

/// All payloads currently readable from live parts (the ground truth).
pub async fn live_payloads(env: &Env) -> HashSet<String> {
    let schema = Arc::new(arrow_schema_from_json(&events_schema()).unwrap());
    let parts = env.catalog.live_parts(env.ht, None).await.unwrap();
    let mut out = HashSet::new();
    for part in &parts {
        let batch =
            rewrite::read_parts_to_batch(&env.store, schema.clone(), std::slice::from_ref(part))
                .await
                .unwrap();
        let idx = batch.schema().index_of("payload").unwrap();
        let arr = batch
            .column(idx)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        for i in 0..arr.len() {
            out.insert(arr.value(i).to_string());
        }
    }
    out
}

#[tokio::test]
async fn packed_compaction_merges_within_partition() {
    let env = setup().await;
    // Three L0 files on day1, one on day2.
    add_l0(&env, "d1", vec![json!({"tenant_id": 2, "ts": 1, "payload": "p1"})]).await;
    add_l0(&env, "d1", vec![json!({"tenant_id": 1, "ts": 2, "payload": "p2"})]).await;
    add_l0(&env, "d1", vec![json!({"tenant_id": 2, "ts": 3, "payload": "p3"})]).await;
    add_l0(&env, "d2", vec![json!({"tenant_id": 9, "ts": 4, "payload": "p4"})]).await;
    let before = live_payloads(&env).await;

    let compactor =
        Compactor::new(env.catalog.clone(), env.store.clone(), CompactorConfig::default());
    let stats = compactor.run_once().await.unwrap();
    assert_eq!(stats.merged_groups, 1, "only day1 has >= 2 L0 files");
    assert_eq!(stats.parts_in, 3);
    assert_eq!(stats.parts_out, 1);
    assert_eq!(stats.conflicts, 0);

    // Data equivalence (invariant I4) and layout expectations.
    assert_eq!(live_payloads(&env).await, before);
    let parts = env.catalog.live_parts(env.ht, None).await.unwrap();
    assert_eq!(parts.len(), 2); // 1 merged L1 (day1) + 1 untouched L0 (day2)
    let l1 = parts.iter().find(|p| p.meta.level == 1).unwrap();
    assert_eq!(l1.meta.row_count, 3);
    assert_eq!((l1.meta.packing_key_min, l1.meta.packing_key_max), (1, 2));
    assert_eq!(l1.meta.partition_values, json!({"day": "d1"}));

    // Idempotence / convergence: a second pass finds nothing to do.
    let stats = compactor.run_once().await.unwrap();
    assert_eq!(stats.merged_groups, 0);
}

#[tokio::test]
async fn separated_compaction_gives_each_key_its_own_file() {
    let env = setup().await;
    env.catalog.set_placement(env.ht, Placement::Separated).await.unwrap();
    add_l0(&env, "d1", vec![
        json!({"tenant_id": 5, "ts": 1, "payload": "x1"}),
        json!({"tenant_id": 6, "ts": 2, "payload": "y1"}),
    ]).await;
    add_l0(&env, "d1", vec![json!({"tenant_id": 5, "ts": 3, "payload": "x2"})]).await;
    let before = live_payloads(&env).await;

    let compactor =
        Compactor::new(env.catalog.clone(), env.store.clone(), CompactorConfig::default());
    let stats = compactor.run_once().await.unwrap();
    assert_eq!(stats.parts_in, 2);
    assert_eq!(stats.parts_out, 2, "keys 5 and 6 each get a dedicated file");

    assert_eq!(live_payloads(&env).await, before);
    let parts = env.catalog.live_parts(env.ht, None).await.unwrap();
    assert_eq!(parts.len(), 2);
    for p in &parts {
        assert_eq!(p.meta.level, 1);
        assert_eq!(p.meta.packing_key_min, p.meta.packing_key_max, "separated = one key per file");
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p ukiel-compactor --test compactor_test -- --test-threads=2`
Expected: compile error (`compactor` module not found).

- [ ] **Step 3: Implement `crates/ukiel-compactor/src/compactor.rs`**

```rust
//! L0 -> L1 compaction. The change feed (via the durable cursor) is only a
//! wake-up signal; every pass plans from current live-parts state, so passes
//! are idempotent and REPLACE conflicts are safely retried next pass.

use std::collections::HashMap;
use std::sync::Arc;

use object_store::path::Path;
use object_store::ObjectStore;
use ukiel_catalog::{CatalogError, PostgresCatalog};
use ukiel_core::{arrow_schema_from_json, CommitOp, Hypertable, Part, PartMeta, Placement};

use crate::rewrite;
use crate::CompactorError;

#[derive(Debug, Clone)]
pub struct CompactorConfig {
    /// Cursor identity in `worker_cursors`.
    pub worker_name: String,
    /// Merge a partition's L0 files once at least this many exist.
    pub min_l0_files: usize,
    /// Loop tick for `run` (Task 4).
    pub poll_interval_ms: u64,
}

impl Default for CompactorConfig {
    fn default() -> Self {
        Self { worker_name: "compactor".to_string(), min_l0_files: 2, poll_interval_ms: 5000 }
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct CompactionStats {
    pub merged_groups: usize,
    pub parts_in: usize,
    pub parts_out: usize,
    pub conflicts: usize,
}

pub struct Compactor {
    catalog: PostgresCatalog,
    store: Arc<dyn ObjectStore>,
    config: CompactorConfig,
}

impl Compactor {
    pub fn new(
        catalog: PostgresCatalog,
        store: Arc<dyn ObjectStore>,
        config: CompactorConfig,
    ) -> Self {
        Self { catalog, store, config }
    }

    /// One idempotent pass over every hypertable.
    pub async fn run_once(&self) -> Result<CompactionStats, CompactorError> {
        let mut stats = CompactionStats::default();
        for hypertable in self.catalog.list_hypertables().await? {
            self.compact_hypertable(&hypertable, &mut stats).await?;
        }
        Ok(stats)
    }

    async fn compact_hypertable(
        &self,
        hypertable: &Hypertable,
        stats: &mut CompactionStats,
    ) -> Result<(), CompactorError> {
        // Wake-up signal: skip hypertables with no feed activity since our cursor.
        let cursor = self.catalog.get_cursor(&self.config.worker_name, hypertable.id).await?;
        let events = self.catalog.changes_since(hypertable.id, cursor, 1000).await?;
        if events.is_empty() {
            return Ok(());
        }

        // Plan from live state, never from the events themselves.
        let live = self.catalog.live_parts(hypertable.id, None).await?;
        let mut groups: HashMap<String, Vec<Part>> = HashMap::new();
        for part in live.into_iter().filter(|p| p.meta.level == 0) {
            groups.entry(part.meta.partition_values.to_string()).or_default().push(part);
        }

        for (_, group) in groups {
            if group.len() < self.config.min_l0_files {
                continue;
            }
            match self.merge_group(hypertable, &group).await {
                Ok(parts_out) => {
                    stats.merged_groups += 1;
                    stats.parts_in += group.len();
                    stats.parts_out += parts_out;
                }
                // Another writer got here first: replan next pass.
                Err(CompactorError::Catalog(CatalogError::Conflict { .. })) => {
                    stats.conflicts += 1;
                }
                Err(e) => return Err(e),
            }
        }

        // Advance past everything we saw. Crash before this line only means
        // re-scanning: replanning from live state finds no L0s left to merge.
        let last = events.last().expect("non-empty").commit_id;
        self.catalog.set_cursor(&self.config.worker_name, hypertable.id, last).await?;
        Ok(())
    }

    /// Merges one partition's L0 files into L1 output per the placement
    /// policy and commits the swap. Returns the number of output parts.
    async fn merge_group(
        &self,
        hypertable: &Hypertable,
        group: &[Part],
    ) -> Result<usize, CompactorError> {
        let schema = Arc::new(arrow_schema_from_json(&hypertable.table_schema)?);
        let merged = rewrite::read_parts_to_batch(&self.store, schema, group).await?;
        let sorted = rewrite::sort_batch(&merged, &hypertable.sort_key)?;
        let partition_values = group[0].meta.partition_values.clone();

        let outputs: Vec<(i64, i64, arrow::array::RecordBatch)> = match hypertable.placement {
            Placement::Packed => {
                let (min, max) = rewrite::key_range(&sorted, &hypertable.packing_key)?;
                vec![(min, max, sorted)]
            }
            Placement::Separated => rewrite::split_by_key(&sorted, &hypertable.packing_key)?
                .into_iter()
                .map(|(key, batch)| (key, key, batch))
                .collect(),
        };

        let mut new_parts = Vec::with_capacity(outputs.len());
        for (key_min, key_max, batch) in &outputs {
            let bytes = rewrite::batch_to_parquet(batch)?;
            let path = format!("ht/{}/L1/{}.parquet", hypertable.id, uuid::Uuid::new_v4());
            let size_bytes = bytes.len() as i64;
            self.store.put(&Path::from(path.clone()), bytes.into()).await?;
            new_parts.push(PartMeta {
                path,
                partition_values: partition_values.clone(),
                packing_key_min: *key_min,
                packing_key_max: *key_max,
                row_count: batch.num_rows() as i64,
                size_bytes,
                level: 1,
                column_stats: None,
            });
        }

        let old = group.iter().map(|p| p.id).collect();
        let count = new_parts.len();
        // Conflict propagates as CatalogError::Conflict; uploaded objects are
        // then orphans — harmless, cleaned by a future GC worker.
        self.catalog
            .commit(hypertable.id, CommitOp::Replace { old, new: new_parts }, None)
            .await?;
        Ok(count)
    }
}
```

Add to `crates/ukiel-compactor/src/lib.rs`:

```rust
pub mod compactor;
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p ukiel-compactor -- --test-threads=2`
Expected: rewrite unit test + both compaction tests pass.

- [ ] **Step 5: Lint and commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add crates/ukiel-compactor
git commit -m "feat: l0-to-l1 compaction honoring placement policy"
```

---

### Task 4: Worker loop + race safety against concurrent writers

**Files:**
- Modify: `crates/ukiel-compactor/src/compactor.rs`
- Modify: `crates/ukiel-compactor/tests/compactor_test.rs`

**Interfaces:**
- Produces: `Compactor::run(&self, shutdown: CancellationToken) -> Result<(), CompactorError>` — `run_once` on every tick until cancelled; conflicts are counted, never fatal.
- Proves: invariant I5 (concurrent ingest + compaction never lose or duplicate rows) at component level; e2e S6 builds on this.

- [ ] **Step 1: Add the loop**

Append to the `impl Compactor` block in `compactor.rs`:

```rust
    /// Ticks `run_once` until cancelled. Conflicts are expected under
    /// concurrency and are never fatal; real errors stop the worker.
    pub async fn run(
        &self,
        shutdown: tokio_util::sync::CancellationToken,
    ) -> Result<(), CompactorError> {
        let mut ticker = tokio::time::interval(std::time::Duration::from_millis(
            self.config.poll_interval_ms,
        ));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => return Ok(()),
                _ = ticker.tick() => {
                    let stats = self.run_once().await?;
                    if stats.merged_groups > 0 || stats.conflicts > 0 {
                        tracing::info!(?stats, "compaction pass");
                    }
                }
            }
        }
    }
```

- [ ] **Step 2: Write the race tests**

Append to `crates/ukiel-compactor/tests/compactor_test.rs`:

```rust
use tokio_util::sync::CancellationToken;

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_ingest_and_compaction_preserve_all_rows() {
    let env = setup().await;
    let compactor = Compactor::new(
        env.catalog.clone(),
        env.store.clone(),
        CompactorConfig { poll_interval_ms: 50, ..CompactorConfig::default() },
    );
    let shutdown = CancellationToken::new();
    let worker = {
        let token = shutdown.clone();
        tokio::spawn(async move { compactor.run(token).await })
    };

    // A writer races the compactor: 20 single-row L0 commits.
    for i in 0..20 {
        add_l0(
            &env,
            "d1",
            vec![json!({"tenant_id": i % 3, "ts": i, "payload": format!("row-{i}")})],
        )
        .await;
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }

    // Let compaction converge, then stop it.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    shutdown.cancel();
    worker.await.unwrap().unwrap();

    // Invariant I5: nothing lost, nothing duplicated — regardless of how many
    // merge rounds won or conflicted.
    let expected: HashSet<String> = (0..20).map(|i| format!("row-{i}")).collect();
    assert_eq!(live_payloads(&env).await, expected);
    let total: i64 = env
        .catalog
        .live_parts(env.ht, None)
        .await
        .unwrap()
        .iter()
        .map(|p| p.meta.row_count)
        .sum();
    assert_eq!(total, 20);

    // Compaction actually happened: fewer live parts than the 20 committed.
    let parts = env.catalog.live_parts(env.ht, None).await.unwrap();
    assert!(parts.len() < 20, "expected some merging, got {} parts", parts.len());
}

#[tokio::test(flavor = "multi_thread")]
async fn competing_compactors_never_lose_data() {
    let env = setup().await;
    for i in 0..4 {
        add_l0(&env, "d1", vec![json!({"tenant_id": 1, "ts": i, "payload": format!("v{i}")})])
            .await;
    }
    let before = live_payloads(&env).await;

    // Two compactors with distinct cursors plan from the same live state.
    let c1 = Compactor::new(
        env.catalog.clone(),
        env.store.clone(),
        CompactorConfig { worker_name: "c1".into(), ..CompactorConfig::default() },
    );
    let c2 = Compactor::new(
        env.catalog.clone(),
        env.store.clone(),
        CompactorConfig { worker_name: "c2".into(), ..CompactorConfig::default() },
    );
    let (r1, r2) = tokio::join!(c1.run_once(), c2.run_once());
    let (s1, s2) = (r1.unwrap(), r2.unwrap());

    // Whatever interleaving happened — one merged, both merged sequentially,
    // or one conflicted — the data is intact and exactly one L1 lineage wins.
    assert_eq!(live_payloads(&env).await, before);
    let parts = env.catalog.live_parts(env.ht, None).await.unwrap();
    let total: i64 = parts.iter().map(|p| p.meta.row_count).sum();
    assert_eq!(total, 4);
    assert!(s1.merged_groups + s2.merged_groups >= 1, "at least one pass merged: {s1:?} {s2:?}");
}
```

- [ ] **Step 3: Run tests to verify they pass**

Run: `cargo test -p ukiel-compactor -- --test-threads=2`
Expected: all pass. If `concurrent_ingest_and_compaction_preserve_all_rows` flakes on the `parts.len() < 20` assertion, the compactor never got a winning pass — raise the settle sleep, don't weaken the data-equivalence asserts.

- [ ] **Step 4: Lint and commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add crates/ukiel-compactor
git commit -m "feat: compactor loop with race-safe conflict handling"
```

---

### Task 5: Key deletion worker

**Files:**
- Create: `crates/ukiel-compactor/src/deletion.rs`
- Modify: `crates/ukiel-compactor/src/lib.rs`
- Create: `crates/ukiel-compactor/tests/deletion_test.rs`

**Interfaces:**
- Consumes: rewrite engine (Task 2), `live_parts(ht, Some(key))`, `commit` REPLACE.
- Produces:
  - `delete_key(catalog: &PostgresCatalog, store: &Arc<dyn ObjectStore>, hypertable: &Hypertable, key: i64) -> Result<DeletionStats, CompactorError>`
  - `DeletionStats { dropped_parts: usize, rewritten_parts: usize, rows_deleted: i64 }`
  - Semantics (spec "Placement policy" section): parts fully owned by the key (`min == max == key`) are tombstoned without touching data; packed parts overlapping the key are rewritten without its rows (original `level` and `partition_values` preserved). Everything lands in **one atomic REPLACE commit** — a reader sees either all of the key's data or none of it.

- [ ] **Step 1: Write the failing test**

Create `crates/ukiel-compactor/tests/deletion_test.rs`:

```rust
mod compactor_test;

use compactor_test::{add_l0, live_payloads, setup};
use serde_json::json;
use std::collections::HashSet;
use ukiel_compactor::deletion::delete_key;

#[tokio::test]
async fn delete_key_drops_dedicated_and_rewrites_packed() {
    let env = setup().await;
    // A packed file where key 7 shares with key 8, and a file wholly owned by 7.
    add_l0(&env, "d1", vec![
        json!({"tenant_id": 7, "ts": 1, "payload": "seven-a"}),
        json!({"tenant_id": 8, "ts": 2, "payload": "eight-a"}),
    ]).await;
    add_l0(&env, "d1", vec![json!({"tenant_id": 7, "ts": 3, "payload": "seven-b"})]).await;
    // An untouched neighbor that doesn't overlap key 7 at all.
    add_l0(&env, "d2", vec![json!({"tenant_id": 9, "ts": 4, "payload": "nine-a"})]).await;

    let ht = env.catalog.get_hypertable("events").await.unwrap();
    let stats = delete_key(&env.catalog, &env.store, &ht, 7).await.unwrap();
    assert_eq!(stats.dropped_parts, 1, "the wholly-owned file is a metadata-only drop");
    assert_eq!(stats.rewritten_parts, 1, "the shared file is rewritten without key 7");
    assert_eq!(stats.rows_deleted, 2);

    // Key 7 is gone; neighbors are intact (invariant I7).
    let expected: HashSet<String> =
        ["eight-a", "nine-a"].into_iter().map(String::from).collect();
    assert_eq!(live_payloads(&env).await, expected);
    for part in env.catalog.live_parts(env.ht, None).await.unwrap() {
        assert!(
            !(part.meta.packing_key_min <= 7 && part.meta.packing_key_max >= 7),
            "no live part may still claim key 7: {part:?}"
        );
    }

    // Idempotent: deleting again is a no-op.
    let stats = delete_key(&env.catalog, &env.store, &ht, 7).await.unwrap();
    assert_eq!(stats.dropped_parts + stats.rewritten_parts, 0);
}
```

Add `#![allow(dead_code)]` at the top of `tests/compactor_test.rs` if the module reuse warns about unused helpers.

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p ukiel-compactor --test deletion_test -- --test-threads=2`
Expected: compile error (`deletion` module not found).

- [ ] **Step 3: Implement `crates/ukiel-compactor/src/deletion.rs`**

```rust
//! Key deletion (GDPR / per-customer erasure). One atomic REPLACE commit:
//! dedicated files are dropped metadata-only, packed files are rewritten
//! without the key's rows. See the spec's "Placement policy" section.

use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::compute::filter_record_batch;
use arrow::compute::kernels::cmp::neq;
use object_store::path::Path;
use object_store::ObjectStore;
use ukiel_catalog::PostgresCatalog;
use ukiel_core::{arrow_schema_from_json, CommitOp, Hypertable, PartMeta};

use crate::rewrite;
use crate::CompactorError;

#[derive(Debug, Default, PartialEq, Eq)]
pub struct DeletionStats {
    pub dropped_parts: usize,
    pub rewritten_parts: usize,
    pub rows_deleted: i64,
}

pub async fn delete_key(
    catalog: &PostgresCatalog,
    store: &Arc<dyn ObjectStore>,
    hypertable: &Hypertable,
    key: i64,
) -> Result<DeletionStats, CompactorError> {
    let overlapping = catalog.live_parts(hypertable.id, Some(key)).await?;
    if overlapping.is_empty() {
        return Ok(DeletionStats::default());
    }

    let schema = Arc::new(arrow_schema_from_json(&hypertable.table_schema)?);
    let mut stats = DeletionStats::default();
    let mut new_parts: Vec<PartMeta> = Vec::new();

    for part in &overlapping {
        if part.meta.packing_key_min == key && part.meta.packing_key_max == key {
            // Wholly owned by the key: tombstone only, no data movement.
            stats.dropped_parts += 1;
            stats.rows_deleted += part.meta.row_count;
            continue;
        }

        // Packed file: rewrite without the key's rows.
        let batch =
            rewrite::read_parts_to_batch(store, schema.clone(), std::slice::from_ref(part))
                .await?;
        let keys = rewrite::int64_column(&batch, &hypertable.packing_key)?;
        let mask = neq(keys, &Int64Array::new_scalar(key))?;
        let kept = filter_record_batch(&batch, &mask)?;
        stats.rewritten_parts += 1;
        stats.rows_deleted += (batch.num_rows() - kept.num_rows()) as i64;

        if kept.num_rows() == 0 {
            continue; // range overlapped but every row was the key's
        }
        let (key_min, key_max) = rewrite::key_range(&kept, &hypertable.packing_key)?;
        let bytes = rewrite::batch_to_parquet(&kept)?;
        let path = format!(
            "ht/{}/L{}/{}.parquet",
            hypertable.id, part.meta.level, uuid::Uuid::new_v4()
        );
        let size_bytes = bytes.len() as i64;
        store.put(&Path::from(path.clone()), bytes.into()).await?;
        new_parts.push(PartMeta {
            path,
            partition_values: part.meta.partition_values.clone(),
            packing_key_min: key_min,
            packing_key_max: key_max,
            row_count: kept.num_rows() as i64,
            size_bytes,
            level: part.meta.level,
            column_stats: None,
        });
    }

    // One atomic swap: readers see all of the key's data or none of it.
    let old = overlapping.iter().map(|p| p.id).collect();
    catalog.commit(hypertable.id, CommitOp::Replace { old, new: new_parts }, None).await?;
    Ok(stats)
}
```

(`rewrite::int64_column` is `pub(crate)`, which is sufficient — deletion.rs is in the same crate.)

Add to `crates/ukiel-compactor/src/lib.rs`:

```rust
pub mod deletion;
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p ukiel-compactor -- --test-threads=2`
Expected: all pass, including double-delete idempotence.

- [ ] **Step 5: Lint and commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add crates/ukiel-compactor
git commit -m "feat: atomic key deletion with metadata-only fast path"
```

---

### Task 6: Full-workspace verification + docs

**Files:**
- Modify: `README.md`
- Modify: `docs/superpowers/plans/2026-07-05-ukiel-v1-roadmap.md`

- [ ] **Step 1: Run the full suite**

Run: `cargo fmt --check && cargo clippy --all-targets -- -D warnings && make test`
Expected: all crates green (Docker running).

- [ ] **Step 2: Update `README.md`**

In the `## Development` crate list, add after the `ukiel-query` bullet:

```markdown
- `crates/ukiel-compactor` — background rewrite workers on the change feed:
  L0→L1 compaction honoring each hypertable's placement policy (`packed` or
  `separated` per key), and atomic key deletion (metadata-only for dedicated
  files, rewrite for packed ones). All swaps are optimistic REPLACE commits;
  losers replan from fresh state.
```

- [ ] **Step 3: Update the roadmap**

In `docs/superpowers/plans/2026-07-05-ukiel-v1-roadmap.md`, set plan 4's status to **Executed**.

- [ ] **Step 4: Commit**

```bash
git add README.md docs/superpowers/plans/2026-07-05-ukiel-v1-roadmap.md
git commit -m "docs: compactor crate overview, roadmap status"
```

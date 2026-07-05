# Ukiel Plan 9: Upload Intent (listing-free orphan GC) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Eliminate the object-store `LIST` from GC's orphan sweep by recording each object's path in a catalog `pending_objects` table **before** it is uploaded; a commit that references the object clears its pending row, so an orphan (uploaded but never committed) is a catalog query — not a bucket scan.

**Architecture:** Ukiel writers upload objects before committing, so a crash or lost REPLACE race between upload and commit leaves an unreferenced object. Today `ukiel-gc`'s sweeper finds these by `LIST`ing every object under `ht/{id}` and diffing against the catalog (`crates/ukiel-gc/src/lib.rs:105`, `all_part_paths` at `crates/ukiel-catalog/src/gc.rs:57`) — `O(objects)` S3 requests per hypertable per pass, plus an unbounded in-memory path set. This plan adds a **write-ahead intent**: writers `register_pending_objects(paths)` (one INSERT) before `store.put`, and the part-insert inside `commit` deletes the matching pending rows in the same transaction. Orphans become "pending rows older than the grace whose commit never landed" — discovered by an indexed query, zero `LIST`. A rarely-run `reconcile` pass keeps the old listing scan as defense-in-depth for objects a buggy writer uploaded without registering.

**Tech Stack:** sqlx 0.9, object_store 0.13, tokio. No arrow/parquet — this plan never reads file contents.

**Prerequisites:** Plans 1–6 executed and green (`ukiel-core`, `ukiel-catalog`, `ukiel-ingest`, `ukiel-query`, `ukiel-compactor`, `ukiel-gc`).

## Global Constraints

- Rust edition 2024, toolchain ≥ 1.96.
- Plain `sqlx::query`/`query_as`/`query_scalar` string queries, no `query!` macros. Dynamic query strings must be wrapped in `sqlx::AssertSqlSafe(...)` (sqlx 0.9 requires it — plain `&String` does not implement `SqlSafeStr`).
- Component tests: testcontainers Postgres + `object_store::memory::InMemory`; run via `make test` (`--test-threads=2`). `object_store` 0.13 exposes `put`/`get`/`delete` via the `ObjectStoreExt` trait — `use object_store::ObjectStoreExt` wherever a test or impl calls them.
- Commit messages: conventional style, no Claude/AI attribution.
- Every task ends with `cargo fmt && cargo clippy --all-targets -- -D warnings` clean.

## Crash-window correctness (the invariant this plan preserves)

Every write site follows the order **register → upload → commit**. The three crash windows:

| Crash point | pending row | object | part row | Outcome |
|---|---|---|---|---|
| after register, before upload | exists | absent | absent | Sweep deletes a missing object (`NotFound` = ok), clears the row. Harmless. |
| after upload, before commit | exists | exists | absent | The orphan case. Sweep (past grace) deletes object + row. Correct. |
| commit succeeds | **deleted in same txn** | exists | exists | Object is referenced; never an orphan. Correct. |
| REPLACE loses the race | exists (txn rolled back) | exists | absent | Object orphaned → swept. Correct. |

Registering **before** upload is the whole safety property: an object uploaded before its intent row would be invisible to the intent sweep. (The `reconcile` pass in Task 5 is the backstop if that order is ever violated.)

Two documented limitations, both inherited from the listing-based design (not regressions):

- **Grace now runs from registration, not upload completion.** The intent row's `created_at` predates the upload, so a very slow upload eats into the orphan grace. With the 1h default vs seconds-long uploads this is academic; if uploads ever take minutes, size the grace accordingly.
- **A writer stalled longer than the grace loses its uncommitted work.** If a writer registers, then stalls past `orphan_grace` before committing, the sweeper deletes its object; the late commit then succeeds but references a deleted object. Enforcing "commit fails if the intent row was already swept" is deliberately NOT done — many legitimate callers (tests, fixtures, external loaders) commit parts without registering, and the same hazard existed with the listing sweep. Mitigation is the same as issue 0002's: bound writer stall time well below the grace.

---

### Task 1: Catalog — `pending_objects` table, register / orphaned / clear, commit clears intent

**Files:**
- Create: `crates/ukiel-catalog/migrations/0005_pending_objects.sql`
- Modify: `crates/ukiel-catalog/src/gc.rs` (add the three pending-object methods)
- Modify: `crates/ukiel-catalog/src/commit.rs:104` (`insert_parts` clears matching pending rows)
- Modify: `crates/ukiel-catalog/tests/catalog_test.rs`

**Interfaces:**
- Produces (on `PostgresCatalog`):
  - `register_pending_objects(&self, hypertable_id: HypertableId, paths: &[String]) -> Result<(), CatalogError>` — idempotent (`ON CONFLICT (path) DO NOTHING`).
  - `orphaned_pending_objects(&self, hypertable_id: HypertableId, grace_seconds: f64) -> Result<Vec<String>, CatalogError>` — pending paths whose row is older than the grace, ordered by path.
  - `clear_pending_objects(&self, paths: &[String]) -> Result<(), CatalogError>`.
- Modifies: `commit()` / `commit_with_offsets()` now delete `pending_objects` rows for every path they insert into `parts`, in the same transaction.

- [ ] **Step 1: Write the migration**

`crates/ukiel-catalog/migrations/0005_pending_objects.sql`:

```sql
-- Write-ahead upload intent: a writer records an object's path here BEFORE
-- uploading it. The commit that references the object deletes its row (same
-- transaction), so a row that outlives the orphan grace is an object whose
-- commit never landed -> a reapable orphan, discoverable without listing S3.
CREATE TABLE pending_objects (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    hypertable_id BIGINT NOT NULL REFERENCES hypertables(id),
    path TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX pending_objects_sweep_idx ON pending_objects (hypertable_id, created_at);
```

- [ ] **Step 2: Write the failing test**

Append to `crates/ukiel-catalog/tests/catalog_test.rs`:

```rust
#[tokio::test]
async fn pending_objects_track_orphans_and_clear_on_commit() {
    let (_pg, catalog) = common::setup().await;
    let ht = setup_hypertable(&catalog).await;

    // A writer records intent for two objects it is about to upload.
    catalog
        .register_pending_objects(ht, &["a.parquet".to_string(), "b.parquet".to_string()])
        .await
        .unwrap();

    // Both are orphan candidates at grace 0 (rows are in the past), none at a
    // large grace (protects in-flight commits).
    let orphans = catalog.orphaned_pending_objects(ht, 0.0).await.unwrap();
    assert_eq!(orphans, vec!["a.parquet".to_string(), "b.parquet".to_string()]);
    assert!(catalog.orphaned_pending_objects(ht, 3600.0).await.unwrap().is_empty());

    // Re-registering the same path is a no-op (idempotent retry).
    catalog.register_pending_objects(ht, &["a.parquet".to_string()]).await.unwrap();
    assert_eq!(catalog.orphaned_pending_objects(ht, 0.0).await.unwrap().len(), 2);

    // Committing a part with path "a.parquet" clears its intent row.
    catalog
        .commit(ht, CommitOp::Add { parts: vec![part_meta("a.parquet", 1, 10)] }, None)
        .await
        .unwrap();
    assert_eq!(
        catalog.orphaned_pending_objects(ht, 0.0).await.unwrap(),
        vec!["b.parquet".to_string()]
    );

    // The sweeper clears the row after deleting the object.
    catalog.clear_pending_objects(&["b.parquet".to_string()]).await.unwrap();
    assert!(catalog.orphaned_pending_objects(ht, 0.0).await.unwrap().is_empty());
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `cargo test -p ukiel-catalog pending_objects -- --test-threads=2`
Expected: compile error (`register_pending_objects` not found).

- [ ] **Step 4: Implement the catalog methods**

Append to the `impl PostgresCatalog` block in `crates/ukiel-catalog/src/gc.rs`:

```rust
    /// Records upload intent for `paths` (idempotent). Callers MUST do this
    /// before uploading, so a crash between upload and commit leaves a
    /// discoverable orphan.
    pub async fn register_pending_objects(
        &self,
        hypertable_id: HypertableId,
        paths: &[String],
    ) -> Result<(), CatalogError> {
        if paths.is_empty() {
            return Ok(());
        }
        sqlx::query(
            "INSERT INTO pending_objects (hypertable_id, path)
             SELECT $1, p FROM UNNEST($2::text[]) AS p
             ON CONFLICT (path) DO NOTHING",
        )
        .bind(hypertable_id.0)
        .bind(paths)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Pending-object paths older than `grace_seconds` whose commit never
    /// landed — i.e. orphaned uploads. Ordered by path for deterministic tests.
    pub async fn orphaned_pending_objects(
        &self,
        hypertable_id: HypertableId,
        grace_seconds: f64,
    ) -> Result<Vec<String>, CatalogError> {
        Ok(sqlx::query_scalar(
            "SELECT path FROM pending_objects
             WHERE hypertable_id = $1
               AND created_at <= now() - make_interval(secs => $2)
             ORDER BY path",
        )
        .bind(hypertable_id.0)
        .bind(grace_seconds)
        .fetch_all(&self.pool)
        .await?)
    }

    /// Removes intent rows for the given paths (after their objects are gone).
    pub async fn clear_pending_objects(&self, paths: &[String]) -> Result<(), CatalogError> {
        if paths.is_empty() {
            return Ok(());
        }
        sqlx::query("DELETE FROM pending_objects WHERE path = ANY($1)")
            .bind(paths)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
```

In `crates/ukiel-catalog/src/commit.rs`, at the end of `insert_parts` (after the `for p in parts` loop, before `Ok(())`), clear intent for the committed paths in the same transaction:

```rust
    // A committed part's object is now catalog-tracked; drop its upload-intent
    // row so the GC sweeper stops treating it as a potential orphan.
    let paths: Vec<String> = parts.iter().map(|p| p.path.clone()).collect();
    sqlx::query("DELETE FROM pending_objects WHERE path = ANY($1)")
        .bind(&paths)
        .execute(&mut **tx)
        .await?;
```

(Empty `parts` — an offset-only ingest flush — makes `paths` empty and deletes nothing.)

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test -p ukiel-catalog -- --test-threads=2`
Expected: all pass, including `pending_objects_track_orphans_and_clear_on_commit`.

- [ ] **Step 6: Lint and commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add crates/ukiel-catalog
git commit -m "feat: upload-intent table - register/orphaned/clear, commit clears intent"
```

---

### Task 2: Ingest — register intent before upload in the flusher

**Files:**
- Modify: `crates/ukiel-ingest/Cargo.toml` (add `async-trait` dev-dependency)
- Modify: `crates/ukiel-ingest/src/flusher.rs`
- Modify: `crates/ukiel-ingest/tests/flusher_test.rs`

**Interfaces:**
- Consumes: `register_pending_objects` (Task 1).
- Produces: `Flusher::flush` registers every part's path before uploading it; a successful flush leaves no pending rows (the commit clears them).

The `FailingPutStore` below implements `object_store::ObjectStore`, whose 0.13 definition is `#[async_trait]`-based, so add `async-trait` to `[dev-dependencies]` in `crates/ukiel-ingest/Cargo.toml`:

```toml
async-trait = { workspace = true }
```

- [ ] **Step 1: Write the failing tests**

Append to `crates/ukiel-ingest/tests/flusher_test.rs`. The first test proves the happy-path clear; the second proves register-happens-before-upload using a store whose `put` always fails, so the commit never runs and the intent rows survive.

```rust
use std::fmt;

use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, PutMultipartOptions,
    PutOptions, PutPayload, PutResult, Result as OsResult,
};
use futures::stream::BoxStream;

/// Object store that fails every upload but delegates reads/lists — lets a test
/// exercise the "registered, upload failed, never committed" window.
#[derive(Debug)]
struct FailingPutStore {
    inner: Arc<dyn ObjectStore>,
}

#[async_trait::async_trait]
impl ObjectStore for FailingPutStore {
    async fn put_opts(
        &self,
        _location: &object_store::path::Path,
        _payload: PutPayload,
        _opts: PutOptions,
    ) -> OsResult<PutResult> {
        Err(object_store::Error::Generic {
            store: "failing",
            source: "put disabled".into(),
        })
    }
    async fn put_multipart_opts(
        &self,
        location: &object_store::path::Path,
        opts: PutMultipartOptions,
    ) -> OsResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }
    async fn get_opts(
        &self,
        location: &object_store::path::Path,
        options: GetOptions,
    ) -> OsResult<GetResult> {
        self.inner.get_opts(location, options).await
    }
    fn delete_stream(
        &self,
        locations: BoxStream<'static, OsResult<object_store::path::Path>>,
    ) -> BoxStream<'static, OsResult<object_store::path::Path>> {
        self.inner.delete_stream(locations)
    }
    fn list(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> BoxStream<'static, OsResult<ObjectMeta>> {
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> OsResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(
        &self,
        from: &object_store::path::Path,
        to: &object_store::path::Path,
        options: object_store::CopyOptions,
    ) -> OsResult<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

impl fmt::Display for FailingPutStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FailingPutStore")
    }
}

#[tokio::test]
async fn successful_flush_leaves_no_pending_intent() {
    let container = Postgres::default().start().await.expect("start postgres");
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
    let catalog = PostgresCatalog::connect(&url).await.unwrap();
    catalog.migrate().await.unwrap();
    let ht_id = catalog
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
    let arrow_schema = arrow_schema_from_json(&ht.table_schema).unwrap();
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let flusher = Flusher::new(catalog.clone(), store.clone());

    let part = rows_to_parquet(
        &arrow_schema,
        "tenant_id",
        "ts",
        vec![json!({"tenant_id": 1, "ts": 1, "payload": "a"})],
    )
    .unwrap();
    flusher
        .flush(
            &ht,
            vec![FlushItem { partition_values: json!({"day": "1970-01-01"}), part }],
            vec![OffsetRange { topic: "events".into(), partition: 0, first: 0, last: 0 }],
        )
        .await
        .unwrap();

    assert_eq!(catalog.live_parts(ht_id, None).await.unwrap().len(), 1);
    assert!(
        catalog.orphaned_pending_objects(ht_id, 0.0).await.unwrap().is_empty(),
        "commit must clear the upload-intent row"
    );
}

#[tokio::test]
async fn failed_upload_leaves_discoverable_intent() {
    let container = Postgres::default().start().await.expect("start postgres");
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
    let catalog = PostgresCatalog::connect(&url).await.unwrap();
    catalog.migrate().await.unwrap();
    let ht_id = catalog
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
    let arrow_schema = arrow_schema_from_json(&ht.table_schema).unwrap();
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let store: Arc<dyn ObjectStore> = Arc::new(FailingPutStore { inner });
    let flusher = Flusher::new(catalog.clone(), store);

    let part = rows_to_parquet(
        &arrow_schema,
        "tenant_id",
        "ts",
        vec![json!({"tenant_id": 1, "ts": 1, "payload": "a"})],
    )
    .unwrap();
    let result = flusher
        .flush(
            &ht,
            vec![FlushItem { partition_values: json!({"day": "1970-01-01"}), part }],
            vec![OffsetRange { topic: "events".into(), partition: 0, first: 0, last: 0 }],
        )
        .await;

    assert!(result.is_err(), "upload failure must abort the flush");
    assert_eq!(catalog.live_parts(ht_id, None).await.unwrap().len(), 0);
    // Intent was registered BEFORE the (failing) upload, so the orphan is
    // discoverable even though the commit never ran.
    assert_eq!(
        catalog.orphaned_pending_objects(ht_id, 0.0).await.unwrap().len(),
        1,
        "register must happen before upload"
    );
}
```

Add any missing imports at the top of `flusher_test.rs` (`use std::sync::Arc;`, `use object_store::ObjectStore;` are already present from Task 4 of the ingest plan).

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p ukiel-ingest --test flusher_test -- --test-threads=2`
Expected: `failed_upload_leaves_discoverable_intent` fails (no intent registered — orphan count 0, not 1).

- [ ] **Step 3: Register intent before uploading in `flush`**

Replace the body of `Flusher::flush` in `crates/ukiel-ingest/src/flusher.rs` (the method starting at line 32) with:

```rust
    pub async fn flush(
        &self,
        hypertable: &Hypertable,
        items: Vec<FlushItem>,
        offsets: Vec<OffsetRange>,
    ) -> Result<CommitResult, IngestError> {
        // Assign every part's path up front and record upload intent BEFORE any
        // upload, so a crash between upload and commit leaves a discoverable
        // orphan for GC (never an untracked object).
        let prepared: Vec<(String, FlushItem)> = items
            .into_iter()
            .map(|item| {
                let path = format!("ht/{}/L0/{}.parquet", hypertable.id, uuid::Uuid::new_v4());
                (path, item)
            })
            .collect();
        let paths: Vec<String> = prepared.iter().map(|(p, _)| p.clone()).collect();
        self.catalog
            .register_pending_objects(hypertable.id, &paths)
            .await?;

        let mut parts = Vec::with_capacity(prepared.len());
        for (path, item) in prepared {
            let size_bytes = item.part.bytes.len() as i64;
            self.store
                .put(&Path::from(path.clone()), item.part.bytes.into())
                .await?;
            parts.push(PartMeta {
                path,
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p ukiel-ingest --test flusher_test -- --test-threads=2`
Expected: both new tests pass, plus the pre-existing `flush_writes_objects_and_commits_atomically`.

- [ ] **Step 5: Lint and commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add crates/ukiel-ingest
git commit -m "feat: ingest flusher records upload intent before uploading parts"
```

---

### Task 3: Compactor & deletion — register intent before upload

**Files:**
- Modify: `crates/ukiel-compactor/Cargo.toml` (add `async-trait` dev-dependency)
- Modify: `crates/ukiel-compactor/src/compactor.rs` (`merge_group`)
- Modify: `crates/ukiel-compactor/src/deletion.rs` (`delete_key`)
- Modify: `crates/ukiel-compactor/tests/compactor_test.rs`

**Interfaces:**
- Consumes: `register_pending_objects` (Task 1).
- Produces: every L1 file (compaction) and rewritten file (deletion) has its path registered before upload; a successful REPLACE clears the rows.

The copied `FailingPutStore` implements `object_store::ObjectStore` (0.13, `#[async_trait]`-based), so add `async-trait` to `[dev-dependencies]` in `crates/ukiel-compactor/Cargo.toml`:

```toml
async-trait = { workspace = true }
```

- [ ] **Step 1: Write the failing tests**

A "pending is empty after a successful compaction" assertion passes *trivially before* the fix (nothing registered → nothing to clear), so it is not a valid failing-first test. Instead prove register-before-upload with a store whose `put` fails: registration runs, the upload fails, the REPLACE never lands, and the path is left as a discoverable orphan.

Copy the `FailingPutStore` definition from Task 2 verbatim into the top of `crates/ukiel-compactor/tests/compactor_test.rs` (integration-test binaries do not share code), then append these two tests:

```rust
#[tokio::test]
async fn compaction_registers_intent_before_upload() {
    let env = setup().await;
    add_l0(&env, "d1", vec![json!({"tenant_id": 1, "ts": 1, "payload": "a"})]).await;
    add_l0(&env, "d1", vec![json!({"tenant_id": 1, "ts": 2, "payload": "b"})]).await;

    let failing: Arc<dyn ObjectStore> = Arc::new(FailingPutStore { inner: env.store.clone() });
    let compactor = Compactor::new(env.catalog.clone(), failing, CompactorConfig::default());
    let result = compactor.run_once().await;

    assert!(result.is_err(), "upload failure must abort the merge");
    assert!(
        !env.catalog.orphaned_pending_objects(env.ht, 0.0).await.unwrap().is_empty(),
        "the L1 path must be registered as intent before it is uploaded"
    );
    // Nothing was committed: the two L0s are still live.
    assert_eq!(env.catalog.live_parts(env.ht, None).await.unwrap().len(), 2);
}

#[tokio::test]
async fn deletion_registers_intent_before_upload() {
    let env = setup().await;
    // A packed file (keys 7 and 8) that a key-7 deletion must rewrite.
    add_l0(
        &env,
        "d1",
        vec![
            json!({"tenant_id": 7, "ts": 1, "payload": "seven"}),
            json!({"tenant_id": 8, "ts": 2, "payload": "eight"}),
        ],
    )
    .await;
    let ht = env.catalog.get_hypertable("events").await.unwrap();
    let failing: Arc<dyn ObjectStore> = Arc::new(FailingPutStore { inner: env.store.clone() });

    let result = ukiel_compactor::deletion::delete_key(&env.catalog, &failing, &ht, 7).await;
    assert!(result.is_err(), "upload failure must abort the deletion rewrite");
    assert!(
        !env.catalog.orphaned_pending_objects(env.ht, 0.0).await.unwrap().is_empty(),
        "the rewritten path must be registered as intent before it is uploaded"
    );
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p ukiel-compactor registers_intent -- --test-threads=2`
Expected: both FAIL — before the fix nothing is registered, so `orphaned_pending_objects` is empty and the `!is_empty()` assertions fail.

- [ ] **Step 3: Register intent in `merge_group`**

In `crates/ukiel-compactor/src/compactor.rs`, inside `merge_group`, after the `outputs` vector is built and before the upload loop, generate paths and register them; then upload using those paths. Replace the upload loop so paths are assigned up front:

```rust
        // Assign L1 paths and record upload intent before any upload.
        let mut new_parts = Vec::with_capacity(outputs.len());
        let paths: Vec<String> = outputs
            .iter()
            .map(|_| format!("ht/{}/L1/{}.parquet", hypertable.id, uuid::Uuid::new_v4()))
            .collect();
        self.catalog
            .register_pending_objects(hypertable.id, &paths)
            .await?;

        for ((key_min, key_max, batch), path) in outputs.iter().zip(paths) {
            let bytes = rewrite::batch_to_parquet(batch)?;
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
```

- [ ] **Step 4: Register intent in `delete_key`**

In `crates/ukiel-compactor/src/deletion.rs`, the rewrite loop uploads one path per packed part it rewrites. Register each path immediately before its upload:

```rust
        let path = format!(
            "ht/{}/L{}/{}.parquet",
            hypertable.id,
            part.meta.level,
            uuid::Uuid::new_v4()
        );
        catalog
            .register_pending_objects(hypertable.id, std::slice::from_ref(&path))
            .await?;
        let size_bytes = bytes.len() as i64;
        store.put(&Path::from(path.clone()), bytes.into()).await?;
```

(Replace the existing `let path = ...; ... store.put(...)` block with the version above; the surrounding `new_parts.push(PartMeta { path, ... })` is unchanged.)

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test -p ukiel-compactor -- --test-threads=2`
Expected: all compactor + deletion tests pass, including the new intent tests.

- [ ] **Step 6: Lint and commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add crates/ukiel-compactor
git commit -m "feat: compactor and deletion record upload intent before uploading"
```

---

### Task 4: GC — intent-based sweep replaces the object listing

**Files:**
- Modify: `crates/ukiel-gc/src/lib.rs` (`sweep`)
- Modify: `crates/ukiel-gc/tests/gc_test.rs`

**Interfaces:**
- Consumes: `orphaned_pending_objects` / `clear_pending_objects` (Task 1).
- Produces: `Gc::run_once` sweeps orphans via the catalog (no `store.list`); reaping is unchanged.

- [ ] **Step 1: Rewrite the sweep test**

The old `sweeps_only_old_unreferenced_objects` test uploaded a bare object with no catalog row and expected it swept — under the intent model that object is invisible to the fast sweep (it has no pending row; the `reconcile` pass in Task 5 handles truly-untracked objects). Replace that test in `crates/ukiel-gc/tests/gc_test.rs` with an intent-based one:

```rust
#[tokio::test]
async fn sweeps_orphaned_intent_after_grace() {
    let env = setup().await;
    // A committed object (has a live part) and an orphan (intent + object, no
    // commit — a crashed writer).
    let committed = upload(&env, "committed").await;
    env.catalog
        .commit(env.ht, CommitOp::Add { parts: vec![committed] }, None)
        .await
        .unwrap();

    let orphan_path = format!("ht/{}/L0/orphan.parquet", env.ht);
    env.catalog
        .register_pending_objects(env.ht, &[orphan_path.clone()])
        .await
        .unwrap();
    env.store
        .put(&Path::from(orphan_path.clone()), b"junk".to_vec().into())
        .await
        .unwrap();

    // Grace not elapsed: the orphan is protected (its commit might still land).
    let stats = gc(&env, 3600.0, 3600).run_once().await.unwrap();
    assert_eq!(stats.swept_orphans, 0);
    assert_eq!(object_paths(&env.store).await.len(), 2);

    // Grace 0: the orphan object and its intent row go; the committed part stays.
    let stats = gc(&env, 3600.0, 0).run_once().await.unwrap();
    assert_eq!(stats.swept_orphans, 1);
    let remaining = object_paths(&env.store).await;
    assert_eq!(remaining.len(), 1);
    assert!(remaining[0].ends_with("committed.parquet"));
    assert!(
        env.catalog.orphaned_pending_objects(env.ht, 0.0).await.unwrap().is_empty(),
        "swept orphan's intent row must be cleared"
    );
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p ukiel-gc sweeps_orphaned_intent -- --test-threads=2`
Expected: FAIL — the current `sweep` lists objects and diffs `all_part_paths`; the orphan (uploaded object not in `all_part_paths`) is swept only when `orphan_grace` elapses, but the intent row is never cleared, so the final assertion fails.

- [ ] **Step 3: Rewrite `sweep` to use the intent table**

Replace the `sweep` method in `crates/ukiel-gc/src/lib.rs` with:

```rust
    /// Deletes objects for orphaned upload-intents past the grace, then clears
    /// their intent rows. No object-store listing — orphans are a catalog query
    /// (see `reconcile` for the listing-based backstop).
    async fn sweep(&self, hypertable: &Hypertable, stats: &mut GcStats) -> Result<(), GcError> {
        let orphans = self
            .catalog
            .orphaned_pending_objects(hypertable.id, self.config.orphan_grace_secs as f64)
            .await?;
        if orphans.is_empty() {
            return Ok(());
        }
        for path in &orphans {
            match self.store.delete(&Path::from(path.clone())).await {
                Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
                Err(e) => return Err(e.into()),
            }
        }
        self.catalog.clear_pending_objects(&orphans).await?;
        stats.swept_orphans += orphans.len();
        Ok(())
    }
```

The `use futures::TryStreamExt;` and the `chrono::Utc::now()` / `all_part_paths` / `HashSet` usage all move out of `sweep` — the listing machinery returns in Task 5's `reconcile`. Remove the now-unused imports (`use std::collections::HashSet;`, `use futures::TryStreamExt;`) in this task — `clippy -D warnings` fails on unused imports. Task 5's `reconcile` code uses `std::collections::HashSet` fully qualified and re-adds `use futures::TryStreamExt;`, so nothing needs to survive here.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p ukiel-gc -- --test-threads=2`
Expected: `reaps_tombstoned_parts_after_grace`, `sweeps_orphaned_intent_after_grace`, and `lagging_consumer_cursor_blocks_reaping` all pass.

- [ ] **Step 5: Lint and commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add crates/ukiel-gc
git commit -m "feat: gc orphan sweep uses upload-intent table, not object listing"
```

---

### Task 5: GC — `reconcile` backstop (rare listing pass, bounded known-set)

**Files:**
- Modify: `crates/ukiel-catalog/src/gc.rs` (`all_part_paths` excludes purged; add `all_pending_paths`)
- Modify: `crates/ukiel-gc/src/lib.rs` (`GcConfig`, `reconcile_once`, `run` cadence)
- Modify: `crates/ukiel-gc/tests/gc_test.rs`

**Interfaces:**
- Produces:
  - `all_pending_paths(&self, hypertable_id: HypertableId) -> Result<Vec<String>, CatalogError>`
  - `Gc::reconcile_once(&self) -> Result<GcStats, GcError>` — listing-based sweep for objects under the prefix that are neither a non-purged part nor a pending intent, older than `orphan_grace` (defense-in-depth against writers that upload without registering).
  - `GcConfig.reconcile_every_n_passes: u64` (0 = never); the `run` loop calls `reconcile_once` every Nth tick.

- [ ] **Step 1: Write the failing test**

Append to `crates/ukiel-gc/tests/gc_test.rs`:

```rust
#[tokio::test]
async fn reconcile_sweeps_untracked_objects_only() {
    let env = setup().await;
    // A committed object, a pending (registered, in-flight) object, and a
    // truly-untracked object (uploaded with NO intent row — the failure mode
    // reconcile exists to catch), plus one outside the hypertable prefix.
    let committed = upload(&env, "committed").await;
    env.catalog
        .commit(env.ht, CommitOp::Add { parts: vec![committed] }, None)
        .await
        .unwrap();

    let pending_path = format!("ht/{}/L0/pending.parquet", env.ht);
    env.catalog.register_pending_objects(env.ht, &[pending_path.clone()]).await.unwrap();
    env.store.put(&Path::from(pending_path), b"x".to_vec().into()).await.unwrap();

    env.store
        .put(&Path::from(format!("ht/{}/L0/untracked.parquet", env.ht)), b"x".to_vec().into())
        .await
        .unwrap();
    env.store.put(&Path::from("unrelated/file"), b"keep".to_vec().into()).await.unwrap();

    // Grace 0: only the untracked object is removed. Committed part, in-flight
    // pending object, and out-of-prefix object all survive.
    let gc = Gc::new(
        env.catalog.clone(),
        env.store.clone(),
        GcConfig { tombstone_grace_secs: 3600.0, orphan_grace_secs: 0, ..GcConfig::default() },
    );
    let stats = gc.reconcile_once().await.unwrap();
    assert_eq!(stats.reconciled_orphans, 1);
    let remaining = object_paths(&env.store).await;
    assert!(remaining.iter().any(|p| p.ends_with("committed.parquet")));
    assert!(remaining.iter().any(|p| p.ends_with("pending.parquet")));
    assert!(remaining.iter().any(|p| p == "unrelated/file"));
    assert!(!remaining.iter().any(|p| p.ends_with("untracked.parquet")));
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p ukiel-gc reconcile_sweeps -- --test-threads=2`
Expected: compile error (`reconcile_once` / `reconciled_orphans` not found).

- [ ] **Step 3: Catalog — bound `all_part_paths`, add `all_pending_paths`**

In `crates/ukiel-catalog/src/gc.rs`, change `all_part_paths` to exclude purged rows (a purged object is already deleted, so it can never be a live object under the prefix — excluding keeps the known-set bounded by real object count instead of all-time history). Update its doc comment too — it currently says "in any state (live, tombstoned, purged)", which becomes wrong:

```rust
    /// Every object path the catalog currently accounts for under this
    /// hypertable: live and tombstoned-but-not-yet-purged parts. Purged parts
    /// are excluded — their objects are already deleted, and excluding them
    /// keeps this set bounded by real object count, not all-time history.
    pub async fn all_part_paths(
        &self,
        hypertable_id: HypertableId,
    ) -> Result<Vec<String>, CatalogError> {
        Ok(sqlx::query_scalar(
            "SELECT path FROM parts WHERE hypertable_id = $1 AND purged_at IS NULL",
        )
        .bind(hypertable_id.0)
        .fetch_all(&self.pool)
        .await?)
    }

    /// Every path with an outstanding upload intent (in-flight or orphaned).
    pub async fn all_pending_paths(
        &self,
        hypertable_id: HypertableId,
    ) -> Result<Vec<String>, CatalogError> {
        Ok(
            sqlx::query_scalar("SELECT path FROM pending_objects WHERE hypertable_id = $1")
                .bind(hypertable_id.0)
                .fetch_all(&self.pool)
                .await?,
        )
    }
```

- [ ] **Step 4: GC — config field, `reconcile_once`, run cadence**

In `crates/ukiel-gc/src/lib.rs`:

Add the imports back if removed in Task 4:

```rust
use futures::TryStreamExt;
```

Add the config field (and its default) and stats field:

```rust
// In GcConfig:
    /// Run the listing-based `reconcile` backstop every Nth `run` tick
    /// (0 = never). Reconcile catches objects uploaded without an intent row.
    pub reconcile_every_n_passes: u64,
```

```rust
// In Default for GcConfig, add:
            reconcile_every_n_passes: 60,
```

```rust
// In GcStats, add:
    pub reconciled_orphans: usize,
```

Add the reconcile pass:

```rust
    /// Defense-in-depth: lists objects under each hypertable's prefix and
    /// deletes any older than the orphan grace that the catalog knows nothing
    /// about — neither a non-purged part nor a pending intent. Costs one
    /// object-store LIST per hypertable, so it runs rarely (see
    /// `reconcile_every_n_passes`).
    pub async fn reconcile_once(&self) -> Result<GcStats, GcError> {
        let mut stats = GcStats::default();
        for hypertable in self.catalog.list_hypertables().await? {
            let mut known: std::collections::HashSet<String> = self
                .catalog
                .all_part_paths(hypertable.id)
                .await?
                .into_iter()
                .collect();
            known.extend(self.catalog.all_pending_paths(hypertable.id).await?);

            let prefix = Path::from(format!("ht/{}", hypertable.id));
            let objects: Vec<_> = self.store.list(Some(&prefix)).try_collect().await?;
            let now = chrono::Utc::now();
            for meta in objects {
                if known.contains(meta.location.as_ref()) {
                    continue;
                }
                if (now - meta.last_modified).num_seconds() >= self.config.orphan_grace_secs {
                    match self.store.delete(&meta.location).await {
                        Ok(()) | Err(object_store::Error::NotFound { .. }) => {
                            stats.reconciled_orphans += 1;
                        }
                        Err(e) => return Err(e.into()),
                    }
                }
            }
        }
        Ok(stats)
    }
```

Update the `run` loop to fire reconcile on the cadence:

```rust
    pub async fn run(
        &self,
        shutdown: tokio_util::sync::CancellationToken,
    ) -> Result<(), GcError> {
        let mut ticker = tokio::time::interval(std::time::Duration::from_millis(
            self.config.poll_interval_ms,
        ));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut pass: u64 = 0;
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => return Ok(()),
                _ = ticker.tick() => {
                    let mut stats = self.run_once().await?;
                    pass += 1;
                    if self.config.reconcile_every_n_passes > 0
                        && pass % self.config.reconcile_every_n_passes == 0
                    {
                        stats.reconciled_orphans += self.reconcile_once().await?.reconciled_orphans;
                    }
                    if stats != GcStats::default() {
                        tracing::info!(?stats, "gc pass");
                    }
                }
            }
        }
    }
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test -p ukiel-gc -- --test-threads=2` and `cargo test -p ukiel-catalog -- --test-threads=2`
Expected: all pass, including `reconcile_sweeps_untracked_objects_only`.

- [ ] **Step 6: Lint and commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add crates/ukiel-catalog crates/ukiel-gc
git commit -m "feat: gc reconcile backstop with bounded known-set; purge-excluded part paths"
```

---

### Task 6: Full-workspace verification + docs

**Files:**
- Modify: `README.md`
- Modify: `docs/superpowers/plans/2026-07-05-ukiel-v1-roadmap.md`

- [ ] **Step 1: Run the full suite**

Run: `cargo fmt --check && cargo clippy --all-targets -- -D warnings && make test`
Expected: all crates green (Docker running). If `ukiel-ingest`'s Kafka `consumer_test` flakes on container cold-start, rerun `cargo test -p ukiel-ingest --test consumer_test -- --test-threads=2` once.

- [ ] **Step 2: Update `README.md`**

Replace the `ukiel-gc` bullet in the `## Development` crate list with:

```markdown
- `crates/ukiel-gc` — object garbage collection: reaps tombstoned parts'
  objects after a grace period (fenced by worker cursors) and sweeps orphaned
  uploads via a write-ahead intent table (`pending_objects`) — no object-store
  listing on the hot path. A rare `reconcile` pass lists the bucket as
  defense-in-depth. Purged parts keep catalog rows for feed replay.
```

- [ ] **Step 3: Add a roadmap row**

In `docs/superpowers/plans/2026-07-05-ukiel-v1-roadmap.md`, add a row after plan 8:

```markdown
| 9 | `2026-07-06-ukiel-upload-intent.md` | Write-ahead upload intent (`pending_objects`): writers register an object's path before upload; the referencing commit clears it. GC's orphan sweep becomes a catalog query (zero S3 `LIST` on the hot path); a rare `reconcile` listing pass is the backstop | **Executed** |
```

- [ ] **Step 4: Commit**

```bash
git add README.md docs/superpowers/plans/2026-07-05-ukiel-v1-roadmap.md
git commit -m "docs: upload-intent gc overview and roadmap entry"
```

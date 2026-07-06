# Ukiel Plan 11: Scan Pushdown (Tier-1 Parquet read performance) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Tier 1 of `docs/notes/2026-07-06-parquet-read-performance.md`: (0) a perf smoke harness with recorded baselines, (1) predicate pushdown into `ParquetSource` (row-group/page pruning + late materialization), (2) projection pushed into the scan instead of decoding every column, (3) declared output ordering so `ORDER BY ts` stops sorting.

**Architecture:** All changes concentrate in `UkielTableProvider::scan` (`crates/ukiel-query/src/provider.rs` — read it first; line references below match its current state) plus a few writer lines. Two invariants must survive every change: **the `FilterExec` isolation backstop stays** (pushdown is a performance layer; the physical-plan filter remains the security boundary), and **pushed user filters are best-effort** (`supports_filters_pushdown` stays `Inexact`, DataFusion re-applies them above — so a filter that fails to convert is silently skipped, never an error).

**Tech Stack:** datafusion 54 / arrow+parquet 58.3 (pinned). DataFusion datasource APIs drift between majors — the code below matches the *executed* provider's API shapes (`ParquetSource::new(schema)`, `FileScanConfigBuilder::new(url, source)`); on any signature mismatch check docs.rs for the workspace's exact version and adapt minimally, keeping the plan shape.

**Prerequisites:** Plans 1–7 executed and green (provider has `physical_schema`/`query_schema`/`columns` from plan 7).

## Global Constraints

- Rust edition 2024, toolchain ≥ 1.96.
- Component tests via `make test` (`--test-threads=2`).
- **Every existing `ukiel-query` test must stay green after every task** — especially the isolation tests (`namespace_sees_only_its_rows_even_in_packed_files`, `projection_excluding_packing_key_still_isolates`, the alias tests, `information_schema` scoping). They are the regression net for this whole plan.
- Commit messages: conventional style, no Claude/AI attribution.
- Every task ends with `cargo fmt && cargo clippy --all-targets -- -D warnings` clean.

---

### Task 1: Perf smoke harness + recorded baseline

**Files:**
- Create: `crates/ukiel-query/tests/perf_smoke.rs`
- Modify: `docs/notes/2026-07-06-parquet-read-performance.md` (baseline column)

**Interfaces:**
- Produces: an `#[ignore]`d timing test printing median latencies for three scenarios. Not CI-gating; run manually before/after each task. Uses a **multi-row-group** fixture (the default writer produces one row group for small files, which would hide row-group pruning entirely).

- [ ] **Step 1: Write the harness**

Create `crates/ukiel-query/tests/perf_smoke.rs`:

```rust
//! Perf smoke: prints median latencies for the three Tier-1 scenarios.
//! Run manually: cargo test -p ukiel-query --test perf_smoke -- --ignored --nocapture
//! Record medians in docs/notes/2026-07-06-parquet-read-performance.md.

mod query_test;

use std::sync::Arc;
use std::time::{Duration, Instant};

use datafusion::arrow::array::{Int64Array, RecordBatch, StringArray};
use datafusion::prelude::SessionContext;
use object_store::path::Path;
use object_store::ObjectStore;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use serde_json::json;
use ukiel_core::{arrow_schema_from_json, CommitOp, NamespaceId, PartMeta};

use query_test::{setup, Harness, STORE_URL};

const TENANTS: i64 = 100;
const ROWS_PER_TENANT: i64 = 1_000;
const WIDE_COLS: usize = 8;

fn wide_schema_json() -> serde_json::Value {
    let mut fields = vec![
        json!({"name": "tenant_id", "type": "int64"}),
        json!({"name": "ts", "type": "timestamp_ms"}),
    ];
    for i in 0..WIDE_COLS {
        fields.push(json!({"name": format!("c{i}"), "type": "utf8"}));
    }
    json!({ "fields": fields })
}

/// One packed file: 100 tenants x 1000 rows, sorted by (tenant_id, ts),
/// max_row_group_size=1000 so each tenant occupies exactly one row group.
async fn build_packed_fixture(h: &Harness) -> NamespaceId {
    let schema = Arc::new(arrow_schema_from_json(&wide_schema_json()).unwrap());
    let ht = h
        .catalog
        .create_hypertable(
            "wide",
            &wide_schema_json(),
            &json!({"columns": []}),
            &["tenant_id".to_string(), "ts".to_string()],
            "tenant_id",
        )
        .await
        .unwrap();
    for t in 1..=TENANTS {
        h.catalog.create_logical_table(NamespaceId(t), "wide", ht).await.unwrap();
    }

    let total = (TENANTS * ROWS_PER_TENANT) as usize;
    let tenants: Int64Array =
        (0..total as i64).map(|i| Some(i / ROWS_PER_TENANT + 1)).collect();
    let ts: Int64Array = (0..total as i64).map(|i| Some(i % ROWS_PER_TENANT)).collect();
    let mut columns: Vec<Arc<dyn datafusion::arrow::array::Array>> =
        vec![Arc::new(tenants), Arc::new(ts)];
    for c in 0..WIDE_COLS {
        let col: StringArray = (0..total).map(|i| Some(format!("v{c}-{i}"))).collect();
        columns.push(Arc::new(col));
    }
    let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();

    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .set_max_row_group_size(ROWS_PER_TENANT as usize)
        .build();
    let mut bytes = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut bytes, schema, Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let path = format!("ht/{ht}/L1/packed-perf.parquet");
    let size = bytes.len() as i64;
    h.store.put(&Path::from(path.clone()), bytes.into()).await.unwrap();
    h.catalog
        .commit(
            ht,
            CommitOp::Add {
                parts: vec![PartMeta {
                    path,
                    partition_values: json!({}),
                    packing_key_min: 1,
                    packing_key_max: TENANTS,
                    row_count: total as i64,
                    size_bytes: size,
                    level: 1,
                    column_stats: None,
                }],
            },
            None,
        )
        .await
        .unwrap();
    NamespaceId(42) // any mid-range tenant
}

async fn median_ms(ctx: &SessionContext, sql: &str, iters: usize) -> f64 {
    // Warm-up (footer reads, cache effects).
    ctx.sql(sql).await.unwrap().collect().await.unwrap();
    let mut samples: Vec<Duration> = Vec::with_capacity(iters);
    for _ in 0..iters {
        let start = Instant::now();
        ctx.sql(sql).await.unwrap().collect().await.unwrap();
        samples.push(start.elapsed());
    }
    samples.sort();
    samples[iters / 2].as_secs_f64() * 1000.0
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "perf smoke: run manually with --nocapture and record medians"]
async fn perf_smoke() {
    let h = setup().await;
    let ns = build_packed_fixture(&h).await;
    let store_url = url::Url::parse(STORE_URL).unwrap();
    let ctx = ukiel_query::context::session_for_namespace(
        &h.catalog,
        ns,
        h.store.clone(),
        &store_url,
    )
    .await
    .unwrap();

    let selective = median_ms(&ctx, "SELECT count(*) AS n FROM wide", 10).await;
    let narrow = median_ms(&ctx, "SELECT c0 FROM wide", 10).await;
    let ordered = median_ms(&ctx, "SELECT ts FROM wide ORDER BY ts LIMIT 100", 10).await;

    println!("PERF selective_count_ms={selective:.1}");
    println!("PERF narrow_projection_ms={narrow:.1}");
    println!("PERF order_by_limit_ms={ordered:.1}");

    // Sanity, not perf: the count must be exactly this tenant's rows.
    let batches = ctx.sql("SELECT count(*) AS n FROM wide").await.unwrap().collect().await.unwrap();
    assert_eq!(query_test::all_i64(&batches, "n"), vec![ROWS_PER_TENANT]);
}
```

(If `mod query_test;` reuse triggers dead-code warnings, `tests/query_test.rs` already carries `#![allow(dead_code)]` from earlier plans; add it if missing.)

- [ ] **Step 2: Run and record the baseline**

Run: `cargo test -p ukiel-query --test perf_smoke -- --ignored --nocapture 2>&1 | grep PERF`
Copy the three medians into the **Baseline** column of the table at the bottom of `docs/notes/2026-07-06-parquet-read-performance.md`.

- [ ] **Step 3: Lint and commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add crates/ukiel-query docs/notes/2026-07-06-parquet-read-performance.md
git commit -m "test: perf smoke harness with recorded baseline"
```

---

### Task 2: Projection pushed into the scan

**Files:**
- Modify: `crates/ukiel-expr/src/lib.rs` (add `column_refs`)
- Modify: `crates/ukiel-query/src/provider.rs` (`scan`)
- Modify: `crates/ukiel-query/tests/query_test.rs`

**Interfaces:**
- Produces: `ukiel_expr::column_refs(sql: &str, schema: &Schema) -> Result<Vec<String>, ExprError>` — the physical columns an expression references.
- Changes `scan()`: the Parquet scan reads only `requested physical columns ∪ alias-referenced columns ∪ {packing key}`; the isolation filter and final projection operate on that reduced schema. Behavior (results) unchanged — all existing tests prove it.

- [ ] **Step 1: Add `column_refs` to ukiel-expr (with unit test)**

Append to `crates/ukiel-expr/src/lib.rs`:

```rust
/// The column names a SQL expression references (for projection planning).
pub fn column_refs(sql: &str, schema: &Schema) -> Result<Vec<String>, ExprError> {
    let ctx = SessionContext::new();
    let df_schema = DFSchema::try_from(schema.clone())?;
    let logical = ctx.parse_sql_expr(sql, &df_schema)?;
    let mut names: Vec<String> =
        logical.column_refs().into_iter().map(|c| c.name.clone()).collect();
    names.sort();
    names.dedup();
    Ok(names)
}
```

and to its `tests` module:

```rust
    #[test]
    fn reports_referenced_columns() {
        let cols = TableColumns::parse(&rich_schema_json()).unwrap();
        let refs = column_refs("amount * 2.0 + tenant_id", &cols.physical_schema()).unwrap();
        assert_eq!(refs, vec!["amount", "tenant_id"]);
    }
```

Run: `cargo test -p ukiel-expr` — expected: passes (implement directly; the behavior is pinned by the provider tests below).

- [ ] **Step 2: Rework `scan()`'s file scan + downstream schemas**

In `crates/ukiel-query/src/provider.rs`, replace the section of `scan()` from the `let files: Vec<PartitionedFile> = ...` statement through the `ProjectionExec` construction with:

```rust
        let files: Vec<PartitionedFile> = parts
            .iter()
            .map(|p| PartitionedFile::new(p.meta.path.clone(), p.meta.size_bytes as u64))
            .collect();

        // Requested query-schema indices (SELECT * when DataFusion sends none).
        let indices: Vec<usize> = match projection {
            Some(p) => p.clone(),
            None => (0..self.query_schema.fields().len()).collect(),
        };

        // Physical columns the scan must read: requested physical columns,
        // columns referenced by requested alias expressions, and the packing
        // key (the isolation filter needs it even when projected away).
        let mut needed: Vec<String> = vec![self.hypertable.packing_key.clone()];
        for &i in &indices {
            let spec = &self.columns.specs[i];
            match &spec.kind {
                ukiel_core::ColumnKind::Alias(sql) => {
                    let refs = ukiel_expr::column_refs(sql, &self.physical_schema)
                        .map_err(|e| DataFusionError::External(Box::new(e)))?;
                    needed.extend(refs);
                }
                _ => needed.push(spec.name.clone()),
            }
        }
        let mut scan_projection: Vec<usize> = needed
            .iter()
            .map(|name| self.physical_schema.index_of(name))
            .collect::<Result<Vec<_>, _>>()?;
        scan_projection.sort_unstable();
        scan_projection.dedup();
        let scanned_schema = Arc::new(self.physical_schema.project(&scan_projection)?);

        let source = Arc::new(ParquetSource::new(self.physical_schema.clone()));
        let config = FileScanConfigBuilder::new(self.store_url.clone(), source)
            .with_file_group(FileGroup::new(files))
            .with_projection(Some(scan_projection))
            .build();
        let scan: Arc<dyn ExecutionPlan> = DataSourceExec::from_data_source(config);

        // Mandatory namespace isolation, before projection can drop the column.
        // Built against the scanned (projected) schema.
        let predicate = binary(
            col(&self.hypertable.packing_key, &scanned_schema)?,
            Operator::Eq,
            lit(ScalarValue::Int64(Some(self.packing_key_value))),
            &scanned_schema,
        )?;
        let filtered: Arc<dyn ExecutionPlan> = Arc::new(FilterExec::try_new(predicate, scan)?);

        // Final projection maps requested query-schema columns onto the
        // scanned schema (alias-referenced columns are present by construction).
        let exprs = indices
            .iter()
            .map(|&i| {
                let field = self.query_schema.field(i);
                let spec = &self.columns.specs[i];
                let expr: Arc<dyn PhysicalExpr> = match &spec.kind {
                    ukiel_core::ColumnKind::Alias(sql) => {
                        let compiled = ukiel_expr::compile(sql, &scanned_schema, &spec.data_type)
                            .map_err(|e| DataFusionError::External(Box::new(e)))?;
                        compiled.into_physical()
                    }
                    _ => col(field.name(), &scanned_schema)?,
                };
                Ok((expr, field.name().clone()))
            })
            .collect::<DfResult<Vec<_>>>()?;
        let mut plan: Arc<dyn ExecutionPlan> = Arc::new(ProjectionExec::try_new(exprs, filtered)?);
```

(`FileScanConfigBuilder::with_projection(Option<Vec<usize>>)` — if the method name differs in the workspace's DataFusion, check docs.rs; the projection indices are into the source's schema, i.e. `physical_schema`.)

- [ ] **Step 3: Add the regression test**

Append to `crates/ukiel-query/tests/query_test.rs`:

```rust
#[tokio::test]
async fn narrow_projection_and_alias_on_unprojected_column() {
    let h = setup().await;
    let ctx = ctx_with_provider(&h, 1).await;

    // Narrow projection: only 'payload' requested; packing key is scanned
    // for isolation but not returned. Results identical to before.
    let batches =
        ctx.sql("SELECT payload FROM events ORDER BY ts").await.unwrap().collect().await.unwrap();
    assert_eq!(all_strings(&batches, "payload"), vec!["a1", "a2"]);
    assert_eq!(batches[0].num_columns(), 1);

    // count(*): empty projection still isolates correctly.
    let batches = ctx.sql("SELECT count(*) AS n FROM events").await.unwrap().collect().await.unwrap();
    assert_eq!(all_i64(&batches, "n"), vec![2]);
}
```

(Alias-referencing-unprojected-column is already covered by `alias_columns_compute_at_query_time` — `SELECT half` reads `amount` — which must stay green.)

- [ ] **Step 4: Run the full query suite**

Run: `cargo test -p ukiel-query -- --test-threads=2`
Expected: all pass, including all isolation and alias tests.

- [ ] **Step 5: Lint and commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add crates/ukiel-expr crates/ukiel-query
git commit -m "feat: push projection into the parquet scan"
```

---

### Task 3: Predicate pushdown into ParquetSource

**Files:**
- Modify: `crates/ukiel-query/src/provider.rs` (`scan`)
- Modify: `crates/ukiel-query/tests/query_test.rs`

**Interfaces:**
- Changes `scan()`: `ParquetSource` receives a pushdown predicate = isolation predicate AND every user filter that references only scanned physical columns (built against `physical_schema` — the file schema; pushdown predicates may reference unprojected columns, late materialization handles them). `supports_filters_pushdown` stays `Inexact`; the isolation `FilterExec` stays. Filters that fail to convert are skipped silently.

- [ ] **Step 1: Implement**

In `scan()`, rename the `_filters` parameter to `filters`, and insert before the `ParquetSource` construction:

```rust
        // Pushdown predicate for the Parquet reader: isolation AND every user
        // filter that converts cleanly. Built against the FILE schema
        // (physical_schema) — pushdown may reference unprojected columns.
        // Best-effort by design: anything skipped is re-applied by DataFusion
        // above (Inexact), and the FilterExec below remains the security
        // boundary regardless.
        let file_isolation = binary(
            col(&self.hypertable.packing_key, &self.physical_schema)?,
            Operator::Eq,
            lit(ScalarValue::Int64(Some(self.packing_key_value))),
            &self.physical_schema,
        )?;
        let physical_names: std::collections::HashSet<&str> =
            self.physical_schema.fields().iter().map(|f| f.name().as_str()).collect();
        let df_schema = datafusion::common::DFSchema::try_from(
            self.physical_schema.as_ref().clone(),
        )?;
        let mut pushdown = file_isolation;
        for filter in filters {
            let refs = filter.column_refs();
            if !refs.iter().all(|c| physical_names.contains(c.name.as_str())) {
                continue; // references an alias or unknown column: not pushable
            }
            match state.create_physical_expr(filter.clone(), &df_schema) {
                Ok(expr) => {
                    pushdown = binary(pushdown, Operator::And, expr, &self.physical_schema)?;
                }
                Err(_) => continue, // unconvertible filter: DataFusion re-applies it
            }
        }

        let source = Arc::new(
            ParquetSource::new(self.physical_schema.clone())
                .with_predicate(pushdown)
                .with_pushdown_filters(true)
                .with_reorder_filters(true),
        );
```

Also rename the `_state` parameter to `state`. (Exact `ParquetSource` builder-method names — `with_predicate`, `with_pushdown_filters`, `with_reorder_filters`, and whether `with_predicate` wants the schema — vary slightly across DataFusion versions; match docs.rs for the workspace version. Page-index pruning is on by default in this version's `TableParquetOptions`; if a `with_enable_page_index(true)` builder exists, set it explicitly.)

- [ ] **Step 2: Add the pruning regression test**

Append to `crates/ukiel-query/tests/query_test.rs` — a file with one row group per tenant, then assert via `EXPLAIN ANALYZE` that querying one tenant prunes the other's row group:

```rust
#[tokio::test]
async fn pushdown_prunes_row_groups_in_packed_files() {
    use datafusion::arrow::array::{Int64Array, StringArray};
    use datafusion::arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;
    use parquet::basic::{Compression, ZstdLevel};
    use parquet::file::properties::WriterProperties;

    let h = setup().await;
    // Two tenants, 4 rows each, max_row_group_size=4 -> one row group each.
    let schema = std::sync::Arc::new(
        ukiel_core::arrow_schema_from_json(&json!({"fields": [
            {"name": "tenant_id", "type": "int64"},
            {"name": "ts", "type": "timestamp_ms"},
            {"name": "payload", "type": "utf8"}
        ]}))
        .unwrap(),
    );
    let ht = h
        .catalog
        .create_hypertable("rg", &json!({"fields": [
            {"name": "tenant_id", "type": "int64"},
            {"name": "ts", "type": "timestamp_ms"},
            {"name": "payload", "type": "utf8"}
        ]}), &json!({"columns": []}), &["tenant_id".to_string(), "ts".to_string()], "tenant_id")
        .await
        .unwrap();
    h.catalog.create_logical_table(NamespaceId(1), "rg", ht).await.unwrap();

    let tenants = Int64Array::from(vec![1, 1, 1, 1, 2, 2, 2, 2]);
    let ts = Int64Array::from(vec![1, 2, 3, 4, 1, 2, 3, 4]);
    let payload = StringArray::from(vec!["a", "b", "c", "d", "e", "f", "g", "h"]);
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![std::sync::Arc::new(tenants), std::sync::Arc::new(ts), std::sync::Arc::new(payload)],
    )
    .unwrap();
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .set_max_row_group_size(4)
        .build();
    let mut bytes = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut bytes, schema, Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let path = format!("ht/{ht}/L1/two-groups.parquet");
    let size = bytes.len() as i64;
    h.store.put(&Path::from(path.clone()), bytes.into()).await.unwrap();
    h.catalog
        .commit(ht, CommitOp::Add { parts: vec![PartMeta {
            path,
            partition_values: json!({}),
            packing_key_min: 1,
            packing_key_max: 2,
            row_count: 8,
            size_bytes: size,
            level: 1,
            column_stats: None,
        }]}, None)
        .await
        .unwrap();

    let store_url = url::Url::parse(STORE_URL).unwrap();
    let ctx = ukiel_query::context::session_for_namespace(&h.catalog, NamespaceId(1), h.store.clone(), &store_url)
        .await
        .unwrap();

    // Correctness: namespace 1 sees exactly its 4 rows.
    let batches = ctx.sql("SELECT payload FROM rg ORDER BY ts").await.unwrap().collect().await.unwrap();
    assert_eq!(all_strings(&batches, "payload"), vec!["a", "b", "c", "d"]);

    // Pruning: tenant 2's row group is skipped by statistics.
    let batches =
        ctx.sql("EXPLAIN ANALYZE SELECT count(*) FROM rg").await.unwrap().collect().await.unwrap();
    let analyzed =
        datafusion::arrow::util::pretty::pretty_format_batches(&batches).unwrap().to_string();
    assert!(
        analyzed.contains("row_groups_pruned_statistics=1"),
        "expected one row group pruned; explain analyze was:\n{analyzed}"
    );
}
```

(If the metric is named differently in this DataFusion version, adjust the needle to the actual `row_groups_pruned...` metric shown in the failure output — the assertion message prints the whole plan for exactly that purpose.)

- [ ] **Step 3: Run the full query suite**

Run: `cargo test -p ukiel-query -- --test-threads=2`
Expected: all pass. The isolation tests double as proof that pushdown did not replace the `FilterExec` boundary.

- [ ] **Step 4: Lint and commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add crates/ukiel-query
git commit -m "feat: push isolation and user predicates into the parquet source"
```

---

### Task 4: Declared output ordering

**Files:**
- Modify: `crates/ukiel-ingest/src/writer.rs` (`finish_batch` writer props)
- Modify: `crates/ukiel-compactor/src/rewrite.rs` (`batch_to_parquet` signature + props)
- Modify: `crates/ukiel-compactor/src/compactor.rs`, `crates/ukiel-compactor/src/deletion.rs` (callers)
- Modify: `crates/ukiel-query/src/provider.rs` (`scan`)
- Modify: `crates/ukiel-query/tests/query_test.rs`

**Interfaces:**
- Writers declare `sorting_columns` in Parquet metadata (packing key, then ts / the sort key).
- `scan()` places **one file per `FileGroup`** (each file is internally sorted; separate groups keep DataFusion partition-ordering sound — files may overlap in key ranges, so they must not be concatenated into one ordered group) and declares `output_ordering` over whichever prefix of `(packing_key, sort_key...)` survives the scan projection.

- [ ] **Step 1: Writers declare sorting columns**

In `crates/ukiel-ingest/src/writer.rs` `finish_batch`, build the properties with sorting columns (packing key then ts — the exact order `decode_rows` sorts by):

```rust
    let sorting = vec![
        parquet::format::SortingColumn {
            column_idx: key_idx as i32,
            descending: false,
            nulls_first: false,
        },
        parquet::format::SortingColumn {
            column_idx: ts_idx as i32,
            descending: false,
            nulls_first: false,
        },
    ];
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .set_sorting_columns(Some(sorting))
        .build();
```

(`finish_batch` already resolves `key_idx`; resolve `ts_idx` the same way from the `_ts_column` parameter — rename it back to `ts_column`.)

In `crates/ukiel-compactor/src/rewrite.rs`, change `batch_to_parquet(batch)` to `batch_to_parquet(batch: &RecordBatch, sort_key: &[String])`, resolving each sort-key name to a column index for `sorting_columns` (skip names not in the batch schema). Update the two callers (`compactor.rs` `merge_group`, `deletion.rs` `delete_key`) to pass `&hypertable.sort_key`, and the rewrite unit test to pass the fixture's sort key.

- [ ] **Step 2: Provider declares ordering, one file per group**

In `scan()`, replace the `FileScanConfigBuilder` call with:

```rust
        // One group per file: each file is internally sorted, but files may
        // overlap in key ranges — concatenating them into one group would
        // falsely claim a total order. Separate groups also parallelize.
        let file_groups: Vec<FileGroup> =
            files.into_iter().map(|f| FileGroup::new(vec![f])).collect();

        // Declared ordering: the longest prefix of (sort_key) present in the
        // scanned schema. (The packing key is always scanned.)
        use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
        let mut sort_exprs: Vec<PhysicalSortExpr> = Vec::new();
        for name in &self.hypertable.sort_key {
            match col(name, &scanned_schema) {
                Ok(expr) => sort_exprs.push(PhysicalSortExpr::new_default(expr)),
                Err(_) => break, // column not scanned: ordering prefix ends here
            }
        }
        let mut builder = FileScanConfigBuilder::new(self.store_url.clone(), source)
            .with_file_groups(file_groups)
            .with_projection(Some(scan_projection));
        if !sort_exprs.is_empty() {
            builder = builder.with_output_ordering(vec![LexOrdering::new(sort_exprs)]);
        }
        let config = builder.build();
```

(`PhysicalSortExpr::new_default` = ascending, nulls last — matching how the writers sort. If `LexOrdering::new` / `with_output_ordering` shapes differ in this DataFusion version, check docs.rs; the semantic is "each partition is ordered by this lexicographic key".)

- [ ] **Step 3: Add the no-sort regression test**

Append to `crates/ukiel-query/tests/query_test.rs`:

```rust
#[tokio::test]
async fn order_by_sort_key_needs_no_sort_exec() {
    let h = setup().await;
    let ctx = ctx_with_provider(&h, 1).await;

    // With packing_key constant (isolation filter), file ordering
    // (tenant_id, ts) collapses to (ts): ORDER BY ts should not sort.
    let batches = ctx
        .sql("EXPLAIN SELECT ts FROM events ORDER BY ts")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let plan =
        datafusion::arrow::util::pretty::pretty_format_batches(&batches).unwrap().to_string();
    assert!(
        !plan.contains("SortExec"),
        "expected sort elimination via declared ordering; plan was:\n{plan}"
    );

    // And the results are still correct and ordered.
    let batches =
        ctx.sql("SELECT payload FROM events ORDER BY ts").await.unwrap().collect().await.unwrap();
    assert_eq!(all_strings(&batches, "payload"), vec!["a1", "a2"]);
}
```

If the `SortExec` assertion fails: first verify the plan shows the declared ordering on the scan node (`output_ordering=[...]`). If the ordering is declared but DataFusion still sorts, the constant-propagation collapse (`tenant_id = k` making `(tenant_id, ts)` ≡ `(ts)`) isn't firing in this version — keep the declared-ordering assertion (`plan.contains("output_ordering")`), change the SortExec assertion to a `// TODO(follow-up)` with a comment, and note it in the perf doc. Do **not** weaken the correctness assertion.

- [ ] **Step 4: Run everything**

Run: `cargo test -p ukiel-query -- --test-threads=2 && cargo test -p ukiel-compactor -- --test-threads=2 && cargo test -p ukiel-ingest -- --test-threads=1`
Expected: all green (writer-metadata changes are invisible to readers except as optimization).

- [ ] **Step 5: Lint and commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add crates/ukiel-ingest crates/ukiel-compactor crates/ukiel-query
git commit -m "feat: declared sort order end to end - writer metadata, per-file groups, scan ordering"
```

---

### Task 5: Measure, record, close out

**Files:**
- Modify: `docs/notes/2026-07-06-parquet-read-performance.md`
- Modify: `docs/superpowers/plans/2026-07-05-ukiel-v1-roadmap.md`

- [ ] **Step 1: Full verification**

Run: `cargo fmt --check && cargo clippy --all-targets -- -D warnings && make test`
Expected: all green. Optionally `make e2e` (scan changes are load-bearing for every scenario — cheap insurance).

- [ ] **Step 2: Re-run the perf smoke and record**

Run: `cargo test -p ukiel-query --test perf_smoke -- --ignored --nocapture 2>&1 | grep PERF`
Fill the **After plan 11** column in the perf note's results table. Expect: `selective_count` and `narrow_projection` substantially down (row-group pruning + column pruning); `order_by_limit` down if sort elimination fired. If any number regressed, investigate before closing the plan.

- [ ] **Step 3: Roadmap + note status**

Set plan 11's roadmap row to **Executed**; update the Tier-1 table's status in the perf note.

- [ ] **Step 4: Commit**

```bash
git add docs/
git commit -m "docs: record plan 11 perf results"
```

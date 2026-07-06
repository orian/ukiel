# Ukiel Plan 12: Catalog Stats Pruning (Tier-2 #4) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Tier-2 item #4 of `docs/notes/2026-07-06-parquet-read-performance.md`: writers populate `parts.column_stats` (per-column Int64 min/max), and the query provider extracts column bounds from query predicates and prunes **parts in the catalog query** — whole files eliminated before any object-store I/O.

**Architecture:** The catalog is a database in front of every file open — pruning there beats pruning in the reader. Writers (ingest flush, compaction, deletion rewrite) compute `{"col": {"min": i64, "max": i64}}` for every Int64 column of the batch they encode and store it in the part's `column_stats` JSONB. The provider walks the pushed-down filter expressions for simple Int64 comparisons (`ts >= X`, `ts < Y`, `ts = Z` — DataFusion splits conjunctions into separate filters before pushdown, so top-level matching suffices), merges them into per-column ranges, and calls a new `live_parts_pruned` that adds JSONB range clauses to the part query. **Parts without stats always survive** (all pre-existing parts have `column_stats: NULL`) — pruning is strictly an optimization, never a correctness dependency. Day-partition pruning is deliberately NOT implemented: part-level ts min/max subsumes it at equal granularity (noted as follow-up in the perf note).

**Tech Stack:** sqlx 0.9 (dynamic clauses via the house `sqlx::AssertSqlSafe` idiom), arrow 58.3 min/max kernels.

**Prerequisites:** Plans 1–7 executed. Plan 11 (scan-pushdown) is the intended predecessor but NOT required — see Global Constraints.

## Global Constraints

- Rust edition 2024, toolchain ≥ 1.96; datafusion 54 / arrow+parquet 58.3 / object_store 0.13 pinned.
- Component tests via `make test` (`--test-threads=2`).
- **Every existing test stays green after every task** — especially all isolation tests in `ukiel-query`.
- Conventional commits, no Claude/AI attribution; `cargo fmt && cargo clippy --all-targets -- -D warnings` clean per task.
- Adapt to the current state of `UkielTableProvider::scan` (`crates/ukiel-query/src/provider.rs`): if plan 11 executed, the `live_parts` call and the `filters` parameter are already in its reworked shape; if not, `scan` still matches its pre-plan-11 form (`_filters` unused). Either way this plan changes exactly one call (`live_parts` → `live_parts_pruned`) plus the range extraction; rename `_filters` to `filters` if needed.
- Dynamic SQL strings must use `sqlx::AssertSqlSafe(...)` (house idiom — see `crates/ukiel-catalog/src/gc.rs`).

---

### Task 1: Stats computation in ukiel-core + ingest writers populate them

**Files:**
- Create: `crates/ukiel-core/src/stats.rs`
- Modify: `crates/ukiel-core/src/lib.rs`
- Modify: `crates/ukiel-ingest/src/writer.rs` (`EncodedPart` gains `column_stats`; `finish_batch` computes it)
- Modify: `crates/ukiel-ingest/src/flusher.rs` (pass it through to `PartMeta`)
- Modify: `crates/ukiel-ingest/tests/flusher_test.rs`

**Interfaces:**
- Produces:
  - `ukiel_core::stats::int64_column_stats(batch: &RecordBatch) -> Option<serde_json::Value>` — `{"<col>": {"min": i64, "max": i64}}` for every non-empty Int64 column; `None` for an empty batch.
  - `EncodedPart` gains `pub column_stats: Option<serde_json::Value>`; `Flusher::flush` writes it into `PartMeta.column_stats`.

- [ ] **Step 1: Write the failing test**

Create `crates/ukiel-core/src/stats.rs`:

```rust
//! Per-part column statistics computed at write time and stored in the
//! catalog (`parts.column_stats`) for part-level pruning before any file I/O.

use arrow::array::{Array, Int64Array, RecordBatch};
use arrow::datatypes::DataType;

/// `{"col": {"min": i64, "max": i64}}` for every Int64 column with at least
/// one non-null value. `None` when nothing is collectable.
pub fn int64_column_stats(batch: &RecordBatch) -> Option<serde_json::Value> {
    todo!()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::StringArray;
    use arrow::datatypes::{Field, Schema};
    use serde_json::json;
    use std::sync::Arc;

    #[test]
    fn computes_min_max_for_int64_columns_only() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("tenant_id", DataType::Int64, false),
            Field::new("ts", DataType::Int64, true),
            Field::new("payload", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![7, 3, 5])),
                Arc::new(Int64Array::from(vec![Some(100), None, Some(50)])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap();

        let stats = int64_column_stats(&batch).unwrap();
        assert_eq!(
            stats,
            json!({
                "tenant_id": {"min": 3, "max": 7},
                "ts": {"min": 50, "max": 100}
            })
        );
    }

    #[test]
    fn empty_batch_yields_none() {
        let schema = Arc::new(Schema::new(vec![Field::new("ts", DataType::Int64, true)]));
        let batch = RecordBatch::new_empty(schema);
        assert!(int64_column_stats(&batch).is_none());
    }
}
```

Add to `crates/ukiel-core/src/lib.rs`:

```rust
pub mod stats;
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p ukiel-core stats`
Expected: panics at `todo!()`.

- [ ] **Step 3: Implement**

```rust
pub fn int64_column_stats(batch: &RecordBatch) -> Option<serde_json::Value> {
    let mut stats = serde_json::Map::new();
    for (i, field) in batch.schema().fields().iter().enumerate() {
        if field.data_type() != &DataType::Int64 {
            continue;
        }
        let Some(col) = batch.column(i).as_any().downcast_ref::<Int64Array>() else {
            continue;
        };
        let (Some(min), Some(max)) = (arrow::compute::min(col), arrow::compute::max(col)) else {
            continue; // all-null or empty column
        };
        stats.insert(
            field.name().clone(),
            serde_json::json!({ "min": min, "max": max }),
        );
    }
    if stats.is_empty() { None } else { Some(serde_json::Value::Object(stats)) }
}
```

- [ ] **Step 4: Thread through the ingest writer**

In `crates/ukiel-ingest/src/writer.rs`:
- Add `pub column_stats: Option<serde_json::Value>` to `EncodedPart`.
- In `finish_batch`, before constructing `EncodedPart`, compute `let column_stats = ukiel_core::stats::int64_column_stats(&batch);` and include it in the struct literal.
- Fix any struct-literal construction of `EncodedPart` in tests within this crate (add `column_stats: None` or the computed value as appropriate — grep for `EncodedPart {`).

In `crates/ukiel-ingest/src/flusher.rs`, in `flush`'s `PartMeta` construction, replace `column_stats: None,` with:

```rust
                column_stats: item.part.column_stats.clone(),
```

(Consume order: read it before `item.part.bytes` is moved, or clone as shown.)

- [ ] **Step 5: Assert stats land in the catalog**

In `crates/ukiel-ingest/tests/flusher_test.rs`, in `flush_writes_objects_and_commits_atomically`, add after the existing part assertions:

```rust
    let with_stats = parts.iter().find(|p| p.meta.row_count == 2).unwrap();
    let stats = with_stats.meta.column_stats.as_ref().expect("stats populated");
    assert_eq!(stats["ts"]["min"], serde_json::json!(1000));
    assert_eq!(stats["ts"]["max"], serde_json::json!(2000));
    assert_eq!(stats["tenant_id"]["min"], serde_json::json!(1));
    assert_eq!(stats["tenant_id"]["max"], serde_json::json!(2));
```

- [ ] **Step 6: Run, lint, commit**

Run: `cargo test -p ukiel-core && cargo test -p ukiel-ingest -- --test-threads=1`
Expected: all green (other crates untouched; `EncodedPart` consumers outside ingest use field access, not literals — `cargo build --workspace` confirms; if a query/compactor test constructs `EncodedPart` literally, add the field there too).

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add crates/ukiel-core crates/ukiel-ingest
git commit -m "feat: int64 column stats computed at ingest and stored on parts"
```

---

### Task 2: Compactor and deletion outputs carry stats

**Files:**
- Modify: `crates/ukiel-compactor/src/compactor.rs` (`merge_group` output `PartMeta`)
- Modify: `crates/ukiel-compactor/src/deletion.rs` (`delete_key` rewrite output `PartMeta`)
- Modify: `crates/ukiel-compactor/tests/compactor_test.rs`

**Interfaces:**
- Every rewrite output part gets `column_stats` from its output batch (`int64_column_stats`), so compaction upgrades stats coverage over time (organic backfill, same as materialized columns).

- [ ] **Step 1: Write the failing test**

Append to `crates/ukiel-compactor/tests/compactor_test.rs`:

```rust
#[tokio::test]
async fn compaction_outputs_carry_column_stats() {
    let env = setup().await;
    add_l0(&env, "d1", vec![json!({"tenant_id": 2, "ts": 10, "payload": "a"})]).await;
    add_l0(&env, "d1", vec![json!({"tenant_id": 5, "ts": 99, "payload": "b"})]).await;

    let compactor =
        Compactor::new(env.catalog.clone(), env.store.clone(), CompactorConfig::default());
    compactor.run_once().await.unwrap();

    let parts = env.catalog.live_parts(env.ht, None).await.unwrap();
    let l1 = parts.iter().find(|p| p.meta.level == 1).expect("merged L1 part");
    let stats = l1.meta.column_stats.as_ref().expect("stats on compaction output");
    assert_eq!(stats["ts"]["min"], json!(10));
    assert_eq!(stats["ts"]["max"], json!(99));
    assert_eq!(stats["tenant_id"]["min"], json!(2));
    assert_eq!(stats["tenant_id"]["max"], json!(5));
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p ukiel-compactor carry_column_stats -- --test-threads=2`
Expected: FAIL at `expect("stats on compaction output")` (compactor writes `column_stats: None`).

- [ ] **Step 3: Implement**

In `crates/ukiel-compactor/src/compactor.rs` (`merge_group`), in the output loop where `PartMeta` is built for each `(key_min, key_max, batch)`, replace `column_stats: None,` with:

```rust
                column_stats: ukiel_core::stats::int64_column_stats(batch),
```

In `crates/ukiel-compactor/src/deletion.rs` (`delete_key`), in the rewritten-part `PartMeta` construction, replace `column_stats: None,` with:

```rust
            column_stats: ukiel_core::stats::int64_column_stats(&kept),
```

- [ ] **Step 4: Run, lint, commit**

Run: `cargo test -p ukiel-compactor -- --test-threads=2`
Expected: all green including the new test.

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add crates/ukiel-compactor
git commit -m "feat: rewrite outputs carry column stats"
```

---

### Task 3: Catalog — `live_parts_pruned`

**Files:**
- Modify: `crates/ukiel-catalog/src/parts.rs`
- Modify: `crates/ukiel-core/src/part.rs` (or wherever fits: the `ColumnRange` type goes in `ukiel-core`)
- Modify: `crates/ukiel-catalog/tests/catalog_test.rs`

**Interfaces:**
- Produces:
  - `ukiel_core::ColumnRange { pub column: String, pub min: Option<i64>, pub max: Option<i64> }` (re-exported from `ukiel_core`) — "the query only needs rows where `column ∈ [min, max]`" (either bound optional).
  - `PostgresCatalog::live_parts_pruned(&self, hypertable_id, key: Option<i64>, ranges: &[ColumnRange]) -> Result<Vec<Part>, CatalogError>` — `live_parts` semantics plus: a part is excluded when its stats **prove** no overlap with a range; parts with missing stats (or stats missing that column) always survive.

- [ ] **Step 1: Add `ColumnRange` to ukiel-core**

In `crates/ukiel-core/src/part.rs`, append:

```rust
/// A per-column bound extracted from query predicates, used for part-level
/// pruning against `PartMeta::column_stats`. Either bound may be open.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnRange {
    pub column: String,
    pub min: Option<i64>,
    pub max: Option<i64>,
}
```

and re-export it in `lib.rs` (`pub use part::{ColumnRange, Part, PartMeta};`).

- [ ] **Step 2: Write the failing test**

Append to `crates/ukiel-catalog/tests/catalog_test.rs`:

```rust
use ukiel_core::ColumnRange;

fn part_meta_with_ts(path: &str, ts_min: i64, ts_max: i64) -> PartMeta {
    let mut meta = part_meta(path, 1, 10);
    meta.column_stats = Some(json!({"ts": {"min": ts_min, "max": ts_max}}));
    meta
}

#[tokio::test]
async fn live_parts_pruned_by_column_stats() {
    let (_pg, catalog) = common::setup().await;
    let ht = setup_hypertable(&catalog).await;
    catalog
        .commit(
            ht,
            CommitOp::Add {
                parts: vec![
                    part_meta_with_ts("early.parquet", 0, 100),
                    part_meta_with_ts("late.parquet", 1000, 2000),
                    part_meta("no-stats.parquet", 1, 10), // must always survive
                ],
            },
            None,
        )
        .await
        .unwrap();

    let range = |min, max| ColumnRange { column: "ts".to_string(), min, max };

    // ts <= 150: excludes 'late'.
    let parts =
        catalog.live_parts_pruned(ht, None, &[range(None, Some(150))]).await.unwrap();
    let paths: Vec<_> = parts.iter().map(|p| p.meta.path.as_str()).collect();
    assert_eq!(paths, vec!["early.parquet", "no-stats.parquet"]);

    // ts >= 500: excludes 'early'.
    let parts =
        catalog.live_parts_pruned(ht, None, &[range(Some(500), None)]).await.unwrap();
    let paths: Vec<_> = parts.iter().map(|p| p.meta.path.as_str()).collect();
    assert_eq!(paths, vec!["late.parquet", "no-stats.parquet"]);

    // 200..=500: only the stats-less part survives.
    let parts = catalog
        .live_parts_pruned(ht, None, &[range(Some(200), Some(500))])
        .await
        .unwrap();
    assert_eq!(parts.len(), 1);
    assert_eq!(parts[0].meta.path, "no-stats.parquet");

    // A range on a column with no stats anywhere prunes nothing.
    let parts = catalog
        .live_parts_pruned(
            ht,
            None,
            &[ColumnRange { column: "nope".to_string(), min: Some(1), max: Some(2) }],
        )
        .await
        .unwrap();
    assert_eq!(parts.len(), 3);

    // Empty ranges == live_parts.
    assert_eq!(catalog.live_parts_pruned(ht, None, &[]).await.unwrap().len(), 3);
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `cargo test -p ukiel-catalog pruned -- --test-threads=2`
Expected: compile error (`live_parts_pruned` not found).

- [ ] **Step 4: Implement**

Append to the `impl PostgresCatalog` block in `crates/ukiel-catalog/src/parts.rs`:

```rust
    /// `live_parts` plus stats-based pruning: a part is excluded only when its
    /// column_stats PROVE it cannot overlap a requested range. Missing stats
    /// (or a missing column entry) keep the part — pruning is an optimization,
    /// never a correctness dependency.
    pub async fn live_parts_pruned(
        &self,
        hypertable_id: HypertableId,
        key: Option<i64>,
        ranges: &[ukiel_core::ColumnRange],
    ) -> Result<Vec<Part>, CatalogError> {
        let mut sql = format!(
            "SELECT {PART_COLUMNS} FROM parts
             WHERE hypertable_id = $1 AND deleted_by_commit IS NULL
               AND ($2::BIGINT IS NULL OR (packing_key_min <= $2 AND packing_key_max >= $2))"
        );
        // Three binds per range: column name, min, max. NULL bind = open bound.
        for i in 0..ranges.len() {
            let col = 3 + i * 3; // column-name bind
            let min = col + 1;
            let max = col + 2;
            sql.push_str(&format!(
                " AND ( column_stats -> ${col} IS NULL
                        OR ( (${min}::BIGINT IS NULL
                              OR (column_stats -> ${col} ->> 'max')::bigint >= ${min})
                         AND (${max}::BIGINT IS NULL
                              OR (column_stats -> ${col} ->> 'min')::bigint <= ${max}) ) )"
            ));
        }
        sql.push_str(" ORDER BY id");

        let mut query = sqlx::query_as::<_, PartRow>(sqlx::AssertSqlSafe(sql));
        query = query.bind(hypertable_id.0).bind(key);
        for range in ranges {
            query = query.bind(&range.column).bind(range.min).bind(range.max);
        }
        let rows: Vec<PartRow> = query.fetch_all(&self.pool).await?;
        Ok(rows.into_iter().map(Part::from).collect())
    }
```

(`column_stats -> $col` uses the text bind as a JSONB key — Postgres `jsonb -> text` — so the column name is a parameter, never string-interpolated. Match `AssertSqlSafe` usage to the house pattern in this file / `gc.rs`.)

- [ ] **Step 5: Run, lint, commit**

Run: `cargo test -p ukiel-catalog -- --test-threads=2`
Expected: all green.

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add crates/ukiel-core crates/ukiel-catalog
git commit -m "feat: live_parts_pruned - stats-based part pruning in the catalog"
```

---

### Task 4: Provider extracts ranges from predicates and prunes

**Files:**
- Modify: `crates/ukiel-query/src/provider.rs`
- Modify: `crates/ukiel-query/tests/query_test.rs`

**Interfaces:**
- New private helper in `provider.rs`: `extract_int64_ranges(filters: &[Expr], schema: &Schema) -> Vec<ColumnRange>` — recognizes `col <op> literal` / `literal <op> col` for `=`, `<`, `<=`, `>`, `>=` on Int64 columns of the physical schema, merging multiple constraints per column to the tightest bounds. Anything unrecognized contributes nothing (never an error).
- `scan()` calls `live_parts_pruned(ht, Some(key), &ranges)` instead of `live_parts(...)`.

- [ ] **Step 1: Write the failing test**

Append to `crates/ukiel-query/tests/query_test.rs`:

```rust
#[tokio::test]
async fn catalog_prunes_parts_by_ts_stats() {
    let h = setup().await;
    // A second table with two parts in disjoint ts ranges, stats populated.
    let ht = h
        .catalog
        .create_hypertable("tsr", &events_schema(), &json!({"columns": []}), &["tenant_id".to_string(), "ts".to_string()], "tenant_id")
        .await
        .unwrap();
    h.catalog.create_logical_table(NamespaceId(1), "tsr", ht).await.unwrap();
    let schema = arrow_schema_from_json(&events_schema()).unwrap();

    for (name, ts_base, payload) in [("early", 100i64, "old"), ("late", 10_000i64, "new")] {
        let part = rows_to_parquet(
            &schema,
            "tenant_id",
            "ts",
            vec![json!({"tenant_id": 1, "ts": ts_base, "payload": payload})],
        )
        .unwrap();
        let path = format!("ht/{ht}/L0/{name}.parquet");
        let size = part.bytes.len() as i64;
        h.store.put(&Path::from(path.clone()), part.bytes.into()).await.unwrap();
        h.catalog
            .commit(ht, CommitOp::Add { parts: vec![PartMeta {
                path,
                partition_values: json!({}),
                packing_key_min: 1,
                packing_key_max: 1,
                row_count: 1,
                size_bytes: size,
                level: 0,
                column_stats: Some(json!({"ts": {"min": ts_base, "max": ts_base}})),
            }]}, None)
            .await
            .unwrap();
    }

    let store_url = url::Url::parse(STORE_URL).unwrap();
    let ctx = ukiel_query::context::session_for_namespace(&h.catalog, NamespaceId(1), h.store.clone(), &store_url)
        .await
        .unwrap();

    // Correctness: the predicate returns only the matching row.
    let batches = ctx
        .sql("SELECT payload FROM tsr WHERE ts < 5000")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(all_strings(&batches, "payload"), vec!["old"]);

    // Pruning: the physical plan's file listing contains only the early part.
    let batches = ctx
        .sql("EXPLAIN SELECT payload FROM tsr WHERE ts < 5000")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let plan =
        datafusion::arrow::util::pretty::pretty_format_batches(&batches).unwrap().to_string();
    assert!(plan.contains("early.parquet"), "plan should list the early part:\n{plan}");
    assert!(
        !plan.contains("late.parquet"),
        "late part must be pruned in the catalog, not scanned:\n{plan}"
    );
}
```

(Reuse the file's existing `events_schema()`/`rows_to_parquet` imports; add any missing `use` lines.)

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p ukiel-query catalog_prunes -- --test-threads=2`
Expected: correctness passes but the `!plan.contains("late.parquet")` assertion FAILS (no pruning yet).

- [ ] **Step 3: Implement**

Add to `crates/ukiel-query/src/provider.rs` (module level):

```rust
/// Extracts per-column Int64 bounds from pushed-down filters for catalog
/// part pruning. Conservative by construction: anything unrecognized simply
/// contributes no bound. DataFusion splits conjunctions before pushdown, so
/// top-level binary comparisons cover the practical cases.
fn extract_int64_ranges(
    filters: &[Expr],
    schema: &datafusion::arrow::datatypes::Schema,
) -> Vec<ukiel_core::ColumnRange> {
    use datafusion::logical_expr::{BinaryExpr, Operator};
    use std::collections::HashMap;

    let mut bounds: HashMap<String, (Option<i64>, Option<i64>)> = HashMap::new();
    let mut apply = |name: &str, op: &Operator, value: i64| {
        let Ok(field) = schema.field_with_name(name) else { return };
        if field.data_type() != &datafusion::arrow::datatypes::DataType::Int64 {
            return;
        }
        let entry = bounds.entry(name.to_string()).or_insert((None, None));
        match op {
            Operator::Eq => {
                entry.0 = Some(entry.0.map_or(value, |m| m.max(value)));
                entry.1 = Some(entry.1.map_or(value, |m| m.min(value)));
            }
            Operator::Gt => entry.0 = Some(entry.0.map_or(value + 1, |m| m.max(value + 1))),
            Operator::GtEq => entry.0 = Some(entry.0.map_or(value, |m| m.max(value))),
            Operator::Lt => entry.1 = Some(entry.1.map_or(value - 1, |m| m.min(value - 1))),
            Operator::LtEq => entry.1 = Some(entry.1.map_or(value, |m| m.min(value))),
            _ => {}
        }
    };

    for filter in filters {
        if let Expr::BinaryExpr(BinaryExpr { left, op, right }) = filter {
            match (left.as_ref(), right.as_ref()) {
                (Expr::Column(c), Expr::Literal(ScalarValue::Int64(Some(v)), _)) => {
                    apply(&c.name, op, *v)
                }
                (Expr::Literal(ScalarValue::Int64(Some(v)), _), Expr::Column(c)) => {
                    // literal <op> col: mirror the operator.
                    let mirrored = match op {
                        Operator::Lt => Operator::Gt,
                        Operator::LtEq => Operator::GtEq,
                        Operator::Gt => Operator::Lt,
                        Operator::GtEq => Operator::LtEq,
                        other => *other,
                    };
                    apply(&c.name, &mirrored, *v)
                }
                _ => {}
            }
        }
    }

    bounds
        .into_iter()
        .map(|(column, (min, max))| ukiel_core::ColumnRange { column, min, max })
        .collect()
}
```

(`Expr::Literal` arity: in this DataFusion version literals may carry metadata as a second tuple field — match the variant shape the compiler reports; if it is single-field, drop the `, _`.)

In `scan()`, replace the `live_parts(...)` call:

```rust
        let ranges = extract_int64_ranges(filters, &self.physical_schema);
        let parts = self
            .catalog
            .live_parts_pruned(self.hypertable.id, Some(self.packing_key_value), &ranges)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
```

(Rename the `_filters` parameter to `filters` if plan 11 has not already done so.)

- [ ] **Step 4: Run the full suite**

Run: `cargo test -p ukiel-query -- --test-threads=2 && cargo test -p ukiel-catalog -- --test-threads=2`
Expected: all green, including every isolation test.

- [ ] **Step 5: Lint and commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add crates/ukiel-query
git commit -m "feat: catalog part pruning from query predicates"
```

---

### Task 5: Measure, record, close out

**Files:**
- Modify: `docs/notes/2026-07-06-parquet-read-performance.md`
- Modify: `docs/superpowers/plans/2026-07-05-ukiel-v1-roadmap.md`

- [ ] **Step 1:** `cargo fmt --check && cargo clippy --all-targets -- -D warnings && make test` — all green.
- [ ] **Step 2:** If plan 11's perf smoke exists, run it (`cargo test -p ukiel-query --test perf_smoke -- --ignored --nocapture | grep PERF`) and fill the "After stats pruning" column in the perf note. (The smoke's single-file fixture won't show part pruning — note the numbers anyway for regression visibility; part pruning shows in the `catalog_prunes_parts_by_ts_stats` plan assertion.)
- [ ] **Step 3:** Perf note: mark Tier-2 #4 done; add a line noting day-partition pruning was subsumed (part-level ts stats give equal granularity). Roadmap: set plan 12's row to **Executed**.
- [ ] **Step 4:**

```bash
git add docs/
git commit -m "docs: record stats-pruning results"
```

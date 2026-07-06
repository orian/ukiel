# Ukiel Plan 7: Materialized Columns, Defaults & Aliases Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** ClickHouse-style column semantics: `default` (fill when the insert omits the column; stored), `materialized` (always computed from other columns; stored), `alias` (computed at query time; never stored) — declared as DataFusion SQL expression strings in the hypertable's schema JSON.

**Architecture:** Each kind has one evaluation point. **Defaults + materialized** are computed at ingest flush on the Arrow batch, and **recomputed on every compactor rewrite** — which gives organic backfill: a materialized column added later reads as NULL from old files (schema adaptation) and materializes as compaction touches each file, with no backfill job. This is why expressions must be **deterministic** (volatile functions like `random()`/`now()` are rejected at compile time). **Aliases** are expanded in the query provider's projection step. A new small crate `ukiel-expr` owns expression compile/evaluate so `ukiel-catalog` stays free of DataFusion.

Deliberate v1 divergences from ClickHouse (documented in the design note, Task 6): materialized and alias columns **are visible in `SELECT *`** (DataFusion has no hidden-column notion), and expressions may reference only plain stored / default columns — no chaining materialized-on-materialized.

**Tech Stack:** datafusion 54 (`parse_sql_expr`, `create_physical_expr`), arrow 58.3 kernels (`zip`, `new_null_array`).

**Prerequisites:** Plans 1–6 executed and green.

## Global Constraints

- Rust edition 2024, toolchain ≥ 1.96; datafusion 54 / arrow 58.3 / object_store 0.13 pinned together.
- Component tests via `make test` (`--test-threads=2`); pure expression/schema tests must pass without Docker.
- Commit messages: conventional style, no Claude/AI attribution.
- Every task ends with `cargo fmt && cargo clippy --all-targets -- -D warnings` clean.
- DataFusion API details may drift; on signature mismatch check docs.rs/datafusion/54.0.0 and adapt minimally, keeping the plan's structure.

---

### Task 1: Column specs in ukiel-core

**Files:**
- Modify: `crates/ukiel-core/src/schema.rs`
- Modify: `crates/ukiel-core/src/lib.rs`

**Interfaces:**
- Schema JSON grows optional, mutually-exclusive per-field attributes `default`, `materialized`, `alias` (DataFusion SQL expression strings).
- Produces:
  - `ColumnKind::{Stored, Default(String), Materialized(String), Alias(String)}`
  - `ColumnSpec { name: String, data_type: DataType, nullable: bool, kind: ColumnKind }`
  - `TableColumns { specs: Vec<ColumnSpec> }` with `parse(&Value) -> Result<TableColumns, SchemaError>`, `physical_schema() -> Schema` (stored + default + materialized — what Parquet files contain), `input_schema() -> Schema` (stored + default — what ingest rows may carry), `query_schema() -> Schema` (physical + alias — what SQL sees).
- Unchanged: `arrow_schema_from_json` keeps returning the **physical** schema (for legacy schemas without attributes it is identical to before), so existing callers keep compiling.

- [ ] **Step 1: Write the failing test**

Append to the `tests` module in `crates/ukiel-core/src/schema.rs`:

```rust
    fn rich_schema() -> serde_json::Value {
        json!({"fields": [
            {"name": "tenant_id", "type": "int64"},
            {"name": "ts", "type": "timestamp_ms"},
            {"name": "amount", "type": "float64", "default": "0.0"},
            {"name": "doubled", "type": "float64", "materialized": "amount * 2.0"},
            {"name": "half", "type": "float64", "alias": "amount / 2.0"}
        ]})
    }

    #[test]
    fn parses_column_kinds_and_derives_schemas() {
        let cols = TableColumns::parse(&rich_schema()).unwrap();
        assert!(matches!(cols.specs[0].kind, ColumnKind::Stored));
        assert!(matches!(cols.specs[2].kind, ColumnKind::Default(ref e) if e == "0.0"));
        assert!(matches!(cols.specs[3].kind, ColumnKind::Materialized(_)));
        assert!(matches!(cols.specs[4].kind, ColumnKind::Alias(_)));

        let names = |s: &Schema| s.fields().iter().map(|f| f.name().clone()).collect::<Vec<_>>();
        assert_eq!(names(&cols.input_schema()), vec!["tenant_id", "ts", "amount"]);
        assert_eq!(names(&cols.physical_schema()), vec!["tenant_id", "ts", "amount", "doubled"]);
        assert_eq!(
            names(&cols.query_schema()),
            vec!["tenant_id", "ts", "amount", "doubled", "half"]
        );

        // arrow_schema_from_json stays the physical schema.
        assert_eq!(arrow_schema_from_json(&rich_schema()).unwrap(), cols.physical_schema());
    }

    #[test]
    fn rejects_conflicting_kind_attributes() {
        let err = TableColumns::parse(&json!({"fields": [
            {"name": "x", "type": "int64", "default": "1", "materialized": "2"}
        ]}))
        .unwrap_err();
        assert!(matches!(err, SchemaError::Invalid(_)));
    }
```

Add the needed imports to the test module header if missing (`use super::*;` already covers the new types).

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p ukiel-core`
Expected: compile error (`TableColumns` not found).

- [ ] **Step 3: Implement**

In `crates/ukiel-core/src/schema.rs`, add above `arrow_schema_from_json`:

```rust
/// How a column gets its value. Expressions are DataFusion SQL strings over
/// this table's stored/default columns; they must be deterministic (enforced
/// by `ukiel-expr` at compile time).
#[derive(Debug, Clone, PartialEq)]
pub enum ColumnKind {
    /// Plain column: value comes from the insert.
    Stored,
    /// Value comes from the insert; NULL/missing slots are filled with the
    /// expression at ingest. Stored.
    Default(String),
    /// Always computed from other columns at write time (insert values are
    /// ignored) and recomputed on every rewrite. Stored.
    Materialized(String),
    /// Computed at query time. Never stored.
    Alias(String),
}

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnSpec {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub kind: ColumnKind,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TableColumns {
    pub specs: Vec<ColumnSpec>,
}

impl TableColumns {
    pub fn parse(v: &serde_json::Value) -> Result<Self, SchemaError> {
        let fields = v
            .get("fields")
            .and_then(|f| f.as_array())
            .ok_or_else(|| SchemaError::Invalid("missing 'fields' array".to_string()))?;

        let mut specs = Vec::with_capacity(fields.len());
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

            let expr_of = |key: &str| f.get(key).and_then(|e| e.as_str()).map(String::from);
            let kind = match (expr_of("default"), expr_of("materialized"), expr_of("alias")) {
                (None, None, None) => ColumnKind::Stored,
                (Some(e), None, None) => ColumnKind::Default(e),
                (None, Some(e), None) => ColumnKind::Materialized(e),
                (None, None, Some(e)) => ColumnKind::Alias(e),
                _ => {
                    return Err(SchemaError::Invalid(format!(
                        "field '{name}': default/materialized/alias are mutually exclusive"
                    )))
                }
            };
            specs.push(ColumnSpec { name: name.to_string(), data_type, nullable, kind });
        }
        Ok(TableColumns { specs })
    }

    fn schema_where(&self, include: impl Fn(&ColumnKind) -> bool) -> Schema {
        Schema::new(
            self.specs
                .iter()
                .filter(|s| include(&s.kind))
                .map(|s| Field::new(&s.name, s.data_type.clone(), s.nullable))
                .collect::<Vec<_>>(),
        )
    }

    /// What ingest rows may carry: stored + default columns.
    pub fn input_schema(&self) -> Schema {
        self.schema_where(|k| matches!(k, ColumnKind::Stored | ColumnKind::Default(_)))
    }

    /// What Parquet files contain: stored + default + materialized.
    pub fn physical_schema(&self) -> Schema {
        self.schema_where(|k| !matches!(k, ColumnKind::Alias(_)))
    }

    /// What SQL sees: physical + alias.
    pub fn query_schema(&self) -> Schema {
        self.schema_where(|_| true)
    }
}
```

Rewrite `arrow_schema_from_json` to delegate:

```rust
pub fn arrow_schema_from_json(v: &serde_json::Value) -> Result<Schema, SchemaError> {
    Ok(TableColumns::parse(v)?.physical_schema())
}
```

In `crates/ukiel-core/src/lib.rs`, extend the schema re-export:

```rust
pub use schema::{arrow_schema_from_json, ColumnKind, ColumnSpec, SchemaError, TableColumns};
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p ukiel-core && cargo build --workspace`
Expected: all core tests pass; workspace still compiles (legacy schemas have no attribute keys, so every existing caller sees identical schemas).

- [ ] **Step 5: Lint and commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add crates/ukiel-core
git commit -m "feat: column kinds (default/materialized/alias) in table schema"
```

---

### Task 2: ukiel-expr — compile, evaluate, determinism, batch application

**Files:**
- Modify: `Cargo.toml` (workspace root: member + dep)
- Create: `crates/ukiel-expr/Cargo.toml`
- Create: `crates/ukiel-expr/src/lib.rs`

**Interfaces:**
- Produces:
  - `compile(sql: &str, schema: &Schema, target: &DataType) -> Result<CompiledExpr, ExprError>` — parses a DataFusion SQL expression against `schema`, rejects volatile functions, casts to `target`.
  - `evaluate(expr: &CompiledExpr, batch: &RecordBatch) -> Result<ArrayRef, ExprError>`
  - `apply_defaults_and_materialized(batch: RecordBatch, cols: &TableColumns) -> Result<RecordBatch, ExprError>` — input: a batch with the **physical** schema (materialized/omitted slots NULL). Fills `Default` columns' NULL slots, then overwrites `Materialized` columns entirely. Used by ingest (Task 3) and the compactor (Task 4).
  - `validate_table_schema(v: &serde_json::Value) -> Result<(), ExprError>` — compiles every expression (defaults/materialized against the input schema, aliases against the physical schema); the operator calls this before `create_hypertable`. **The catalog does not validate expressions in v1** (keeping it DataFusion-free); invalid expressions otherwise surface as errors at first use.

- [ ] **Step 1: Scaffold**

Root `Cargo.toml`: add `"crates/ukiel-expr"` to `members` and to `[workspace.dependencies]`:

```toml
ukiel-expr = { path = "crates/ukiel-expr" }
```

Create `crates/ukiel-expr/Cargo.toml`:

```toml
[package]
name = "ukiel-expr"
version.workspace = true
edition.workspace = true

[dependencies]
ukiel-core = { workspace = true }
datafusion = { workspace = true }
arrow = { workspace = true }
thiserror = { workspace = true }
serde_json = { workspace = true }
```

- [ ] **Step 2: Write the failing test**

Create `crates/ukiel-expr/src/lib.rs` with the API stubbed and tests at the bottom:

```rust
//! Compile and evaluate deterministic DataFusion SQL expressions for
//! default/materialized/alias columns.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, RecordBatch};
use arrow::compute::kernels::zip::zip;
use arrow::compute::is_null;
use arrow::datatypes::{DataType, Schema};
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::DFSchema;
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_expr::{Expr, ExprSchemable, Volatility};
use datafusion::physical_expr::{create_physical_expr, PhysicalExpr};
use datafusion::prelude::SessionContext;
use ukiel_core::{ColumnKind, TableColumns};

#[derive(Debug, thiserror::Error)]
pub enum ExprError {
    #[error(transparent)]
    Schema(#[from] ukiel_core::SchemaError),
    #[error(transparent)]
    DataFusion(#[from] datafusion::error::DataFusionError),
    #[error(transparent)]
    Arrow(#[from] arrow::error::ArrowError),
    #[error("expression uses volatile function '{0}' — expressions must be deterministic")]
    Volatile(String),
}

pub struct CompiledExpr {
    physical: Arc<dyn PhysicalExpr>,
}

pub fn compile(sql: &str, schema: &Schema, target: &DataType) -> Result<CompiledExpr, ExprError> {
    todo!()
}

pub fn evaluate(expr: &CompiledExpr, batch: &RecordBatch) -> Result<ArrayRef, ExprError> {
    todo!()
}

pub fn apply_defaults_and_materialized(
    batch: RecordBatch,
    cols: &TableColumns,
) -> Result<RecordBatch, ExprError> {
    todo!()
}

pub fn validate_table_schema(v: &serde_json::Value) -> Result<(), ExprError> {
    todo!()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int64Array};
    use serde_json::json;

    fn rich_schema_json() -> serde_json::Value {
        json!({"fields": [
            {"name": "tenant_id", "type": "int64"},
            {"name": "amount", "type": "float64", "default": "0.0"},
            {"name": "doubled", "type": "float64", "materialized": "amount * 2.0"}
        ]})
    }

    /// Physical-schema batch as ingest sees it after JSON decoding:
    /// amount NULL in row 1 (omitted), doubled all-NULL (never in input).
    fn input_batch(cols: &TableColumns) -> RecordBatch {
        let schema = Arc::new(cols.physical_schema());
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(Float64Array::from(vec![Some(10.0), None])),
                Arc::new(Float64Array::from(vec![None::<f64>, None])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn compile_evaluate_and_cast() {
        let cols = TableColumns::parse(&rich_schema_json()).unwrap();
        let schema = cols.physical_schema();
        // Int expression cast to the declared float64 target.
        let e = compile("tenant_id + 1", &schema, &DataType::Float64).unwrap();
        let batch = input_batch(&cols);
        let arr = evaluate(&e, &batch).unwrap();
        let arr = arr.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(arr.values().as_ref(), &[2.0, 3.0]);
    }

    #[test]
    fn rejects_volatile_functions() {
        let cols = TableColumns::parse(&rich_schema_json()).unwrap();
        let err = compile("random()", &cols.physical_schema(), &DataType::Float64).unwrap_err();
        assert!(matches!(err, ExprError::Volatile(_)), "got {err:?}");
        // Unknown column is also a compile error (from DataFusion).
        assert!(compile("nope + 1", &cols.physical_schema(), &DataType::Float64).is_err());
    }

    #[test]
    fn fills_defaults_and_computes_materialized() {
        let cols = TableColumns::parse(&rich_schema_json()).unwrap();
        let out = apply_defaults_and_materialized(input_batch(&cols), &cols).unwrap();

        let amount = out.column(1).as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(amount.value(0), 10.0, "provided value wins over default");
        assert_eq!(amount.value(1), 0.0, "NULL slot filled with default");

        let doubled = out.column(2).as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(doubled.value(0), 20.0);
        assert_eq!(doubled.value(1), 0.0, "materialized sees the filled default");
    }

    #[test]
    fn validates_whole_table_schema() {
        assert!(validate_table_schema(&rich_schema_json()).is_ok());
        // Materialized referencing an alias (not in the input schema) fails.
        let bad = json!({"fields": [
            {"name": "a", "type": "int64"},
            {"name": "al", "type": "int64", "alias": "a + 1"},
            {"name": "m", "type": "int64", "materialized": "al + 1"}
        ]});
        assert!(validate_table_schema(&bad).is_err());
        // Volatile default fails.
        let bad = json!({"fields": [
            {"name": "a", "type": "float64", "default": "random()"}
        ]});
        assert!(matches!(validate_table_schema(&bad).unwrap_err(), ExprError::Volatile(_)));
    }
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `cargo test -p ukiel-expr`
Expected: panics at `todo!()`.

- [ ] **Step 4: Implement**

Replace the `todo!()` bodies:

```rust
pub fn compile(sql: &str, schema: &Schema, target: &DataType) -> Result<CompiledExpr, ExprError> {
    let ctx = SessionContext::new();
    let df_schema = DFSchema::try_from(schema.clone())?;
    let logical = ctx.parse_sql_expr(sql, &df_schema)?;
    reject_volatile(&logical)?;
    let logical = logical.cast_to(target, &df_schema)?;
    let physical = create_physical_expr(&logical, &df_schema, &ExecutionProps::new())?;
    Ok(CompiledExpr { physical })
}

fn reject_volatile(expr: &Expr) -> Result<(), ExprError> {
    let mut volatile: Option<String> = None;
    expr.apply(|e| {
        if let Expr::ScalarFunction(f) = e {
            if f.func.signature().volatility == Volatility::Volatile {
                volatile = Some(f.func.name().to_string());
                return Ok(TreeNodeRecursion::Stop);
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    match volatile {
        Some(name) => Err(ExprError::Volatile(name)),
        None => Ok(()),
    }
}

pub fn evaluate(expr: &CompiledExpr, batch: &RecordBatch) -> Result<ArrayRef, ExprError> {
    Ok(expr.physical.evaluate(batch)?.into_array(batch.num_rows())?)
}

pub fn apply_defaults_and_materialized(
    batch: RecordBatch,
    cols: &TableColumns,
) -> Result<RecordBatch, ExprError> {
    let schema = batch.schema();
    let mut columns: Vec<ArrayRef> = batch.columns().to_vec();

    // Defaults first (materialized expressions may reference them), always in
    // spec order. Expressions are compiled against the physical schema, so
    // they may reference stored and default columns.
    for spec in &cols.specs {
        let expr_sql = match &spec.kind {
            ColumnKind::Default(e) | ColumnKind::Materialized(e) => e,
            _ => continue,
        };
        let idx = schema.index_of(&spec.name)?;
        // Evaluate against the batch with columns filled so far.
        let current = RecordBatch::try_new(schema.clone(), columns.clone())?;
        let compiled = compile(expr_sql, &schema, &spec.data_type)?;
        let computed = evaluate(&compiled, &current)?;

        columns[idx] = match &spec.kind {
            ColumnKind::Materialized(_) => computed,
            ColumnKind::Default(_) => {
                let nulls = is_null(columns[idx].as_ref())?;
                // Where the input was NULL take the computed default, else keep.
                zip(&nulls, &computed, &columns[idx])?
            }
            _ => unreachable!(),
        };
    }
    Ok(RecordBatch::try_new(schema, columns)?)
}

pub fn validate_table_schema(v: &serde_json::Value) -> Result<(), ExprError> {
    let cols = TableColumns::parse(v)?;
    let input = cols.input_schema();
    let physical = cols.physical_schema();
    for spec in &cols.specs {
        match &spec.kind {
            ColumnKind::Stored => {}
            // Write-time expressions may only reference input columns.
            ColumnKind::Default(e) | ColumnKind::Materialized(e) => {
                compile(e, &input, &spec.data_type)?;
            }
            // Read-time expressions may reference anything stored.
            ColumnKind::Alias(e) => {
                compile(e, &physical, &spec.data_type)?;
            }
        }
    }
    Ok(())
}
```

Note the subtlety the test pins down: `apply_defaults_and_materialized` compiles write-time expressions against the **physical** schema (the batch it evaluates on) but `validate_table_schema` checks them against the **input** schema — this is what forbids a materialized column referencing another materialized/alias column while still letting it see filled defaults at runtime.

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test -p ukiel-expr`
Expected: 4 tests pass (no Docker).

- [ ] **Step 6: Lint and commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add Cargo.toml Cargo.lock crates/ukiel-expr
git commit -m "feat: ukiel-expr - deterministic expression compile/evaluate and batch application"
```

---

### Task 3: Ingest fills defaults and computes materialized columns

**Files:**
- Modify: `crates/ukiel-ingest/Cargo.toml` (add `ukiel-expr = { workspace = true }`)
- Modify: `crates/ukiel-ingest/src/writer.rs`
- Modify: `crates/ukiel-ingest/src/consumer.rs`
- Modify: `crates/ukiel-ingest/src/error.rs`
- Modify: `crates/ukiel-ingest/src/lib.rs`

**Interfaces:**
- Produces: `writer::encode_rows(cols: &TableColumns, packing_key: &str, ts_column: &str, rows: Vec<serde_json::Value>) -> Result<EncodedPart, IngestError>` — decode against the physical schema (materialized slots NULL, extra JSON fields ignored), apply defaults + materialized, sort, encode. The consumer switches to it.
- Unchanged: `rows_to_parquet(schema, packing_key, ts_column, rows)` keeps its exact signature and behavior (it is the plain-schema path used by existing tests, fixtures, and other crates' dev-dependencies) and becomes a thin delegate.

- [ ] **Step 1: Write the failing test**

Append to the `tests` module in `crates/ukiel-ingest/src/writer.rs`:

```rust
    #[test]
    fn encode_rows_fills_defaults_and_materialized() {
        let cols = ukiel_core::TableColumns::parse(&json!({"fields": [
            {"name": "tenant_id", "type": "int64"},
            {"name": "ts", "type": "timestamp_ms"},
            {"name": "amount", "type": "float64", "default": "0.0"},
            {"name": "doubled", "type": "float64", "materialized": "amount * 2.0"}
        ]}))
        .unwrap();

        let rows = vec![
            json!({"tenant_id": 1, "ts": 10, "amount": 5.0}),
            json!({"tenant_id": 1, "ts": 20}),                     // amount omitted -> default
            json!({"tenant_id": 1, "ts": 30, "doubled": 999.0}),   // materialized input ignored
        ];
        let part = encode_rows(&cols, "tenant_id", "ts", rows).unwrap();

        let reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(part.bytes))
            .unwrap()
            .build()
            .unwrap();
        let batch = reader.map(|b| b.unwrap()).next().unwrap();
        assert_eq!(batch.num_columns(), 4, "materialized column is stored");
        let amount: &arrow::array::Float64Array =
            batch.column(2).as_any().downcast_ref().unwrap();
        let doubled: &arrow::array::Float64Array =
            batch.column(3).as_any().downcast_ref().unwrap();
        assert_eq!(amount.values().as_ref(), &[5.0, 0.0, 0.0]);
        assert_eq!(doubled.values().as_ref(), &[10.0, 0.0, 0.0]);
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p ukiel-ingest encode_rows`
Expected: compile error (`encode_rows` not found).

- [ ] **Step 3: Implement**

Add to `crates/ukiel-ingest/Cargo.toml` `[dependencies]`:

```toml
ukiel-expr = { workspace = true }
```

Add a variant to `IngestError` in `crates/ukiel-ingest/src/error.rs`:

```rust
    #[error(transparent)]
    Expr(#[from] ukiel_expr::ExprError),
```

In `crates/ukiel-ingest/src/writer.rs`, refactor so both entry points share the core (keep all existing code paths intact):

```rust
use ukiel_core::TableColumns;

/// Column-spec-aware encoding: decode against the physical schema, fill
/// defaults, compute materialized columns, then sort + encode.
pub fn encode_rows(
    cols: &TableColumns,
    packing_key: &str,
    ts_column: &str,
    rows: Vec<serde_json::Value>,
) -> Result<EncodedPart, IngestError> {
    let physical = cols.physical_schema();
    let batch = decode_rows(&physical, packing_key, ts_column, rows)?;
    let batch = ukiel_expr::apply_defaults_and_materialized(batch, cols)?;
    finish_batch(batch, packing_key, ts_column)
}
```

and split the existing `rows_to_parquet` body into three private helpers it now calls:

- `decode_rows(schema, packing_key, ts_column, rows) -> Result<RecordBatch, IngestError>` — the existing empty-check, row validation (packing key + ts present as i64), and arrow-json decode.
- `finish_batch(batch, packing_key, ts_column) -> Result<EncodedPart, IngestError>` — sorting moves here, operating on the batch: reuse `ukiel_compactor`-style lexsort? **No** — keep the existing pre-decode row sort exactly as it is by sorting in `decode_rows` (as today), and let `finish_batch` only compute the key range and write Parquet. Materialized columns cannot be sort keys in v1 (they don't exist until after decode); note this limitation in the doc task.
- `rows_to_parquet(schema, packing_key, ts_column, rows)` — unchanged signature, now `decode_rows` + `finish_batch`.

Concretely, `writer.rs` after the refactor:

```rust
pub fn rows_to_parquet(
    schema: &Schema,
    packing_key: &str,
    ts_column: &str,
    rows: Vec<serde_json::Value>,
) -> Result<EncodedPart, IngestError> {
    let batch = decode_rows(schema, packing_key, ts_column, rows)?;
    finish_batch(batch, packing_key, ts_column)
}

fn decode_rows(
    schema: &Schema,
    packing_key: &str,
    ts_column: &str,
    mut rows: Vec<serde_json::Value>,
) -> Result<RecordBatch, IngestError> {
    if rows.is_empty() {
        return Err(IngestError::EmptyFlush);
    }
    let get = |row: &serde_json::Value, col: &str| row.get(col).and_then(|v| v.as_i64());
    for (i, row) in rows.iter().enumerate() {
        for col in [packing_key, ts_column] {
            if get(row, col).is_none() {
                return Err(IngestError::MissingColumn { row: i, column: col.to_string() });
            }
        }
    }
    rows.sort_by_key(|r| (get(r, packing_key).unwrap(), get(r, ts_column).unwrap()));

    let schema_ref = Arc::new(schema.clone());
    let mut decoder = ReaderBuilder::new(schema_ref).build_decoder()?;
    decoder.serialize(&rows)?;
    decoder.flush()?.ok_or(IngestError::EmptyFlush)
}

fn finish_batch(
    batch: RecordBatch,
    packing_key: &str,
    _ts_column: &str,
) -> Result<EncodedPart, IngestError> {
    let key_idx = batch
        .schema()
        .index_of(packing_key)
        .map_err(|_| IngestError::MissingColumn { row: 0, column: packing_key.to_string() })?;
    let keys = batch
        .column(key_idx)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .ok_or_else(|| IngestError::MissingColumn { row: 0, column: packing_key.to_string() })?;
    let key_min = arrow::compute::min(keys).ok_or(IngestError::EmptyFlush)?;
    let key_max = arrow::compute::max(keys).ok_or(IngestError::EmptyFlush)?;

    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .build();
    let mut bytes = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut bytes, batch.schema(), Some(props))?;
    writer.write(&batch)?;
    writer.close()?;

    Ok(EncodedPart { bytes, row_count: batch.num_rows() as i64, key_min, key_max })
}
```

(Adjust imports: `arrow::array::Int64Array`, `arrow::compute::{min, max}` join the existing use list; the old inline sort/min/max code is replaced by this split. Existing tests must still pass unchanged.)

Export `encode_rows` from `crates/ukiel-ingest/src/lib.rs`:

```rust
pub use writer::{encode_rows, rows_to_parquet, EncodedPart};
```

In `crates/ukiel-ingest/src/consumer.rs`, switch the flush path to specs: in `flush()`, replace

```rust
        let schema = arrow_schema_from_json(&hypertable.table_schema)?;
```

with

```rust
        let cols = ukiel_core::TableColumns::parse(&hypertable.table_schema)?;
```

and the `rows_to_parquet(&schema, ...)` call with

```rust
            let part = crate::writer::encode_rows(&cols, &hypertable.packing_key, &self.route.ts_column, rows)?;
```

(remove the now-unused `arrow_schema_from_json` import if nothing else uses it).

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p ukiel-ingest -- --test-threads=1`
Expected: all previous writer/flusher/consumer tests still pass (plain schemas take the same code path) plus the new one.

- [ ] **Step 5: Lint and commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add crates/ukiel-ingest
git commit -m "feat: ingest fills defaults and computes materialized columns"
```

---

### Task 4: Compactor — schema-adapting reads + recompute on rewrite (organic backfill)

**Files:**
- Modify: `crates/ukiel-compactor/Cargo.toml` (add `ukiel-expr = { workspace = true }`)
- Modify: `crates/ukiel-compactor/src/rewrite.rs`
- Modify: `crates/ukiel-compactor/src/compactor.rs`
- Modify: `crates/ukiel-compactor/src/error.rs`
- Modify: `crates/ukiel-compactor/tests/compactor_test.rs`

**Interfaces:**
- `rewrite::read_parts_to_batch` becomes **schema-adapting**: a file missing a target-schema column (written before the column existed) contributes NULLs for it instead of failing. This is required for any additive schema evolution, not just materialized columns.
- `Compactor::merge_group` recomputes materialized columns (and fills defaults) on the merged batch before sorting — old files' NULLs get materialized during normal compaction: **backfill happens organically, no backfill job**.
- `deletion::delete_key`'s rewrite path inherits both behaviors automatically (it goes through `read_parts_to_batch`; add the recompute call there too).

- [ ] **Step 1: Write the failing test**

Append to `crates/ukiel-compactor/tests/compactor_test.rs`:

```rust
#[tokio::test]
async fn compaction_backfills_materialized_columns_added_later() {
    let env = setup().await;

    // Write two L0 files with the ORIGINAL schema (no materialized column) —
    // these are "old files" from before the column existed.
    add_l0(&env, "d1", vec![json!({"tenant_id": 1, "ts": 5, "payload": "a"})]).await;
    add_l0(&env, "d1", vec![json!({"tenant_id": 1, "ts": 6, "payload": "b"})]).await;

    // Schema evolves: a materialized column is added (additive, nullable).
    let evolved = json!({"fields": [
        {"name": "tenant_id", "type": "int64"},
        {"name": "ts", "type": "timestamp_ms"},
        {"name": "payload", "type": "utf8"},
        {"name": "ts_plus_key", "type": "int64", "materialized": "ts + tenant_id"}
    ]});
    sqlx_update_schema(&env, &evolved).await;

    let compactor =
        Compactor::new(env.catalog.clone(), env.store.clone(), CompactorConfig::default());
    let stats = compactor.run_once().await.unwrap();
    assert_eq!(stats.merged_groups, 1);

    // The L1 file physically contains the backfilled materialized column.
    let ht = env.catalog.get_hypertable("events").await.unwrap();
    let schema = std::sync::Arc::new(
        ukiel_core::TableColumns::parse(&ht.table_schema).unwrap().physical_schema(),
    );
    let parts = env.catalog.live_parts(env.ht, None).await.unwrap();
    assert_eq!(parts.len(), 1);
    let batch = rewrite::read_parts_to_batch(&env.store, schema, &parts).await.unwrap();
    let idx = batch.schema().index_of("ts_plus_key").unwrap();
    let col: &arrow::array::Int64Array = batch.column(idx).as_any().downcast_ref().unwrap();
    assert_eq!(col.values().as_ref(), &[6, 7], "ts + tenant_id, backfilled and sorted");
}
```

and add this helper near the other helpers in the same file (v1 has no ALTER API — the test pokes the schema column directly, which is exactly what a future `alter_hypertable_add_column` will do):

```rust
pub async fn sqlx_update_schema(env: &Env, schema: &serde_json::Value) {
    // Direct DB update: stands in for the future alter_hypertable_add_column.
    sqlx::query("UPDATE hypertables SET table_schema = $1 WHERE id = $2")
        .bind(schema)
        .bind(env.ht.0)
        .execute(env.catalog.pool_for_tests())
        .await
        .unwrap();
}
```

This needs a test-only accessor on the catalog. Add to `crates/ukiel-catalog/src/lib.rs` inside `impl PostgresCatalog`:

```rust
    /// Raw pool access for tests and migrations-adjacent tooling.
    /// Not part of the stable API.
    pub fn pool_for_tests(&self) -> &sqlx::PgPool {
        &self.pool
    }
```

and add `sqlx = { workspace = true }` to `crates/ukiel-compactor/Cargo.toml` `[dev-dependencies]`.

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p ukiel-compactor backfills -- --test-threads=2`
Expected: after compiling, the test fails inside `read_parts_to_batch` (schema mismatch: old files lack `ts_plus_key`) — or at `concat_batches`. That failure is the point.

- [ ] **Step 3: Implement**

Add to `crates/ukiel-compactor/Cargo.toml` `[dependencies]`:

```toml
ukiel-expr = { workspace = true }
```

Add a variant to `CompactorError` in `crates/ukiel-compactor/src/error.rs`:

```rust
    #[error(transparent)]
    Expr(#[from] ukiel_expr::ExprError),
```

In `crates/ukiel-compactor/src/rewrite.rs`, make reading schema-adapting — replace the body of `read_parts_to_batch`:

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
            batches.push(adapt_to_schema(batch?, &schema)?);
        }
    }
    Ok(concat_batches(&schema, &batches)?)
}

/// Adapts a file batch to the target schema: columns the file predates are
/// filled with NULLs (additive schema evolution).
fn adapt_to_schema(batch: RecordBatch, target: &SchemaRef) -> Result<RecordBatch, CompactorError> {
    let columns = target
        .fields()
        .iter()
        .map(|field| match batch.schema().index_of(field.name()) {
            Ok(idx) => Ok(batch.column(idx).clone()),
            Err(_) => Ok(arrow::array::new_null_array(field.data_type(), batch.num_rows())),
        })
        .collect::<Result<Vec<_>, CompactorError>>()?;
    Ok(RecordBatch::try_new(target.clone(), columns)?)
}
```

(add `use arrow::array::new_null_array;`-style import or call it fully qualified as shown).

In `crates/ukiel-compactor/src/compactor.rs`, recompute during merges — in `merge_group`, replace:

```rust
        let schema = Arc::new(arrow_schema_from_json(&hypertable.table_schema)?);
        let merged = rewrite::read_parts_to_batch(&self.store, schema, group).await?;
        let sorted = rewrite::sort_batch(&merged, &hypertable.sort_key)?;
```

with:

```rust
        let cols = ukiel_core::TableColumns::parse(&hypertable.table_schema)?;
        let schema = Arc::new(cols.physical_schema());
        let merged = rewrite::read_parts_to_batch(&self.store, schema, group).await?;
        // Rewrites recompute write-time columns: this is what backfills a
        // materialized column added after these files were written.
        let merged = ukiel_expr::apply_defaults_and_materialized(merged, &cols)?;
        let sorted = rewrite::sort_batch(&merged, &hypertable.sort_key)?;
```

(`TableColumns::parse` returns `SchemaError`, already convertible via `CompactorError::Schema`.)

In `crates/ukiel-compactor/src/deletion.rs`, apply the same recompute after reading — replace:

```rust
        let batch =
            rewrite::read_parts_to_batch(store, schema.clone(), std::slice::from_ref(part))
                .await?;
```

with:

```rust
        let batch =
            rewrite::read_parts_to_batch(store, schema.clone(), std::slice::from_ref(part))
                .await?;
        let batch = ukiel_expr::apply_defaults_and_materialized(batch, &cols)?;
```

adding `let cols = ukiel_core::TableColumns::parse(&hypertable.table_schema)?;` next to the existing schema construction in `delete_key` (and build `schema` from `cols.physical_schema()` instead of `arrow_schema_from_json`).

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p ukiel-compactor -- --test-threads=2`
Expected: all previous compactor/deletion tests pass (plain schemas: `apply_defaults_and_materialized` is a no-op) plus the backfill test.

- [ ] **Step 5: Lint and commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add crates/ukiel-catalog crates/ukiel-compactor
git commit -m "feat: schema-adapting rewrites recompute materialized columns (organic backfill)"
```

---

### Task 5: Query — alias expansion in the provider

**Files:**
- Modify: `crates/ukiel-query/Cargo.toml` (add `ukiel-expr = { workspace = true }`)
- Modify: `crates/ukiel-query/src/provider.rs`
- Modify: `crates/ukiel-query/src/error.rs`
- Modify: `crates/ukiel-query/tests/query_test.rs`

**Interfaces:**
- `UkielTableProvider::schema()` returns the **query schema** (physical + alias fields). `scan()` still reads files with the physical schema and injects the isolation filter first; the projection step then maps each requested column to either a physical `col(...)` or the alias's compiled physical expression. When DataFusion requests no projection (`SELECT *`), the provider appends a projection computing every alias so the output matches `schema()`.

- [ ] **Step 1: Write the failing test**

Append to `crates/ukiel-query/tests/query_test.rs`:

```rust
#[tokio::test]
async fn alias_columns_compute_at_query_time() {
    let h = setup().await;
    // A second hypertable with default/materialized/alias columns.
    let rich = json!({"fields": [
        {"name": "tenant_id", "type": "int64"},
        {"name": "ts", "type": "timestamp_ms"},
        {"name": "amount", "type": "float64", "default": "0.0"},
        {"name": "doubled", "type": "float64", "materialized": "amount * 2.0"},
        {"name": "half", "type": "float64", "alias": "amount / 2.0"}
    ]});
    let ht = h
        .catalog
        .create_hypertable("rich", &rich, &json!({"columns": []}), &["tenant_id".to_string(), "ts".to_string()], "tenant_id")
        .await
        .unwrap();
    h.catalog.create_logical_table(NamespaceId(1), "rich", ht).await.unwrap();

    // One file via the spec-aware encoder (amount omitted in row 2).
    let cols = ukiel_core::TableColumns::parse(&rich).unwrap();
    let part = ukiel_ingest::encode_rows(
        &cols,
        "tenant_id",
        "ts",
        vec![
            json!({"tenant_id": 1, "ts": 1, "amount": 8.0}),
            json!({"tenant_id": 1, "ts": 2}),
        ],
    )
    .unwrap();
    let path = format!("ht/{ht}/L0/rich.parquet");
    let size = part.bytes.len() as i64;
    h.store.put(&Path::from(path.clone()), part.bytes.into()).await.unwrap();
    h.catalog
        .commit(
            ht,
            CommitOp::Add {
                parts: vec![PartMeta {
                    path,
                    partition_values: json!({}),
                    packing_key_min: part.key_min,
                    packing_key_max: part.key_max,
                    row_count: part.row_count,
                    size_bytes: size,
                    level: 0,
                    column_stats: None,
                }],
            },
            None,
        )
        .await
        .unwrap();

    let store_url = url::Url::parse(STORE_URL).unwrap();
    let ctx = ukiel_query::context::session_for_namespace(
        &h.catalog,
        NamespaceId(1),
        h.store.clone(),
        &store_url,
    )
    .await
    .unwrap();

    // Alias computed at read time; default and materialized read from storage.
    let batches = ctx
        .sql("SELECT amount, doubled, half FROM rich ORDER BY ts")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let amounts = all_f64(&batches, "amount");
    let doubled = all_f64(&batches, "doubled");
    let half = all_f64(&batches, "half");
    assert_eq!(amounts, vec![8.0, 0.0]);
    assert_eq!(doubled, vec![16.0, 0.0]);
    assert_eq!(half, vec![4.0, 0.0]);

    // SELECT * includes alias and materialized columns (documented divergence
    // from ClickHouse) and still works.
    let batches = ctx.sql("SELECT * FROM rich").await.unwrap().collect().await.unwrap();
    assert_eq!(batches[0].num_columns(), 5);

    // Filtering on an alias works (DataFusion applies it above our scan).
    let batches = ctx
        .sql("SELECT ts FROM rich WHERE half > 1.0")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(all_i64(&batches, "ts"), vec![1]);
}
```

and add this helper next to `all_i64`/`all_strings`:

```rust
pub fn all_f64(batches: &[RecordBatch], column: &str) -> Vec<f64> {
    let mut out = Vec::new();
    for b in batches {
        let idx = b.schema().index_of(column).unwrap();
        let arr: &datafusion::arrow::array::Float64Array =
            b.column(idx).as_any().downcast_ref().unwrap();
        for i in 0..arr.len() {
            out.push(arr.value(i));
        }
    }
    out
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p ukiel-query alias -- --test-threads=2`
Expected: fails at planning ("column half not found") — the provider's schema doesn't include alias fields yet.

- [ ] **Step 3: Implement**

Add to `crates/ukiel-query/Cargo.toml` `[dependencies]`:

```toml
ukiel-expr = { workspace = true }
```

Add a variant to `QueryError` in `crates/ukiel-query/src/error.rs`:

```rust
    #[error(transparent)]
    Expr(#[from] ukiel_expr::ExprError),
```

Rework `crates/ukiel-query/src/provider.rs`:

1. Store the parsed specs and both schemas:

```rust
pub struct UkielTableProvider {
    catalog: PostgresCatalog,
    hypertable: Hypertable,
    /// stored + default + materialized — what the Parquet scan produces.
    physical_schema: SchemaRef,
    /// physical + alias — what SQL sees.
    query_schema: SchemaRef,
    columns: ukiel_core::TableColumns,
    packing_key_value: i64,
    store_url: ObjectStoreUrl,
}
```

2. In `try_new`, build both from `TableColumns::parse(&hypertable.table_schema)?` (replacing the `arrow_schema_from_json` call).

3. `schema()` returns `self.query_schema.clone()`.

4. In `scan()`:
   - Build the file scan and the isolation `FilterExec` exactly as today, but against `self.physical_schema` (the packing key is always physical).
   - Replace the projection block with one that always runs (aliases require it even for `SELECT *`):

```rust
        // Projection maps each requested query-schema column to either a
        // physical column or the alias's compiled expression. Runs even for
        // SELECT * so alias columns exist in the output.
        let indices: Vec<usize> = match projection {
            Some(p) => p.clone(),
            None => (0..self.query_schema.fields().len()).collect(),
        };
        let exprs = indices
            .iter()
            .map(|&i| {
                let field = self.query_schema.field(i);
                let spec = &self.columns.specs[i];
                let expr: Arc<dyn PhysicalExpr> = match &spec.kind {
                    ukiel_core::ColumnKind::Alias(sql) => {
                        let compiled =
                            ukiel_expr::compile(sql, &self.physical_schema, &spec.data_type)
                                .map_err(|e| DataFusionError::External(Box::new(e)))?;
                        compiled.into_physical()
                    }
                    _ => col(field.name(), &self.physical_schema)?,
                };
                Ok((expr, field.name().clone()))
            })
            .collect::<DfResult<Vec<_>>>()?;
        let mut plan: Arc<dyn ExecutionPlan> = Arc::new(ProjectionExec::try_new(exprs, filtered)?);
```

   (the `if let Some(indices) = projection` block is gone; the limit handling stays). Note `TableColumns.specs` is index-aligned with `query_schema` because `query_schema()` includes every spec in order.

5. Add `use datafusion::physical_expr::PhysicalExpr;` to the imports.

Expose the compiled physical expression from `ukiel-expr` — add to `crates/ukiel-expr/src/lib.rs`:

```rust
impl CompiledExpr {
    /// The underlying DataFusion physical expression (for planner embedding).
    pub fn into_physical(self) -> Arc<dyn PhysicalExpr> {
        self.physical
    }
}
```

(with `pub use datafusion::physical_expr::PhysicalExpr;` re-export if needed for the signature; `Arc` is already imported there.)

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p ukiel-query -- --test-threads=2`
Expected: all previous query/server/cache tests pass (plain schemas: query schema == physical schema, projection now always present but semantically identical) plus the alias test.

- [ ] **Step 5: Lint and commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add crates/ukiel-expr crates/ukiel-query
git commit -m "feat: alias columns computed at query time in the table provider"
```

---

### Task 6: Full verification + design note + docs

**Files:**
- Create: `docs/notes/2026-07-06-materialized-columns.md`
- Modify: `README.md`
- Modify: `docs/notes/schema-management.md`
- Modify: `docs/superpowers/plans/2026-07-05-ukiel-v1-roadmap.md`

- [ ] **Step 1: Run the full suite**

Run: `cargo fmt --check && cargo clippy --all-targets -- -D warnings && make test`
Expected: all crates green (Docker running). Optionally run `make e2e` (plain schemas are untouched, but it's cheap insurance).

- [ ] **Step 2: Write `docs/notes/2026-07-06-materialized-columns.md`**

```markdown
# Materialized columns, defaults & aliases

ClickHouse-style column semantics, mapped onto Ukiel's pipeline. Declared as
DataFusion SQL expression strings in the hypertable schema JSON:

    {"fields": [
      {"name": "ts",         "type": "timestamp_ms"},
      {"name": "tenant_id",  "type": "int64"},
      {"name": "amount",     "type": "float64", "default": "0.0"},
      {"name": "doubled",    "type": "float64", "materialized": "amount * 2.0"},
      {"name": "half",       "type": "float64", "alias": "amount / 2.0"}
    ]}

| Kind | Semantics | Evaluated | Stored |
|---|---|---|---|
| `default` | fills NULL/omitted insert slots | ingest flush (+ rewrites) | yes |
| `materialized` | always computed; insert values ignored | ingest flush + **every rewrite** | yes |
| `alias` | computed per query | provider projection | no |

Three schemas derive from one spec list (`ukiel_core::TableColumns`):
**input** (stored + default — what ingest rows may carry), **physical**
(+ materialized — what Parquet files contain), **query** (+ alias — what SQL
sees).

Rules:

- **Deterministic expressions only** — `random()`/`now()` are rejected at
  compile time (`ukiel-expr`). Required because rewrites re-evaluate
  expressions; volatile ones would make compaction change data.
- Expressions reference only stored/default columns (no chaining on other
  materialized/alias columns). Write-time expressions are validated against
  the input schema; aliases against the physical schema.
- Materialized columns cannot be sort keys in v1 (ingest sorts before
  computing them).
- Validation: call `ukiel_expr::validate_table_schema` before
  `create_hypertable` — the catalog itself does not validate expressions (it
  stays DataFusion-free); bad expressions otherwise fail at first use.

**Organic backfill.** Adding a materialized column later needs no backfill
job: old files read as NULL for it (schema-adapting reads fill missing
columns), and every compactor rewrite recomputes write-time columns — so the
column materializes as compaction naturally touches each file.

**Divergences from ClickHouse (v1):** materialized and alias columns are
visible in `SELECT *` (DataFusion has no hidden columns); defaults apply only
to NULL/omitted values, and MATERIALIZED silently ignores (rather than
rejects) values supplied by the insert.

GDPR note: materialized values derived from PII are stored bytes; key
deletion rewrites whole rows, so they are erased with everything else.
```

- [ ] **Step 3: Update `docs/notes/schema-management.md`**

In the "Evolution" section, after the constraint list, add:

```markdown
Column kinds (`default` / `materialized` / `alias` expressions) are documented
in [2026-07-06-materialized-columns.md](2026-07-06-materialized-columns.md). Note that compactor
reads are schema-adapting (old files contribute NULLs for later-added
columns) and rewrites recompute write-time expressions — additive evolution
plus organic backfill already works at the storage layer; only the
`alter_hypertable_add_column` API with validation remains future work.
```

- [ ] **Step 4: Update `README.md` and the roadmap**

README crate list, after `ukiel-gc` (or `ukiel-compactor` if GC hasn't merged yet):

```markdown
- `crates/ukiel-expr` — deterministic SQL expression engine for column
  specs: `default` / `materialized` (computed at write + recomputed on
  rewrites = organic backfill) / `alias` (computed at query time). See
  `docs/notes/2026-07-06-materialized-columns.md`.
```

Roadmap: set plan 7's status to **Executed**.

- [ ] **Step 5: Commit**

```bash
git add docs/ README.md
git commit -m "docs: materialized columns design note and crate overview"
```

//! Compile and evaluate deterministic DataFusion SQL expressions for
//! default/materialized/alias columns.

use std::sync::Arc;

use arrow::array::{ArrayRef, RecordBatch};
use arrow::compute::is_null;
use arrow::compute::kernels::zip::zip;
use arrow::datatypes::{DataType, Schema};
use datafusion::common::DFSchema;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_expr::{Expr, ExprSchemable, Volatility};
use datafusion::physical_expr::{PhysicalExpr, create_physical_expr};
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

#[derive(Debug)]
pub struct CompiledExpr {
    physical: Arc<dyn PhysicalExpr>,
}

impl CompiledExpr {
    /// The underlying DataFusion physical expression (for planner embedding).
    pub fn into_physical(self) -> Arc<dyn PhysicalExpr> {
        self.physical
    }
}

/// The column names a SQL expression references (for projection planning).
pub fn column_refs(sql: &str, schema: &Schema) -> Result<Vec<String>, ExprError> {
    let ctx = SessionContext::new();
    let df_schema = DFSchema::try_from(schema.clone())?;
    let logical = ctx.parse_sql_expr(sql, &df_schema)?;
    let mut names: Vec<String> = logical
        .column_refs()
        .into_iter()
        .map(|c| c.name.clone())
        .collect();
    names.sort();
    names.dedup();
    Ok(names)
}

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
        if let Expr::ScalarFunction(f) = e
            && f.func.signature().volatility == Volatility::Volatile
        {
            volatile = Some(f.func.name().to_string());
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    match volatile {
        Some(name) => Err(ExprError::Volatile(name)),
        None => Ok(()),
    }
}

pub fn evaluate(expr: &CompiledExpr, batch: &RecordBatch) -> Result<ArrayRef, ExprError> {
    Ok(expr
        .physical
        .evaluate(batch)?
        .into_array(batch.num_rows())?)
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
    fn reports_referenced_columns() {
        let cols = TableColumns::parse(&rich_schema_json()).unwrap();
        let refs = column_refs("amount * 2.0 + tenant_id", &cols.physical_schema()).unwrap();
        assert_eq!(refs, vec!["amount", "tenant_id"]);
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

        let amount = out
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(amount.value(0), 10.0, "provided value wins over default");
        assert_eq!(amount.value(1), 0.0, "NULL slot filled with default");

        let doubled = out
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(doubled.value(0), 20.0);
        assert_eq!(
            doubled.value(1),
            0.0,
            "materialized sees the filled default"
        );
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
        assert!(matches!(
            validate_table_schema(&bad).unwrap_err(),
            ExprError::Volatile(_)
        ));
    }
}

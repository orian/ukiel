//! Compile and evaluate DataFusion SQL expressions for
//! default/materialized/alias columns.

use std::sync::Arc;

use arrow::array::{ArrayRef, RecordBatch};
use arrow::compute::is_null;
use arrow::compute::kernels::zip::zip;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::common::DFSchema;
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_expr::ExprSchemable;
use datafusion::logical_expr::physical_planning_context::PhysicalPlanningContext;
use datafusion::logical_expr::simplify::SimplifyContext;
use datafusion::optimizer::simplify_expressions::ExprSimplifier;
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
    compile_to_storage(sql, schema, target, target)
}

/// Compile with the field's logical type as well as its physical representation.
/// timestamp_ms is stored as Int64, but a native SQL timestamp must change to
/// milliseconds before that integer cast (rather than leaking nanoseconds).
pub fn compile_for_field(
    sql: &str,
    schema: &Schema,
    target: &Field,
) -> Result<CompiledExpr, ExprError> {
    let intermediate = if target
        .metadata()
        .get(ukiel_core::UKIEL_TYPE_META)
        .map(String::as_str)
        == Some("timestamp_ms")
    {
        DataType::Timestamp(TimeUnit::Millisecond, None)
    } else {
        target.data_type().clone()
    };
    compile_to_storage(sql, schema, &intermediate, target.data_type())
}

fn compile_to_storage(
    sql: &str,
    schema: &Schema,
    intermediate: &DataType,
    target: &DataType,
) -> Result<CompiledExpr, ExprError> {
    let ctx = SessionContext::new();
    let df_schema = DFSchema::try_from(schema.clone())?;
    let logical = ctx.parse_sql_expr(sql, &df_schema)?;
    let logical = logical
        .cast_to(intermediate, &df_schema)?
        .cast_to(target, &df_schema)?;
    // Use DataFusion's normal coercion/simplification path. In particular,
    // now() must be resolved against an execution start time before physical
    // evaluation; its UDF deliberately cannot execute unsimplified.
    let config = Arc::clone(ctx.state().config_options());
    let mut props = ExecutionProps::new();
    props.mark_start_execution(Arc::clone(&config));
    let context = SimplifyContext::builder()
        .with_schema(Arc::new(df_schema.clone()))
        .with_config_options(config)
        .with_query_execution_start_time(props.query_execution_start_time)
        .build();
    let simplifier = ExprSimplifier::new(context);
    let logical = simplifier.coerce(logical, &df_schema)?;
    let logical = simplifier.simplify(logical)?;
    let physical = create_physical_expr(
        &logical,
        &df_schema,
        &props,
        &PhysicalPlanningContext::default(),
    )?;
    Ok(CompiledExpr { physical })
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
        let compiled = compile_for_field(expr_sql, &schema, schema.field(idx))?;
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
    let query = cols.query_schema();
    for spec in &cols.specs {
        match &spec.kind {
            ColumnKind::Stored => {}
            // Write-time expressions may only reference input columns.
            ColumnKind::Default(e) | ColumnKind::Materialized(e) => {
                compile_for_field(e, &input, query.field_with_name(&spec.name)?)?;
            }
            // Read-time expressions may reference anything stored.
            ColumnKind::Alias(e) => {
                compile_for_field(e, &physical, query.field_with_name(&spec.name)?)?;
            }
        }
    }
    Ok(())
}

/// Backfill expressions only for columns absent from the original file.
/// `batch` has already been adapted to the current physical schema; retain the
/// original `file_schema` so a stored NULL is not confused with a missing column.
/// Rewrites must preserve stored clock/random values, including stored NULLs.
pub fn apply_missing_defaults_and_materialized(
    batch: RecordBatch,
    cols: &TableColumns,
    file_schema: &Schema,
) -> Result<RecordBatch, ExprError> {
    let mut missing = cols.clone();
    missing
        .specs
        .retain(|spec| file_schema.index_of(&spec.name).is_err());
    apply_defaults_and_materialized(batch, &missing)
}

#[cfg(test)]
mod tests {
    /// Plan 39: the query path reads strings as `Utf8View`, so an alias
    /// expression is compiled against a view-typed schema. It must evaluate to
    /// the right values *and* to the declared target representation — a
    /// projection that produced `Utf8` where the query schema says `Utf8View`
    /// is a schema mismatch, not a slow path.
    #[test]
    fn alias_over_view_typed_strings_matches_the_declared_representation() {
        use arrow::array::StringViewArray;
        use arrow::datatypes::Field;

        let schema = Schema::new(vec![
            Field::new("url", DataType::Utf8View, true),
            Field::new("n", DataType::Int64, true),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(StringViewArray::from(vec![
                    Some("https://example.com/a"),
                    None,
                ])),
                Arc::new(arrow::array::Int64Array::from(vec![Some(1), Some(2)])),
            ],
        )
        .unwrap();

        // A string-returning alias, declared (as the view-typed query schema
        // declares it) as Utf8View.
        let compiled = compile("upper(url)", &schema, &DataType::Utf8View).unwrap();
        let out = evaluate(&compiled, &batch).unwrap();
        assert_eq!(
            out.data_type(),
            &DataType::Utf8View,
            "the alias must produce the representation the query schema declares"
        );
        let views = out.as_any().downcast_ref::<StringViewArray>().unwrap();
        assert_eq!(views.value(0), "HTTPS://EXAMPLE.COM/A");
        assert!(arrow::array::Array::is_null(views, 1));

        // The same expression declared as owned Utf8 still works — the cast is on
        // the alias output only, which is a single narrow column.
        let compiled = compile("upper(url)", &schema, &DataType::Utf8).unwrap();
        let out = evaluate(&compiled, &batch).unwrap();
        assert_eq!(out.data_type(), &DataType::Utf8);
    }

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
    fn evaluates_random_and_still_rejects_unknown_columns() {
        let cols = TableColumns::parse(&rich_schema_json()).unwrap();
        let expr = compile("random()", &cols.physical_schema(), &DataType::Float64).unwrap();
        let values = evaluate(&expr, &input_batch(&cols)).unwrap();
        let values = values.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(values.len(), 2);
        assert!(values.values().iter().all(|x| (0.0..1.0).contains(x)));
        // Unknown column is also a compile error (from DataFusion).
        assert!(compile("nope + 1", &cols.physical_schema(), &DataType::Float64).is_err());
    }

    #[test]
    fn now_executes_with_a_real_execution_time() {
        use arrow::array::TimestampNanosecondArray;
        use arrow::datatypes::TimeUnit;
        use std::time::{SystemTime, UNIX_EPOCH};

        let nanos = || {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos() as i64
        };
        let cols = TableColumns::parse(&rich_schema_json()).unwrap();
        let before = nanos();
        let expr = compile(
            "now()",
            &cols.physical_schema(),
            &DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into())),
        )
        .unwrap();
        let values = evaluate(&expr, &input_batch(&cols)).unwrap();
        let after = nanos();
        let values = values
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();
        assert_eq!(values.len(), 2);
        assert_eq!(values.value(0), values.value(1));
        assert!((before..=after).contains(&values.value(0)));
    }

    #[test]
    fn clock_defaults_and_random_materialized_columns_execute_at_write_time() {
        let cols = TableColumns::parse(&json!({"fields": [
            {"name": "tenant_id", "type": "int64"},
            {"name": "inserted_at", "type": "int64", "default": "CAST(now() AS BIGINT)"},
            {"name": "draw", "type": "float64", "materialized": "random()"}
        ]}))
        .unwrap();
        let input = RecordBatch::try_new(
            Arc::new(cols.physical_schema()),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(Int64Array::from(vec![Some(42), None])),
                Arc::new(Float64Array::from(vec![Some(-1.0), None])),
            ],
        )
        .unwrap();
        let out = apply_defaults_and_materialized(input, &cols).unwrap();
        let times = out.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(times.value(0), 42, "explicit defaults survive insertion");
        assert!(
            times.value(1) > 0,
            "now() evaluated rather than merely validated"
        );
        let draws = out
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!(draws.values().iter().all(|x| (0.0..1.0).contains(x)));
    }

    #[test]
    fn native_clock_default_respects_timestamp_millisecond_storage() {
        use std::time::{SystemTime, UNIX_EPOCH};
        let millis = || {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64
        };
        let cols = TableColumns::parse(&json!({"fields":[
            {"name":"tenant_id","type":"int64"},
            {"name":"inserted_at","type":"timestamp_ms","default":"now()"}
        ]}))
        .unwrap();
        let input = RecordBatch::try_new(
            Arc::new(cols.physical_schema()),
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(Int64Array::from(vec![None::<i64>])),
            ],
        )
        .unwrap();
        let before = millis();
        let output = apply_defaults_and_materialized(input, &cols).unwrap();
        let after = millis();
        let times = output
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert!(
            (before..=after).contains(&times.value(0)),
            "timestamp_ms must not store epoch nanoseconds"
        );
    }

    #[test]
    fn backfill_preserves_stored_values_and_nulls() {
        let cols = TableColumns::parse(&json!({"fields": [
            {"name": "tenant_id", "type": "int64"},
            {"name": "inserted_at", "type": "int64", "default": "CAST(now() AS BIGINT)"},
            {"name": "draw", "type": "float64", "materialized": "random()"},
            {"name": "added_later", "type": "int64", "materialized": "tenant_id + 1"}
        ]}))
        .unwrap();
        let schema = cols.physical_schema();
        let file_schema = schema.project(&[0, 1, 2]).unwrap();
        let input = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(Int64Array::from(vec![Some(42), None])),
                Arc::new(Float64Array::from(vec![Some(-1.0), None])),
                Arc::new(Int64Array::from(vec![None::<i64>, None])),
            ],
        )
        .unwrap();
        let out =
            apply_missing_defaults_and_materialized(input.clone(), &cols, &file_schema).unwrap();
        assert_eq!(out.column(1), input.column(1));
        assert_eq!(out.column(2), input.column(2));
        assert_eq!(out.column(3).as_ref(), &Int64Array::from(vec![2, 3]));
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
        // Clock/random defaults are ordinary write-time expressions.
        let clocks = json!({"fields": [
            {"name": "inserted_at", "type": "int64", "default": "CAST(now() AS BIGINT)"},
            {"name": "a", "type": "float64", "default": "random()"}
        ]});
        validate_table_schema(&clocks).unwrap();
    }
}

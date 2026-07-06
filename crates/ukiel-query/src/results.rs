//! Result formatting for the query API: row-JSON (default), columnar JSON, and
//! Arrow IPC. All three run *after* `df.collect()` — isolation and every other
//! security property are upstream and unaffected. See
//! `docs/notes/2026-07-06-columnar-results.md`.

use datafusion::arrow::array::{
    Array, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray,
};
use datafusion::arrow::datatypes::SchemaRef;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::QueryError;

/// Requested result encoding. `rows` is the default (unchanged legacy shape,
/// now with an additive `schema` sibling).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ResultFormat {
    #[default]
    Rows,
    Columns,
    Arrow,
}

/// Content type for the `arrow` format / `Accept` negotiation.
pub const ARROW_STREAM_CONTENT_TYPE: &str = "application/vnd.apache.arrow.stream";

/// The `schema` block shared by both JSON formats: `[{name, type, nullable}]`
/// in projection order, types resolved per `ukiel_core::ukiel_type_of`.
pub fn schema_block(schema: &SchemaRef) -> Value {
    Value::Array(
        schema
            .fields()
            .iter()
            .map(|f| {
                json!({
                    "name": f.name(),
                    "type": ukiel_core::ukiel_type_of(f),
                    "nullable": f.is_nullable(),
                })
            })
            .collect(),
    )
}

/// `rows` format: `{"schema": [...], "rows": [{...}, ...]}`. The `rows` array is
/// exactly the legacy body (via Arrow's row-oriented JSON writer).
pub fn to_rows(batches: &[RecordBatch], schema: &SchemaRef) -> Result<Value, QueryError> {
    let mut writer = datafusion::arrow::json::ArrayWriter::new(Vec::new());
    for batch in batches {
        writer.write(batch)?;
    }
    writer.finish()?;
    let rows: Value =
        serde_json::from_slice(&writer.into_inner()).unwrap_or_else(|_| Value::Array(vec![]));
    Ok(json!({ "schema": schema_block(schema), "rows": rows }))
}

/// `columns` format: schema block + one positionally-aligned value array per
/// column, keyed by name.
pub fn to_columns(batches: &[RecordBatch], schema: &SchemaRef) -> Result<Value, QueryError> {
    let mut columns = serde_json::Map::new();
    let mut row_count = 0usize;
    for (idx, field) in schema.fields().iter().enumerate() {
        let mut values: Vec<Value> = Vec::new();
        for batch in batches {
            append_column_values(batch.column(idx).as_ref(), &mut values);
        }
        row_count = values.len();
        columns.insert(field.name().clone(), Value::Array(values));
    }
    Ok(json!({
        "schema": schema_block(schema),
        "columns": Value::Object(columns),
        "row_count": row_count,
    }))
}

/// `arrow` format: the batches serialized as an Arrow IPC stream (lossless).
pub fn to_arrow_ipc(batches: &[RecordBatch], schema: &SchemaRef) -> Result<Vec<u8>, QueryError> {
    let mut writer = datafusion::arrow::ipc::writer::StreamWriter::try_new(Vec::new(), schema)?;
    for batch in batches {
        writer.write(batch)?;
    }
    writer.finish()?;
    Ok(writer.into_inner()?)
}

/// Appends one JSON value per row of a single column, NULLs as `null`. The five
/// Ukiel core Arrow types are handled precisely; any other type an expression
/// might yield is stringified (best-effort, per the note).
fn append_column_values(array: &dyn Array, out: &mut Vec<Value>) {
    use datafusion::arrow::datatypes::DataType;
    match array.data_type() {
        DataType::Int64 => {
            let a = array.as_any().downcast_ref::<Int64Array>().unwrap();
            for i in 0..a.len() {
                out.push(if a.is_null(i) {
                    Value::Null
                } else {
                    json!(a.value(i))
                });
            }
        }
        DataType::Float64 => {
            let a = array.as_any().downcast_ref::<Float64Array>().unwrap();
            for i in 0..a.len() {
                out.push(if a.is_null(i) {
                    Value::Null
                } else {
                    json!(a.value(i))
                });
            }
        }
        DataType::Utf8 => {
            let a = array.as_any().downcast_ref::<StringArray>().unwrap();
            for i in 0..a.len() {
                out.push(if a.is_null(i) {
                    Value::Null
                } else {
                    json!(a.value(i))
                });
            }
        }
        DataType::Boolean => {
            let a = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            for i in 0..a.len() {
                out.push(if a.is_null(i) {
                    Value::Null
                } else {
                    json!(a.value(i))
                });
            }
        }
        _ => {
            // Best-effort for types outside Ukiel's core five (dates, decimals
            // from future functions): stringify via Arrow's display.
            for i in 0..array.len() {
                if array.is_null(i) {
                    out.push(Value::Null);
                } else {
                    match datafusion::arrow::util::display::array_value_to_string(array, i) {
                        Ok(s) => out.push(Value::String(s)),
                        Err(_) => out.push(Value::Null),
                    }
                }
            }
        }
    }
}

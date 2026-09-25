//! Lossless physical-type projection.
//!
//! A variant may store `Int64` as `Int32`, or a millisecond `Int64` timestamp as an Arrow
//! `Timestamp(Millisecond)`, but only when every value round-trips bit-exactly and the
//! declared logical type permits it. An overflow, a rounding conversion, a float
//! narrowing, or a date guessed from an integer is refused *before* any output is written —
//! and the logical fingerprint, computed under the unchanged declared types, is the final
//! backstop that a projection changed the representation and nothing else.

use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::{Result, bail};
use arrow::array::{Array, ArrayRef, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use parquet_lab_integrity::LogicalType;

/// A lossless physical-type projection target.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PhysicalType {
    Int8,
    Int16,
    Int32,
    Int64,
    /// Millisecond timestamp, physical Arrow `Timestamp(Millisecond, None)`.
    TimestampMillis,
    /// Day-epoch date, physical Arrow `Date32`.
    Date32,
}

/// Parse a physical-type projection name. An unsupported name is refused up front.
pub fn parse_physical_type(s: &str) -> Result<PhysicalType> {
    Ok(match s {
        "int8" => PhysicalType::Int8,
        "int16" => PhysicalType::Int16,
        "int32" => PhysicalType::Int32,
        "int64" => PhysicalType::Int64,
        "timestamp_ms" | "timestamp_millis" => PhysicalType::TimestampMillis,
        "date32" | "date" => PhysicalType::Date32,
        other => bail!("unsupported physical type '{other}'"),
    })
}

/// Compute the output Arrow schema after applying the projections.
pub fn projected_schema(input: &Schema, projections: &BTreeMap<String, PhysicalType>) -> Schema {
    let fields = input
        .fields()
        .iter()
        .map(|f| {
            let ty = match projections.get(f.name()) {
                None => f.data_type().clone(),
                Some(PhysicalType::Int8) => DataType::Int8,
                Some(PhysicalType::Int16) => DataType::Int16,
                Some(PhysicalType::Int32) => DataType::Int32,
                Some(PhysicalType::Int64) => DataType::Int64,
                Some(PhysicalType::TimestampMillis) => {
                    DataType::Timestamp(TimeUnit::Millisecond, None)
                }
                Some(PhysicalType::Date32) => DataType::Date32,
            };
            Arc::new(Field::new(f.name(), ty, f.is_nullable()))
        })
        .collect::<Vec<_>>();
    Schema::new(fields)
}

/// Validate that every requested projection is lossless and permitted, given the declared
/// logical types. Runs once up front so a bad projection fails before any file is touched.
pub fn validate_projections(
    projections: &BTreeMap<String, PhysicalType>,
    declared: &BTreeMap<String, LogicalType>,
    schema: &Schema,
) -> Result<()> {
    for (col, target) in projections {
        let field = schema
            .fields()
            .iter()
            .find(|f| f.name() == col)
            .ok_or_else(|| anyhow::anyhow!("projection column '{col}' not in schema"))?;
        let logical = declared.get(col).copied();
        match target {
            PhysicalType::Int8
            | PhysicalType::Int16
            | PhysicalType::Int32
            | PhysicalType::Int64 => {
                if logical != Some(LogicalType::SignedInt) {
                    bail!(
                        "column '{col}' is declared {:?}, so it cannot be projected to a narrower \
                         signed integer — a laboratory never reinterprets a non-integer as one",
                        logical
                    );
                }
                if field.data_type() != &DataType::Int64 {
                    bail!(
                        "column '{col}' is physically {:?}, expected Int64 to narrow",
                        field.data_type()
                    );
                }
            }
            PhysicalType::TimestampMillis => {
                if logical != Some(LogicalType::TimestampMillis) {
                    bail!(
                        "column '{col}' is declared {:?}, not a millisecond timestamp",
                        logical
                    );
                }
                if field.data_type() != &DataType::Int64 {
                    bail!(
                        "column '{col}' is physically {:?}, expected Int64 ms",
                        field.data_type()
                    );
                }
            }
            PhysicalType::Date32 => {
                if logical != Some(LogicalType::DateDays) {
                    bail!(
                        "column '{col}' is declared {:?}, not a date — Date32 is only for a column \
                         semantically declared as a date, never guessed from an integer",
                        logical
                    );
                }
            }
        }
    }
    Ok(())
}

/// Apply the projections to one batch, verifying every narrowing is lossless per column.
pub fn project_batch(
    batch: &RecordBatch,
    projections: &BTreeMap<String, PhysicalType>,
) -> Result<RecordBatch> {
    if projections.is_empty() {
        return Ok(batch.clone());
    }
    let mut fields: Vec<Arc<Field>> = Vec::with_capacity(batch.num_columns());
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns());
    for (i, field) in batch.schema().fields().iter().enumerate() {
        let col = batch.column(i);
        match projections.get(field.name()) {
            None => {
                fields.push(field.clone());
                columns.push(col.clone());
            }
            Some(target) => {
                let (new_type, new_col) = project_column(field.name(), col, *target)?;
                fields.push(Arc::new(Field::new(
                    field.name(),
                    new_type,
                    field.is_nullable(),
                )));
                columns.push(new_col);
            }
        }
    }
    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        columns,
    )?)
}

fn project_column(
    name: &str,
    col: &ArrayRef,
    target: PhysicalType,
) -> Result<(DataType, ArrayRef)> {
    let (target_type, min, max) = match target {
        PhysicalType::Int8 => (DataType::Int8, i8::MIN as i64, i8::MAX as i64),
        PhysicalType::Int16 => (DataType::Int16, i16::MIN as i64, i16::MAX as i64),
        PhysicalType::Int32 => (DataType::Int32, i32::MIN as i64, i32::MAX as i64),
        PhysicalType::Int64 => (DataType::Int64, i64::MIN, i64::MAX),
        PhysicalType::TimestampMillis => {
            let cast =
                arrow::compute::cast(col, &DataType::Timestamp(TimeUnit::Millisecond, None))?;
            return Ok((DataType::Timestamp(TimeUnit::Millisecond, None), cast));
        }
        PhysicalType::Date32 => {
            // Days as an integer -> Date32. Verify the day count fits i32 first.
            check_int_range(name, col, i32::MIN as i64, i32::MAX as i64)?;
            let as_i32 = arrow::compute::cast(col, &DataType::Int32)?;
            let cast = arrow::compute::cast(&as_i32, &DataType::Date32)?;
            return Ok((DataType::Date32, cast));
        }
    };
    // Signed-integer narrowing: prove every value fits before casting.
    check_int_range(name, col, min, max)?;
    let cast = arrow::compute::cast(col, &target_type)?;
    Ok((target_type, cast))
}

/// Refuse the projection if any non-null value lies outside `[min, max]`.
fn check_int_range(name: &str, col: &ArrayRef, min: i64, max: i64) -> Result<()> {
    let a = col.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
        anyhow::anyhow!("column '{name}' is not Int64; cannot narrow it losslessly")
    })?;
    if let Some(lo) = arrow::compute::min(a)
        && lo < min
    {
        bail!(
            "column '{name}': value {lo} does not fit the target type (< {min}); narrowing is lossy"
        );
    }
    if let Some(hi) = arrow::compute::max(a)
        && hi > max
    {
        bail!(
            "column '{name}': value {hi} does not fit the target type (> {max}); narrowing is lossy"
        );
    }
    Ok(())
}

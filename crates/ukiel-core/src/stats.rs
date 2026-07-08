//! Per-part column statistics computed at write time and stored in the
//! catalog (`parts.column_stats`) for part-level pruning before any file I/O.

use arrow::array::{Array, Int64Array, RecordBatch};
use arrow::datatypes::DataType;

/// `{"col": {"min": i64, "max": i64}}` for every Int64 column with at least
/// one non-null value. `None` when nothing is collectable.
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
    if stats.is_empty() {
        None
    } else {
        Some(serde_json::Value::Object(stats))
    }
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

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

/// Fold-in variant of [`int64_column_stats`] for streaming writers (plan 29):
/// accumulate per batch, then `finish` to the identical
/// `{"col": {"min", "max"}}` JSON — a whole batch and its slices fed one at a
/// time produce the same result.
#[derive(Default)]
pub struct Int64StatsAccumulator {
    /// Int64 column names in first-seen order.
    order: Vec<String>,
    /// name -> running (min, max) over non-null values.
    stats: std::collections::HashMap<String, (i64, i64)>,
}

impl Int64StatsAccumulator {
    /// Folds one batch's Int64 columns into the running min/max.
    pub fn update(&mut self, batch: &RecordBatch) {
        for (i, field) in batch.schema().fields().iter().enumerate() {
            if field.data_type() != &DataType::Int64 {
                continue;
            }
            let Some(col) = batch.column(i).as_any().downcast_ref::<Int64Array>() else {
                continue;
            };
            let (Some(bmin), Some(bmax)) = (arrow::compute::min(col), arrow::compute::max(col))
            else {
                continue; // all-null or empty in this batch
            };
            match self.stats.get_mut(field.name()) {
                Some((mn, mx)) => {
                    *mn = (*mn).min(bmin);
                    *mx = (*mx).max(bmax);
                }
                None => {
                    self.order.push(field.name().clone());
                    self.stats.insert(field.name().clone(), (bmin, bmax));
                }
            }
        }
    }

    /// The accumulated stats as JSON, or `None` if no Int64 column ever had a
    /// non-null value (matching [`int64_column_stats`]).
    pub fn finish(self) -> Option<serde_json::Value> {
        if self.stats.is_empty() {
            return None;
        }
        let mut map = serde_json::Map::new();
        for name in &self.order {
            let (mn, mx) = self.stats[name];
            map.insert(name.clone(), serde_json::json!({ "min": mn, "max": mx }));
        }
        Some(serde_json::Value::Object(map))
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

    #[test]
    fn accumulator_folds_to_the_whole_batch_result() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("tenant_id", DataType::Int64, false),
            Field::new("ts", DataType::Int64, true),
            // All-null in the whole batch -> excluded by both paths.
            Field::new("empty_int", DataType::Int64, true),
            Field::new("payload", DataType::Utf8, true),
        ]));
        let whole = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![7, 3, 5, 9])),
                Arc::new(Int64Array::from(vec![Some(100), None, Some(50), Some(70)])),
                Arc::new(Int64Array::from(vec![None, None, None, None])),
                Arc::new(StringArray::from(vec!["a", "b", "c", "d"])),
            ],
        )
        .unwrap();

        // Fold three uneven slices; must equal the whole-batch computation.
        let mut acc = Int64StatsAccumulator::default();
        for (offset, len) in [(0, 1), (1, 2), (3, 1)] {
            acc.update(&whole.slice(offset, len));
        }
        assert_eq!(acc.finish(), int64_column_stats(&whole));

        // No Int64 columns / empty -> None either way.
        let utf8_only = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("p", DataType::Utf8, true)])),
            vec![Arc::new(StringArray::from(vec!["x"]))],
        )
        .unwrap();
        let mut acc = Int64StatsAccumulator::default();
        acc.update(&utf8_only);
        assert_eq!(acc.finish(), None);
        assert_eq!(int64_column_stats(&utf8_only), None);
    }
}

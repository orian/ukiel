//! Per-part column statistics computed at write time and stored in the
//! catalog (`parts.column_stats`) for part-level pruning before any file I/O.
//!
//! Plan 16 adds two more entries to the same JSONB blob (no migration):
//! [`PACKING_KEYS_STAT`], a roaring treemap of the part's distinct packing keys,
//! and [`KEY_ROW_GROUPS_STAT`], the per-row-group key spans. Both exist to make
//! pruning *exact* where min/max can only be conservative.
//!
//! **Every ambiguous state degrades to "keep the part."** A missing index, an
//! undecodable one, a negative key, a partial span list — all resolve to `None`,
//! never to a wrongful "skip". Pruning may only ever remove a part it can
//! *prove* holds no matching rows; the query provider's isolation `FilterExec`
//! remains the correctness backstop regardless, so an over-broad decision costs
//! performance and an over-narrow one cannot happen.

use arrow::array::{Array, Int64Array, RecordBatch};
use arrow::datatypes::DataType;
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as B64;
use parquet::file::metadata::RowGroupMetaData;
use roaring::RoaringTreemap;

/// `column_stats` key: base64 of the roaring treemap of distinct packing keys.
pub const PACKING_KEYS_STAT: &str = "packing_keys";
/// `column_stats` key: `[[min, max], …]` per row group, in file order.
pub const KEY_ROW_GROUPS_STAT: &str = "key_row_groups";

/// Above this many distinct keys the bitmap stops earning its place: min/max is
/// already dense enough to prune well, and the serialized blob keeps growing in
/// every catalog row and every planning query. Not a knob — three writers would
/// have to thread it, and no deployment has asked. Promote it to
/// `hypertables` if one ever does.
pub const MAX_BITMAP_KEYS: usize = 10_000;

/// Accumulates the distinct packing keys of a part, fold-in style (the plan-29
/// streaming writer feeds one slice at a time).
///
/// `poisoned` is sticky: a negative key, or a missing / non-Int64 packing-key
/// column, means we cannot build a sound treemap (its domain is u64), so no
/// bitmap is written at all and pruning falls back to min/max.
#[derive(Default)]
pub struct KeyBitmapAccumulator {
    keys: RoaringTreemap,
    poisoned: bool,
}

impl KeyBitmapAccumulator {
    /// Folds one batch's packing-key column into the set.
    pub fn update(&mut self, batch: &RecordBatch, packing_key: &str) {
        if self.poisoned {
            return;
        }
        let Ok(idx) = batch.schema().index_of(packing_key) else {
            self.poisoned = true;
            return;
        };
        let Some(col) = batch.column(idx).as_any().downcast_ref::<Int64Array>() else {
            self.poisoned = true;
            return;
        };
        for v in col.iter().flatten() {
            if v < 0 {
                self.poisoned = true;
                return;
            }
            self.keys.insert(v as u64);
        }
    }

    /// base64 of the serialized treemap, or `None` when poisoned, empty, or over
    /// the distinct-key cap. `None` means "no index" — never "prune nothing".
    pub fn finish(self) -> Option<String> {
        if self.poisoned || self.keys.is_empty() {
            return None;
        }
        if self.keys.len() > MAX_BITMAP_KEYS as u64 {
            return None;
        }
        let mut bytes = Vec::new();
        self.keys.serialize_into(&mut bytes).ok()?;
        Some(B64.encode(bytes))
    }
}

/// One-shot [`KeyBitmapAccumulator`] over a single batch — the ingest and
/// deletion call shape.
pub fn key_bitmap(batch: &RecordBatch, packing_key: &str) -> Option<String> {
    let mut acc = KeyBitmapAccumulator::default();
    acc.update(batch, packing_key);
    acc.finish()
}

/// Whether `key` is in the encoded treemap.
///
/// `None` means **undecidable — keep the part**: an undecodable blob, or a
/// negative key (which cannot be represented, so its absence proves nothing).
/// Only `Some(false)` licenses a skip.
pub fn bitmap_contains(encoded: &str, key: i64) -> Option<bool> {
    if key < 0 {
        return None;
    }
    let bytes = B64.decode(encoded).ok()?;
    let map = RoaringTreemap::deserialize_from(&bytes[..]).ok()?;
    Some(map.contains(key as u64))
}

/// Per-row-group `[min, max]` of the packing key, in file order, read from the
/// writer's own row-group metadata (call before `close()` — see
/// `ArrowWriter::flushed_row_groups`).
///
/// All-or-nothing: if any group lacks Int64 statistics for the packing key we
/// return `None` rather than a partial list, because the spans are positional —
/// a gap would silently mis-index every group after it.
pub fn key_row_groups(
    row_groups: &[RowGroupMetaData],
    packing_key: &str,
) -> Option<serde_json::Value> {
    use parquet::file::statistics::Statistics;

    if row_groups.is_empty() {
        return None;
    }
    let mut spans = Vec::with_capacity(row_groups.len());
    for rg in row_groups {
        let col = rg
            .columns()
            .iter()
            .find(|c| c.column_path().string() == packing_key)?;
        let Some(Statistics::Int64(s)) = col.statistics() else {
            return None;
        };
        let (Some(min), Some(max)) = (s.min_opt(), s.max_opt()) else {
            return None;
        };
        spans.push(serde_json::json!([*min, *max]));
    }
    Some(serde_json::Value::Array(spans))
}

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

    fn keys_batch(keys: Vec<i64>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("tenant_id", DataType::Int64, false),
            Field::new("payload", DataType::Utf8, true),
        ]));
        let n = keys.len();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(keys)),
                Arc::new(StringArray::from(vec!["x"; n])),
            ],
        )
        .unwrap()
    }

    /// The whole point of the bitmap: a key *inside* the part's min/max range
    /// that owns no rows must test `false`. Range pruning cannot see that; this
    /// is the sparse-tenant false-positive killer.
    #[test]
    fn key_bitmap_is_exact_for_in_range_absent_keys() {
        let encoded = key_bitmap(&keys_batch(vec![1, 1, 2, 4500]), "tenant_id").unwrap();
        for present in [1, 2, 4500] {
            assert_eq!(bitmap_contains(&encoded, present), Some(true), "{present}");
        }
        // 300 sits inside [1, 4500] but owns no rows — range pruning keeps the
        // part, the bitmap drops it.
        assert_eq!(bitmap_contains(&encoded, 300), Some(false));
        assert_eq!(bitmap_contains(&encoded, 4499), Some(false));
    }

    /// Every ambiguous state degrades to "keep the part" — `None`, never a
    /// wrongful `Some(false)`.
    #[test]
    fn ambiguity_never_prunes() {
        assert_eq!(bitmap_contains("!!! not base64 !!!", 100), None);
        assert_eq!(bitmap_contains("", 100), None);
        // Valid base64, garbage treemap bytes.
        assert_eq!(bitmap_contains("aGVsbG8gd29ybGQ=", 100), None);

        let encoded = key_bitmap(&keys_batch(vec![1, 2]), "tenant_id").unwrap();
        // A negative key cannot be in a u64 treemap: undecidable, so keep.
        assert_eq!(bitmap_contains(&encoded, -5), None);

        // Poison: a negative key anywhere in the data, or a missing/non-Int64
        // packing-key column, means no bitmap is written at all.
        assert!(key_bitmap(&keys_batch(vec![1, -7]), "tenant_id").is_none());
        assert!(key_bitmap(&keys_batch(vec![1, 2]), "nope").is_none());
        assert!(key_bitmap(&keys_batch(vec![1, 2]), "payload").is_none());
    }

    /// Beyond the cap the bitmap stops paying for itself (min/max is dense
    /// enough) and the blob grows — so it is simply not written.
    #[test]
    fn distinct_key_cap_suppresses_the_bitmap() {
        let under: Vec<i64> = (0..MAX_BITMAP_KEYS as i64).collect();
        assert!(key_bitmap(&keys_batch(under), "tenant_id").is_some());

        let over: Vec<i64> = (0..=MAX_BITMAP_KEYS as i64).collect();
        assert!(key_bitmap(&keys_batch(over), "tenant_id").is_none());
    }

    /// Set semantics: fed in slices or whole, the accumulator agrees with the
    /// one-shot helper (the streaming writer feeds slices).
    #[test]
    fn bitmap_accumulator_matches_the_one_shot() {
        let whole = keys_batch(vec![5, 5, 9, 1, 9]);
        let mut acc = KeyBitmapAccumulator::default();
        for (offset, len) in [(0, 2), (2, 1), (3, 2)] {
            acc.update(&whole.slice(offset, len), "tenant_id");
        }
        assert_eq!(acc.finish(), key_bitmap(&whole, "tenant_id"));

        // Poison propagates through the fold, not just the one-shot.
        let mut acc = KeyBitmapAccumulator::default();
        acc.update(&keys_batch(vec![1, 2]), "tenant_id");
        acc.update(&keys_batch(vec![-1]), "tenant_id");
        assert_eq!(acc.finish(), None);
    }

    /// Spans come from the file's own row-group statistics — this checks them
    /// against a real two-row-group Parquet file.
    #[test]
    fn key_row_groups_reads_spans_from_the_writers_own_metadata() {
        use parquet::arrow::ArrowWriter;
        use parquet::file::properties::WriterProperties;

        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(2))
            .build();
        let batch = keys_batch(vec![1, 1, 7, 7]);
        let mut buf = Vec::new();
        let mut w = ArrowWriter::try_new(&mut buf, batch.schema(), Some(props)).unwrap();
        w.write(&batch).unwrap();
        w.flush().unwrap(); // force the row groups out before we read metadata

        // Read the spans BEFORE close(): flushed_row_groups() is only valid then.
        let spans = key_row_groups(w.flushed_row_groups(), "tenant_id").unwrap();
        assert_eq!(spans, json!([[1, 1], [7, 7]]));
        w.close().unwrap();

        // A column that is not the packing key (or absent) yields no spans, and
        // a partial span list is never produced — all-or-nothing.
        let mut w = ArrowWriter::try_new(Vec::new(), batch.schema(), None).unwrap();
        w.write(&batch).unwrap();
        w.flush().unwrap();
        assert!(key_row_groups(w.flushed_row_groups(), "missing").is_none());
    }

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

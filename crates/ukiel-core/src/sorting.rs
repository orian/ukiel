//! Canonical sort semantics shared by ingest and compaction so L0 and L1
//! ordering can never drift, plus registration-time `sort_key` validation.
//!
//! One convention everywhere: **ascending, nulls first** (the arrow/DataFusion
//! default). The query provider declares `PhysicalSortExpr::new_default`
//! (asc/nulls-first) and DataFusion elides sorts on that basis, so the files
//! must physically match — this module is the single source of that ordering.

use arrow::array::RecordBatch;
use arrow::compute::{SortColumn, SortOptions, lexsort_to_indices, take};
use arrow::datatypes::Schema;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::metadata::SortingColumn;
use parquet::file::properties::WriterProperties;

/// Canonical order: ascending.
pub const SORT_DESCENDING: bool = false;
/// Canonical null placement: nulls first (arrow/DataFusion default).
pub const SORT_NULLS_FIRST: bool = true;

fn sort_options() -> SortOptions {
    SortOptions {
        descending: SORT_DESCENDING,
        nulls_first: SORT_NULLS_FIRST,
    }
}

/// Errors from sorting or validating a `sort_key`.
#[derive(Debug, thiserror::Error)]
pub enum SortKeyError {
    #[error("sort_key must be non-empty")]
    Empty,
    #[error("sort_key[0] ('{first}') must equal the packing key ('{packing_key}')")]
    PackingKeyNotFirst { first: String, packing_key: String },
    #[error("sort_key/ts column '{0}' is not in the table schema")]
    UnknownColumn(String),
    #[error("ts_column '{0}' must appear in sort_key")]
    TsNotInSortKey(String),
    #[error(transparent)]
    Arrow(#[from] arrow::error::ArrowError),
}

/// Lexicographic sort of `batch` by `sort_key` (in order) under the canonical
/// options. Any sort column absent from the batch is an error.
pub fn sort_batch(batch: &RecordBatch, sort_key: &[String]) -> Result<RecordBatch, SortKeyError> {
    let mut columns = Vec::with_capacity(sort_key.len());
    for name in sort_key {
        let idx = batch
            .schema()
            .index_of(name)
            .map_err(|_| SortKeyError::UnknownColumn(name.clone()))?;
        columns.push(SortColumn {
            values: batch.column(idx).clone(),
            options: Some(sort_options()),
        });
    }
    let indices = lexsort_to_indices(&columns, None)?;
    let sorted = batch
        .columns()
        .iter()
        .map(|c| take(c.as_ref(), &indices, None))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(RecordBatch::try_new(batch.schema(), sorted)?)
}

/// Parquet `SortingColumn` metadata matching `sort_batch`'s order, for the
/// `sort_key` columns present in `schema` (in order).
pub fn sorting_columns(schema: &Schema, sort_key: &[String]) -> Vec<SortingColumn> {
    sort_key
        .iter()
        .filter_map(|name| schema.index_of(name).ok())
        .map(|idx| SortingColumn {
            column_idx: idx as i32,
            descending: SORT_DESCENDING,
            nulls_first: SORT_NULLS_FIRST,
        })
        .collect()
}

/// ZSTD writer properties stamping `sort_key` as the file's sorting columns —
/// used by both the ingest and compaction writers so compression and the
/// declared ordering stay identical.
pub fn sorted_writer_props(schema: &Schema, sort_key: &[String]) -> WriterProperties {
    WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .set_sorting_columns(Some(sorting_columns(schema, sort_key)))
        .build()
}

/// Registration-time invariants (design note `docs/notes/2026-07-07-ingest-sort-by-sort-key.md`):
/// non-empty, packing key leads, every column exists in the (physical) schema,
/// and — when `ts_column` is known — the ts column is in `sort_key`. Validated
/// at table creation and re-checked at bootstrap / consumer startup.
pub fn validate_sort_key(
    schema: &Schema,
    sort_key: &[String],
    packing_key: &str,
    ts_column: Option<&str>,
) -> Result<(), SortKeyError> {
    let first = sort_key.first().ok_or(SortKeyError::Empty)?;
    if first != packing_key {
        return Err(SortKeyError::PackingKeyNotFirst {
            first: first.clone(),
            packing_key: packing_key.to_string(),
        });
    }
    for name in sort_key {
        if schema.index_of(name).is_err() {
            return Err(SortKeyError::UnknownColumn(name.clone()));
        }
    }
    if let Some(ts) = ts_column
        && !sort_key.iter().any(|c| c == ts)
    {
        return Err(SortKeyError::TsNotInSortKey(ts.to_string()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field};
    use parquet::arrow::ArrowWriter;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use std::sync::Arc;

    fn schema3() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("tenant_id", DataType::Int64, false),
            Field::new("region", DataType::Utf8, true),
            Field::new("ts", DataType::Int64, true),
        ]))
    }

    #[test]
    fn sorts_by_multi_column_key_including_utf8() {
        let batch = RecordBatch::try_new(
            schema3(),
            vec![
                Arc::new(Int64Array::from(vec![2, 1, 1, 1])),
                Arc::new(StringArray::from(vec![
                    Some("z"),
                    Some("b"),
                    Some("a"),
                    Some("a"),
                ])),
                Arc::new(Int64Array::from(vec![Some(1), Some(9), Some(5), Some(2)])),
            ],
        )
        .unwrap();
        let sort_key = vec![
            "tenant_id".to_string(),
            "region".to_string(),
            "ts".to_string(),
        ];
        let sorted = sort_batch(&batch, &sort_key).unwrap();
        let t: &Int64Array = sorted.column(0).as_any().downcast_ref().unwrap();
        let r: &StringArray = sorted.column(1).as_any().downcast_ref().unwrap();
        let ts: &Int64Array = sorted.column(2).as_any().downcast_ref().unwrap();
        assert_eq!(t.values().as_ref(), &[1, 1, 1, 2]);
        assert_eq!(
            (0..4).map(|i| r.value(i)).collect::<Vec<_>>(),
            vec!["a", "a", "b", "z"]
        );
        assert_eq!(ts.values().as_ref(), &[2, 5, 9, 1]);
    }

    #[test]
    fn nulls_sort_first() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 1, 1])),
                Arc::new(Int64Array::from(vec![Some(5), None, Some(1)])),
            ],
        )
        .unwrap();
        let sorted = sort_batch(&batch, &["k".to_string(), "v".to_string()]).unwrap();
        let v: &Int64Array = sorted.column(1).as_any().downcast_ref().unwrap();
        assert!(v.is_null(0), "null sorts first");
        assert_eq!(v.value(1), 1);
        assert_eq!(v.value(2), 5);
    }

    #[test]
    fn sorting_columns_stamp_nulls_first_and_survive_a_round_trip() {
        let schema = schema3();
        let sort_key = vec!["tenant_id".to_string(), "ts".to_string()];
        let cols = sorting_columns(&schema, &sort_key);
        assert_eq!(cols.len(), 2);
        assert_eq!(cols[0].column_idx, 0);
        assert_eq!(cols[1].column_idx, 2);
        assert!(cols.iter().all(|c| c.nulls_first && !c.descending));

        // The props actually stamp them into the file metadata.
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1_i64])),
                Arc::new(StringArray::from(vec![Some("a")])),
                Arc::new(Int64Array::from(vec![Some(1_i64)])),
            ],
        )
        .unwrap();
        let mut bytes = Vec::new();
        let props = sorted_writer_props(&schema, &sort_key);
        let mut w = ArrowWriter::try_new(&mut bytes, schema, Some(props)).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes)).unwrap();
        let stamped = reader
            .metadata()
            .row_group(0)
            .sorting_columns()
            .expect("sorting columns present")
            .clone();
        assert_eq!(stamped, cols);
    }

    #[test]
    fn validate_rejects_bad_sort_keys() {
        let s = schema3();
        let ok = vec!["tenant_id".to_string(), "ts".to_string()];
        validate_sort_key(&s, &ok, "tenant_id", Some("ts")).unwrap();

        assert!(matches!(
            validate_sort_key(&s, &[], "tenant_id", None),
            Err(SortKeyError::Empty)
        ));
        assert!(matches!(
            validate_sort_key(&s, &["ts".to_string()], "tenant_id", None),
            Err(SortKeyError::PackingKeyNotFirst { .. })
        ));
        assert!(matches!(
            validate_sort_key(
                &s,
                &["tenant_id".to_string(), "nope".to_string()],
                "tenant_id",
                None
            ),
            Err(SortKeyError::UnknownColumn(_))
        ));
        assert!(matches!(
            validate_sort_key(&s, &["tenant_id".to_string()], "tenant_id", Some("ts")),
            Err(SortKeyError::TsNotInSortKey(_))
        ));
    }
}

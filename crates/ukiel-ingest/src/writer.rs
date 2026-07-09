//! Encodes buffered JSON rows into a sorted, ZSTD-compressed Parquet file.

use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::Schema;
use arrow::json::ReaderBuilder;
use parquet::arrow::ArrowWriter;
use ukiel_core::TableColumns;

use crate::IngestError;

/// One encoded L0 Parquet file, ready for the object store.
#[derive(Debug)]
pub struct EncodedPart {
    pub bytes: Vec<u8>,
    pub row_count: i64,
    pub key_min: i64,
    pub key_max: i64,
    /// Per-column Int64 min/max for catalog part pruning.
    pub column_stats: Option<serde_json::Value>,
}

/// Decodes `rows` against a plain schema, sorts by `sort_key`, and encodes one
/// Parquet file. Every row must carry the packing key and ts as JSON integers.
pub fn rows_to_parquet(
    schema: &Schema,
    packing_key: &str,
    ts_column: &str,
    sort_key: &[String],
    rows: Vec<serde_json::Value>,
) -> Result<EncodedPart, IngestError> {
    let batch = decode_rows(schema, packing_key, ts_column, rows)?;
    let batch = ukiel_core::sort_batch(&batch, sort_key)?;
    finish_batch(batch, packing_key, sort_key)
}

/// Column-spec-aware encoding: decode against the physical schema (materialized
/// slots NULL, extra JSON fields ignored), fill defaults, compute materialized
/// columns, then **sort the Arrow batch by `sort_key`** (after materialization,
/// so `sort_key` may include computed columns) and encode. One shared columnar
/// sort with compaction (`ukiel_core::sort_batch`), not a JSON-level i64 hack —
/// so L0 files physically match the ordering the query provider declares.
pub fn encode_rows(
    cols: &TableColumns,
    packing_key: &str,
    ts_column: &str,
    sort_key: &[String],
    rows: Vec<serde_json::Value>,
) -> Result<EncodedPart, IngestError> {
    let physical = cols.physical_schema();
    let batch = decode_rows(&physical, packing_key, ts_column, rows)?;
    let batch = ukiel_expr::apply_defaults_and_materialized(batch, cols)?;
    let batch = ukiel_core::sort_batch(&batch, sort_key)?;
    finish_batch(batch, packing_key, sort_key)
}

/// Validates the packing key + ts are present as i64 (needed for the packing-key
/// range and day partitioning) and decodes into an Arrow batch of `schema`.
/// Sorting happens on the decoded batch, not here.
fn decode_rows(
    schema: &Schema,
    packing_key: &str,
    ts_column: &str,
    rows: Vec<serde_json::Value>,
) -> Result<RecordBatch, IngestError> {
    if rows.is_empty() {
        return Err(IngestError::EmptyFlush);
    }

    let get = |row: &serde_json::Value, col: &str| row.get(col).and_then(|v| v.as_i64());
    for (i, row) in rows.iter().enumerate() {
        for col in [packing_key, ts_column] {
            if get(row, col).is_none() {
                return Err(IngestError::MissingColumn {
                    row: i,
                    column: col.to_string(),
                });
            }
        }
    }

    let schema_ref = Arc::new(schema.clone());
    let mut decoder = ReaderBuilder::new(schema_ref).build_decoder()?;
    decoder.serialize(&rows)?;
    decoder.flush()?.ok_or(IngestError::EmptyFlush)
}

/// Computes the packing-key range from the already-sorted batch and writes ZSTD
/// Parquet, declaring `sort_key` as the file's sorting columns via the shared
/// `ukiel_core::sorted_writer_props` — the exact order the batch was sorted by,
/// so readers can trust the declared ordering.
fn finish_batch(
    batch: RecordBatch,
    packing_key: &str,
    sort_key: &[String],
) -> Result<EncodedPart, IngestError> {
    let key_idx = batch
        .schema()
        .index_of(packing_key)
        .map_err(|_| IngestError::MissingColumn {
            row: 0,
            column: packing_key.to_string(),
        })?;
    let keys = batch
        .column(key_idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| IngestError::MissingColumn {
            row: 0,
            column: packing_key.to_string(),
        })?;
    let key_min = arrow::compute::min(keys).ok_or(IngestError::EmptyFlush)?;
    let key_max = arrow::compute::max(keys).ok_or(IngestError::EmptyFlush)?;
    let column_stats = ukiel_core::stats::int64_column_stats(&batch);

    let props = ukiel_core::sorted_writer_props(&batch.schema(), sort_key);
    let mut bytes = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut bytes, batch.schema(), Some(props))?;
    writer.write(&batch)?;
    writer.close()?;

    Ok(EncodedPart {
        bytes,
        row_count: batch.num_rows() as i64,
        key_min,
        key_max,
        column_stats,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int64Array, StringArray};
    use bytes::Bytes;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use serde_json::json;
    use ukiel_core::arrow_schema_from_json;

    fn test_schema() -> Schema {
        arrow_schema_from_json(&json!({"fields": [
            {"name": "tenant_id", "type": "int64"},
            {"name": "ts", "type": "timestamp_ms"},
            {"name": "payload", "type": "utf8"}
        ]}))
        .unwrap()
    }

    fn sk() -> Vec<String> {
        vec!["tenant_id".to_string(), "ts".to_string()]
    }

    #[test]
    fn sorts_rows_and_reports_key_range() {
        let rows = vec![
            json!({"tenant_id": 7, "ts": 300, "payload": "c"}),
            json!({"tenant_id": 2, "ts": 200, "payload": "b"}),
            json!({"tenant_id": 7, "ts": 100, "payload": "a"}),
        ];
        let part = rows_to_parquet(&test_schema(), "tenant_id", "ts", &sk(), rows).unwrap();
        assert_eq!(part.row_count, 3);
        assert_eq!(part.key_min, 2);
        assert_eq!(part.key_max, 7);

        // Read back and verify sort order (tenant_id asc, ts asc).
        let reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(part.bytes))
            .unwrap()
            .build()
            .unwrap();
        let batches: Vec<_> = reader.map(|b| b.unwrap()).collect();
        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        let tenants: &Int64Array = batch.column(0).as_any().downcast_ref().unwrap();
        let ts: &Int64Array = batch.column(1).as_any().downcast_ref().unwrap();
        let payloads: &StringArray = batch.column(2).as_any().downcast_ref().unwrap();
        assert_eq!(tenants.values().as_ref(), &[2, 7, 7]);
        assert_eq!(ts.values().as_ref(), &[200, 100, 300]);
        assert_eq!(payloads.value(0), "b");
        assert_eq!(payloads.value(1), "a");
        assert_eq!(payloads.value(2), "c");
    }

    #[test]
    fn rejects_missing_key_and_empty_input() {
        let err = rows_to_parquet(
            &test_schema(),
            "tenant_id",
            "ts",
            &sk(),
            vec![json!({"ts": 1})],
        )
        .unwrap_err();
        assert!(matches!(err, IngestError::MissingColumn { .. }));

        let err = rows_to_parquet(&test_schema(), "tenant_id", "ts", &sk(), vec![]).unwrap_err();
        assert!(matches!(err, IngestError::EmptyFlush));
    }

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
            json!({"tenant_id": 1, "ts": 20}), // amount omitted -> default
            json!({"tenant_id": 1, "ts": 30, "doubled": 999.0}), // materialized input ignored
        ];
        let part = encode_rows(&cols, "tenant_id", "ts", &sk(), rows).unwrap();

        let reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(part.bytes))
            .unwrap()
            .build()
            .unwrap();
        let batch = reader.map(|b| b.unwrap()).next().unwrap();
        assert_eq!(batch.num_columns(), 4, "materialized column is stored");
        let amount: &arrow::array::Float64Array = batch.column(2).as_any().downcast_ref().unwrap();
        let doubled: &arrow::array::Float64Array = batch.column(3).as_any().downcast_ref().unwrap();
        assert_eq!(amount.values().as_ref(), &[5.0, 0.0, 0.0]);
        assert_eq!(doubled.values().as_ref(), &[10.0, 0.0, 0.0]);
    }

    #[test]
    fn sorts_by_a_materialized_column_after_computing_it() {
        // The batch is sorted AFTER `apply_defaults_and_materialized`, so
        // `sort_key` can reference a computed column — impossible with the old
        // pre-decode JSON row sort, which never saw `doubled`.
        let cols = ukiel_core::TableColumns::parse(&json!({"fields": [
            {"name": "tenant_id", "type": "int64"},
            {"name": "ts", "type": "timestamp_ms"},
            {"name": "amount", "type": "float64", "default": "0.0"},
            {"name": "doubled", "type": "float64", "materialized": "amount * 2.0"}
        ]}))
        .unwrap();
        // Same tenant, input order unrelated to `doubled`.
        let rows = vec![
            json!({"tenant_id": 1, "ts": 10, "amount": 5.0}), // doubled 10
            json!({"tenant_id": 1, "ts": 20, "amount": 1.0}), // doubled 2
            json!({"tenant_id": 1, "ts": 30, "amount": 3.0}), // doubled 6
        ];
        let sort_key = vec!["tenant_id".to_string(), "doubled".to_string()];
        let part = encode_rows(&cols, "tenant_id", "ts", &sort_key, rows).unwrap();

        let reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(part.bytes))
            .unwrap()
            .build()
            .unwrap();
        let batch = reader.map(|b| b.unwrap()).next().unwrap();
        let doubled: &arrow::array::Float64Array = batch.column(3).as_any().downcast_ref().unwrap();
        assert_eq!(
            doubled.values().as_ref(),
            &[2.0, 6.0, 10.0],
            "rows are ordered by the computed column, not input order"
        );
    }
}

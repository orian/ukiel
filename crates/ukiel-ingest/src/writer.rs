//! Encodes buffered JSON rows into a sorted, ZSTD-compressed Parquet file.

use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::json::ReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;

use crate::IngestError;

/// One encoded L0 Parquet file, ready for the object store.
#[derive(Debug)]
pub struct EncodedPart {
    pub bytes: Vec<u8>,
    pub row_count: i64,
    pub key_min: i64,
    pub key_max: i64,
}

/// Sorts `rows` by (packing key, ts) and encodes them as one Parquet file.
/// Every row must carry both columns as JSON integers.
pub fn rows_to_parquet(
    schema: &Schema,
    packing_key: &str,
    ts_column: &str,
    mut rows: Vec<serde_json::Value>,
) -> Result<EncodedPart, IngestError> {
    if rows.is_empty() {
        return Err(IngestError::EmptyFlush);
    }

    // Validate up front so sorting can't panic on malformed rows.
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

    rows.sort_by_key(|r| (get(r, packing_key).unwrap(), get(r, ts_column).unwrap()));
    let key_min = get(&rows[0], packing_key).unwrap();
    let key_max = get(rows.last().unwrap(), packing_key).unwrap();

    let schema_ref = Arc::new(schema.clone());
    let mut decoder = ReaderBuilder::new(schema_ref.clone()).build_decoder()?;
    decoder.serialize(&rows)?;
    let batch = decoder.flush()?.ok_or(IngestError::EmptyFlush)?;

    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .build();
    let mut bytes = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut bytes, schema_ref, Some(props))?;
    writer.write(&batch)?;
    writer.close()?;

    Ok(EncodedPart {
        bytes,
        row_count: batch.num_rows() as i64,
        key_min,
        key_max,
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

    #[test]
    fn sorts_rows_and_reports_key_range() {
        let rows = vec![
            json!({"tenant_id": 7, "ts": 300, "payload": "c"}),
            json!({"tenant_id": 2, "ts": 200, "payload": "b"}),
            json!({"tenant_id": 7, "ts": 100, "payload": "a"}),
        ];
        let part = rows_to_parquet(&test_schema(), "tenant_id", "ts", rows).unwrap();
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
        let err =
            rows_to_parquet(&test_schema(), "tenant_id", "ts", vec![json!({"ts": 1})]).unwrap_err();
        assert!(matches!(err, IngestError::MissingColumn { .. }));

        let err = rows_to_parquet(&test_schema(), "tenant_id", "ts", vec![]).unwrap_err();
        assert!(matches!(err, IngestError::EmptyFlush));
    }
}

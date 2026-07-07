//! The shared rewrite engine: read parts -> merge -> sort -> split -> encode.
//! Used by both compaction (merge small files) and deletion (rewrite without
//! one key). Pure functions over Arrow batches; no catalog access.

use std::sync::Arc;

use arrow::array::{Array, Int64Array, RecordBatch};
use arrow::compute::{SortColumn, concat_batches, lexsort_to_indices, take};
use arrow::datatypes::SchemaRef;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use ukiel_core::{Part, Placement};

use crate::CompactorError;

/// Downloads every part and concatenates all row batches into one, adapting
/// each file to `schema`: a column the file predates contributes NULLs
/// (additive schema evolution).
pub async fn read_parts_to_batch(
    store: &Arc<dyn ObjectStore>,
    schema: SchemaRef,
    parts: &[Part],
) -> Result<RecordBatch, CompactorError> {
    let mut batches = Vec::new();
    for part in parts {
        let bytes = store
            .get(&Path::from(part.meta.path.clone()))
            .await?
            .bytes()
            .await?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)?.build()?;
        for batch in reader {
            batches.push(adapt_to_schema(batch?, &schema)?);
        }
    }
    Ok(concat_batches(&schema, &batches)?)
}

/// Adapts a file batch to the target schema: columns the file predates are
/// filled with NULLs (additive schema evolution).
fn adapt_to_schema(batch: RecordBatch, target: &SchemaRef) -> Result<RecordBatch, CompactorError> {
    let columns = target
        .fields()
        .iter()
        .map(|field| match batch.schema().index_of(field.name()) {
            Ok(idx) => batch.column(idx).clone(),
            Err(_) => arrow::array::new_null_array(field.data_type(), batch.num_rows()),
        })
        .collect::<Vec<_>>();
    Ok(RecordBatch::try_new(target.clone(), columns)?)
}

/// Lexicographic sort by the named columns (in order).
pub fn sort_batch(batch: &RecordBatch, sort_key: &[String]) -> Result<RecordBatch, CompactorError> {
    let mut columns = Vec::with_capacity(sort_key.len());
    for name in sort_key {
        let idx = batch
            .schema()
            .index_of(name)
            .map_err(|_| CompactorError::MissingColumn(name.clone()))?;
        columns.push(SortColumn {
            values: batch.column(idx).clone(),
            options: None,
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

/// Splits a batch (already sorted with `packing_key` as primary sort column)
/// into one contiguous slice per distinct key value.
pub fn split_by_key(
    batch: &RecordBatch,
    packing_key: &str,
) -> Result<Vec<(i64, RecordBatch)>, CompactorError> {
    let keys = int64_column(batch, packing_key)?;
    let mut out = Vec::new();
    let mut start = 0usize;
    for i in 1..=keys.len() {
        if i == keys.len() || keys.value(i) != keys.value(start) {
            out.push((keys.value(start), batch.slice(start, i - start)));
            start = i;
        }
    }
    Ok(out)
}

/// Min/max of the packing column.
pub fn key_range(batch: &RecordBatch, packing_key: &str) -> Result<(i64, i64), CompactorError> {
    let keys = int64_column(batch, packing_key)?;
    let min = arrow::compute::min(keys)
        .ok_or_else(|| CompactorError::MissingColumn(packing_key.to_string()))?;
    let max = arrow::compute::max(keys)
        .ok_or_else(|| CompactorError::MissingColumn(packing_key.to_string()))?;
    Ok((min, max))
}

/// Plans merge output chunks per the placement policy, over a batch already
/// sorted with `packing_key` as the primary sort column. Chunks are
/// contiguous slices in key order, so every output set is key-disjoint —
/// one merge's outputs form one "run". `bytes_per_row` is the caller's
/// encoded-size estimate (input parts' size/rows), used to hit
/// `SizeTargeted` before encoding; a key run bigger than the target stays
/// whole in its own chunk (`min == max`), never split mid-key.
pub fn plan_chunks(
    sorted: &RecordBatch,
    packing_key: &str,
    placement: &Placement,
    bytes_per_row: f64,
) -> Result<Vec<(i64, i64, RecordBatch)>, CompactorError> {
    match placement {
        Placement::Packed => {
            let (min, max) = key_range(sorted, packing_key)?;
            Ok(vec![(min, max, sorted.clone())])
        }
        Placement::Separated => Ok(split_by_key(sorted, packing_key)?
            .into_iter()
            .map(|(key, batch)| (key, key, batch))
            .collect()),
        Placement::SizeTargeted(target) => {
            let target = (*target).max(1) as f64;
            let bytes_per_row = bytes_per_row.max(1.0);
            let mut chunks = Vec::new();
            let mut start = 0usize; // first row of the open chunk
            let mut rows = 0usize; // rows in the open chunk
            let (mut min, mut max) = (0i64, 0i64);
            for (key, run) in split_by_key(sorted, packing_key)? {
                if rows > 0 && (rows + run.num_rows()) as f64 * bytes_per_row > target {
                    chunks.push((min, max, sorted.slice(start, rows)));
                    start += rows;
                    rows = 0;
                }
                if rows == 0 {
                    min = key;
                }
                max = key;
                rows += run.num_rows();
            }
            if rows > 0 {
                chunks.push((min, max, sorted.slice(start, rows)));
            }
            Ok(chunks)
        }
    }
}

/// ZSTD Parquet bytes for one batch, declaring `sort_key` (whichever names are
/// present in the batch, in order) as the file's sorting columns so readers can
/// trust the declared ordering. The batch is assumed already sorted by them.
pub fn batch_to_parquet(
    batch: &RecordBatch,
    sort_key: &[String],
) -> Result<Vec<u8>, CompactorError> {
    let sorting: Vec<parquet::file::metadata::SortingColumn> = sort_key
        .iter()
        .filter_map(|name| batch.schema().index_of(name).ok())
        .map(|idx| parquet::file::metadata::SortingColumn {
            column_idx: idx as i32,
            descending: false,
            nulls_first: false,
        })
        .collect();
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .set_sorting_columns(Some(sorting))
        .build();
    let mut bytes = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut bytes, batch.schema(), Some(props))?;
    writer.write(batch)?;
    writer.close()?;
    Ok(bytes)
}

pub(crate) fn int64_column<'a>(
    batch: &'a RecordBatch,
    name: &str,
) -> Result<&'a Int64Array, CompactorError> {
    let idx = batch
        .schema()
        .index_of(name)
        .map_err(|_| CompactorError::MissingColumn(name.to_string()))?;
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| CompactorError::NotInt64(name.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use serde_json::json;
    use ukiel_core::{CommitId, HypertableId, PartId, PartMeta, arrow_schema_from_json};
    use ukiel_ingest::rows_to_parquet;

    fn schema_json() -> serde_json::Value {
        json!({"fields": [
            {"name": "tenant_id", "type": "int64"},
            {"name": "ts", "type": "timestamp_ms"},
            {"name": "payload", "type": "utf8"}
        ]})
    }

    fn part(path: &str, meta_src: &ukiel_ingest::EncodedPart) -> Part {
        Part {
            id: PartId(0),
            hypertable_id: HypertableId(1),
            meta: PartMeta {
                path: path.to_string(),
                partition_values: json!({}),
                packing_key_min: meta_src.key_min,
                packing_key_max: meta_src.key_max,
                row_count: meta_src.row_count,
                size_bytes: meta_src.bytes.len() as i64,
                level: 0,
                column_stats: None,
            },
            created_by_commit: CommitId(0),
        }
    }

    #[tokio::test]
    async fn merge_sort_split_round_trip() {
        let arrow_schema = Arc::new(arrow_schema_from_json(&schema_json()).unwrap());
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

        // Two unsorted-across-files L0 parts: keys interleaved.
        let f1 = rows_to_parquet(
            &arrow_schema,
            "tenant_id",
            "ts",
            vec![
                json!({"tenant_id": 2, "ts": 20, "payload": "b1"}),
                json!({"tenant_id": 7, "ts": 10, "payload": "c1"}),
            ],
        )
        .unwrap();
        let f2 = rows_to_parquet(
            &arrow_schema,
            "tenant_id",
            "ts",
            vec![
                json!({"tenant_id": 1, "ts": 30, "payload": "a1"}),
                json!({"tenant_id": 7, "ts": 5,  "payload": "c0"}),
            ],
        )
        .unwrap();
        store
            .put(&Path::from("p1"), f1.bytes.clone().into())
            .await
            .unwrap();
        store
            .put(&Path::from("p2"), f2.bytes.clone().into())
            .await
            .unwrap();
        let parts = vec![part("p1", &f1), part("p2", &f2)];

        let merged = read_parts_to_batch(&store, arrow_schema.clone(), &parts)
            .await
            .unwrap();
        assert_eq!(merged.num_rows(), 4);

        let sorted = sort_batch(&merged, &["tenant_id".to_string(), "ts".to_string()]).unwrap();
        let keys = int64_column(&sorted, "tenant_id").unwrap();
        assert_eq!(keys.values().as_ref(), &[1, 2, 7, 7]);
        let ts = int64_column(&sorted, "ts").unwrap();
        assert_eq!(ts.values().as_ref(), &[30, 20, 5, 10]); // ts sorted within key 7

        assert_eq!(key_range(&sorted, "tenant_id").unwrap(), (1, 7));

        let splits = split_by_key(&sorted, "tenant_id").unwrap();
        let split_keys: Vec<i64> = splits.iter().map(|(k, _)| *k).collect();
        assert_eq!(split_keys, vec![1, 2, 7]);
        assert_eq!(splits[2].1.num_rows(), 2);

        // Encode a slice and read it back.
        let bytes =
            batch_to_parquet(&splits[2].1, &["tenant_id".to_string(), "ts".to_string()]).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes))
            .unwrap()
            .build()
            .unwrap();
        let rows: usize = reader.map(|b| b.unwrap().num_rows()).sum();
        assert_eq!(rows, 2);
    }

    /// (tenant_id, ts, payload) batch sorted by tenant: run lengths per key.
    fn sorted_batch(runs: &[(i64, usize)]) -> RecordBatch {
        let arrow_schema = Arc::new(arrow_schema_from_json(&schema_json()).unwrap());
        let mut tenants = Vec::new();
        let mut ts = Vec::new();
        for &(key, n) in runs {
            for i in 0..n {
                tenants.push(key);
                ts.push(i as i64);
            }
        }
        let payload: arrow::array::StringArray =
            tenants.iter().map(|k| Some(format!("p{k}"))).collect();
        RecordBatch::try_new(
            arrow_schema,
            vec![
                Arc::new(Int64Array::from(tenants)),
                Arc::new(Int64Array::from(ts)),
                Arc::new(payload),
            ],
        )
        .unwrap()
    }

    fn ranges_and_rows(chunks: &[(i64, i64, RecordBatch)]) -> Vec<(i64, i64, usize)> {
        chunks
            .iter()
            .map(|(min, max, b)| (*min, *max, b.num_rows()))
            .collect()
    }

    #[test]
    fn plan_chunks_packed_and_separated_match_old_semantics() {
        use ukiel_core::Placement;
        let batch = sorted_batch(&[(1, 3), (2, 3), (3, 6), (4, 1)]);

        let packed = plan_chunks(&batch, "tenant_id", &Placement::Packed, 10.0).unwrap();
        assert_eq!(ranges_and_rows(&packed), vec![(1, 4, 13)]);

        let separated = plan_chunks(&batch, "tenant_id", &Placement::Separated, 10.0).unwrap();
        assert_eq!(
            ranges_and_rows(&separated),
            vec![(1, 1, 3), (2, 2, 3), (3, 3, 6), (4, 4, 1)]
        );
    }

    #[test]
    fn plan_chunks_size_targeted_packs_small_keys_and_cuts_at_boundaries() {
        use ukiel_core::Placement;
        let batch = sorted_batch(&[(1, 3), (2, 3), (3, 6), (4, 1)]);

        // 10 bytes/row, 100-byte target: k1+k2 = 60 fits, +k3 would be 120
        // -> cut; k3+k4 = 70 fits.
        let chunks = plan_chunks(&batch, "tenant_id", &Placement::SizeTargeted(100), 10.0).unwrap();
        assert_eq!(ranges_and_rows(&chunks), vec![(1, 2, 6), (3, 4, 7)]);

        // Chunks are contiguous, ordered, disjoint, and lossless.
        let total: usize = chunks.iter().map(|(_, _, b)| b.num_rows()).sum();
        assert_eq!(total, batch.num_rows());
        for w in chunks.windows(2) {
            assert!(w[0].1 < w[1].0, "overlapping chunk ranges");
        }
    }

    #[test]
    fn plan_chunks_isolates_keys_bigger_than_the_target() {
        use ukiel_core::Placement;
        let batch = sorted_batch(&[(1, 3), (2, 3), (3, 6), (4, 1)]);

        // 40-byte target: every key run alone overflows any shared chunk;
        // the 60-byte whale (k3) still lands whole in its own chunk.
        let chunks = plan_chunks(&batch, "tenant_id", &Placement::SizeTargeted(40), 10.0).unwrap();
        assert_eq!(
            ranges_and_rows(&chunks),
            vec![(1, 1, 3), (2, 2, 3), (3, 3, 6), (4, 4, 1)]
        );
    }
}

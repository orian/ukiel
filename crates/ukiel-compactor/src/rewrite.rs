//! The shared rewrite engine: read parts -> merge -> sort -> split -> encode.
//! Used by both compaction (merge small files) and deletion (rewrite without
//! one key). Pure functions over Arrow batches; no catalog access.

use std::sync::Arc;

use arrow::array::{Array, Int64Array, RecordBatch};
use arrow::compute::concat_batches;
use arrow::datatypes::SchemaRef;
use futures::StreamExt;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::async_reader::{ParquetObjectReader, ParquetRecordBatchStreamBuilder};
use parquet::arrow::async_writer::{AsyncArrowWriter, ParquetObjectWriter};
use ukiel_core::stats::Int64StatsAccumulator;
use ukiel_core::{Part, Placement, TableColumns, WriteOpts};

use crate::CompactorError;
use futures::Stream;

/// Batch size for merge input streams: small enough that K streams stay cheap,
/// large enough to amortize decode (K × `MERGE_BATCH_ROWS` rows resident).
pub const MERGE_BATCH_ROWS: usize = 8 * 1024;

/// One async, schema-adapted, materialized-recomputed batch stream over a part,
/// carrying its path for error attribution (the merge guard names the file).
pub struct PartBatchStream {
    pub path: String,
    pub stream: BoxStream<'static, Result<RecordBatch, CompactorError>>,
}

/// One async batch stream per input part. Range-reads via `ParquetObjectReader`
/// (O(row-group) memory per stream — the file is never resident), applying the
/// existing `adapt_to_schema` (additive evolution: old files NULL-fill new
/// columns) and `apply_defaults_and_materialized` (organic backfill) **per
/// batch** — both row-wise deterministic, so per-batch ≡ the old post-concat
/// application. `read_parts_to_batch` is untouched (deletion still uses it).
pub async fn part_streams(
    store: &Arc<dyn ObjectStore>,
    schema: SchemaRef,
    cols: &TableColumns,
    parts: &[Part],
) -> Result<Vec<PartBatchStream>, CompactorError> {
    let mut out = Vec::with_capacity(parts.len());
    for part in parts {
        let path = part.meta.path.clone();
        let reader = ParquetObjectReader::new(store.clone(), Path::from(path.clone()));
        let raw = ParquetRecordBatchStreamBuilder::new(reader)
            .await
            .map_err(|source| CompactorError::PartRead {
                path: path.clone(),
                source,
            })?
            .with_batch_size(MERGE_BATCH_ROWS)
            .build()
            .map_err(|source| CompactorError::PartRead {
                path: path.clone(),
                source,
            })?;
        let schema = schema.clone();
        let cols = cols.clone();
        let err_path = path.clone();
        let stream = raw
            .map(move |batch| {
                let batch = batch.map_err(|source| CompactorError::PartRead {
                    path: err_path.clone(),
                    source,
                })?;
                let adapted = adapt_to_schema(batch, &schema)?;
                Ok(ukiel_expr::apply_defaults_and_materialized(adapted, &cols)?)
            })
            .boxed();
        out.push(PartBatchStream { path, stream });
    }
    Ok(out)
}

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

/// Lexicographic sort by the named columns (in order). Delegates to the shared
/// `ukiel_core::sorting` implementation so L0 (ingest) and L1+ (compaction)
/// ordering — including nulls placement — can never drift.
pub fn sort_batch(batch: &RecordBatch, sort_key: &[String]) -> Result<RecordBatch, CompactorError> {
    Ok(ukiel_core::sort_batch(batch, sort_key)?)
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
    let props = ukiel_core::sorted_writer_props(&batch.schema(), sort_key);
    let mut bytes = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut bytes, batch.schema(), Some(props))?;
    writer.write(batch)?;
    writer.close()?;
    Ok(bytes)
}

/// Like `batch_to_parquet` but with explicit layout `opts` (level → compression,
/// delta-ts, blooms, row-group cap). One row group per write, cap-split.
pub fn batch_to_parquet_opts(
    batch: &RecordBatch,
    sort_key: &[String],
    opts: &ukiel_core::WriteOpts,
) -> Result<Vec<u8>, CompactorError> {
    let props = ukiel_core::writer_props(&batch.schema(), sort_key, opts);
    let mut bytes = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut bytes, batch.schema(), Some(props))?;
    writer.write(batch)?;
    writer.close()?;
    Ok(bytes)
}

/// Writes one file whose row groups end on packing-key boundaries (Tier-3 #7):
/// each per-key slice (from `split_by_key`, so the batch must be key-sorted)
/// streams into one `ArrowWriter`, and the current group is flushed before a
/// slice that would overflow `opts.max_row_group_rows`. Tiny keys share a
/// group; a key larger than the cap spans several (the writer's own cap-split).
/// Consecutive groups' key ranges never interleave, so per-group key stats
/// prune exactly for the isolation predicate.
pub fn batch_to_parquet_key_aligned(
    batch: &RecordBatch,
    packing_key: &str,
    sort_key: &[String],
    opts: &ukiel_core::WriteOpts,
) -> Result<Vec<u8>, CompactorError> {
    let props = ukiel_core::writer_props(&batch.schema(), sort_key, opts);
    let mut bytes = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut bytes, batch.schema(), Some(props))?;
    for (_key, slice) in split_by_key(batch, packing_key)? {
        if writer.in_progress_rows() > 0
            && writer.in_progress_rows() + slice.num_rows() > opts.max_row_group_rows
        {
            writer.flush()?; // end the row group before the next key run
        }
        writer.write(&slice)?;
    }
    writer.close()?;
    Ok(bytes)
}

/// Callback fired before an output file's first byte — the caller registers
/// upload intent here (plan-9 flow, now per file).
pub type OnOpen = Box<dyn FnMut(String) -> BoxFuture<'static, Result<(), CompactorError>> + Send>;
/// Generates the next output file's object-store path.
pub type NextPath = Box<dyn FnMut() -> String + Send>;

/// One written output file's catalog metadata.
#[derive(Debug)]
pub struct ChunkOutput {
    pub path: String,
    pub key_min: i64,
    pub key_max: i64,
    pub row_count: i64,
    pub size_bytes: i64,
    pub column_stats: Option<serde_json::Value>,
}

/// One open output file being streamed to the object store.
struct OpenFile {
    writer: AsyncArrowWriter<ParquetObjectWriter>,
    path: String,
    key_min: i64,
    key_max: i64,
    current_key: i64,
    rows: i64,
    stats: Int64StatsAccumulator,
    /// A `SizeTargeted` whale continuation: a single-key dedicated file that
    /// must not absorb a following different key.
    dedicated: bool,
}

impl OpenFile {
    async fn write_slice(&mut self, key: i64, slice: &RecordBatch) -> Result<(), CompactorError> {
        self.writer.write(slice).await?;
        self.key_max = self.key_max.max(key);
        self.current_key = key;
        self.rows += slice.num_rows() as i64;
        self.stats.update(slice);
        Ok(())
    }

    /// Encoded bytes so far (flushed row groups + buffered) — actual size, not
    /// the retired `bytes_per_row` estimate.
    fn encoded_size(&self) -> usize {
        self.writer.bytes_written() + self.writer.in_progress_size()
    }

    async fn close(self, store: &Arc<dyn ObjectStore>) -> Result<ChunkOutput, CompactorError> {
        let OpenFile {
            writer,
            path,
            key_min,
            key_max,
            rows,
            stats,
            ..
        } = self;
        writer.close().await?;
        let size_bytes = store.head(&Path::from(path.clone())).await?.size as i64;
        Ok(ChunkOutput {
            path,
            key_min,
            key_max,
            row_count: rows,
            size_bytes,
            column_stats: stats.finish(),
        })
    }
}

/// Consumes a sorted batch stream and writes placement-shaped output files via
/// multipart uploads, cutting on the writer's **actual encoded size** (not the
/// old `bytes_per_row` estimate). `SizeTargeted` shares small keys and
/// **whale-splits** a single key whose run exceeds the target into ts-sliced
/// dedicated files (`key_min == key_max`), which plan-12 ts stats then prune
/// and `delete_key` drops metadata-only. Every file uses plan-14
/// `writer_props` and the key-aligned row-group flush rule.
pub struct StreamingChunkWriter {
    store: Arc<dyn ObjectStore>,
    schema: SchemaRef,
    opts: WriteOpts,
    placement: Placement,
    packing_key: String,
    sort_key: Vec<String>,
    next_path: NextPath,
    on_open: OnOpen,
    /// The `current_key` of the last file closed — a same-key continuation is a
    /// whale slice (opened `dedicated`).
    last_closed_key: Option<i64>,
}

impl StreamingChunkWriter {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        store: Arc<dyn ObjectStore>,
        schema: SchemaRef,
        opts: WriteOpts,
        placement: Placement,
        packing_key: String,
        sort_key: Vec<String>,
        next_path: NextPath,
        on_open: OnOpen,
    ) -> Self {
        Self {
            store,
            schema,
            opts,
            placement,
            packing_key,
            sort_key,
            next_path,
            on_open,
            last_closed_key: None,
        }
    }

    async fn open_file(&mut self, key: i64, dedicated: bool) -> Result<OpenFile, CompactorError> {
        let path = (self.next_path)();
        (self.on_open)(path.clone()).await?;
        let props = ukiel_core::writer_props(&self.schema, &self.sort_key, &self.opts);
        let obj = ParquetObjectWriter::new(self.store.clone(), Path::from(path.clone()));
        let writer = AsyncArrowWriter::try_new(obj, self.schema.clone(), Some(props))?;
        Ok(OpenFile {
            writer,
            path,
            key_min: key,
            key_max: key,
            current_key: key,
            rows: 0,
            stats: Int64StatsAccumulator::default(),
            dedicated,
        })
    }

    pub async fn write(
        mut self,
        stream: impl Stream<Item = Result<RecordBatch, CompactorError>>,
    ) -> Result<Vec<ChunkOutput>, CompactorError> {
        futures::pin_mut!(stream);
        let mut outputs = Vec::new();
        let mut open: Option<OpenFile> = None;
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            for (k, run) in split_by_key(&batch, &self.packing_key)? {
                self.consume_key_run(&mut open, &mut outputs, k, run)
                    .await?;
            }
        }
        if let Some(f) = open.take() {
            outputs.push(f.close(&self.store).await?);
        }
        Ok(outputs)
    }

    async fn consume_key_run(
        &mut self,
        open: &mut Option<OpenFile>,
        outputs: &mut Vec<ChunkOutput>,
        k: i64,
        run: RecordBatch,
    ) -> Result<(), CompactorError> {
        // Cut before a new key when placement demands it.
        if let Some(f) = open.as_ref() {
            let cut = match &self.placement {
                Placement::Packed => false,
                Placement::Separated => f.current_key != k,
                // Cut at the key boundary when the file is full, or when it is a
                // dedicated whale file (which must not absorb a different key).
                Placement::SizeTargeted(n) => {
                    f.current_key != k && (f.encoded_size() >= *n as usize || f.dedicated)
                }
            };
            if cut {
                let f = open.take().unwrap();
                self.last_closed_key = Some(f.current_key);
                outputs.push(f.close(&self.store).await?);
            }
        }

        let cap = self.opts.max_row_group_rows.max(1);
        let run_rows = run.num_rows();
        let mut start = 0;
        while start < run_rows {
            if open.is_none() {
                let dedicated = self.last_closed_key == Some(k);
                *open = Some(self.open_file(k, dedicated).await?);
            }
            let f = open.as_mut().unwrap();
            // Key-aligned row groups: end the current group before this run
            // would overflow the cap (plan-14 rule, streaming form).
            if f.writer.in_progress_rows() > 0
                && f.writer.in_progress_rows() + (run_rows - start) > cap
            {
                f.writer.flush().await?;
            }
            let take = (run_rows - start).min(cap);
            f.write_slice(k, &run.slice(start, take)).await?;
            start += take;

            // SizeTargeted: cut once the file reaches the target (small keys
            // share until here; a whale key cuts mid-run into dedicated slices).
            if let Placement::SizeTargeted(n) = &self.placement
                && f.encoded_size() >= *n as usize
            {
                let f = open.take().unwrap();
                self.last_closed_key = Some(f.current_key);
                outputs.push(f.close(&self.store).await?);
            }
        }
        Ok(())
    }
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
    use futures::FutureExt;
    use object_store::memory::InMemory;
    use serde_json::json;
    use ukiel_core::{CommitId, HypertableId, PartId, PartMeta, arrow_schema_from_json};
    use ukiel_ingest::rows_to_parquet;

    // --- StreamingChunkWriter (Task 4) test helpers -------------------------

    fn kv_schema() -> SchemaRef {
        use arrow::datatypes::{DataType, Field, Schema};
        Arc::new(Schema::new(vec![
            Field::new("tenant_id", DataType::Int64, false),
            Field::new("ts", DataType::Int64, true),
        ]))
    }

    /// A key-sorted batch: `key` repeated `n` times with ts = 0..n.
    fn kv_run(key: i64, n: usize) -> RecordBatch {
        RecordBatch::try_new(
            kv_schema(),
            vec![
                Arc::new(Int64Array::from(vec![key; n])),
                Arc::new(Int64Array::from((0..n as i64).collect::<Vec<_>>())),
            ],
        )
        .unwrap()
    }

    fn one_stream(
        batches: Vec<RecordBatch>,
    ) -> impl Stream<Item = Result<RecordBatch, CompactorError>> {
        futures::stream::iter(batches.into_iter().map(Ok))
    }

    /// Builds a writer that records `on_open` paths (asserting the file does not
    /// yet exist) and generates sequential paths.
    fn writer_over(
        store: &Arc<dyn ObjectStore>,
        placement: Placement,
        opts: WriteOpts,
        opened: Arc<std::sync::Mutex<Vec<String>>>,
    ) -> StreamingChunkWriter {
        let mut counter = 0;
        let next_path: NextPath = Box::new(move || {
            counter += 1;
            format!("out/{counter}.parquet")
        });
        let store_c = store.clone();
        let on_open: OnOpen = Box::new(move |path| {
            let opened = opened.clone();
            let store = store_c.clone();
            async move {
                assert!(
                    store.head(&Path::from(path.clone())).await.is_err(),
                    "on_open must fire before the file exists"
                );
                opened.lock().unwrap().push(path);
                Ok(())
            }
            .boxed()
        });
        StreamingChunkWriter::new(
            store.clone(),
            kv_schema(),
            opts,
            placement,
            "tenant_id".to_string(),
            vec!["tenant_id".to_string(), "ts".to_string()],
            next_path,
            on_open,
        )
    }

    #[tokio::test]
    async fn separated_writes_one_file_per_key() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let opened = Arc::new(std::sync::Mutex::new(Vec::new()));
        let w = writer_over(
            &store,
            Placement::Separated,
            WriteOpts::for_level(1),
            opened.clone(),
        );
        // A key spanning two batches must still land in ONE file.
        let out = w
            .write(one_stream(vec![kv_run(1, 3), kv_run(1, 2), kv_run(2, 4)]))
            .await
            .unwrap();
        assert_eq!(out.len(), 2, "one file per key");
        assert_eq!((out[0].key_min, out[0].key_max), (1, 1));
        assert_eq!(out[0].row_count, 5);
        assert_eq!((out[1].key_min, out[1].key_max), (2, 2));
        assert_eq!(opened.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn packed_writes_one_file_with_key_aligned_groups() {
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let opened = Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut opts = WriteOpts::for_level(1);
        opts.max_row_group_rows = 4;
        opts.ts_column = Some("ts".to_string());
        let w = writer_over(&store, Placement::Packed, opts, opened.clone());
        let out = w
            .write(one_stream(vec![kv_run(1, 3), kv_run(2, 3), kv_run(3, 6)]))
            .await
            .unwrap();
        assert_eq!(out.len(), 1, "packed: one file");
        assert_eq!(out[0].row_count, 12);
        assert_eq!((out[0].key_min, out[0].key_max), (1, 3));

        let bytes = store
            .get(&Path::from(out[0].path.clone()))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let md = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .unwrap()
            .metadata()
            .clone();
        let rg0 = md.row_group(0);
        assert!(matches!(
            rg0.column(0).compression(),
            parquet::basic::Compression::ZSTD(_)
        ));
        assert!(rg0.sorting_columns().unwrap().iter().all(|c| c.nulls_first));
        // Row groups end on key boundaries (no group interleaves keys).
        for rg in md.row_groups() {
            if let parquet::file::statistics::Statistics::Int64(v) =
                rg.column(0).statistics().unwrap()
            {
                assert_eq!(v.min_opt(), v.max_opt(), "row group spans one key");
            }
        }
    }

    #[tokio::test]
    async fn size_targeted_whale_splits_big_key_into_dedicated_slices() {
        use arrow::array::StringArray;
        use arrow::datatypes::{DataType, Field, Schema};
        // Distinct payloads so encoded size grows ~with rows (limits compression).
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("tenant_id", DataType::Int64, false),
            Field::new("ts", DataType::Int64, true),
            Field::new("payload", DataType::Utf8, true),
        ]));
        let run = |key: i64, n: usize| {
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(vec![key; n])),
                    Arc::new(Int64Array::from((0..n as i64).collect::<Vec<_>>())),
                    Arc::new(StringArray::from(
                        (0..n)
                            .map(|i| format!("{key}-{i}-payload-{}", i * 7 + 3))
                            .collect::<Vec<_>>(),
                    )),
                ],
            )
            .unwrap()
        };

        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let sk = vec!["tenant_id".to_string(), "ts".to_string()];
        let mut opts = WriteOpts::for_level(1);
        opts.max_row_group_rows = 128;
        let whale = run(3, 3000);
        let whale_bytes = batch_to_parquet_opts(&whale, &sk, &opts).unwrap().len();
        let target = (whale_bytes / 6).max(1) as i64;

        let mut counter = 0;
        let next_path: NextPath = Box::new(move || {
            counter += 1;
            format!("w/{counter}.parquet")
        });
        let on_open: OnOpen =
            Box::new(|_p: String| async move { Ok::<(), CompactorError>(()) }.boxed());
        let w = StreamingChunkWriter::new(
            store.clone(),
            schema.clone(),
            opts,
            Placement::SizeTargeted(target),
            "tenant_id".to_string(),
            sk.clone(),
            next_path,
            on_open,
        );
        let out = w
            .write(one_stream(vec![
                run(1, 200),
                run(2, 200),
                whale,
                run(4, 200),
            ]))
            .await
            .unwrap();

        assert_eq!(
            out.iter().map(|c| c.row_count).sum::<i64>(),
            200 + 200 + 3000 + 200
        );

        let whale_slices: Vec<_> = out
            .iter()
            .filter(|c| c.key_min == 3 && c.key_max == 3)
            .collect();
        assert!(
            whale_slices.len() >= 2,
            "whale split into ≥2 dedicated slices: {out:?}"
        );
        // Dedicated slices tile key 3's ts range without overlap.
        let mut ranges: Vec<(i64, i64)> = whale_slices
            .iter()
            .map(|c| {
                let s = c.column_stats.as_ref().unwrap();
                (
                    s["ts"]["min"].as_i64().unwrap(),
                    s["ts"]["max"].as_i64().unwrap(),
                )
            })
            .collect();
        ranges.sort();
        assert!(
            ranges.windows(2).all(|w| w[0].1 < w[1].0),
            "slices tile ts without overlap: {ranges:?}"
        );
        // Key 4 lands in its own file, never mixed into a key-3 slice.
        let k4 = out.iter().find(|c| c.key_max == 4).unwrap();
        assert_eq!((k4.key_min, k4.key_max), (4, 4));
    }

    #[tokio::test]
    async fn mid_stream_error_propagates_leaving_completed_files() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let opened = Arc::new(std::sync::Mutex::new(Vec::new()));
        let w = writer_over(
            &store,
            Placement::Separated,
            WriteOpts::for_level(1),
            opened.clone(),
        );
        // Separated closes file 1 at the key-2 boundary, before the error hits.
        let stream = futures::stream::iter(vec![
            Ok(kv_run(1, 40)),
            Ok(kv_run(2, 40)),
            Err(CompactorError::MissingColumn("boom".into())),
        ]);
        let err = w.write(stream).await;
        assert!(
            matches!(err, Err(CompactorError::MissingColumn(_))),
            "error propagates"
        );
        let paths = opened.lock().unwrap().clone();
        assert_eq!(
            paths.len(),
            2,
            "two files opened via on_open before the error"
        );
        assert!(
            store.head(&Path::from(paths[0].clone())).await.is_ok(),
            "the completed file survives as an intent-covered object"
        );
    }

    #[test]
    fn key_aligned_row_groups_end_on_key_boundaries() {
        use arrow::datatypes::{DataType, Field, Schema};
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        use parquet::file::statistics::Statistics;

        // Key runs 1×3, 2×3, 3×6, 4×1 (already key-sorted); cap 4.
        let mut keys = Vec::new();
        for (k, n) in [(1i64, 3), (2, 3), (3, 6), (4, 1)] {
            keys.extend(std::iter::repeat_n(k, n));
        }
        let ts: Vec<i64> = (0..keys.len() as i64).collect();
        let schema = Arc::new(Schema::new(vec![
            Field::new("tenant_id", DataType::Int64, false),
            Field::new("ts", DataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(keys)),
                Arc::new(Int64Array::from(ts)),
            ],
        )
        .unwrap();
        let sort_key = vec!["tenant_id".to_string(), "ts".to_string()];
        let opts = ukiel_core::WriteOpts {
            level: 1,
            ts_column: Some("ts".to_string()),
            bloom_columns: vec![],
            max_row_group_rows: 4,
        };
        let bytes = batch_to_parquet_key_aligned(&batch, "tenant_id", &sort_key, &opts).unwrap();

        let md = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes))
            .unwrap()
            .metadata()
            .clone();
        let counts: Vec<i64> = md.row_groups().iter().map(|rg| rg.num_rows()).collect();
        assert_eq!(counts, vec![3, 3, 4, 3], "row groups end on key boundaries");

        let ranges: Vec<(i64, i64)> = md
            .row_groups()
            .iter()
            .map(|rg| match rg.column(0).statistics().unwrap() {
                Statistics::Int64(v) => (*v.min_opt().unwrap(), *v.max_opt().unwrap()),
                _ => panic!("int64 key stats"),
            })
            .collect();
        assert_eq!(
            ranges,
            vec![(1, 1), (2, 2), (3, 3), (3, 4)],
            "consecutive groups' key ranges never interleave"
        );

        // Shared opts: ZSTD (L1+), delta-ts, nulls-first stamps.
        let rg0 = md.row_group(0);
        assert!(matches!(
            rg0.column(0).compression(),
            parquet::basic::Compression::ZSTD(_)
        ));
        let ts_enc: Vec<_> = rg0.column(1).encodings().collect();
        assert!(ts_enc.contains(&parquet::basic::Encoding::DELTA_BINARY_PACKED));
        assert!(rg0.sorting_columns().unwrap().iter().all(|c| c.nulls_first));
    }

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
    async fn part_streams_adapt_schema_recompute_and_attribute_errors() {
        use futures::StreamExt;
        use ukiel_core::TableColumns;

        // Target: adds `region` and a materialized `doubled = region * 2`.
        let cols = TableColumns::parse(&json!({"fields": [
            {"name": "tenant_id", "type": "int64"},
            {"name": "ts", "type": "timestamp_ms"},
            {"name": "region", "type": "int64"},
            {"name": "doubled", "type": "int64", "materialized": "region * 2"}
        ]}))
        .unwrap();
        let target = Arc::new(cols.physical_schema());
        let sk = vec!["tenant_id".to_string(), "ts".to_string()];
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

        // Part A predates `region`/`doubled` (only tenant_id, ts).
        let schema_a = arrow_schema_from_json(&json!({"fields": [
            {"name": "tenant_id", "type": "int64"},
            {"name": "ts", "type": "timestamp_ms"}
        ]}))
        .unwrap();
        let a = rows_to_parquet(
            &schema_a,
            "tenant_id",
            "ts",
            &sk,
            vec![json!({"tenant_id": 1, "ts": 10})],
        )
        .unwrap();
        store
            .put(&Path::from("a.parquet"), a.bytes.clone().into())
            .await
            .unwrap();
        // Part B carries `region`.
        let schema_b = arrow_schema_from_json(&json!({"fields": [
            {"name": "tenant_id", "type": "int64"},
            {"name": "ts", "type": "timestamp_ms"},
            {"name": "region", "type": "int64"}
        ]}))
        .unwrap();
        let b = rows_to_parquet(
            &schema_b,
            "tenant_id",
            "ts",
            &sk,
            vec![json!({"tenant_id": 2, "ts": 20, "region": 5})],
        )
        .unwrap();
        store
            .put(&Path::from("b.parquet"), b.bytes.clone().into())
            .await
            .unwrap();

        let pa = part("a.parquet", &a);
        let pb = part("b.parquet", &b);
        let mut streams = part_streams(&store, target.clone(), &cols, &[pa, pb])
            .await
            .unwrap();
        assert_eq!(streams[0].path, "a.parquet");

        let ba = streams[0].stream.next().await.unwrap().unwrap();
        assert!(ba.num_rows() <= MERGE_BATCH_ROWS);
        let ri = ba.schema().index_of("region").unwrap();
        assert!(ba.column(ri).is_null(0), "predating column is NULL-filled");

        let bb = streams[1].stream.next().await.unwrap().unwrap();
        let di = bb.schema().index_of("doubled").unwrap();
        let doubled: &Int64Array = bb.column(di).as_any().downcast_ref().unwrap();
        assert_eq!(doubled.value(0), 10, "materialized recomputed per batch");

        // A corrupt file fails with the offending path attributed.
        store
            .put(
                &Path::from("bad.parquet"),
                bytes::Bytes::from_static(b"not parquet").into(),
            )
            .await
            .unwrap();
        let mut bad = part("bad.parquet", &b);
        bad.meta.path = "bad.parquet".to_string();
        match part_streams(&store, target, &cols, &[bad]).await {
            Err(CompactorError::PartRead { path, .. }) => assert_eq!(path, "bad.parquet"),
            other => panic!("expected PartRead(bad.parquet), got {:?}", other.err()),
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
            &["tenant_id".to_string(), "ts".to_string()],
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
            &["tenant_id".to_string(), "ts".to_string()],
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

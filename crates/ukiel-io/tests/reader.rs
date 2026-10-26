use std::{error::Error, sync::Arc};

use arrow::array::{Int64Array, RecordBatch};
use futures::TryStreamExt;
use object_store::{ObjectStore, ObjectStoreExt, memory::InMemory, path::Path};
use parquet::{
    arrow::{
        ArrowWriter,
        arrow_reader::ArrowReaderOptions,
        async_reader::{AsyncFileReader, ParquetRecordBatchStreamBuilder},
    },
    file::{metadata::PageIndexPolicy, properties::WriterProperties},
};
use ukiel_io::ParquetObjectReader;

#[tokio::test]
async fn suffix_metadata_page_indexes_and_ranged_decode_round_trip() {
    let batch = RecordBatch::try_from_iter([(
        "key",
        Arc::new(Int64Array::from(vec![1, 2, 3])) as arrow::array::ArrayRef,
    )])
    .unwrap();
    let mut bytes = Vec::new();
    let mut writer = ArrowWriter::try_new(
        &mut bytes,
        batch.schema(),
        Some(
            WriterProperties::builder()
                .set_offset_index_disabled(false)
                .build(),
        ),
    )
    .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let path = Path::from("data.parquet");
    store.put(&path, bytes.clone().into()).await.unwrap();
    let mut reader = ParquetObjectReader::new(store, path);
    let ranges = reader.get_byte_ranges(vec![0..4, 4..8]).await.unwrap();
    assert_eq!(ranges[0].as_ref(), &bytes[..4]);
    assert_eq!(ranges[1].as_ref(), &bytes[4..8]);
    let options = ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Required);
    let metadata = reader.get_metadata(Some(&options)).await.unwrap();
    assert!(metadata.offset_index().is_some());
    let decoded = ParquetRecordBatchStreamBuilder::new(reader)
        .await
        .unwrap()
        .build()
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(decoded, vec![batch]);
}

#[tokio::test]
async fn missing_object_retains_its_typed_cause_through_metadata_fetch() {
    let mut reader =
        ParquetObjectReader::new(Arc::new(InMemory::new()), Path::from("missing.parquet"));
    let error = reader.get_metadata(None).await.unwrap_err();
    let mut cause: Option<&(dyn Error + 'static)> = Some(&error);
    while let Some(error) = cause {
        if matches!(
            error.downcast_ref::<object_store::Error>(),
            Some(object_store::Error::NotFound { .. })
        ) {
            return;
        }
        cause = error.source();
    }
    panic!("metadata error lost the object-store NotFound cause: {error}");
}

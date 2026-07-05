//! Writes encoded parts to the object store and commits them (with offsets)
//! to the catalog in one transaction.

use std::sync::Arc;

use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use ukiel_catalog::{OffsetRange, PostgresCatalog};
use ukiel_core::{CommitOp, CommitResult, Hypertable, PartMeta};

use crate::IngestError;
use crate::writer::EncodedPart;

pub struct FlushItem {
    pub partition_values: serde_json::Value,
    pub part: EncodedPart,
}

pub struct Flusher {
    catalog: PostgresCatalog,
    store: Arc<dyn ObjectStore>,
}

impl Flusher {
    pub fn new(catalog: PostgresCatalog, store: Arc<dyn ObjectStore>) -> Self {
        Self { catalog, store }
    }

    /// Uploads every item, then commits all parts + offsets atomically.
    /// If the commit fails, uploaded objects are orphaned in the store —
    /// harmless (never referenced) and cleaned up by a later GC worker.
    pub async fn flush(
        &self,
        hypertable: &Hypertable,
        items: Vec<FlushItem>,
        offsets: Vec<OffsetRange>,
    ) -> Result<CommitResult, IngestError> {
        let mut parts = Vec::with_capacity(items.len());
        for item in items {
            let path_str = format!("ht/{}/L0/{}.parquet", hypertable.id, uuid::Uuid::new_v4());
            let size_bytes = item.part.bytes.len() as i64;
            self.store
                .put(&Path::from(path_str.clone()), item.part.bytes.into())
                .await?;
            parts.push(PartMeta {
                path: path_str,
                partition_values: item.partition_values,
                packing_key_min: item.part.key_min,
                packing_key_max: item.part.key_max,
                row_count: item.part.row_count,
                size_bytes,
                level: 0,
                column_stats: None,
            });
        }
        let result = self
            .catalog
            .commit_with_offsets(hypertable.id, CommitOp::Add { parts }, &offsets)
            .await?;
        Ok(result)
    }
}

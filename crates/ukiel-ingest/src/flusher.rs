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
    /// Every part's path is registered as upload intent BEFORE any upload, so a
    /// crash between upload and commit leaves a discoverable orphan for GC
    /// (never an untracked object). If the commit fails, uploaded objects are
    /// orphaned in the store — harmless (never referenced) and reaped by GC.
    pub async fn flush(
        &self,
        hypertable: &Hypertable,
        items: Vec<FlushItem>,
        offsets: Vec<OffsetRange>,
    ) -> Result<CommitResult, IngestError> {
        // Assign every part's path up front and record upload intent BEFORE any
        // upload, so a crash between upload and commit leaves a discoverable
        // orphan for GC (never an untracked object).
        let prepared: Vec<(String, FlushItem)> = items
            .into_iter()
            .map(|item| {
                let path = format!("ht/{}/L0/{}.parquet", hypertable.id, uuid::Uuid::new_v4());
                (path, item)
            })
            .collect();
        let paths: Vec<String> = prepared.iter().map(|(p, _)| p.clone()).collect();
        self.catalog
            .register_pending_objects(hypertable.id, &paths)
            .await?;

        let mut parts = Vec::with_capacity(prepared.len());
        for (path, item) in prepared {
            let size_bytes = item.part.bytes.len() as i64;
            self.store
                .put(&Path::from(path.clone()), item.part.bytes.into())
                .await?;
            parts.push(PartMeta {
                path,
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

//! Writes encoded parts to the object store and commits them (with offsets)
//! to the catalog in one transaction.

use std::sync::Arc;

use metrics::{counter, histogram};
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use ukiel_catalog::{OffsetRange, PostgresCatalog};
use ukiel_core::{
    CommitOp, CommitResult, Hypertable, HypertableId, IngestRange, OperationIdentity, PartMeta,
};

use crate::IngestError;
use crate::writer::EncodedPart;

/// The version of "what these Kafka records mean" (plan 43).
///
/// It is inside the operation fingerprint, so bumping it makes the *same* offset
/// range a *different* operation. Bump it whenever a code or policy change would
/// map the same Kafka values to different rows — a new event-time bound, a
/// changed poison rule, a different materialized-column definition. Not bumping
/// it after such a change means a replay could reconcile against a commit that
/// no longer represents what this code would produce.
///
/// It is not a schema version and not a build id: an ordinary release that
/// changes nothing about the mapping must not change it, or every deploy would
/// orphan every in-flight identity.
pub const INGEST_TRANSFORMATION_VERSION: u32 = 1;

pub struct FlushItem {
    pub partition_values: serde_json::Value,
    pub part: EncodedPart,
}

/// The identity of "consume exactly these offsets into this hypertable".
///
/// Built from the offset ranges alone — not the rows, not the encoded Parquet,
/// not the generated paths. A retry of this flush reads the same offsets from
/// Kafka and must reconcile to the same operation even though it will produce
/// different files with different UUIDs.
pub fn ingest_identity(
    hypertable_id: HypertableId,
    offsets: &[OffsetRange],
) -> Result<OperationIdentity, IngestError> {
    let ranges: Vec<IngestRange> = offsets
        .iter()
        .map(|o| IngestRange {
            topic: o.topic.clone(),
            partition: o.partition,
            first: o.first,
            last: o.last,
        })
        .collect();
    Ok(OperationIdentity::ingest(
        hypertable_id,
        &ranges,
        INGEST_TRANSFORMATION_VERSION,
    )?)
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
        // Metric inputs captured before `items` is consumed (metrics P1).
        let flush_rows: u64 = items.iter().map(|i| i.part.row_count.max(0) as u64).sum();
        let flush_bytes: u64 = items.iter().map(|i| i.part.bytes.len() as u64).sum();
        let flush_partitions = items.len();
        let started = std::time::Instant::now();

        let result = self.flush_inner(hypertable, items, offsets).await;

        match &result {
            Ok(_) => {
                histogram!("ingest_flush_duration_seconds").record(started.elapsed().as_secs_f64());
                histogram!("ingest_flush_rows").record(flush_rows as f64);
                histogram!("ingest_flush_bytes").record(flush_bytes as f64);
                histogram!("ingest_flush_partitions").record(flush_partitions as f64);
                counter!("ingest_rows_total", "hypertable" => hypertable.name.clone())
                    .increment(flush_rows);
            }
            Err(_) => {
                counter!("ingest_commit_failures_total").increment(1);
            }
        }
        result
    }

    async fn flush_inner(
        &self,
        hypertable: &Hypertable,
        items: Vec<FlushItem>,
        offsets: Vec<OffsetRange>,
    ) -> Result<CommitResult, IngestError> {
        // Before a single byte is uploaded: the operation's identity is the
        // offsets it consumes, and nothing about this attempt. Building it here
        // — rather than at commit time — is also what makes a malformed batch
        // (inverted or overlapping ranges) fail before it costs an upload.
        let identity = ingest_identity(hypertable.id, &offsets)?;

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
            let column_stats = item.part.column_stats.clone();
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
                column_stats,
            });
        }
        // A transport failure *around* this commit leaves the transaction's
        // outcome unknown: it may have committed and lost the acknowledgement.
        // It is never retried in place — that could double-apply it — and the
        // worker is torn down.
        //
        // Recovery was already safe without the answer, because Kafka and the
        // catalog offsets are the log: the rebuilt worker reloads offsets and
        // seeks, replaying the batch only if the commit did not land, and
        // uploaded-but-uncommitted objects stay intent-covered and are reaped as
        // orphans. What the identity adds is that the outcome is now *knowable*:
        // the error carries it, and the supervisor asks the catalog which of the
        // two it was instead of arranging not to need to know.
        let result = self
            .catalog
            .commit_with_offsets(hypertable.id, CommitOp::Add { parts }, &offsets, &identity)
            .await
            .inspect_err(|e| {
                if e.is_recoverable_transport() {
                    metrics::counter!("catalog_ambiguous_mutation_total", "role" => "ingest")
                        .increment(1);
                }
            })?;
        Ok(result)
    }
}

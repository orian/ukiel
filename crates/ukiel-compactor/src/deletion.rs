//! Key deletion (GDPR / per-customer erasure). One atomic REPLACE commit:
//! dedicated files are dropped metadata-only, packed files are rewritten
//! without the key's rows. See the spec's "Placement policy" section.

use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::compute::filter_record_batch;
use arrow::compute::kernels::cmp::neq;
use object_store::ObjectStore;
use object_store::ObjectStoreExt;
use object_store::path::Path;
use ukiel_catalog::PostgresCatalog;
use ukiel_core::{CommitOp, DeleteKeyIntent, Hypertable, OperationIdentity, PartMeta};

use crate::CompactorError;
use crate::rewrite;

/// The version of "what erasing this key means" (plan 43). Inside the operation
/// fingerprint. Bump it when a change would make the same key over the same
/// inputs produce materially different remaining data.
pub const DELETION_TRANSFORMATION_VERSION: u32 = 1;

#[derive(Debug, Default, PartialEq, Eq)]
pub struct DeletionStats {
    pub dropped_parts: usize,
    pub rewritten_parts: usize,
    pub rows_deleted: i64,
}

pub async fn delete_key(
    catalog: &PostgresCatalog,
    store: &Arc<dyn ObjectStore>,
    hypertable: &Hypertable,
    key: i64,
) -> Result<DeletionStats, CompactorError> {
    let overlapping = catalog.live_parts(hypertable.id, Some(key)).await?;
    if overlapping.is_empty() {
        return Ok(DeletionStats::default());
    }

    // Named from the authoritative read, before the first rewrite: "erase this
    // key from exactly these live parts". The rewritten files are the attempt —
    // fresh UUIDs on every try — and stay out of it, so a retry after a lost
    // acknowledgement reconciles instead of erasing twice (which, for a
    // *deletion*, would mean tombstoning parts a peer has since rebuilt).
    let input_parts: Vec<ukiel_core::PartId> = overlapping.iter().map(|p| p.id).collect();
    let identity = OperationIdentity::delete_key(
        hypertable.id,
        DeleteKeyIntent {
            packing_key: key,
            input_parts: &input_parts,
            table_schema: &hypertable.table_schema,
            sort_key: &hypertable.sort_key,
            placement: hypertable.placement,
            transformation_version: DELETION_TRANSFORMATION_VERSION,
        },
    )?;

    let cols = ukiel_core::TableColumns::parse(&hypertable.table_schema)?;
    let schema = Arc::new(cols.physical_schema());
    let mut stats = DeletionStats::default();
    let mut new_parts: Vec<PartMeta> = Vec::new();

    for part in &overlapping {
        if part.meta.packing_key_min == key && part.meta.packing_key_max == key {
            // Wholly owned by the key: tombstone only, no data movement.
            stats.dropped_parts += 1;
            stats.rows_deleted += part.meta.row_count;
            continue;
        }

        // Packed file: rewrite without the key's rows.
        let batch =
            rewrite::read_parts_to_batch(store, schema.clone(), std::slice::from_ref(part)).await?;
        let batch = ukiel_expr::apply_defaults_and_materialized(batch, &cols)?;
        let keys = rewrite::int64_column(&batch, &hypertable.packing_key)?;
        let mask = neq(keys, &Int64Array::new_scalar(key))?;
        let kept = filter_record_batch(&batch, &mask)?;
        stats.rewritten_parts += 1;
        stats.rows_deleted += (batch.num_rows() - kept.num_rows()) as i64;

        if kept.num_rows() == 0 {
            continue; // range overlapped but every row was the key's
        }
        let (key_min, key_max) = rewrite::key_range(&kept, &hypertable.packing_key)?;
        // Rewrite at the original level with key-aligned row groups (the kept
        // batch stays sorted after the filter).
        let opts =
            ukiel_core::WriteOpts::from_columns(&cols, &hypertable.sort_key, part.meta.level);
        let (bytes, spans) = rewrite::batch_to_parquet_key_aligned(
            &kept,
            &hypertable.packing_key,
            &hypertable.sort_key,
            &opts,
        )?;
        let path = format!(
            "ht/{}/L{}/{}.parquet",
            hypertable.id,
            part.meta.level,
            uuid::Uuid::new_v4()
        );
        catalog
            .register_pending_objects(hypertable.id, std::slice::from_ref(&path))
            .await?;
        let size_bytes = bytes.len() as i64;
        store.put(&Path::from(path.clone()), bytes.into()).await?;
        new_parts.push(PartMeta {
            path,
            partition_values: part.meta.partition_values.clone(),
            packing_key_min: key_min,
            packing_key_max: key_max,
            row_count: kept.num_rows() as i64,
            size_bytes,
            level: part.meta.level,
            // Plan 16: the rewritten part's index is rebuilt from the *kept*
            // rows, so the deleted key drops out of the bitmap — which is what
            // makes a later query for it prune this part instead of opening it.
            column_stats: ukiel_core::stats::with_key_index(
                ukiel_core::stats::int64_column_stats(&kept),
                (key_min != key_max)
                    .then(|| ukiel_core::stats::key_bitmap(&kept, &hypertable.packing_key))
                    .flatten(),
                (key_min != key_max).then_some(spans).flatten(),
            ),
        });
    }

    // One atomic swap: readers see all of the key's data or none of it.
    catalog
        .commit(
            hypertable.id,
            CommitOp::Replace {
                old: input_parts,
                new: new_parts,
            },
            Some(&identity),
        )
        .await?;
    Ok(stats)
}

//! Apply a conservative sidecar to an actual Parquet scan.
//!
//! For a probe with a bound sidecar, evaluate the typed predicate for every row group and
//! build a per-file `ParquetAccessPlan` that omits *only* `NoMatch` groups — leaving native
//! statistics/page/Bloom pruning enabled underneath. `Maybe`, `Unknown`, corrupt payloads,
//! and unsupported predicates all keep the group. A spy object store proves a skipped row
//! group's data range is never fetched.

use std::ops::Range;
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::datatypes::SchemaRef;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::datasource::physical_plan::{FileGroup, FileScanConfigBuilder, ParquetSource};
use datafusion::datasource::source::DataSourceExec;
use datafusion::physical_expr::expressions::{binary, col, lit};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use datafusion::scalar::ScalarValue;
use datafusion_datasource_parquet::ParquetAccessPlan;
use object_store::ObjectStore;
use object_store::memory::InMemory;
use object_store::path::Path as ObjPath;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet_lab_contract::SkipManifest;
use parquet_skip_index_core::{Decision, Predicate};

use crate::compare;
use crate::counting_store::{Counters, CountingObjectStore};

/// One row group's decision and its column-data byte span (for the spy proof).
#[derive(Debug, Clone)]
pub struct RowGroupSpan {
    pub file: String,
    pub row_group: usize,
    pub data_start: u64,
    pub data_end: u64,
    pub decision: Decision,
}

/// The pruning ledger for one probe scan.
#[derive(Debug, Clone, serde::Serialize)]
pub struct PruningLedger {
    pub row_groups_total: usize,
    pub custom_no_match: usize,
    pub maybe: usize,
    pub unknown: usize,
    pub selected_row_groups: usize,
    pub decoded_rows: u64,
    pub answer_digest: String,
    pub sidecar_requested_bytes: u64,
}

/// The outcome of a probe scan, with the spy log for correctness proofs.
pub struct ScanOutcome {
    pub ledger: PruningLedger,
    pub fetched_ranges: Vec<(String, Range<u64>)>,
    pub spans: Vec<RowGroupSpan>,
}

/// Scan one equality predicate over the artifact, optionally applying the sidecar. Returns
/// the ledger, the spy log of fetched byte ranges, and every row group's data span.
pub async fn scan_equality(
    files: &[(String, Vec<u8>)],
    column: &str,
    eq_value: ScalarValue,
    core_pred: &Predicate,
    sidecar: Option<(&SkipManifest, &[u8])>,
) -> Result<ScanOutcome> {
    // Load files into a counting in-memory store.
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    for (path, bytes) in files {
        inner
            .put_opts(
                &ObjPath::from(path.as_str()),
                bytes.clone().into(),
                object_store::PutOptions::default(),
            )
            .await?;
    }
    let (store, counters): (Arc<dyn ObjectStore>, Arc<Counters>) = {
        let (c, ctr) = CountingObjectStore::new(inner);
        (c, ctr)
    };

    // Infer the physical schema from the first file.
    let schema: SchemaRef =
        ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::copy_from_slice(&files[0].1))?
            .schema()
            .clone();

    // Per-file access plan + row-group spans.
    let mut partitioned = Vec::with_capacity(files.len());
    let mut spans = Vec::new();
    let mut ledger = PruningLedger {
        row_groups_total: 0,
        custom_no_match: 0,
        maybe: 0,
        unknown: 0,
        selected_row_groups: 0,
        decoded_rows: 0,
        answer_digest: String::new(),
        sidecar_requested_bytes: 0,
    };

    for (path, bytes) in files {
        let meta = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::copy_from_slice(bytes))?
            .metadata()
            .clone();
        let n = meta.num_row_groups();
        ledger.row_groups_total += n;
        let mut plan = ParquetAccessPlan::new_all(n);
        for rg_idx in 0..n {
            let rg = meta.row_group(rg_idx);
            // The row group's column-data byte span, for the spy proof.
            let mut start = u64::MAX;
            let mut end = 0u64;
            for c in rg.columns() {
                let (s, len) = c.byte_range();
                start = start.min(s);
                end = end.max(s + len);
            }
            let decision = sidecar
                .map(|(m, payload)| decide(m, payload, path, rg_idx, column, core_pred))
                .unwrap_or(Decision::Maybe);
            match decision {
                Decision::NoMatch => {
                    ledger.custom_no_match += 1;
                    plan.skip(rg_idx);
                }
                Decision::Maybe => ledger.maybe += 1,
                Decision::Unknown => ledger.unknown += 1,
            }
            spans.push(RowGroupSpan {
                file: path.clone(),
                row_group: rg_idx,
                data_start: start,
                data_end: end,
                decision,
            });
        }
        ledger.selected_row_groups += plan.row_group_indexes().len();
        let mut pf = PartitionedFile::new(path.clone(), bytes.len() as u64);
        // Only attach a plan when it actually prunes; an all-select plan is a no-op. The
        // extension is the `ParquetAccessPlan` itself (by value) — DataFusion downcasts the
        // extension to that exact type, so wrapping it in an `Arc` would be silently ignored.
        if plan.row_group_indexes().len() < n {
            pf = pf.with_extension(plan);
        }
        partitioned.push(pf);
    }

    // Build the scan: predicate pushdown ON (native pruning underneath the access plan).
    let predicate = binary(
        col(column, &schema)?,
        datafusion::logical_expr::Operator::Eq,
        lit(eq_value),
        &schema,
    )?;
    let source = Arc::new(
        ParquetSource::new(schema.clone())
            .with_pushdown_filters(true)
            .with_predicate(predicate),
    );
    let url = ObjectStoreUrl::parse("memory://")?;
    let file_groups: Vec<FileGroup> = partitioned
        .into_iter()
        .map(|f| FileGroup::new(vec![f]))
        .collect();
    let config = FileScanConfigBuilder::new(url.clone(), source)
        .with_file_groups(file_groups)
        .build();
    let exec: Arc<dyn ExecutionPlan> = DataSourceExec::from_data_source(config);

    let ctx = SessionContext::new();
    ctx.register_object_store(url.as_ref(), store);
    let before = counters.snapshot();
    let batches = datafusion::physical_plan::collect(exec, ctx.task_ctx())
        .await
        .context("executing the sidecar scan")?;
    let after = counters.snapshot();
    ledger.sidecar_requested_bytes = 0; // sidecar payload fetch is out-of-band (local blob)
    ledger.decoded_rows = batches.iter().map(|b| b.num_rows() as u64).sum::<u64>();
    let _ = (before, after);
    // Answer digest under multiset semantics (row order not part of the answer).
    ledger.answer_digest = if let Some(first) = batches.first() {
        compare::result_digest(
            &first.schema(),
            &batches,
            compare::ResultSemantics::Multiset,
        )?
    } else {
        compare::result_digest(&schema, &[], compare::ResultSemantics::Multiset)?
    };

    let fetched = counters
        .fetched_ranges
        .lock()
        .map(|g| g.clone())
        .unwrap_or_default();
    Ok(ScanOutcome {
        ledger,
        fetched_ranges: fetched,
        spans,
    })
}

/// Evaluate the sidecar's decision for one (file, row group) on the indexed column.
fn decide(
    manifest: &SkipManifest,
    payload: &[u8],
    file: &str,
    rg: usize,
    column: &str,
    pred: &Predicate,
) -> Decision {
    for c in &manifest.columns {
        if c.column != column || c.file != file {
            continue;
        }
        for r in &c.row_groups {
            if r.row_group as usize != rg {
                continue;
            }
            let end = (r.payload_offset + r.payload_length) as usize;
            let Some(slice) = payload.get(r.payload_offset as usize..end) else {
                return Decision::Unknown;
            };
            return parquet_skip_index_core::evaluate(r.kind, slice, &r.payload_digest, pred);
        }
        // The column is indexed but this row group is not: abstain (keep).
        return Decision::Unknown;
    }
    Decision::Unknown
}

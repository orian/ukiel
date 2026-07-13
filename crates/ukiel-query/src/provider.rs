//! A DataFusion TableProvider that plans scans from the Ukiel catalog and
//! enforces namespace isolation by injecting `packing_key = value` into the
//! physical plan, upstream of any projection.
//!
//! The isolation slice is explicit: `Some(key)` is the product path (every
//! namespace session), `None` is the unscoped operator path (benchmarks —
//! see [`crate::context::operator_session`]), which scans every live part with
//! no isolation predicate. Everything else — catalog pruning, stats pruning,
//! the footer cache, the declared output ordering — is identical in both modes.

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::common::Result as DfResult;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::datasource::physical_plan::{FileGroup, FileScanConfigBuilder, ParquetSource};
use datafusion::datasource::source::DataSourceExec;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{Expr, Operator, TableProviderFilterPushDown};
use datafusion::physical_expr::expressions::{binary, col, lit};
use datafusion::physical_expr::{LexOrdering, PhysicalExpr, PhysicalSortExpr};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::limit::GlobalLimitExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::scalar::ScalarValue;
use datafusion_datasource_parquet::ParquetAccessPlan;
use ukiel_catalog::PostgresCatalog;
use ukiel_core::{Hypertable, TableColumns};

use crate::QueryError;
use crate::metadata_cache::CachingParquetFileReaderFactory;

/// Extracts per-column Int64 bounds from pushed-down filters for catalog
/// part pruning. Conservative by construction: anything unrecognized simply
/// contributes no bound. DataFusion splits conjunctions before pushdown, so
/// top-level binary comparisons cover the practical cases.
fn extract_int64_ranges(
    filters: &[Expr],
    schema: &datafusion::arrow::datatypes::Schema,
) -> Vec<ukiel_core::ColumnRange> {
    use datafusion::logical_expr::BinaryExpr;
    use std::collections::HashMap;

    let mut bounds: HashMap<String, (Option<i64>, Option<i64>)> = HashMap::new();
    let mut apply = |name: &str, op: &Operator, value: i64| {
        let Ok(field) = schema.field_with_name(name) else {
            return;
        };
        if field.data_type() != &datafusion::arrow::datatypes::DataType::Int64 {
            return;
        }
        let entry = bounds.entry(name.to_string()).or_insert((None, None));
        match op {
            Operator::Eq => {
                entry.0 = Some(entry.0.map_or(value, |m| m.max(value)));
                entry.1 = Some(entry.1.map_or(value, |m| m.min(value)));
            }
            Operator::Gt => entry.0 = Some(entry.0.map_or(value + 1, |m| m.max(value + 1))),
            Operator::GtEq => entry.0 = Some(entry.0.map_or(value, |m| m.max(value))),
            Operator::Lt => entry.1 = Some(entry.1.map_or(value - 1, |m| m.min(value - 1))),
            Operator::LtEq => entry.1 = Some(entry.1.map_or(value, |m| m.min(value))),
            _ => {}
        }
    };

    for filter in filters {
        if let Expr::BinaryExpr(BinaryExpr { left, op, right }) = filter {
            match (left.as_ref(), right.as_ref()) {
                (Expr::Column(c), Expr::Literal(ScalarValue::Int64(Some(v)), _)) => {
                    apply(&c.name, op, *v)
                }
                (Expr::Literal(ScalarValue::Int64(Some(v)), _), Expr::Column(c)) => {
                    // literal <op> col: mirror the operator.
                    let mirrored = match op {
                        Operator::Lt => Operator::Gt,
                        Operator::LtEq => Operator::GtEq,
                        Operator::Gt => Operator::Lt,
                        Operator::GtEq => Operator::LtEq,
                        other => *other,
                    };
                    apply(&c.name, &mirrored, *v)
                }
                _ => {}
            }
        }
    }

    bounds
        .into_iter()
        .map(|(column, (min, max))| ukiel_core::ColumnRange { column, min, max })
        .collect()
}

/// Whether the part's key bitmap *proves* it holds no rows for `key`.
///
/// Only a decodable bitmap that answers `false` licenses a skip. Every other
/// state — no `column_stats`, no bitmap entry, a non-string value, an
/// undecodable blob, a negative key — is undecidable and keeps the part. Pruning
/// may only ever remove what it can prove is absent.
fn part_excludes_key(part: &ukiel_core::Part, key: i64) -> bool {
    part.meta
        .column_stats
        .as_ref()
        .and_then(|s| s.get(ukiel_core::stats::PACKING_KEYS_STAT))
        .and_then(|v| v.as_str())
        .and_then(|encoded| ukiel_core::stats::bitmap_contains(encoded, key))
        .is_some_and(|present| !present)
}

/// A per-file `ParquetAccessPlan` skipping every row group whose catalog-stored
/// key span provably excludes `key` (plan 16, Tier-2 #6).
///
/// `None` means "no plan" — read the file the normal way. That covers a part
/// with no spans (everything written before plan 16), malformed spans (wrong
/// arity, non-integer bounds), and the *every group survives* case, where a plan
/// would be a no-op with extra allocation.
///
/// `Some(plan)` where every group is skipped is possible and is handled by the
/// caller, which drops the file from the scan entirely rather than opening it to
/// read nothing.
///
/// Safety: the spans come from the file's own row-group statistics, so a skip is
/// proven, not guessed. DataFusion *reduces* a supplied plan further with its own
/// predicate and statistics pruning — it never widens one — so an over-broad plan
/// costs only performance, and the isolation `FilterExec` sits above the scan
/// regardless.
fn access_plan_for(part: &ukiel_core::Part, key: i64) -> Option<ParquetAccessPlan> {
    let spans = part
        .meta
        .column_stats
        .as_ref()?
        .get(ukiel_core::stats::KEY_ROW_GROUPS_STAT)?
        .as_array()?;
    if spans.is_empty() {
        return None;
    }

    let mut plan = ParquetAccessPlan::new_all(spans.len());
    let mut skipped = 0usize;
    for (i, span) in spans.iter().enumerate() {
        // Malformed spans are positional poison: we cannot trust *any* index if
        // one entry is wrong, so bail out entirely rather than skip a guess.
        let (Some(lo), Some(hi)) = (
            span.get(0).and_then(serde_json::Value::as_i64),
            span.get(1).and_then(serde_json::Value::as_i64),
        ) else {
            return None;
        };
        if key < lo || key > hi {
            plan.skip(i);
            skipped += 1;
        }
    }
    (skipped > 0).then_some(plan)
}

pub struct UkielTableProvider {
    catalog: PostgresCatalog,
    hypertable: Hypertable,
    /// stored + default + materialized — what the Parquet scan produces.
    physical_schema: SchemaRef,
    /// physical + alias — what SQL sees.
    query_schema: SchemaRef,
    columns: TableColumns,
    /// The isolation slice: `Some(packing_key)` scopes the table to one
    /// namespace; `None` is the unscoped operator mode (no isolation).
    slice: Option<i64>,
    store_url: ObjectStoreUrl,
    reader_factory: Arc<CachingParquetFileReaderFactory>,
}

impl fmt::Debug for UkielTableProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UkielTableProvider")
            .field("hypertable", &self.hypertable.name)
            .field("slice", &self.slice)
            .finish()
    }
}

impl UkielTableProvider {
    /// `slice`: `Some(packing_key)` for a namespace-scoped table (the product
    /// path — the only one the HTTP server ever builds); `None` for the
    /// unscoped operator/bench path, which reads every namespace's rows.
    pub fn try_new(
        catalog: PostgresCatalog,
        hypertable: Hypertable,
        slice: Option<i64>,
        store_url: ObjectStoreUrl,
        reader_factory: Arc<CachingParquetFileReaderFactory>,
    ) -> Result<Self, QueryError> {
        let columns = TableColumns::parse(&hypertable.table_schema)?;
        // Plan 39: the query path reads strings as `Utf8View` — **parity with
        // DataFusion's own `schema_force_view_types` default**, which handing the
        // reader an explicit `Utf8` schema silently opted out of. Zero-copy views
        // then flow through repartition and aggregation instead of owned copies;
        // opting out cost +62% peak memory and +218% repartition time on a
        // byte-identical scan (plan 37's H6).
        //
        // Query-side representation ONLY: `TableColumns` stays `Utf8`, so ingest,
        // the compactor and materialized columns are untouched, and the Parquet
        // bytes are identical either way.
        //
        // Both schemas flip together, and that is not cosmetic: if SQL still saw
        // `Utf8`, DataFusion would insert a cast that re-materializes every
        // string and hand the entire win back.
        let physical_schema = Arc::new(crate::view_types::view_typed(&columns.physical_schema()));
        let query_schema = Arc::new(crate::view_types::view_typed(&columns.query_schema()));
        Ok(Self {
            catalog,
            hypertable,
            physical_schema,
            query_schema,
            columns,
            slice,
            store_url,
            reader_factory,
        })
    }
}

#[async_trait]
impl TableProvider for UkielTableProvider {
    fn schema(&self) -> SchemaRef {
        self.query_schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DfResult<Vec<TableProviderFilterPushDown>> {
        // We don't evaluate user filters ourselves; DataFusion re-applies them.
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        // Catalog-pruned file list: only parts whose packing-key range contains
        // this namespace's key (every live part when unscoped). Never lists the
        // object store.
        let ranges = extract_int64_ranges(filters, &self.physical_schema);
        let parts = self
            .catalog
            .live_parts_pruned(self.hypertable.id, self.slice, &ranges)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        // Plan 16: min/max ranges are conservative — a part spanning keys
        // 100..4500 "contains" a tenant that owns no rows in it, and the scan
        // opens the file to find nothing. The packing-key bitmap makes this
        // exact: drop a part only when it *proves* the key is absent.
        //
        // Exactness is the bitmap's job; safety is the keep-by-default rule —
        // a missing index, an undecodable one, or a negative key all yield
        // `None` and keep the part — plus the isolation `FilterExec` below,
        // which is the correctness boundary no matter what pruning decides.
        // Scoped mode only: the operator session has no key to test against.
        //
        // Surviving parts additionally get a catalog-driven `ParquetAccessPlan`
        // (plan 16, #6): the row-group key spans let us pre-skip groups without
        // reading the footer at all. DataFusion only ever *reduces* a supplied
        // plan further, so this composes with predicate and statistics pruning.
        let mut files: Vec<PartitionedFile> = Vec::with_capacity(parts.len());
        for p in &parts {
            let file = PartitionedFile::new(p.meta.path.clone(), p.meta.size_bytes as u64);
            let Some(key) = self.slice else {
                files.push(file);
                continue;
            };
            if part_excludes_key(p, key) {
                continue; // the bitmap proves this part holds none of the key's rows
            }
            match access_plan_for(p, key) {
                // Every row group provably excludes the key: don't open the file
                // at all. Cheaper than handing the reader an all-skip plan, and
                // it keeps the plan's semantics trivially sound.
                Some(plan) if plan.row_group_indexes().is_empty() => continue,
                Some(plan) => files.push(file.with_extension(plan)),
                None => files.push(file),
            }
        }

        // Requested query-schema indices (SELECT * when DataFusion sends none).
        let indices: Vec<usize> = match projection {
            Some(p) => p.clone(),
            None => (0..self.query_schema.fields().len()).collect(),
        };

        // Physical columns the scan must read: requested physical columns,
        // columns referenced by requested alias expressions, and — when scoped —
        // the packing key (the isolation filter needs it even when projected
        // away). Unscoped, the packing key is just another column: reading it
        // unconditionally would tax every operator query with a column it never
        // asked for.
        let mut needed: Vec<String> = match self.slice {
            Some(_) => vec![self.hypertable.packing_key.clone()],
            None => Vec::new(),
        };
        for &i in &indices {
            let spec = &self.columns.specs[i];
            match &spec.kind {
                ukiel_core::ColumnKind::Alias(sql) => {
                    let refs = ukiel_expr::column_refs(sql, &self.physical_schema)
                        .map_err(|e| DataFusionError::External(Box::new(e)))?;
                    needed.extend(refs);
                }
                _ => needed.push(spec.name.clone()),
            }
        }
        let mut scan_projection: Vec<usize> = needed
            .iter()
            .map(|name| self.physical_schema.index_of(name))
            .collect::<Result<Vec<_>, _>>()?;
        scan_projection.sort_unstable();
        scan_projection.dedup();
        let scanned_schema = Arc::new(self.physical_schema.project(&scan_projection)?);

        // Pushdown predicate for the Parquet reader: **the isolation predicate
        // only** (scoped mode). Built against the FILE schema (physical_schema),
        // because pushdown may reference columns the projection drops.
        //
        // The user's own filters are deliberately NOT folded in here, even though
        // they could be. DataFusion already pushes them into this same
        // `ParquetSource` (we enable `pushdown_filters`), so hand-building them a
        // second time made the reader evaluate every predicate column **twice** —
        // measured as exactly 2x `predicate_cache_records` (181.8M vs 90.9M on
        // ClickBench q14) and ~15% of wall, with byte-identical pruning and
        // answers. The isolation predicate is different in kind: DataFusion has no
        // way to know about it, so it is ours to supply.
        //
        // The `FilterExec` below remains the security boundary regardless of what
        // any of this pushes down (plan 37, F3).
        let pushdown: Option<Arc<dyn PhysicalExpr>> = match self.slice {
            Some(key) => Some(binary(
                col(&self.hypertable.packing_key, &self.physical_schema)?,
                Operator::Eq,
                lit(ScalarValue::Int64(Some(key))),
                &self.physical_schema,
            )?),
            None => None,
        };

        let has_predicate = pushdown.is_some();
        let mut source = ParquetSource::new(self.physical_schema.clone())
            .with_pushdown_filters(true)
            .with_reorder_filters(true)
            .with_parquet_file_reader_factory(self.reader_factory.clone());
        if let Some(predicate) = pushdown {
            source = source.with_predicate(predicate);
        }
        let source = Arc::new(source);
        // One group per file: each file is internally sorted, but files may
        // overlap in key ranges — concatenating them into one group would
        // falsely claim a total order. Separate groups also parallelize.
        let file_groups: Vec<FileGroup> =
            files.into_iter().map(|f| FileGroup::new(vec![f])).collect();

        // Declared ordering — predicated scans only; a measured split, not a
        // principle (`docs/notes/2026-07-12-official-suite-gap.md`). Two
        // same-binary-A/B facts on DataFusion 54: (a) on a *filterless* scan
        // the declaration survives to file repartitioning and forces
        // order-preserving mode — whole-file groups instead of balanced
        // byte-range splits, gating low-fan-out layouts on their largest file
        // (packed hits_cb GROUP BY CounterID: a 732 MB single-threaded
        // critical path, 1.54x); (b) on *predicated* scans (a WHERE clause or
        // the isolation slice — every product query) the declaration is
        // cleared from the displayed plan by pushdown, yet dropping it still
        // cost 15–25% wall on a broad band of GROUP BYs via lower achieved
        // scan concurrency (identical plans, metrics, and total CPU; ~1450%
        // vs ~1130% utilization — mechanism in DataFusion not fully pinned).
        // So: declare exactly when a pushdown predicate exists. Sort
        // elimination never fires on 54 either way
        // (`order_by_sort_key_results_stay_correct_and_ordered`); files stamp
        // `sorting_columns` (plan 27) regardless. Declaring the *full*
        // `(packing_key, ts, …)` prefix is the only sound claim: a packed
        // file is NOT ts-sorted on its own (ts resets per key), so a
        // `[ts]`-only claim would be rejected by stats validation.
        let mut builder = FileScanConfigBuilder::new(self.store_url.clone(), source)
            .with_file_groups(file_groups)
            .with_projection_indices(Some(scan_projection))?;
        if has_predicate {
            let mut sort_exprs: Vec<PhysicalSortExpr> = Vec::new();
            for name in &self.hypertable.sort_key {
                match col(name, &self.physical_schema) {
                    Ok(expr) => sort_exprs.push(PhysicalSortExpr::new_default(expr)),
                    Err(_) => break, // column not in the file: ordering prefix ends
                }
            }
            if let Some(ordering) = LexOrdering::new(sort_exprs) {
                builder = builder.with_output_ordering(vec![ordering]);
            }
        }
        let config = builder.build();
        let scan: Arc<dyn ExecutionPlan> = DataSourceExec::from_data_source(config);

        // Mandatory namespace isolation, before projection can drop the column.
        // Built against the scanned (projected) schema. Absent only in unscoped
        // operator mode, which by definition has no namespace to isolate.
        let filtered: Arc<dyn ExecutionPlan> = match self.slice {
            Some(key) => {
                let predicate = binary(
                    col(&self.hypertable.packing_key, &scanned_schema)?,
                    Operator::Eq,
                    lit(ScalarValue::Int64(Some(key))),
                    &scanned_schema,
                )?;
                Arc::new(FilterExec::try_new(predicate, scan)?)
            }
            None => scan,
        };

        // Final projection maps requested query-schema columns onto the scanned
        // schema (alias-referenced columns are present by construction). Runs
        // even for SELECT * so alias columns exist in the output.
        // `TableColumns.specs` is index-aligned with `query_schema`.
        let exprs = indices
            .iter()
            .map(|&i| {
                let field = self.query_schema.field(i);
                let spec = &self.columns.specs[i];
                let expr: Arc<dyn PhysicalExpr> = match &spec.kind {
                    ukiel_core::ColumnKind::Alias(sql) => {
                        // Compile against the **query schema's** declared type,
                        // not `ColumnKind`'s. They agree on everything except
                        // string representation: the query path reads `Utf8View`
                        // (plan 39) while `TableColumns` is the write-side `Utf8`.
                        // An alias projecting `Utf8` into a `Utf8View`-declared
                        // field is a schema mismatch, so the target must be the
                        // field's. `compile` casts to it — one narrow output
                        // column, never the scan.
                        let compiled = ukiel_expr::compile(sql, &scanned_schema, field.data_type())
                            .map_err(|e| DataFusionError::External(Box::new(e)))?;
                        compiled.into_physical()
                    }
                    _ => col(field.name(), &scanned_schema)?,
                };
                Ok((expr, field.name().clone()))
            })
            .collect::<DfResult<Vec<_>>>()?;
        let mut plan: Arc<dyn ExecutionPlan> = Arc::new(ProjectionExec::try_new(exprs, filtered)?);

        if let Some(n) = limit {
            plan = Arc::new(GlobalLimitExec::new(plan, 0, Some(n)));
        }
        Ok(plan)
    }
}

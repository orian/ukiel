//! A DataFusion TableProvider that plans scans from the Ukiel catalog and
//! enforces namespace isolation by injecting `packing_key = value` into the
//! physical plan, upstream of any projection.

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

pub struct UkielTableProvider {
    catalog: PostgresCatalog,
    hypertable: Hypertable,
    /// stored + default + materialized — what the Parquet scan produces.
    physical_schema: SchemaRef,
    /// physical + alias — what SQL sees.
    query_schema: SchemaRef,
    columns: TableColumns,
    packing_key_value: i64,
    store_url: ObjectStoreUrl,
    reader_factory: Arc<CachingParquetFileReaderFactory>,
}

impl fmt::Debug for UkielTableProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UkielTableProvider")
            .field("hypertable", &self.hypertable.name)
            .field("packing_key_value", &self.packing_key_value)
            .finish()
    }
}

impl UkielTableProvider {
    pub fn try_new(
        catalog: PostgresCatalog,
        hypertable: Hypertable,
        packing_key_value: i64,
        store_url: ObjectStoreUrl,
        reader_factory: Arc<CachingParquetFileReaderFactory>,
    ) -> Result<Self, QueryError> {
        let columns = TableColumns::parse(&hypertable.table_schema)?;
        let physical_schema = Arc::new(columns.physical_schema());
        let query_schema = Arc::new(columns.query_schema());
        Ok(Self {
            catalog,
            hypertable,
            physical_schema,
            query_schema,
            columns,
            packing_key_value,
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
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        // Catalog-pruned file list: only parts whose packing-key range
        // contains this namespace's key. Never lists the object store.
        let ranges = extract_int64_ranges(filters, &self.physical_schema);
        let parts = self
            .catalog
            .live_parts_pruned(self.hypertable.id, Some(self.packing_key_value), &ranges)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let files: Vec<PartitionedFile> = parts
            .iter()
            .map(|p| PartitionedFile::new(p.meta.path.clone(), p.meta.size_bytes as u64))
            .collect();

        // Requested query-schema indices (SELECT * when DataFusion sends none).
        let indices: Vec<usize> = match projection {
            Some(p) => p.clone(),
            None => (0..self.query_schema.fields().len()).collect(),
        };

        // Physical columns the scan must read: requested physical columns,
        // columns referenced by requested alias expressions, and the packing
        // key (the isolation filter needs it even when projected away).
        let mut needed: Vec<String> = vec![self.hypertable.packing_key.clone()];
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

        // Pushdown predicate for the Parquet reader: isolation AND every user
        // filter that converts cleanly. Built against the FILE schema
        // (physical_schema) — pushdown may reference unprojected columns.
        // Best-effort by design: anything skipped is re-applied by DataFusion
        // above (Inexact), and the FilterExec below remains the security
        // boundary regardless.
        let file_isolation = binary(
            col(&self.hypertable.packing_key, &self.physical_schema)?,
            Operator::Eq,
            lit(ScalarValue::Int64(Some(self.packing_key_value))),
            &self.physical_schema,
        )?;
        let physical_names: std::collections::HashSet<&str> = self
            .physical_schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        let df_schema =
            datafusion::common::DFSchema::try_from(self.physical_schema.as_ref().clone())?;
        let mut pushdown = file_isolation;
        for filter in filters {
            let refs = filter.column_refs();
            if !refs
                .iter()
                .all(|c| physical_names.contains(c.name.as_str()))
            {
                continue; // references an alias or unknown column: not pushable
            }
            match state.create_physical_expr(filter.clone(), &df_schema) {
                Ok(expr) => {
                    pushdown = binary(pushdown, Operator::And, expr, &self.physical_schema)?;
                }
                Err(_) => continue, // unconvertible filter: DataFusion re-applies it
            }
        }

        let source = Arc::new(
            ParquetSource::new(self.physical_schema.clone())
                .with_predicate(pushdown)
                .with_pushdown_filters(true)
                .with_reorder_filters(true)
                .with_parquet_file_reader_factory(self.reader_factory.clone()),
        );
        // One group per file: each file is internally sorted, but files may
        // overlap in key ranges — concatenating them into one group would
        // falsely claim a total order. Separate groups also parallelize.
        let file_groups: Vec<FileGroup> =
            files.into_iter().map(|f| FileGroup::new(vec![f])).collect();

        // Declared ordering: the file's true physical order, the longest prefix
        // of the sort key present in the scan. Built against the file schema —
        // DataFusion validates the declared ordering against the unprojected
        // table schema (and each file's min/max stats), then maps it through the
        // scan projection. Declaring the *full* `(packing_key, ts, …)` order is
        // the only sound choice: a packed file is NOT ts-sorted on its own (ts
        // resets per key), so a `[ts]`-only claim would be rejected by
        // stats validation. Sort elimination for `ORDER BY ts` would need the
        // isolation predicate's `packing_key = const` equivalence to collapse
        // `(packing_key, ts)` to `(ts)`; see the note in `order_by_sort_key_*`.
        let mut sort_exprs: Vec<PhysicalSortExpr> = Vec::new();
        for name in &self.hypertable.sort_key {
            match col(name, &self.physical_schema) {
                Ok(expr) => sort_exprs.push(PhysicalSortExpr::new_default(expr)),
                Err(_) => break, // column not in the file: ordering prefix ends
            }
        }
        let mut builder = FileScanConfigBuilder::new(self.store_url.clone(), source)
            .with_file_groups(file_groups)
            .with_projection_indices(Some(scan_projection))?;
        if let Some(ordering) = LexOrdering::new(sort_exprs) {
            builder = builder.with_output_ordering(vec![ordering]);
        }
        let config = builder.build();
        let scan: Arc<dyn ExecutionPlan> = DataSourceExec::from_data_source(config);

        // Mandatory namespace isolation, before projection can drop the column.
        // Built against the scanned (projected) schema.
        let predicate = binary(
            col(&self.hypertable.packing_key, &scanned_schema)?,
            Operator::Eq,
            lit(ScalarValue::Int64(Some(self.packing_key_value))),
            &scanned_schema,
        )?;
        let filtered: Arc<dyn ExecutionPlan> = Arc::new(FilterExec::try_new(predicate, scan)?);

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
                        let compiled = ukiel_expr::compile(sql, &scanned_schema, &spec.data_type)
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

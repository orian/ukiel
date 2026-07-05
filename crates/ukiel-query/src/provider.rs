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
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::limit::GlobalLimitExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::scalar::ScalarValue;
use ukiel_catalog::PostgresCatalog;
use ukiel_core::{Hypertable, arrow_schema_from_json};

use crate::QueryError;

pub struct UkielTableProvider {
    catalog: PostgresCatalog,
    hypertable: Hypertable,
    schema: SchemaRef,
    packing_key_value: i64,
    store_url: ObjectStoreUrl,
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
    ) -> Result<Self, QueryError> {
        let schema = Arc::new(arrow_schema_from_json(&hypertable.table_schema)?);
        Ok(Self {
            catalog,
            hypertable,
            schema,
            packing_key_value,
            store_url,
        })
    }
}

#[async_trait]
impl TableProvider for UkielTableProvider {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
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
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        // Catalog-pruned file list: only parts whose packing-key range
        // contains this namespace's key. Never lists the object store.
        let parts = self
            .catalog
            .live_parts(self.hypertable.id, Some(self.packing_key_value))
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let files: Vec<PartitionedFile> = parts
            .iter()
            .map(|p| PartitionedFile::new(p.meta.path.clone(), p.meta.size_bytes as u64))
            .collect();

        let source = Arc::new(ParquetSource::new(self.schema.clone()));
        let config = FileScanConfigBuilder::new(self.store_url.clone(), source)
            .with_file_group(FileGroup::new(files))
            .build();
        let scan: Arc<dyn ExecutionPlan> = DataSourceExec::from_data_source(config);

        // Mandatory namespace isolation, before projection can drop the column.
        let predicate = binary(
            col(&self.hypertable.packing_key, &self.schema)?,
            Operator::Eq,
            lit(ScalarValue::Int64(Some(self.packing_key_value))),
            &self.schema,
        )?;
        let mut plan: Arc<dyn ExecutionPlan> = Arc::new(FilterExec::try_new(predicate, scan)?);

        if let Some(indices) = projection {
            let exprs = indices
                .iter()
                .map(|&i| {
                    let field = self.schema.field(i);
                    Ok((col(field.name(), &self.schema)?, field.name().clone()))
                })
                .collect::<DfResult<Vec<_>>>()?;
            plan = Arc::new(ProjectionExec::try_new(exprs, plan)?);
        }
        if let Some(n) = limit {
            plan = Arc::new(GlobalLimitExec::new(plan, 0, Some(n)));
        }
        Ok(plan)
    }
}

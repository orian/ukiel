//! Builds a per-namespace SessionContext: custom catalog/schema providers
//! resolve table names through the Ukiel catalog, scoped to one namespace.
//!
//! [`operator_session`] is the deliberate exception — an unscoped, whole-table
//! session for benchmarks and operator tooling. It is never reachable from the
//! HTTP server; see its doc comment.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::{CatalogProvider, SchemaProvider};
use datafusion::common::Result as DfResult;
use datafusion::datasource::TableProvider;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::error::DataFusionError;
use datafusion::execution::config::SessionConfig;
use datafusion::prelude::SessionContext;
use object_store::ObjectStore;
use ukiel_catalog::{CatalogError, PostgresCatalog};
use ukiel_core::NamespaceId;

use crate::QueryError;
use crate::metadata_cache::{CachingParquetFileReaderFactory, ParquetMetadataCache};
use crate::provider::UkielTableProvider;

pub async fn session_for_namespace(
    catalog: &PostgresCatalog,
    namespace: NamespaceId,
    store: Arc<dyn ObjectStore>,
    store_url: &url::Url,
    metadata_cache: Arc<ParquetMetadataCache>,
) -> Result<SessionContext, QueryError> {
    let table_names: Vec<String> = catalog
        .list_logical_tables(namespace)
        .await?
        .into_iter()
        .map(|t| t.name)
        .collect();

    // Enable `information_schema` so clients can introspect tables/columns over
    // the SQL endpoint (`SELECT ... FROM information_schema.tables`), scoped to
    // this namespace's registered schema provider.
    let config = SessionConfig::new()
        .with_default_catalog_and_schema("ukiel", "public")
        .with_information_schema(true);
    let ctx = SessionContext::new_with_config(config);
    ctx.register_object_store(store_url, store.clone());

    // One factory per session over the process-wide metadata cache. Sessions
    // are cheap and per-request; the cache is the long-lived part.
    let reader_factory = Arc::new(CachingParquetFileReaderFactory::new(store, metadata_cache));

    let schema = Arc::new(UkielSchemaProvider {
        catalog: catalog.clone(),
        namespace,
        table_names,
        store_url: ObjectStoreUrl::parse(store_url.as_str())?,
        reader_factory,
    });
    ctx.register_catalog("ukiel", Arc::new(UkielCatalogProvider { schema }));
    Ok(ctx)
}

/// An **unscoped** session: every hypertable registered under its hypertable
/// name, each provider reading *all* namespaces' rows.
///
/// There is no tenant isolation here — that is the entire point. The whole-table
/// benchmark suites (ClickBench, JSONBench) are defined over one global table,
/// and Ukiel's product path is namespace-scoped by construction, so measuring
/// the read stack against the official suites needs a door around the scoping.
/// Every *other* layer is untouched: catalog pruning, stats pruning, the footer
/// cache and the declared output ordering all behave exactly as in a namespace
/// session, so what this measures is the real stack minus the slice predicate.
///
/// **Operator/bench use only. Never wire this into the HTTP server** — the
/// server's caller-supplied `namespace_id` is unauthenticated today (issue
/// 0001), and an unscoped route would hand any caller every tenant's data.
/// Nothing in `server.rs` references this function, and nothing should.
pub async fn operator_session(
    catalog: &PostgresCatalog,
    store: Arc<dyn ObjectStore>,
    store_url: &url::Url,
    metadata_cache: Arc<ParquetMetadataCache>,
) -> Result<SessionContext, QueryError> {
    let config = SessionConfig::new().with_information_schema(true);
    let ctx = SessionContext::new_with_config(config);
    ctx.register_object_store(store_url, store.clone());

    let reader_factory = Arc::new(CachingParquetFileReaderFactory::new(store, metadata_cache));
    let url = ObjectStoreUrl::parse(store_url.as_str())?;

    for hypertable in catalog.list_hypertables().await? {
        let name = hypertable.name.clone();
        let provider = UkielTableProvider::try_new(
            catalog.clone(),
            hypertable,
            None, // unscoped: no isolation predicate, all live parts
            url.clone(),
            reader_factory.clone(),
        )?;
        ctx.register_table(&name, Arc::new(provider))?;
    }
    Ok(ctx)
}

#[derive(Debug)]
struct UkielCatalogProvider {
    schema: Arc<UkielSchemaProvider>,
}

impl CatalogProvider for UkielCatalogProvider {
    fn schema_names(&self) -> Vec<String> {
        vec!["public".to_string()]
    }
    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        (name == "public").then(|| self.schema.clone() as Arc<dyn SchemaProvider>)
    }
}

struct UkielSchemaProvider {
    catalog: PostgresCatalog,
    namespace: NamespaceId,
    /// Names snapshot taken at session build (used for sync `table_exist`).
    table_names: Vec<String>,
    store_url: ObjectStoreUrl,
    reader_factory: Arc<CachingParquetFileReaderFactory>,
}

impl std::fmt::Debug for UkielSchemaProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UkielSchemaProvider")
            .field("namespace", &self.namespace)
            .field("table_names", &self.table_names)
            .finish()
    }
}

#[async_trait]
impl SchemaProvider for UkielSchemaProvider {
    fn table_names(&self) -> Vec<String> {
        self.table_names.clone()
    }

    async fn table(&self, name: &str) -> DfResult<Option<Arc<dyn TableProvider>>> {
        let logical = match self.catalog.get_logical_table(self.namespace, name).await {
            Ok(t) => t,
            Err(CatalogError::NotFound(_)) => return Ok(None),
            Err(e) => return Err(DataFusionError::External(Box::new(e))),
        };
        let hypertable = self
            .catalog
            .get_hypertable_by_id(logical.hypertable_id)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let provider = UkielTableProvider::try_new(
            self.catalog.clone(),
            hypertable,
            Some(self.namespace.0), // v1 convention: slice = packing_key == namespace id
            self.store_url.clone(),
            self.reader_factory.clone(),
        )
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(Some(Arc::new(provider)))
    }

    fn table_exist(&self, name: &str) -> bool {
        self.table_names.iter().any(|n| n == name)
    }
}

use datafusion::error::DataFusionError;
use ukiel_catalog::CatalogError;

#[derive(Debug, thiserror::Error)]
pub enum QueryError {
    #[error(transparent)]
    Catalog(#[from] ukiel_catalog::CatalogError),
    #[error(transparent)]
    Schema(#[from] ukiel_core::SchemaError),
    #[error(transparent)]
    DataFusion(#[from] datafusion::error::DataFusionError),
    #[error(transparent)]
    Arrow(#[from] datafusion::arrow::error::ArrowError),
    #[error(transparent)]
    Expr(#[from] ukiel_expr::ExprError),
    #[error("unknown table '{0}'")]
    UnknownTable(String),
}

/// Finds a `CatalogError` inside a DataFusion error, however deeply the engine
/// wrapped it (plan 42).
///
/// The session and the table provider hand catalog failures to DataFusion as
/// `External(Box<CatalogError>)`, and DataFusion nests those under `Context`,
/// `Shared` and friends on the way back out. This walks that structure and
/// **downcasts**; it never matches on message text, because a classification
/// that depends on wording breaks silently the day the wording changes.
///
/// It is what keeps "the database is unreachable" (503, retryable) from being
/// reported to a tenant as "your SQL is bad" (400, permanent).
pub fn catalog_error_in(err: &DataFusionError) -> Option<&CatalogError> {
    match err {
        DataFusionError::External(inner) => inner.downcast_ref::<CatalogError>(),
        DataFusionError::Context(_, inner) => catalog_error_in(inner),
        DataFusionError::Shared(inner) => catalog_error_in(inner),
        DataFusionError::Diagnostic(_, inner) => catalog_error_in(inner),
        DataFusionError::Collection(errors) => errors.iter().find_map(catalog_error_in),
        _ => None,
    }
}

/// Anything that might be hiding a catalog failure. Lets the HTTP layer treat a
/// session-build failure and a planning failure the same way without caring
/// which error type carried it.
pub trait CatalogFailure {
    fn catalog_failure(&self) -> Option<&CatalogError>;
}

impl CatalogFailure for DataFusionError {
    fn catalog_failure(&self) -> Option<&CatalogError> {
        catalog_error_in(self)
    }
}

impl CatalogFailure for QueryError {
    fn catalog_failure(&self) -> Option<&CatalogError> {
        match self {
            QueryError::Catalog(e) => Some(e),
            QueryError::DataFusion(e) => catalog_error_in(e),
            _ => None,
        }
    }
}

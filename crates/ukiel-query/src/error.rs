#[derive(Debug, thiserror::Error)]
pub enum QueryError {
    #[error(transparent)]
    Catalog(#[from] ukiel_catalog::CatalogError),
    #[error(transparent)]
    Schema(#[from] ukiel_core::SchemaError),
    #[error(transparent)]
    DataFusion(#[from] datafusion::error::DataFusionError),
    #[error(transparent)]
    Expr(#[from] ukiel_expr::ExprError),
    #[error("unknown table '{0}'")]
    UnknownTable(String),
}

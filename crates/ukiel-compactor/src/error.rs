#[derive(Debug, thiserror::Error)]
pub enum CompactorError {
    #[error(transparent)]
    Schema(#[from] ukiel_core::SchemaError),
    #[error(transparent)]
    Arrow(#[from] arrow::error::ArrowError),
    #[error(transparent)]
    Parquet(#[from] parquet::errors::ParquetError),
    #[error(transparent)]
    ObjectStore(#[from] object_store::Error),
    #[error(transparent)]
    Catalog(#[from] ukiel_catalog::CatalogError),
    #[error(transparent)]
    Expr(#[from] ukiel_expr::ExprError),
    #[error(transparent)]
    SortKey(#[from] ukiel_core::SortKeyError),
    #[error("column '{0}' not found in schema")]
    MissingColumn(String),
    #[error("column '{0}' is not Int64")]
    NotInt64(String),
    #[error("reading part '{path}': {source}")]
    PartRead {
        path: String,
        source: parquet::errors::ParquetError,
    },
    /// A merge input is out of order (plan 29): the streaming merge NEVER
    /// re-sorts — plan 27's validated ordering is the trust anchor — so an
    /// input that violates it fails loud instead of silently reintroducing an
    /// O(partition) sort. The one legitimate trigger is a changed materialized
    /// expression on a `sort_key` column; remediation: re-author the column or
    /// drop it from `sort_key`.
    #[error(
        "part '{path}' is not sorted by sort_key at row {row} (streaming merge never re-sorts)"
    )]
    MergeOrderViolation { path: String, row: usize },
}

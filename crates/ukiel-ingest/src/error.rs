#[derive(Debug, thiserror::Error)]
pub enum IngestError {
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
    Kafka(#[from] rdkafka::error::KafkaError),
    #[error(transparent)]
    Expr(#[from] ukiel_expr::ExprError),
    #[error(transparent)]
    SortKey(#[from] ukiel_core::SortKeyError),
    /// A flush whose offset ranges cannot name a coherent operation — inverted
    /// or overlapping ranges, an empty topic. A consumer bug, caught before the
    /// batch costs an upload.
    #[error(transparent)]
    Operation(#[from] ukiel_core::OperationError),
    #[error("row {row} is missing required i64 column '{column}'")]
    MissingColumn { row: usize, column: String },
    #[error("flush called with no rows")]
    EmptyFlush,
    #[error("config error: {0}")]
    Config(String),
}

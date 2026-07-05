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
    #[error("row {row} is missing required i64 column '{column}'")]
    MissingColumn { row: usize, column: String },
    #[error("flush called with no rows")]
    EmptyFlush,
    #[error("config error: {0}")]
    Config(String),
}

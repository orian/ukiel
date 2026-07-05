#[derive(Debug, thiserror::Error)]
pub enum CatalogError {
    /// A REPLACE/DELETE lost the optimistic-concurrency race: some input
    /// parts were already tombstoned by another commit. Retry on fresh state.
    #[error("commit conflict: only {live_matched} of {expected} input parts were still live")]
    Conflict {
        expected: usize,
        live_matched: usize,
    },

    #[error("not found: {0}")]
    NotFound(String),

    #[error(transparent)]
    Db(#[from] sqlx::Error),

    #[error("migration failed: {0}")]
    Migrate(#[from] sqlx::migrate::MigrateError),
}

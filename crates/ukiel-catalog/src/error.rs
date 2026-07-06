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

    /// A concurrent writer already advanced this topic-partition's ingest
    /// offset past the range being committed (issue 0003: two ingest workers
    /// on one topic). The commit was rolled back; the losing worker must stop —
    /// its consumed batch is a duplicate.
    #[error(
        "ingest offset race on {topic}/{partition}: stored next_offset moved past this commit's range"
    )]
    OffsetRace { topic: String, partition: i32 },

    #[error(transparent)]
    Db(#[from] sqlx::Error),

    #[error("migration failed: {0}")]
    Migrate(#[from] sqlx::migrate::MigrateError),
}

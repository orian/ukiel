/// What a catalog failure *means*, decided from the error itself and nothing
/// else (plan 42). The class says what happened; only the caller's context can
/// say what to do about it — the same transport failure is a harmless retry
/// around a read and an **unknown outcome** around a commit.
///
/// Deliberately not derived from message text: an error whose meaning depends on
/// a driver's wording is an error whose meaning changes on a dependency bump.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatalogErrorClass {
    /// Optimistic concurrency lost. Re-read live state and replan.
    SemanticConflict,
    /// This worker is not (or no longer) the owner. Stop; never retry.
    OwnershipViolation,
    /// The request itself is wrong and will be wrong again. Fail the caller.
    PermanentInput,
    /// The thing asked for does not exist. Caller-specific, usually permanent.
    AbsentResource,
    /// The database could not be reached or the connection died. Recoverable —
    /// but for a mutation the outcome is *unknown*, not "did not happen".
    Transport,
    /// PostgreSQL asked us to try the transaction again (serialization failure,
    /// deadlock). Safe to retry the whole transaction.
    RetryableDatabase,
    /// Everything else, including every error we have not classified. The
    /// default is permanent on purpose: an unknown failure treated as transient
    /// becomes an infinite retry loop against a database that will never agree.
    PermanentDatabase,
}

impl CatalogErrorClass {
    /// Stable metric-label spelling. Fixed strings, not `Debug` — a label an
    /// operator alerts on must not change because someone renamed a variant.
    pub fn as_label(self) -> &'static str {
        match self {
            CatalogErrorClass::SemanticConflict => "semantic_conflict",
            CatalogErrorClass::OwnershipViolation => "ownership_violation",
            CatalogErrorClass::PermanentInput => "permanent_input",
            CatalogErrorClass::AbsentResource => "absent_resource",
            CatalogErrorClass::Transport => "transport",
            CatalogErrorClass::RetryableDatabase => "retryable_database",
            CatalogErrorClass::PermanentDatabase => "permanent_database",
        }
    }
}

/// SQLSTATE → class. Split out as a pure function so the mapping is pinned by
/// unit tests without a database or a constructible driver error double.
///
/// - `08*` connection exception, `57P01/02/03` admin shutdown / crash shutdown /
///   cannot connect now (exactly what a managed failover emits), `53300` too
///   many connections → transport: the server is going away or unreachable.
/// - `40001` serialization failure, `40P01` deadlock detected → retryable: the
///   database is explicitly inviting a retry of the transaction.
/// - anything else → permanent. Unknown means permanent, always.
pub(crate) fn class_for_sqlstate(code: &str) -> CatalogErrorClass {
    match code {
        "40001" | "40P01" => CatalogErrorClass::RetryableDatabase,
        "57P01" | "57P02" | "57P03" | "53300" => CatalogErrorClass::Transport,
        _ if code.starts_with("08") => CatalogErrorClass::Transport,
        _ => CatalogErrorClass::PermanentDatabase,
    }
}

impl CatalogError {
    /// The stable meaning of this failure. See [`CatalogErrorClass`].
    pub fn class(&self) -> CatalogErrorClass {
        match self {
            CatalogError::Conflict { .. } => CatalogErrorClass::SemanticConflict,
            CatalogError::LeaseLost { .. } | CatalogError::OffsetRace { .. } => {
                CatalogErrorClass::OwnershipViolation
            }
            CatalogError::Schema(_)
            | CatalogError::SortKey(_)
            | CatalogError::LeasePartitionMismatch { .. }
            | CatalogError::InvalidLeaseTtl(_) => CatalogErrorClass::PermanentInput,
            CatalogError::NotFound(_) => CatalogErrorClass::AbsentResource,
            // A migration that failed because the database was unreachable is a
            // *transport* failure wearing a migration's coat. Calling it
            // permanent is how a writer failover during a rolling deploy kills
            // every starting process — the exact crash loop plan 42 exists to
            // stop. A genuinely bad migration (checksum, version, bad SQL) stays
            // permanent, because waiting will not fix it.
            CatalogError::Migrate(e) => match e {
                sqlx::migrate::MigrateError::Execute(inner)
                | sqlx::migrate::MigrateError::ExecuteMigration(inner, _) => {
                    class_for_db_error(inner)
                }
                _ => CatalogErrorClass::PermanentDatabase,
            },
            CatalogError::Db(e) => class_for_db_error(e),
        }
    }

    /// True when the failure is a lost/unavailable connection: the role can be
    /// rebuilt from catalog authority once the writer is reachable again.
    ///
    /// This is **not** permission to replay a mutation. A transport failure
    /// around a commit leaves its outcome unknown — recovery means reloading
    /// authoritative state, never calling the same mutation again (plan 43
    /// supplies the operation identities that would make replay decidable).
    pub fn is_recoverable_transport(&self) -> bool {
        matches!(
            self.class(),
            CatalogErrorClass::Transport | CatalogErrorClass::RetryableDatabase
        )
    }
}

fn class_for_db_error(e: &sqlx::Error) -> CatalogErrorClass {
    match e {
        // The socket died, or the pool could not hand us a live connection
        // inside its acquire timeout — both mean "the database is not reachable
        // right now", which is the failover signature we must survive.
        sqlx::Error::Io(_) | sqlx::Error::PoolTimedOut => CatalogErrorClass::Transport,
        // A connection killed *during the handshake* does not arrive as an I/O
        // error — it arrives as garbage on the wire ("unexpected response from
        // SSLRequest: 0x00"), which sqlx reports as `Protocol`. Found by S10:
        // classifying it permanent made the compactor fail the process on a
        // failover, which is precisely the crash loop this plan exists to stop.
        //
        // The trade: a genuine protocol incompatibility (wrong server, wrong
        // version) now retries instead of failing fast. That is the better error
        // to have — it backs off, stays Degraded, and says so in
        // `catalog_retry_total`, rather than taking the fleet down.
        sqlx::Error::Protocol(_) => CatalogErrorClass::Transport,
        // The pool was closed under us: that is our own shutdown, not the
        // database's, and reconnecting the same pool will never work.
        sqlx::Error::PoolClosed => CatalogErrorClass::PermanentDatabase,
        sqlx::Error::Database(db) => db
            .code()
            .map(|c| class_for_sqlstate(&c))
            .unwrap_or(CatalogErrorClass::PermanentDatabase),
        _ => CatalogErrorClass::PermanentDatabase,
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CatalogError {
    /// A REPLACE/DELETE lost the optimistic-concurrency race: some input
    /// parts were already tombstoned by another commit. Retry on fresh state.
    #[error("commit conflict: only {live_matched} of {expected} input parts were still live")]
    Conflict {
        expected: usize,
        live_matched: usize,
    },

    /// A leased REPLACE reached the commit transaction without owning its
    /// partition any more: the lease expired, was reclaimed, or was released,
    /// and the fence inside the transaction refused it (plan 41). The commit
    /// was rolled back — nothing was tombstoned and nothing inserted. For a
    /// compactor this is ordinary scheduling churn (a peer now owns the
    /// partition and will redo the work), not a fault.
    #[error("compaction lease lost for partition {partition} (owner/generation no longer current)")]
    LeaseLost { partition: String },

    /// A leased REPLACE named parts outside its leased partition — a caller
    /// bug. Fencing partition A must never license a rewrite of partition B.
    #[error("leased replace on partition {partition} touches parts outside it")]
    LeasePartitionMismatch { partition: String },

    /// A lease TTL that PostgreSQL cannot represent (zero, or beyond i64
    /// milliseconds). Caught in Rust: an unusable interval must not reach SQL.
    #[error("invalid lease ttl: {0}")]
    InvalidLeaseTtl(String),

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

    /// A hypertable was registered with an invalid schema or `sort_key`
    /// (plan 27: packing key must lead the sort key, columns must exist).
    #[error(transparent)]
    Schema(#[from] ukiel_core::SchemaError),
    #[error(transparent)]
    SortKey(#[from] ukiel_core::SortKeyError),

    #[error(transparent)]
    Db(#[from] sqlx::Error),

    #[error("migration failed: {0}")]
    Migrate(#[from] sqlx::migrate::MigrateError),
}

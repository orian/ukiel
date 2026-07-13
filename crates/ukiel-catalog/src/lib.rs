//! Postgres-backed catalog: parts, commits, change feed.

mod commit;
mod cursors;
mod error;
mod feed;
mod gc;
mod leases;
mod offsets;
mod parts;
mod tables;

pub use error::{CatalogError, CatalogErrorClass};
pub use gc::GcBacklogs;
pub use leases::CompactionLease;
pub use offsets::OffsetRange;
pub use parts::CandidateWindow;

#[cfg(test)]
mod error_tests;

use std::time::Duration;

use sqlx::PgPool;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};

/// The catalog service handle. Cheap to clone (wraps a connection pool).
#[derive(Clone)]
pub struct PostgresCatalog {
    pub(crate) pool: PgPool,
}

/// SQLx pool lifecycle, as a deployment concern rather than a constant (plan
/// 42). The defaults reproduce today's behavior; what they add is the ability to
/// survive a managed writer failover — `test_before_acquire` so a promoted
/// endpoint does not hand out dead connections, and `max_lifetime` so the pool
/// eventually stops holding connections to an old primary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogPoolConfig {
    pub max_connections: u32,
    pub min_connections: u32,
    /// How long a caller waits for a connection before the pool gives up. This
    /// is the knob that turns "the database is gone" into a prompt, classifiable
    /// `PoolTimedOut` instead of a request that hangs until its own deadline.
    pub acquire_timeout: Duration,
    pub idle_timeout: Option<Duration>,
    pub max_lifetime: Option<Duration>,
    pub test_before_acquire: bool,
}

impl Default for CatalogPoolConfig {
    fn default() -> Self {
        Self {
            max_connections: PostgresCatalog::DEFAULT_POOL_SIZE,
            min_connections: 0,
            acquire_timeout: Duration::from_millis(2_000),
            idle_timeout: Some(Duration::from_secs(600)),
            max_lifetime: Some(Duration::from_secs(1_800)),
            test_before_acquire: true,
        }
    }
}

impl CatalogPoolConfig {
    fn options(&self) -> PgPoolOptions {
        PgPoolOptions::new()
            .max_connections(self.max_connections)
            .min_connections(self.min_connections)
            .acquire_timeout(self.acquire_timeout)
            .idle_timeout(self.idle_timeout)
            .max_lifetime(self.max_lifetime)
            .test_before_acquire(self.test_before_acquire)
    }
}

impl PostgresCatalog {
    /// The per-process pool size. **Per process, not per fleet** — a deployment
    /// with N ukield processes opens N × this many connections to the one
    /// primary, which is the number `max_connections` actually has to survive.
    /// A single big pool is not equivalent to many small ones, and conflating
    /// them is how a pool knob makes a system worse (roadmap row 40).
    pub const DEFAULT_POOL_SIZE: u32 = 16;

    pub async fn connect(url: &str) -> Result<Self, CatalogError> {
        Self::connect_with_pool_size(url, Self::DEFAULT_POOL_SIZE).await
    }

    /// `connect` with an explicit pool size. Exists so the catalog bench can
    /// model **process-equivalent topologies** (several `PostgresCatalog`
    /// instances, each its own pool) instead of pretending one large pool is a
    /// fleet.
    pub async fn connect_with_pool_size(
        url: &str,
        max_connections: u32,
    ) -> Result<Self, CatalogError> {
        Self::connect_with_config(
            url,
            &CatalogPoolConfig {
                max_connections,
                // The bench's topology must not silently change shape: keep the
                // eager, untested-acquire pool it has always measured.
                test_before_acquire: false,
                ..CatalogPoolConfig::default()
            },
        )
        .await
    }

    /// Connects eagerly: the first connection is established before returning,
    /// so a bad URL or an unreachable database fails here.
    pub async fn connect_with_config(
        url: &str,
        config: &CatalogPoolConfig,
    ) -> Result<Self, CatalogError> {
        let pool = config.options().connect(url).await?;
        Ok(Self { pool })
    }

    /// Builds the pool **without** contacting the database (plan 42).
    ///
    /// This is what ukield runs: a process must be able to start, bind its
    /// listeners, and answer liveness while the writer is still being promoted.
    /// Connections are established on first use and, because the pool outlives
    /// the outage, the same pool reaches the new primary once it is up — no
    /// process restart, no crash loop. Errors surface at the call site (as
    /// classifiable transport failures), not at startup.
    pub fn connect_lazy_with_config(
        url: &str,
        config: &CatalogPoolConfig,
    ) -> Result<Self, CatalogError> {
        let options: PgConnectOptions = url.parse()?;
        Ok(Self {
            pool: config.options().connect_lazy_with(options),
        })
    }

    pub async fn migrate(&self) -> Result<(), CatalogError> {
        sqlx::migrate!("./migrations").run(&self.pool).await?;
        Ok(())
    }

    /// Raw pool access for tests and migrations-adjacent tooling.
    /// Not part of the stable API.
    pub fn pool_for_tests(&self) -> &sqlx::PgPool {
        &self.pool
    }

    /// Liveness probe for `/readyz`: a trivial `SELECT 1` proving the pool can
    /// reach Postgres.
    pub async fn ping(&self) -> Result<(), CatalogError> {
        sqlx::query("SELECT 1").execute(&self.pool).await?;
        Ok(())
    }

    /// sqlx pool saturation as `(size, num_idle)` — the
    /// `catalog_pool_connections{state}` gauge (metrics P2). Synchronous;
    /// reads the pool's own counters, issues no query.
    pub fn pool_stats(&self) -> (u32, usize) {
        (self.pool.size(), self.pool.num_idle())
    }
}

//! Postgres-backed catalog: parts, commits, change feed.

mod commit;
mod cursors;
mod error;
mod feed;
mod gc;
mod offsets;
mod parts;
mod tables;

pub use error::CatalogError;
pub use gc::GcBacklogs;
pub use offsets::OffsetRange;

use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

/// The catalog service handle. Cheap to clone (wraps a connection pool).
#[derive(Clone)]
pub struct PostgresCatalog {
    pub(crate) pool: PgPool,
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
        let pool = PgPoolOptions::new()
            .max_connections(max_connections)
            .connect(url)
            .await?;
        Ok(Self { pool })
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

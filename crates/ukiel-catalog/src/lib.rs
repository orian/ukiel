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
    pub async fn connect(url: &str) -> Result<Self, CatalogError> {
        let pool = PgPoolOptions::new()
            .max_connections(16)
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

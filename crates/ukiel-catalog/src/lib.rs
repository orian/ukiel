//! Postgres-backed catalog: parts, commits, change feed.

mod error;

pub use error::CatalogError;

use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

/// The catalog service handle. Cheap to clone (wraps a connection pool).
#[derive(Clone)]
pub struct PostgresCatalog {
    pool: PgPool,
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
}

use std::collections::HashMap;

use ukiel_core::HypertableId;

use crate::{CatalogError, PostgresCatalog};

/// A consumed Kafka offset range, stored transactionally with the part commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetRange {
    pub topic: String,
    pub partition: i32,
    /// First consumed offset in this flush (inclusive).
    pub first: i64,
    /// Last consumed offset in this flush (inclusive).
    pub last: i64,
}

impl PostgresCatalog {
    /// Next offset to consume per Kafka partition. Empty when nothing stored.
    pub async fn ingest_offsets(
        &self,
        hypertable_id: HypertableId,
        topic: &str,
    ) -> Result<HashMap<i32, i64>, CatalogError> {
        let rows: Vec<(i32, i64)> = sqlx::query_as(
            "SELECT kafka_partition, next_offset FROM ingest_offsets
             WHERE hypertable_id = $1 AND topic = $2",
        )
        .bind(hypertable_id.0)
        .bind(topic)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().collect())
    }
}

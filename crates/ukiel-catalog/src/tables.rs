use sqlx::Row;
use ukiel_core::{Hypertable, HypertableId, LogicalTable, LogicalTableId, NamespaceId, Placement};

use crate::{CatalogError, PostgresCatalog};

fn hypertable_from_row(row: &sqlx::postgres::PgRow) -> Hypertable {
    Hypertable {
        id: HypertableId(row.get("id")),
        name: row.get("name"),
        table_schema: row.get("table_schema"),
        partition_spec: row.get("partition_spec"),
        sort_key: row.get("sort_key"),
        packing_key: row.get("packing_key"),
        placement: Placement::from_db(row.get("target_file_bytes")),
    }
}

impl PostgresCatalog {
    pub async fn create_hypertable(
        &self,
        name: &str,
        table_schema: &serde_json::Value,
        partition_spec: &serde_json::Value,
        sort_key: &[String],
        packing_key: &str,
    ) -> Result<HypertableId, CatalogError> {
        let id: i64 = sqlx::query_scalar(
            "INSERT INTO hypertables (name, table_schema, partition_spec, sort_key, packing_key)
             VALUES ($1, $2, $3, $4, $5) RETURNING id",
        )
        .bind(name)
        .bind(table_schema)
        .bind(partition_spec)
        .bind(sort_key)
        .bind(packing_key)
        .fetch_one(&self.pool)
        .await?;
        Ok(HypertableId(id))
    }

    pub async fn get_hypertable(&self, name: &str) -> Result<Hypertable, CatalogError> {
        let row = sqlx::query(
            "SELECT id, name, table_schema, partition_spec, sort_key, packing_key, target_file_bytes
             FROM hypertables WHERE name = $1",
        )
        .bind(name)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| CatalogError::NotFound(format!("hypertable '{name}'")))?;

        Ok(hypertable_from_row(&row))
    }

    pub async fn create_logical_table(
        &self,
        namespace_id: NamespaceId,
        name: &str,
        hypertable_id: HypertableId,
    ) -> Result<LogicalTableId, CatalogError> {
        let id: i64 = sqlx::query_scalar(
            "INSERT INTO logical_tables (namespace_id, name, hypertable_id)
             VALUES ($1, $2, $3) RETURNING id",
        )
        .bind(namespace_id.0)
        .bind(name)
        .bind(hypertable_id.0)
        .fetch_one(&self.pool)
        .await?;
        Ok(LogicalTableId(id))
    }

    pub async fn get_logical_table(
        &self,
        namespace_id: NamespaceId,
        name: &str,
    ) -> Result<LogicalTable, CatalogError> {
        let row = sqlx::query(
            "SELECT id, namespace_id, name, hypertable_id, column_mapping
             FROM logical_tables WHERE namespace_id = $1 AND name = $2",
        )
        .bind(namespace_id.0)
        .bind(name)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| {
            CatalogError::NotFound(format!(
                "logical table '{name}' in namespace {namespace_id}"
            ))
        })?;

        Ok(LogicalTable {
            id: LogicalTableId(row.get("id")),
            namespace_id: NamespaceId(row.get("namespace_id")),
            name: row.get("name"),
            hypertable_id: HypertableId(row.get("hypertable_id")),
            column_mapping: row.get("column_mapping"),
        })
    }

    pub async fn get_hypertable_by_id(&self, id: HypertableId) -> Result<Hypertable, CatalogError> {
        let row = sqlx::query(
            "SELECT id, name, table_schema, partition_spec, sort_key, packing_key, target_file_bytes
             FROM hypertables WHERE id = $1",
        )
        .bind(id.0)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| CatalogError::NotFound(format!("hypertable id {id}")))?;

        Ok(hypertable_from_row(&row))
    }

    pub async fn list_logical_tables(
        &self,
        namespace_id: NamespaceId,
    ) -> Result<Vec<LogicalTable>, CatalogError> {
        let rows = sqlx::query(
            "SELECT id, namespace_id, name, hypertable_id, column_mapping
             FROM logical_tables WHERE namespace_id = $1 ORDER BY name",
        )
        .bind(namespace_id.0)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|row| LogicalTable {
                id: LogicalTableId(row.get("id")),
                namespace_id: NamespaceId(row.get("namespace_id")),
                name: row.get("name"),
                hypertable_id: HypertableId(row.get("hypertable_id")),
                column_mapping: row.get("column_mapping"),
            })
            .collect())
    }

    pub async fn set_placement(
        &self,
        id: HypertableId,
        placement: Placement,
    ) -> Result<(), CatalogError> {
        sqlx::query("UPDATE hypertables SET target_file_bytes = $1 WHERE id = $2")
            .bind(placement.to_db())
            .bind(id.0)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn list_hypertables(&self) -> Result<Vec<Hypertable>, CatalogError> {
        let rows = sqlx::query(
            "SELECT id, name, table_schema, partition_spec, sort_key, packing_key, target_file_bytes
             FROM hypertables ORDER BY id",
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.iter().map(hypertable_from_row).collect())
    }
}

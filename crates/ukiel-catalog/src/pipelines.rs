use crate::{CatalogError, PostgresCatalog, feed::feed_head_in_tx};
use sqlx::{Row, postgres::PgRow};
use ukiel_core::{
    ColumnKind, CommitId, Delivery, Endpoint, ErrorPolicy, HypertableId, Pipeline,
    PipelineDefinition, PipelineId, StreamTable, StreamTableDefinition, StreamTableId,
    TableColumns,
};

const PIPELINE_COLUMNS: &str = "id, name, source_kind, source_id, target_kind, target_id, sql, delivery, error_policy, event_time_column";

fn pipeline(row: PgRow) -> Result<Pipeline, CatalogError> {
    let source_id: i64 = row.try_get("source_id")?;
    let source = match row.try_get::<String, _>("source_kind")?.as_str() {
        "kafka" => Endpoint::Kafka(StreamTableId(source_id)),
        "parquet" => Endpoint::Parquet(HypertableId(source_id)),
        unknown => {
            return Err(CatalogError::Invariant(format!(
                "unknown pipeline source kind {unknown}"
            )));
        }
    };
    let delivery = match row.try_get::<String, _>("delivery")?.as_str() {
        "exactly_once" => Delivery::ExactlyOnce,
        unknown => {
            return Err(CatalogError::Invariant(format!(
                "unknown pipeline delivery {unknown}"
            )));
        }
    };
    let error_policy = match row.try_get::<String, _>("error_policy")?.as_str() {
        "stall" => ErrorPolicy::Stall,
        "skip" => ErrorPolicy::Skip,
        unknown => {
            return Err(CatalogError::Invariant(format!(
                "unknown pipeline error policy {unknown}"
            )));
        }
    };
    if row.try_get::<String, _>("target_kind")? != "parquet" {
        return Err(CatalogError::Invariant(
            "pipeline target is not parquet".into(),
        ));
    }
    Ok(Pipeline {
        id: PipelineId(row.try_get("id")?),
        definition: PipelineDefinition {
            name: row.try_get("name")?,
            source,
            target: HypertableId(row.try_get("target_id")?),
            sql: row.try_get("sql")?,
            delivery,
            error_policy,
            event_time_column: row.try_get("event_time_column")?,
        },
    })
}

fn stream_table(row: PgRow) -> Result<StreamTable, CatalogError> {
    let format: String = row.try_get("format")?;
    if format != "json" {
        return Err(CatalogError::Invariant(format!(
            "unknown stream format {format}"
        )));
    }
    Ok(StreamTable {
        id: StreamTableId(row.try_get("id")?),
        definition: StreamTableDefinition {
            name: row.try_get("name")?,
            topic: row.try_get("topic")?,
            format,
            schema: row.try_get("table_schema")?,
        },
    })
}

impl PostgresCatalog {
    pub async fn ensure_stream_table(
        &self,
        def: &StreamTableDefinition,
    ) -> Result<StreamTable, CatalogError> {
        if def.name.trim().is_empty() || def.topic.trim().is_empty() || def.format != "json" {
            return Err(CatalogError::InvalidPipeline(
                "stream name/topic and JSON format are required".into(),
            ));
        }
        let cols = TableColumns::parse(&def.schema)?;
        if cols
            .specs
            .iter()
            .any(|c| c.name.starts_with("__ukiel_") || !matches!(c.kind, ColumnKind::Stored))
        {
            return Err(CatalogError::InvalidPipeline(
                "stream fields must be stored and cannot use the __ukiel_ prefix".into(),
            ));
        }
        let mut tx = self.pool.begin().await?;
        sqlx::query("INSERT INTO stream_tables(name, topic, format, table_schema) VALUES($1,$2,$3,$4) ON CONFLICT(name) DO NOTHING")
            .bind(&def.name).bind(&def.topic).bind(&def.format).bind(&def.schema).execute(&mut *tx).await?;
        let found = stream_table(
            sqlx::query(
                "SELECT id,name,topic,format,table_schema FROM stream_tables WHERE name=$1",
            )
            .bind(&def.name)
            .fetch_one(&mut *tx)
            .await?,
        )?;
        if found.definition != *def {
            return Err(CatalogError::InvalidPipeline(format!(
                "stream {} differs from its immutable definition",
                def.name
            )));
        }
        sqlx::query("UPDATE ingest_offsets SET stream_table_id=$1 WHERE topic=$2 AND stream_table_id IS NULL")
            .bind(found.id.0).bind(&def.topic).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(found)
    }

    pub async fn get_stream_table(&self, name: &str) -> Result<StreamTable, CatalogError> {
        let row = sqlx::query(
            "SELECT id,name,topic,format,table_schema FROM stream_tables WHERE name=$1",
        )
        .bind(name)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| CatalogError::NotFound(format!("stream table {name}")))?;
        stream_table(row)
    }

    pub async fn get_stream_table_by_id(
        &self,
        id: StreamTableId,
    ) -> Result<StreamTable, CatalogError> {
        let row =
            sqlx::query("SELECT id,name,topic,format,table_schema FROM stream_tables WHERE id=$1")
                .bind(id.0)
                .fetch_optional(&self.pool)
                .await?
                .ok_or_else(|| CatalogError::NotFound(format!("stream table {id}")))?;
        stream_table(row)
    }

    /// Exact-match bootstrap, never an update or an activation operation.
    pub async fn ensure_pipeline(
        &self,
        def: &PipelineDefinition,
    ) -> Result<Pipeline, CatalogError> {
        if def.name.trim().is_empty()
            || def.sql.trim().is_empty()
            || def.source.id() <= 0
            || def.target.0 <= 0
        {
            return Err(CatalogError::InvalidPipeline(
                "pipeline name, SQL, and valid endpoints are required".into(),
            ));
        }
        let mut tx = self
            .pool
            .begin_with("BEGIN ISOLATION LEVEL READ COMMITTED")
            .await?;
        // Two-int lock namespace is separate from the one-bigint feed barriers.
        sqlx::query("SELECT pg_advisory_xact_lock(20123, 8)")
            .execute(&mut *tx)
            .await?;
        let sql = format!("SELECT {PIPELINE_COLUMNS} FROM pipelines WHERE name=$1");
        if let Some(row) = sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(&def.name)
            .fetch_optional(&mut *tx)
            .await?
        {
            let found = pipeline(row)?;
            if found.definition != *def {
                return Err(CatalogError::InvalidPipeline(format!(
                    "pipeline {} differs from its immutable definition",
                    def.name
                )));
            }
            tx.commit().await?;
            return Ok(found);
        }
        let (kind, source_exists): (&str, bool) = match def.source {
            Endpoint::Kafka(id) => (
                "kafka",
                sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM stream_tables WHERE id=$1)")
                    .bind(id.0)
                    .fetch_one(&mut *tx)
                    .await?,
            ),
            Endpoint::Parquet(id) => (
                "parquet",
                sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM hypertables WHERE id=$1)")
                    .bind(id.0)
                    .fetch_one(&mut *tx)
                    .await?,
            ),
        };
        let target_exists: bool =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM hypertables WHERE id=$1)")
                .bind(def.target.0)
                .fetch_one(&mut *tx)
                .await?;
        if !source_exists || !target_exists {
            return Err(CatalogError::InvalidPipeline(
                "pipeline endpoint does not exist in the declared kind".into(),
            ));
        }
        if let Endpoint::Parquet(source) = def.source {
            let cycle: bool = sqlx::query_scalar(
                "WITH RECURSIVE reachable(id) AS (
                    SELECT $1::bigint UNION
                    SELECT p.target_id FROM pipelines p JOIN reachable r ON p.source_id=r.id WHERE p.source_kind='parquet'
                 ) SELECT EXISTS(SELECT 1 FROM reachable WHERE id=$2)")
                .bind(def.target.0).bind(source.0).fetch_one(&mut *tx).await?;
            if cycle {
                return Err(CatalogError::InvalidPipeline(
                    "pipeline would create a cycle".into(),
                ));
            }
        }
        let sql = format!(
            "INSERT INTO pipelines(name,source_kind,source_id,target_id,sql,delivery,error_policy,event_time_column) VALUES($1,$2,$3,$4,$5,'exactly_once',$6,$7) RETURNING {PIPELINE_COLUMNS}"
        );
        let found = pipeline(
            sqlx::query(sqlx::AssertSqlSafe(sql))
                .bind(&def.name)
                .bind(kind)
                .bind(def.source.id())
                .bind(def.target.0)
                .bind(&def.sql)
                .bind(match def.error_policy {
                    ErrorPolicy::Stall => "stall",
                    ErrorPolicy::Skip => "skip",
                })
                .bind(&def.event_time_column)
                .fetch_one(&mut *tx)
                .await?,
        )?;
        tx.commit().await?;
        Ok(found)
    }

    pub async fn get_pipeline(&self, name: &str) -> Result<Pipeline, CatalogError> {
        let sql = format!("SELECT {PIPELINE_COLUMNS} FROM pipelines WHERE name=$1");
        let row = sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(name)
            .fetch_optional(&self.pool)
            .await?
            .ok_or_else(|| CatalogError::NotFound(format!("pipeline {name}")))?;
        pipeline(row)
    }

    pub async fn get_pipeline_by_id(&self, id: PipelineId) -> Result<Pipeline, CatalogError> {
        let sql = format!("SELECT {PIPELINE_COLUMNS} FROM pipelines WHERE id=$1");
        let row = sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(id.0)
            .fetch_optional(&self.pool)
            .await?
            .ok_or_else(|| CatalogError::NotFound(format!("pipeline {id}")))?;
        pipeline(row)
    }

    pub async fn list_pipelines(&self) -> Result<Vec<Pipeline>, CatalogError> {
        let sql = format!("SELECT {PIPELINE_COLUMNS} FROM pipelines ORDER BY name");
        sqlx::query(sqlx::AssertSqlSafe(sql))
            .fetch_all(&self.pool)
            .await?
            .into_iter()
            .map(pipeline)
            .collect()
    }

    pub async fn ensure_mv_cursor(&self, id: PipelineId) -> Result<CommitId, CatalogError> {
        let mut tx = self
            .pool
            .begin_with("BEGIN ISOLATION LEVEL READ COMMITTED")
            .await?;
        let row: Option<(String, i64)> =
            sqlx::query_as("SELECT source_kind,source_id FROM pipelines WHERE id=$1")
                .bind(id.0)
                .fetch_optional(&mut *tx)
                .await?;
        let Some((kind, source)) = row else {
            return Err(CatalogError::NotFound(format!("pipeline {id}")));
        };
        if kind != "parquet" {
            return Err(CatalogError::InvalidPipeline(
                "MV cursor requires a Parquet source".into(),
            ));
        }
        let head = feed_head_in_tx(&mut tx, HypertableId(source)).await?;
        let worker = format!("mv/{id}");
        sqlx::query("INSERT INTO worker_cursors(worker,hypertable_id,last_commit_id) VALUES($1,$2,$3) ON CONFLICT(worker,hypertable_id) DO NOTHING")
            .bind(&worker).bind(source).bind(head.0).execute(&mut *tx).await?;
        let cursor: i64 = sqlx::query_scalar(
            "SELECT last_commit_id FROM worker_cursors WHERE worker=$1 AND hypertable_id=$2",
        )
        .bind(&worker)
        .bind(source)
        .fetch_one(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(CommitId(cursor))
    }

    pub async fn read_mv_cursor(&self, id: PipelineId) -> Result<CommitId, CatalogError> {
        let row: Option<(String, Option<i64>)> = sqlx::query_as(
            "SELECT p.source_kind,c.last_commit_id FROM pipelines p LEFT JOIN worker_cursors c
             ON c.worker='mv/' || p.id::text AND c.hypertable_id=p.source_id WHERE p.id=$1",
        )
        .bind(id.0)
        .fetch_optional(&self.pool)
        .await?;
        let Some((kind, cursor)) = row else {
            return Err(CatalogError::NotFound(format!("pipeline {id}")));
        };
        if kind != "parquet" {
            return Err(CatalogError::InvalidPipeline(
                "MV cursor requires a Parquet source".into(),
            ));
        }
        cursor
            .map(CommitId)
            .ok_or_else(|| CatalogError::Invariant(format!("MV {id} has no registered cursor")))
    }
}

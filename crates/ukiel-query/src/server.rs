//! Tenant-facing SQL over HTTP. Isolation comes from the session build
//! (namespace-scoped providers), not from anything in the SQL text.

use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use datafusion::execution::context::SQLOptions;
use object_store::ObjectStore;
use ukiel_catalog::PostgresCatalog;
use ukiel_core::NamespaceId;

use crate::context::session_for_namespace;

#[derive(Clone)]
pub struct AppState {
    pub catalog: PostgresCatalog,
    pub store: Arc<dyn ObjectStore>,
    pub store_url: url::Url,
}

#[derive(serde::Deserialize)]
pub struct QueryRequest {
    pub namespace_id: i64,
    pub sql: String,
}

#[derive(serde::Serialize)]
pub struct QueryResponse {
    pub rows: serde_json::Value,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/api/query", post(run_query))
        .route("/healthz", get(|| async { "ok" }))
        .with_state(state)
}

fn bad_request<E: std::fmt::Display>(e: E) -> (StatusCode, String) {
    (StatusCode::BAD_REQUEST, e.to_string())
}

async fn run_query(
    State(state): State<AppState>,
    Json(req): Json<QueryRequest>,
) -> Result<Json<QueryResponse>, (StatusCode, String)> {
    let ctx = session_for_namespace(
        &state.catalog,
        NamespaceId(req.namespace_id),
        state.store.clone(),
        &state.store_url,
    )
    .await
    .map_err(bad_request)?;

    let options = SQLOptions::new()
        .with_allow_ddl(false)
        .with_allow_dml(false)
        .with_allow_statements(false);
    let df = ctx
        .sql_with_options(&req.sql, options)
        .await
        .map_err(bad_request)?;
    let batches = df.collect().await.map_err(bad_request)?;

    let mut writer = datafusion::arrow::json::ArrayWriter::new(Vec::new());
    for batch in &batches {
        writer.write(batch).map_err(bad_request)?;
    }
    writer.finish().map_err(bad_request)?;
    let rows: serde_json::Value =
        serde_json::from_slice(&writer.into_inner()).unwrap_or_else(|_| serde_json::json!([]));

    Ok(Json(QueryResponse { rows }))
}

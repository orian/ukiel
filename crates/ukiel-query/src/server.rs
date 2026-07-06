//! Tenant-facing SQL over HTTP. Isolation comes from the session build
//! (namespace-scoped providers), not from anything in the SQL text.

use std::sync::Arc;

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::context::SQLOptions;
use object_store::ObjectStore;
use serde_json::json;
use ukiel_catalog::PostgresCatalog;
use ukiel_core::NamespaceId;

use crate::context::session_for_namespace;
use crate::results::{self, ARROW_STREAM_CONTENT_TYPE, ResultFormat};

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
    /// Result encoding; when absent, the `Accept` header decides (arrow stream
    /// vs. default rows). An explicit value wins over `Accept`.
    #[serde(default)]
    pub format: Option<ResultFormat>,
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

/// Errors are always JSON (`{"error": "..."}`), regardless of the requested
/// result format — a client never has to sniff whether an error body is Arrow.
async fn run_query(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<QueryRequest>,
) -> Response {
    match run_query_inner(state, headers, req).await {
        Ok(resp) => resp,
        Err((status, msg)) => (status, Json(json!({ "error": msg }))).into_response(),
    }
}

async fn run_query_inner(
    state: AppState,
    headers: HeaderMap,
    req: QueryRequest,
) -> Result<Response, (StatusCode, String)> {
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

    // Capture the result schema before `collect` consumes the frame; the
    // collected batches carry the definitive runtime schema when non-empty.
    let planned_schema: SchemaRef = df.schema().inner().clone();
    let batches = df.collect().await.map_err(bad_request)?;
    let schema = batches
        .first()
        .map(|b| b.schema())
        .unwrap_or(planned_schema);

    // Explicit `format` wins; otherwise the Arrow `Accept` header opts into the
    // stream, and everything else takes the default (`columns`).
    let format = req.format.unwrap_or_else(|| {
        let accepts_arrow = headers
            .get(header::ACCEPT)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.contains(ARROW_STREAM_CONTENT_TYPE))
            .unwrap_or(false);
        if accepts_arrow {
            ResultFormat::Arrow
        } else {
            ResultFormat::default()
        }
    });

    let resp = match format {
        ResultFormat::Rows => {
            Json(results::to_rows(&batches, &schema).map_err(bad_request)?).into_response()
        }
        ResultFormat::Columns => {
            Json(results::to_columns(&batches, &schema).map_err(bad_request)?).into_response()
        }
        ResultFormat::Arrow => {
            let bytes = results::to_arrow_ipc(&batches, &schema).map_err(bad_request)?;
            ([(header::CONTENT_TYPE, ARROW_STREAM_CONTENT_TYPE)], bytes).into_response()
        }
    };
    Ok(resp)
}

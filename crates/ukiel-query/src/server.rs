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
use metrics::{counter, histogram};
use object_store::{ObjectStore, ObjectStoreExt};
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
    /// Issue 0002: bounds query duration so the GC tombstone grace is a real
    /// guarantee (deployment invariant: this < gc.tombstone_grace). Also the
    /// runaway-query rail for a tenant-facing SQL endpoint.
    pub statement_timeout: std::time::Duration,
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
        .route("/readyz", get(readyz))
        .with_state(state)
}

/// Readiness for load-balancer rotation of query nodes: the catalog answers a
/// `SELECT 1` and the object store is reachable. `NotFound` on the sentinel is
/// still "reachable" (auth + network OK); only a transport error fails.
async fn readyz(State(state): State<AppState>) -> Response {
    if let Err(e) = state.catalog.ping().await {
        return not_ready(format!("catalog: {e}"));
    }
    let sentinel = object_store::path::Path::from("__ukiel_readyz_sentinel");
    match state.store.head(&sentinel).await {
        Ok(_) | Err(object_store::Error::NotFound { .. }) => {
            (StatusCode::OK, "ready").into_response()
        }
        Err(e) => not_ready(format!("object_store: {e}")),
    }
}

fn not_ready(msg: String) -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(json!({ "error": msg })),
    )
        .into_response()
}

fn bad_request<E: std::fmt::Display>(e: E) -> (StatusCode, String) {
    (StatusCode::BAD_REQUEST, e.to_string())
}

/// Coarse query shape for the `class` label — never the SQL text (metrics P1).
pub(crate) fn query_class(sql: &str) -> &'static str {
    let lower = sql.to_ascii_lowercase();
    const AGG: [&str; 5] = ["count(", "sum(", "avg(", "min(", "max("];
    if lower.contains("information_schema") {
        "introspection"
    } else if lower.contains(" group by ") || AGG.iter().any(|t| lower.contains(t)) {
        "aggregate"
    } else {
        "select"
    }
}

fn format_label(format: &ResultFormat) -> &'static str {
    match format {
        ResultFormat::Rows => "rows",
        ResultFormat::Columns => "columns",
        ResultFormat::Arrow => "arrow",
    }
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
        Err((status, msg)) => {
            // Success requests are counted in run_query_inner once the format is
            // known; errors count here with an "unknown" format (metrics P1).
            counter!("query_requests_total", "format" => "unknown", "status" => "error")
                .increment(1);
            (status, Json(json!({ "error": msg }))).into_response()
        }
    }
}

async fn run_query_inner(
    state: AppState,
    headers: HeaderMap,
    req: QueryRequest,
) -> Result<Response, (StatusCode, String)> {
    // Issue 0002: planning + execution run under one statement timeout, so a
    // query can never outlive the GC tombstone grace and read reaped objects.
    // Result formatting stays outside — it's cheap and already has the data.
    let started = std::time::Instant::now();
    let timeout = state.statement_timeout;
    let (planned_schema, batches): (SchemaRef, Vec<_>) = tokio::time::timeout(timeout, async {
        let ctx = session_for_namespace(
            &state.catalog,
            NamespaceId(req.namespace_id),
            state.store.clone(),
            &state.store_url,
        )
        .await
        .map_err(bad_request)?;
        counter!("query_sessions_built_total").increment(1);

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
        Ok::<_, (StatusCode, String)>((planned_schema, batches))
    })
    .await
    .map_err(|_| {
        counter!("query_timeouts_total").increment(1);
        (
            StatusCode::REQUEST_TIMEOUT,
            format!("query exceeded statement timeout ({}s)", timeout.as_secs()),
        )
    })??;

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
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

    let fmt_label = format_label(&format);
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

    // Success metrics emitted only once the response is fully built, so a
    // formatting failure counts as an error (in `run_query`), not a double
    // count (metrics P1).
    histogram!("query_duration_seconds", "class" => query_class(&req.sql))
        .record(started.elapsed().as_secs_f64());
    histogram!("query_rows_returned").record(total_rows as f64);
    counter!("query_requests_total", "format" => fmt_label, "status" => "ok").increment(1);
    Ok(resp)
}

#[cfg(test)]
mod tests {
    use super::query_class;

    #[test]
    fn query_class_coarse_shapes() {
        assert_eq!(query_class("SELECT * FROM events"), "select");
        assert_eq!(
            query_class("select count(*) from events group by tenant_id"),
            "aggregate"
        );
        assert_eq!(query_class("SELECT sum(amount) FROM events"), "aggregate");
        assert_eq!(
            query_class("SELECT table_name FROM information_schema.tables"),
            "introspection"
        );
    }
}

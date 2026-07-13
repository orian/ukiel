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
use crate::metadata_cache::ParquetMetadataCache;
use crate::results::{self, ARROW_STREAM_CONTENT_TYPE, ResultFormat};

/// Structured process health, supplied by whoever owns role state (ukield).
/// A callback rather than a dependency: the query crate must not know about
/// roles, and ukield must not be importable from here.
pub type HealthReport = Arc<dyn Fn() -> serde_json::Value + Send + Sync>;

#[derive(Clone)]
pub struct AppState {
    pub catalog: PostgresCatalog,
    pub store: Arc<dyn ObjectStore>,
    pub store_url: url::Url,
    /// Issue 0002: bounds query duration so the GC tombstone grace is a real
    /// guarantee (deployment invariant: this < gc.tombstone_grace). Also the
    /// runaway-query rail for a tenant-facing SQL endpoint.
    pub statement_timeout: std::time::Duration,
    /// Process-wide Parquet footer cache, shared by every request's session.
    pub metadata_cache: Arc<ParquetMetadataCache>,
    /// How long a query may spend obtaining an **authoritative** plan (plan 42).
    /// Exceeded → 503 + `Retry-After`. Never a stale plan: a query that cannot
    /// read the catalog does not get to guess which parts are live.
    pub admission_timeout: std::time::Duration,
    /// `Retry-After` on a 503, in seconds.
    pub retry_after_secs: u64,
    /// Role/catalog health for `/readyz`. `None` = report reachability only.
    pub health: Option<HealthReport>,
}

impl AppState {
    /// The pre-plan-42 defaults, for tests and callers that do not configure
    /// admission explicitly.
    pub fn with_defaults(
        catalog: PostgresCatalog,
        store: Arc<dyn ObjectStore>,
        store_url: url::Url,
        statement_timeout: std::time::Duration,
        metadata_cache: Arc<ParquetMetadataCache>,
    ) -> Self {
        Self {
            catalog,
            store,
            store_url,
            statement_timeout,
            metadata_cache,
            admission_timeout: std::time::Duration::from_secs(1),
            retry_after_secs: 1,
            health: None,
        }
    }
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

/// Structured readiness (plan 42): can this process take new work?
///
/// Three independent facts, reported separately rather than collapsed into one
/// boolean: the catalog answers, the object store is reachable, and every
/// configured role has actually reconciled. A role that is `Reconciling` is
/// *not* ready — it has not finished reloading its authoritative state — and a
/// process that says otherwise is a process that will act on stale state.
///
/// `/healthz` stays unconditionally 200 (see `router`): liveness must never
/// depend on PostgreSQL, or a managed failover restarts the whole fleet at once.
async fn readyz(State(state): State<AppState>) -> Response {
    let mut body = json!({});
    let object_store_ok = object_store_reachable(&state).await;
    body["object_store"] = json!(if object_store_ok {
        "healthy"
    } else {
        "unavailable"
    });

    let mut ready = object_store_ok;
    match &state.health {
        // ukield owns role state: report its snapshot verbatim (it is already
        // sanitized — classes, never error text, never a URL).
        Some(report) => {
            let snapshot = report();
            ready = ready && snapshot["ready"].as_bool().unwrap_or(false);
            if let Some(obj) = snapshot.as_object() {
                for (k, v) in obj {
                    body[k] = v.clone();
                }
            }
        }
        // No role registry (a bare query server): reachability is all we can say.
        None => {
            let catalog_ok = state.catalog.ping().await.is_ok();
            body["catalog"] = json!(if catalog_ok { "healthy" } else { "unavailable" });
            ready = ready && catalog_ok;
        }
    }
    body["ready"] = json!(ready);

    let status = if ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    let mut resp = (status, Json(body)).into_response();
    if !ready {
        resp.headers_mut().insert(
            header::RETRY_AFTER,
            state.retry_after_secs.to_string().parse().expect("numeric"),
        );
    }
    resp
}

/// `NotFound` on the sentinel still proves reachability (auth + network are
/// fine); only a transport error means the store is unusable.
async fn object_store_reachable(state: &AppState) -> bool {
    let sentinel = object_store::path::Path::from("__ukiel_readyz_sentinel");
    matches!(
        state.store.head(&sentinel).await,
        Ok(_) | Err(object_store::Error::NotFound { .. })
    )
}

fn bad_request<E: std::fmt::Display>(e: E) -> (StatusCode, String) {
    (StatusCode::BAD_REQUEST, e.to_string())
}

/// The plan-42 rule: a query that failed because the *catalog* is unreachable is
/// a retryable 503, not a 400. Everything else — bad SQL, unknown table, a
/// schema error — keeps its 400. The distinction is drawn by downcasting to
/// `CatalogError`, never by inspecting a message.
///
/// The mapping matters to a tenant: a 400 says "your query is wrong, do not
/// retry it"; during a failover that is a lie, and a client that believes it
/// will drop work it should have retried.
fn admission_error<E: crate::error::CatalogFailure + std::fmt::Display>(
    state: &AppState,
    err: E,
) -> (StatusCode, String) {
    match err.catalog_failure() {
        Some(c) if c.is_recoverable_transport() => {
            counter!("query_catalog_admission_rejected_total", "reason" => "transport")
                .increment(1);
            unavailable(state, "catalog unavailable")
        }
        // A permanent catalog/configuration failure is not the tenant's fault
        // and not retryable: it is ours, and it must not be dressed up as a 400.
        Some(c) if !matches!(c, ukiel_catalog::CatalogError::NotFound(_)) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "catalog error".to_string(),
        ),
        _ => bad_request(err),
    }
}

fn unavailable(state: &AppState, msg: &str) -> (StatusCode, String) {
    let _ = state;
    (StatusCode::SERVICE_UNAVAILABLE, msg.to_string())
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
    let retry_after = state.retry_after_secs;
    match run_query_inner(state, headers, req).await {
        Ok(resp) => resp,
        Err((status, msg)) => {
            // Success requests are counted in run_query_inner once the format is
            // known; errors count here with an "unknown" format (metrics P1).
            counter!("query_requests_total", "format" => "unknown", "status" => "error")
                .increment(1);
            // A 503 carries a stable, machine-readable contract: the client is
            // told this is *retryable* and roughly when to come back. Anything
            // less and a well-behaved client either gives up or hot-loops.
            if status == StatusCode::SERVICE_UNAVAILABLE {
                let body = json!({
                    "error": msg,
                    "retryable": true,
                    "code": "catalog_unavailable",
                });
                let mut resp = (status, Json(body)).into_response();
                resp.headers_mut().insert(
                    header::RETRY_AFTER,
                    retry_after.to_string().parse().expect("numeric"),
                );
                return resp;
            }
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

    // The statement timeout bounds the WHOLE query — admission and execution
    // together. That is issue 0002's invariant: a query that outlives the GC
    // tombstone grace can read objects GC has already reaped, and it does not
    // matter which half of the query was slow.
    let (planned_schema, batches): (SchemaRef, Vec<_>) = tokio::time::timeout(timeout, async {
        // ADMISSION (plan 42). Everything that needs the catalog happens here:
        // the namespace session, and the physical plan — which is where the
        // table provider reads live parts. It carries its own, much shorter
        // budget, because a query that cannot reach the catalog must be refused
        // promptly and must never fall back on a stale idea of what is live.
        let admission = std::time::Instant::now();
        let (task_ctx, plan, planned_schema) =
            tokio::time::timeout(state.admission_timeout, async {
                let ctx = session_for_namespace(
                    &state.catalog,
                    NamespaceId(req.namespace_id),
                    state.store.clone(),
                    &state.store_url,
                    state.metadata_cache.clone(),
                )
                .await
                .map_err(|e| admission_error(&state, e))?;
                counter!("query_sessions_built_total").increment(1);

                let options = SQLOptions::new()
                    .with_allow_ddl(false)
                    .with_allow_dml(false)
                    .with_allow_statements(false);
                let df = ctx
                    .sql_with_options(&req.sql, options)
                    .await
                    .map_err(|e| admission_error(&state, e))?;

                // The result schema, captured before the frame is consumed.
                let planned_schema: SchemaRef = df.schema().inner().clone();
                let plan = df
                    .create_physical_plan()
                    .await
                    .map_err(|e| admission_error(&state, e))?;
                Ok::<_, (StatusCode, String)>((ctx.task_ctx(), plan, planned_schema))
            })
            .await
            .map_err(|_| {
                counter!("query_catalog_admission_rejected_total", "reason" => "timeout")
                    .increment(1);
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    "catalog unavailable".to_string(),
                )
            })??;
        histogram!("catalog_operation_duration_seconds", "operation" => "query_admission")
            .record(admission.elapsed().as_secs_f64());

        // EXECUTION. The plan already owns its file list, so it finishes from
        // object storage even if the catalog dies right now — nothing from here
        // needs the database.
        let batches = datafusion::physical_plan::collect(plan, task_ctx)
            .await
            .map_err(bad_request)?;
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

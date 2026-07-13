mod query_test;

use query_test::{STORE_URL, setup};
use serde_json::json;
use ukiel_query::server::{AppState, router};

async fn spawn_server() -> (String, query_test::Harness) {
    spawn_server_with_timeout(std::time::Duration::from_secs(300)).await
}

async fn spawn_server_with_timeout(
    statement_timeout: std::time::Duration,
) -> (String, query_test::Harness) {
    let h = setup().await;
    let state = AppState::with_defaults(
        h.catalog.clone(),
        h.store.clone(),
        url::Url::parse(STORE_URL).unwrap(),
        statement_timeout,
        std::sync::Arc::new(ukiel_query::metadata_cache::ParquetMetadataCache::default()),
    );
    serve(state, h).await
}

async fn serve(state: AppState, h: query_test::Harness) -> (String, query_test::Harness) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, router(state)).await.unwrap();
    });
    (format!("http://{addr}"), h)
}

#[tokio::test]
async fn statement_timeout_returns_408() {
    // ZERO timeout makes expiry deterministic without a genuinely slow query.
    let (base, _h) = spawn_server_with_timeout(std::time::Duration::ZERO).await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{base}/api/query"))
        .json(&json!({"namespace_id": 2, "sql": "SELECT payload FROM events"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 408);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(
        body["error"]
            .as_str()
            .is_some_and(|e| e.contains("statement timeout")),
        "expected statement-timeout error, got {body}"
    );
}

#[tokio::test]
async fn http_query_returns_namespace_scoped_rows() {
    let (base, _h) = spawn_server().await;
    let client = reqwest::Client::new();

    let health = client.get(format!("{base}/healthz")).send().await.unwrap();
    assert_eq!(health.status(), 200);

    let resp = client
        .post(format!("{base}/api/query"))
        .json(&json!({"namespace_id": 2, "sql": "SELECT payload FROM events ORDER BY ts", "format": "rows"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["rows"], json!([{"payload": "b1"}, {"payload": "b2"}]));

    // Bad SQL -> 400 with a message, not a 500.
    let resp = client
        .post(format!("{base}/api/query"))
        .json(&json!({"namespace_id": 1, "sql": "SELEKT nope"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);

    // DDL is rejected.
    let resp = client
        .post(format!("{base}/api/query"))
        .json(&json!({"namespace_id": 1, "sql": "CREATE TABLE hack (x INT)"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
}

#[tokio::test]
async fn rows_format_carries_schema_with_timestamp_hint() {
    let (base, _h) = spawn_server().await;
    let client = reqwest::Client::new();

    // Namespace 2 has rows b1 (ts 150) and b2 (ts 300).
    let resp = client
        .post(format!("{base}/api/query"))
        .json(&json!({"namespace_id": 2, "sql": "SELECT ts, payload FROM events ORDER BY ts", "format": "rows"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();

    // Legacy `rows` shape is preserved, now with a `schema` sibling that
    // recovers `timestamp_ms` from the field metadata (through the provider
    // projection + DataFusion planning).
    assert_eq!(
        body["rows"],
        json!([{"ts": 150, "payload": "b1"}, {"ts": 300, "payload": "b2"}])
    );
    assert_eq!(
        body["schema"],
        json!([
            {"name": "ts", "type": "timestamp_ms", "nullable": true},
            {"name": "payload", "type": "utf8", "nullable": true}
        ])
    );

    // A computed column drops the hint (correctly): ts + 1 is just int64.
    let resp = client
        .post(format!("{base}/api/query"))
        .json(&json!({"namespace_id": 2, "sql": "SELECT ts + 1 AS bumped FROM events", "format": "rows"}))
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["schema"][0]["type"], "int64");
}

#[tokio::test]
async fn columns_format_is_columnar() {
    let (base, _h) = spawn_server().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{base}/api/query"))
        .json(&json!({
            "namespace_id": 2,
            "sql": "SELECT ts, payload FROM events ORDER BY ts",
            "format": "columns"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();

    assert_eq!(body["row_count"], 2);
    assert_eq!(body["columns"]["ts"], json!([150, 300]));
    assert_eq!(body["columns"]["payload"], json!(["b1", "b2"]));
    assert_eq!(body["schema"][0]["type"], "timestamp_ms");
}

#[tokio::test]
async fn arrow_format_round_trips_via_field_and_header() {
    use datafusion::arrow::ipc::reader::StreamReader;

    let (base, _h) = spawn_server().await;
    let client = reqwest::Client::new();

    // (a) explicit format field
    let resp = client
        .post(format!("{base}/api/query"))
        .json(&json!({
            "namespace_id": 2,
            "sql": "SELECT ts, payload FROM events ORDER BY ts",
            "format": "arrow"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(
        resp.headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok()),
        Some("application/vnd.apache.arrow.stream")
    );
    let bytes = resp.bytes().await.unwrap();
    let reader = StreamReader::try_new(std::io::Cursor::new(bytes), None).unwrap();
    let batches: Vec<_> = reader.map(|b| b.unwrap()).collect();
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 2);
    // Lossless: the IPC schema keeps the ukiel:type hint on `ts`.
    let schema = batches[0].schema();
    assert_eq!(
        schema
            .field(0)
            .metadata()
            .get("ukiel:type")
            .map(String::as_str),
        Some("timestamp_ms")
    );

    // (b) Accept header, no format field
    let resp = client
        .post(format!("{base}/api/query"))
        .header("accept", "application/vnd.apache.arrow.stream")
        .json(&json!({"namespace_id": 2, "sql": "SELECT payload FROM events"}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok()),
        Some("application/vnd.apache.arrow.stream")
    );
}

#[tokio::test]
async fn errors_are_json_regardless_of_format() {
    let (base, _h) = spawn_server().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{base}/api/query"))
        .json(&json!({"namespace_id": 1, "sql": "SELEKT nope", "format": "arrow"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(body["error"].is_string(), "error body must be JSON: {body}");
}

#[tokio::test]
async fn default_format_is_columns() {
    let (base, _h) = spawn_server().await;
    let client = reqwest::Client::new();

    // No `format` field, no Accept header -> the default columnar shape.
    let resp = client
        .post(format!("{base}/api/query"))
        .json(&json!({"namespace_id": 2, "sql": "SELECT ts, payload FROM events ORDER BY ts"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();

    assert_eq!(body["row_count"], 2);
    assert_eq!(body["columns"]["ts"], json!([150, 300]));
    assert_eq!(body["columns"]["payload"], json!(["b1", "b2"]));
    assert!(
        body.get("rows").is_none(),
        "default is now columns, not rows: {body}"
    );
}

// ---------------------------------------------------------------------------
// Plan 42: catalog admission. A query that cannot reach the catalog is refused
// with a retryable 503 — never answered from a stale idea of which parts are
// live, and never dressed up as a 400 ("your SQL is bad") that a client would
// take as final.
// ---------------------------------------------------------------------------

/// Cuts the catalog's TCP connections — the real outage signature (a managed
/// failover drops every connection), not a closed pool. A closed pool is *our*
/// shutdown and is deliberately classified permanent; using it here would have
/// tested the wrong thing (and did, first time round: it produced a 500).
struct CatalogCable {
    shutdown: tokio_util::sync::CancellationToken,
}

impl CatalogCable {
    fn cut(&self) {
        self.shutdown.cancel();
    }
}

/// A server whose catalog reaches PostgreSQL through a forwarder we can cut.
async fn spawn_server_behind_a_cuttable_cable() -> (String, query_test::Harness, CatalogCable) {
    let h = setup().await;
    let upstream = h.url.clone();
    let port: u16 = upstream
        .rsplit(':')
        .next()
        .and_then(|p| p.split('/').next())
        .and_then(|p| p.parse().ok())
        .expect("port in url");

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_port = listener.local_addr().unwrap().port();
    let shutdown = tokio_util::sync::CancellationToken::new();
    {
        let token = shutdown.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    () = token.cancelled() => return,
                    accepted = listener.accept() => {
                        let Ok((mut inbound, _)) = accepted else { return };
                        let token = token.clone();
                        tokio::spawn(async move {
                            let Ok(mut out) =
                                tokio::net::TcpStream::connect(("127.0.0.1", port)).await
                            else { return };
                            // Cancelling drops established connections too — the
                            // server's pooled sockets die exactly as they would
                            // in a failover.
                            tokio::select! {
                                () = token.cancelled() => {}
                                _ = tokio::io::copy_bidirectional(&mut inbound, &mut out) => {}
                            }
                        });
                    }
                }
            }
        });
    }

    let catalog = ukiel_catalog::PostgresCatalog::connect_lazy_with_config(
        &format!("postgres://postgres:postgres@127.0.0.1:{proxy_port}/postgres"),
        &ukiel_catalog::CatalogPoolConfig {
            acquire_timeout: std::time::Duration::from_millis(500),
            ..Default::default()
        },
    )
    .unwrap();
    let state = AppState::with_defaults(
        catalog,
        h.store.clone(),
        url::Url::parse(STORE_URL).unwrap(),
        std::time::Duration::from_secs(300),
        std::sync::Arc::new(ukiel_query::metadata_cache::ParquetMetadataCache::default()),
    );
    let (base, h) = serve(state, h).await;
    (base, h, CatalogCable { shutdown })
}

#[tokio::test]
async fn an_unreachable_catalog_is_a_retryable_503_not_a_400() {
    let (base, _h, cable) = spawn_server_behind_a_cuttable_cable().await;
    let client = reqwest::Client::new();

    // Healthy first: the query works.
    let resp = client
        .post(format!("{base}/api/query"))
        .json(&json!({"namespace_id": 2, "sql": "SELECT payload FROM events"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // The catalog goes away under the server.
    cable.cut();

    let resp = client
        .post(format!("{base}/api/query"))
        .json(&json!({"namespace_id": 2, "sql": "SELECT payload FROM events"}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        503,
        "a query that cannot reach the catalog is unavailable, not malformed"
    );
    assert_eq!(
        resp.headers()
            .get("retry-after")
            .map(|v| v.to_str().unwrap()),
        Some("1"),
        "a 503 must tell the client when to come back"
    );
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["retryable"], json!(true));
    assert_eq!(body["code"], json!("catalog_unavailable"));
    // The body must not leak the connection string or driver internals.
    let text = body.to_string();
    assert!(!text.contains("postgres://"), "{text}");
    assert!(!text.contains("password"), "{text}");
}

#[tokio::test]
async fn bad_sql_is_still_a_400_during_normal_operation() {
    // The 503 path must not swallow ordinary client errors: a tenant that sends
    // broken SQL still gets told so.
    let (base, _h) = spawn_server().await;
    let client = reqwest::Client::new();

    for sql in [
        "SELECT * FROM no_such_table",
        "SELECT nonsense_column FROM events",
        "NOT SQL AT ALL",
    ] {
        let resp = client
            .post(format!("{base}/api/query"))
            .json(&json!({"namespace_id": 2, "sql": sql}))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 400, "{sql}");
    }
}

#[tokio::test]
async fn readyz_is_structured_and_refuses_while_the_catalog_is_down() {
    let (base, _h, cable) = spawn_server_behind_a_cuttable_cable().await;
    let client = reqwest::Client::new();

    let resp = client.get(format!("{base}/readyz")).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["ready"], json!(true));
    assert_eq!(body["catalog"], json!("healthy"));
    assert_eq!(body["object_store"], json!("healthy"));

    cable.cut();
    let resp = client.get(format!("{base}/readyz")).send().await.unwrap();
    assert_eq!(
        resp.status(),
        503,
        "an unready node must leave the rotation"
    );
    assert_eq!(
        resp.headers()
            .get("retry-after")
            .map(|v| v.to_str().unwrap()),
        Some("1")
    );
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["ready"], json!(false));
    assert_eq!(body["catalog"], json!("unavailable"));

    // Liveness is NOT readiness. If /healthz depended on PostgreSQL, a managed
    // failover would fail every liveness probe in the fleet and the orchestrator
    // would restart every process at once — turning a 30s failover into an outage.
    let resp = client.get(format!("{base}/healthz")).send().await.unwrap();
    assert_eq!(
        resp.status(),
        200,
        "liveness must not depend on the catalog"
    );
}

#[tokio::test]
async fn an_already_planned_query_finishes_during_an_outage_but_a_new_one_is_refused() {
    // The admission/execution split, stated as behavior. Once the physical plan
    // exists it owns its file list, so it reads from object storage and needs
    // nothing further from the catalog — killing the database mid-flight must
    // not fail it. A *new* query, which would have to ask the catalog which
    // parts are live, is refused rather than answered from a guess.
    let (base, _h, cable) = spawn_server_behind_a_cuttable_cable().await;
    let client = reqwest::Client::new();

    // Warm the path so the failure below can only come from admission.
    let resp = client
        .post(format!("{base}/api/query"))
        .json(&json!({"namespace_id": 2, "sql": "SELECT payload FROM events"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // A slow query: admitted (plan built) before the cable is cut, still
    // executing after. `SLEEP`-free — the sort/limit keeps it in execution long
    // enough on the already-planned inputs.
    let slow = {
        let (base, client) = (base.clone(), client.clone());
        tokio::spawn(async move {
            client
                .post(format!("{base}/api/query"))
                .json(&json!({
                    "namespace_id": 2,
                    "sql": "SELECT payload FROM events ORDER BY ts",
                }))
                .send()
                .await
                .unwrap()
        })
    };
    // Give it time to get past admission, then pull the plug.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    cable.cut();

    let resp = slow.await.unwrap();
    assert_eq!(
        resp.status(),
        200,
        "a plan that already owns its files must not need the catalog again"
    );

    // But nothing new gets admitted while the catalog is unreachable.
    let resp = client
        .post(format!("{base}/api/query"))
        .json(&json!({"namespace_id": 2, "sql": "SELECT payload FROM events"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 503);
}

#[tokio::test]
async fn a_zero_admission_budget_refuses_deterministically() {
    // The budget is a real rail, not decoration: with none of it, every query is
    // refused with the same retryable 503 rather than hanging.
    let h = setup().await;
    let mut state = AppState::with_defaults(
        h.catalog.clone(),
        h.store.clone(),
        url::Url::parse(STORE_URL).unwrap(),
        std::time::Duration::from_secs(300),
        std::sync::Arc::new(ukiel_query::metadata_cache::ParquetMetadataCache::default()),
    );
    state.admission_timeout = std::time::Duration::ZERO;
    state.retry_after_secs = 7;
    let (base, _h) = serve(state, h).await;

    let resp = reqwest::Client::new()
        .post(format!("{base}/api/query"))
        .json(&json!({"namespace_id": 2, "sql": "SELECT payload FROM events"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 503);
    assert_eq!(
        resp.headers()
            .get("retry-after")
            .map(|v| v.to_str().unwrap()),
        Some("7"),
        "Retry-After comes from configuration"
    );
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["code"], json!("catalog_unavailable"));
}

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
    let state = AppState {
        catalog: h.catalog.clone(),
        store: h.store.clone(),
        store_url: url::Url::parse(STORE_URL).unwrap(),
        statement_timeout,
    };
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

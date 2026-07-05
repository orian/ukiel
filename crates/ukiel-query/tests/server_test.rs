mod query_test;

use query_test::{STORE_URL, setup};
use serde_json::json;
use ukiel_query::server::{AppState, router};

async fn spawn_server() -> (String, query_test::Harness) {
    let h = setup().await;
    let state = AppState {
        catalog: h.catalog.clone(),
        store: h.store.clone(),
        store_url: url::Url::parse(STORE_URL).unwrap(),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, router(state)).await.unwrap();
    });
    (format!("http://{addr}"), h)
}

#[tokio::test]
async fn http_query_returns_namespace_scoped_rows() {
    let (base, _h) = spawn_server().await;
    let client = reqwest::Client::new();

    let health = client.get(format!("{base}/healthz")).send().await.unwrap();
    assert_eq!(health.status(), 200);

    let resp = client
        .post(format!("{base}/api/query"))
        .json(&json!({"namespace_id": 2, "sql": "SELECT payload FROM events ORDER BY ts"}))
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

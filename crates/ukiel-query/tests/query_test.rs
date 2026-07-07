#![allow(dead_code)]

use std::sync::Arc;

use datafusion::arrow::array::{Array, Int64Array, StringArray};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::prelude::SessionContext;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use serde_json::json;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::ContainerAsync;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use ukiel_catalog::PostgresCatalog;
use ukiel_core::{CommitOp, HypertableId, NamespaceId, PartMeta, arrow_schema_from_json};
use ukiel_ingest::rows_to_parquet;
use ukiel_query::provider::UkielTableProvider;

pub const STORE_URL: &str = "mem://ukiel/";

pub struct Harness {
    pub _pg: ContainerAsync<Postgres>,
    pub catalog: PostgresCatalog,
    pub store: Arc<dyn ObjectStore>,
    pub ht: HypertableId,
}

fn events_schema() -> serde_json::Value {
    json!({"fields": [
        {"name": "tenant_id", "type": "int64"},
        {"name": "ts", "type": "timestamp_ms"},
        {"name": "payload", "type": "utf8"}
    ]})
}

/// Seeds: one packed file with namespaces 1 and 2, one file with only
/// namespace 2. Namespace 1 rows: (1, 100, "a1"), (1, 200, "a2").
/// Namespace 2 rows: (2, 150, "b1") packed + (2, 300, "b2") dedicated.
pub async fn setup() -> Harness {
    let pg = Postgres::default().start().await.expect("postgres");
    let port = pg.get_host_port_ipv4(5432).await.unwrap();
    let catalog = PostgresCatalog::connect(&format!(
        "postgres://postgres:postgres@127.0.0.1:{port}/postgres"
    ))
    .await
    .unwrap();
    catalog.migrate().await.unwrap();

    let ht = catalog
        .create_hypertable(
            "events",
            &events_schema(),
            &json!({"columns": ["day"]}),
            &["tenant_id".to_string(), "ts".to_string()],
            "tenant_id",
        )
        .await
        .unwrap();
    catalog
        .create_logical_table(NamespaceId(1), "events", ht)
        .await
        .unwrap();
    catalog
        .create_logical_table(NamespaceId(2), "events", ht)
        .await
        .unwrap();

    let schema = arrow_schema_from_json(&events_schema()).unwrap();
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    let packed = rows_to_parquet(
        &schema,
        "tenant_id",
        "ts",
        vec![
            json!({"tenant_id": 1, "ts": 100, "payload": "a1"}),
            json!({"tenant_id": 1, "ts": 200, "payload": "a2"}),
            json!({"tenant_id": 2, "ts": 150, "payload": "b1"}),
        ],
    )
    .unwrap();
    let dedicated = rows_to_parquet(
        &schema,
        "tenant_id",
        "ts",
        vec![json!({"tenant_id": 2, "ts": 300, "payload": "b2"})],
    )
    .unwrap();

    let mut parts = Vec::new();
    for (name, part) in [("packed", packed), ("dedicated", dedicated)] {
        let path = format!("ht/{ht}/L0/{name}.parquet");
        let size = part.bytes.len() as i64;
        store
            .put(&Path::from(path.clone()), part.bytes.into())
            .await
            .unwrap();
        parts.push(PartMeta {
            path,
            partition_values: json!({"day": "1970-01-01"}),
            packing_key_min: part.key_min,
            packing_key_max: part.key_max,
            row_count: part.row_count,
            size_bytes: size,
            level: 0,
            column_stats: None,
        });
    }
    catalog
        .commit(ht, CommitOp::Add { parts }, None)
        .await
        .unwrap();

    Harness {
        _pg: pg,
        catalog,
        store,
        ht,
    }
}

pub fn all_strings(batches: &[RecordBatch], column: &str) -> Vec<String> {
    let mut out = Vec::new();
    for b in batches {
        let idx = b.schema().index_of(column).unwrap();
        let arr: &StringArray = b.column(idx).as_any().downcast_ref().unwrap();
        for i in 0..arr.len() {
            out.push(arr.value(i).to_string());
        }
    }
    out
}

pub fn all_i64(batches: &[RecordBatch], column: &str) -> Vec<i64> {
    let mut out = Vec::new();
    for b in batches {
        let idx = b.schema().index_of(column).unwrap();
        let arr: &Int64Array = b.column(idx).as_any().downcast_ref().unwrap();
        for i in 0..arr.len() {
            out.push(arr.value(i));
        }
    }
    out
}

pub fn all_f64(batches: &[RecordBatch], column: &str) -> Vec<f64> {
    let mut out = Vec::new();
    for b in batches {
        let idx = b.schema().index_of(column).unwrap();
        let arr: &datafusion::arrow::array::Float64Array =
            b.column(idx).as_any().downcast_ref().unwrap();
        for i in 0..arr.len() {
            out.push(arr.value(i));
        }
    }
    out
}

/// Registers the provider directly on a bare SessionContext (schema-provider
/// wiring is exercised separately in the context tests).
async fn ctx_with_provider(h: &Harness, namespace: i64) -> SessionContext {
    let ctx = SessionContext::new();
    ctx.register_object_store(&url::Url::parse(STORE_URL).unwrap(), h.store.clone());
    let ht = h.catalog.get_hypertable_by_id(h.ht).await.unwrap();
    let provider = UkielTableProvider::try_new(
        h.catalog.clone(),
        ht,
        namespace,
        ObjectStoreUrl::parse(STORE_URL).unwrap(),
    )
    .unwrap();
    ctx.register_table("events", Arc::new(provider)).unwrap();
    ctx
}

#[tokio::test]
async fn namespace_sees_only_its_rows_even_in_packed_files() {
    let h = setup().await;

    // Namespace 1: rows only from the packed file, and only its own.
    let ctx = ctx_with_provider(&h, 1).await;
    let batches = ctx
        .sql("SELECT payload FROM events ORDER BY ts")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(all_strings(&batches, "payload"), vec!["a1", "a2"]);

    // Namespace 2: one row from the shared file, one from its dedicated file.
    let ctx = ctx_with_provider(&h, 2).await;
    let batches = ctx
        .sql("SELECT payload FROM events ORDER BY ts")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(all_strings(&batches, "payload"), vec!["b1", "b2"]);

    // A namespace with no data gets an empty result, not an error.
    let ctx = ctx_with_provider(&h, 42).await;
    let batches = ctx
        .sql("SELECT * FROM events")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 0);
}

#[tokio::test]
async fn projection_excluding_packing_key_still_isolates() {
    let h = setup().await;
    let ctx = ctx_with_provider(&h, 1).await;
    // 'payload' only: the packing key is projected away AFTER the isolation
    // filter — this query must not see namespace 2's "b1" from the packed file.
    let batches = ctx
        .sql("SELECT payload FROM events WHERE ts < 1000 ORDER BY payload")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(all_strings(&batches, "payload"), vec!["a1", "a2"]);

    // Aggregates work through the provider.
    let ctx = ctx_with_provider(&h, 2).await;
    let batches = ctx
        .sql("SELECT count(*) AS n FROM events")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(all_i64(&batches, "n"), vec![2]);
}

use ukiel_query::context::session_for_namespace;

#[tokio::test]
async fn session_resolves_tables_by_namespace() {
    let h = setup().await;
    let store_url = url::Url::parse(STORE_URL).unwrap();

    let ctx = session_for_namespace(&h.catalog, NamespaceId(1), h.store.clone(), &store_url)
        .await
        .unwrap();
    let batches = ctx
        .sql("SELECT payload FROM events ORDER BY ts")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(all_strings(&batches, "payload"), vec!["a1", "a2"]);

    // Unknown table -> planning error, not a panic.
    assert!(ctx.sql("SELECT * FROM nope").await.is_err());

    // A namespace without that logical table cannot see it at all.
    let ctx = session_for_namespace(&h.catalog, NamespaceId(42), h.store.clone(), &store_url)
        .await
        .unwrap();
    assert!(ctx.sql("SELECT * FROM events").await.is_err());
}

use ukiel_query::cache::CachingObjectStore;

#[tokio::test]
async fn cache_serves_reads_after_inner_store_loses_the_object() {
    let h = setup().await;
    let cache_dir = tempfile::tempdir().unwrap();
    let cached: Arc<dyn ObjectStore> = Arc::new(CachingObjectStore::new(
        h.store.clone(),
        cache_dir.path().to_path_buf(),
    ));
    let store_url = url::Url::parse(STORE_URL).unwrap();

    let ctx = session_for_namespace(&h.catalog, NamespaceId(1), cached.clone(), &store_url)
        .await
        .unwrap();
    let batches = ctx
        .sql("SELECT payload FROM events ORDER BY ts")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(all_strings(&batches, "payload"), vec!["a1", "a2"]);

    // Files were cached locally.
    let cached_files = std::fs::read_dir(cache_dir.path()).unwrap().count();
    assert!(cached_files >= 1, "expected at least one cached file");

    // Delete everything from the inner store; reads must now come from cache.
    use futures::TryStreamExt;
    let objects: Vec<_> = h.store.list(None).try_collect().await.unwrap();
    for obj in objects {
        h.store.delete(&obj.location).await.unwrap();
    }

    let ctx = session_for_namespace(&h.catalog, NamespaceId(1), cached, &store_url)
        .await
        .unwrap();
    let batches = ctx
        .sql("SELECT payload FROM events ORDER BY ts")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(all_strings(&batches, "payload"), vec!["a1", "a2"]);
}

#[tokio::test]
async fn alias_columns_compute_at_query_time() {
    let h = setup().await;
    // A second hypertable with default/materialized/alias columns.
    let rich = json!({"fields": [
        {"name": "tenant_id", "type": "int64"},
        {"name": "ts", "type": "timestamp_ms"},
        {"name": "amount", "type": "float64", "default": "0.0"},
        {"name": "doubled", "type": "float64", "materialized": "amount * 2.0"},
        {"name": "half", "type": "float64", "alias": "amount / 2.0"}
    ]});
    let ht = h
        .catalog
        .create_hypertable(
            "rich",
            &rich,
            &json!({"columns": []}),
            &["tenant_id".to_string(), "ts".to_string()],
            "tenant_id",
        )
        .await
        .unwrap();
    h.catalog
        .create_logical_table(NamespaceId(1), "rich", ht)
        .await
        .unwrap();

    // One file via the spec-aware encoder (amount omitted in row 2).
    let cols = ukiel_core::TableColumns::parse(&rich).unwrap();
    let part = ukiel_ingest::encode_rows(
        &cols,
        "tenant_id",
        "ts",
        vec![
            json!({"tenant_id": 1, "ts": 1, "amount": 8.0}),
            json!({"tenant_id": 1, "ts": 2}),
        ],
    )
    .unwrap();
    let path = format!("ht/{ht}/L0/rich.parquet");
    let size = part.bytes.len() as i64;
    h.store
        .put(&Path::from(path.clone()), part.bytes.into())
        .await
        .unwrap();
    h.catalog
        .commit(
            ht,
            CommitOp::Add {
                parts: vec![PartMeta {
                    path,
                    partition_values: json!({}),
                    packing_key_min: part.key_min,
                    packing_key_max: part.key_max,
                    row_count: part.row_count,
                    size_bytes: size,
                    level: 0,
                    column_stats: None,
                }],
            },
            None,
        )
        .await
        .unwrap();

    let store_url = url::Url::parse(STORE_URL).unwrap();
    let ctx = ukiel_query::context::session_for_namespace(
        &h.catalog,
        NamespaceId(1),
        h.store.clone(),
        &store_url,
    )
    .await
    .unwrap();

    // Alias computed at read time; default and materialized read from storage.
    let batches = ctx
        .sql("SELECT amount, doubled, half FROM rich ORDER BY ts")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(all_f64(&batches, "amount"), vec![8.0, 0.0]);
    assert_eq!(all_f64(&batches, "doubled"), vec![16.0, 0.0]);
    assert_eq!(all_f64(&batches, "half"), vec![4.0, 0.0]);

    // SELECT * includes alias and materialized columns (documented divergence
    // from ClickHouse) and still works.
    let batches = ctx
        .sql("SELECT * FROM rich")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(batches[0].num_columns(), 5);

    // Filtering on an alias works (DataFusion applies it above our scan).
    let batches = ctx
        .sql("SELECT ts FROM rich WHERE half > 1.0")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(all_i64(&batches, "ts"), vec![1]);
}

#[tokio::test]
async fn information_schema_lists_namespace_tables_and_columns() {
    let h = setup().await;
    let store_url = url::Url::parse(STORE_URL).unwrap();

    // Give namespace 2 a distinctly-named table so a scoping leak would be
    // visible: if namespace 1's information_schema saw namespace 2's tables,
    // `secrets` would show up. (setup() already gives both ns 1 and ns 2 an
    // `events` table, which alone can't distinguish scoping from a leak.)
    h.catalog
        .create_logical_table(NamespaceId(2), "secrets", h.ht)
        .await
        .unwrap();

    async fn table_names(h: &Harness, ns: i64, store_url: &url::Url) -> Vec<String> {
        let ctx = ukiel_query::context::session_for_namespace(
            &h.catalog,
            NamespaceId(ns),
            h.store.clone(),
            store_url,
        )
        .await
        .unwrap();
        let batches = ctx
            .sql(
                "SELECT table_name FROM information_schema.tables \
                  WHERE table_schema = 'public' ORDER BY table_name",
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        all_strings(&batches, "table_name")
    }

    // Namespace 1 sees only its own table — never namespace 2's `secrets`.
    let ns1 = table_names(&h, 1, &store_url).await;
    assert_eq!(ns1, vec!["events"]);
    assert!(
        !ns1.contains(&"secrets".to_string()),
        "namespace 1 must not see namespace 2's tables"
    );

    // Namespace 2 sees both of its own tables.
    assert_eq!(
        table_names(&h, 2, &store_url).await,
        vec!["events", "secrets"]
    );

    // A namespace with no logical tables gets an empty list, not an error.
    assert!(table_names(&h, 999, &store_url).await.is_empty());

    // Columns too (name + type), via the provider's query schema.
    let ctx = ukiel_query::context::session_for_namespace(
        &h.catalog,
        NamespaceId(1),
        h.store.clone(),
        &store_url,
    )
    .await
    .unwrap();
    let batches = ctx
        .sql(
            "SELECT column_name FROM information_schema.columns \
              WHERE table_name = 'events' ORDER BY ordinal_position",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(
        all_strings(&batches, "column_name"),
        vec!["tenant_id", "ts", "payload"]
    );
}

#[tokio::test]
async fn narrow_projection_and_alias_on_unprojected_column() {
    let h = setup().await;
    let ctx = ctx_with_provider(&h, 1).await;

    // Narrow projection: only 'payload' requested; packing key is scanned
    // for isolation but not returned. Results identical to before.
    let batches = ctx
        .sql("SELECT payload FROM events ORDER BY ts")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(all_strings(&batches, "payload"), vec!["a1", "a2"]);
    assert_eq!(batches[0].num_columns(), 1);

    // count(*): empty projection still isolates correctly.
    let batches = ctx
        .sql("SELECT count(*) AS n FROM events")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(all_i64(&batches, "n"), vec![2]);
}

#[tokio::test]
async fn pushdown_prunes_row_groups_in_packed_files() {
    use datafusion::arrow::array::{Int64Array, StringArray};
    use datafusion::arrow::record_batch::RecordBatch;
    use object_store::ObjectStoreExt;
    use parquet::arrow::ArrowWriter;
    use parquet::basic::{Compression, ZstdLevel};
    use parquet::file::properties::WriterProperties;

    let h = setup().await;
    // Two tenants, 4 rows each, one row group each.
    let schema = std::sync::Arc::new(
        ukiel_core::arrow_schema_from_json(&json!({"fields": [
            {"name": "tenant_id", "type": "int64"},
            {"name": "ts", "type": "timestamp_ms"},
            {"name": "payload", "type": "utf8"}
        ]}))
        .unwrap(),
    );
    let ht = h
        .catalog
        .create_hypertable(
            "rg",
            &json!({"fields": [
                {"name": "tenant_id", "type": "int64"},
                {"name": "ts", "type": "timestamp_ms"},
                {"name": "payload", "type": "utf8"}
            ]}),
            &json!({"columns": []}),
            &["tenant_id".to_string(), "ts".to_string()],
            "tenant_id",
        )
        .await
        .unwrap();
    h.catalog
        .create_logical_table(NamespaceId(1), "rg", ht)
        .await
        .unwrap();

    let tenants = Int64Array::from(vec![1, 1, 1, 1, 2, 2, 2, 2]);
    let ts = Int64Array::from(vec![1, 2, 3, 4, 1, 2, 3, 4]);
    let payload = StringArray::from(vec!["a", "b", "c", "d", "e", "f", "g", "h"]);
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            std::sync::Arc::new(tenants),
            std::sync::Arc::new(ts),
            std::sync::Arc::new(payload),
        ],
    )
    .unwrap();
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .set_max_row_group_row_count(Some(4))
        .build();
    let mut bytes = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut bytes, schema, Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let path = format!("ht/{ht}/L1/two-groups.parquet");
    let size = bytes.len() as i64;
    h.store
        .put(&Path::from(path.clone()), bytes.into())
        .await
        .unwrap();
    h.catalog
        .commit(
            ht,
            CommitOp::Add {
                parts: vec![PartMeta {
                    path,
                    partition_values: json!({}),
                    packing_key_min: 1,
                    packing_key_max: 2,
                    row_count: 8,
                    size_bytes: size,
                    level: 1,
                    column_stats: None,
                }],
            },
            None,
        )
        .await
        .unwrap();

    let store_url = url::Url::parse(STORE_URL).unwrap();
    let ctx = session_for_namespace(&h.catalog, NamespaceId(1), h.store.clone(), &store_url)
        .await
        .unwrap();

    // Correctness: namespace 1 sees exactly its 4 rows.
    let batches = ctx
        .sql("SELECT payload FROM rg ORDER BY ts")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(all_strings(&batches, "payload"), vec!["a", "b", "c", "d"]);

    // Pruning: tenant 2's row group is skipped by statistics.
    let batches = ctx
        .sql("EXPLAIN ANALYZE SELECT count(*) FROM rg")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let analyzed = datafusion::arrow::util::pretty::pretty_format_batches(&batches)
        .unwrap()
        .to_string();
    // This DataFusion version reports "N total → M matched"; 2 groups, 1
    // matched means tenant 2's row group was pruned by statistics.
    assert!(
        analyzed.contains("row_groups_pruned_statistics=2 total → 1 matched"),
        "expected one row group pruned; explain analyze was:\n{analyzed}"
    );
}

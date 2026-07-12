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
        &["tenant_id".to_string(), "ts".to_string()],
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
        &["tenant_id".to_string(), "ts".to_string()],
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
        Some(namespace),
        ObjectStoreUrl::parse(STORE_URL).unwrap(),
        Arc::new(CachingParquetFileReaderFactory::new(
            h.store.clone(),
            test_metadata_cache(),
        )),
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

    let ctx = session_for_namespace(
        &h.catalog,
        NamespaceId(1),
        h.store.clone(),
        &store_url,
        test_metadata_cache(),
    )
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
    let ctx = session_for_namespace(
        &h.catalog,
        NamespaceId(42),
        h.store.clone(),
        &store_url,
        test_metadata_cache(),
    )
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

    let ctx = session_for_namespace(
        &h.catalog,
        NamespaceId(1),
        cached.clone(),
        &store_url,
        test_metadata_cache(),
    )
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

    let ctx = session_for_namespace(
        &h.catalog,
        NamespaceId(1),
        cached,
        &store_url,
        test_metadata_cache(),
    )
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
        &["tenant_id".to_string(), "ts".to_string()],
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
        test_metadata_cache(),
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
            test_metadata_cache(),
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
        test_metadata_cache(),
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
    let ctx = session_for_namespace(
        &h.catalog,
        NamespaceId(1),
        h.store.clone(),
        &store_url,
        test_metadata_cache(),
    )
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

#[tokio::test]
async fn order_by_sort_key_results_stay_correct_and_ordered() {
    let h = setup().await;
    let ctx = ctx_with_provider(&h, 1).await;

    // Correctness is the load-bearing assertion: ORDER BY ts returns this
    // namespace's rows in ts order, whether or not the plan sorts.
    let batches = ctx
        .sql("SELECT payload FROM events ORDER BY ts")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(all_strings(&batches, "payload"), vec!["a1", "a2"]);

    // The provider declares the `(packing_key, ts)` output ordering only on
    // *predicated* scans (a WHERE clause or the isolation slice) — the
    // measured split from `docs/notes/2026-07-12-official-suite-gap.md`: on
    // filterless scans the declaration forces order-preserving file
    // repartitioning (whole-file groups, largest-file critical path, 1.54x on
    // packed scan aggregates), while on predicated scans dropping it cost
    // 15-25% wall via lower scan concurrency. Both sides are pinned here so a
    // change in either direction is a deliberate, re-measured act.

    // Predicated (scoped) scan: declaration active. `sort_order_for_reorder`
    // tracks it on this query — pushdown clears the declaration from the
    // DataSourceExec display, but the reorder marker only appears when the
    // scan declared an ordering.
    let batches = ctx
        .sql("EXPLAIN SELECT ts FROM events ORDER BY ts")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let plan = datafusion::arrow::util::pretty::pretty_format_batches(&batches)
        .unwrap()
        .to_string();
    assert!(
        plan.contains("sort_order_for_reorder"),
        "expected the predicated scan to declare its ordering; plan was:\n{plan}"
    );

    // Filterless unscoped scan: no declaration (`output_ordering=` absent
    // from DataSourceExec even with the full sort key projected).
    let store_url = url::Url::parse(STORE_URL).unwrap();
    let operator = ukiel_query::context::operator_session(
        &h.catalog,
        h.store.clone(),
        &store_url,
        test_metadata_cache(),
    )
    .await
    .unwrap();
    let batches = operator
        .sql("EXPLAIN SELECT tenant_id, ts FROM events")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let plan = datafusion::arrow::util::pretty::pretty_format_batches(&batches)
        .unwrap()
        .to_string();
    assert!(
        !plan.contains("output_ordering="),
        "expected no declared ordering on a filterless scan (see provider.rs scan comment); plan was:\n{plan}"
    );
}

#[tokio::test]
async fn catalog_prunes_parts_by_ts_stats() {
    let h = setup().await;
    // A second table with two parts in disjoint ts ranges, stats populated.
    let ht = h
        .catalog
        .create_hypertable(
            "tsr",
            &events_schema(),
            &json!({"columns": []}),
            &["tenant_id".to_string(), "ts".to_string()],
            "tenant_id",
        )
        .await
        .unwrap();
    h.catalog
        .create_logical_table(NamespaceId(1), "tsr", ht)
        .await
        .unwrap();
    let schema = arrow_schema_from_json(&events_schema()).unwrap();

    for (name, ts_base, payload) in [("early", 100i64, "old"), ("late", 10_000i64, "new")] {
        let part = rows_to_parquet(
            &schema,
            "tenant_id",
            "ts",
            &["tenant_id".to_string(), "ts".to_string()],
            vec![json!({"tenant_id": 1, "ts": ts_base, "payload": payload})],
        )
        .unwrap();
        let path = format!("ht/{ht}/L0/{name}.parquet");
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
                        packing_key_min: 1,
                        packing_key_max: 1,
                        row_count: 1,
                        size_bytes: size,
                        level: 0,
                        column_stats: Some(json!({"ts": {"min": ts_base, "max": ts_base}})),
                    }],
                },
                None,
            )
            .await
            .unwrap();
    }

    let store_url = url::Url::parse(STORE_URL).unwrap();
    let ctx = ukiel_query::context::session_for_namespace(
        &h.catalog,
        NamespaceId(1),
        h.store.clone(),
        &store_url,
        test_metadata_cache(),
    )
    .await
    .unwrap();

    // Correctness: the predicate returns only the matching row.
    let batches = ctx
        .sql("SELECT payload FROM tsr WHERE ts < 5000")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(all_strings(&batches, "payload"), vec!["old"]);

    // Pruning: the physical plan's file listing contains only the early part.
    let batches = ctx
        .sql("EXPLAIN SELECT payload FROM tsr WHERE ts < 5000")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let plan = datafusion::arrow::util::pretty::pretty_format_batches(&batches)
        .unwrap()
        .to_string();
    assert!(
        plan.contains("early.parquet"),
        "plan should list the early part:\n{plan}"
    );
    assert!(
        !plan.contains("late.parquet"),
        "late part must be pruned in the catalog, not scanned:\n{plan}"
    );
}

use ukiel_query::metadata_cache::{CachingParquetFileReaderFactory, ParquetMetadataCache};

/// A fresh cache for tests that don't assert on cache behavior. (In a real
/// process there is exactly one, shared; tests isolate by making their own.)
pub fn test_metadata_cache() -> Arc<ParquetMetadataCache> {
    Arc::new(ParquetMetadataCache::default())
}

#[tokio::test]
async fn metadata_cache_hits_across_queries_with_identical_results() {
    let h = setup().await;
    let store_url = url::Url::parse(STORE_URL).unwrap();
    let cache = Arc::new(ParquetMetadataCache::default());

    // First query: cold cache — every footer read is a miss.
    let ctx = session_for_namespace(
        &h.catalog,
        NamespaceId(1),
        h.store.clone(),
        &store_url,
        cache.clone(),
    )
    .await
    .unwrap();
    let first = ctx
        .sql("SELECT payload FROM events ORDER BY ts")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(all_strings(&first, "payload"), vec!["a1", "a2"]);
    assert!(cache.misses() > 0, "cold cache: first query must miss");
    assert!(!cache.is_empty(), "first query must populate the cache");
    let misses_after_first = cache.misses();
    let hits_after_first = cache.hits();

    // Second query, new session, same process-wide cache: footers are served
    // from memory (hits), no new misses, identical results.
    let ctx = session_for_namespace(
        &h.catalog,
        NamespaceId(1),
        h.store.clone(),
        &store_url,
        cache.clone(),
    )
    .await
    .unwrap();
    let second = ctx
        .sql("SELECT payload FROM events ORDER BY ts")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(
        all_strings(&second, "payload"),
        all_strings(&first, "payload"),
        "results must be identical with and without a warm cache"
    );
    assert!(
        cache.hits() > hits_after_first,
        "warm cache: second query must hit (hits {} -> {})",
        hits_after_first,
        cache.hits()
    );
    assert_eq!(
        cache.misses(),
        misses_after_first,
        "an identical second query must add no misses"
    );
}

/// The operator (unscoped) session sees every namespace's rows in one table —
/// what the whole-table benchmark suites need — while the namespace session it
/// sits next to keeps isolating. The security posture is a *construction*
/// invariant, re-asserted here in prose because it cannot be asserted in code:
/// `operator_session` is referenced by the bench binary and this test only, and
/// `ukiel_query::server` must never call it (issue 0001 — the server's
/// `namespace_id` is caller-supplied, so an unscoped route would leak every
/// tenant's data). The `server_never_references_operator_session` test below
/// holds that line mechanically.
#[tokio::test]
async fn operator_session_sees_every_namespace_while_scoped_sessions_isolate() {
    let h = setup().await;
    let store_url = url::Url::parse(STORE_URL).unwrap();

    // Scoped: namespace 1 still sees only its own rows, packed file and all.
    let scoped = session_for_namespace(
        &h.catalog,
        NamespaceId(1),
        h.store.clone(),
        &store_url,
        test_metadata_cache(),
    )
    .await
    .unwrap();
    let batches = scoped
        .sql("SELECT payload FROM events ORDER BY ts")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(all_strings(&batches, "payload"), vec!["a1", "a2"]);

    // Unscoped: the hypertable is registered under its *hypertable* name and
    // every namespace's rows are visible — across the packed and dedicated file.
    let operator = ukiel_query::context::operator_session(
        &h.catalog,
        h.store.clone(),
        &store_url,
        test_metadata_cache(),
    )
    .await
    .unwrap();
    let batches = operator
        .sql("SELECT payload FROM events ORDER BY tenant_id, ts")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(
        all_strings(&batches, "payload"),
        vec!["a1", "a2", "b1", "b2"],
        "unscoped session must see both namespaces"
    );

    // The aggregate the official suites are made of: a whole-table count, with
    // the packing key projected away entirely.
    let batches = operator
        .sql("SELECT count(*) AS n FROM events")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(all_i64(&batches, "n"), vec![4]);

    // Grouping by the packing key recovers the per-namespace split — proof the
    // rows are genuinely commingled rather than one namespace's leaking through.
    let batches = operator
        .sql("SELECT tenant_id, count(*) AS n FROM events GROUP BY tenant_id ORDER BY tenant_id")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(all_i64(&batches, "tenant_id"), vec![1, 2]);
    assert_eq!(all_i64(&batches, "n"), vec![2, 2]);
}

/// The unscoped session must stay unreachable from the HTTP server. This is the
/// one invariant a runtime test cannot express (you cannot assert the absence of
/// a route that was never written), so assert it against the source itself.
#[test]
fn server_never_references_operator_session() {
    let server = include_str!("../src/server.rs");
    assert!(
        !server.contains("operator_session"),
        "server.rs must never build an unscoped session (issue 0001): every \
         HTTP path is namespace-scoped, and the namespace is caller-supplied"
    );
}

/// Plan 16, the headline: a tenant whose key falls *inside* a part's min/max but
/// owns no rows in it gets **zero object-store requests**.
///
/// Range pruning cannot see this — a part spanning keys 1..4500 "contains" 300,
/// so today the scan opens the file to find nothing. The packing-key bitmap makes
/// the decision exact, and the file is never opened.
///
/// The degradation rule is pinned right next to it: a part whose stats carry no
/// bitmap (every file written before this plan) is still scanned. Pruning may
/// only remove what it can *prove* is absent.
#[tokio::test]
async fn bitmap_prunes_a_sparse_in_range_tenant_to_zero_files() {
    use ukiel_core::stats::{key_bitmap, with_key_index};

    let h = setup().await;
    let store_url = url::Url::parse(STORE_URL).unwrap();

    // The seeded `events` fixture has one packed part (keys 1,2) and one
    // dedicated part (key 2). Widen the packed part's claimed key range to
    // 1..4500 and give it a real bitmap over {1, 2} — the shape a production
    // packed file has: a wide range, a sparse key set.
    let schema = ukiel_core::arrow_schema_from_json(&json!({"fields": [
        {"name": "tenant_id", "type": "int64"},
        {"name": "ts", "type": "timestamp_ms"},
        {"name": "payload", "type": "utf8"}
    ]}))
    .unwrap();
    let part = ukiel_ingest::rows_to_parquet(
        &schema,
        "tenant_id",
        "ts",
        &["tenant_id".to_string(), "ts".to_string()],
        vec![
            json!({"tenant_id": 1, "ts": 100, "payload": "a1"}),
            json!({"tenant_id": 2, "ts": 150, "payload": "b1"}),
        ],
    )
    .unwrap();

    let batch = {
        // Rebuild the batch to hand `key_bitmap` the exact rows written.
        use datafusion::arrow::array::{Int64Array, StringArray};
        use datafusion::arrow::record_batch::RecordBatch;
        RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(Int64Array::from(vec![1i64, 2])),
                Arc::new(Int64Array::from(vec![100i64, 150])),
                Arc::new(StringArray::from(vec!["a1", "b1"])),
            ],
        )
        .unwrap()
    };
    let bitmap = key_bitmap(&batch, "tenant_id").expect("bitmap over {1,2}");

    let ht2 = h
        .catalog
        .create_hypertable(
            "sparse",
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
    for ns in [1i64, 2, 300] {
        h.catalog
            .create_logical_table(NamespaceId(ns), "sparse", ht2)
            .await
            .unwrap();
    }

    let path = format!("ht/{ht2}/L1/sparse.parquet");
    let size = part.bytes.len() as i64;
    h.store
        .put(&Path::from(path.clone()), part.bytes.into())
        .await
        .unwrap();
    h.catalog
        .commit(
            ht2,
            CommitOp::Add {
                parts: vec![PartMeta {
                    path,
                    partition_values: json!({}),
                    // A wide claimed range: 300 is inside it, so range pruning
                    // keeps this part. Only the bitmap knows better.
                    packing_key_min: 1,
                    packing_key_max: 4500,
                    row_count: 2,
                    size_bytes: size,
                    level: 1,
                    column_stats: with_key_index(None, Some(bitmap.clone()), None),
                }],
            },
            None,
        )
        .await
        .unwrap();

    let session = async |ns: i64| {
        session_for_namespace(
            &h.catalog,
            NamespaceId(ns),
            h.store.clone(),
            &store_url,
            test_metadata_cache(),
        )
        .await
        .unwrap()
    };

    // Namespace 300: in range, absent from the bitmap → no file is planned at
    // all. This is the zero-object-store-request headline.
    let ctx = session(300).await;
    let plan = ctx
        .sql("EXPLAIN SELECT count(*) FROM sparse")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let text = datafusion::arrow::util::pretty::pretty_format_batches(&plan)
        .unwrap()
        .to_string();
    assert!(
        !text.contains(".parquet"),
        "a sparse in-range tenant must plan zero files, got:\n{text}"
    );
    let batches = ctx
        .sql("SELECT count(*) AS n FROM sparse")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(all_i64(&batches, "n"), vec![0]);

    // The tenants that *are* in the bitmap still see exactly their rows —
    // isolation re-asserted next to the new pruning path.
    for (ns, expected) in [(1i64, "a1"), (2, "b1")] {
        let ctx = session(ns).await;
        let batches = ctx
            .sql("SELECT payload FROM sparse")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(all_strings(&batches, "payload"), vec![expected]);
    }

    // Degradation pin: the seeded `events` parts carry `column_stats: None`
    // (every file written before plan 16 does). They must still be scanned —
    // absence of an index is not evidence of absence of rows.
    let ctx = session(1).await;
    let batches = ctx
        .sql("SELECT payload FROM events ORDER BY ts")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(all_strings(&batches, "payload"), vec!["a1", "a2"]);
}

/// Plan 16 #6: catalog row-group key spans become a `ParquetAccessPlan`.
///
/// **What this test can and cannot show.** For a *partial* skip, DataFusion's
/// own statistics pruning reaches the same answer once it has read the footer,
/// and its metrics do not distinguish "the provider skipped this group" from
/// "statistics pruned it" — so a partial-skip assertion would pass with the
/// feature disabled and prove nothing (verified: it does). The access plan's
/// real value there is the *cold* path — deciding without reading the footer at
/// all — which no metric exposes. That gap is recorded in the perf note rather
/// than papered over with a test that only looks convincing.
///
/// What IS observable, and what this test pins: when the spans prove *every*
/// row group excludes the key, the provider drops the file from the scan
/// entirely — zero files planned. Statistics pruning cannot do that, because it
/// must open the file to find out.
#[tokio::test]
async fn catalog_spans_preskip_row_groups() {
    use datafusion::arrow::array::{Int64Array, StringArray};
    use datafusion::arrow::record_batch::RecordBatch;
    use object_store::ObjectStoreExt;

    let h = setup().await;
    let store_url = url::Url::parse(STORE_URL).unwrap();

    let schema = Arc::new(
        ukiel_core::arrow_schema_from_json(&json!({"fields": [
            {"name": "tenant_id", "type": "int64"},
            {"name": "ts", "type": "timestamp_ms"},
            {"name": "payload", "type": "utf8"}
        ]}))
        .unwrap(),
    );
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1i64, 1, 2, 2])),
            Arc::new(Int64Array::from(vec![10i64, 20, 30, 40])),
            Arc::new(StringArray::from(vec!["a", "b", "c", "d"])),
        ],
    )
    .unwrap();

    // The real key-aligned writer produces the file AND the spans that go in the
    // catalog, so the two cannot drift apart in this test.
    let sort_key = vec!["tenant_id".to_string(), "ts".to_string()];
    let opts = ukiel_core::WriteOpts {
        level: 1,
        ts_column: Some("ts".to_string()),
        bloom_columns: vec![],
        max_row_group_rows: 2, // one row group per key
    };
    let (bytes, spans) = ukiel_compactor::rewrite::batch_to_parquet_key_aligned(
        &batch,
        "tenant_id",
        &sort_key,
        &opts,
    )
    .unwrap();
    let spans = spans.expect("the key-aligned writer records spans");
    assert_eq!(spans, json!([[1, 1], [2, 2]]), "one row group per key");

    let ht2 = h
        .catalog
        .create_hypertable(
            "spans",
            &json!({"fields": [
                {"name": "tenant_id", "type": "int64"},
                {"name": "ts", "type": "timestamp_ms"},
                {"name": "payload", "type": "utf8"}
            ]}),
            &json!({"columns": []}),
            &sort_key,
            "tenant_id",
        )
        .await
        .unwrap();
    for ns in [1i64, 2, 9] {
        h.catalog
            .create_logical_table(NamespaceId(ns), "spans", ht2)
            .await
            .unwrap();
    }

    let path = format!("ht/{ht2}/L1/aligned.parquet");
    let size = bytes.len() as i64;
    h.store
        .put(&Path::from(path.clone()), bytes.into())
        .await
        .unwrap();
    h.catalog
        .commit(
            ht2,
            CommitOp::Add {
                parts: vec![PartMeta {
                    path,
                    partition_values: json!({}),
                    // The claimed range is deliberately wide: key 9 is inside it,
                    // so range pruning keeps the part and only the SPANS can
                    // eliminate it. No bitmap here, so this isolates the access
                    // plan from the plan-16 bitmap path.
                    packing_key_min: 1,
                    packing_key_max: 100,
                    row_count: 4,
                    size_bytes: size,
                    level: 1,
                    column_stats: ukiel_core::stats::with_key_index(None, None, Some(spans)),
                }],
            },
            None,
        )
        .await
        .unwrap();

    let session = async |ns: i64| {
        session_for_namespace(
            &h.catalog,
            NamespaceId(ns),
            h.store.clone(),
            &store_url,
            test_metadata_cache(),
        )
        .await
        .unwrap()
    };

    // Correctness under a partial skip: row group 1 is pre-skipped, and the rows
    // that survive are exactly key 1's. An access plan that lost rows would show
    // up here.
    let ctx = session(1).await;
    let batches = ctx
        .sql("SELECT payload FROM spans ORDER BY ts")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(all_strings(&batches, "payload"), vec!["a", "b"]);

    // The observable win: key 9 is inside the part's claimed range [1, 100] but
    // no row group's span contains it, so the file is never planned. Statistics
    // pruning would have had to open it to learn that.
    let ctx = session(9).await;
    let plan = ctx
        .sql("EXPLAIN SELECT count(*) FROM spans")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let text = datafusion::arrow::util::pretty::pretty_format_batches(&plan)
        .unwrap()
        .to_string();
    assert!(
        !text.contains(".parquet"),
        "spans exclude every row group: the file must not be planned at all, got:\n{text}"
    );
    let batches = ctx
        .sql("SELECT count(*) AS n FROM spans")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(all_i64(&batches, "n"), vec![0]);
}

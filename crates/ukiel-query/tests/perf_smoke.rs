//! Perf smoke (P1 micro tier): prints median latencies for the Tier-1 read
//! scenarios, one line per (dataset, scenario). Not CI-gating.
//!
//! Run manually:
//!   cargo test -p ukiel-query --test perf_smoke -- --ignored --nocapture 2>&1 | grep PERF
//! Record the medians in docs/notes/2026-07-06-parquet-read-performance.md.
//!
//! The harness is **dataset-parametric**: a [`Dataset`] describes a fixture
//! (schema, seeded row generator, row-group sizing, query namespace) plus the
//! append-only list of query scenarios to time against it. `datasets()` returns
//! every dataset the smoke test runs; adding coverage is one more entry there,
//! not another copy of the timing loop. Scenarios are append-only — existing
//! ones never change semantics (that would orphan recorded history).

mod query_test;

use std::sync::Arc;
use std::time::{Duration, Instant};

use datafusion::arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::prelude::SessionContext;
use object_store::ObjectStoreExt;
use object_store::path::Path;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use serde_json::json;
use ukiel_core::{CommitOp, NamespaceId, PartMeta, arrow_schema_from_json};

use query_test::{Harness, STORE_URL, setup};

/// Builds all column arrays for a fixture (whole table, already sorted by the
/// packing key then ts). Signature: `(schema, tenants, rows_per_tenant)`.
type ColumnBuilder = fn(&SchemaRef, i64, i64) -> Vec<ArrayRef>;

/// A reusable perf fixture + the scenarios timed against it. One packed file:
/// `tenants` × `rows_per_tenant` rows, `row_group_size` chosen so pruning has
/// something to prune (the default writer emits one row group for small files,
/// which would hide row-group pruning entirely).
struct Dataset {
    /// Hypertable + logical-table name; also the scenario-line prefix.
    name: &'static str,
    schema_json: serde_json::Value,
    packing_key: &'static str,
    sort_key: &'static [&'static str],
    tenants: i64,
    rows_per_tenant: i64,
    row_group_size: usize,
    /// Namespace to run scenarios as (== packing-key value; see the v1
    /// convention in `session_for_namespace`).
    query_ns: i64,
    build_columns: ColumnBuilder,
    /// `(label, sql)`. Append-only across the plan's history.
    scenarios: Vec<(&'static str, String)>,
}

/// Every dataset the smoke test measures. Add one here to extend coverage.
fn datasets() -> Vec<Dataset> {
    vec![wide_packed()]
}

const WIDE_COLS: usize = 8;

/// The plan-11 baseline dataset: 100 tenants × 1000 rows in one packed file,
/// sorted by (tenant_id, ts), one row group per tenant, 8 wide utf8 columns.
fn wide_packed() -> Dataset {
    let mut fields = vec![
        json!({"name": "tenant_id", "type": "int64"}),
        json!({"name": "ts", "type": "timestamp_ms"}),
    ];
    for i in 0..WIDE_COLS {
        fields.push(json!({"name": format!("c{i}"), "type": "utf8"}));
    }
    let name = "wide";
    Dataset {
        name,
        schema_json: json!({ "fields": fields }),
        packing_key: "tenant_id",
        sort_key: &["tenant_id", "ts"],
        tenants: 100,
        rows_per_tenant: 1_000,
        row_group_size: 1_000, // one row group per tenant
        query_ns: 42,          // any mid-range tenant
        build_columns: wide_columns,
        scenarios: vec![
            (
                "selective_count_ms",
                format!("SELECT count(*) AS n FROM {name}"),
            ),
            ("narrow_projection_ms", format!("SELECT c0 FROM {name}")),
            (
                "order_by_limit_ms",
                format!("SELECT ts FROM {name} ORDER BY ts LIMIT 100"),
            ),
        ],
    }
}

/// tenant_id = row/rows_per_tenant + 1; ts = row%rows_per_tenant; the remaining
/// (schema-width - 2) columns are deterministic utf8. Fully seeded by index.
fn wide_columns(schema: &SchemaRef, tenants: i64, rows_per_tenant: i64) -> Vec<ArrayRef> {
    let total = (tenants * rows_per_tenant) as usize;
    let tenant_arr: Int64Array = (0..total as i64)
        .map(|i| Some(i / rows_per_tenant + 1))
        .collect();
    let ts: Int64Array = (0..total as i64)
        .map(|i| Some(i % rows_per_tenant))
        .collect();
    let mut columns: Vec<ArrayRef> = vec![Arc::new(tenant_arr), Arc::new(ts)];
    let wide_cols = schema.fields().len() - 2;
    for c in 0..wide_cols {
        let col: StringArray = (0..total).map(|i| Some(format!("v{c}-{i}"))).collect();
        columns.push(Arc::new(col));
    }
    columns
}

/// Materializes `ds`'s fixture into the catalog + object store and returns a
/// session scoped to its query namespace.
async fn build_and_session(h: &Harness, ds: &Dataset) -> SessionContext {
    let schema = Arc::new(arrow_schema_from_json(&ds.schema_json).unwrap());
    let sort_key: Vec<String> = ds.sort_key.iter().map(|s| s.to_string()).collect();
    let ht = h
        .catalog
        .create_hypertable(
            ds.name,
            &ds.schema_json,
            &json!({"columns": []}),
            &sort_key,
            ds.packing_key,
        )
        .await
        .unwrap();
    for t in 1..=ds.tenants {
        h.catalog
            .create_logical_table(NamespaceId(t), ds.name, ht)
            .await
            .unwrap();
    }

    let columns = (ds.build_columns)(&schema, ds.tenants, ds.rows_per_tenant);
    let total = ds.tenants * ds.rows_per_tenant;
    let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();

    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .set_max_row_group_row_count(Some(ds.row_group_size))
        .build();
    let mut bytes = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut bytes, schema, Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let path = format!("ht/{ht}/L1/{}-perf.parquet", ds.name);
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
                    packing_key_max: ds.tenants,
                    row_count: total,
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
    ukiel_query::context::session_for_namespace(
        &h.catalog,
        NamespaceId(ds.query_ns),
        h.store.clone(),
        &store_url,
        query_test::test_metadata_cache(),
    )
    .await
    .unwrap()
}

async fn median_ms(ctx: &SessionContext, sql: &str, iters: usize) -> f64 {
    // Warm-up (footer reads, cache effects).
    ctx.sql(sql).await.unwrap().collect().await.unwrap();
    let mut samples: Vec<Duration> = Vec::with_capacity(iters);
    for _ in 0..iters {
        let start = Instant::now();
        ctx.sql(sql).await.unwrap().collect().await.unwrap();
        samples.push(start.elapsed());
    }
    samples.sort();
    samples[iters / 2].as_secs_f64() * 1000.0
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "perf smoke: run manually with --nocapture and record medians"]
async fn perf_smoke() {
    let h = setup().await;
    for ds in datasets() {
        let ctx = build_and_session(&h, &ds).await;
        for (label, sql) in &ds.scenarios {
            let ms = median_ms(&ctx, sql, 10).await;
            println!("PERF {}/{label}={ms:.1}", ds.name);
        }

        // Sanity, not perf: the query namespace must see exactly its own rows.
        let count_sql = format!("SELECT count(*) AS n FROM {}", ds.name);
        let batches = ctx.sql(&count_sql).await.unwrap().collect().await.unwrap();
        assert_eq!(
            query_test::all_i64(&batches, "n"),
            vec![ds.rows_per_tenant],
            "{} count sanity",
            ds.name
        );
    }
}

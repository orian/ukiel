//! I/O accounting: object-store mode attributes reads to each query, and the counter delta
//! arithmetic is exact. No coordinated omission — every scheduled query is completed and
//! counted.

mod common;

use parquet_lab_bench::RunParams;
use parquet_lab_bench::counting_store::{CounterSnapshot, CountingObjectStore};
use parquet_lab_bench::runner::{Mode, ReaderFlags};
use parquet_lab_contract::{RunReport, SuiteKind};

#[test]
fn counter_deltas_are_exact() {
    let a = CounterSnapshot {
        head_requests: 5,
        get_requests: 10,
        get_range_requests: 3,
        ranges: 8,
        requested_bytes: 4096,
    };
    let b = CounterSnapshot {
        head_requests: 2,
        get_requests: 4,
        get_range_requests: 1,
        ranges: 2,
        requested_bytes: 1024,
    };
    let d = CounterSnapshot::delta(&a, &b);
    assert_eq!(d.head_requests, 3);
    assert_eq!(d.get_requests, 6);
    assert_eq!(d.requested_bytes, 3072);
}

#[tokio::test]
async fn the_counting_store_reports_reads_and_delegates_writes() {
    use object_store::ObjectStore;
    let inner: std::sync::Arc<dyn ObjectStore> =
        std::sync::Arc::new(object_store::memory::InMemory::new());
    let (store, counters) = CountingObjectStore::new(inner);
    let path = object_store::path::Path::from("a/b.bin");
    store
        .put_opts(
            &path,
            bytes::Bytes::from_static(b"hello world").into(),
            Default::default(),
        )
        .await
        .unwrap();
    // A ranged read is counted with its requested bytes.
    let _ = store
        .get_opts(
            &path,
            object_store::GetOptions {
                range: Some((0..5).into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let snap = counters.snapshot();
    assert_eq!(snap.get_requests, 1);
    assert_eq!(snap.requested_bytes, 5);
}

#[tokio::test]
async fn object_store_mode_attributes_io_to_every_query() {
    let tmp = tempfile::tempdir().unwrap();
    let mp = common::write_snapshot(tmp.path(), false, 0, 400);
    let sql = common::write_sql(tmp.path());
    let suite_out = tmp.path().join("suite.json");
    parquet_lab_bench::compile(&mp, SuiteKind::ProdSynth, &sql, &suite_out, false)
        .await
        .unwrap();

    let result = tmp.path().join("result.json");
    parquet_lab_bench::run(
        &mp,
        &suite_out,
        &result,
        RunParams {
            mode: Mode::ObjectStore,
            cold_iters: 1,
            warm_iters: 2,
            reader_flags: ReaderFlags::default(),
            run_order: 0,
            skip_manifest: None,
        },
        false,
    )
    .await
    .unwrap();

    let report = RunReport::parse(
        &result.display().to_string(),
        &std::fs::read(&result).unwrap(),
    )
    .unwrap();
    let queries = report.body["queries"].as_array().unwrap();
    // Every query completed and carries an I/O record; the parquet reader issued reads.
    for q in queries {
        let io = &q["io"];
        assert!(!io.is_null(), "object-store mode records I/O per query");
        let total = io["get_requests"].as_u64().unwrap()
            + io["get_range_requests"].as_u64().unwrap()
            + io["head_requests"].as_u64().unwrap();
        assert!(total > 0, "a scan must issue at least one read");
    }
    assert_eq!(report.body["mode"], serde_json::json!("object-store"));
}

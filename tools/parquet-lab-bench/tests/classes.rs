//! The seven registered query classes compile against a control and run under the plan
//! guard: every class projects the columns it declares and folds them into an aggregate. A
//! mis-declared class (a metadata `count(*)` masquerading as a scan) is rejected.

mod common;

use parquet_lab_bench::RunParams;
use parquet_lab_bench::runner::{Mode, ReaderFlags};
use parquet_lab_contract::{
    PredicateShape, Query, QuerySink, ResultSemantics, RunReport, Suite, SuiteKind,
};

fn bindings_json() -> serde_json::Value {
    serde_json::json!({
        "key_col": "team_id",
        "numeric_col": "value",
        "string_col": "name",
        "time_col": "timestamp",
        "hot_columns": [
            {"name": "team_id", "kind": "numeric"},
            {"name": "timestamp", "kind": "numeric"}
        ],
        "all_columns": [
            {"name": "team_id", "kind": "numeric"},
            {"name": "timestamp", "kind": "numeric"},
            {"name": "name", "kind": "string"},
            {"name": "value", "kind": "numeric"}
        ],
        "aligned_literal": 5,
        "absent_literal": 99999999
    })
}

#[tokio::test]
async fn the_seven_classes_compile_and_pass_the_plan_guard() {
    let tmp = tempfile::tempdir().unwrap();
    let mp = common::write_snapshot(tmp.path(), false, 0, 400);
    let bindings = tmp.path().join("bindings.json");
    std::fs::write(&bindings, serde_json::to_vec_pretty(&bindings_json()).unwrap()).unwrap();

    let suite_out = tmp.path().join("classes-suite.json");
    parquet_lab_bench::compile_classes(&mp, SuiteKind::ProdSynth, &bindings, &suite_out, false)
        .await
        .unwrap();

    let suite = Suite::parse(&suite_out.display().to_string(), &std::fs::read(&suite_out).unwrap()).unwrap();
    assert_eq!(suite.queries.len(), 7);
    // Every class declares required columns, a sink, and a predicate shape.
    assert!(suite.queries.iter().all(|q| !q.required_columns.is_empty()));
    let names: Vec<&str> = suite.queries.iter().map(|q| q.name.as_str()).collect();
    for expected in [
        "full_numeric_scan",
        "full_string_scan",
        "hot_set_scan",
        "all_column_scan",
        "aligned_selective_narrow",
        "scattered_selective",
        "absent_predicate",
    ] {
        assert!(names.contains(&expected), "missing class {expected}");
    }

    // Running the suite: every class passes the plan guard (or run() would error) and its
    // answer matches the control.
    let result = tmp.path().join("classes-result.json");
    parquet_lab_bench::run(
        &mp,
        &suite_out,
        &result,
        RunParams {
            mode: Mode::Local,
            cold_iters: 1,
            warm_iters: 2,
            reader_flags: ReaderFlags::default(),
            run_order: 0,
            skip_manifest: None,
            cache_receipt: None,
        },
        false,
    )
    .await
    .unwrap();

    let report = RunReport::parse(&result.display().to_string(), &std::fs::read(&result).unwrap()).unwrap();
    let queries = report.body["queries"].as_array().unwrap();
    assert_eq!(queries.len(), 7);
    for q in queries {
        assert_eq!(q["expected_match"], serde_json::json!(true));
        assert_eq!(q["plan_assertion_passed"], serde_json::json!(true));
        // New session vocabulary is present; the old cold/warm names are gone.
        assert!(q["reused_session_ms"].is_array());
        assert!(q.get("warm_ms").is_none());
    }
}

#[tokio::test]
async fn a_metadata_query_masquerading_as_a_scan_is_rejected() {
    let tmp = tempfile::tempdir().unwrap();
    let mp = common::write_snapshot(tmp.path(), false, 0, 200);

    // Hand-build a suite whose one query declares a full string scan but is actually a
    // metadata count(*) — the plan guard must reject it at run time.
    let view = parquet_lab_bench::suites::build_view_sql(
        &parquet_lab_contract::SnapshotManifest::parse(
            &mp.display().to_string(),
            &std::fs::read(&mp).unwrap(),
        )
        .unwrap(),
    )
    .unwrap();
    let suite = Suite {
        suite_version: parquet_lab_contract::SUITE_VERSION.into(),
        kind: SuiteKind::ProdSynth,
        view_sql: view,
        queries: vec![Query {
            name: "fake_string_scan".into(),
            sql: "SELECT count(*) FROM events".into(),
            expected_result_digest: String::new(), // skip the correctness gate; test the plan guard
            result_semantics: ResultSemantics::Ordered,
            required_columns: vec!["name".into()],
            sink: QuerySink::Aggregate,
            predicate_shape: PredicateShape::FullScan,
        }],
        probes: vec![],
        skipped_probes: vec![],
    };
    let suite_path = tmp.path().join("fake-suite.json");
    std::fs::write(&suite_path, serde_json::to_vec_pretty(&suite).unwrap()).unwrap();

    let err = parquet_lab_bench::run(
        &mp,
        &suite_path,
        &tmp.path().join("r.json"),
        RunParams {
            mode: Mode::Local,
            cold_iters: 1,
            warm_iters: 1,
            reader_flags: ReaderFlags::default(),
            run_order: 0,
            skip_manifest: None,
            cache_receipt: None,
        },
        false,
    )
    .await
    .unwrap_err();
    assert!(
        err.to_string().contains("does not project required column")
            || err.to_string().contains("metadata"),
        "{err}"
    );
}

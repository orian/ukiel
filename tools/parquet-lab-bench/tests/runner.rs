//! Compile a suite from a control and run it: result equivalence, cold/warm timing, plan
//! capture, and atomic reports.

mod common;

use parquet_lab_bench::RunParams;
use parquet_lab_bench::runner::{Mode, ReaderFlags};
use parquet_lab_contract::{RunReport, Suite, SuiteKind};

#[tokio::test]
async fn compile_then_run_produces_a_bound_result_report() {
    let tmp = tempfile::tempdir().unwrap();
    let mp = common::write_snapshot(tmp.path(), false, 0, 200);
    let sql = common::write_sql(tmp.path());
    let suite_out = tmp.path().join("suite.json");

    parquet_lab_bench::compile(&mp, SuiteKind::ProdSynth, &sql, &suite_out, false)
        .await
        .unwrap();

    // The compiled suite baked a view and three queries with expected digests.
    let suite = Suite::parse(
        &suite_out.display().to_string(),
        &std::fs::read(&suite_out).unwrap(),
    )
    .unwrap();
    assert_eq!(suite.queries.len(), 3);
    assert!(suite.view_sql.contains("CREATE OR REPLACE VIEW events"));
    assert!(
        suite
            .queries
            .iter()
            .all(|q| q.expected_result_digest.len() == 64)
    );

    let result = tmp.path().join("result.json");
    parquet_lab_bench::run(
        &mp,
        &suite_out,
        &result,
        RunParams {
            mode: Mode::Local,
            cold_iters: 1,
            warm_iters: 5,
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
    assert_eq!(report.identity.snapshot_digest.len(), 64);
    assert!(report.identity.suite_digest.is_some());
    let queries = report.body["queries"].as_array().unwrap();
    assert_eq!(queries.len(), 3);
    for q in queries {
        assert_eq!(q["expected_match"], serde_json::json!(true));
        assert_eq!(q["warm_ms"].as_array().unwrap().len(), 5);
        assert!(
            q["plan"].as_str().unwrap().contains("Exec"),
            "a physical plan was captured"
        );
    }
}

#[tokio::test]
async fn a_report_will_not_overwrite_without_replace() {
    let tmp = tempfile::tempdir().unwrap();
    let mp = common::write_snapshot(tmp.path(), false, 0, 40);
    let sql = common::write_sql(tmp.path());
    let suite_out = tmp.path().join("suite.json");
    parquet_lab_bench::compile(&mp, SuiteKind::ProdSynth, &sql, &suite_out, false)
        .await
        .unwrap();
    let result = tmp.path().join("result.json");
    let params = || RunParams {
        mode: Mode::Local,
        cold_iters: 1,
        warm_iters: 1,
        reader_flags: ReaderFlags::default(),
        run_order: 0,
        skip_manifest: None,
    };
    parquet_lab_bench::run(&mp, &suite_out, &result, params(), false)
        .await
        .unwrap();
    let err = parquet_lab_bench::run(&mp, &suite_out, &result, params(), false)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("--replace"), "{err}");
    parquet_lab_bench::run(&mp, &suite_out, &result, params(), true)
        .await
        .unwrap();
}

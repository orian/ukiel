//! Result equivalence across physical representations: a variant that stores team_id as
//! Int32 answers identically to the Int64 control, while a variant whose rows actually
//! differ is rejected before any timing.

mod common;

use parquet_lab_bench::RunParams;
use parquet_lab_bench::runner::{Mode, ReaderFlags};
use parquet_lab_contract::SuiteKind;

fn params() -> RunParams {
    RunParams {
        mode: Mode::Local,
        cold_iters: 1,
        warm_iters: 2,
        reader_flags: ReaderFlags::default(),
        run_order: 0,
        skip_manifest: None,
    }
}

#[tokio::test]
async fn an_int32_variant_answers_identically_to_the_int64_control() {
    let tmp = tempfile::tempdir().unwrap();
    // Control: Int64 team_id. Compile the suite against it.
    let control_dir = tmp.path().join("control");
    std::fs::create_dir_all(&control_dir).unwrap();
    let control_mp = common::write_snapshot(&control_dir, false, 0, 200);
    let sql = common::write_sql(tmp.path());
    let suite_out = tmp.path().join("suite.json");
    parquet_lab_bench::compile(&control_mp, SuiteKind::ProdSynth, &sql, &suite_out, false)
        .await
        .unwrap();

    // Variant: the same values, stored physically as Int32. Same rows => same answers.
    let variant_dir = tmp.path().join("variant");
    std::fs::create_dir_all(&variant_dir).unwrap();
    let variant_mp = common::write_snapshot(&variant_dir, true, 0, 200);

    let result = tmp.path().join("result.json");
    // No bail => every query's answer matched the control's expected digest.
    parquet_lab_bench::run(&variant_mp, &suite_out, &result, params(), false)
        .await
        .unwrap();
}

#[tokio::test]
async fn a_variant_whose_rows_differ_is_rejected() {
    let tmp = tempfile::tempdir().unwrap();
    let control_dir = tmp.path().join("control");
    std::fs::create_dir_all(&control_dir).unwrap();
    let control_mp = common::write_snapshot(&control_dir, false, 0, 200);
    let sql = common::write_sql(tmp.path());
    let suite_out = tmp.path().join("suite.json");
    parquet_lab_bench::compile(&control_mp, SuiteKind::ProdSynth, &sql, &suite_out, false)
        .await
        .unwrap();

    // A "variant" with a genuinely different row set: half the rows, so count(*) and the
    // grouped aggregate both change.
    let bad_dir = tmp.path().join("bad");
    std::fs::create_dir_all(&bad_dir).unwrap();
    let bad_mp = common::write_snapshot(&bad_dir, false, 0, 100);

    let result = tmp.path().join("result.json");
    let err = parquet_lab_bench::run(&bad_mp, &suite_out, &result, params(), false)
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("different answer"),
        "a changed answer must reject the variant: {err}"
    );
}

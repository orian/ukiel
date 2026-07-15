//! Probe compilation: typed literals and observed selectivity are frozen against the
//! control, unsupported probes are recorded with a reason (never dropped), and at least one
//! equality probe reaches the runner.

mod common;

use parquet_lab_bench::RunParams;
use parquet_lab_bench::runner::{Mode, ReaderFlags};
use parquet_lab_contract::{ProbeFamily, ProbeSkipReason, ResultSemantics, Suite, SuiteKind};

const PROBES: &str = r#"
[[probe]]
name = "eq_name"
family = "equality"
column = "name"
target_selectivity = 0.1

[[probe]]
name = "isnull_name"
family = "is_null"
column = "name"

[[probe]]
name = "range_ts"
family = "range"
column = "timestamp"
target_selectivity = 0.2

[[probe]]
name = "prefix_name"
family = "prefix"
column = "name"
prefix_len = 3

[[probe]]
name = "wide_proj_team"
family = "wide_projection"
column = "team_id"
target_selectivity = 0.1

[[probe]]
name = "eq_missing"
family = "equality"
column = "does_not_exist"
"#;

#[tokio::test]
async fn compile_suite_materializes_probes_and_records_skips() {
    let tmp = tempfile::tempdir().unwrap();
    let mp = common::write_snapshot(tmp.path(), false, 0, 400);
    let queries = common::write_sql(tmp.path());
    let probes = tmp.path().join("probes.toml");
    std::fs::write(&probes, PROBES).unwrap();
    let suite_out = tmp.path().join("suite.json");

    parquet_lab_bench::compile_suite(
        &mp,
        SuiteKind::ProdSynth,
        &queries,
        &probes,
        &suite_out,
        false,
    )
    .await
    .unwrap();

    let suite = Suite::parse(
        &suite_out.display().to_string(),
        &std::fs::read(&suite_out).unwrap(),
    )
    .unwrap();

    // The missing column is recorded skipped, not dropped.
    assert_eq!(suite.skipped_probes.len(), 1);
    assert_eq!(suite.skipped_probes[0].name, "eq_missing");
    assert_eq!(
        suite.skipped_probes[0].reason,
        ProbeSkipReason::ColumnMissing
    );

    // Every compiled probe has an observed selectivity in [0, 1] and a frozen digest.
    assert!(!suite.probes.is_empty());
    for p in &suite.probes {
        assert!(
            p.observed_selectivity >= 0.0 && p.observed_selectivity <= 1.0,
            "{}",
            p.name
        );
        assert_eq!(p.expected_result_digest.len(), 64);
        assert_eq!(p.control_row_count, 400);
    }

    // At least one equality probe compiled with a typed literal.
    let eq = suite
        .probes
        .iter()
        .find(|p| p.family == ProbeFamily::Equality)
        .expect("an equality probe");
    assert!(
        !eq.literals.is_empty(),
        "equality probe carries a typed literal"
    );
    assert!(
        eq.observed_selectivity > 0.0,
        "equality probe matched some rows"
    );

    // The wide-projection probe is a multiset answer.
    let wide = suite
        .probes
        .iter()
        .find(|p| p.family == ProbeFamily::WideProjection)
        .unwrap();
    assert_eq!(wide.result_semantics, ResultSemantics::Multiset);

    // The compiled probes reach the runner and their answers match the control.
    let result = tmp.path().join("result.json");
    parquet_lab_bench::run(
        &mp,
        &suite_out,
        &result,
        RunParams {
            mode: Mode::Local,
            cold_iters: 1,
            warm_iters: 1,
            reader_flags: ReaderFlags::default(),
            run_order: 0,
            skip_manifest: None,
        },
        false,
    )
    .await
    .unwrap();
    let report = parquet_lab_contract::RunReport::parse(
        &result.display().to_string(),
        &std::fs::read(&result).unwrap(),
    )
    .unwrap();
    let names: Vec<&str> = report.body["queries"]
        .as_array()
        .unwrap()
        .iter()
        .map(|q| q["name"].as_str().unwrap())
        .collect();
    assert!(
        names.contains(&"eq_name"),
        "an equality probe reached the runner: {names:?}"
    );
}

#[tokio::test]
async fn a_probe_set_that_compiles_nothing_is_refused() {
    let tmp = tempfile::tempdir().unwrap();
    let mp = common::write_snapshot(tmp.path(), false, 0, 40);
    let queries = common::write_sql(tmp.path());
    let probes = tmp.path().join("probes.toml");
    std::fs::write(
        &probes,
        "[[probe]]\nname = \"x\"\nfamily = \"equality\"\ncolumn = \"nope\"\n",
    )
    .unwrap();
    let err = parquet_lab_bench::compile_suite(
        &mp,
        SuiteKind::ProdSynth,
        &queries,
        &probes,
        &tmp.path().join("suite.json"),
        false,
    )
    .await
    .unwrap_err();
    assert!(
        err.to_string().contains("no requested probe compiled"),
        "{err}"
    );
}

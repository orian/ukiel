//! The physical-plan guard: it accepts a plan that projects the required columns and folds
//! them into an aggregate, and rejects a metadata fast path, a dropped projection, or a
//! full-result-set plan where an aggregate was declared. A legacy query (no declared class)
//! is never touched.

use parquet_lab_bench::plan_guard::assert_physical_plan;
use parquet_lab_contract::{PredicateShape, QuerySink};

const GOOD_SCAN: &str = "\
AggregateExec: mode=Final, gby=[], aggr=[sum(events.value)]
  DataSourceExec: file_groups=..., projection=[value], predicate=...";

const METADATA_COUNT: &str = "\
ProjectionExec: expr=[100 as count(*)]
  PlaceholderRowCountExec: rows=100";

const NO_AGGREGATE: &str = "\
DataSourceExec: file_groups=..., projection=[value]";

#[test]
fn a_good_full_scan_plan_is_accepted() {
    assert!(
        assert_physical_plan(
            "full_numeric_scan",
            GOOD_SCAN,
            &["value".to_string()],
            QuerySink::Aggregate,
            PredicateShape::FullScan,
        )
        .is_ok()
    );
}

#[test]
fn a_metadata_fast_path_is_rejected_for_a_declared_scan() {
    let err = assert_physical_plan(
        "full_numeric_scan",
        METADATA_COUNT,
        &["value".to_string()],
        QuerySink::Aggregate,
        PredicateShape::FullScan,
    )
    .unwrap_err();
    assert!(err.to_string().contains("does not project required column"), "{err}");
}

#[test]
fn a_dropped_projection_is_rejected() {
    // The plan projects only `value`, but the class requires `name` too.
    let err = assert_physical_plan(
        "hot_set_scan",
        GOOD_SCAN,
        &["value".to_string(), "name".to_string()],
        QuerySink::Aggregate,
        PredicateShape::FullScan,
    )
    .unwrap_err();
    assert!(err.to_string().contains("'name'"), "{err}");
}

#[test]
fn an_aggregate_sink_without_an_aggregate_exec_is_rejected() {
    let err = assert_physical_plan(
        "full_numeric_scan",
        NO_AGGREGATE,
        &["value".to_string()],
        QuerySink::Aggregate,
        PredicateShape::FullScan,
    )
    .unwrap_err();
    assert!(err.to_string().contains("no \n") || err.to_string().contains("AggregateExec"), "{err}");
}

#[test]
fn a_labelled_metadata_control_is_exempt() {
    // A count(*) explicitly labelled as a metadata control is allowed to answer from metadata.
    assert!(
        assert_physical_plan(
            "metadata_count",
            METADATA_COUNT,
            &["value".to_string()],
            QuerySink::Count,
            PredicateShape::Metadata,
        )
        .is_ok()
    );
}

#[test]
fn a_legacy_query_with_no_class_is_never_guarded() {
    // Empty required columns => no class declared => guard is a no-op even on a bare plan.
    assert!(
        assert_physical_plan("legacy_q", "AnythingExec", &[], QuerySink::Aggregate, PredicateShape::FullScan)
            .is_ok()
    );
}

#[test]
fn word_boundaries_prevent_a_false_projection_match() {
    // A plan projecting `value_hash` must not satisfy a requirement for `value`.
    let plan = "AggregateExec: aggr=[x]\n  DataSourceExec: projection=[value_hash]";
    let err = assert_physical_plan(
        "q",
        plan,
        &["value".to_string()],
        QuerySink::Aggregate,
        PredicateShape::FullScan,
    )
    .unwrap_err();
    assert!(err.to_string().contains("does not project"), "{err}");
}

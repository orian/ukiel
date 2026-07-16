//! Physical-plan assertions: prove the optimizer did not remove the work a scenario meant
//! to measure.
//!
//! A benchmark that declares "full string scan" but whose plan answered from metadata, or
//! dropped the string column, or returned the whole result set instead of an aggregate, is
//! not measuring what it claims. The guard runs against the displayed physical plan (with
//! metrics) before any timing is trusted:
//!
//! - every required column must appear in the plan — a dropped projection or a `count(*)`
//!   metadata fast path (which projects no columns) fails this; and
//! - an aggregate/checksum sink must have an `AggregateExec` — a plan that returns the full
//!   result set instead of folding it into an aggregate fails this.
//!
//! The guard only applies to queries that *declare a class* (a non-empty required-column
//! set); legacy plan-47/48 queries carry none and are left untouched.

use anyhow::{Result, bail};
use parquet_lab_contract::{PredicateShape, QuerySink};

/// Does the displayed plan project the named column? DataFusion prints projected columns in
/// scan/projection nodes; a metadata `count(*)` prints an empty projection and no column
/// names, so an absent name is exactly the signal we want.
fn plan_projects(plan: &str, column: &str) -> bool {
    // Match the column as a delimited token so `url` does not spuriously match `url_hash`.
    let bytes = plan.as_bytes();
    let col = column.as_bytes();
    let is_word = |b: u8| b.is_ascii_alphanumeric() || b == b'_';
    let mut i = 0;
    while let Some(pos) = plan[i..].find(column) {
        let start = i + pos;
        let end = start + col.len();
        let before_ok = start == 0 || !is_word(bytes[start - 1]);
        let after_ok = end >= bytes.len() || !is_word(bytes[end]);
        if before_ok && after_ok {
            return true;
        }
        i = start + 1;
        if i >= plan.len() {
            break;
        }
    }
    false
}

/// Assert the plan measured the declared work. A no-op for a query with no declared class.
pub fn assert_physical_plan(
    query_name: &str,
    plan: &str,
    required_columns: &[String],
    sink: QuerySink,
    predicate: PredicateShape,
) -> Result<()> {
    if required_columns.is_empty() {
        return Ok(()); // legacy/undeclared query: no class to enforce
    }
    // A metadata-labelled control (an explicit count(*) fast path) is exempt from the scan
    // requirement — it is *supposed* to answer from metadata, and is labelled as such.
    let metadata_control =
        matches!(predicate, PredicateShape::Metadata) || matches!(sink, QuerySink::Count);
    if metadata_control {
        return Ok(());
    }

    for col in required_columns {
        if !plan_projects(plan, col) {
            bail!(
                "query '{query_name}' declares a {sink:?}/{predicate:?} class but its physical \
                 plan does not project required column '{col}'. The optimizer answered from \
                 metadata or dropped the projection, so the timing would measure the wrong work.\n\
                 Plan:\n{plan}"
            );
        }
    }
    if matches!(sink, QuerySink::Aggregate | QuerySink::Checksum)
        && !plan.contains("AggregateExec")
    {
        bail!(
            "query '{query_name}' declares an {sink:?} sink but its physical plan has no \
             AggregateExec — it returns the full result set instead of folding it into an \
             aggregate, which is a result-materialization benchmark, not a scan.\nPlan:\n{plan}"
        );
    }
    Ok(())
}

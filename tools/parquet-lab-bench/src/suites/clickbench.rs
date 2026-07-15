//! The ClickBench OLAP confirmation suite label. The adapted SQL lives under
//! `bench/queries/clickbench/` and is imported as data by the `compile` command over the
//! bounded 10M-row slice; queries that cannot preserve answer semantics under a
//! physical-type view are excluded with a named reason, never silently rewritten.

/// The suite kind label used in reports.
pub const KIND: &str = "clickbench";

//! The seven registered SQL query classes of Plan 49.
//!
//! Each class declares the columns its plan must project, the sink that forces the work,
//! and the predicate shape it realizes — so the plan guard can reject a plan that answered
//! from metadata, dropped a required projection, or returned the full result set. The SQL is
//! generated from a workload binding onto real columns, so the same seven classes bind to a
//! prod-synth event shape and a ClickBench OLAP shape.

use anyhow::{Result, bail};
use parquet_lab_contract::{PredicateShape, QuerySink};
use serde::{Deserialize, Serialize};

/// A column's role in an aggregate term: numeric columns fold with `sum`, strings with
/// `sum(length(...))` so the decode is forced without returning rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ColKind {
    Numeric,
    String,
}

/// One column bound to a class, with the kind that decides its aggregate term.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClassColumn {
    pub name: String,
    pub kind: ColKind,
}

impl ClassColumn {
    fn agg_term(&self) -> String {
        match self.kind {
            ColKind::Numeric => format!("sum(\"{}\")", self.name),
            ColKind::String => format!("sum(length(\"{}\"))", self.name),
        }
    }
}

/// The workload binding for the seven classes onto real columns.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClassBindings {
    /// A fixed-width key with a prunable/aligned predicate (the sort/packing key).
    pub key_col: String,
    /// A numeric column to fold in a full numeric scan.
    pub numeric_col: String,
    /// A high-cardinality string column to fold in a full string scan.
    pub string_col: String,
    /// A numeric time-like column for the scattered (`MOD(...) = 0`) predicate.
    pub time_col: String,
    /// The hot column set.
    pub hot_columns: Vec<ClassColumn>,
    /// Every column.
    pub all_columns: Vec<ClassColumn>,
    /// A key value that sits in few (prunable) row groups — realizes the aligned class.
    pub aligned_literal: i64,
    /// A key value present in no row group — realizes the absent class.
    pub absent_literal: i64,
}

/// One generated query class, fully specified except its expected digest (computed against
/// the control at compile time).
#[derive(Debug, Clone)]
pub struct ClassQuery {
    pub name: String,
    pub sql: String,
    pub required_columns: Vec<String>,
    pub sink: QuerySink,
    pub predicate_shape: PredicateShape,
}

fn join_terms(cols: &[ClassColumn]) -> String {
    cols.iter().map(|c| c.agg_term()).collect::<Vec<_>>().join(" + ")
}

/// Generate the seven registered query classes for a binding.
pub fn seven_classes(b: &ClassBindings) -> Result<Vec<ClassQuery>> {
    if b.hot_columns.is_empty() || b.all_columns.is_empty() {
        bail!("class bindings must name at least one hot column and all columns");
    }
    let hot_terms = join_terms(&b.hot_columns);
    let all_terms = join_terms(&b.all_columns);
    let hot_cols: Vec<String> = b.hot_columns.iter().map(|c| c.name.clone()).collect();
    let all_cols: Vec<String> = b.all_columns.iter().map(|c| c.name.clone()).collect();

    Ok(vec![
        ClassQuery {
            name: "full_numeric_scan".into(),
            sql: format!("SELECT sum(\"{}\") FROM events", b.numeric_col),
            required_columns: vec![b.numeric_col.clone()],
            sink: QuerySink::Aggregate,
            predicate_shape: PredicateShape::FullScan,
        },
        ClassQuery {
            name: "full_string_scan".into(),
            sql: format!("SELECT sum(length(\"{}\")) FROM events", b.string_col),
            required_columns: vec![b.string_col.clone()],
            sink: QuerySink::Aggregate,
            predicate_shape: PredicateShape::FullScan,
        },
        ClassQuery {
            name: "hot_set_scan".into(),
            sql: format!("SELECT {hot_terms} FROM events"),
            required_columns: hot_cols,
            sink: QuerySink::Aggregate,
            predicate_shape: PredicateShape::FullScan,
        },
        ClassQuery {
            name: "all_column_scan".into(),
            sql: format!("SELECT {all_terms} FROM events"),
            required_columns: all_cols,
            sink: QuerySink::Aggregate,
            predicate_shape: PredicateShape::FullScan,
        },
        ClassQuery {
            name: "aligned_selective_narrow".into(),
            sql: format!(
                "SELECT sum(\"{}\") FROM events WHERE \"{}\" = {}",
                b.numeric_col, b.key_col, b.aligned_literal
            ),
            required_columns: vec![b.key_col.clone(), b.numeric_col.clone()],
            sink: QuerySink::Aggregate,
            predicate_shape: PredicateShape::Aligned,
        },
        ClassQuery {
            name: "scattered_selective".into(),
            sql: format!(
                "SELECT sum(\"{}\") FROM events WHERE (CAST(\"{}\" AS BIGINT) % 100) = 0",
                b.numeric_col, b.time_col
            ),
            required_columns: vec![b.time_col.clone(), b.numeric_col.clone()],
            sink: QuerySink::Aggregate,
            predicate_shape: PredicateShape::Scattered,
        },
        ClassQuery {
            name: "absent_predicate".into(),
            sql: format!(
                "SELECT sum(\"{}\") FROM events WHERE \"{}\" = {}",
                b.numeric_col, b.key_col, b.absent_literal
            ),
            required_columns: vec![b.key_col.clone(), b.numeric_col.clone()],
            sink: QuerySink::Aggregate,
            predicate_shape: PredicateShape::Absent,
        },
    ])
}

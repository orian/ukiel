//! Declarative probes: parameterized equality/range/prefix/substring/is-null/projection
//! queries at measured selectivity bands. Task 47B owns the real compiler (typed literals,
//! observed selectivity, and answer semantics compiled against the immutable control); this
//! module holds the structured-probe constructor those parts assemble into.

use parquet_lab_contract::{Probe, ProbeFamily, ResultSemantics, TypedLiteral};

/// Assemble a compiled probe from its resolved parts.
#[allow(clippy::too_many_arguments)]
pub fn probe(
    name: &str,
    family: ProbeFamily,
    column: &str,
    literals: Vec<TypedLiteral>,
    sql: &str,
    expected_result_digest: String,
    result_semantics: ResultSemantics,
    control_row_count: u64,
    match_count: u64,
) -> Probe {
    let observed_selectivity = if control_row_count == 0 {
        0.0
    } else {
        match_count as f64 / control_row_count as f64
    };
    Probe {
        name: name.to_string(),
        family,
        column: column.to_string(),
        literals,
        sql: sql.to_string(),
        expected_result_digest,
        result_semantics,
        control_row_count,
        match_count,
        observed_selectivity,
    }
}

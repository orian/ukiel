//! Declarative probes: parameterized equality/range/prefix/substring/is-null/projection
//! queries at measured selectivity bands. A probe is compiled only when the column
//! actually holds enough values to realize its declared band; the compiler records the
//! observed count before timing. Probes run through the same path as named queries.

use parquet_lab_contract::Probe;

/// Build a probe from its parts. `observed_count` and `expected_result_digest` are filled
/// by the compiler after running the probe against the control.
#[allow(clippy::too_many_arguments)]
pub fn probe(
    name: &str,
    family: &str,
    column: &str,
    sql: &str,
    target_selectivity: f64,
    observed_count: Option<u64>,
    expected_result_digest: Option<String>,
) -> Probe {
    Probe {
        name: name.to_string(),
        family: family.to_string(),
        column: column.to_string(),
        sql: sql.to_string(),
        target_selectivity,
        observed_count,
        expected_result_digest,
    }
}

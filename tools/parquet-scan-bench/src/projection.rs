//! Resolve a generic projection role onto concrete columns via a workload binding, and
//! compile those into a Parquet leaf-column projection mask.

use anyhow::{Result, bail};
use parquet::arrow::ProjectionMask;
use parquet::schema::types::SchemaDescriptor;
use parquet_lab_contract::{ProjectionRole, WorkloadBinding};

/// The columns a role resolves to, given a workload binding and the full column list.
pub fn columns_for_role(
    role: ProjectionRole,
    workload: &WorkloadBinding,
    all_columns: &[String],
) -> Result<Vec<String>> {
    let single = |key: &str| -> Result<Vec<String>> {
        let col = workload
            .roles
            .get(key)
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("workload binding has no column for role '{key}'"))?;
        Ok(vec![col.to_string()])
    };
    let cols = match role {
        ProjectionRole::FixedWidthKey => single("fixed_width_key")?,
        ProjectionRole::HighCardinalityString => single("high_cardinality_string")?,
        ProjectionRole::WideText => single("wide_text")?,
        ProjectionRole::HotSet => {
            if workload.hot_columns.is_empty() {
                bail!("workload binding has an empty hot_columns set");
            }
            workload.hot_columns.clone()
        }
        ProjectionRole::AllColumns => all_columns.to_vec(),
    };
    // Every resolved column must exist in the schema.
    for c in &cols {
        if !all_columns.iter().any(|a| a == c) {
            bail!("role column '{c}' is not present in the artifact schema");
        }
    }
    Ok(cols)
}

/// Parse a projection-role name.
pub fn parse_role(s: &str) -> Result<ProjectionRole> {
    Ok(match s {
        "fixed_width_key" => ProjectionRole::FixedWidthKey,
        "high_cardinality_string" => ProjectionRole::HighCardinalityString,
        "wide_text" => ProjectionRole::WideText,
        "hot_set" => ProjectionRole::HotSet,
        "all_columns" => ProjectionRole::AllColumns,
        other => bail!("unknown projection role '{other}'"),
    })
}

/// Build a leaf-column projection mask for the named columns. Assumes a flat schema (no
/// nesting), which the laboratory datasets always have: leaf index == top-level index.
pub fn mask_for(
    schema: &SchemaDescriptor,
    all_columns: &[String],
    selected: &[String],
) -> Result<ProjectionMask> {
    let mut indices = Vec::with_capacity(selected.len());
    for name in selected {
        let idx = all_columns
            .iter()
            .position(|c| c == name)
            .ok_or_else(|| anyhow::anyhow!("column '{name}' not in schema"))?;
        indices.push(idx);
    }
    Ok(ProjectionMask::roots(schema, indices))
}

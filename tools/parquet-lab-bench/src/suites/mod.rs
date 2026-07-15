//! Suite compilation: build a versioned query suite from a snapshot control.
//!
//! The suite bakes the logical-projection view (so every variant presents the identical
//! logical schema) and, for each query, the expected result digest computed against the
//! control. Queries are imported *as data* — from a `.sql` file — never by calling another
//! executable's handler.

pub mod clickbench;
pub mod probes;
pub mod prod_synth;

use anyhow::{Result, bail};
use parquet_lab_contract::{Query, SnapshotManifest};
use parquet_lab_integrity::LogicalType;

/// Build the `CREATE OR REPLACE VIEW events` body that normalizes each physical column to
/// its declared logical type, so answers are comparable across variants by construction.
pub fn build_view_sql(snapshot: &SnapshotManifest) -> Result<String> {
    let proj = snapshot
        .logical_projection
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("snapshot has no logical projection"))?;
    let fields = snapshot
        .physical_schema
        .get("fields")
        .and_then(|f| f.as_array())
        .ok_or_else(|| anyhow::anyhow!("snapshot physical schema has no fields"))?;
    let mut selects = Vec::with_capacity(fields.len());
    for f in fields {
        let name = f
            .get("name")
            .and_then(|n| n.as_str())
            .ok_or_else(|| anyhow::anyhow!("physical field missing name"))?;
        let type_name = proj
            .logical_types
            .get(name)
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("projection missing column '{name}'"))?;
        let sql_type = match LogicalType::parse(type_name)
            .map_err(|e| anyhow::anyhow!("column '{name}': {e}"))?
        {
            // Milliseconds and dates present as BIGINT to match the original event SQL,
            // which filters on integer epoch/day values. Every physical representation
            // (Int64, Int32, Timestamp, Date32) casts to the same BIGINT here.
            LogicalType::SignedInt | LogicalType::TimestampMillis | LogicalType::DateDays => {
                "BIGINT"
            }
            LogicalType::Float64 => "DOUBLE",
            LogicalType::Utf8 => "VARCHAR",
            LogicalType::Boolean => "BOOLEAN",
        };
        selects.push(format!("CAST(\"{name}\" AS {sql_type}) AS \"{name}\""));
    }
    Ok(format!(
        "CREATE OR REPLACE VIEW events AS SELECT {} FROM events_physical",
        selects.join(", ")
    ))
}

/// Split a `.sql` file into named statements. A leading `-- name:` comment names the
/// following statement; otherwise statements are named `q1`, `q2`, …
pub fn parse_sql_queries(text: &str) -> Result<Vec<(String, String)>> {
    let mut out = Vec::new();
    let mut pending_name: Option<String> = None;
    let mut buf = String::new();
    let mut auto = 0usize;
    for line in text.lines() {
        let trimmed = line.trim();
        if let Some(rest) = trimmed.strip_prefix("--") {
            // A `-- name: ...` marker names the next statement.
            if let Some((name, _)) = rest.trim().split_once(':') {
                let name = name.trim();
                if !name.is_empty() && !name.contains(' ') {
                    pending_name = Some(name.to_string());
                }
            }
            continue;
        }
        buf.push_str(line);
        buf.push('\n');
        if trimmed.ends_with(';') {
            let sql = buf.trim().trim_end_matches(';').trim().to_string();
            if !sql.is_empty() {
                auto += 1;
                let name = pending_name.take().unwrap_or_else(|| format!("q{auto}"));
                out.push((name, sql));
            }
            buf.clear();
        }
    }
    let tail = buf.trim().trim_end_matches(';').trim();
    if !tail.is_empty() {
        auto += 1;
        let name = pending_name.take().unwrap_or_else(|| format!("q{auto}"));
        out.push((name, tail.to_string()));
    }
    if out.is_empty() {
        bail!("no SQL statements found");
    }
    Ok(out)
}

/// A compiled query with its expected digest.
pub fn to_query(name: String, sql: String, expected_result_digest: String) -> Query {
    Query {
        name,
        sql,
        expected_result_digest,
    }
}

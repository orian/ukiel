//! Compile declarative probes against the immutable control.
//!
//! A probe is *requested* by family + column + target band; the compiler materializes a
//! typed literal that realizes a usable band on the control, renders the predicate SQL,
//! and freezes the exact answer digest, control row count, match count, and *observed*
//! selectivity. A requested probe that cannot be compiled is recorded with one stable
//! reason (`column_missing`, `no_literal_in_band`, `unsupported_type`, `empty_control`) —
//! never silently dropped.

use anyhow::Result;
use arrow::array::{
    Array, BooleanArray, Float64Array, Int32Array, Int64Array, StringArray, StringViewArray,
};
use arrow::datatypes::DataType;
use parquet_lab_contract::{
    Probe, ProbeFamily, ProbeSkipReason, ResultSemantics, SkippedProbe, TypedLiteral,
};
use serde::Deserialize;

use crate::compare;
use crate::runner::{Session, run_query};

/// A requested probe (TOML input), before compilation.
#[derive(Debug, Clone, Deserialize)]
pub struct ProbeRequest {
    pub name: String,
    /// `equality`, `range`, `prefix`, `substring`, `is_null`, `narrow_projection`,
    /// `wide_projection`.
    pub family: String,
    pub column: String,
    /// The selectivity band to aim a literal at (best-effort; the *observed* selectivity is
    /// what is recorded). Ignored by `is_null`.
    #[serde(default)]
    pub target_selectivity: Option<f64>,
    /// Prefix/substring length. Defaults to 3.
    #[serde(default)]
    pub prefix_len: Option<usize>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ProbeRequestFile {
    #[serde(default)]
    pub probe: Vec<ProbeRequest>,
}

pub fn parse_requests(bytes: &[u8], path: &str) -> Result<Vec<ProbeRequest>> {
    let f: ProbeRequestFile =
        toml::from_str(std::str::from_utf8(bytes)?).map_err(|e| anyhow::anyhow!("{path}: {e}"))?;
    Ok(f.probe)
}

fn family(s: &str) -> Result<ProbeFamily> {
    Ok(match s {
        "equality" => ProbeFamily::Equality,
        "range" => ProbeFamily::Range,
        "prefix" => ProbeFamily::Prefix,
        "substring" => ProbeFamily::Substring,
        "is_null" => ProbeFamily::IsNull,
        "narrow_projection" => ProbeFamily::NarrowProjection,
        "wide_projection" => ProbeFamily::WideProjection,
        other => anyhow::bail!("unknown probe family '{other}'"),
    })
}

/// The outcome of compiling one requested probe.
pub enum Compiled {
    Probe(Box<Probe>),
    Skipped(SkippedProbe),
}

/// Compile one probe against the control session. `control_rows` is the control's row count.
pub async fn compile_probe(
    session: &Session,
    req: &ProbeRequest,
    control_rows: u64,
) -> Result<Compiled> {
    let fam = family(&req.family)?;
    let skip = |reason| {
        Compiled::Skipped(SkippedProbe {
            name: req.name.clone(),
            family: fam,
            column: req.column.clone(),
            reason,
        })
    };

    if control_rows == 0 {
        return Ok(skip(ProbeSkipReason::EmptyControl));
    }
    // The column must exist in the logical view.
    let Some(col_type) = column_type(session, &req.column).await? else {
        return Ok(skip(ProbeSkipReason::ColumnMissing));
    };
    let q = quote_ident(&req.column);

    match fam {
        ProbeFamily::IsNull => {
            let matches = scalar_u64(
                session,
                &format!("SELECT count(*) FROM events WHERE {q} IS NULL"),
            )
            .await?;
            let sql = format!("SELECT count(*) AS matches FROM events WHERE {q} IS NULL");
            finish(
                session,
                req,
                fam,
                vec![],
                sql,
                ResultSemantics::Ordered,
                control_rows,
                matches,
            )
            .await
        }
        ProbeFamily::Equality => {
            // Pick the value whose frequency is closest to the target band (or the most
            // frequent when no target is given), among non-null values.
            let Some((lit, lit_sql, n)) =
                pick_value(session, &q, col_type, req.target_selectivity).await?
            else {
                return Ok(skip(ProbeSkipReason::NoLiteralInBand));
            };
            let sql = format!("SELECT count(*) AS matches FROM events WHERE {q} = {lit_sql}");
            finish(
                session,
                req,
                fam,
                vec![lit],
                sql,
                ResultSemantics::Ordered,
                control_rows,
                n,
            )
            .await
        }
        ProbeFamily::Range | ProbeFamily::NarrowProjection | ProbeFamily::WideProjection => {
            // A `col <= hi` band, with hi at the target quantile.
            let sel = req.target_selectivity.unwrap_or(0.1);
            let Some((hi_lit, hi_sql)) = quantile_value(session, &q, col_type, sel).await? else {
                return Ok(skip(ProbeSkipReason::NoLiteralInBand));
            };
            let matches = scalar_u64(
                session,
                &format!("SELECT count(*) FROM events WHERE {q} <= {hi_sql}"),
            )
            .await?;
            let (sql, semantics) = match fam {
                ProbeFamily::Range => (
                    format!("SELECT count(*) AS matches FROM events WHERE {q} <= {hi_sql}"),
                    ResultSemantics::Ordered,
                ),
                ProbeFamily::NarrowProjection => (
                    format!("SELECT {q} FROM events WHERE {q} <= {hi_sql}"),
                    ResultSemantics::Multiset,
                ),
                _ => (
                    format!("SELECT * FROM events WHERE {q} <= {hi_sql}"),
                    ResultSemantics::Multiset,
                ),
            };
            finish(
                session,
                req,
                fam,
                vec![hi_lit],
                sql,
                semantics,
                control_rows,
                matches,
            )
            .await
        }
        ProbeFamily::Prefix | ProbeFamily::Substring => {
            if !is_string(col_type) {
                return Ok(skip(ProbeSkipReason::UnsupportedType));
            }
            let l = req.prefix_len.unwrap_or(3);
            let Some((prefix, n)) = pick_prefix(session, &q, l).await? else {
                return Ok(skip(ProbeSkipReason::NoLiteralInBand));
            };
            let esc = escape_str(&prefix);
            let (pat, matches) = if fam == ProbeFamily::Prefix {
                (format!("'{esc}%'"), n)
            } else {
                // substring: measure the actual match of '%p%'.
                let m = scalar_u64(
                    session,
                    &format!("SELECT count(*) FROM events WHERE {q} LIKE '%{esc}%'"),
                )
                .await?;
                (format!("'%{esc}%'"), m)
            };
            let sql = format!("SELECT count(*) AS matches FROM events WHERE {q} LIKE {pat}");
            finish(
                session,
                req,
                fam,
                vec![TypedLiteral::Str(prefix)],
                sql,
                ResultSemantics::Ordered,
                control_rows,
                matches,
            )
            .await
        }
    }
}

/// Run the compiled probe once against the control to freeze its exact result digest.
#[allow(clippy::too_many_arguments)]
async fn finish(
    session: &Session,
    req: &ProbeRequest,
    fam: ProbeFamily,
    literals: Vec<TypedLiteral>,
    sql: String,
    semantics: ResultSemantics,
    control_rows: u64,
    match_count: u64,
) -> Result<Compiled> {
    let run = run_query(session, &sql).await?;
    let digest = compare::result_digest(&run.schema, &run.batches, semantics)?;
    let observed = match_count as f64 / control_rows as f64;
    Ok(Compiled::Probe(Box::new(Probe {
        name: req.name.clone(),
        family: fam,
        column: req.column.clone(),
        literals,
        sql,
        expected_result_digest: digest,
        result_semantics: semantics,
        control_row_count: control_rows,
        match_count,
        observed_selectivity: observed,
    })))
}

// -- control introspection helpers ------------------------------------------

async fn column_type(session: &Session, column: &str) -> Result<Option<DataType>> {
    let df = session.ctx.sql("SELECT * FROM events LIMIT 0").await?;
    let schema = df.schema();
    Ok(schema
        .fields()
        .iter()
        .find(|f| f.name() == column)
        .map(|f| f.data_type().clone()))
}

fn is_string(dt: DataType) -> bool {
    matches!(
        dt,
        DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8
    )
}

async fn scalar_u64(session: &Session, sql: &str) -> Result<u64> {
    let run = run_query(session, sql).await?;
    for b in &run.batches {
        if b.num_rows() > 0 {
            let c = b.column(0);
            if let Some(a) = c.as_any().downcast_ref::<Int64Array>() {
                return Ok(a.value(0).max(0) as u64);
            }
        }
    }
    Ok(0)
}

/// Pick the value nearest the target selectivity (or the most frequent), returning the typed
/// literal, its SQL rendering, and its match count.
async fn pick_value(
    session: &Session,
    q: &str,
    _col_type: DataType,
    target: Option<f64>,
) -> Result<Option<(TypedLiteral, String, u64)>> {
    let sql = format!(
        "SELECT {q} AS v, count(*) AS n FROM events WHERE {q} IS NOT NULL GROUP BY v ORDER BY n DESC"
    );
    let run = run_query(session, &sql).await?;
    let total: u64 = run
        .batches
        .iter()
        .flat_map(|b| {
            let n = b.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
            (0..b.num_rows()).map(move |i| n.value(i).max(0) as u64)
        })
        .sum();
    if total == 0 {
        return Ok(None);
    }
    let mut best: Option<(f64, TypedLiteral, String, u64)> = None;
    for b in &run.batches {
        let ncol = b.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        for i in 0..b.num_rows() {
            let n = ncol.value(i).max(0) as u64;
            let Some((lit, sql)) = literal_at(b.column(0).as_ref(), i) else {
                continue;
            };
            let sel = n as f64 / total as f64;
            let score = match target {
                Some(t) => (sel - t).abs(),
                None => -(n as f64), // most frequent
            };
            if best.as_ref().is_none_or(|(s, ..)| score < *s) {
                best = Some((score, lit, sql, n));
            }
        }
    }
    Ok(best.map(|(_, lit, sql, n)| (lit, sql, n)))
}

/// The value at the `sel` quantile (ascending), as a `col <= hi` upper bound.
async fn quantile_value(
    session: &Session,
    q: &str,
    _col_type: DataType,
    sel: f64,
) -> Result<Option<(TypedLiteral, String)>> {
    let n = scalar_u64(
        session,
        &format!("SELECT count(*) FROM events WHERE {q} IS NOT NULL"),
    )
    .await?;
    if n == 0 {
        return Ok(None);
    }
    let offset = ((sel.clamp(0.0, 1.0)) * (n.saturating_sub(1)) as f64) as u64;
    let sql = format!(
        "SELECT {q} AS v FROM events WHERE {q} IS NOT NULL ORDER BY v LIMIT 1 OFFSET {offset}"
    );
    let run = run_query(session, &sql).await?;
    for b in &run.batches {
        if b.num_rows() > 0
            && let Some((lit, sql)) = literal_at(b.column(0).as_ref(), 0)
        {
            return Ok(Some((lit, sql)));
        }
    }
    Ok(None)
}

/// Pick the most frequent `l`-byte prefix of a string column.
async fn pick_prefix(session: &Session, q: &str, l: usize) -> Result<Option<(String, u64)>> {
    let sql = format!(
        "SELECT substr({q}, 1, {l}) AS p, count(*) AS n FROM events WHERE {q} IS NOT NULL AND length({q}) >= {l} GROUP BY p ORDER BY n DESC LIMIT 1"
    );
    let run = run_query(session, &sql).await?;
    for b in &run.batches {
        if b.num_rows() > 0 {
            let p = b.column(0);
            let n = b
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0)
                .max(0) as u64;
            if let Some(s) = string_at(p.as_ref(), 0) {
                return Ok(Some((s, n)));
            }
        }
    }
    Ok(None)
}

/// Extract a typed literal and its SQL rendering from an Arrow array cell.
fn literal_at(col: &dyn Array, row: usize) -> Option<(TypedLiteral, String)> {
    if col.is_null(row) {
        return None;
    }
    match col.data_type() {
        DataType::Int64 => {
            let v = col.as_any().downcast_ref::<Int64Array>()?.value(row);
            Some((TypedLiteral::Int(v), v.to_string()))
        }
        DataType::Int32 => {
            let v = col.as_any().downcast_ref::<Int32Array>()?.value(row) as i64;
            Some((TypedLiteral::Int(v), v.to_string()))
        }
        DataType::Float64 => {
            let v = col.as_any().downcast_ref::<Float64Array>()?.value(row);
            Some((TypedLiteral::Float(v), format!("CAST({v} AS DOUBLE)")))
        }
        DataType::Boolean => {
            let v = col.as_any().downcast_ref::<BooleanArray>()?.value(row);
            Some((TypedLiteral::Bool(v), v.to_string()))
        }
        DataType::Utf8 | DataType::Utf8View => {
            let s = string_at(col, row)?;
            let sql = format!("'{}'", escape_str(&s));
            Some((TypedLiteral::Str(s), sql))
        }
        _ => None,
    }
}

fn string_at(col: &dyn Array, row: usize) -> Option<String> {
    match col.data_type() {
        DataType::Utf8 => Some(
            col.as_any()
                .downcast_ref::<StringArray>()?
                .value(row)
                .to_string(),
        ),
        DataType::Utf8View => Some(
            col.as_any()
                .downcast_ref::<StringViewArray>()?
                .value(row)
                .to_string(),
        ),
        _ => None,
    }
}

fn escape_str(s: &str) -> String {
    s.replace('\'', "''")
}

fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

//! `ukiel-parquet-suite/v1` — a versioned, declarative query suite.
//!
//! A suite is a list of named queries and parameterized probes with their expected
//! selectivity and result digests. It imports the six plan-45 prod-synth query
//! classes and the existing adapted ClickBench SQL *as data* — the bench tool reads
//! this file, it never calls another executable's query handler. A probe is included
//! only when the column actually holds enough values to realize its declared
//! selectivity; the compiler records the observed count before any timing.

use serde::{Deserialize, Serialize};

use crate::{ContractError, check_version};

/// The suite format. A reader that does not recognise it fails closed.
pub const SUITE_VERSION: &str = "ukiel-parquet-suite/v1";

/// The structured-probe contract version. Probes declare predicates independently of their
/// SQL rendering so the runner and a sidecar reason about the same typed predicate.
pub const PROBES_VERSION: &str = "ukiel-parquet-probes/v1";

/// How a query/probe result is compared. A scalar aggregate is `Ordered`; a multi-row
/// answer without a deterministic total order is a `Multiset` (compared independent of batch
/// and row order, preserving duplicate counts).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResultSemantics {
    #[default]
    Ordered,
    Multiset,
}

/// A probe's predicate family — declared, not inferred from SQL text.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProbeFamily {
    Equality,
    Range,
    Prefix,
    Substring,
    IsNull,
    NarrowProjection,
    WideProjection,
}

/// A typed literal in a probe predicate. Kept typed (not a rendered SQL string) so a sidecar
/// can evaluate the predicate without parsing SQL.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type", content = "value")]
pub enum TypedLiteral {
    Int(i64),
    Float(f64),
    Str(String),
    Bool(bool),
    Null,
}

/// Which workload a suite belongs to.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SuiteKind {
    /// The six plan-45 prod-synth event query classes plus declarative probes.
    ProdSynth,
    /// The adapted ClickBench SQL over the bounded 10M-row slice.
    ClickBench,
}

/// One named query. The SQL runs against the declared logical projection, so a
/// physical-type variant answers the identical query without a rewrite.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Query {
    pub name: String,
    pub sql: String,
    /// The expected result digest (a canonical fingerprint of the normalized answer),
    /// checked before timing. A changed answer rejects the variant, never a rewrite.
    pub expected_result_digest: crate::Digest,
    /// How the answer is compared. Defaults to `Ordered` (a scalar aggregate).
    #[serde(default)]
    pub result_semantics: ResultSemantics,
}

/// A compiled probe: a structured predicate at a measured selectivity band, with its
/// materialized typed literals, exact control answer, and observed counts frozen in.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Probe {
    pub name: String,
    pub family: ProbeFamily,
    pub column: String,
    /// The typed literals the predicate binds (e.g. the equality value, the range bounds).
    pub literals: Vec<TypedLiteral>,
    pub sql: String,
    /// The exact result digest computed against the immutable control.
    pub expected_result_digest: crate::Digest,
    pub result_semantics: ResultSemantics,
    /// The control's total row count, and how many rows this probe matched — the observed
    /// selectivity is `match_count / control_row_count`.
    pub control_row_count: u64,
    pub match_count: u64,
    pub observed_selectivity: f64,
}

/// The compiled suite.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Suite {
    pub suite_version: String,
    pub kind: SuiteKind,
    /// The `CREATE VIEW events AS ...` body that casts each physical column back to the
    /// snapshot's declared logical type. Baked from the snapshot at compile time so every
    /// variant — whatever physical types it stores — presents the identical logical
    /// schema, and query answers are comparable across variants by construction.
    #[serde(default)]
    pub view_sql: String,
    pub queries: Vec<Query>,
    pub probes: Vec<Probe>,
}

impl Suite {
    pub fn parse(path: &str, bytes: &[u8]) -> Result<Self, ContractError> {
        let value: serde_json::Value =
            serde_json::from_slice(bytes).map_err(|source| ContractError::Parse {
                path: path.to_string(),
                source,
            })?;
        check_version(path, &value, SUITE_VERSION, "suite_version")?;
        let suite: Suite =
            serde_json::from_value(value).map_err(|source| ContractError::Parse {
                path: path.to_string(),
                source,
            })?;
        suite.validate(path)?;
        Ok(suite)
    }

    /// Query and probe names must be unique so a report can key results by name.
    pub fn validate(&self, path: &str) -> Result<(), ContractError> {
        let mut seen = std::collections::HashSet::new();
        for name in self
            .queries
            .iter()
            .map(|q| q.name.as_str())
            .chain(self.probes.iter().map(|p| p.name.as_str()))
        {
            if !seen.insert(name) {
                return Err(ContractError::DuplicateLabel {
                    context: path.to_string(),
                    duplicate: name.to_string(),
                });
            }
        }
        Ok(())
    }
}

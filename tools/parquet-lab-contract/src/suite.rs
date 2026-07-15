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
}

/// A parameterized probe at a measured selectivity band.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Probe {
    pub name: String,
    /// The probe family: `equality`, `range`, `prefix`, `substring`, `is_null`,
    /// `narrow_projection`, `wide_projection`.
    pub family: String,
    pub column: String,
    pub sql: String,
    /// The selectivity band this probe targets, e.g. `0.001`.
    pub target_selectivity: f64,
    /// The count the compiler actually observed for the column. A probe is included
    /// only if the column can realize its band; this records what was seen.
    pub observed_count: Option<u64>,
    pub expected_result_digest: Option<crate::Digest>,
}

/// The compiled suite.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Suite {
    pub suite_version: String,
    pub kind: SuiteKind,
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

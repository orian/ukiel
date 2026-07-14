//! The versioned artifact contract shared by `prod-synth`, `ukiel-prod-load`,
//! and `ukiel-prod-bench` (plan 45).
//!
//! The three tools are separate executables joined *only* by the files described
//! here. They never call each other's command handlers, and no executable package
//! depends on another executable package. This library is what makes that
//! possible, so it must stay what it is: serde types and version checks. It
//! parses no profile, compiles no topology, writes no Parquet, opens no socket.
//!
//! ## The identity rule
//!
//! Everything the pipeline emits is **synthetic and profile-derived**. It
//! reproduces the *geometry* of an observed workload — tenant activity skew,
//! part-to-tenant membership, key sparsity — and nothing else. It carries no
//! production rows, no real tenant identifiers, and no real column values. Every
//! manifest states that in [`SYNTHETIC_DISCLAIMER`], and the readers here refuse
//! a manifest that does not.

use serde::{Deserialize, Serialize};

pub mod manifest;

pub use manifest::{
    FidelityCheck, GeneratedSummary, GenerationConfig, Generator, Manifest, ManifestPart,
    Membership, PartProvenance, Quantiles, Representatives, SourceProfile, SourceSummary,
    TableSpec, Tenant, Tier, Topology, TopologyShard, ValueModel, WeightedValue,
};

/// The manifest/topology format both files declare and every reader checks.
///
/// A reader that does not recognise the exact string fails closed. There is no
/// "best effort" parse of an unknown version: a benchmark that silently reads a
/// format it half-understands reports a number nobody can trust.
pub const MANIFEST_VERSION: &str = "ukiel-prod-synth/v1";

/// Stamped into every artifact and re-asserted by every reader.
///
/// This is load-bearing, not decoration. The whole point of plan 45 is that a
/// report from this fixture can never be mistaken for a measurement of a real
/// production system, and a file that does not say so out loud is a file someone
/// will eventually misread.
pub const SYNTHETIC_DISCLAIMER: &str = "Synthetic, profile-derived fixture. Reproduces the geometry of an observed workload (tenant skew, part membership, key sparsity) from anonymized aggregates only. Contains no production rows, no real tenant identifiers, and no real column values. Not a production copy.";

/// The 14-day window every generated timestamp lands in: 2026-07-01T00:00:00Z.
///
/// A constant, never the wall clock — a fixture whose contents depend on when it
/// was generated is not reproducible, and `--seed` would be a lie.
pub const FIXTURE_WINDOW_START_MS: i64 = 1_782_864_000_000;
/// The source observation window, so a part's time span has somewhere to live.
pub const FIXTURE_WINDOW_DAYS: i64 = 14;
/// One past the last millisecond a generated row may carry.
pub const FIXTURE_WINDOW_END_MS: i64 =
    FIXTURE_WINDOW_START_MS + FIXTURE_WINDOW_DAYS * 24 * 60 * 60 * 1000;

#[derive(Debug, thiserror::Error)]
pub enum ContractError {
    #[error("{path}: not a {MANIFEST_VERSION} artifact (found version '{found}')")]
    Version { path: String, found: String },
    #[error("{path}: {source}")]
    Parse {
        path: String,
        #[source]
        source: serde_json::Error,
    },
    #[error("{path}: {0}", source)]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error(
        "{path}: digest mismatch — expected {expected}, got {actual}. The artifact has been \
         modified or truncated since it was generated; regenerate it rather than loading it."
    )]
    Digest {
        path: String,
        expected: String,
        actual: String,
    },
    #[error(
        "{path}: the artifact does not carry the synthetic-fixture disclaimer, so it cannot be \
         proven to be one. Refusing to treat it as generated data."
    )]
    MissingDisclaimer { path: String },
}

/// A blake3 digest, lowercase hex. The one hash the pipeline speaks.
pub type Digest = String;

pub use manifest::digest_bytes;

/// Sizes/digests are recorded once and re-checked at every boundary, so a report
/// can always name the exact bytes it measured.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileDigest {
    /// Path relative to the manifest's own directory. Never absolute: the
    /// artifact must survive being copied to a benchmark host.
    pub path: String,
    pub bytes: u64,
    pub digest: Digest,
}

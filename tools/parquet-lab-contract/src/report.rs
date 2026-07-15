//! `ukiel-parquet-report/v1` — the immutable raw report every laboratory command
//! writes, plus the identity block that pins what was measured.
//!
//! Raw JSON is immutable and self-describing: a report names the exact snapshot,
//! variant, suite, and skip-index digests it measured, the tool versions and host
//! it ran on, and the run order. Markdown interpretation is *derived* from these;
//! the laboratory never retains only charts or aggregate prose. The report body is
//! deliberately an open `serde_json::Value` — census, rewrite, and bench each carry
//! different payloads, but all share the identity envelope so no number can float
//! free of what produced it.

use serde::{Deserialize, Serialize};

/// The report envelope version. Every command's raw JSON declares it.
pub const REPORT_VERSION: &str = "ukiel-parquet-report/v1";

/// The pinned toolchain versions, recorded in every artifact so a result can never
/// be silently attributed to a different Arrow/Parquet/DataFusion build.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolVersions {
    pub git_sha: String,
    pub arrow: String,
    pub parquet: String,
    pub datafusion: String,
}

/// The host a measurement ran on. Missing metrics are `null`, never zero — a host
/// observer that cannot read RSS reports absence, not a fabricated `0`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HostInfo {
    pub target_cpu: Option<String>,
    pub host_cpu: Option<String>,
    pub host_ram_bytes: Option<u64>,
    pub kernel: Option<String>,
    /// `local` or an object-store kind. Never inferred; recorded from the run mode.
    pub storage_kind: String,
    /// Whether the host page cache was actually dropped, and how. A claim of "cold"
    /// is only credited when the mechanism is recorded here.
    pub page_cache_dropped: Option<String>,
}

/// The identity block that binds a report to exactly what it measured. Every
/// atomic report carries one; a report missing any binding it needs is invalid.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReportIdentity {
    pub report_version: String,
    pub tool_versions: ToolVersions,
    /// The snapshot this report descends from, by digest. Always present.
    pub snapshot_digest: crate::Digest,
    /// The variant measured, by digest. `None` for a report over the raw snapshot
    /// (e.g. the product-control census).
    pub variant_digest: Option<crate::Digest>,
    /// The suite executed, by digest. `None` for a census (which runs no queries).
    pub suite_digest: Option<crate::Digest>,
    /// The skip-index sidecar consulted, by digest. `None` when none was used.
    pub skip_digest: Option<crate::Digest>,
    /// Position in the interleaved run order, so machine drift stays visible.
    pub run_order: u32,
    /// The repetition this report belongs to, when run under a run set. `None` for an
    /// ad-hoc single run.
    #[serde(default)]
    pub repetition: Option<u32>,
    /// The run set's seed, so the interleaved order is reproducible.
    #[serde(default)]
    pub seed: Option<u64>,
    /// A digest of the scheduled order this report was produced under.
    #[serde(default)]
    pub order_digest: Option<crate::Digest>,
    /// The backend identity (`memory`, `local`, or an object-store endpoint identity). A
    /// timing is meaningless without knowing what it read from.
    #[serde(default)]
    pub backend: Option<String>,
}

/// A raw report: identity envelope plus a command-specific body.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunReport {
    pub identity: ReportIdentity,
    pub host: HostInfo,
    /// The command-specific payload (census aggregates, rewrite cost, query timings
    /// and I/O accounting). Kept open so one envelope serves every command.
    pub body: serde_json::Value,
}

impl RunReport {
    pub fn parse(path: &str, bytes: &[u8]) -> Result<Self, crate::ContractError> {
        let value: serde_json::Value =
            serde_json::from_slice(bytes).map_err(|source| crate::ContractError::Parse {
                path: path.to_string(),
                source,
            })?;
        let version = value
            .get("identity")
            .and_then(|i| i.get("report_version"))
            .and_then(|v| v.as_str())
            .unwrap_or("<absent>");
        if version != REPORT_VERSION {
            return Err(crate::ContractError::Version {
                path: path.to_string(),
                expected: REPORT_VERSION.to_string(),
                found: version.to_string(),
            });
        }
        serde_json::from_value(value).map_err(|source| crate::ContractError::Parse {
            path: path.to_string(),
            source,
        })
    }
}

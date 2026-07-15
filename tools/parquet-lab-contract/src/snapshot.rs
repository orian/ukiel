//! `ukiel-parquet-snapshot/v1` — a verified, immutable freeze of an explicit set of
//! Parquet files.
//!
//! A snapshot is the laboratory's product control. It copies the exact bytes a
//! source produced (a plan-46 compaction receipt's converged objects, or an
//! explicit file list) and records everything a downstream tool needs to prove it
//! is reading those bytes and nothing else: per-file digests, a physical
//! `row-multiset/v1` where the Arrow schema is unchanged, and a logical
//! [`crate::LOGICAL_ROW_MULTISET_VERSION`] fingerprint that survives a lossless
//! physical-type rewrite.

use serde::{Deserialize, Serialize};

use crate::{
    ContractError, FileDigest, Fingerprint, LOGICAL_ROW_MULTISET_VERSION, check_relative_path,
    check_version,
};

/// The snapshot manifest format. A reader that does not recognise it fails closed.
pub const SNAPSHOT_MANIFEST_VERSION: &str = "ukiel-parquet-snapshot/v1";

/// Where a snapshot's rows came from.
///
/// The two adapters have different trust requirements: `Plan46Receipt` revalidates a
/// catalog identity, convergence, and object HEADs before copying; `ExplicitFiles`
/// takes a caller-declared schema and sorted file list and never lists a directory.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceKind {
    /// Frozen from a plan-46 `ukiel-parquet-snapshot` adapter over converged parts.
    Plan46Receipt,
    /// Frozen from an explicit schema and sorted file list (e.g. a ClickBench slice).
    ExplicitFiles,
}

/// A logical projection needed to compare physical-type variants against the
/// snapshot's declared logical schema.
///
/// A variant may store a column as `Int32` where the snapshot declared `int64`. The
/// query layer runs unchanged SQL by casting the physical column back through this
/// projection before timing; the logical fingerprint is computed under the same
/// declared types on both sides.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogicalProjection {
    /// The declared logical type per column name, e.g. `{"count": "int64"}`. The
    /// laboratory's canonical encoder and the query cast both key off this.
    pub logical_types: serde_json::Map<String, serde_json::Value>,
}

/// One frozen local file and its provenance.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotFile {
    /// Relative to the snapshot manifest's directory.
    pub path: String,
    /// The original object-store key, when frozen from a Ukiel source. `None` for an
    /// explicit local file list.
    pub object_key: Option<String>,
    pub digest: crate::Digest,
    pub bytes: u64,
    pub rows: u64,
    pub row_groups: u32,
    /// The source part identity (catalog part id or the explicit list index), for
    /// tracing a frozen file back to what produced it.
    pub source_part: Option<String>,
}

/// The immutable snapshot manifest.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SnapshotManifest {
    pub manifest_version: String,
    pub source_kind: SourceKind,
    /// The source contract files this was frozen from, by digest (a plan-46 receipt
    /// and its L0/source manifests, or an explicit schema file). Reverified before
    /// anything downstream trusts a byte.
    pub source_digests: Vec<FileDigest>,
    pub tool_versions: crate::ToolVersions,
    /// The exact command that produced this snapshot, for reproduction.
    pub creation_command: String,
    /// The declared logical schema (Ukiel's table schema, or the explicit adapter's).
    pub logical_schema: serde_json::Value,
    /// The physical Arrow schema of the frozen files, serialized.
    pub physical_schema: serde_json::Value,
    pub packing_key: String,
    pub sort_key: Vec<String>,
    /// The projection used to compare physical-type variants, when a variant may
    /// change physical types. `None` when the snapshot fixes the physical schema.
    pub logical_projection: Option<LogicalProjection>,
    pub files: Vec<SnapshotFile>,
    pub total_rows: u64,
    pub total_bytes: u64,
    /// The physical `row-multiset/v1` fingerprint, present only when the Arrow schema
    /// is unchanged (so it is comparable to a plan-46 receipt). Absent once a variant
    /// may change physical types; the logical fingerprint is the invariant then.
    pub physical_fingerprint: Option<Fingerprint>,
    /// The logical-row fingerprint. Every downstream variant must reproduce it.
    pub logical_fingerprint: Fingerprint,
    /// The synthetic disclaimer, when the source carried one. `None` for a
    /// non-synthetic explicit dataset (e.g. ClickBench).
    pub disclaimer: Option<String>,
}

impl SnapshotManifest {
    pub fn parse(path: &str, bytes: &[u8]) -> Result<Self, ContractError> {
        let value: serde_json::Value =
            serde_json::from_slice(bytes).map_err(|source| ContractError::Parse {
                path: path.to_string(),
                source,
            })?;
        check_version(path, &value, SNAPSHOT_MANIFEST_VERSION, "manifest_version")?;
        let manifest: SnapshotManifest =
            serde_json::from_value(value).map_err(|source| ContractError::Parse {
                path: path.to_string(),
                source,
            })?;
        manifest.validate(path)?;
        Ok(manifest)
    }

    /// Enforce the structural invariants no serde schema can: relative paths, unique
    /// file paths, a correctly-versioned logical fingerprint, and consistent totals.
    pub fn validate(&self, path: &str) -> Result<(), ContractError> {
        if self.logical_fingerprint.version != LOGICAL_ROW_MULTISET_VERSION {
            return Err(ContractError::FingerprintVersion {
                context: path.to_string(),
                expected: LOGICAL_ROW_MULTISET_VERSION.to_string(),
                found: self.logical_fingerprint.version.clone(),
            });
        }
        let mut seen = std::collections::HashSet::new();
        for f in &self.files {
            check_relative_path(path, &f.path)?;
            if !seen.insert(f.path.as_str()) {
                return Err(ContractError::DuplicateFile {
                    context: path.to_string(),
                    duplicate: f.path.clone(),
                });
            }
        }
        Ok(())
    }
}

//! `ukiel-parquet-skip/v1` — an experimental custom skip-index sidecar.
//!
//! A sidecar is bound to the exact variant manifest digest and *every* file digest.
//! For each indexed column and row group it records the index kind, version,
//! parameters, and a payload offset/length/digest. It is deliberately conservative:
//! any unknown version, schema mismatch, file mismatch, missing row group, corrupt
//! payload, or unsupported predicate must resolve to `Unknown`/keep, never a skip.
//! A false-negative skip would silently drop matching rows — the one failure a
//! laboratory measuring pruning can never tolerate.
//!
//! This is not a proposed production format. It exists to *price* build time,
//! bytes, metadata requests, pruning, and latency before deciding whether a real
//! implementation belongs in Parquet metadata, an object sidecar, or the catalog.

use serde::{Deserialize, Serialize};

use crate::{ContractError, check_version};

/// The sidecar format. A reader that does not recognise it fails closed to `keep`.
pub const SKIP_MANIFEST_VERSION: &str = "ukiel-parquet-skip/v1";

/// The bounded prototype index kinds. New kinds are a new closed variant, never an
/// open string — an unrecognised kind must be a validation error, not a silent skip.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IndexKind {
    /// External min/max zone map for a scalar or string column.
    ZoneMap,
    /// A small exact value set for equality/`IN` on low-NDV row groups, capped by an
    /// explicit payload-byte budget.
    ValueSet,
    /// A byte-prefix set at a declared length for `LIKE 'literal%'`.
    PrefixSet,
}

/// One indexed row group's payload within a file's sidecar.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RowGroupIndex {
    pub row_group: u32,
    pub kind: IndexKind,
    /// Kind-specific parameters (prefix length, value-set budget, zone-map bounds).
    pub parameters: serde_json::Value,
    /// Offset and length of this row group's payload within the sidecar payload file.
    pub payload_offset: u64,
    pub payload_length: u64,
    /// Digest of exactly `payload_length` bytes at `payload_offset`. A corrupt payload
    /// keeps the row group.
    pub payload_digest: crate::Digest,
}

/// The per-column index over every row group of every bound file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexedColumn {
    pub column: String,
    pub kind: IndexKind,
    /// Version of this index kind's payload encoding. An unknown version keeps.
    pub kind_version: String,
    /// The bound file, by relative path. Indices are per file per column.
    pub file: String,
    pub row_groups: Vec<RowGroupIndex>,
}

/// The sidecar manifest.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SkipManifest {
    pub manifest_version: String,
    /// The parent variant manifest, by digest. A mismatch keeps every group.
    pub parent_variant_digest: crate::Digest,
    /// Every bound file digest. A file that has changed since the sidecar was built
    /// keeps every row group of that file.
    pub file_digests: Vec<crate::FileDigest>,
    /// The relative path of the payload blob these offsets index into.
    pub payload_path: String,
    pub columns: Vec<IndexedColumn>,
    /// Build cost, so a report can price the sidecar against the pruning it buys.
    pub build_wall_ms: u64,
    pub payload_bytes: u64,
}

impl SkipManifest {
    pub fn parse(path: &str, bytes: &[u8]) -> Result<Self, ContractError> {
        let value: serde_json::Value =
            serde_json::from_slice(bytes).map_err(|source| ContractError::Parse {
                path: path.to_string(),
                source,
            })?;
        check_version(path, &value, SKIP_MANIFEST_VERSION, "manifest_version")?;
        let manifest: SkipManifest =
            serde_json::from_value(value).map_err(|source| ContractError::Parse {
                path: path.to_string(),
                source,
            })?;
        crate::check_relative_path(path, &manifest.payload_path)?;
        for c in &manifest.columns {
            crate::check_relative_path(path, &c.file)?;
        }
        Ok(manifest)
    }

    /// Prove this sidecar is still bound to the variant it was built for: the parent
    /// digest and every named file digest must match. Any disagreement means the
    /// sidecar must not be trusted to skip — the caller keeps every row group.
    pub fn is_bound_to(&self, variant_digest: &str, files: &[crate::FileDigest]) -> bool {
        if self.parent_variant_digest != variant_digest {
            return false;
        }
        // Every file the sidecar indexes must be present with an identical digest.
        self.file_digests.iter().all(|bound| {
            files
                .iter()
                .any(|f| f.path == bound.path && f.digest == bound.digest)
        })
    }
}

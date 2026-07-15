//! Versioned artifact contracts for the plan-47 parquet storage laboratory.
//!
//! The laboratory is a chain of small single-purpose executables — snapshot,
//! census, rewrite, skip-index, and bench — joined *only* by the files described
//! here. They never call each other's command handlers, and no executable package
//! depends on another executable package. This library is what makes that
//! possible, so it must stay what it is: serde types, version gates, and BLAKE3
//! digests. It reads no Parquet, materializes no Arrow, opens no socket.
//!
//! ## Why version everything
//!
//! Every file the laboratory emits carries a version string, and every reader
//! re-asserts the exact one it understands. A reader that half-parses an unknown
//! format reports a number nobody can trust — worse in a measurement laboratory
//! than a loud failure. There is no "best effort" parse here: an unrecognised
//! version fails closed.
//!
//! ## Why digests bind
//!
//! A variant manifest names its parent snapshot by digest; a skip-index sidecar
//! names its parent variant *and every file* by digest. The chain is only
//! meaningful if a downstream tool can prove the bytes it is reading are the exact
//! bytes an upstream tool produced. `FileDigest` and the binding checks below are
//! how that proof travels.

use serde::{Deserialize, Serialize};

pub mod index;
pub mod report;
pub mod run_set;
pub mod snapshot;
pub mod store;
pub mod suite;
pub mod variant;

pub use index::{IndexKind, IndexedColumn, RowGroupIndex, SKIP_MANIFEST_VERSION, SkipManifest};
pub use report::{HostInfo, REPORT_VERSION, ReportIdentity, RunReport, ToolVersions};
pub use run_set::{RUN_SET_VERSION, RunSet, RunSetEntry, RunSetState};
pub use snapshot::{
    LogicalProjection, SNAPSHOT_MANIFEST_VERSION, SnapshotFile, SnapshotManifest, SourceKind,
};
pub use store::{STORE_RECEIPT_VERSION, StoreObject, StoreReceipt};
pub use suite::{
    PROBES_VERSION, Probe, ProbeFamily, ProbeSkipReason, Query, ResultSemantics, SUITE_VERSION,
    SkippedProbe, Suite, SuiteKind, TypedLiteral,
};
pub use variant::{
    ColumnProperties, VARIANT_MANIFEST_VERSION, VariantFileMap, VariantManifest, WriterProperties,
};

/// A BLAKE3 digest, lowercase hex. The one hash the laboratory speaks.
pub type Digest = String;

/// The logical-row fingerprint version, mirrored from `parquet-lab-integrity`.
///
/// The contract carries no Arrow, so it cannot compute this fingerprint — but it
/// must be able to *carry* one and compare two. [`Fingerprint`] is the Arrow-free
/// serializable shape the integrity crate converts into; this constant is the
/// version a logical fingerprint declares, distinct from the physical
/// `row-multiset/v1` a plan-46 receipt carries.
pub const LOGICAL_ROW_MULTISET_VERSION: &str = "logical-row-multiset/v1";

#[derive(Debug, thiserror::Error)]
pub enum ContractError {
    #[error("{path}: not a {expected} artifact (found version '{found}')")]
    Version {
        path: String,
        expected: String,
        found: String,
    },
    #[error("{path}: {source}")]
    Parse {
        path: String,
        #[source]
        source: serde_json::Error,
    },
    #[error("{path}: {source}")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error(
        "{path}: digest mismatch — expected {expected}, got {actual}. The artifact has been \
         modified or truncated since it was written; regenerate it rather than loading it."
    )]
    Digest {
        path: String,
        expected: String,
        actual: String,
    },
    #[error(
        "{context}: path '{offending}' is not relative. Laboratory manifests record only \
         manifest-relative paths so an artifact survives being copied to a benchmark host."
    )]
    NonRelativePath { context: String, offending: String },
    #[error("{context}: file path '{duplicate}' appears more than once in one manifest")]
    DuplicateFile { context: String, duplicate: String },
    #[error("{context}: label '{duplicate}' appears more than once; labels must be unique")]
    DuplicateLabel { context: String, duplicate: String },
    #[error(
        "{context}: column '{column}' has requested writer properties but no resolved \
         properties. A variant must read back what the writer actually did, never trust \
         the request."
    )]
    IncompleteProperties { context: String, column: String },
    #[error("{context}: unknown skip-index kind '{kind}'")]
    UnknownIndexKind { context: String, kind: String },
    #[error(
        "{context}: fingerprint version mismatch — a '{expected}' fingerprint cannot be \
         compared against a '{found}' one."
    )]
    FingerprintVersion {
        context: String,
        expected: String,
        found: String,
    },
    #[error(
        "{context}: the parent binding does not hold — expected parent digest {expected}, \
         the manifest names {actual}."
    )]
    ParentMismatch {
        context: String,
        expected: String,
        actual: String,
    },
    #[error("{context}: object '{offending}' is out of sorted order; a store receipt is sorted")]
    UnsortedObjects { context: String, offending: String },
    #[error("{context}: a run set must schedule at least one measurement")]
    EmptySchedule { context: String },
    #[error("{context}: a complete run set is missing the report for scheduled entry '{expected}'")]
    MissingReport { context: String, expected: String },
    #[error("{context}: report digest '{digest}' is bound to more than one scheduled entry")]
    DuplicateReport { context: String, digest: String },
}

/// BLAKE3 of raw bytes, lowercase hex. Deliberately over the bytes on disk, not a
/// reserialized struct: the point is to prove *these* bytes are the ones written,
/// and a round-trip through serde would launder the corruption this catches.
pub fn digest_bytes(bytes: &[u8]) -> Digest {
    blake3::hash(bytes).to_hex().to_string()
}

/// A file's relative path, byte length, and digest, re-checked at every boundary
/// so a report can always name the exact bytes it measured.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileDigest {
    /// Path relative to the manifest's own directory. Never absolute, never
    /// `..`-traversing: the artifact must survive being copied to a benchmark host.
    pub path: String,
    pub bytes: u64,
    pub digest: Digest,
}

impl FileDigest {
    pub fn of(path: impl Into<String>, bytes: &[u8]) -> Self {
        FileDigest {
            path: path.into(),
            bytes: bytes.len() as u64,
            digest: digest_bytes(bytes),
        }
    }

    /// Re-check a digest at a tool boundary, so a corrupt artifact fails on the
    /// filesystem rather than halfway through a measurement.
    pub fn verify(&self, path: &str, bytes: &[u8]) -> Result<(), ContractError> {
        let actual = digest_bytes(bytes);
        if actual != self.digest {
            return Err(ContractError::Digest {
                path: path.to_string(),
                expected: self.digest.clone(),
                actual,
            });
        }
        Ok(())
    }
}

/// A multiset fingerprint as carried in a manifest — the Arrow-free serializable
/// mirror of a `parquet-lab-integrity` accumulator.
///
/// The same shape carries both the physical `row-multiset/v1` (from a plan-46
/// receipt) and the logical [`LOGICAL_ROW_MULTISET_VERSION`]; the `version` string
/// is what tells them apart. Two fingerprints agree iff their version, count, XOR
/// and sum lanes all match — a version mismatch is a disagreement, loudly, because
/// fingerprints under different encodings are simply not comparable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Fingerprint {
    pub version: String,
    pub count: u64,
    /// Lowercase hex of the 32-byte XOR accumulator.
    pub xor: String,
    pub sum: [u64; 4],
    /// Lowercase hex of the whole-accumulator digest. Derived; for eyeballing.
    pub digest: String,
}

impl Fingerprint {
    /// Do these fingerprints describe the same multiset of rows?
    pub fn agrees_with(&self, other: &Fingerprint) -> bool {
        self.version == other.version
            && self.count == other.count
            && self.xor == other.xor
            && self.sum == other.sum
    }
}

/// The version gate every manifest shares. Reads the version as a bare value first,
/// so a mismatch is reported as a version error rather than as whatever field fails
/// to deserialize under the wrong schema.
pub(crate) fn check_version(
    path: &str,
    value: &serde_json::Value,
    expected: &str,
    field: &str,
) -> Result<(), ContractError> {
    let found = value
        .get(field)
        .and_then(|v| v.as_str())
        .unwrap_or("<absent>");
    if found != expected {
        return Err(ContractError::Version {
            path: path.to_string(),
            expected: expected.to_string(),
            found: found.to_string(),
        });
    }
    Ok(())
}

/// A relative, non-traversing path check shared by every manifest that records file
/// paths. Absolute paths and `..` segments are refused: a laboratory artifact must
/// survive being copied wholesale to a benchmark host.
pub(crate) fn check_relative_path(context: &str, path: &str) -> Result<(), ContractError> {
    let p = std::path::Path::new(path);
    let offending = p.is_absolute()
        || path.starts_with('/')
        || p.components().any(|c| {
            matches!(
                c,
                std::path::Component::ParentDir | std::path::Component::RootDir
            )
        });
    if offending {
        return Err(ContractError::NonRelativePath {
            context: context.to_string(),
            offending: path.to_string(),
        });
    }
    Ok(())
}

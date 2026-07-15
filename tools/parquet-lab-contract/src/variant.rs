//! `ukiel-parquet-variant/v1` — one snapshot rewritten under one explicit spec.
//!
//! A variant preserves file membership, row order, sort semantics, and file count;
//! it may change row-group/page boundaries and the physical schema. Its manifest
//! records both the *requested* writer properties and the *resolved* ones read back
//! from the output footers — because a writer may fall back (a dictionary that
//! overflows its page limit becomes `PLAIN`), and a laboratory that trusted the
//! request rather than the footer would credit an encoding that never happened.

use serde::{Deserialize, Serialize};

use crate::{ContractError, FileDigest, Fingerprint, check_relative_path, check_version};

/// The variant manifest format. A reader that does not recognise it fails closed.
pub const VARIANT_MANIFEST_VERSION: &str = "ukiel-parquet-variant/v1";

/// Writer properties applied globally to a variant. Every field is a pinned
/// Parquet 58.3 control the matrix actually varies; an unsupported combination is a
/// validation error before any output is written, never an approximation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WriterProperties {
    pub row_group_rows: u64,
    /// Whether row groups flush at packing-key boundaries (the product policy) or at
    /// fixed row caps regardless of key.
    pub key_boundary_flush: bool,
    pub write_batch_rows: u64,
    pub data_page_bytes: u64,
    pub dictionary_page_bytes: u64,
    /// `none`, `chunk`, or `page` — the `EnabledStatistics` mode.
    pub statistics: String,
    /// Whether the offset (page) index is written.
    pub offset_index: bool,
    /// The whole-file compression, e.g. `zstd(3)`, `lz4_raw`, `snappy`, `uncompressed`.
    pub compression: String,
}

/// Per-column overrides. A column absent here inherits the global properties.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnProperties {
    pub column: String,
    /// Requested encoding, e.g. `plain`, `delta_binary_packed`, `delta_byte_array`.
    pub encoding: Option<String>,
    pub dictionary: Option<bool>,
    pub compression: Option<String>,
    /// Requested Bloom-filter FPP; `None` disables the Bloom filter on this column.
    pub bloom_fpp: Option<f64>,
    /// Requested Bloom NDV; `None` uses the writer default when a Bloom is requested.
    pub bloom_ndv: Option<u64>,
    /// A declared lossless physical-type projection, e.g. `int32` for a column the
    /// census proved fits. Empty when the column keeps its snapshot physical type.
    pub physical_type: Option<String>,
    /// The encodings the writer *actually* emitted for this column, read from the
    /// output footer. Requesting a property is not evidence the writer used it.
    pub resolved_encodings: Vec<String>,
    /// Whether the writer actually dictionary-encoded, read from the footer.
    pub resolved_dictionary: Option<bool>,
    /// The compression the writer actually applied, read from the footer.
    pub resolved_compression: Option<String>,
}

/// The input→output mapping for one file. File membership and order are preserved,
/// so this is a per-file pairing, never a repartition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VariantFileMap {
    pub input: FileDigest,
    pub output: FileDigest,
    pub input_rows: u64,
    pub output_rows: u64,
    /// A compact footer summary of the output (row groups, encodings, codecs), for a
    /// report that does not want to re-open the file.
    pub footer_summary: serde_json::Value,
}

/// The variant manifest.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VariantManifest {
    pub manifest_version: String,
    /// The parent snapshot manifest, by digest. The rewrite is only meaningful bound
    /// to the exact bytes it rewrote.
    pub parent_snapshot_digest: crate::Digest,
    /// The variant spec (TOML) this was produced from, by digest.
    pub spec_digest: crate::Digest,
    /// A human label, e.g. `pages-rowgroup-32k`. Unique within a matrix block.
    pub label: String,
    pub properties: WriterProperties,
    pub columns: Vec<ColumnProperties>,
    /// Input and output census reports (the `parquet-census` body), for size deltas.
    pub input_census: serde_json::Value,
    pub output_census: serde_json::Value,
    /// The logical fingerprint of the rewritten rows. Must equal the snapshot's — a
    /// mismatch invalidates the entire variant.
    pub logical_fingerprint: Fingerprint,
    pub files: Vec<VariantFileMap>,
}

impl VariantManifest {
    pub fn parse(path: &str, bytes: &[u8]) -> Result<Self, ContractError> {
        let value: serde_json::Value =
            serde_json::from_slice(bytes).map_err(|source| ContractError::Parse {
                path: path.to_string(),
                source,
            })?;
        check_version(path, &value, VARIANT_MANIFEST_VERSION, "manifest_version")?;
        let manifest: VariantManifest =
            serde_json::from_value(value).map_err(|source| ContractError::Parse {
                path: path.to_string(),
                source,
            })?;
        manifest.validate(path)?;
        Ok(manifest)
    }

    /// Enforce the invariants: relative output paths, unique output paths, and every
    /// column that requested a property having read back its resolved counterpart.
    pub fn validate(&self, path: &str) -> Result<(), ContractError> {
        let mut seen = std::collections::HashSet::new();
        for f in &self.files {
            check_relative_path(path, &f.output.path)?;
            if !seen.insert(f.output.path.as_str()) {
                return Err(ContractError::DuplicateFile {
                    context: path.to_string(),
                    duplicate: f.output.path.clone(),
                });
            }
        }
        for c in &self.columns {
            let requested_something =
                c.encoding.is_some() || c.dictionary.is_some() || c.compression.is_some();
            let resolved_something = !c.resolved_encodings.is_empty()
                || c.resolved_dictionary.is_some()
                || c.resolved_compression.is_some();
            if requested_something && !resolved_something {
                return Err(ContractError::IncompleteProperties {
                    context: path.to_string(),
                    column: c.column.clone(),
                });
            }
        }
        Ok(())
    }

    /// Prove this variant descends from the given snapshot digest.
    pub fn check_parent(&self, snapshot_digest: &str) -> Result<(), ContractError> {
        if self.parent_snapshot_digest != snapshot_digest {
            return Err(ContractError::ParentMismatch {
                context: format!("variant '{}'", self.label),
                expected: snapshot_digest.to_string(),
                actual: self.parent_snapshot_digest.clone(),
            });
        }
        Ok(())
    }
}

//! Plan 46 contracts: the L0 staging manifest and the compaction receipt.
//!
//! These are the two files the part-shape pipeline speaks through, on top of plan 45's
//! `manifest.json`/`topology.json`. Both stay in this tiny library for the same reason
//! the plan-45 manifest does: every executable may depend on the contract, and none may
//! depend on another executable. The contract carries **no** Arrow, no Parquet, no
//! service client — so the row-multiset fingerprint is stored here as plain serializable
//! fields (a version, a count, a hex XOR, four sum lanes), and the crate that actually
//! computes it (`prod-synth-integrity`, which needs Arrow) converts into this shape.

use serde::{Deserialize, Serialize};

use crate::{FileDigest, digest_bytes};

/// The L0 staging artifact format. A reader that does not recognise it fails closed.
pub const PROD_SYNTH_L0_VERSION: &str = "ukiel-prod-synth-l0/v1";

/// The compaction receipt format.
pub const PART_SHAPE_RECEIPT_VERSION: &str = "ukiel-prod-part-shape/v1";

/// A row-multiset fingerprint, as carried in a manifest.
///
/// A serializable mirror of `prod_synth_integrity::RowMultiset`, kept here so the
/// contract stays Arrow-free. Two fingerprints agree iff their version, count, XOR and
/// sum lanes all match — `digest` is derived and included only for eyeballing a report.
///
/// The comparison is the whole job: it is what proves that the rows a fixture holds
/// after compaction are the same multiset it held as staged input, even though every
/// file, path, and grouping has changed underneath.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RowFingerprint {
    /// The encoding version (`row-multiset/v1`). A fingerprint under a different version
    /// is not comparable — the canonical row bytes may differ — so `agrees_with` treats
    /// a version mismatch as disagreement, loudly.
    pub version: String,
    pub count: u64,
    /// Lowercase hex of the 32-byte XOR accumulator.
    pub xor: String,
    pub sum: [u64; 4],
    /// Lowercase hex of the whole-accumulator digest. Derived; for reports.
    pub digest: String,
}

impl RowFingerprint {
    /// Do these fingerprints describe the same multiset of rows?
    ///
    /// A version mismatch is a disagreement, not an error to swallow: two fixtures
    /// fingerprinted under different encodings cannot be compared, and pretending they
    /// agree would defeat the guard entirely.
    pub fn agrees_with(&self, other: &RowFingerprint) -> bool {
        self.version == other.version
            && self.count == other.count
            && self.xor == other.xor
            && self.sum == other.sum
    }
}

/// One staged L0 Parquet file. Level-0 shaped, sorted by the table sort key, and
/// committed as its own independent run so the real ladder — not a test shortcut — does
/// the merging.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct L0File {
    /// Relative to the L0 manifest's directory, e.g. `parquet/2026-07-01/flush-00003.parquet`.
    pub path: String,
    /// UTC day this file's rows fall in, `YYYY-MM-DD`. Derived from the declared
    /// timestamp column — **never** from the source's opaque ClickHouse partition hash.
    pub day: String,
    /// The deterministic flush this file came from. A day may span several flushes and a
    /// flush several days, so `(day, flush_index)` names a file uniquely.
    pub flush_index: u32,
    pub rows: u64,
    pub bytes: u64,
    pub digest: crate::Digest,
    pub key_min: i64,
    pub key_max: i64,
    pub ts_min: i64,
    pub ts_max: i64,
}

/// How the staging regrouped the rows. Deterministic, so a reader can reproduce it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct L0StagingConfig {
    /// Rows per flush before day-partitioning splits it. Each resulting day slice is one
    /// committed L0 run.
    pub flush_rows: u64,
    /// Always "utc-day-from-timestamp". Recorded so the day derivation is auditable and
    /// can never be confused with the source's monthly partition hash.
    pub day_policy: String,
    /// The source seed, carried through so the whole chain shares one identity.
    pub seed: u64,
}

/// The staged L0 artifact.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct L0Manifest {
    pub manifest_version: String,
    pub disclaimer: String,
    /// The plan-45 source artifact this was staged from, by digest. Reverified before
    /// anything downstream trusts a byte.
    pub source_manifest: FileDigest,
    pub source_topology: FileDigest,
    pub staging: L0StagingConfig,
    /// The Ukiel table spec, verbatim from the source manifest — schema, sort key,
    /// packing key, ts column.
    pub table: crate::TableSpec,
    /// Rows read from the source artifact.
    pub input_rows: u64,
    /// Rows written across all L0 files. Must equal `input_rows`: staging regroups rows,
    /// it never adds or drops one.
    pub output_rows: u64,
    /// The order-independent fingerprint of every staged row. The source artifact and
    /// the compacted output must both reproduce it.
    pub fingerprint: RowFingerprint,
    pub files: Vec<L0File>,
}

/// Placement policy for the compaction-input load. Mirrors `ukiel_core::Placement`
/// without depending on it — the contract stays Arrow- and ukiel-core-free.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "target_file_bytes")]
pub enum PlacementSpec {
    /// One file per merge output. `target_file_bytes` NULL in the catalog.
    Packed,
    /// Every packing key its own files. `target_file_bytes = 0`.
    Separated,
    /// Merge outputs cut at key boundaries into ~N-byte files.
    SizeTargeted(i64),
}

impl PlacementSpec {
    /// The label a report and a fixture name use.
    pub fn as_str(self) -> &'static str {
        match self {
            PlacementSpec::Packed => "packed",
            PlacementSpec::Separated => "separated",
            PlacementSpec::SizeTargeted(_) => "size-targeted",
        }
    }

    /// The `hypertables.target_file_bytes` value this placement stores.
    pub fn target_file_bytes(self) -> Option<i64> {
        match self {
            PlacementSpec::Packed => None,
            PlacementSpec::Separated => Some(0),
            PlacementSpec::SizeTargeted(n) => Some(n),
        }
    }
}

/// The compactor settings a run is expected to use.
///
/// Recorded in the receipt so the runner can prove the fixture it is measuring was
/// compacted under the intended configuration — a fixture converged under a different
/// `finalize_after_secs` is a different experiment, and the number it yields would be
/// attributed to the wrong config.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExpectedCompactorConfig {
    pub l0_fanout: usize,
    pub fanout: usize,
    pub finalize_after_secs: u64,
    pub finalize_poll_interval_ms: u64,
    pub lease_ttl_secs: u64,
    pub lease_renew_interval_secs: u64,
    pub candidate_limit: i64,
}

/// The load receipt: identity, not authority.
///
/// It survives REPLACE, where original object paths and part counts do not. Every
/// reader rechecks the catalog table name/id, schema, partition marker, row census, and
/// manifest digests against it rather than trusting it — the receipt says what *should*
/// be there; the catalog says what is.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PartShapeReceipt {
    pub receipt_version: String,
    pub disclaimer: String,
    pub source_manifest: FileDigest,
    pub l0_manifest: FileDigest,
    pub label: String,
    pub hypertable: String,
    pub hypertable_id: i64,
    pub placement: PlacementSpec,
    /// Input parts committed (one per L0 file), their rows, and their bytes — measured
    /// from the objects at load time.
    pub input_parts: u64,
    pub input_rows: u64,
    pub input_bytes: u64,
    pub packing_key: String,
    pub sort_key: Vec<String>,
    /// The `partition_values` marker stamped into every input part: the L0 manifest
    /// digest and the UTC day. Compaction preserves partition values, so a final part's
    /// marker proves it descends from this exact staged artifact.
    pub partition_marker_digest: String,
    pub compactor: ExpectedCompactorConfig,
    /// The staged-row fingerprint, carried through so the runner can check the compacted
    /// output against it without re-reading the L0 artifact.
    pub fingerprint: RowFingerprint,
}

impl L0Manifest {
    /// Parse and validate the contract: right version, and it says it is synthetic.
    pub fn parse(path: &str, bytes: &[u8]) -> Result<Self, crate::ContractError> {
        let value: serde_json::Value =
            serde_json::from_slice(bytes).map_err(|source| crate::ContractError::Parse {
                path: path.to_string(),
                source,
            })?;
        check_version(path, &value, PROD_SYNTH_L0_VERSION, "manifest_version")?;
        let manifest: L0Manifest =
            serde_json::from_value(value).map_err(|source| crate::ContractError::Parse {
                path: path.to_string(),
                source,
            })?;
        if manifest.disclaimer != crate::SYNTHETIC_DISCLAIMER {
            return Err(crate::ContractError::MissingDisclaimer {
                path: path.to_string(),
            });
        }
        Ok(manifest)
    }
}

impl PartShapeReceipt {
    pub fn parse(path: &str, bytes: &[u8]) -> Result<Self, crate::ContractError> {
        let value: serde_json::Value =
            serde_json::from_slice(bytes).map_err(|source| crate::ContractError::Parse {
                path: path.to_string(),
                source,
            })?;
        check_version(path, &value, PART_SHAPE_RECEIPT_VERSION, "receipt_version")?;
        let receipt: PartShapeReceipt =
            serde_json::from_value(value).map_err(|source| crate::ContractError::Parse {
                path: path.to_string(),
                source,
            })?;
        if receipt.disclaimer != crate::SYNTHETIC_DISCLAIMER {
            return Err(crate::ContractError::MissingDisclaimer {
                path: path.to_string(),
            });
        }
        Ok(receipt)
    }

    /// The partition-values marker every input part carries. The runner rebuilds this
    /// from the L0 manifest digest and a day and checks a final part against it, proving
    /// descent from the selected staged artifact without a production table.
    pub fn partition_marker(l0_manifest_digest: &str, day: &str) -> serde_json::Value {
        serde_json::json!({
            "l0_manifest": l0_manifest_digest,
            "utc_day": day,
        })
    }

    /// The digest of the marker's *identity* half (the L0 manifest digest), used to
    /// prove every part descends from the same artifact regardless of its day.
    pub fn marker_digest(l0_manifest_digest: &str) -> String {
        digest_bytes(l0_manifest_digest.as_bytes())
    }
}

/// The version gate both files share, read as a bare `Value` first so a mismatch is
/// reported as a version error rather than as whatever field fails to deserialize.
fn check_version(
    path: &str,
    value: &serde_json::Value,
    expected: &str,
    field: &str,
) -> Result<(), crate::ContractError> {
    let found = value
        .get(field)
        .and_then(|v| v.as_str())
        .unwrap_or("<absent>");
    if found != expected {
        return Err(crate::ContractError::Version {
            path: path.to_string(),
            found: found.to_string(),
        });
    }
    Ok(())
}

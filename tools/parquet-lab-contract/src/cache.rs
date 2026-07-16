//! `ukiel-parquet-cache-receipt/v1` — proof that a scenario ran under the cache
//! state it claimed.
//!
//! A "cold" or "warm" label is worthless without evidence. The cache controller
//! prepares one profile over an explicit set of benchmark files, probes page
//! residency before and after, and writes this receipt. A bench tool binds the
//! receipt's digest into every sample it timed under that profile; the analyzer
//! refuses a cold/warm claim whose receipt is invalid. If eviction was ineffective
//! or the kernel is unsupported, the receipt is marked invalid and no sample may be
//! relabelled to a state that was never achieved.

use serde::{Deserialize, Serialize};

use crate::{CacheProfile, ContractError, FileDigest, check_version};

/// The cache-receipt format. A reader that does not recognise it fails closed.
pub const CACHE_RECEIPT_VERSION: &str = "ukiel-parquet-cache-receipt/v1";

/// A page-residency reading over the target files: the fraction of the target byte
/// ranges resident in the OS page cache, sampled with `mincore` or an equivalent.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Residency {
    /// Resident fraction in `[0, 1]`. A whole-artifact reading over the registered
    /// target ranges, not a single file.
    pub resident_fraction: f64,
    /// Pages probed, so a reading over a trivially small artifact is not over-trusted.
    pub pages_probed: u64,
}

/// One prepared cache profile over one explicit set of benchmark files.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CacheReceipt {
    pub receipt_version: String,
    /// The artifact manifest whose files this profile was prepared over, by digest.
    pub target_manifest_digest: crate::Digest,
    /// The exact files prepared, by digest. The receipt is only meaningful bound to
    /// the bytes it evicted or resident-loaded.
    pub target_files: Vec<FileDigest>,
    pub requested_profile: CacheProfile,
    /// How the profile was prepared, e.g. `read-all-ranges` or
    /// `posix_fadvise(DONTNEED)+mincore`. Recorded, never inferred.
    pub preparation_method: String,
    /// Residency probed before preparation.
    pub residency_before: Residency,
    /// Residency probed after preparation — the reading the validity gate uses.
    pub residency_after: Residency,
    /// The warm residency floor this profile required, when it is a warm profile.
    #[serde(default)]
    pub warm_floor: Option<f64>,
    /// The cold residency ceiling this profile required, when it is a cold profile.
    #[serde(default)]
    pub cold_ceiling: Option<f64>,
    /// Whether the profile was actually achieved. A cold profile whose eviction did
    /// not drop below its ceiling, or a warm profile that did not reach its floor, is
    /// invalid — the controller records the failure rather than relabelling a sample.
    pub valid: bool,
}

impl CacheReceipt {
    pub fn parse(path: &str, bytes: &[u8]) -> Result<Self, ContractError> {
        let value: serde_json::Value =
            serde_json::from_slice(bytes).map_err(|source| ContractError::Parse {
                path: path.to_string(),
                source,
            })?;
        check_version(path, &value, CACHE_RECEIPT_VERSION, "receipt_version")?;
        let m: CacheReceipt =
            serde_json::from_value(value).map_err(|source| ContractError::Parse {
                path: path.to_string(),
                source,
            })?;
        m.validate(path)?;
        Ok(m)
    }

    /// Enforce that the recorded validity is consistent with the residency reading and
    /// the profile's own threshold. A receipt that claims validity its numbers do not
    /// support is refused: the controller cannot launder an ineffective eviction into a
    /// valid cold sample.
    pub fn validate(&self, path: &str) -> Result<(), ContractError> {
        if self.target_files.is_empty() {
            return Err(ContractError::EmptyScenarioField {
                context: path.to_string(),
                field: "target_files".to_string(),
            });
        }
        for f in &self.target_files {
            crate::check_relative_path(path, &f.path)?;
        }
        let after = self.residency_after.resident_fraction;
        let consistent = if self.requested_profile.requires_cold() {
            let ceiling = self.cold_ceiling.unwrap_or(0.10);
            !self.valid || after <= ceiling
        } else if self.requested_profile.requires_warm() {
            let floor = self.warm_floor.unwrap_or(0.90);
            !self.valid || after >= floor
        } else {
            // decode-resident does not constrain OS residency.
            true
        };
        if !consistent {
            return Err(ContractError::CacheReceiptInconsistent {
                context: path.to_string(),
                profile: format!("{:?}", self.requested_profile),
                resident: after,
            });
        }
        Ok(())
    }

    /// May a sample timed under this receipt be trusted at its claimed profile?
    pub fn is_usable(&self) -> bool {
        self.valid
    }
}

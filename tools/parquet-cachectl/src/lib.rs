//! `parquet-cachectl` — prepare and verify one local cache profile over explicit files.
//!
//! A "cold" or "warm" label is worthless without proof. This tool prepares one profile
//! over an explicit set of benchmark files, probes page residency before and after with
//! `mincore`, and emits a [`CacheReceipt`]. A bench binds the receipt's digest into every
//! sample it times under that profile. If eviction is ineffective (residency stays above
//! the cold ceiling) or the kernel is unsupported, the receipt is marked invalid and the
//! process exits non-zero — a sample is never silently relabelled to a state it never had.
//! Only the explicit files are touched; there is no global `drop_caches`.

pub mod linux;

use std::path::Path;

use anyhow::{Context, Result, bail};
use parquet_lab_contract::{
    CacheProfile, CacheReceipt, FileDigest, ReconstructionManifest, Residency, SnapshotManifest,
    VariantDeltaManifest, digest_bytes,
};

/// The default warm residency floor and cold residency ceiling.
pub const DEFAULT_WARM_FLOOR: f64 = 0.90;
pub const DEFAULT_COLD_CEILING: f64 = 0.10;

/// Parse a profile name.
pub fn parse_profile(s: &str) -> Result<CacheProfile> {
    Ok(match s {
        "decode-resident" => CacheProfile::DecodeResident,
        "local-os-warm" => CacheProfile::LocalOsWarm,
        "local-os-cold" => CacheProfile::LocalOsCold,
        "local-reader-warm" => CacheProfile::LocalReaderWarm,
        other => bail!("unknown cache profile '{other}'"),
    })
}

/// Resolve an artifact manifest's files into `(relative path, FileDigest)` pairs, whichever
/// manifest kind it is. The controller works on any manifest that lists files.
fn artifact_files(bytes: &[u8], path: &str) -> Result<Vec<(String, FileDigest)>> {
    if let Ok(r) = ReconstructionManifest::parse(path, bytes) {
        return Ok(r
            .files
            .into_iter()
            .map(|f| (f.output.path.clone(), f.output))
            .collect());
    }
    if let Ok(v) = VariantDeltaManifest::parse(path, bytes) {
        return Ok(v
            .files
            .into_iter()
            .map(|f| (f.output.path.clone(), f.output))
            .collect());
    }
    if let Ok(s) = SnapshotManifest::parse(path, bytes) {
        return Ok(s
            .files
            .into_iter()
            .map(|f| {
                (
                    f.path.clone(),
                    FileDigest {
                        path: f.path,
                        bytes: f.bytes,
                        digest: f.digest,
                    },
                )
            })
            .collect());
    }
    bail!("{path}: not a reconstruction, variant-delta, or snapshot manifest")
}

/// Aggregate page residency across a set of files.
fn aggregate_residency(paths: &[std::path::PathBuf]) -> Result<Residency> {
    let mut resident = 0u64;
    let mut total = 0u64;
    for p in paths {
        let (r, t) = linux::page_residency(p)?;
        resident += r;
        total += t;
    }
    let fraction = if total == 0 {
        1.0
    } else {
        resident as f64 / total as f64
    };
    Ok(Residency {
        resident_fraction: fraction,
        pages_probed: total,
    })
}

/// Prepare `profile` over the manifest's files and write a receipt. Returns the receipt and
/// whether it is valid (the caller maps invalidity to a non-zero exit).
pub fn prepare(
    manifest_path: &Path,
    profile: CacheProfile,
    receipt_path: &Path,
    warm_floor: f64,
    cold_ceiling: f64,
    replace: bool,
) -> Result<(CacheReceipt, bool)> {
    if receipt_path.exists() && !replace {
        bail!(
            "receipt {} already exists; pass --replace",
            receipt_path.display()
        );
    }
    let dir = manifest_path.parent().unwrap_or(Path::new("."));
    let bytes = std::fs::read(manifest_path)
        .with_context(|| format!("reading {}", manifest_path.display()))?;
    let manifest_digest = digest_bytes(&bytes);
    let files = artifact_files(&bytes, &manifest_path.display().to_string())?;
    if files.is_empty() {
        bail!("the manifest lists no files to prepare");
    }
    let paths: Vec<std::path::PathBuf> = files.iter().map(|(rel, _)| dir.join(rel)).collect();

    // Verify each file against its recorded digest before touching the cache.
    for ((rel, fd), p) in files.iter().zip(&paths) {
        let b = std::fs::read(p).with_context(|| format!("reading {rel}"))?;
        fd.verify(rel, &b)?;
    }

    if !linux::SUPPORTED {
        let receipt = unavailable_receipt(&manifest_digest, &files, profile, "unsupported-kernel");
        write_receipt(receipt_path, &receipt, replace)?;
        return Ok((receipt, false));
    }

    let residency_before = aggregate_residency(&paths)?;
    let method = match profile {
        CacheProfile::LocalOsCold => {
            for p in &paths {
                linux::evict(p)?;
            }
            "posix_fadvise(DONTNEED)+mincore"
        }
        CacheProfile::LocalOsWarm
        | CacheProfile::LocalReaderWarm
        | CacheProfile::DecodeResident => {
            for p in &paths {
                linux::warm(p)?;
            }
            "read-all-ranges+mincore"
        }
    };
    let residency_after = aggregate_residency(&paths)?;

    let after = residency_after.resident_fraction;
    let valid = if profile.requires_cold() {
        after <= cold_ceiling
    } else if profile.requires_warm() {
        after >= warm_floor
    } else {
        true // decode-resident does not constrain OS residency
    };

    let receipt = CacheReceipt {
        receipt_version: parquet_lab_contract::CACHE_RECEIPT_VERSION.to_string(),
        target_manifest_digest: manifest_digest,
        target_files: files.into_iter().map(|(_, fd)| fd).collect(),
        requested_profile: profile,
        preparation_method: method.to_string(),
        residency_before,
        residency_after,
        warm_floor: profile.requires_warm().then_some(warm_floor),
        cold_ceiling: profile.requires_cold().then_some(cold_ceiling),
        valid,
    };
    // The contract refuses a receipt whose validity its numbers do not support.
    receipt.validate(&receipt_path.display().to_string())?;
    write_receipt(receipt_path, &receipt, replace)?;
    Ok((receipt, valid))
}

fn unavailable_receipt(
    manifest_digest: &str,
    files: &[(String, FileDigest)],
    profile: CacheProfile,
    reason: &str,
) -> CacheReceipt {
    CacheReceipt {
        receipt_version: parquet_lab_contract::CACHE_RECEIPT_VERSION.to_string(),
        target_manifest_digest: manifest_digest.to_string(),
        target_files: files.iter().map(|(_, fd)| fd.clone()).collect(),
        requested_profile: profile,
        preparation_method: reason.to_string(),
        residency_before: Residency {
            resident_fraction: 0.0,
            pages_probed: 0,
        },
        residency_after: Residency {
            resident_fraction: 0.0,
            pages_probed: 0,
        },
        warm_floor: None,
        cold_ceiling: None,
        valid: false,
    }
}

fn write_receipt(path: &Path, receipt: &CacheReceipt, replace: bool) -> Result<()> {
    if path.exists() && !replace {
        bail!("receipt {} already exists", path.display());
    }
    let tmp = path.with_extension("json.tmp");
    std::fs::write(&tmp, serde_json::to_vec_pretty(receipt)?)
        .with_context(|| format!("writing {}", tmp.display()))?;
    std::fs::rename(&tmp, path).with_context(|| format!("publishing {}", path.display()))?;
    Ok(())
}

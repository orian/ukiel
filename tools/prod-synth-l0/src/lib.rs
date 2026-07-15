//! `prod-synth-l0` — restage a plan-45 source artifact as Ukiel-shaped L0 Parquet.
//!
//! Offline, deterministic, and installable with only a Rust toolchain. It reads a
//! `prod-synth` fixture and writes the same rows regrouped into UTC-day-partitioned,
//! flush-sized level-0 files, so the real compactor — fed by `ukiel-prod-load
//! compaction-input` — can turn them into final parts whose shape plan 46 measures.
//!
//! It is not a loader and not a generator. It changes how the rows are grouped and
//! nothing else: same rows, same values, proven by an order-independent fingerprint.

pub mod stage;

pub use stage::{StageError, Staged, stage};

use std::path::Path;

use anyhow::{Context, Result};
use prod_synth_contract::{FileDigest, Manifest};

/// Read and verify a plan-45 source artifact, then restage it.
///
/// Verifies the source manifest and every source Parquet part **before** writing a byte
/// of output — a corrupt source must fail up front, not halfway through a staged
/// artifact that then looks complete.
pub fn stage_source(
    manifest_path: &Path,
    output: &Path,
    flush_rows: u64,
    replace: bool,
) -> Result<Staged> {
    let dir = manifest_path.parent().unwrap_or(Path::new("."));

    let manifest_bytes = std::fs::read(manifest_path)
        .with_context(|| format!("reading {}", manifest_path.display()))?;
    let manifest = Manifest::parse(&manifest_path.display().to_string(), &manifest_bytes)?;

    let topology_path = dir.join(&manifest.topology.path);
    let topology_bytes = std::fs::read(&topology_path)
        .with_context(|| format!("reading {}", topology_path.display()))?;
    manifest
        .topology
        .verify(&topology_path.display().to_string(), &topology_bytes)?;

    // Every source part, by the digest the manifest recorded. Staging reads these files;
    // if one changed under the manifest, the staged rows would not be the fixture's.
    for part in &manifest.parts {
        let path = dir.join(&part.path);
        let raw = std::fs::read(&path).with_context(|| format!("reading {}", path.display()))?;
        FileDigest {
            path: part.path.clone(),
            bytes: part.bytes,
            digest: part.digest.clone(),
        }
        .verify(&path.display().to_string(), &raw)?;
    }

    let source_topology = manifest.topology.clone();
    Ok(stage::stage(
        &manifest,
        &manifest_bytes,
        source_topology,
        dir,
        output,
        flush_rows,
        replace,
    )?)
}

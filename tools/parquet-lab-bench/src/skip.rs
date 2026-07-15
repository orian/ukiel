//! Consume an experimental skip-index sidecar and price it.
//!
//! The sidecar's pruning *decisions* and their no-false-negative guarantee are owned and
//! property-tested by `parquet-skip-index`; the two tools are joined by the versioned
//! `ukiel-parquet-skip/v1` file, not by linking. Here the benchmark verifies the sidecar is
//! bound to the exact variant it is measuring — an unbound or file-mismatched sidecar is
//! refused, never trusted to skip — and records its build/storage cost and coverage so a
//! report can weigh a custom index against native Parquet pruning (which the DataFusion plan
//! reports separately as row groups pruned).

use std::path::Path;

use anyhow::{Context, Result, bail};
use parquet_lab_contract::{FileDigest, SkipManifest};
use serde::Serialize;

/// The cost and coverage of a bound sidecar, for the report.
#[derive(Debug, Clone, Serialize)]
pub struct SkipCost {
    pub skip_digest: String,
    pub payload_bytes: u64,
    pub build_wall_ms: u64,
    pub indexed_columns: usize,
    /// Total (column, file, row_group) index entries — the pruning surface the sidecar buys.
    pub indexed_row_groups: usize,
    pub columns: Vec<SkipColumn>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SkipColumn {
    pub column: String,
    pub kind: String,
    pub files: usize,
    pub row_groups: usize,
}

/// Load a sidecar, prove it is bound to `variant_digest` and `variant_files`, and price it.
/// A sidecar that is not provably bound is refused — it must never be trusted to skip.
pub fn load_and_bind(
    skip_manifest_path: &Path,
    variant_digest: &str,
    variant_files: &[FileDigest],
) -> Result<SkipCost> {
    let bytes = std::fs::read(skip_manifest_path)
        .with_context(|| format!("reading {}", skip_manifest_path.display()))?;
    let skip_digest = parquet_lab_contract::digest_bytes(&bytes);
    let sidecar = SkipManifest::parse(&skip_manifest_path.display().to_string(), &bytes)?;

    if !sidecar.is_bound_to(variant_digest, variant_files) {
        bail!(
            "the skip sidecar at {} is not bound to the variant being measured (wrong variant \
             digest or a file has changed under it). Refusing to credit its pruning.",
            skip_manifest_path.display()
        );
    }

    let mut columns = Vec::new();
    let mut indexed_row_groups = 0usize;
    // Group by (column) across files for the summary.
    let mut by_col: std::collections::BTreeMap<String, (String, usize, usize)> =
        std::collections::BTreeMap::new();
    for c in &sidecar.columns {
        indexed_row_groups += c.row_groups.len();
        let entry = by_col
            .entry(c.column.clone())
            .or_insert_with(|| (format!("{:?}", c.kind), 0, 0));
        entry.1 += 1; // one more file
        entry.2 += c.row_groups.len();
    }
    for (column, (kind, files, row_groups)) in by_col {
        columns.push(SkipColumn {
            column,
            kind,
            files,
            row_groups,
        });
    }

    Ok(SkipCost {
        skip_digest,
        payload_bytes: sidecar.payload_bytes,
        build_wall_ms: sidecar.build_wall_ms,
        indexed_columns: columns.len(),
        indexed_row_groups,
        columns,
    })
}

//! Rewrite one snapshot file under one variant spec: a thin orchestrator over the shared
//! `parquet-lab-write-core`. Property resolution, projection, the write itself, and footer
//! readback all live in the write core; this file adds only the rewrite-tool concerns —
//! per-file input verification, the on-disk output, and assembling the [`VariantFileMap`].

use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use parquet_lab_contract::{FileDigest, VariantFileMap};
use parquet_lab_integrity::{LogicalRowMultiset, LogicalSchema};
use parquet_lab_write_core::{
    PhysicalType, input_schema_and_rows, out_schema_for, resolve_footer, sorting_columns,
    validate_projections, write_projected,
};

// Re-export the resolved-column shape so the rest of the crate keeps one name for it.
pub use parquet_lab_write_core::ResolvedColumn;

use crate::spec::VariantSpec;

/// What one rewritten file resolved to, read back from its footer.
pub struct RewrittenFile {
    pub map: VariantFileMap,
    pub columns: BTreeMap<String, ResolvedColumn>,
    pub compressed_bytes: i64,
    pub row_groups: u32,
}

/// Rewrite one file. Folds the rewritten rows into `logical` under the declared schema so
/// the caller can prove the whole variant preserves the snapshot's logical fingerprint.
#[allow(clippy::too_many_arguments)]
pub fn rewrite_file(
    input_digest: FileDigest,
    input_bytes: &[u8],
    out_rel_path: &str,
    spec: &VariantSpec,
    projections: &BTreeMap<String, PhysicalType>,
    declared: &BTreeMap<String, parquet_lab_integrity::LogicalType>,
    sort_key: &[String],
    logical_schema: &LogicalSchema,
    logical: &mut LogicalRowMultiset,
    out_dir: &std::path::Path,
) -> Result<RewrittenFile> {
    let (input_schema, input_rows) = input_schema_and_rows(input_bytes)
        .with_context(|| format!("reading input metadata for {out_rel_path}"))?;

    validate_projections(projections, declared, &input_schema)?;
    let out_schema = Arc::new(out_schema_for(&input_schema, projections));
    let sorting = sorting_columns(&out_schema, sort_key);
    let props = spec.writer_properties(sorting)?;

    // No added context here: the projection's own lossless-narrowing error (`... is lossy`)
    // must surface at the top level, not be masked behind a generic "writing <file>" wrapper.
    let mut buf: Vec<u8> = Vec::new();
    let output_rows = write_projected(
        &mut buf,
        input_bytes,
        projections,
        props,
        out_schema,
        logical_schema,
        logical,
    )?;

    if output_rows != input_rows {
        bail!(
            "{out_rel_path}: wrote {output_rows} rows, input had {input_rows}; membership must be preserved"
        );
    }

    let dst = out_dir.join(out_rel_path);
    if let Some(parent) = dst.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(&dst, &buf).with_context(|| format!("writing {}", dst.display()))?;

    let outcome = resolve_footer(&buf)?;
    let output_digest = FileDigest::of(out_rel_path.to_string(), &buf);
    Ok(RewrittenFile {
        map: VariantFileMap {
            input: input_digest,
            output: output_digest,
            input_rows,
            output_rows,
            footer_summary: outcome.footer_summary,
        },
        columns: outcome.columns,
        compressed_bytes: outcome.compressed_bytes,
        row_groups: outcome.row_groups,
    })
}

//! Rewrite one snapshot file under one variant spec: project types, write under the
//! resolved properties, fold the logical fingerprint over the rewritten rows, and read the
//! output footer back to record what the writer *actually* did.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::metadata::SortingColumn;
use parquet_lab_contract::{FileDigest, VariantFileMap};
use parquet_lab_integrity::{LogicalRowMultiset, LogicalSchema};

use crate::spec::{PhysicalType, VariantSpec};
use crate::types::{project_batch, validate_projections};

/// What one rewritten file resolved to, read back from its footer.
pub struct RewrittenFile {
    pub map: VariantFileMap,
    pub columns: BTreeMap<String, ResolvedColumn>,
    pub compressed_bytes: i64,
    pub row_groups: u32,
}

/// A single column's resolved footer facts, aggregated across the file's row groups.
#[derive(Debug, Clone, Default)]
pub struct ResolvedColumn {
    pub encodings: BTreeSet<String>,
    pub dictionary: bool,
    pub compression: String,
    pub compressed_bytes: i64,
    pub bloom: bool,
}

/// Compute the output Arrow schema after applying the projections.
fn projected_schema(input: &Schema, projections: &BTreeMap<String, PhysicalType>) -> Schema {
    let fields = input
        .fields()
        .iter()
        .map(|f| {
            let ty = match projections.get(f.name()) {
                None => f.data_type().clone(),
                Some(PhysicalType::Int8) => DataType::Int8,
                Some(PhysicalType::Int16) => DataType::Int16,
                Some(PhysicalType::Int32) => DataType::Int32,
                Some(PhysicalType::Int64) => DataType::Int64,
                Some(PhysicalType::TimestampMillis) => {
                    DataType::Timestamp(TimeUnit::Millisecond, None)
                }
                Some(PhysicalType::Date32) => DataType::Date32,
            };
            Arc::new(Field::new(f.name(), ty, f.is_nullable()))
        })
        .collect::<Vec<_>>();
    Schema::new(fields)
}

/// Build Parquet sorting-column metadata for the sort key, matching the product's
/// nulls-first ascending order.
fn sorting_columns(schema: &Schema, sort_key: &[String]) -> Vec<SortingColumn> {
    sort_key
        .iter()
        .filter_map(|name| schema.index_of(name).ok())
        .map(|idx| SortingColumn {
            column_idx: idx as i32,
            descending: false,
            nulls_first: true,
        })
        .collect()
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
    let builder =
        ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::copy_from_slice(input_bytes))
            .with_context(|| format!("opening input for {out_rel_path}"))?;
    let input_schema = builder.schema().as_ref().clone();
    let input_rows = builder.metadata().file_metadata().num_rows() as u64;

    validate_projections(projections, declared, &input_schema)?;
    let out_schema = Arc::new(projected_schema(&input_schema, projections));
    let sorting = sorting_columns(&out_schema, sort_key);
    let props = spec.writer_properties(sorting)?;

    let reader = builder.build().context("building input reader")?;
    let mut buf: Vec<u8> = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buf, out_schema.clone(), Some(props))
        .context("creating output writer")?;
    let mut output_rows = 0u64;
    for batch in reader {
        let batch = batch?;
        let projected = project_batch(&batch, projections)?;
        // Fold the rewritten rows under the declared logical schema — the invariant.
        logical
            .update(&projected, logical_schema)
            .map_err(|e| anyhow::anyhow!("{out_rel_path}: logical fingerprint: {e}"))?;
        output_rows += projected.num_rows() as u64;
        writer.write(&projected).context("writing batch")?;
    }
    writer.close().context("closing output writer")?;

    if output_rows != input_rows {
        bail!(
            "{out_rel_path}: wrote {output_rows} rows, input had {input_rows}; membership must be preserved"
        );
    }

    // Write the output file.
    let dst = out_dir.join(out_rel_path);
    if let Some(parent) = dst.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(&dst, &buf).with_context(|| format!("writing {}", dst.display()))?;

    // Read the footer back: this is what the writer actually did, not what was requested.
    let (columns, compressed_bytes, row_groups, footer_summary) = resolve_footer(&buf)?;

    let output_digest = FileDigest::of(out_rel_path.to_string(), &buf);
    Ok(RewrittenFile {
        map: VariantFileMap {
            input: input_digest,
            output: output_digest,
            input_rows,
            output_rows,
            footer_summary,
        },
        columns,
        compressed_bytes,
        row_groups,
    })
}

/// Read an output file's footer into per-column resolved facts plus a compact summary.
fn resolve_footer(
    bytes: &[u8],
) -> Result<(
    BTreeMap<String, ResolvedColumn>,
    i64,
    u32,
    serde_json::Value,
)> {
    let meta = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::copy_from_slice(bytes))?
        .metadata()
        .clone();
    let mut columns: BTreeMap<String, ResolvedColumn> = BTreeMap::new();
    let mut compressed_total = 0i64;
    for rg in meta.row_groups() {
        for col in rg.columns() {
            let name = col.column_path().string();
            let entry = columns.entry(name).or_default();
            for e in col.encodings() {
                entry.encodings.insert(format!("{e:?}"));
            }
            if col.dictionary_page_offset().is_some() {
                entry.dictionary = true;
            }
            if col.bloom_filter_offset().is_some() {
                entry.bloom = true;
            }
            entry.compression = format!("{:?}", col.compression());
            entry.compressed_bytes += col.compressed_size();
            compressed_total += col.compressed_size();
        }
    }
    let summary = serde_json::json!({
        "row_groups": meta.num_row_groups(),
        "compressed_bytes": compressed_total,
        "columns": columns.iter().map(|(name, c)| serde_json::json!({
            "column": name,
            "encodings": c.encodings.iter().cloned().collect::<Vec<_>>(),
            "dictionary": c.dictionary,
            "compression": c.compression,
            "compressed_bytes": c.compressed_bytes,
            "bloom": c.bloom,
        })).collect::<Vec<_>>(),
    });
    Ok((
        columns,
        compressed_total,
        meta.num_row_groups() as u32,
        summary,
    ))
}

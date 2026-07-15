//! Build one sidecar from a variant: for each indexed column and row group, build the
//! requested index, append its payload to a single blob, and record the offset/length/digest
//! in a `ukiel-parquet-skip/v1` manifest bound to the variant and every file digest.

use std::path::Path;

use anyhow::{Context, Result, bail};
use arrow::array::{Array, Int32Array, Int64Array, StringArray, StringViewArray};
use arrow::datatypes::DataType;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet_lab_contract::{
    FileDigest, IndexKind, IndexedColumn, RowGroupIndex, SKIP_MANIFEST_VERSION, SkipManifest,
    VariantManifest, digest_bytes,
};
use serde::Deserialize;

use crate::prefix_set::PrefixSet;
use crate::value_set::ValueSet;
use crate::zone_map::ZoneMap;
use crate::{Value, payload_digest};

/// The sidecar spec (TOML): which columns to index, how, and with what budget.
#[derive(Debug, Clone, Deserialize)]
pub struct SkipSpec {
    #[serde(default)]
    pub column: Vec<ColumnIndexSpec>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ColumnIndexSpec {
    pub name: String,
    /// `zone_map`, `value_set`, or `prefix_set`.
    pub kind: String,
    /// Payload-byte budget per row group for `value_set`/`prefix_set`.
    #[serde(default = "default_budget")]
    pub budget_bytes: usize,
    /// Prefix length for `prefix_set`.
    #[serde(default = "default_prefix_len")]
    pub prefix_len: usize,
}

fn default_budget() -> usize {
    4096
}
fn default_prefix_len() -> usize {
    8
}

fn index_kind(s: &str) -> Result<IndexKind> {
    Ok(match s {
        "zone_map" => IndexKind::ZoneMap,
        "value_set" => IndexKind::ValueSet,
        "prefix_set" => IndexKind::PrefixSet,
        other => bail!("unknown skip-index kind '{other}' (zone_map|value_set|prefix_set)"),
    })
}

/// Read one row group's column values as laboratory `Value`s (nulls as `None`). The event
/// and ClickBench schemas are flat, so a top-level column name is one leaf column.
fn column_values(bytes: &[u8], rg: usize, column: &str) -> Result<Vec<Option<Value>>> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::copy_from_slice(bytes))?;
    let leaf = builder
        .parquet_schema()
        .columns()
        .iter()
        .position(|c| c.name() == column)
        .ok_or_else(|| anyhow::anyhow!("column '{column}' not found"))?;
    let mask = parquet::arrow::ProjectionMask::leaves(builder.parquet_schema(), [leaf]);
    let reader = builder
        .with_row_groups(vec![rg])
        .with_projection(mask)
        .build()?;
    let mut out = Vec::new();
    for batch in reader {
        let batch = batch?;
        append_values(batch.column(0).as_ref(), &mut out)?;
    }
    Ok(out)
}

fn append_values(col: &dyn Array, out: &mut Vec<Option<Value>>) -> Result<()> {
    match col.data_type() {
        DataType::Int64 => {
            let a = col.as_any().downcast_ref::<Int64Array>().unwrap();
            for i in 0..a.len() {
                out.push((!a.is_null(i)).then(|| Value::Int(a.value(i))));
            }
        }
        DataType::Int32 => {
            let a = col.as_any().downcast_ref::<Int32Array>().unwrap();
            for i in 0..a.len() {
                out.push((!a.is_null(i)).then(|| Value::Int(a.value(i) as i64)));
            }
        }
        DataType::Utf8 => {
            let a = col.as_any().downcast_ref::<StringArray>().unwrap();
            for i in 0..a.len() {
                out.push((!a.is_null(i)).then(|| Value::Str(a.value(i).as_bytes().to_vec())));
            }
        }
        DataType::Utf8View => {
            let a = col.as_any().downcast_ref::<StringViewArray>().unwrap();
            for i in 0..a.len() {
                out.push((!a.is_null(i)).then(|| Value::Str(a.value(i).as_bytes().to_vec())));
            }
        }
        other => bail!("skip-index does not support column type {other:?}"),
    }
    Ok(())
}

/// Build the encoded payload for one row group, or `None` when the index cannot be built
/// (budget exceeded, wrong column type) — the group is then simply not indexed and
/// evaluation abstains for it.
fn build_payload(spec: &ColumnIndexSpec, values: &[Option<Value>]) -> Option<(IndexKind, Vec<u8>)> {
    match index_kind(&spec.kind).ok()? {
        IndexKind::ZoneMap => Some((IndexKind::ZoneMap, ZoneMap::build(values).encode())),
        IndexKind::ValueSet => {
            ValueSet::build(values, spec.budget_bytes).map(|v| (IndexKind::ValueSet, v.encode()))
        }
        IndexKind::PrefixSet => PrefixSet::build(values, spec.prefix_len, spec.budget_bytes)
            .map(|p| (IndexKind::PrefixSet, p.encode())),
    }
}

/// Build a sidecar and write it (skip.json + payload.bin) to `output_dir`.
pub fn build(
    variant_manifest_path: &Path,
    spec: &SkipSpec,
    output_dir: &Path,
    replace: bool,
) -> Result<std::path::PathBuf> {
    if output_dir.exists() {
        let non_empty = std::fs::read_dir(output_dir)
            .map(|mut d| d.next().is_some())
            .unwrap_or(false);
        if non_empty && !replace {
            bail!(
                "output '{}' already exists; pass --replace",
                output_dir.display()
            );
        }
        if replace {
            std::fs::remove_dir_all(output_dir).ok();
        }
    }
    std::fs::create_dir_all(output_dir)?;

    let dir = variant_manifest_path.parent().unwrap_or(Path::new("."));
    let vbytes = std::fs::read(variant_manifest_path)
        .with_context(|| format!("reading {}", variant_manifest_path.display()))?;
    let parent_variant_digest = digest_bytes(&vbytes);
    let variant = VariantManifest::parse(&variant_manifest_path.display().to_string(), &vbytes)?;

    // Every column/kind must be valid up front.
    for c in &spec.column {
        index_kind(&c.kind)?;
    }

    let started = std::time::Instant::now();
    let mut payload: Vec<u8> = Vec::new();
    let mut columns: Vec<IndexedColumn> = Vec::new();
    let mut file_digests: Vec<FileDigest> = Vec::new();

    for f in &variant.files {
        let fbytes = std::fs::read(dir.join(&f.output.path))
            .with_context(|| format!("reading {}", f.output.path))?;
        f.output.verify(&f.output.path, &fbytes)?;
        file_digests.push(f.output.clone());
        let meta =
            ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::copy_from_slice(&fbytes))?
                .metadata()
                .clone();
        let n_rg = meta.num_row_groups();

        for spec_col in &spec.column {
            let mut row_groups = Vec::new();
            let mut kind = index_kind(&spec_col.kind)?;
            for rg in 0..n_rg {
                let values = column_values(&fbytes, rg, &spec_col.name).with_context(|| {
                    format!("{}: rg {rg} column {}", f.output.path, spec_col.name)
                })?;
                let Some((k, bytes)) = build_payload(spec_col, &values) else {
                    continue; // group not indexed -> evaluation abstains
                };
                kind = k;
                let offset = payload.len() as u64;
                let digest = payload_digest(&bytes);
                payload.extend_from_slice(&bytes);
                row_groups.push(RowGroupIndex {
                    row_group: rg as u32,
                    kind: k,
                    parameters: serde_json::json!({
                        "budget_bytes": spec_col.budget_bytes,
                        "prefix_len": spec_col.prefix_len,
                    }),
                    payload_offset: offset,
                    payload_length: bytes.len() as u64,
                    payload_digest: digest,
                });
            }
            columns.push(IndexedColumn {
                column: spec_col.name.clone(),
                kind,
                kind_version: format!("{}/v1", spec_col.kind),
                file: f.output.path.clone(),
                row_groups,
            });
        }
    }

    let payload_path = "payload.bin".to_string();
    std::fs::write(output_dir.join(&payload_path), &payload).context("writing payload.bin")?;

    let manifest = SkipManifest {
        manifest_version: SKIP_MANIFEST_VERSION.to_string(),
        parent_variant_digest,
        file_digests,
        payload_path,
        columns,
        build_wall_ms: started.elapsed().as_millis() as u64,
        payload_bytes: payload.len() as u64,
    };
    let out = output_dir.join("skip.json");
    std::fs::write(&out, serde_json::to_vec_pretty(&manifest)?).context("writing skip.json")?;
    Ok(out)
}

impl SkipSpec {
    pub fn parse(bytes: &[u8], path: &str) -> Result<Self> {
        toml::from_str(std::str::from_utf8(bytes)?).map_err(|e| anyhow::anyhow!("{path}: {e}"))
    }
}

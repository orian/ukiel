//! Writer-property resolution and the pure Arrow-to-Parquet write.
//!
//! This is the single place that turns a resolved writer configuration into concrete
//! Parquet 58.3 `WriterProperties`, applies the lossless projection, writes logical
//! batches to a supplied sink while folding the logical fingerprint, and reads back
//! what the writer actually did from the output footer. It is shared by
//! `parquet-rewrite` (which publishes verified artifacts) and `parquet-write-bench`
//! (which times only this write), so the two can never disagree about what a config means.

use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use arrow::datatypes::Schema;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::{Compression, Encoding, ZstdLevel};
use parquet::file::metadata::SortingColumn;
use parquet::file::properties::{EnabledStatistics, WriterProperties, WriterPropertiesBuilder};
use parquet::schema::types::ColumnPath;
use parquet_lab_contract::ResolvedConfig;
use parquet_lab_integrity::{LogicalRowMultiset, LogicalSchema};

use crate::project::{PhysicalType, parse_physical_type, project_batch, projected_schema};

/// One column's writer overrides, resolved from a config.
#[derive(Debug, Clone, Default)]
pub struct ColumnConfig {
    pub name: String,
    pub encoding: Option<String>,
    pub dictionary: Option<bool>,
    pub compression: Option<String>,
    pub bloom_fpp: Option<f64>,
    pub bloom_ndv: Option<u64>,
    pub physical_type: Option<String>,
}

/// A resolved writer configuration, independent of any TOML or manifest shape. Both the
/// `parquet-rewrite` variant spec and a causal [`ResolvedConfig`] map convert into this.
#[derive(Debug, Clone)]
pub struct WriterConfig {
    pub row_group_rows: u64,
    pub key_boundary_flush: bool,
    pub write_batch_rows: u64,
    pub data_page_bytes: u64,
    pub dictionary_page_bytes: u64,
    pub statistics: String,
    pub offset_index: bool,
    pub compression: String,
    pub columns: Vec<ColumnConfig>,
}

impl WriterConfig {
    /// The physical-type projection this config requests, by column.
    pub fn projections(&self) -> Result<BTreeMap<String, PhysicalType>> {
        let mut m = BTreeMap::new();
        for c in &self.columns {
            if let Some(pt) = &c.physical_type {
                m.insert(c.name.clone(), parse_physical_type(pt)?);
            }
        }
        Ok(m)
    }

    /// Resolve to concrete Parquet writer properties, stamping the given sorting columns.
    pub fn writer_properties(&self, sorting: Vec<SortingColumn>) -> Result<WriterProperties> {
        let mut b: WriterPropertiesBuilder = WriterProperties::builder()
            .set_max_row_group_row_count(Some(self.row_group_rows as usize))
            .set_write_batch_size(self.write_batch_rows as usize)
            .set_data_page_size_limit(self.data_page_bytes as usize)
            .set_dictionary_page_size_limit(self.dictionary_page_bytes as usize)
            .set_statistics_enabled(parse_statistics(&self.statistics)?)
            .set_compression(parse_compression(&self.compression)?)
            .set_offset_index_disabled(!self.offset_index)
            .set_sorting_columns(Some(sorting));

        for c in &self.columns {
            let path = ColumnPath::from(c.name.clone());
            if let Some(enc) = &c.encoding {
                b = b.set_column_encoding(path.clone(), parse_encoding(enc)?);
            }
            if let Some(dict) = c.dictionary {
                b = b.set_column_dictionary_enabled(path.clone(), dict);
            }
            if let Some(codec) = &c.compression {
                b = b.set_column_compression(path.clone(), parse_compression(codec)?);
            }
            if let Some(fpp) = c.bloom_fpp {
                b = b
                    .set_column_bloom_filter_enabled(path.clone(), true)
                    .set_column_bloom_filter_fpp(path.clone(), fpp);
                if let Some(ndv) = c.bloom_ndv {
                    b = b.set_column_bloom_filter_ndv(path.clone(), ndv);
                }
            }
        }
        Ok(b.build())
    }

    /// Validate every codec/encoding/statistics/physical-type string up front, before any
    /// output is written — a laboratory never approximates an unsupported setting.
    pub fn validate(&self) -> Result<()> {
        parse_compression(&self.compression)?;
        parse_statistics(&self.statistics)?;
        for c in &self.columns {
            if let Some(codec) = &c.compression {
                parse_compression(codec)?;
            }
            if let Some(enc) = &c.encoding {
                parse_encoding(enc)?;
            }
            if let Some(pt) = &c.physical_type {
                parse_physical_type(pt)?;
            }
        }
        Ok(())
    }

    /// Build a [`WriterConfig`] from a canonical [`ResolvedConfig`] path→value map.
    pub fn from_resolved_config(config: &ResolvedConfig) -> Result<Self> {
        let f = &config.fields;
        let u64_of = |k: &str, d: u64| -> Result<u64> {
            match f.get(k) {
                None => Ok(d),
                Some(v) => v.as_u64().ok_or_else(|| {
                    anyhow::anyhow!("config field '{k}' is not an unsigned integer")
                }),
            }
        };
        let bool_of = |k: &str, d: bool| -> Result<bool> {
            match f.get(k) {
                None => Ok(d),
                Some(v) => v
                    .as_bool()
                    .ok_or_else(|| anyhow::anyhow!("config field '{k}' is not a boolean")),
            }
        };
        let str_of = |k: &str, d: &str| -> Result<String> {
            match f.get(k) {
                None => Ok(d.to_string()),
                Some(v) => v
                    .as_str()
                    .map(str::to_string)
                    .ok_or_else(|| anyhow::anyhow!("config field '{k}' is not a string")),
            }
        };
        let mut columns: BTreeMap<String, ColumnConfig> = BTreeMap::new();
        for (path, value) in f {
            let Some(rest) = path.strip_prefix("columns.") else {
                continue;
            };
            let Some((name, suffix)) = rest.rsplit_once('.') else {
                bail!("malformed column config path '{path}'");
            };
            let col = columns
                .entry(name.to_string())
                .or_insert_with(|| ColumnConfig {
                    name: name.to_string(),
                    ..Default::default()
                });
            match suffix {
                "encoding" => col.encoding = value.as_str().map(str::to_string),
                "dictionary" => col.dictionary = value.as_bool(),
                "compression" => col.compression = value.as_str().map(str::to_string),
                "bloom_fpp" => col.bloom_fpp = value.as_f64(),
                "bloom_ndv" => col.bloom_ndv = value.as_u64(),
                "physical_type" => col.physical_type = value.as_str().map(str::to_string),
                other => bail!("unknown per-column config suffix '{other}' in '{path}'"),
            }
        }
        let cfg = WriterConfig {
            row_group_rows: u64_of("global.row_group_rows", 1_048_576)?,
            key_boundary_flush: bool_of("global.key_boundary_flush", false)?,
            write_batch_rows: u64_of("global.write_batch_rows", 1024)?,
            data_page_bytes: u64_of("global.data_page_bytes", 1 << 20)?,
            dictionary_page_bytes: u64_of("global.dictionary_page_bytes", 1 << 20)?,
            statistics: str_of("global.statistics", "page")?,
            offset_index: bool_of("global.offset_index", true)?,
            compression: str_of("global.compression", "zstd(3)")?,
            columns: columns.into_values().collect(),
        };
        cfg.validate()?;
        Ok(cfg)
    }
}

/// Build Parquet sorting-column metadata for the sort key, matching the product's
/// nulls-first ascending order.
pub fn sorting_columns(schema: &Schema, sort_key: &[String]) -> Vec<SortingColumn> {
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

/// A single column's resolved footer facts, aggregated across a file's row groups.
#[derive(Debug, Clone, Default)]
pub struct ResolvedColumn {
    pub encodings: BTreeSet<String>,
    pub dictionary: bool,
    pub compression: String,
    pub compressed_bytes: i64,
    pub bloom: bool,
}

/// What one written file resolved to, read back from its footer.
pub struct WriteOutcome {
    pub output_rows: u64,
    pub columns: BTreeMap<String, ResolvedColumn>,
    pub compressed_bytes: i64,
    pub row_groups: u32,
    pub footer_summary: serde_json::Value,
}

/// Write the projected logical rows of one input file to `sink` under `props`, folding the
/// rewritten rows into `logical` under the declared schema. Returns nothing observable
/// about the bytes — the caller reads the sink back and calls [`resolve_footer`] — so a
/// timing harness can wrap exactly this call.
#[allow(clippy::too_many_arguments)]
pub fn write_projected<W: Write + Send>(
    sink: W,
    input_bytes: &[u8],
    projections: &BTreeMap<String, PhysicalType>,
    props: WriterProperties,
    out_schema: Arc<Schema>,
    logical_schema: &LogicalSchema,
    logical: &mut LogicalRowMultiset,
) -> Result<u64> {
    let builder =
        ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::copy_from_slice(input_bytes))
            .context("opening input")?;
    let reader = builder.build().context("building input reader")?;
    let mut writer =
        ArrowWriter::try_new(sink, out_schema, Some(props)).context("creating output writer")?;
    let mut output_rows = 0u64;
    for batch in reader {
        let batch = batch?;
        let projected = project_batch(&batch, projections)?;
        logical
            .update(&projected, logical_schema)
            .map_err(|e| anyhow::anyhow!("logical fingerprint: {e}"))?;
        output_rows += projected.num_rows() as u64;
        writer.write(&projected).context("writing batch")?;
    }
    writer.close().context("closing output writer")?;
    Ok(output_rows)
}

/// Decode every batch of an input file into memory, returning them and the input schema.
/// A timing harness calls this *outside* the clock so only the write is measured.
pub fn decode_batches(input_bytes: &[u8]) -> Result<(Vec<arrow::array::RecordBatch>, Schema)> {
    let builder =
        ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::copy_from_slice(input_bytes))
            .context("opening input")?;
    let schema = builder.schema().as_ref().clone();
    let reader = builder.build().context("building input reader")?;
    let mut batches = Vec::new();
    for b in reader {
        batches.push(b?);
    }
    Ok((batches, schema))
}

/// Apply the physical-type projection to already-decoded batches (outside the clock).
pub fn project_batches(
    batches: &[arrow::array::RecordBatch],
    projections: &BTreeMap<String, PhysicalType>,
) -> Result<Vec<arrow::array::RecordBatch>> {
    batches
        .iter()
        .map(|b| project_batch(b, projections))
        .collect()
}

/// Write already-decoded, already-projected batches to a sink under `props`. This is the
/// single call a writer benchmark wraps with its clock — no decode, no projection, no
/// fingerprint fold happens inside it.
pub fn write_prepared<W: Write + Send>(
    sink: W,
    batches: &[arrow::array::RecordBatch],
    out_schema: Arc<Schema>,
    props: WriterProperties,
) -> Result<u64> {
    let mut writer =
        ArrowWriter::try_new(sink, out_schema, Some(props)).context("creating output writer")?;
    let mut rows = 0u64;
    for b in batches {
        rows += b.num_rows() as u64;
        writer.write(b).context("writing batch")?;
    }
    writer.close().context("closing output writer")?;
    Ok(rows)
}

/// Open an input file's schema and declared row count without decoding it.
pub fn input_schema_and_rows(input_bytes: &[u8]) -> Result<(Schema, u64)> {
    let builder =
        ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::copy_from_slice(input_bytes))
            .context("opening input")?;
    let schema = builder.schema().as_ref().clone();
    let rows = builder.metadata().file_metadata().num_rows() as u64;
    Ok((schema, rows))
}

/// Compute the projected output schema for a set of physical-type projections.
pub fn out_schema_for(
    input_schema: &Schema,
    projections: &BTreeMap<String, PhysicalType>,
) -> Schema {
    projected_schema(input_schema, projections)
}

/// Read an output file's footer into per-column resolved facts plus a compact summary.
pub fn resolve_footer(bytes: &[u8]) -> Result<WriteOutcome> {
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
    Ok(WriteOutcome {
        output_rows: meta.file_metadata().num_rows() as u64,
        columns,
        compressed_bytes: compressed_total,
        row_groups: meta.num_row_groups() as u32,
        footer_summary: summary,
    })
}

// -- codec/encoding/statistics string resolution ----------------------------

pub fn parse_statistics(s: &str) -> Result<EnabledStatistics> {
    Ok(match s {
        "none" => EnabledStatistics::None,
        "chunk" => EnabledStatistics::Chunk,
        "page" => EnabledStatistics::Page,
        other => bail!("unsupported statistics mode '{other}' (none|chunk|page)"),
    })
}

pub fn parse_compression(s: &str) -> Result<Compression> {
    let s = s.trim().to_lowercase();
    if s == "lz4_raw" {
        return Ok(Compression::LZ4_RAW);
    }
    if s == "snappy" {
        return Ok(Compression::SNAPPY);
    }
    if s == "uncompressed" {
        return Ok(Compression::UNCOMPRESSED);
    }
    if s == "zstd" {
        return Ok(Compression::ZSTD(ZstdLevel::try_new(3)?));
    }
    if let Some(rest) = s.strip_prefix("zstd(").and_then(|r| r.strip_suffix(')')) {
        let level: i32 = rest
            .parse()
            .map_err(|_| anyhow::anyhow!("bad zstd level '{rest}'"))?;
        return Ok(Compression::ZSTD(ZstdLevel::try_new(level)?));
    }
    bail!("unsupported compression '{s}' (zstd(N)|lz4_raw|snappy|uncompressed)")
}

pub fn parse_encoding(s: &str) -> Result<Encoding> {
    Ok(match s {
        "plain" => Encoding::PLAIN,
        "delta_binary_packed" => Encoding::DELTA_BINARY_PACKED,
        "delta_length_byte_array" => Encoding::DELTA_LENGTH_BYTE_ARRAY,
        "delta_byte_array" => Encoding::DELTA_BYTE_ARRAY,
        "byte_stream_split" => Encoding::BYTE_STREAM_SPLIT,
        "rle" => Encoding::RLE,
        other => bail!("unsupported encoding '{other}'"),
    })
}

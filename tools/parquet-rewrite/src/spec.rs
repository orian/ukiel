//! The variant spec: a versioned TOML declaration of the pinned Parquet 58.3 controls the
//! matrix varies, resolved into concrete writer properties.
//!
//! An invalid type/encoding/codec combination is a validation error *before* any output is
//! written — a laboratory never approximates an unsupported setting with a different one.

use std::collections::BTreeMap;

use anyhow::{Result, bail};
use parquet::basic::{Compression, Encoding, ZstdLevel};
use parquet::file::properties::{EnabledStatistics, WriterProperties, WriterPropertiesBuilder};
use parquet::schema::types::ColumnPath;
use serde::Deserialize;

/// One variant spec, parsed from TOML.
#[derive(Debug, Clone, Deserialize)]
pub struct VariantSpec {
    /// A human label, unique within a matrix block.
    pub label: String,
    pub row_group_rows: u64,
    /// Whether row groups flush at packing-key boundaries. Recorded; the single-file
    /// rewrite preserves membership, so this influences row-group cutting, not files.
    #[serde(default)]
    pub key_boundary_flush: bool,
    #[serde(default = "default_batch")]
    pub write_batch_rows: u64,
    #[serde(default = "default_data_page")]
    pub data_page_bytes: u64,
    #[serde(default = "default_dict_page")]
    pub dictionary_page_bytes: u64,
    /// `none`, `chunk`, or `page`.
    #[serde(default = "default_stats")]
    pub statistics: String,
    #[serde(default = "default_true")]
    pub offset_index: bool,
    /// Whole-file compression: `zstd(N)`, `lz4_raw`, `snappy`, `uncompressed`.
    #[serde(default = "default_compression")]
    pub compression: String,
    #[serde(default)]
    pub column: Vec<ColumnSpec>,
}

fn default_batch() -> u64 {
    1024
}
fn default_data_page() -> u64 {
    1 << 20
}
fn default_dict_page() -> u64 {
    1 << 20
}
fn default_stats() -> String {
    "page".to_string()
}
fn default_true() -> bool {
    true
}
fn default_compression() -> String {
    "zstd(3)".to_string()
}

/// Per-column overrides. A column absent here inherits the global properties.
#[derive(Debug, Clone, Deserialize)]
pub struct ColumnSpec {
    pub name: String,
    pub encoding: Option<String>,
    pub dictionary: Option<bool>,
    pub compression: Option<String>,
    pub bloom_fpp: Option<f64>,
    pub bloom_ndv: Option<u64>,
    /// A declared lossless physical-type projection, e.g. `int32`.
    pub physical_type: Option<String>,
}

impl VariantSpec {
    pub fn parse(bytes: &[u8], path: &str) -> Result<Self> {
        let text = std::str::from_utf8(bytes)?;
        let spec: VariantSpec = toml::from_str(text).map_err(|e| anyhow::anyhow!("{path}: {e}"))?;
        if spec.label.trim().is_empty() {
            bail!("{path}: spec has no label");
        }
        // Validate every codec/encoding/stats string up front, before any output.
        parse_compression(&spec.compression)?;
        parse_statistics(&spec.statistics)?;
        for c in &spec.column {
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
        Ok(spec)
    }

    /// The physical-type projection this spec requests, by column.
    pub fn projections(&self) -> Result<BTreeMap<String, PhysicalType>> {
        let mut m = BTreeMap::new();
        for c in &self.column {
            if let Some(pt) = &c.physical_type {
                m.insert(c.name.clone(), parse_physical_type(pt)?);
            }
        }
        Ok(m)
    }

    /// Resolve to concrete Parquet writer properties.
    pub fn writer_properties(
        &self,
        sorting: Vec<parquet::file::metadata::SortingColumn>,
    ) -> Result<WriterProperties> {
        let mut b: WriterPropertiesBuilder = WriterProperties::builder()
            .set_max_row_group_row_count(Some(self.row_group_rows as usize))
            .set_write_batch_size(self.write_batch_rows as usize)
            .set_data_page_size_limit(self.data_page_bytes as usize)
            .set_dictionary_page_size_limit(self.dictionary_page_bytes as usize)
            .set_statistics_enabled(parse_statistics(&self.statistics)?)
            .set_compression(parse_compression(&self.compression)?)
            .set_offset_index_disabled(!self.offset_index)
            .set_sorting_columns(Some(sorting));

        for c in &self.column {
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
}

/// A lossless physical-type projection target.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PhysicalType {
    Int8,
    Int16,
    Int32,
    Int64,
    /// Millisecond timestamp, physical Arrow `Timestamp(Millisecond, None)`.
    TimestampMillis,
    /// Day-epoch date, physical Arrow `Date32`.
    Date32,
}

fn parse_physical_type(s: &str) -> Result<PhysicalType> {
    Ok(match s {
        "int8" => PhysicalType::Int8,
        "int16" => PhysicalType::Int16,
        "int32" => PhysicalType::Int32,
        "int64" => PhysicalType::Int64,
        "timestamp_ms" | "timestamp_millis" => PhysicalType::TimestampMillis,
        "date32" | "date" => PhysicalType::Date32,
        other => bail!("unsupported physical type '{other}'"),
    })
}

fn parse_statistics(s: &str) -> Result<EnabledStatistics> {
    Ok(match s {
        "none" => EnabledStatistics::None,
        "chunk" => EnabledStatistics::Chunk,
        "page" => EnabledStatistics::Page,
        other => bail!("unsupported statistics mode '{other}' (none|chunk|page)"),
    })
}

fn parse_compression(s: &str) -> Result<Compression> {
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

fn parse_encoding(s: &str) -> Result<Encoding> {
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

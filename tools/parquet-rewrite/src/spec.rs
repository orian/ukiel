//! The variant spec: a versioned TOML declaration of the pinned Parquet 58.3 controls the
//! matrix varies. Property *resolution* lives in the shared `parquet-lab-write-core`; this
//! file is just the TOML shape and its up-front validation.
//!
//! An invalid type/encoding/codec combination is a validation error *before* any output is
//! written — a laboratory never approximates an unsupported setting with a different one.

use std::collections::BTreeMap;

use anyhow::{Result, bail};
use parquet::file::metadata::SortingColumn;
use parquet::file::properties::WriterProperties;
use parquet_lab_write_core::{ColumnConfig, WriterConfig};
use serde::{Deserialize, Serialize};

// The physical-type projection target now lives in the shared write core.
pub use parquet_lab_write_core::PhysicalType;

/// One variant spec, parsed from TOML.
#[derive(Debug, Clone, Deserialize, Serialize)]
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
#[derive(Debug, Clone, Deserialize, Serialize)]
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
        // Validate every codec/encoding/stats/physical-type string up front, before output.
        spec.to_writer_config()
            .validate()
            .map_err(|e| anyhow::anyhow!("{path}: {e}"))?;
        Ok(spec)
    }

    /// Convert to the shared resolved writer configuration.
    pub fn to_writer_config(&self) -> WriterConfig {
        WriterConfig {
            row_group_rows: self.row_group_rows,
            key_boundary_flush: self.key_boundary_flush,
            write_batch_rows: self.write_batch_rows,
            data_page_bytes: self.data_page_bytes,
            dictionary_page_bytes: self.dictionary_page_bytes,
            statistics: self.statistics.clone(),
            offset_index: self.offset_index,
            compression: self.compression.clone(),
            columns: self
                .column
                .iter()
                .map(|c| ColumnConfig {
                    name: c.name.clone(),
                    encoding: c.encoding.clone(),
                    dictionary: c.dictionary,
                    compression: c.compression.clone(),
                    bloom_fpp: c.bloom_fpp,
                    bloom_ndv: c.bloom_ndv,
                    physical_type: c.physical_type.clone(),
                })
                .collect(),
        }
    }

    /// The physical-type projection this spec requests, by column.
    pub fn projections(&self) -> Result<BTreeMap<String, PhysicalType>> {
        self.to_writer_config().projections()
    }

    /// Resolve to concrete Parquet writer properties.
    pub fn writer_properties(&self, sorting: Vec<SortingColumn>) -> Result<WriterProperties> {
        self.to_writer_config().writer_properties(sorting)
    }
}

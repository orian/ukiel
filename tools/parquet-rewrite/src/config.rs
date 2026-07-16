//! Canonical writer-config path↔spec conversion for the causal (reconstruction/vary)
//! rewrite modes.
//!
//! The causal contracts compare a reconstruction and its one-variable child at a
//! single altitude: a flat, sorted `path -> value` map (`ResolvedConfig`). This module
//! is the only place that translates between that map and the concrete
//! [`VariantSpec`] the writer consumes, so the structural diff and the writer never
//! disagree about what a config means.

use std::collections::BTreeMap;

use anyhow::{Result, bail};
use parquet_lab_contract::ResolvedConfig;

use crate::spec::{ColumnSpec, VariantSpec};

/// Build the canonical `path -> value` map from a resolved spec. Only per-column fields
/// that were actually set appear, so an unset column property never pollutes the diff.
pub fn spec_to_config(spec: &VariantSpec) -> ResolvedConfig {
    let mut m: BTreeMap<String, serde_json::Value> = BTreeMap::new();
    m.insert("global.row_group_rows".into(), spec.row_group_rows.into());
    m.insert("global.key_boundary_flush".into(), spec.key_boundary_flush.into());
    m.insert("global.write_batch_rows".into(), spec.write_batch_rows.into());
    m.insert("global.data_page_bytes".into(), spec.data_page_bytes.into());
    m.insert("global.dictionary_page_bytes".into(), spec.dictionary_page_bytes.into());
    m.insert("global.statistics".into(), spec.statistics.clone().into());
    m.insert("global.offset_index".into(), spec.offset_index.into());
    m.insert("global.compression".into(), spec.compression.clone().into());
    for c in &spec.column {
        let p = |suffix: &str| format!("columns.{}.{}", c.name, suffix);
        if let Some(v) = &c.encoding {
            m.insert(p("encoding"), v.clone().into());
        }
        if let Some(v) = c.dictionary {
            m.insert(p("dictionary"), v.into());
        }
        if let Some(v) = &c.compression {
            m.insert(p("compression"), v.clone().into());
        }
        if let Some(v) = c.bloom_fpp {
            m.insert(p("bloom_fpp"), v.into());
        }
        if let Some(v) = c.bloom_ndv {
            m.insert(p("bloom_ndv"), v.into());
        }
        if let Some(v) = &c.physical_type {
            m.insert(p("physical_type"), v.clone().into());
        }
    }
    ResolvedConfig::new(m)
}

/// Rebuild a [`VariantSpec`] from a canonical config map plus a label. The inverse of
/// [`spec_to_config`], used by `vary` to reconstitute the writer configuration from a
/// reconstruction manifest and its applied delta.
pub fn config_to_spec(label: &str, config: &ResolvedConfig) -> Result<VariantSpec> {
    let f = &config.fields;
    let get_u64 = |k: &str, default: u64| -> Result<u64> {
        match f.get(k) {
            None => Ok(default),
            Some(v) => v
                .as_u64()
                .ok_or_else(|| anyhow::anyhow!("config field '{k}' is not an unsigned integer")),
        }
    };
    let get_bool = |k: &str, default: bool| -> Result<bool> {
        match f.get(k) {
            None => Ok(default),
            Some(v) => v
                .as_bool()
                .ok_or_else(|| anyhow::anyhow!("config field '{k}' is not a boolean")),
        }
    };
    let get_str = |k: &str, default: &str| -> Result<String> {
        match f.get(k) {
            None => Ok(default.to_string()),
            Some(v) => v
                .as_str()
                .map(|s| s.to_string())
                .ok_or_else(|| anyhow::anyhow!("config field '{k}' is not a string")),
        }
    };

    // Group per-column fields by column name.
    let mut columns: BTreeMap<String, ColumnSpec> = BTreeMap::new();
    for (path, value) in f {
        let Some(rest) = path.strip_prefix("columns.") else {
            continue;
        };
        let Some((name, suffix)) = rest.rsplit_once('.') else {
            bail!("malformed column config path '{path}'");
        };
        let col = columns.entry(name.to_string()).or_insert_with(|| ColumnSpec {
            name: name.to_string(),
            encoding: None,
            dictionary: None,
            compression: None,
            bloom_fpp: None,
            bloom_ndv: None,
            physical_type: None,
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

    let spec = VariantSpec {
        label: label.to_string(),
        row_group_rows: get_u64("global.row_group_rows", 1_048_576)?,
        key_boundary_flush: get_bool("global.key_boundary_flush", false)?,
        write_batch_rows: get_u64("global.write_batch_rows", 1024)?,
        data_page_bytes: get_u64("global.data_page_bytes", 1 << 20)?,
        dictionary_page_bytes: get_u64("global.dictionary_page_bytes", 1 << 20)?,
        statistics: get_str("global.statistics", "page")?,
        offset_index: get_bool("global.offset_index", true)?,
        compression: get_str("global.compression", "zstd(3)")?,
        column: columns.into_values().collect(),
    };
    // Re-run the spec's own validation (codecs, encodings, physical types).
    VariantSpec::parse(&toml_of(&spec)?, label)?;
    Ok(spec)
}

/// Serialize a spec back to TOML so `config_to_spec` can reuse `VariantSpec::parse`'s
/// validation without duplicating it.
fn toml_of(spec: &VariantSpec) -> Result<Vec<u8>> {
    Ok(toml::to_string(spec)?.into_bytes())
}

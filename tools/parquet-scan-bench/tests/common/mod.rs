//! Test helper: build a multi-file, multi-row-group reconstruction artifact with strings,
//! nulls, and a fixed-width key, plus a workload binding over its roles.

#![allow(dead_code)]

use std::path::Path;
use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use parquet_lab_contract::{
    FileDigest, Fingerprint, ReconstructionManifest, ResolvedConfig, RewriteBias, VariantFileMap,
    WorkloadBinding,
};

pub fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("team_id", DataType::Int64, false),
        Field::new("url", DataType::Utf8, true),
        Field::new("body", DataType::Utf8, true),
    ]))
}

fn batch(base: i64, n: usize) -> RecordBatch {
    let teams: Vec<i64> = (0..n as i64).map(|i| base + i).collect();
    let urls: Vec<Option<String>> = (0..n)
        .map(|i| if i % 9 == 0 { None } else { Some(format!("http://h/{}/{i}", base)) })
        .collect();
    let body: Vec<Option<String>> = (0..n)
        .map(|i| Some(format!("body text number {} for row {i} lorem ipsum", base + i as i64)))
        .collect();
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(teams)),
            Arc::new(StringArray::from(urls)),
            Arc::new(StringArray::from(body)),
        ],
    )
    .unwrap()
}

/// Write a reconstruction with `files` files, each holding `rows_per_file` rows cut into
/// row groups of `rg_rows`. Returns the manifest path.
pub fn write_reconstruction(
    dir: &Path,
    files: usize,
    rows_per_file: usize,
    rg_rows: usize,
    compression: &str,
) -> std::path::PathBuf {
    // Deterministic logical fingerprint across all files (physical types only, no projection).
    let mut file_maps = Vec::new();
    for fi in 0..files {
        let rel = format!("parquet/part-{fi:05}.parquet");
        let path = dir.join(&rel);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(rg_rows))
            .build();
        let file = std::fs::File::create(&path).unwrap();
        let mut w = ArrowWriter::try_new(file, schema(), Some(props)).unwrap();
        let b = batch((fi * rows_per_file) as i64, rows_per_file);
        w.write(&b).unwrap();
        w.close().unwrap();
        let bytes = std::fs::read(&path).unwrap();
        file_maps.push(VariantFileMap {
            input: FileDigest::of(format!("product/part-{fi:05}.parquet"), b"in"),
            output: FileDigest::of(&rel, &bytes),
            input_rows: rows_per_file as u64,
            output_rows: rows_per_file as u64,
            footer_summary: serde_json::json!({}),
        });
    }
    let mut proj = serde_json::Map::new();
    proj.insert("team_id".into(), serde_json::json!("int64"));
    proj.insert("url".into(), serde_json::json!("utf8"));
    proj.insert("body".into(), serde_json::json!("utf8"));
    let m = ReconstructionManifest {
        reconstruction_version: parquet_lab_contract::RECONSTRUCTION_VERSION.into(),
        parent_product_digest: "prod".repeat(16),
        baseline_spec_digest: "ba5e".repeat(16),
        label: "reconstruction".into(),
        requested_config: cfg(compression),
        resolved_config: cfg(compression),
        logical_projection: proj,
        sort_key: vec!["team_id".into()],
        physical_schema: serde_json::json!({"fields": [
            {"name": "team_id"}, {"name": "url"}, {"name": "body"}
        ]}),
        logical_fingerprint: Fingerprint {
            version: "logical-row-multiset/v1".into(),
            count: (files * rows_per_file) as u64,
            xor: "00".repeat(32),
            sum: [0; 4],
            digest: "00".repeat(32),
        },
        files: file_maps,
        input_census: serde_json::json!({}),
        output_census: serde_json::json!({}),
        rewrite_bias: RewriteBias {
            product_total_bytes: 0,
            reconstruction_total_bytes: 0,
            per_column: serde_json::json!({}),
        },
    };
    let mp = dir.join("manifest.json");
    std::fs::write(&mp, serde_json::to_vec_pretty(&m).unwrap()).unwrap();
    mp
}

fn cfg(compression: &str) -> ResolvedConfig {
    let mut m = std::collections::BTreeMap::new();
    m.insert("global.compression".into(), serde_json::json!(compression));
    ResolvedConfig::new(m)
}

/// Write a workload binding mapping roles onto this schema's columns.
pub fn write_workload(dir: &Path) -> std::path::PathBuf {
    let mut roles = serde_json::Map::new();
    roles.insert("fixed_width_key".into(), serde_json::json!("team_id"));
    roles.insert("high_cardinality_string".into(), serde_json::json!("url"));
    roles.insert("wide_text".into(), serde_json::json!("body"));
    let w = WorkloadBinding {
        dataset_id: "test".into(),
        roles,
        hot_columns: vec!["team_id".into(), "url".into()],
    };
    let p = dir.join("workload.json");
    std::fs::write(&p, serde_json::to_vec_pretty(&w).unwrap()).unwrap();
    p
}

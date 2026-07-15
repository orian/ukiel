//! Test helpers: write Parquet files with controlled writer properties and wrap them in a
//! hand-built snapshot manifest so the census can be driven without the snapshot tool.

#![allow(dead_code)]

use std::path::Path;
use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use parquet_lab_contract::{
    FileDigest, Fingerprint, LogicalProjection, SNAPSHOT_MANIFEST_VERSION, SnapshotFile,
    SnapshotManifest, SourceKind, digest_bytes,
};

pub fn event_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("team_id", DataType::Int64, false),
        Field::new("timestamp", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("value", DataType::Float64, true),
    ]))
}

/// Build a batch of `n` rows: team_id sorted with runs, ts increasing, name high-NDV with
/// some nulls, value a float.
pub fn event_batch(n: usize, team_base: i64) -> RecordBatch {
    let teams: Vec<i64> = (0..n as i64).map(|i| team_base + i / 4).collect();
    let ts: Vec<i64> = (0..n as i64)
        .map(|i| 1_782_864_000_000 + i * 1000)
        .collect();
    let names: Vec<Option<String>> = (0..n)
        .map(|i| {
            if i % 5 == 0 {
                None
            } else {
                Some(format!("evt-{i}-{team_base}"))
            }
        })
        .collect();
    let vals: Vec<Option<f64>> = (0..n).map(|i| Some(i as f64 * 1.25)).collect();
    RecordBatch::try_new(
        event_schema(),
        vec![
            Arc::new(Int64Array::from(teams)),
            Arc::new(Int64Array::from(ts)),
            Arc::new(StringArray::from(names)),
            Arc::new(Float64Array::from(vals)),
        ],
    )
    .unwrap()
}

/// Write one Parquet file with the given properties, one batch per row-group boundary.
pub fn write_parquet(path: &Path, props: WriterProperties, batches: &[RecordBatch]) -> (u64, u64) {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    let file = std::fs::File::create(path).unwrap();
    let mut writer = ArrowWriter::try_new(file, event_schema(), Some(props)).unwrap();
    let mut rows = 0u64;
    for b in batches {
        rows += b.num_rows() as u64;
        writer.write(b).unwrap();
    }
    writer.close().unwrap();
    let bytes = std::fs::metadata(path).unwrap().len();
    (rows, bytes)
}

/// The event logical projection.
pub fn projection() -> LogicalProjection {
    let mut m = serde_json::Map::new();
    m.insert("team_id".into(), serde_json::json!("int64"));
    m.insert("timestamp".into(), serde_json::json!("timestamp_ms"));
    m.insert("name".into(), serde_json::json!("utf8"));
    m.insert("value".into(), serde_json::json!("float64"));
    LogicalProjection { logical_types: m }
}

/// Build and write a snapshot manifest referencing the given (relative path, rows) files.
pub fn write_manifest(dir: &Path, files: &[(String, u64)]) -> std::path::PathBuf {
    let mut snap_files = Vec::new();
    let mut total_rows = 0u64;
    let mut total_bytes = 0u64;
    for (rel, rows) in files {
        let bytes = std::fs::read(dir.join(rel)).unwrap();
        total_rows += rows;
        total_bytes += bytes.len() as u64;
        snap_files.push(SnapshotFile {
            path: rel.clone(),
            object_key: None,
            digest: digest_bytes(&bytes),
            bytes: bytes.len() as u64,
            rows: *rows,
            row_groups: 0,
            source_part: None,
        });
    }
    let manifest = SnapshotManifest {
        manifest_version: SNAPSHOT_MANIFEST_VERSION.into(),
        source_kind: SourceKind::ExplicitFiles,
        source_digests: vec![FileDigest::of("schema.json", b"{}")],
        tool_versions: parquet_lab_contract::ToolVersions {
            git_sha: "test".into(),
            arrow: "58.3".into(),
            parquet: "58.3".into(),
            datafusion: "54".into(),
        },
        creation_command: "test".into(),
        logical_schema: serde_json::json!({}),
        physical_schema: serde_json::json!({}),
        packing_key: "team_id".into(),
        sort_key: vec!["team_id".into(), "timestamp".into()],
        logical_projection: Some(projection()),
        files: snap_files,
        total_rows,
        total_bytes,
        physical_fingerprint: None,
        logical_fingerprint: Fingerprint {
            version: parquet_lab_contract::LOGICAL_ROW_MULTISET_VERSION.into(),
            count: total_rows,
            xor: "00".repeat(32),
            sum: [0; 4],
            digest: "00".repeat(32),
        },
        disclaimer: None,
    };
    let mp = dir.join("manifest.json");
    std::fs::write(&mp, serde_json::to_vec_pretty(&manifest).unwrap()).unwrap();
    mp
}

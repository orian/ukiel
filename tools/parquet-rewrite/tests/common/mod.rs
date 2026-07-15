//! Test helpers: build an event snapshot (Parquet files + a manifest carrying the real
//! logical fingerprint) so the rewriter can be driven without the snapshot tool.

#![allow(dead_code)]

use std::path::Path;
use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use parquet_lab_contract::{
    Fingerprint, LogicalProjection, SNAPSHOT_MANIFEST_VERSION, SnapshotFile, SnapshotManifest,
    SourceKind, digest_bytes,
};
use parquet_lab_integrity::{LogicalColumn, LogicalRowMultiset, LogicalSchema, LogicalType};

pub fn event_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("team_id", DataType::Int64, false),
        Field::new("timestamp", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("value", DataType::Float64, true),
    ]))
}

/// A batch whose team_id values fit i32 (so int32 narrowing is lossless).
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
                Some(format!("evt-{i}"))
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

fn logical_schema() -> LogicalSchema {
    LogicalSchema::new(vec![
        LogicalColumn {
            name: "team_id".into(),
            logical: LogicalType::SignedInt,
        },
        LogicalColumn {
            name: "timestamp".into(),
            logical: LogicalType::TimestampMillis,
        },
        LogicalColumn {
            name: "name".into(),
            logical: LogicalType::Utf8,
        },
        LogicalColumn {
            name: "value".into(),
            logical: LogicalType::Float64,
        },
    ])
}

fn projection() -> LogicalProjection {
    let mut m = serde_json::Map::new();
    m.insert("team_id".into(), serde_json::json!("int64"));
    m.insert("timestamp".into(), serde_json::json!("timestamp_ms"));
    m.insert("name".into(), serde_json::json!("utf8"));
    m.insert("value".into(), serde_json::json!("float64"));
    LogicalProjection { logical_types: m }
}

fn physical_schema_json() -> serde_json::Value {
    serde_json::json!({
        "fields": [
            {"name": "team_id", "type": "Int64"},
            {"name": "timestamp", "type": "Int64"},
            {"name": "name", "type": "Utf8"},
            {"name": "value", "type": "Float64"}
        ]
    })
}

/// Write a one-file event snapshot with the given batches and return the manifest path.
pub fn write_snapshot(dir: &Path, batches: &[RecordBatch]) -> std::path::PathBuf {
    let rel = "parquet/part-00000.parquet";
    let path = dir.join(rel);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let file = std::fs::File::create(&path).unwrap();
    let mut writer = ArrowWriter::try_new(
        file,
        event_schema(),
        Some(WriterProperties::builder().build()),
    )
    .unwrap();
    let mut rows = 0u64;
    let mut logical = LogicalRowMultiset::default();
    let schema = logical_schema();
    for b in batches {
        rows += b.num_rows() as u64;
        logical.update(b, &schema).unwrap();
        writer.write(b).unwrap();
    }
    writer.close().unwrap();

    let bytes = std::fs::read(&path).unwrap();
    let fingerprint = Fingerprint {
        version: parquet_lab_integrity::LOGICAL_ROW_MULTISET_VERSION.into(),
        count: logical.count,
        xor: logical.xor_hex(),
        sum: logical.sum,
        digest: logical.digest_hex(),
    };
    let manifest = SnapshotManifest {
        manifest_version: SNAPSHOT_MANIFEST_VERSION.into(),
        source_kind: SourceKind::ExplicitFiles,
        source_digests: vec![],
        tool_versions: parquet_lab_contract::ToolVersions {
            git_sha: "test".into(),
            arrow: "58.3".into(),
            parquet: "58.3".into(),
            datafusion: "54".into(),
        },
        creation_command: "test".into(),
        logical_schema: serde_json::json!({}),
        physical_schema: physical_schema_json(),
        packing_key: "team_id".into(),
        sort_key: vec!["team_id".into(), "timestamp".into()],
        logical_projection: Some(projection()),
        files: vec![SnapshotFile {
            path: rel.into(),
            object_key: None,
            digest: digest_bytes(&bytes),
            bytes: bytes.len() as u64,
            rows,
            row_groups: 1,
            source_part: None,
        }],
        total_rows: rows,
        total_bytes: bytes.len() as u64,
        physical_fingerprint: None,
        logical_fingerprint: fingerprint,
        disclaimer: None,
    };
    let mp = dir.join("manifest.json");
    std::fs::write(&mp, serde_json::to_vec_pretty(&manifest).unwrap()).unwrap();
    mp
}

/// Write a spec TOML and return its path.
pub fn write_spec(dir: &Path, name: &str, toml: &str) -> std::path::PathBuf {
    let p = dir.join(name);
    std::fs::write(&p, toml).unwrap();
    p
}

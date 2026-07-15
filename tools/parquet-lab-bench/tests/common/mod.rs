//! Test helpers: build event snapshots (optionally with a narrowed physical team_id) whose
//! manifests carry the real logical fingerprint, so the bench can be driven without the
//! snapshot or rewrite tools.

#![allow(dead_code)]

use std::path::Path;
use std::sync::Arc;

use arrow::array::{Float64Array, Int32Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use parquet_lab_contract::{
    Fingerprint, LogicalProjection, SNAPSHOT_MANIFEST_VERSION, SnapshotFile, SnapshotManifest,
    SourceKind, digest_bytes,
};
use parquet_lab_integrity::{LogicalColumn, LogicalRowMultiset, LogicalSchema, LogicalType};

fn schema(narrow_team: bool) -> Arc<Schema> {
    let team_type = if narrow_team {
        DataType::Int32
    } else {
        DataType::Int64
    };
    Arc::new(Schema::new(vec![
        Field::new("team_id", team_type, false),
        Field::new("timestamp", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("value", DataType::Float64, true),
    ]))
}

/// A batch of `n` rows. `shift` perturbs the values so two snapshots can differ.
pub fn batch(n: usize, narrow_team: bool, shift: i64) -> RecordBatch {
    let teams: Vec<i64> = (0..n as i64).map(|i| 1 + (i / 4) + shift).collect();
    let ts: Vec<i64> = (0..n as i64)
        .map(|i| 1_782_864_000_000 + i * 1000)
        .collect();
    let names: Vec<Option<String>> = (0..n)
        .map(|i| {
            if i % 5 == 0 {
                None
            } else {
                Some(format!("evt-{}", i % 7))
            }
        })
        .collect();
    let vals: Vec<Option<f64>> = (0..n).map(|i| Some(i as f64 * 1.25)).collect();
    let team_arr: Arc<dyn arrow::array::Array> = if narrow_team {
        Arc::new(Int32Array::from(
            teams.iter().map(|&t| t as i32).collect::<Vec<_>>(),
        ))
    } else {
        Arc::new(Int64Array::from(teams))
    };
    RecordBatch::try_new(
        schema(narrow_team),
        vec![
            team_arr,
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

fn physical_schema_json(narrow_team: bool) -> serde_json::Value {
    let team = if narrow_team { "Int32" } else { "Int64" };
    serde_json::json!({
        "fields": [
            {"name": "team_id", "type": team},
            {"name": "timestamp", "type": "Int64"},
            {"name": "name", "type": "Utf8"},
            {"name": "value", "type": "Float64"}
        ]
    })
}

/// Write a one-file event snapshot and return the manifest path. `narrow_team` stores
/// team_id physically as Int32; `shift` perturbs values.
pub fn write_snapshot(dir: &Path, narrow_team: bool, shift: i64, n: usize) -> std::path::PathBuf {
    let rel = "parquet/part-00000.parquet";
    let path = dir.join(rel);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let b = batch(n, narrow_team, shift);
    let file = std::fs::File::create(&path).unwrap();
    let mut writer = ArrowWriter::try_new(
        file,
        schema(narrow_team),
        Some(WriterProperties::builder().build()),
    )
    .unwrap();
    writer.write(&b).unwrap();
    writer.close().unwrap();

    // The logical fingerprint is computed under the declared logical types, so an Int32 and
    // an Int64 team_id with equal values fingerprint identically.
    let mut logical = LogicalRowMultiset::default();
    logical.update(&b, &logical_schema()).unwrap();
    let bytes = std::fs::read(&path).unwrap();
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
        physical_schema: physical_schema_json(narrow_team),
        packing_key: "team_id".into(),
        sort_key: vec!["team_id".into(), "timestamp".into()],
        logical_projection: Some(projection()),
        files: vec![SnapshotFile {
            path: rel.into(),
            object_key: None,
            digest: digest_bytes(&bytes),
            bytes: bytes.len() as u64,
            rows: n as u64,
            row_groups: 1,
            source_part: None,
        }],
        total_rows: n as u64,
        total_bytes: bytes.len() as u64,
        physical_fingerprint: None,
        logical_fingerprint: Fingerprint {
            version: parquet_lab_integrity::LOGICAL_ROW_MULTISET_VERSION.into(),
            count: logical.count,
            xor: logical.xor_hex(),
            sum: logical.sum,
            digest: logical.digest_hex(),
        },
        disclaimer: None,
    };
    let mp = dir.join("manifest.json");
    std::fs::write(&mp, serde_json::to_vec_pretty(&manifest).unwrap()).unwrap();
    mp
}

/// A small event query suite covering count, a filter, and a grouped aggregate.
pub const QUERIES_SQL: &str = "\
-- q_count:
SELECT count(*) AS events FROM events;
-- q_window:
SELECT count(*) AS events FROM events WHERE timestamp >= 1782864000000 AND timestamp < 1782864010000;
-- q_group:
SELECT name, count(*) AS n FROM events GROUP BY name ORDER BY n DESC, name ASC;
";

pub fn write_sql(dir: &Path) -> std::path::PathBuf {
    let p = dir.join("queries.sql");
    std::fs::write(&p, QUERIES_SQL).unwrap();
    p
}

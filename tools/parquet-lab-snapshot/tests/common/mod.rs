//! Shared test helpers: build small deterministic Parquet fixtures on disk.

use std::path::Path;
use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;

/// The Ukiel-style declared schema JSON for the event fixtures.
pub fn ukiel_schema_json() -> serde_json::Value {
    serde_json::json!({
        "fields": [
            {"name": "team_id", "type": "int64"},
            {"name": "timestamp", "type": "timestamp_ms"},
            {"name": "name", "type": "utf8", "nullable": true},
            {"name": "value", "type": "float64", "nullable": true}
        ]
    })
}

/// The ordered logical-schema declaration `from-files` consumes.
pub fn logical_schema_json() -> serde_json::Value {
    serde_json::json!({
        "columns": [
            {"name": "team_id", "logical": "int64"},
            {"name": "timestamp", "logical": "timestamp_ms"},
            {"name": "name", "logical": "utf8"},
            {"name": "value", "logical": "float64"}
        ]
    })
}

fn event_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("team_id", DataType::Int64, false),
        Field::new("timestamp", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("value", DataType::Float64, true),
    ]))
}

/// Write one Parquet file with the given rows and return its byte length.
pub fn write_event_parquet(path: &Path, teams: &[i64]) -> u64 {
    let schema = event_schema();
    let n = teams.len();
    let ts: Vec<i64> = (0..n as i64)
        .map(|i| 1_782_864_000_000 + i * 1000)
        .collect();
    let names: Vec<Option<String>> = (0..n)
        .map(|i| {
            if i % 3 == 0 {
                None
            } else {
                Some(format!("e{i}"))
            }
        })
        .collect();
    let vals: Vec<Option<f64>> = (0..n).map(|i| Some(i as f64 * 0.5)).collect();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(teams.to_vec())),
            Arc::new(Int64Array::from(ts)),
            Arc::new(StringArray::from(names)),
            Arc::new(Float64Array::from(vals)),
        ],
    )
    .unwrap();
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    let file = std::fs::File::create(path).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    std::fs::metadata(path).unwrap().len()
}

/// Write the schema/logical-schema/files-from inputs and return their paths.
pub fn write_inputs(
    dir: &Path,
    sources: &[std::path::PathBuf],
) -> (std::path::PathBuf, std::path::PathBuf, std::path::PathBuf) {
    let schema = dir.join("schema.json");
    std::fs::write(
        &schema,
        serde_json::to_vec_pretty(&ukiel_schema_json()).unwrap(),
    )
    .unwrap();
    let logical = dir.join("logical-schema.json");
    std::fs::write(
        &logical,
        serde_json::to_vec_pretty(&logical_schema_json()).unwrap(),
    )
    .unwrap();
    let files_from = dir.join("files.txt");
    let listing = sources
        .iter()
        .map(|p| p.display().to_string())
        .collect::<Vec<_>>()
        .join("\n");
    std::fs::write(&files_from, listing).unwrap();
    (schema, files_from, logical)
}

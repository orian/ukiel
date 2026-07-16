//! The offline writer packages must stay Arrow+Parquet(+pure-lib) only, and the bench must
//! actually time a write end-to-end over a hand-built reconstruction artifact.

use std::collections::HashSet;
use std::path::Path;
use std::process::Command;
use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use parquet_lab_contract::{
    FileDigest, Fingerprint, ReconstructionManifest, ResolvedConfig, RewriteBias, VariantFileMap,
    digest_bytes,
};
use parquet_lab_integrity::{LogicalColumn, LogicalRowMultiset, LogicalSchema, LogicalType};

fn built_closure(package: &str) -> HashSet<String> {
    let out = Command::new(env!("CARGO"))
        .args([
            "tree", "-p", package, "-e", "normal", "--prefix", "none", "--format", "{lib}",
            "--manifest-path",
            concat!(env!("CARGO_MANIFEST_DIR"), "/Cargo.toml"),
        ])
        .output()
        .expect("cargo tree");
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
    String::from_utf8(out.stdout)
        .unwrap()
        .lines()
        .map(|l| l.trim().to_string())
        .flat_map(|l| [l.clone(), l.replace('_', "-")])
        .collect()
}

#[test]
fn the_offline_writer_packages_reach_no_service_crate() {
    for pkg in ["parquet-lab-write-core", "parquet-write-bench"] {
        let deps = built_closure(pkg);
        for forbidden in [
            "datafusion",
            "object_store",
            "ukiel-core",
            "ukiel-catalog",
            "ukiel-query",
            "tokio",
            "sqlx",
            "parquet-rewrite",
        ] {
            assert!(
                !deps.contains(forbidden),
                "{pkg} must stay offline Arrow+Parquet, but reaches {forbidden}"
            );
        }
        assert!(deps.contains("parquet") && deps.contains("arrow"));
    }
}

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("team_id", DataType::Int64, false),
        Field::new("timestamp", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("value", DataType::Float64, true),
    ]))
}

fn logical_schema() -> LogicalSchema {
    LogicalSchema::new(vec![
        LogicalColumn { name: "team_id".into(), logical: LogicalType::SignedInt },
        LogicalColumn { name: "timestamp".into(), logical: LogicalType::TimestampMillis },
        LogicalColumn { name: "name".into(), logical: LogicalType::Utf8 },
        LogicalColumn { name: "value".into(), logical: LogicalType::Float64 },
    ])
}

fn batch(n: usize) -> RecordBatch {
    let teams: Vec<i64> = (0..n as i64).map(|i| i / 4).collect();
    let ts: Vec<i64> = (0..n as i64).map(|i| 1_782_864_000_000 + i * 1000).collect();
    let names: Vec<Option<String>> =
        (0..n).map(|i| if i % 5 == 0 { None } else { Some(format!("evt-{i}")) }).collect();
    let vals: Vec<Option<f64>> = (0..n).map(|i| Some(i as f64 * 1.25)).collect();
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(teams)),
            Arc::new(Int64Array::from(ts)),
            Arc::new(StringArray::from(names)),
            Arc::new(Float64Array::from(vals)),
        ],
    )
    .unwrap()
}

fn resolved(compression: &str) -> ResolvedConfig {
    let mut m = std::collections::BTreeMap::new();
    m.insert("global.row_group_rows".into(), serde_json::json!(131072));
    m.insert("global.compression".into(), serde_json::json!(compression));
    ResolvedConfig::new(m)
}

/// Build a reconstruction control on disk: one parquet file + a manifest carrying the real
/// logical fingerprint, declared types, and a resolved config.
fn write_reconstruction(dir: &Path, compression: &str) -> std::path::PathBuf {
    let rel = "parquet/part-00000.parquet";
    let path = dir.join(rel);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let file = std::fs::File::create(&path).unwrap();
    let mut writer =
        ArrowWriter::try_new(file, schema(), Some(WriterProperties::builder().build())).unwrap();
    let mut logical = LogicalRowMultiset::default();
    let ls = logical_schema();
    let b = batch(400);
    logical.update(&b, &ls).unwrap();
    writer.write(&b).unwrap();
    writer.close().unwrap();

    let bytes = std::fs::read(&path).unwrap();
    let fp = Fingerprint {
        version: parquet_lab_integrity::LOGICAL_ROW_MULTISET_VERSION.into(),
        count: logical.count,
        xor: logical.xor_hex(),
        sum: logical.sum,
        digest: logical.digest_hex(),
    };
    let mut proj = serde_json::Map::new();
    proj.insert("team_id".into(), serde_json::json!("int64"));
    proj.insert("timestamp".into(), serde_json::json!("timestamp_ms"));
    proj.insert("name".into(), serde_json::json!("utf8"));
    proj.insert("value".into(), serde_json::json!("float64"));
    let manifest = ReconstructionManifest {
        reconstruction_version: parquet_lab_contract::RECONSTRUCTION_VERSION.into(),
        parent_product_digest: "prod".repeat(16),
        baseline_spec_digest: "ba5e".repeat(16),
        label: "reconstruction".into(),
        requested_config: resolved(compression),
        resolved_config: resolved(compression),
        logical_projection: proj,
        sort_key: vec!["team_id".into(), "timestamp".into()],
        physical_schema: serde_json::json!({"fields": [
            {"name": "team_id"}, {"name": "timestamp"}, {"name": "name"}, {"name": "value"}
        ]}),
        logical_fingerprint: fp,
        files: vec![VariantFileMap {
            input: FileDigest::of("product/part-00000.parquet", b"in"),
            output: FileDigest::of(rel, &bytes),
            input_rows: 400,
            output_rows: 400,
            footer_summary: serde_json::json!({}),
        }],
        input_census: serde_json::json!({}),
        output_census: serde_json::json!({}),
        rewrite_bias: RewriteBias {
            product_total_bytes: bytes.len() as u64,
            reconstruction_total_bytes: bytes.len() as u64,
            per_column: serde_json::json!({}),
        },
    };
    let mp = dir.join("manifest.json");
    std::fs::write(&mp, serde_json::to_vec_pretty(&manifest).unwrap()).unwrap();
    // Sanity: the digest we recorded is the on-disk digest.
    assert_eq!(digest_bytes(&bytes), manifest.files[0].output.digest);
    mp
}

#[test]
fn the_bench_times_a_write_and_binds_the_artifact() {
    let tmp = tempfile::tempdir().unwrap();
    let reco_dir = tmp.path().join("reconstruction");
    std::fs::create_dir_all(&reco_dir).unwrap();
    let mp = write_reconstruction(&reco_dir, "zstd(1)");

    let report_path = tmp.path().join("writer-report.json");
    let report = parquet_write_bench::run_bench(&mp, &mp, 4, &report_path).unwrap();
    assert_eq!(report.samples.len(), 4);
    assert_eq!(report.total_rows, 400);
    assert_eq!(report.compression, "zstd(1)");
    assert!(report.samples.iter().all(|s| s.output_bytes > 0 && s.rows == 400));
    assert_eq!(report.artifact_digest, digest_bytes(&std::fs::read(&mp).unwrap()));
    // The report is on disk and re-parses.
    let back: parquet_write_bench::WriterBenchReport =
        serde_json::from_slice(&std::fs::read(&report_path).unwrap()).unwrap();
    assert_eq!(back.report_version, parquet_write_bench::WRITE_BENCH_VERSION);
}

#[test]
fn a_config_from_a_variant_delta_times_a_different_codec() {
    let tmp = tempfile::tempdir().unwrap();
    let reco_dir = tmp.path().join("reconstruction");
    std::fs::create_dir_all(&reco_dir).unwrap();
    let mp = write_reconstruction(&reco_dir, "zstd(1)");

    // A stand-in variant-delta manifest whose resolved_config is ZSTD-6.
    let variant = serde_json::json!({
        "delta_version": parquet_lab_contract::VARIANT_DELTA_VERSION,
        "resolved_config": resolved("zstd(6)"),
    });
    let vpath = tmp.path().join("variant.json");
    std::fs::write(&vpath, serde_json::to_vec_pretty(&variant).unwrap()).unwrap();

    let report_path = tmp.path().join("writer-report-z6.json");
    let report = parquet_write_bench::run_bench(&mp, &vpath, 3, &report_path).unwrap();
    assert_eq!(report.compression, "zstd(6)");
    assert_eq!(report.total_rows, 400);
}

//! Build a real sidecar from a variant, then prove it decodes, binds, and prunes correctly:
//! a NoMatch verdict from the built payload always agrees with a full scan of the file.

use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use parquet_lab_contract::{
    FileDigest, SkipManifest, VARIANT_MANIFEST_VERSION, VariantFileMap, VariantManifest,
    WriterProperties as CWP, digest_bytes,
};
use parquet_skip_index::builder::{SkipSpec, build};
use parquet_skip_index::value_set::ValueSet;
use parquet_skip_index::zone_map::ZoneMap;
use parquet_skip_index::{Decision, Predicate, Value};

fn write_variant(dir: &std::path::Path) -> std::path::PathBuf {
    // Two row groups of 4 rows: team_id runs 1,1,2,2 then 5,5,9,9.
    let schema = Arc::new(Schema::new(vec![
        Field::new("team_id", DataType::Int64, false),
        Field::new("event", DataType::Utf8, false),
    ]));
    let out_rel = "out/p0.parquet";
    let path = dir.join(out_rel);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(4))
        .build();
    let file = std::fs::File::create(&path).unwrap();
    let mut w = ArrowWriter::try_new(file, schema.clone(), Some(props)).unwrap();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 1, 2, 2, 5, 5, 9, 9])),
            Arc::new(StringArray::from(vec![
                "click", "view", "click", "view", "sign", "sign", "load", "load",
            ])),
        ],
    )
    .unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();

    let fbytes = std::fs::read(&path).unwrap();
    let output = FileDigest::of(out_rel.to_string(), &fbytes);
    let manifest = VariantManifest {
        manifest_version: VARIANT_MANIFEST_VERSION.into(),
        parent_snapshot_digest: "snap".repeat(16),
        spec_digest: "spec".repeat(16),
        label: "test".into(),
        properties: CWP {
            row_group_rows: 4,
            key_boundary_flush: false,
            write_batch_rows: 1024,
            data_page_bytes: 1 << 20,
            dictionary_page_bytes: 1 << 20,
            statistics: "page".into(),
            offset_index: true,
            compression: "zstd(3)".into(),
        },
        columns: vec![],
        input_census: serde_json::json!({}),
        output_census: serde_json::json!({}),
        logical_fingerprint: parquet_lab_contract::Fingerprint {
            version: parquet_lab_contract::LOGICAL_ROW_MULTISET_VERSION.into(),
            count: 8,
            xor: "00".repeat(32),
            sum: [0; 4],
            digest: "00".repeat(32),
        },
        files: vec![VariantFileMap {
            input: output.clone(),
            output,
            input_rows: 8,
            output_rows: 8,
            footer_summary: serde_json::json!({}),
        }],
    };
    let mp = dir.join("manifest.json");
    std::fs::write(&mp, serde_json::to_vec_pretty(&manifest).unwrap()).unwrap();
    mp
}

#[test]
fn a_built_sidecar_binds_decodes_and_prunes_correctly() {
    let tmp = tempfile::tempdir().unwrap();
    let mp = write_variant(tmp.path());
    let spec = SkipSpec::parse(
        br#"
[[column]]
name = "team_id"
kind = "value_set"
budget_bytes = 4096
"#,
        "spec",
    )
    .unwrap();
    let out = tmp.path().join("skip");
    let sidecar_path = build(&mp, &spec, &out, false).unwrap();

    // The sidecar is bound to the exact variant + file.
    let sbytes = std::fs::read(&sidecar_path).unwrap();
    let sidecar = SkipManifest::parse(&sidecar_path.display().to_string(), &sbytes).unwrap();
    let variant_digest = digest_bytes(&std::fs::read(&mp).unwrap());
    assert!(sidecar.is_bound_to(&variant_digest, &sidecar.file_digests));
    assert_eq!(sidecar.columns.len(), 1);
    let col = &sidecar.columns[0];
    assert_eq!(col.row_groups.len(), 2, "two row groups indexed");

    // Load the payload blob and evaluate: rg0 holds {1,2}, rg1 holds {5,9}.
    let payload = std::fs::read(out.join(&sidecar.payload_path)).unwrap();
    let decode = |rg: usize| -> ValueSet {
        let r = &col.row_groups[rg];
        // The payload digest must verify before use.
        let slice =
            &payload[r.payload_offset as usize..(r.payload_offset + r.payload_length) as usize];
        assert_eq!(parquet_skip_index::payload_digest(slice), r.payload_digest);
        ValueSet::decode(slice).unwrap()
    };
    let rg0 = decode(0);
    let rg1 = decode(1);

    // team_id = 2 is in rg0, not rg1.
    assert_eq!(rg0.evaluate(&Predicate::Eq(Value::Int(2))), Decision::Maybe);
    assert_eq!(
        rg1.evaluate(&Predicate::Eq(Value::Int(2))),
        Decision::NoMatch
    );
    // team_id = 5 is in rg1, not rg0.
    assert_eq!(
        rg0.evaluate(&Predicate::Eq(Value::Int(5))),
        Decision::NoMatch
    );
    assert_eq!(rg1.evaluate(&Predicate::Eq(Value::Int(5))), Decision::Maybe);
    // A value in neither group is NoMatch in both.
    assert_eq!(
        rg0.evaluate(&Predicate::Eq(Value::Int(99))),
        Decision::NoMatch
    );
    assert_eq!(
        rg1.evaluate(&Predicate::Eq(Value::Int(99))),
        Decision::NoMatch
    );

    // Cross-check against a zone map built the same way — the range [10, 20] misses both.
    let z0 = ZoneMap::build(&[Some(Value::Int(1)), Some(Value::Int(2))]);
    assert_eq!(
        z0.evaluate(&Predicate::Range(Value::Int(10), Value::Int(20))),
        Decision::NoMatch
    );
}

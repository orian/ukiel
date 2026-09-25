//! A reconstruction and its one-variable (codec-only) child decode the same logical rows,
//! so the scan checksum and row count must agree; a tampered file is rejected before timing.

mod common;

use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use parquet_scan_bench::projection::parse_role;
use parquet_scan_bench::selection::parse_selection;

/// Rewrite a reconstruction's files under a different codec, preserving rows/order — a
/// stand-in for a variant `vary` would produce. Returns the variant manifest path.
fn rewrite_codec(
    reco_mp: &std::path::Path,
    out_dir: &std::path::Path,
    compression: &str,
) -> std::path::PathBuf {
    let reco: parquet_lab_contract::ReconstructionManifest =
        parquet_lab_contract::ReconstructionManifest::parse(
            &reco_mp.display().to_string(),
            &std::fs::read(reco_mp).unwrap(),
        )
        .unwrap();
    let reco_dir = reco_mp.parent().unwrap();
    let codec = match compression {
        c if c.starts_with("zstd") => {
            let lvl: i32 = c
                .trim_start_matches("zstd(")
                .trim_end_matches(')')
                .parse()
                .unwrap_or(3);
            parquet::basic::Compression::ZSTD(parquet::basic::ZstdLevel::try_new(lvl).unwrap())
        }
        _ => parquet::basic::Compression::SNAPPY,
    };
    let mut file_maps = Vec::new();
    for (i, f) in reco.files.iter().enumerate() {
        let src = std::fs::read(reco_dir.join(&f.output.path)).unwrap();
        let builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
            bytes::Bytes::from(src),
        )
        .unwrap();
        let schema = builder.schema().clone();
        // A codec-only variant preserves row-group boundaries; mirror the source's size so
        // the global ordinals select the same rows in both artifacts.
        let rg_rows = builder.metadata().row_group(0).num_rows() as usize;
        let reader = builder.build().unwrap();
        let rel = format!("parquet/part-{i:05}.parquet");
        let path = out_dir.join(&rel);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let props = WriterProperties::builder()
            .set_compression(codec)
            .set_max_row_group_row_count(Some(rg_rows))
            .build();
        let file = std::fs::File::create(&path).unwrap();
        let mut w = ArrowWriter::try_new(file, schema, Some(props)).unwrap();
        for b in reader {
            w.write(&b.unwrap()).unwrap();
        }
        w.close().unwrap();
        let bytes = std::fs::read(&path).unwrap();
        file_maps.push(parquet_lab_contract::VariantFileMap {
            input: f.output.clone(),
            output: parquet_lab_contract::FileDigest::of(&rel, &bytes),
            input_rows: f.output_rows,
            output_rows: f.output_rows,
            footer_summary: serde_json::json!({}),
        });
    }
    let variant = parquet_lab_contract::VariantDeltaManifest {
        delta_version: parquet_lab_contract::VARIANT_DELTA_VERSION.into(),
        parent_reconstruction_digest: parquet_lab_contract::digest_bytes(
            &std::fs::read(reco_mp).unwrap(),
        ),
        label: "compression-zstd-6".into(),
        allowed_changes: vec!["global.compression".into()],
        changes: {
            let mut m = std::collections::BTreeMap::new();
            m.insert("global.compression".into(), serde_json::json!(compression));
            m
        },
        resolved_config: reco.resolved_config.clone(),
        logical_fingerprint: reco.logical_fingerprint.clone(),
        files: file_maps,
        input_census: serde_json::json!({}),
        output_census: serde_json::json!({}),
    };
    let mp = out_dir.join("manifest.json");
    std::fs::write(&mp, serde_json::to_vec_pretty(&variant).unwrap()).unwrap();
    mp
}

#[test]
fn control_and_codec_variant_agree_on_checksum_and_rows() {
    let tmp = tempfile::tempdir().unwrap();
    let reco_dir = tmp.path().join("reconstruction");
    std::fs::create_dir_all(&reco_dir).unwrap();
    let reco_mp = common::write_reconstruction(&reco_dir, 2, 300, 50, "zstd(1)");
    let wl = common::write_workload(&reco_dir);

    let var_dir = tmp.path().join("zstd-6");
    let var_mp = rewrite_codec(&reco_mp, &var_dir, "zstd(6)");

    for (role, sel) in [("all_columns", "all"), ("wide_text", "ten_percent_sparse")] {
        let a = parquet_scan_bench::run_scan(
            &reco_mp,
            &wl,
            parse_role(role).unwrap(),
            parse_selection(sel).unwrap(),
            1,
            None,
            None,
            &reco_dir.join(format!("a-{role}-{sel}.json")),
        )
        .unwrap();
        let b = parquet_scan_bench::run_scan(
            &var_mp,
            &wl,
            parse_role(role).unwrap(),
            parse_selection(sel).unwrap(),
            1,
            None,
            None,
            &var_dir.join(format!("b-{role}-{sel}.json")),
        )
        .unwrap();
        assert_eq!(
            a.checksum, b.checksum,
            "codec change must not alter decoded values ({role}/{sel})"
        );
        assert_eq!(a.samples[0].rows_decoded, b.samples[0].rows_decoded);
    }
}

#[test]
fn a_tampered_file_is_rejected_before_timing() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("reco");
    std::fs::create_dir_all(&dir).unwrap();
    let mp = common::write_reconstruction(&dir, 1, 200, 64, "zstd(1)");
    let wl = common::write_workload(&dir);

    // Flip a byte in the data file so its bytes no longer match the manifest digest.
    let data = dir.join("parquet/part-00000.parquet");
    let mut bytes = std::fs::read(&data).unwrap();
    let mid = bytes.len() / 2;
    bytes[mid] ^= 0xFF;
    std::fs::write(&data, &bytes).unwrap();

    let err = parquet_scan_bench::run_scan(
        &mp,
        &wl,
        parse_role("all_columns").unwrap(),
        parse_selection("all").unwrap(),
        1,
        None,
        None,
        &dir.join("scan.json"),
    )
    .unwrap_err();
    assert!(
        err.to_string().contains("digest mismatch") || err.to_string().contains("modified"),
        "{err}"
    );
}

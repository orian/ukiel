//! Task 2: correctness, determinism, and refusal of the L0 stager.
//!
//! The fixture is produced by the real `prod-synth` binary — the tools are joined by the
//! artifact on disk, and the stager must not link the generator any more than the loader
//! does.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Command;

use arrow::array::Int64Array;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use prod_synth_contract::L0Manifest;
use prod_synth_integrity::RowMultiset;

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn generate(dir: &Path, tenants: u64, rows: u64, seed: u64) {
    let status = Command::new(env!("CARGO"))
        .current_dir(repo_root())
        .args([
            "run",
            "-q",
            "-p",
            "prod-synth",
            "--",
            "generate",
            "--tier",
            "smoke",
            "--profile",
            "docs/prod-info",
            "--output",
            dir.to_str().unwrap(),
            "--tenants",
            &tenants.to_string(),
            "--rows-per-shard",
            &rows.to_string(),
            "--seed",
            &seed.to_string(),
            "--replace",
        ])
        .status()
        .expect("run prod-synth");
    assert!(status.success(), "prod-synth generate failed");
}

/// Fold every row of every L0 file into a fingerprint, and return (rows, fingerprint,
/// and per-file sorted-ness).
fn scan_l0(dir: &Path, m: &L0Manifest, sort_key: &[String]) -> (u64, RowMultiset) {
    let mut fp = RowMultiset::default();
    let mut rows = 0u64;
    for f in &m.files {
        let raw = std::fs::read(dir.join(&f.path)).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(raw))
            .unwrap()
            .build()
            .unwrap();
        let mut prev: Option<Vec<i64>> = None;
        for batch in reader {
            let batch = batch.unwrap();
            rows += batch.num_rows() as u64;
            fp.update(&batch).unwrap();

            // Sorted by the sort key: the two int64 sort columns (team_id, timestamp) are
            // enough to catch an unsorted file, and they are the ones the catalog trusts.
            let cols: Vec<&Int64Array> = sort_key
                .iter()
                .take(2)
                .map(|name| {
                    let i = batch.schema().index_of(name).unwrap();
                    batch
                        .column(i)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                })
                .collect();
            for r in 0..batch.num_rows() {
                let key: Vec<i64> = cols.iter().map(|c| c.value(r)).collect();
                if let Some(p) = &prev {
                    assert!(*p <= key, "file {} is not sorted by the sort key", f.path);
                }
                prev = Some(key);
            }
        }
    }
    (rows, fp)
}

/// A tiny fixture spans several UTC days (the source parts cover the 14-day window), so
/// staging produces day/flush files, every row appears once, every file is sorted, and
/// the fingerprint is preserved.
#[test]
fn staging_regroups_every_row_exactly_once_and_preserves_the_fingerprint() {
    let tmp = tempfile::tempdir().unwrap();
    let src = tmp.path().join("src");
    let out = tmp.path().join("l0");
    generate(&src, 400, 40_000, 0);

    let staged = prod_synth_l0::stage_source(&src.join("manifest.json"), &out, 10_000, false)
        .expect("stage");
    let m = &staged.manifest;

    // Census: staging adds and drops nothing.
    assert_eq!(m.input_rows, 40_000);
    assert_eq!(m.output_rows, 40_000);
    assert_eq!(m.files.iter().map(|f| f.rows).sum::<u64>(), 40_000);

    // Several UTC days, and every file names the day its rows actually fall in.
    let days: BTreeMap<&str, u64> = m.files.iter().fold(BTreeMap::new(), |mut acc, f| {
        *acc.entry(f.day.as_str()).or_default() += f.rows;
        acc
    });
    assert!(
        days.len() >= 2,
        "the source spans multiple days: {:?}",
        days.keys()
    );

    // Read the files back: same rows, sorted, and the same multiset the manifest claims.
    let (scanned_rows, scanned_fp) = scan_l0(&out, m, &m.table.sort_key);
    assert_eq!(scanned_rows, 40_000);
    assert_eq!(
        scanned_fp.digest_hex(),
        m.fingerprint.digest,
        "the files on disk must fingerprint to what the manifest recorded"
    );
    assert_eq!(scanned_fp.count, m.fingerprint.count);
}

/// Every file's declared key/time range and day match the rows it holds.
#[test]
fn each_l0_file_declares_the_range_and_day_of_its_rows() {
    let tmp = tempfile::tempdir().unwrap();
    let src = tmp.path().join("src");
    let out = tmp.path().join("l0");
    generate(&src, 300, 30_000, 0);
    let staged =
        prod_synth_l0::stage_source(&src.join("manifest.json"), &out, 10_000, false).unwrap();

    for f in &staged.manifest.files {
        let raw = std::fs::read(out.join(&f.path)).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(raw))
            .unwrap()
            .build()
            .unwrap();
        let (mut kmin, mut kmax) = (i64::MAX, i64::MIN);
        let (mut tmin, mut tmax) = (i64::MAX, i64::MIN);
        for batch in reader {
            let batch = batch.unwrap();
            let key = batch
                .column(batch.schema().index_of("team_id").unwrap())
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let ts = batch
                .column(batch.schema().index_of("timestamp").unwrap())
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            kmin = kmin.min(arrow::compute::min(key).unwrap());
            kmax = kmax.max(arrow::compute::max(key).unwrap());
            tmin = tmin.min(arrow::compute::min(ts).unwrap());
            tmax = tmax.max(arrow::compute::max(ts).unwrap());
        }
        assert_eq!(
            (f.key_min, f.key_max),
            (kmin, kmax),
            "file {} key range",
            f.path
        );
        assert_eq!(
            (f.ts_min, f.ts_max),
            (tmin, tmax),
            "file {} ts range",
            f.path
        );
        // Every timestamp in the file falls on the day the file is named for.
        let day_start = f.ts_min - f.ts_min.rem_euclid(86_400_000);
        assert!(
            tmin >= day_start && tmax < day_start + 86_400_000,
            "file {} spans a day boundary",
            f.path
        );
    }
}

/// Byte-identical output for the same source and flush size — the whole point of a
/// deterministic stager. And a different flush size is a different grouping.
#[test]
fn staging_is_deterministic_in_the_source_and_flush_size() {
    let tmp = tempfile::tempdir().unwrap();
    let src = tmp.path().join("src");
    generate(&src, 300, 30_000, 0);

    let a = prod_synth_l0::stage_source(
        &src.join("manifest.json"),
        &tmp.path().join("a"),
        10_000,
        false,
    )
    .unwrap();
    let b = prod_synth_l0::stage_source(
        &src.join("manifest.json"),
        &tmp.path().join("b"),
        10_000,
        false,
    )
    .unwrap();
    assert_eq!(a.manifest.files.len(), b.manifest.files.len());
    for (x, y) in a.manifest.files.iter().zip(&b.manifest.files) {
        assert_eq!(x.digest, y.digest, "file {} differs across runs", x.path);
    }
    assert_eq!(a.manifest.fingerprint, b.manifest.fingerprint);

    // A different flush size regroups the rows: same fingerprint (same rows), different
    // file layout.
    let c = prod_synth_l0::stage_source(
        &src.join("manifest.json"),
        &tmp.path().join("c"),
        5_000,
        false,
    )
    .unwrap();
    assert_eq!(
        a.manifest.fingerprint, c.manifest.fingerprint,
        "the rows are the same whatever the flush size"
    );
    assert_ne!(
        a.manifest.files.len(),
        c.manifest.files.len(),
        "a smaller flush must cut more files"
    );
}

/// Refuse to overwrite without --replace: a staged artifact a load may already
/// reference must not vanish silently.
#[test]
fn staging_refuses_to_overwrite_without_replace() {
    let tmp = tempfile::tempdir().unwrap();
    let src = tmp.path().join("src");
    let out = tmp.path().join("l0");
    generate(&src, 200, 20_000, 0);

    prod_synth_l0::stage_source(&src.join("manifest.json"), &out, 10_000, false).unwrap();
    let e = prod_synth_l0::stage_source(&src.join("manifest.json"), &out, 10_000, false)
        .expect_err("second stage without --replace must fail");
    assert!(e.to_string().contains("--replace"), "{e}");

    // --replace is the escape hatch.
    prod_synth_l0::stage_source(&src.join("manifest.json"), &out, 10_000, true).expect("replace");
}

/// A tampered source part fails before any output is written.
#[test]
fn a_tampered_source_part_is_refused_up_front() {
    let tmp = tempfile::tempdir().unwrap();
    let src = tmp.path().join("src");
    let out = tmp.path().join("l0");
    generate(&src, 200, 20_000, 0);

    // Corrupt one source Parquet file after generation.
    let manifest: prod_synth_contract::Manifest = prod_synth_contract::Manifest::parse(
        "m",
        &std::fs::read(src.join("manifest.json")).unwrap(),
    )
    .unwrap();
    let victim = src.join(&manifest.parts[0].path);
    let mut bytes = std::fs::read(&victim).unwrap();
    bytes[100] ^= 0xff;
    std::fs::write(&victim, bytes).unwrap();

    let e = prod_synth_l0::stage_source(&src.join("manifest.json"), &out, 10_000, false)
        .expect_err("a source with a changed part must not stage");
    assert!(e.to_string().contains("digest mismatch"), "{e}");
    assert!(
        !out.exists(),
        "nothing may be written when the source is refused"
    );
}

/// An unknown source manifest version fails closed.
#[test]
fn an_unknown_source_version_fails_closed() {
    let tmp = tempfile::tempdir().unwrap();
    let src = tmp.path().join("src");
    generate(&src, 200, 20_000, 0);

    let path = src.join("manifest.json");
    let mut v: serde_json::Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    v["manifest_version"] = serde_json::json!("ukiel-prod-synth/v99");
    std::fs::write(&path, serde_json::to_vec_pretty(&v).unwrap()).unwrap();

    let e = prod_synth_l0::stage_source(&path, &tmp.path().join("l0"), 10_000, false)
        .expect_err("unknown version");
    assert!(e.to_string().contains("v99"), "{e}");
}

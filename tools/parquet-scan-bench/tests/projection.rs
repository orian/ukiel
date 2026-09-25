//! Every core projection role and every selection mode decodes and produces a checksum;
//! the zero selection is an untimed metadata control.

mod common;

use parquet_scan_bench::projection::parse_role;
use parquet_scan_bench::selection::parse_selection;

#[test]
fn every_role_and_selection_decodes() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("reco");
    std::fs::create_dir_all(&dir).unwrap();
    let mp = common::write_reconstruction(&dir, 2, 400, 64, "zstd(1)");
    let wl = common::write_workload(&dir);

    let roles = [
        "fixed_width_key",
        "high_cardinality_string",
        "wide_text",
        "hot_set",
        "all_columns",
    ];
    let selections = ["all", "one", "ten_percent_contiguous", "ten_percent_sparse"];
    let mut n = 0;
    for role in roles {
        for sel in selections {
            let report_path = tmp.path().join(format!("scan-{n}.json"));
            n += 1;
            let r = parquet_scan_bench::run_scan(
                &mp,
                &wl,
                parse_role(role).unwrap(),
                parse_selection(sel).unwrap(),
                2,
                None,
                None,
                &report_path,
            )
            .unwrap();
            assert!(!r.metadata_only);
            assert!(r.checksum.is_some());
            assert_eq!(r.samples.len(), 2);
            assert!(r.samples.iter().all(|s| s.rows_decoded > 0));
            assert_eq!(r.role, role);
            // Row groups opened is > 0 and consistent with the plan.
            assert!(r.samples[0].row_groups_opened > 0);
        }
    }
}

#[test]
fn the_zero_selection_is_an_untimed_metadata_control() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("reco");
    std::fs::create_dir_all(&dir).unwrap();
    let mp = common::write_reconstruction(&dir, 1, 200, 64, "zstd(1)");
    let wl = common::write_workload(&dir);
    let report_path = tmp.path().join("zero.json");
    let r = parquet_scan_bench::run_scan(
        &mp,
        &wl,
        parse_role("all_columns").unwrap(),
        parse_selection("zero").unwrap(),
        3,
        None,
        None,
        &report_path,
    )
    .unwrap();
    assert!(r.metadata_only);
    assert!(r.checksum.is_none());
    assert!(
        r.samples.is_empty(),
        "zero selection must not time a decode"
    );
}

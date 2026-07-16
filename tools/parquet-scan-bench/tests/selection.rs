//! Row-group selection: `all` opens every group, `one` opens exactly one, the 10% plans
//! open ~10%, and ordinals map correctly across files.

mod common;

use parquet_scan_bench::projection::parse_role;
use parquet_scan_bench::selection::{self, Selection};

#[test]
fn selection_opens_the_expected_number_of_row_groups() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("reco");
    std::fs::create_dir_all(&dir).unwrap();
    // 3 files x 320 rows / 32 per group = 10 groups/file = 30 total.
    let mp = common::write_reconstruction(&dir, 3, 320, 32, "zstd(1)");
    let wl = common::write_workload(&dir);

    let run = |sel: &str, n: u32| {
        let path = tmp.path().join(format!("s-{sel}.json"));
        parquet_scan_bench::run_scan(
            &mp,
            &wl,
            parse_role("fixed_width_key").unwrap(),
            selection::parse_selection(sel).unwrap(),
            n,
            None,
            None,
            &path,
        )
        .unwrap()
    };

    let all = run("all", 1);
    assert_eq!(all.samples[0].row_groups_opened, 30);

    let one = run("one", 1);
    assert_eq!(one.samples[0].row_groups_opened, 1);

    let contiguous = run("ten_percent_contiguous", 1);
    assert_eq!(contiguous.samples[0].row_groups_opened, 3); // ceil(0.1*30)

    let sparse = run("ten_percent_sparse", 1);
    assert_eq!(sparse.samples[0].row_groups_opened, 3);
    // Sparse and contiguous open the same count but different (scattered) groups.
    assert_eq!(
        contiguous.samples[0].row_groups_opened,
        sparse.samples[0].row_groups_opened
    );
}

#[test]
fn global_ordinals_map_across_files() {
    // Two files, 4 and 6 row groups.
    let (spans, total) = selection::spans(&[4, 6]);
    assert_eq!(total, 10);
    let ords = selection::global_ordinals(Selection::All, total);
    let per_file = selection::to_local(&spans, &ords);
    assert_eq!(per_file[0].1.len(), 4);
    assert_eq!(per_file[1].1.len(), 6);
    // Ordinal 5 is local group 1 in file 1.
    let one = selection::to_local(&spans, &[5]);
    assert_eq!(one, vec![(1usize, vec![1usize])]);
}

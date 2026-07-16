//! Access-plan generation: coverage, the equal-returned-byte contract between contiguous
//! and sparse, and that ranges stay inside their files.

use file_read_bench::ranges::{self, Plan};

#[test]
fn all_and_one_cover_what_they_claim() {
    let lens = [1000u64, 2000, 3000];
    let all = ranges::build(Plan::All, &lens, 0.1);
    assert_eq!(all.len(), 3);
    assert_eq!(ranges::total_bytes(&all), 6000);

    let one = ranges::build(Plan::One, &lens, 0.1);
    assert_eq!(one.len(), 1);
    assert_eq!(one[0].length, 1000);
}

#[test]
fn contiguous_and_sparse_return_the_same_bytes() {
    let lens = [100_000u64, 250_000, 40_000];
    for fraction in [0.05, 0.1, 0.25] {
        let contiguous = ranges::build(Plan::Contiguous, &lens, fraction);
        let sparse = ranges::build(Plan::Sparse, &lens, fraction);
        assert_eq!(
            ranges::total_bytes(&contiguous),
            ranges::total_bytes(&sparse),
            "contiguous and sparse must read the same total at fraction {fraction}"
        );
        // Sparse touches more distinct ranges than contiguous (scattered access).
        assert!(sparse.len() > contiguous.len());
    }
}

#[test]
fn ranges_stay_inside_their_files() {
    let lens = [12_345u64, 67_890];
    for plan in [Plan::All, Plan::One, Plan::Contiguous, Plan::Sparse] {
        for r in ranges::build(plan, &lens, 0.1) {
            assert!(
                r.offset + r.length <= lens[r.file],
                "range {r:?} escapes file {} of length {}",
                r.file,
                lens[r.file]
            );
        }
    }
}

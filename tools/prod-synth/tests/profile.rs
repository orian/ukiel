//! Task 1: the profile parser, validator, and summary.
//!
//! The pinned figures here are the whole point. If the committed profile ever
//! changes, these fail — which is what we want, because every downstream fidelity
//! gate is stated against them and a silently-edited profile would move the goalposts
//! and the measurements at the same time.

use std::path::{Path, PathBuf};

use prod_synth::profile::{ProductionProfile, quantile};

fn profile_dir() -> PathBuf {
    // The committed profile, relative to this crate — not to a working directory.
    // The tool must never assume it is being run from a repository root.
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../docs/prod-info")
}

fn load() -> ProductionProfile {
    ProductionProfile::load(&profile_dir()).expect("the committed profile must parse")
}

/// Every figure plan 45 quotes, reproduced from the committed files.
#[test]
fn pins_the_source_figures_the_plan_is_stated_in() {
    let p = load();
    let s = p.summary();

    assert_eq!(s.parts, 68);
    assert_eq!(s.tenant_samples, 534);
    assert_eq!(s.memberships, 2_389_316);

    assert_eq!(s.exact_parts.p50, 11.0);
    assert_eq!(s.exact_parts.p90, 43.0);
    assert_eq!(s.range_parts.p50, 63.0);
    assert_eq!(s.range_parts.p90, 63.0);
    assert_eq!(s.range_overfetch.p50, 5.727);
    assert_eq!(s.range_overfetch.p90, 63.0);

    assert_eq!(s.tenant_rows.p50, 127.0);
    assert_eq!(s.tenant_rows.p90, 12_493.0);
    assert_eq!(s.tenant_rows.p99, 439_457.0);

    assert_eq!(s.key_density.p10, 0.00625175);
    assert_eq!(s.key_density.p50, 0.10264053);
    assert_eq!(s.key_density.p90, 0.13749292);

    assert_eq!(s.max_key_span, 511_474);
    assert_eq!(s.observed_rows, 4_470_703_452);
}

/// The estimate, and its formula.
#[test]
fn estimates_the_active_tenant_count_from_two_margins() {
    let p = load();
    assert_eq!(p.memberships(), 2_389_316);
    assert!((p.mean_exact_parts() - 16.818_352).abs() < 1e-6);
    assert_eq!(p.estimated_active_tenants(), 142_066);

    // The formula and its caveats travel with every artifact, so a reader never has
    // to go and find the plan to learn that this is an estimate.
    let assumptions = p.assumptions();
    assert!(assumptions[0].contains("142066"));
    assert!(
        assumptions[0].contains("ESTIMATE"),
        "the estimate must say so in capitals: {}",
        assumptions[0]
    );
    assert!(
        assumptions
            .iter()
            .any(|a| a.contains("no real tenant IDs") || a.contains("no real tenant")),
        "the artifact must state what the capture does NOT contain"
    );
}

/// The quantile definition is pinned, because every gate is stated in it.
///
/// `round(p * (n-1))`. Nearest-rank and linear interpolation each reproduce some of
/// the plan's figures and miss others; only this one reproduces all of them.
#[test]
fn quantile_definition_is_the_one_the_gates_assume() {
    let v: Vec<f64> = (1..=10).map(|x| x as f64).collect();
    assert_eq!(quantile(&v, 0.0), 1.0);
    assert_eq!(quantile(&v, 1.0), 10.0);
    // rank = round(0.5 * 9) = round(4.5) = 5 -> v[5] = 6
    assert_eq!(quantile(&v, 0.5), 6.0);
    // rank = round(0.9 * 9) = round(8.1) = 8 -> v[8] = 9
    assert_eq!(quantile(&v, 0.9), 9.0);

    assert_eq!(
        quantile(&[42.0], 0.5),
        42.0,
        "a single sample is its own quantile"
    );
    assert_eq!(quantile(&[], 0.5), 0.0, "empty is not a panic");
}

/// The scope inconsistency between the two capture files is *found and reported*,
/// not absorbed. See `ProductionProfile::scope_inconsistency`.
#[test]
fn reports_that_the_two_capture_files_disagree_about_the_part_population() {
    let p = load();
    assert_eq!(
        p.multi_key_parts(),
        61,
        "only 61 parts have any range width"
    );

    let note = p
        .scope_inconsistency()
        .expect("63 range candidates against 61 multi-key parts is impossible and must be flagged");
    assert!(note.contains("61") && note.contains("63"), "{note}");
    assert!(
        p.assumptions().iter().any(|a| a.contains("part scopes")),
        "the inconsistency must reach the artifact, not just this test"
    );
}

// ---------------------------------------------------------------------------
// Validation. Every rejection names the file and the record — a profile error that
// says only "invalid number" leaves someone bisecting a 500-line JSONL by hand.
// ---------------------------------------------------------------------------

/// Writes a profile directory with one file's contents replaced.
fn profile_with(file: &str, contents: &str) -> tempfile::TempDir {
    let dir = tempfile::tempdir().expect("tempdir");
    for name in [
        "table.json",
        "show-create.sql",
        "part-geometry.jsonl",
        "tenant-fanout.jsonl",
    ] {
        let src = profile_dir().join(name);
        let dst = dir.path().join(name);
        if name == file {
            std::fs::write(&dst, contents).unwrap();
        } else {
            std::fs::copy(&src, &dst).unwrap();
        }
    }
    dir
}

fn err_of(file: &str, contents: &str) -> String {
    let dir = profile_with(file, contents);
    ProductionProfile::load(dir.path())
        .expect_err("must be rejected")
        .to_string()
}

#[test]
fn rejects_invalid_jsonl_naming_the_file_and_line() {
    let e = err_of(
        "tenant-fanout.jsonl",
        "{\"tenant_rows\":1,\"exact_parts\":1,\"range_parts\":1,\"range_overfetch\":1}\nnot json\n",
    );
    assert!(e.contains("tenant-fanout.jsonl:2"), "{e}");
    assert!(e.contains("invalid JSON"), "{e}");
}

#[test]
fn rejects_negative_and_non_finite_values() {
    let e = err_of(
        "tenant-fanout.jsonl",
        r#"{"tenant_rows":-5,"exact_parts":1,"range_parts":1,"range_overfetch":1}"#,
    );
    assert!(e.contains("tenant-fanout.jsonl:1"), "{e}");
    assert!(e.contains("negative"), "{e}");
}

#[test]
fn rejects_observed_rows_exceeding_physical_rows() {
    // A part's filtered rows cannot outnumber the part's own rows.
    let e = err_of(
        "part-geometry.jsonl",
        r#"{"part_id":1,"partition_id":1,"level":0,"part_type":"Wide","physical_rows":10,"bytes_on_disk":1,"data_compressed_bytes":1,"data_uncompressed_bytes":1,"observed_rows":11,"distinct_keys":1,"key_span":1,"key_density":1,"time_span_ms":0}"#,
    );
    assert!(e.contains("part-geometry.jsonl:1"), "{e}");
    assert!(
        e.contains("observed_rows") && e.contains("physical_rows"),
        "{e}"
    );
}

#[test]
fn rejects_a_density_that_does_not_match_its_own_keys_and_span() {
    // 5/100 = 0.05, not 0.9. A record whose derived field contradicts its own inputs
    // is a record we cannot reason about, and a fixture built on it is built on sand.
    let e = err_of(
        "part-geometry.jsonl",
        r#"{"part_id":1,"partition_id":1,"level":0,"part_type":"Wide","physical_rows":10,"bytes_on_disk":1,"data_compressed_bytes":1,"data_uncompressed_bytes":1,"observed_rows":10,"distinct_keys":5,"key_span":100,"key_density":0.9,"time_span_ms":0}"#,
    );
    assert!(e.contains("key_density"), "{e}");
}

#[test]
fn rejects_an_overfetch_that_does_not_match_its_own_counts() {
    let e = err_of(
        "tenant-fanout.jsonl",
        r#"{"tenant_rows":100,"exact_parts":2,"range_parts":10,"range_overfetch":99.0}"#,
    );
    assert!(e.contains("range_overfetch"), "{e}");
}

/// The source rounds its derived fields, and the tolerance is exactly that rounding
/// — no wider. A looser one would start accepting records that are simply wrong.
#[test]
fn accepts_the_sources_own_rounding_but_not_more() {
    // 63/29 = 2.1724137... recorded as 2.172. Half an ulp at three decimals.
    let ok = r#"{"tenant_rows":100,"exact_parts":29,"range_parts":63,"range_overfetch":2.172}"#;
    let dir = profile_with("tenant-fanout.jsonl", ok);
    assert!(
        ProductionProfile::load(dir.path()).is_ok(),
        "three-decimal rounding is what the source does and must be accepted"
    );

    // Ten times the rounding error is not rounding.
    let bad = r#"{"tenant_rows":100,"exact_parts":29,"range_parts":63,"range_overfetch":2.177}"#;
    let dir = profile_with("tenant-fanout.jsonl", bad);
    assert!(
        ProductionProfile::load(dir.path()).is_err(),
        "5e-3 is ten times the source's own rounding and must be rejected"
    );
}

#[test]
fn rejects_range_parts_below_exact_parts() {
    // Every part that holds the tenant also brackets it, so the range set is a
    // superset by construction. A record saying otherwise is incoherent.
    let e = err_of(
        "tenant-fanout.jsonl",
        r#"{"tenant_rows":100,"exact_parts":10,"range_parts":3,"range_overfetch":0.3}"#,
    );
    assert!(
        e.contains("superset") || e.contains("smaller than exact_parts"),
        "{e}"
    );
}

#[test]
fn rejects_an_empty_profile() {
    let e = err_of("part-geometry.jsonl", "\n");
    assert!(e.contains("no part records"), "{e}");
}

#[test]
fn rejects_a_duplicate_part_id() {
    let rec = r#"{"part_id":7,"partition_id":1,"level":0,"part_type":"Wide","physical_rows":10,"bytes_on_disk":1,"data_compressed_bytes":1,"data_uncompressed_bytes":1,"observed_rows":10,"distinct_keys":1,"key_span":1,"key_density":1,"time_span_ms":0}"#;
    let e = err_of("part-geometry.jsonl", &format!("{rec}\n{rec}\n"));
    assert!(e.contains("duplicate part_id"), "{e}");
}

/// The source's part/partition hashes fill the whole 64-bit domain. Reading them as
/// i64 silently rejects half the file — which is exactly what the first cut did.
#[test]
fn accepts_part_ids_above_2_pow_63() {
    let p = load();
    assert!(
        p.parts.iter().any(|x| x.part_id > i64::MAX as u64),
        "the committed profile contains part ids above 2^63; if this stops being \
         true the u64 parse is no longer load-bearing and this test is a lie"
    );
}

/// `show-create.sql` is hashed, never parsed — it is ClickHouse DDL for a type
/// system Ukiel does not implement. But it must still be *present*: it pins which
/// schema the profile came from.
#[test]
fn hashes_the_ddl_as_provenance_and_requires_it() {
    let p = load();
    let ddl = p
        .files
        .iter()
        .find(|f| f.path == "show-create.sql")
        .expect("the DDL is part of the profile's identity");
    assert_eq!(ddl.digest.len(), 64, "blake3 hex");
    assert!(ddl.bytes > 0);

    let dir = profile_with("show-create.sql", "");
    let loaded = ProductionProfile::load(dir.path()).expect("an empty DDL still parses");
    assert_ne!(
        loaded
            .files
            .iter()
            .find(|f| f.path == "show-create.sql")
            .unwrap()
            .digest,
        ddl.digest,
        "its bytes are part of the profile digest, so changing them changes the identity"
    );
}

/// Digests are over the raw bytes as read, not a reserialized struct — a round trip
/// through serde would launder exactly the corruption they exist to catch.
#[test]
fn digests_cover_all_four_files() {
    let p = load();
    let names: Vec<&str> = p.files.iter().map(|f| f.path.as_str()).collect();
    assert_eq!(
        names,
        vec![
            "table.json",
            "show-create.sql",
            "part-geometry.jsonl",
            "tenant-fanout.jsonl"
        ]
    );
    assert!(p.files.iter().all(|f| f.digest.len() == 64 && f.bytes > 0));
}

//! Read-only boundary: the bench links no catalog mutator, writer, generator, or compactor.
//! It uses DataFusion and object_store to read; it must not reach a Ukiel service crate.

use std::collections::HashSet;
use std::process::Command;

#[test]
fn the_bench_has_no_catalog_writer_or_compactor_dependency() {
    let out = Command::new(env!("CARGO"))
        .args([
            "tree",
            "-p",
            "parquet-lab-bench",
            "-e",
            "normal",
            "--prefix",
            "none",
            "--format",
            "{lib}",
            "--manifest-path",
            concat!(env!("CARGO_MANIFEST_DIR"), "/Cargo.toml"),
        ])
        .output()
        .expect("cargo tree");
    let deps: HashSet<String> = String::from_utf8(out.stdout)
        .unwrap()
        .lines()
        .map(|l| l.trim().to_string())
        .flat_map(|l| [l.clone(), l.replace('_', "-")])
        .collect();
    for forbidden in [
        "ukiel-core",
        "ukiel-catalog",
        "ukiel-compactor",
        "ukiel-ingest",
        "ukield",
        "ukiel-gc",
        "sqlx",
        "rdkafka",
        // No other laboratory executable is linked in.
        "parquet-lab-snapshot",
        "parquet-census",
        "parquet-rewrite",
        "parquet-skip-index",
    ] {
        assert!(
            !deps.contains(forbidden),
            "parquet-lab-bench is read-only and standalone, but its graph reaches {forbidden}"
        );
    }
    // It does need DataFusion and object_store to read.
    assert!(deps.contains("datafusion"));
    assert!(deps.contains("object_store") || deps.contains("object-store"));
}

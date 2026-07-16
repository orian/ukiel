//! The cache controller is Parquet-blind and calls no other executable: it only reads,
//! evicts, and probes explicit files. A Parquet or service dependency here would defeat the
//! point of a single-purpose cache tool.

use std::collections::HashSet;
use std::process::Command;

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
fn the_cache_controller_parses_no_parquet_and_reaches_no_service() {
    let deps = built_closure("parquet-cachectl");
    for forbidden in [
        "parquet",
        "arrow",
        "datafusion",
        "object_store",
        "ukiel-core",
        "tokio",
        "sqlx",
        "file-read-bench",
        "parquet-rewrite",
    ] {
        assert!(!deps.contains(forbidden), "parquet-cachectl must not reach {forbidden}");
    }
    assert!(deps.contains("libc"));
    assert!(deps.contains("parquet-lab-contract"));
}

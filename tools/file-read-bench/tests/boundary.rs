//! The raw reader never parses Parquet and never invokes another executable — it is the
//! backend/cache ceiling, and an Arrow/Parquet dependency would defeat that role.

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
fn the_raw_reader_parses_no_parquet_and_reaches_no_service() {
    let deps = built_closure("file-read-bench");
    for forbidden in [
        "parquet",
        "arrow",
        "datafusion",
        "object_store",
        "ukiel-core",
        "tokio",
        "sqlx",
        "parquet-cachectl",
        "parquet-rewrite",
    ] {
        assert!(!deps.contains(forbidden), "file-read-bench must not reach {forbidden}");
    }
    assert!(deps.contains("blake3"));
    assert!(deps.contains("libc"));
}

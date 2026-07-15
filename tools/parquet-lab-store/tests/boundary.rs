//! The publisher stays object-store + hashing only — no DataFusion, no Ukiel, no Parquet
//! parsing (it copies bytes, it does not read footers).

use std::collections::HashSet;
use std::process::Command;

#[test]
fn the_store_tool_has_no_query_or_service_dependency() {
    let out = Command::new(env!("CARGO"))
        .args([
            "tree",
            "-p",
            "parquet-lab-store",
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
        "datafusion",
        "arrow",
        "parquet",
        "ukiel-core",
        "ukiel-catalog",
        "sqlx",
        "parquet-lab-bench",
        "parquet-rewrite",
    ] {
        assert!(
            !deps.contains(forbidden),
            "parquet-lab-store reaches {forbidden}"
        );
    }
    assert!(deps.contains("object_store") || deps.contains("object-store"));
    assert!(deps.contains("sha2"));
}

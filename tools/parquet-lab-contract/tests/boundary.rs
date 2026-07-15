//! The contract is the shared join between every laboratory executable, so it must
//! stay serde + BLAKE3 only. An Arrow, Parquet, DataFusion, or service dependency here
//! would land in the built graph of every tool that depends on the contract — which is
//! all of them — and defeat the point of splitting the offline tools out at all.

use std::collections::HashSet;
use std::process::Command;

const FORBIDDEN: &[&str] = &[
    "arrow",
    "parquet",
    "datafusion",
    "object_store",
    "tokio",
    "reqwest",
    "sqlx",
    "ukiel-core",
];

fn built_closure(package: &str) -> HashSet<String> {
    let out = Command::new(env!("CARGO"))
        .args([
            "tree",
            "-p",
            package,
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
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout)
        .unwrap()
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .flat_map(|l| [l.to_string(), l.replace('_', "-")])
        .collect()
}

#[test]
fn the_contract_is_serde_and_blake3_only() {
    let deps = built_closure("parquet-lab-contract");
    let found: Vec<&str> = FORBIDDEN
        .iter()
        .copied()
        .filter(|f| deps.contains(*f))
        .collect();
    assert!(
        found.is_empty(),
        "parquet-lab-contract must stay a pure serde+BLAKE3 contract, but its graph reaches: \
         {found:?}. Every laboratory tool depends on this crate; a reader or service dependency \
         here leaks into all of them."
    );
    assert!(deps.contains("serde"));
    assert!(deps.contains("blake3"));
}

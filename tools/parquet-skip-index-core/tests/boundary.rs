//! The core is pure: no Arrow, no Parquet, no service. Only the contract (for IndexKind) and
//! BLAKE3. Both the builder and the benchmark depend on it, so a heavy dependency here would
//! leak into both.

use std::collections::HashSet;
use std::process::Command;

#[test]
fn the_core_is_pure_predicate_logic() {
    let out = Command::new(env!("CARGO"))
        .args([
            "tree",
            "-p",
            "parquet-skip-index-core",
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
    for forbidden in ["arrow", "parquet", "datafusion", "object_store", "tokio"] {
        assert!(
            !deps.contains(forbidden),
            "parquet-skip-index-core must stay pure, reaches {forbidden}"
        );
    }
    assert!(deps.contains("blake3"));
}

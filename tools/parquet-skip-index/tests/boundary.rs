//! The sidecar builder stays Arrow+Parquet only — no DataFusion, no Ukiel service, no other
//! laboratory executable.

use std::collections::HashSet;
use std::process::Command;

#[test]
fn the_skip_index_depends_on_no_datafusion_or_service_crate() {
    let out = Command::new(env!("CARGO"))
        .args([
            "tree",
            "-p",
            "parquet-skip-index",
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
        "object_store",
        "ukiel-core",
        "ukiel-catalog",
        "tokio",
        "sqlx",
        "parquet-lab-bench",
        "parquet-lab-snapshot",
        "parquet-rewrite",
        "parquet-census",
    ] {
        assert!(
            !deps.contains(forbidden),
            "parquet-skip-index reaches {forbidden}"
        );
    }
    assert!(deps.contains("parquet") && deps.contains("arrow"));
}

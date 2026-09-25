//! The scan bench decodes through Arrow/Parquet directly and must not reach DataFusion, a
//! catalog, an object store, or any Ukiel service — that separation is the whole point of
//! measuring decode without SQL.

use std::collections::HashSet;
use std::process::Command;

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
        .map(|l| l.trim().to_string())
        .flat_map(|l| [l.clone(), l.replace('_', "-")])
        .collect()
}

#[test]
fn the_scan_bench_reaches_no_datafusion_or_service() {
    let deps = built_closure("parquet-scan-bench");
    for forbidden in [
        "datafusion",
        "datafusion-datasource-parquet",
        "object_store",
        "ukiel-core",
        "ukiel-query",
        "ukiel-catalog",
        "tokio",
        "sqlx",
        "parquet-rewrite",
    ] {
        assert!(
            !deps.contains(forbidden),
            "parquet-scan-bench must not reach {forbidden}"
        );
    }
    assert!(deps.contains("parquet") && deps.contains("arrow"));
}

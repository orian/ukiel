//! Keep SQL, shared writing, and MV execution independent of Kafka/query roles.
use std::{collections::HashSet, process::Command};

fn dependencies(package: &str) -> HashSet<String> {
    let output = Command::new(env!("CARGO"))
        .args([
            "tree",
            "--locked",
            "--offline",
            "-p",
            package,
            "-e",
            "normal",
            "--prefix",
            "none",
            "--format",
            "{p}",
            "--manifest-path",
            concat!(env!("CARGO_MANIFEST_DIR"), "/Cargo.toml"),
        ])
        .output()
        .expect("cargo tree");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout)
        .unwrap()
        .lines()
        .filter_map(|line| line.split_whitespace().next().map(str::to_owned))
        .collect()
}

#[test]
fn source_runtime_boundaries_hold_transitively() {
    for (package, forbidden) in [
        (
            "ukiel-mv",
            vec!["ukiel-ingest", "ukiel-query", "ukield", "rdkafka"],
        ),
        (
            "ukiel-write",
            vec![
                "ukiel-ingest",
                "ukiel-mv",
                "ukiel-query",
                "ukield",
                "rdkafka",
            ],
        ),
        (
            "ukiel-pipeline",
            vec![
                "ukiel-catalog",
                "ukiel-write",
                "ukiel-ingest",
                "ukiel-mv",
                "ukiel-query",
                "ukield",
                "rdkafka",
                "sqlx",
            ],
        ),
    ] {
        let deps = dependencies(package);
        for forbidden in forbidden {
            assert!(
                !deps.contains(forbidden),
                "{package} must not reach {forbidden}"
            );
        }
        if package == "ukiel-write" {
            assert!(
                deps.contains("ukiel-expr"),
                "write expressions must use the shared engine"
            );
        }
    }
}

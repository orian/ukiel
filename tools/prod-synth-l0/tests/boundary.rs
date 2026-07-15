//! The stager is offline. It writes Parquet with ukiel-core's primitives and touches no
//! service — the same boundary the plan-45 generator holds, for the same reason: it must
//! stay installable and runnable with only a Rust toolchain.

use std::collections::HashSet;
use std::process::Command;

const FORBIDDEN: &[&str] = &[
    "ukiel-catalog",
    "ukiel-query",
    "ukiel-ingest",
    "ukiel-compactor",
    "ukiel-gc",
    "ukield",
    "sqlx",
    "rdkafka",
    "object_store",
    "datafusion",
    "axum",
    "reqwest",
    "tokio",
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
        "cargo tree: {}",
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
fn the_stager_has_no_service_dependency() {
    let deps = built_closure("prod-synth-l0");
    let found: Vec<&str> = FORBIDDEN
        .iter()
        .copied()
        .filter(|f| deps.contains(*f))
        .collect();
    assert!(
        found.is_empty(),
        "prod-synth-l0 must stay offline and installable with only a Rust toolchain, but its \
         built graph now reaches: {found:?}"
    );
    // The one Ukiel dependency it is allowed: the stable writer/sort primitives, so a
    // staged file is written the way ingest writes one.
    assert!(
        deps.contains("ukiel-core"),
        "it writes files the product's way"
    );
    assert!(
        deps.contains("prod-synth-integrity"),
        "it shares the fingerprint"
    );
    assert!(deps.contains("prod-synth-contract"));
}

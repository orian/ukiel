//! The logical fingerprint library is pure. It reads Arrow and hashes; it must never
//! acquire a Parquet reader, DataFusion, a Ukiel crate, or a service client, because
//! the whole point of a shared canonical encoder is that both the snapshot tool and
//! the rewrite tool depend on it without inheriting a query engine.

use std::collections::HashSet;
use std::process::Command;

const FORBIDDEN: &[&str] = &[
    "parquet",
    "datafusion",
    "object_store",
    "ukiel-core",
    "ukiel-catalog",
    "ukiel-query",
    "ukiel-ingest",
    "ukiel-compactor",
    "ukield",
    "sqlx",
    "rdkafka",
    "tokio",
    "reqwest",
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
fn the_logical_fingerprint_depends_on_no_reader_or_service_crate() {
    let deps = built_closure("parquet-lab-integrity");
    let found: Vec<&str> = FORBIDDEN
        .iter()
        .copied()
        .filter(|f| deps.contains(*f))
        .collect();
    assert!(
        found.is_empty(),
        "parquet-lab-integrity must stay a pure Arrow+BLAKE3 library, but its built graph now \
         reaches: {found:?}. It is the canonical logical encoder shared by snapshot and rewrite; \
         a reader or service crate here would leak into both."
    );
    assert!(deps.contains("arrow"), "it reads Arrow batches");
    assert!(deps.contains("blake3"), "it hashes with BLAKE3");
}

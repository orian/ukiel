//! The integrity library is pure. It reads Arrow and hashes; it must never acquire a
//! service or client crate, because the whole point of a shared fingerprint is that
//! both the offline stager and the read-only runner can depend on it without
//! inheriting a database driver.

use std::collections::HashSet;
use std::process::Command;

/// Crates that must never appear in the built graph of `prod-synth-integrity`.
const FORBIDDEN: &[&str] = &[
    "ukiel-core",
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
fn the_fingerprint_library_depends_on_no_service_crate() {
    let deps = built_closure("prod-synth-integrity");
    let found: Vec<&str> = FORBIDDEN
        .iter()
        .copied()
        .filter(|f| deps.contains(*f))
        .collect();
    assert!(
        found.is_empty(),
        "prod-synth-integrity must stay a pure Arrow+BLAKE3 library, but its built graph now \
         reaches: {found:?}. It is shared by an offline stager and a read-only runner; a service \
         crate here would leak into both."
    );
    // The two it is allowed — and needs — are present.
    assert!(deps.contains("arrow"), "it reads Arrow batches");
    assert!(deps.contains("blake3"), "it hashes with BLAKE3");
}

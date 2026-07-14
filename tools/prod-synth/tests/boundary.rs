//! The tool boundary, enforced rather than asserted in a README.
//!
//! Plan 45's central structural claim is that the generator is *offline*: it must
//! stay installable and runnable with nothing but a Rust toolchain, so that anyone
//! can reproduce the fixture from the committed profile and check they got the same
//! bytes. A README saying so is worth nothing — the first time someone reaches for a
//! convenient helper in `ukiel-e2e`, the generator quietly acquires PostgreSQL,
//! Kafka, an object store and DataFusion, and nobody notices until a fresh checkout
//! fails to build.
//!
//! So the dependency graph is inspected — the one that actually gets *built*, not the
//! one the workspace metadata describes. It does not care what anyone intended.

use std::collections::HashSet;
use std::process::Command;

/// Packages the generator must never be able to reach, transitively.
///
/// The Ukiel service crates, and the clients they would drag in. `ukiel-core` is the
/// one permitted Ukiel dependency: it is schema, sort, and Parquet-writer primitives,
/// with no service client in its own graph — which is why the fixture can be written
/// *the way the product writes files* without the generator becoming the product.
const FORBIDDEN: &[&str] = &[
    "ukiel-e2e",
    "ukield",
    "ukiel-catalog",
    "ukiel-query",
    "ukiel-ingest",
    "ukiel-compactor",
    "ukiel-gc",
    // The clients themselves. Even arriving indirectly, any of these means the
    // generator can no longer be built without them.
    "sqlx",
    "rdkafka",
    "object_store",
    "datafusion",
    "axum",
    "reqwest",
    "tokio",
    "testcontainers-modules",
];

/// The transitive dependency closure of one package, as it is actually built.
///
/// `cargo tree -p <pkg> -e normal`, deliberately, and **not** `cargo metadata`.
/// Metadata resolves features across the *whole workspace* and reports their union,
/// so it shows `prod-synth` reaching `tokio` merely because `ukiel-query` elsewhere
/// in the repo turns on `parquet/async`. That union is not what gets compiled:
/// resolver v2 resolves features per build, so `cargo install --path tools/prod-synth`
/// builds `parquet` without those features and never sees tokio at all.
///
/// The test has to measure the graph that is built, not the graph that is described —
/// otherwise it either fails for no reason or, worse, passes for no reason.
///
/// (`-e normal` drops dev-dependencies, which do not ship in the installed binary.)
fn closure(package: &str) -> HashSet<String> {
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
        "cargo tree failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    String::from_utf8(out.stdout)
        .expect("utf8")
        .lines()
        .map(|l| l.trim())
        .filter(|l| !l.is_empty())
        // `{lib}` prints the lib target name, which uses underscores; the package
        // names in FORBIDDEN use the crate.io spelling. Normalize both ways so a
        // hyphenated package name still matches.
        .flat_map(|l| [l.to_string(), l.replace('_', "-")])
        .collect()
}

/// The generator cannot reach a service client. This is the plan's boundary, and it
/// is the reason `cargo install --path tools/prod-synth` works from a fresh checkout
/// with no compose stack, no PostgreSQL headers, and no cmake for librdkafka.
#[test]
fn the_generator_has_no_service_dependency() {
    let deps = closure("prod-synth");

    let found: Vec<&str> = FORBIDDEN
        .iter()
        .copied()
        .filter(|f| deps.contains(*f))
        .collect();

    assert!(
        found.is_empty(),
        "prod-synth must stay offline and installable with only a Rust toolchain, but its \
         dependency graph now reaches: {found:?}.\n\n\
         If one of these arrived through a convenience helper, move the helper — the boundary \
         is the point, not the inconvenience."
    );

    // And the permitted one is genuinely there: the fixture is written with the
    // product's own schema, sort and writer primitives, not a benchmark's imitation
    // of them.
    assert!(
        deps.contains("ukiel-core"),
        "the generator must write files the way the product does"
    );
    assert!(deps.contains("prod-synth-contract"));
}

/// The contract library is the joint the three executables meet at, so it has to
/// stay a joint: types and version checks, nothing else. If it ever grows a profile
/// parser, a service client, or a benchmark, all three tools inherit it.
#[test]
fn the_contract_library_stays_tiny() {
    let deps = closure("prod-synth-contract");

    let heavy: Vec<&str> = FORBIDDEN
        .iter()
        .chain(["ukiel-core", "arrow", "parquet", "clap"].iter())
        .copied()
        .filter(|f| deps.contains(*f))
        .collect();

    assert!(
        heavy.is_empty(),
        "prod-synth-contract is the public artifact format that all three executables depend on. \
         It must parse no profile, generate nothing, contact no service, and benchmark nothing — \
         but it now reaches: {heavy:?}"
    );
}

/// No executable package may depend on another executable package. They communicate
/// through the versioned manifest on disk, and nothing else — which is what stops
/// the loader from "just calling" the generator when a fixture is missing, and the
/// benchmark runner from "just regenerating" one when a digest fails.
#[test]
fn no_executable_depends_on_another_executable() {
    let executables = ["prod-synth", "ukiel-prod-load", "ukiel-prod-bench"];
    for exe in executables {
        let deps = closure(exe);
        for other in executables {
            if other == exe {
                continue;
            }
            assert!(
                !deps.contains(other),
                "{exe} depends on {other}. The tools are joined by the manifest, not by linking: \
                 a loader that can call the generator will eventually regenerate a fixture a \
                 report already cites."
            );
        }
    }
}

//! `ukiel-prod-bench` — measure an existing `prod-synth` load.
//!
//! **Read-only, and enforced.** It cannot generate a fixture, upload an object, create a
//! table, mutate a catalog, or clean anything up. If the load it was pointed at is
//! missing or incomplete it says so and stops.
//!
//! That restriction is the whole reason to have a third binary rather than a third
//! subcommand on the loader. A benchmark runner that can repair its own inputs will
//! eventually repair them — and then its numbers describe an input nobody chose, under a
//! seed nobody wrote down, and the report that quotes them is worthless in a way nobody
//! can see.
//!
//! Generation and loading are **facts read from the manifest and the catalog**, not
//! actions this process may take.

pub mod catalog;
pub mod queries;

use anyhow::{Result, bail};

/// Fail startup if the requested work would need a mutation.
///
/// Called before anything else, so the refusal costs nothing and cannot be half-done.
pub fn require_read_only(command: &str) -> Result<()> {
    // There is no code path in this binary that writes. This exists so that if one is
    // ever added, it has to argue with this function first — and so the *intent* is
    // stated somewhere a reader will find it.
    const MUTATING: &[&str] = &[
        "generate", "load", "create", "upload", "repair", "clean", "drop", "migrate", "seed",
    ];
    if MUTATING.contains(&command) {
        bail!(
            "'{command}' would mutate a fixture. ukiel-prod-bench is read-only: it measures a load \
             that already exists. Generate with `prod-synth`, load with `ukiel-prod-load`, and \
             measure here — the separation is what makes a report reproducible."
        );
    }
    Ok(())
}

/// The hypertable a label refers to. Must match `ukiel-prod-load`'s naming, and is
/// duplicated rather than imported because no executable package may depend on another:
/// the tools are joined by the manifest on disk, not by linking.
pub fn hypertable_name(label: &str) -> String {
    format!("prod_synth_events_{label}")
}

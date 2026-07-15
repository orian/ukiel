//! `ukiel-prod-load` — load one `prod-synth` manifest into an explicitly selected
//! Ukiel deployment.
//!
//! The only mutating tool in the pipeline, and the only one that touches a service.
//! Its side effects are declared by the command you run and by nothing else. It
//! cannot parse a profile, compile a topology, generate a row, or benchmark
//! anything: if the fixture is missing or corrupt it says so and stops. A loader
//! that can regenerate a fixture will eventually regenerate one a report already
//! cites, under a seed nobody wrote down.
//!
//! ## Two arms, and why they are different
//!
//! **`materialized`** puts real Parquet objects in the object store and registers
//! them through the product's ordinary write path — the same `PartMeta`, the same
//! `column_stats`, the same `commit(Add)`. It is the arm that can answer questions
//! about scans, planning and object behaviour, because there really are objects.
//! Fabricated part rows are forbidden here: a catalog row whose `size_bytes` is a
//! guess makes every byte-accounting number downstream a guess too.
//!
//! **`catalog-only`** seeds the same *truthful topology* with `size_bytes = 0` and
//! `catalog-only://` paths, and no objects at all. It measures catalog membership
//! geometry — range candidates versus filter candidates versus exact members — which
//! needs the graph and nothing else. It takes seconds instead of minutes, and it is
//! honest about what it cannot answer: nothing about scans, and nothing about bytes.
//!
//! The paths are deliberately un-fetchable. A `catalog-only://` object cannot be
//! opened by accident, and a background compactor that finds one will fail loudly
//! rather than quietly merging a file that was never there.

pub mod catalog_only;
pub mod compaction_input;
pub mod load;

pub use load::{LoadError, LoadReport, Loaded};

use anyhow::{Result, bail};
use ukiel_catalog::PostgresCatalog;

/// Refuse to write into a catalog that holds hypertables this experiment did not create,
/// unless the operator has explicitly named them as expected.
///
/// The part-shape experiment mutates a catalog and an object store, and it is only safe
/// on a stack you are willing to throw away. This is the tripwire: a catalog with a
/// stranger's `events` table in it is probably not that stack, and loading into it —
/// then, worse, cleaning up after — could touch data nobody meant to expose to a
/// benchmark. Every experiment hypertable is named `prod_synth_events_*`; anything else
/// must be on the allow-list or the load stops.
pub async fn require_disposable_catalog(catalog: &PostgresCatalog, allow: &[String]) -> Result<()> {
    let unexpected: Vec<String> = catalog
        .list_hypertables()
        .await?
        .into_iter()
        .map(|h| h.name)
        .filter(|n| !n.starts_with("prod_synth_events_") && !allow.contains(n))
        .collect();
    if !unexpected.is_empty() {
        bail!(
            "this catalog holds hypertables the part-shape experiment did not create: {unexpected:?}. \
             It may not be a disposable stack. Point --config at a throwaway catalog, or pass \
             --allow-hypertable for each one you have verified is safe to run alongside."
        );
    }
    Ok(())
}

/// The hypertable a fixture is loaded as: `prod_synth_events_<label>`.
///
/// The label is mandatory and must be fresh. Two fixtures in one catalog under one
/// name is how a benchmark ends up measuring a mixture of two seeds and reporting
/// the mean.
pub fn hypertable_name(label: &str) -> String {
    format!("prod_synth_events_{label}")
}

/// The object-store prefix a materialized fixture's parts live under.
pub fn object_prefix(label: &str) -> String {
    format!("prod-synth/{label}")
}

/// The scheme for a catalog-only part's path.
///
/// Not a real URL, and deliberately not fetchable. If something ever tries to open
/// one, it must fail immediately and audibly — never fall back to an empty read that
/// would look like an empty part.
pub const CATALOG_ONLY_SCHEME: &str = "catalog-only://";

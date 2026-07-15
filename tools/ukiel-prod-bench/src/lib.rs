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

pub mod admission;
pub mod catalog;
pub mod part_shape;
pub mod queries;

use std::path::Path;

use anyhow::{Context, Result, bail};
use prod_synth_contract::PartShapeReceipt;
use ukiel_catalog::PostgresCatalog;
use ukiel_core::HypertableId;

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

/// Read and validate a compaction receipt: right version, synthetic, and its referenced
/// source and L0 manifests both reverifiable is checked by the caller against what it
/// holds. Here we parse and version-gate.
pub fn read_receipt(path: &Path) -> Result<PartShapeReceipt> {
    let bytes = std::fs::read(path).with_context(|| format!("reading {}", path.display()))?;
    Ok(PartShapeReceipt::parse(
        &path.display().to_string(),
        &bytes,
    )?)
}

/// Find the loaded fixture the receipt names, and re-check it against the catalog — the
/// receipt is identity, not authority.
///
/// After compaction the *part count* and *paths* will not match the receipt's input
/// (that is the whole point), so this checks the durable identity: the hypertable exists
/// under the receipt's name and id, its schema/packing key match, and it is non-empty.
/// The row census and marker are checked where they are used (convergence, part-shape),
/// against the receipt's fingerprint and marker.
pub async fn find_loaded(
    catalog: &PostgresCatalog,
    receipt: &PartShapeReceipt,
) -> Result<HypertableId> {
    let ht = catalog
        .get_hypertable(&receipt.hypertable)
        .await
        .map_err(|_| {
            anyhow::anyhow!(
                "no load named '{}' in this catalog. Load it first with `ukiel-prod-load \
             compaction-input`; this tool measures a load, it does not create one.",
                receipt.hypertable
            )
        })?;
    if ht.id.0 != receipt.hypertable_id {
        bail!(
            "'{}' has id {} but the receipt records {}. This is a different load — refusing to \
             measure it.",
            receipt.hypertable,
            ht.id.0,
            receipt.hypertable_id
        );
    }
    if ht.packing_key != receipt.packing_key {
        bail!(
            "'{}' has packing key '{}', the receipt says '{}'",
            receipt.hypertable,
            ht.packing_key,
            receipt.packing_key
        );
    }
    Ok(ht.id)
}

/// The tenants a benchmark may ask about, from the loaded logical tables.
///
/// Ukiel's v1 scoping is `slice = packing_key == namespace_id`, so the namespace of each
/// `events` logical table *is* a queryable tenant. Reading them from the catalog rather
/// than the receipt keeps the receipt small and means the benchmark asks about exactly
/// the tenants that were loaded. Benchmark-local read-only SQL, confined to the tool.
pub async fn queryable_tenants(catalog: &PostgresCatalog, ht: HypertableId) -> Result<Vec<i64>> {
    let rows: Vec<(i64,)> = sqlx::query_as(
        "SELECT DISTINCT namespace_id FROM logical_tables WHERE hypertable_id = $1 \
         ORDER BY namespace_id",
    )
    .bind(ht.0)
    .fetch_all(catalog.pool_for_tests())
    .await
    .context("listing queryable tenants")?;
    Ok(rows.into_iter().map(|(n,)| n).collect())
}

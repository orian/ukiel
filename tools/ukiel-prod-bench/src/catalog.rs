//! The catalog arm: range candidates against filter candidates against exact members.
//!
//! This is the issue-0014 A/B, measured on production-shaped geometry. It reads; it
//! never writes.
//!
//! What the three numbers mean, and why all three are reported together:
//!
//! * **range** — what min/max pruning alone would ship. The catalog's range index says
//!   "this part's keys span 100..4500, and your tenant is 3000, so it might hold rows".
//!   For a packed file that is almost always a lie of omission: it spans the tenant, it
//!   does not contain it.
//! * **shipped** — what the catalog actually returns, once the per-part Bloom filter has
//!   rejected the parts it can *prove* do not hold the tenant. The gap from `range` is
//!   the work issue 0014 removed before a single row left PostgreSQL.
//! * **exact** — the truth, from the topology. The gap from `shipped` is what the Bloom
//!   filter's false positives still cost, and it is the honest ceiling on how much better
//!   this could get.
//!
//! Reporting only two of them would be a choice about which story to tell.

use std::collections::BTreeMap;
use std::time::Instant;

use anyhow::{Context, Result, bail};
use prod_synth_contract::{Manifest, Topology};
use ukiel_catalog::PostgresCatalog;
use ukiel_core::HypertableId;

#[derive(Debug, Clone, serde::Serialize)]
pub struct CatalogRow {
    pub tenant: i64,
    pub class: String,
    pub exact: u64,
    pub shipped: u64,
    pub range: u64,
    /// `range / exact` — what pruning would have over-fetched without the key filter.
    pub range_overfetch: f64,
    /// `shipped / exact` — what it still over-fetches with it.
    pub shipped_overfetch: f64,
    /// Bytes the shipped parts carry, from the catalog's own `size_bytes`.
    pub shipped_bytes: i64,
    pub rows: u64,
    /// Wall time of the catalog lookup that produced `shipped`.
    pub lookup_ms: f64,
}

/// Measure one loaded fixture's catalog geometry.
///
/// `range_only` reproduces the *pre*-issue-0014 behaviour by ignoring the key filter and
/// counting what the range predicate alone would have returned. It is computed from the
/// manifest rather than by asking the catalog to prune badly on purpose: the catalog no
/// longer has a range-only mode, and adding one so a benchmark could measure it would be
/// putting a worse product in the product to make a graph.
pub async fn measure(
    catalog: &PostgresCatalog,
    hypertable_id: HypertableId,
    manifest: &Manifest,
    topology: &Topology,
    tenants: &[(i64, String)],
) -> Result<Vec<CatalogRow>> {
    let mut exact: BTreeMap<i64, u64> = BTreeMap::new();
    for (tenant, _part, _rows) in topology.edges() {
        *exact.entry(tenant).or_default() += 1;
    }
    let rows_of: BTreeMap<i64, u64> = topology.tenants.iter().map(|t| (t.id, t.rows)).collect();
    let ranges: Vec<(i64, i64)> = manifest
        .parts
        .iter()
        .map(|p| (p.key_min, p.key_max))
        .collect();

    let mut out = Vec::new();
    for (tenant, class) in tenants {
        let range = ranges
            .iter()
            .filter(|(lo, hi)| *lo <= *tenant && *tenant <= *hi)
            .count() as u64;

        let start = Instant::now();
        let shipped_parts = catalog
            .live_parts_pruned(hypertable_id, Some(*tenant), &[])
            .await
            .with_context(|| format!("pruned lookup for tenant {tenant}"))?;
        let lookup_ms = start.elapsed().as_secs_f64() * 1000.0;

        let shipped = shipped_parts.len() as u64;
        let shipped_bytes: i64 = shipped_parts.iter().map(|p| p.meta.size_bytes).sum();
        let exact_n = *exact.get(tenant).unwrap_or(&0);

        // The one-directional guarantee, re-asserted at measurement time. If this ever
        // trips, the number next to it is not a performance result — it is a correctness
        // bug, and rows are missing from query results with nothing in any log.
        if shipped < exact_n {
            bail!(
                "tenant {tenant}: the catalog shipped {shipped} parts but the tenant is really in \
                 {exact_n}. FALSE NEGATIVE — the key filter dropped a part that holds rows."
            );
        }
        if shipped > range {
            bail!(
                "tenant {tenant}: the filter shipped {shipped} parts, more than the {range} the \
                 range predicate alone would have. A filter can only ever remove candidates."
            );
        }

        out.push(CatalogRow {
            tenant: *tenant,
            class: class.clone(),
            exact: exact_n,
            shipped,
            range,
            range_overfetch: range as f64 / exact_n.max(1) as f64,
            shipped_overfetch: shipped as f64 / exact_n.max(1) as f64,
            shipped_bytes,
            rows: *rows_of.get(tenant).unwrap_or(&0),
            lookup_ms,
        });
    }
    Ok(out)
}

/// The aggregate a report leads with.
#[derive(Debug, Clone, serde::Serialize)]
pub struct CatalogSummary {
    pub tenants: usize,
    pub total_exact: u64,
    pub total_shipped: u64,
    pub total_range: u64,
    /// How much of the range over-fetch the key filter removed, as a fraction of the
    /// candidates it *could* have removed. 1.0 would mean perfect pruning.
    pub overfetch_removed: f64,
    pub median_lookup_ms: f64,
}

pub fn summarize(rows: &[CatalogRow]) -> CatalogSummary {
    let total_exact: u64 = rows.iter().map(|r| r.exact).sum();
    let total_shipped: u64 = rows.iter().map(|r| r.shipped).sum();
    let total_range: u64 = rows.iter().map(|r| r.range).sum();

    // The removable over-fetch is (range - exact); the removed part is (range - shipped).
    let removable = total_range.saturating_sub(total_exact);
    let removed = total_range.saturating_sub(total_shipped);

    let mut lat: Vec<f64> = rows.iter().map(|r| r.lookup_ms).collect();
    lat.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let median = lat.get(lat.len() / 2).copied().unwrap_or(0.0);

    CatalogSummary {
        tenants: rows.len(),
        total_exact,
        total_shipped,
        total_range,
        overfetch_removed: if removable == 0 {
            1.0
        } else {
            removed as f64 / removable as f64
        },
        median_lookup_ms: median,
    }
}

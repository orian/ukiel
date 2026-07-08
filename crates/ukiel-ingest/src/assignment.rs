//! Which of a topic's Kafka partitions this ingest worker consumes, and where
//! each one starts.
//!
//! The rule is pure — no Kafka, no Postgres — so the ownership/positioning
//! logic is unit-testable and cannot drift as it grows. Today every worker
//! owns every partition ([`Ownership::All`]); the offset CAS
//! (`ingest_offsets`, issue 0003) is what keeps a mistakenly-duplicated writer
//! from corrupting data. Horizontal ingest (roadmap row 26,
//! `docs/notes/2026-07-07-ingest-scaleout.md`) narrows ownership to a leased
//! [`Ownership::Subset`] computed from catalog leases — it plugs in here and
//! nothing about offset positioning changes.

use std::collections::{HashMap, HashSet};

/// The partitions of a topic this worker is responsible for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Ownership {
    /// Every partition of the topic (single writer per topic; the plan-19
    /// offset CAS makes a duplicate writer fail loudly rather than corrupt).
    All,
    /// A leased subset (roadmap row 26). The set holds Kafka partition ids.
    Subset(HashSet<i32>),
}

impl Ownership {
    fn owns(&self, partition: i32) -> bool {
        match self {
            Ownership::All => true,
            Ownership::Subset(set) => set.contains(&partition),
        }
    }
}

/// A partition to consume and the offset to start at.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionStart {
    pub partition: i32,
    pub offset: i64,
}

/// Build the desired assignment: every partition that currently exists on the
/// topic *and* is owned, positioned at its catalog offset (or 0 for a
/// partition the catalog has never committed). Sorted by partition id so the
/// assignment is deterministic and tests are stable.
///
/// `existing` are the topic's partition ids from cluster metadata; `stored`
/// are the `ingest_offsets` positions for this `(hypertable, topic)`. A stored
/// offset for a partition that no longer exists is ignored — we only assign
/// partitions the cluster still reports.
pub fn desired_assignment(
    existing: &[i32],
    ownership: &Ownership,
    stored: &HashMap<i32, i64>,
) -> Vec<PartitionStart> {
    let mut out: Vec<PartitionStart> = existing
        .iter()
        .copied()
        .filter(|p| ownership.owns(*p))
        .map(|partition| PartitionStart {
            partition,
            offset: stored.get(&partition).copied().unwrap_or(0),
        })
        .collect();
    out.sort_by_key(|ps| ps.partition);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn start(partition: i32, offset: i64) -> PartitionStart {
        PartitionStart { partition, offset }
    }

    #[test]
    fn all_partitions_positioned_from_catalog_or_zero() {
        let existing = [2, 0, 1];
        let stored = HashMap::from([(0, 100), (2, 50)]); // partition 1 never committed
        let got = desired_assignment(&existing, &Ownership::All, &stored);
        // Sorted by partition; missing stored offset defaults to 0.
        assert_eq!(got, vec![start(0, 100), start(1, 0), start(2, 50)]);
    }

    #[test]
    fn subset_ownership_filters_out_unowned_partitions() {
        let existing = [0, 1, 2, 3];
        let stored = HashMap::from([(1, 10), (3, 30)]);
        let owned = Ownership::Subset(HashSet::from([1, 3]));
        let got = desired_assignment(&existing, &owned, &stored);
        assert_eq!(got, vec![start(1, 10), start(3, 30)]);
    }

    #[test]
    fn stored_offset_for_vanished_partition_is_ignored() {
        // Catalog remembers partition 5, but the cluster no longer reports it
        // (topic shrink / stale row). It must not be assigned.
        let existing = [0, 1];
        let stored = HashMap::from([(0, 7), (5, 999)]);
        let got = desired_assignment(&existing, &Ownership::All, &stored);
        assert_eq!(got, vec![start(0, 7), start(1, 0)]);
    }

    #[test]
    fn empty_subset_assigns_nothing() {
        let existing = [0, 1, 2];
        let stored = HashMap::from([(0, 1)]);
        let owned = Ownership::Subset(HashSet::new());
        assert!(desired_assignment(&existing, &owned, &stored).is_empty());
    }

    #[test]
    fn no_partitions_yields_empty_assignment() {
        let stored = HashMap::from([(0, 1)]);
        assert!(desired_assignment(&[], &Ownership::All, &stored).is_empty());
    }
}

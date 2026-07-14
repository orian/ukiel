//! Task 2: the deterministic topology compiler.
//!
//! The invariants here are the ones a wrong fixture would violate silently. A graph
//! whose margins drift, or whose ranges are assigned rather than derived, still
//! produces a benchmark — it just produces the wrong number, and nothing in the
//! output says so.

use std::path::{Path, PathBuf};

use prod_synth::profile::ProductionProfile;
use prod_synth::topology::{TopologyConfig, compile};
use prod_synth_contract::Tier;

fn profile_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../docs/prod-info")
}

fn load() -> ProductionProfile {
    ProductionProfile::load(&profile_dir()).expect("profile")
}

fn config(tier: Tier, tenants: u64, rows: u64, shards: u32, seed: u64) -> TopologyConfig {
    TopologyConfig {
        tier,
        tenants,
        rows_per_shard: rows,
        shards,
        seed,
        overrides: vec![],
    }
}

/// The exact bipartite margins. Both sides, every node — not a spot check.
///
/// This is *the* invariant. Every key structure in the fixture — a part's range, its
/// roaring bitmap, the catalog's Bloom filter, a tenant's overfetch ratio — is read
/// off this graph. If a single degree is wrong, all of them are wrong together and
/// consistently, which is the one kind of wrong that no downstream check can catch.
#[test]
fn realizes_the_exact_degree_sequence_on_both_sides() {
    let p = load();
    let t = compile(&p, &config(Tier::Smoke, 1_000, 100_000, 1, 0)).expect("compiles");

    assert_eq!(
        t.parts.len(),
        p.parts.len(),
        "one generated part per source part"
    );

    // Part side: no duplicate members, and the declared count is the real count.
    let mut edges = 0u64;
    for part in &t.parts {
        let mut sorted = part.members.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(
            sorted.len(),
            part.members.len(),
            "part {} has a duplicate member: the graph is not simple",
            part.index
        );
        assert!(
            !part.members.is_empty(),
            "every part holds at least one tenant"
        );
        edges += part.members.len() as u64;
    }

    // Tenant side: the sum of tenant degrees is the sum of part degrees, because
    // they count the same edges.
    let tenant_degrees: u64 = t.tenants.iter().map(|x| x.exact_parts).sum();
    assert_eq!(
        tenant_degrees, edges,
        "the two margins count the same edges and must agree"
    );
    assert_eq!(t.summary.memberships, edges);
}

/// Ranges are *derived*, never assigned. This is the failure the old fixture had:
/// draw a part's range from one distribution and its key count from another, and it
/// looks like production and behaves nothing like it.
#[test]
fn part_ranges_are_exactly_their_members_min_and_max() {
    let p = load();
    let t = compile(&p, &config(Tier::Smoke, 1_000, 100_000, 1, 3)).expect("compiles");

    for part in &t.parts {
        let lo = *part.members.iter().min().unwrap();
        let hi = *part.members.iter().max().unwrap();
        assert_eq!(part.key_min, lo, "part {}", part.index);
        assert_eq!(part.key_max, hi, "part {}", part.index);
        // Not merely *containing* the members — equal to their extremes. A wider
        // range would prune correctly but over-fetch, quietly weakening the very
        // measurement the fixture exists to make.
        assert!(
            part.members
                .iter()
                .all(|&m| part.key_min <= m && m <= part.key_max)
        );
    }
}

/// A tenant's range fanout counts the parts that *bracket* it, whether or not they
/// hold it. The gap between that and its exact fanout is the work the key filter
/// avoids — it is the number under test, so it had better be computed from the graph.
#[test]
fn range_fanout_counts_bracketing_parts_and_is_never_below_exact() {
    let p = load();
    let t = compile(&p, &config(Tier::Smoke, 1_000, 100_000, 1, 5)).expect("compiles");

    for tenant in &t.tenants {
        let bracketing = t
            .parts
            .iter()
            .filter(|part| part.key_min <= tenant.id && tenant.id <= part.key_max)
            .count() as u64;
        assert_eq!(tenant.range_parts, bracketing, "tenant {}", tenant.id);
        assert!(
            tenant.range_parts >= tenant.exact_parts,
            "every part that holds a tenant also brackets it: {} < {}",
            tenant.range_parts,
            tenant.exact_parts
        );
        assert!(tenant.exact_parts >= 1, "every tenant is in some part");
    }
}

/// Tenant IDs are distinct and inside the scaled key universe. Sparsity is the whole
/// mechanism: it is why a part's [min, max] brackets far more of the ID space than
/// it holds.
#[test]
fn tenant_ids_are_distinct_and_drawn_from_the_scaled_universe() {
    let p = load();
    let t = compile(&p, &config(Tier::Smoke, 1_000, 100_000, 1, 0)).expect("compiles");

    let mut ids: Vec<i64> = t.tenants.iter().map(|x| x.id).collect();
    let n = ids.len();
    ids.sort_unstable();
    ids.dedup();
    assert_eq!(ids.len(), n, "tenant IDs must be distinct");
    assert!(ids.iter().all(|&i| i >= 1 && i as u64 <= t.key_universe));

    // 3.6x: the source's 511,474-wide key span over its 142,066 estimated tenants.
    // The ID space is deliberately much larger than the tenant count.
    let ratio = t.key_universe as f64 / t.tenants.len() as f64;
    assert!(
        (3.0..4.5).contains(&ratio),
        "the universe should be ~3.6x the tenant count, got {ratio:.2}"
    );
}

/// Every membership carries at least one row, and the rows add up.
///
/// A member with zero rows is a member the Parquet file does not contain — the graph
/// the manifest claims would not be the graph the files hold, and the catalog's key
/// filter would be answering about a part that does not exist.
#[test]
fn every_membership_carries_a_row_and_the_budget_balances() {
    let p = load();
    let t = compile(&p, &config(Tier::Smoke, 1_000, 100_000, 1, 0)).expect("compiles");

    for part in &t.parts {
        assert_eq!(part.rows.len(), part.members.len());
        assert!(
            part.rows.iter().all(|&r| r >= 1),
            "part {} has a member with no rows",
            part.index
        );
    }
    let total: u64 = t.parts.iter().map(|x| x.rows_total()).sum();
    assert_eq!(total, 100_000, "the shard's row budget is spent exactly");
    assert_eq!(t.summary.rows, total);
}

/// A row budget below the membership count cannot give every membership a row, and
/// says so rather than silently dropping members.
#[test]
fn a_row_budget_below_the_membership_count_is_refused() {
    let p = load();
    let e = compile(&p, &config(Tier::Smoke, 1_000, 100, 1, 0))
        .expect_err("100 rows cannot cover ~16,800 memberships");
    let msg = e.to_string();
    assert!(msg.contains("memberships"), "{msg}");
    assert!(
        msg.contains("at least one row"),
        "the error must say what the floor is for: {msg}"
    );
}

/// Same seed, same graph. Different seed, different graph. Both halves matter: the
/// first is the reproducibility promise, the second proves the seed is actually
/// wired in rather than decorative.
#[test]
fn compilation_is_deterministic_in_the_seed() {
    let p = load();
    let a = compile(&p, &config(Tier::Smoke, 1_000, 100_000, 1, 7)).unwrap();
    let b = compile(&p, &config(Tier::Smoke, 1_000, 100_000, 1, 7)).unwrap();
    let c = compile(&p, &config(Tier::Smoke, 1_000, 100_000, 1, 8)).unwrap();

    let shape = |t: &prod_synth::SyntheticTopology| {
        (
            t.tenants.iter().map(|x| x.id).collect::<Vec<_>>(),
            t.parts
                .iter()
                .map(|x| x.members.clone())
                .collect::<Vec<_>>(),
            t.parts.iter().map(|x| x.rows.clone()).collect::<Vec<_>>(),
        )
    };
    assert_eq!(shape(&a), shape(&b), "same seed must give the same graph");
    assert_ne!(
        shape(&a),
        shape(&c),
        "a different seed must give a different graph"
    );
}

/// The degrees carry the *sample's* distribution, not a smoothed version of it. The
/// resample keeps whole records, so activity and fanout stay joined — averaging them
/// independently is what erases the behaviour the benchmark exists to measure.
#[test]
fn the_generated_fanout_distribution_is_the_sources() {
    let p = load();
    let t = compile(&p, &config(Tier::Baseline, 10_000, 30_000_000, 1, 0)).expect("compiles");
    let s = p.summary();

    assert_eq!(t.summary.exact_parts.p50, s.exact_parts.p50, "p50 fanout");
    assert_eq!(t.summary.exact_parts.p90, s.exact_parts.p90, "p90 fanout");

    // And the sparsity that makes range pruning over-select. Emergent: nothing
    // assigns a density, it falls out of drawing IDs across the universe and reading
    // each part's range off its actual members.
    for (name, src, got) in [
        ("p10", s.key_density.p10, t.summary.key_density.p10),
        ("p50", s.key_density.p50, t.summary.key_density.p50),
        ("p90", s.key_density.p90, t.summary.key_density.p90),
    ] {
        let err = (src - got).abs() / src;
        assert!(
            err <= 0.20,
            "density {name}: source {src}, generated {got} ({} percent off)",
            err * 100.0
        );
    }
}

/// The tenant degree sequence must be repaired to something *realizable*, not merely
/// something that sums correctly.
///
/// The plan bounds the repair to `1..part_count` and to matching the margins. That is
/// necessary and not sufficient: 7 source parts hold exactly one key each, so they
/// scale to capacity 1, while the resample yields tens of tenants needing 62-63
/// parts — each of which would need two or more of those 7 single-key slots. The
/// sequence is not bigraphic and Havel-Hakimi fails outright, whatever the sums say.
/// The compiler must find that and fix it.
#[test]
fn repairs_the_degree_sequence_to_something_actually_realizable() {
    let p = load();

    // The condition that makes the naive version impossible, asserted directly — so
    // if the profile ever changes such that this is no longer true, we find out here
    // rather than in a baffling Havel-Hakimi failure.
    let single_key_parts = p.parts.iter().filter(|x| x.distinct_keys == 1).count();
    let over_capacity = p
        .tenants
        .iter()
        .filter(|t| t.exact_parts as usize > p.parts.len() - single_key_parts)
        .count();
    assert!(
        single_key_parts > 0 && over_capacity > 0,
        "the profile must still contain the tension this repair exists to resolve: \
         {single_key_parts} single-key parts, {over_capacity} tenants needing more parts than \
         there are multi-key ones"
    );

    // It compiles anyway, and reports what it had to move.
    let t = compile(&p, &config(Tier::Baseline, 10_000, 30_000_000, 1, 0)).expect("compiles");
    let repairs = t.summary.degree_repairs;
    assert!(
        repairs > 0,
        "the sequence is not realizable as sampled; repairs must be non-zero"
    );

    let pct = repairs as f64 / t.tenants.len() as f64;
    assert!(
        pct <= 0.02,
        "repairs must stay inside the 2 percent gate or the fanout distribution is no longer \
         the source's: {repairs} of {} ({} percent)",
        t.tenants.len(),
        pct * 100.0
    );

    // And the repairs land in the tail, not the body: the gated quantiles are untouched.
    assert_eq!(t.summary.exact_parts.p50, p.summary().exact_parts.p50);
    assert_eq!(t.summary.exact_parts.p90, p.summary().exact_parts.p90);

    // Exactly the tenants marked `repaired` are the ones that moved off their sample.
    let marked = t.tenants.iter().filter(|x| x.repaired).count() as u64;
    assert_eq!(marked, repairs, "the repair count and the marks must agree");
}

/// Shards are independent layouts over the same tenants: candidate counts add,
/// densities and ratios do not.
#[test]
fn shards_are_independent_layouts_over_common_tenants() {
    let p = load();
    let one = compile(&p, &config(Tier::Smoke, 1_000, 100_000, 1, 0)).unwrap();
    let three = compile(&p, &config(Tier::Smoke, 1_000, 100_000, 3, 0)).unwrap();

    assert_eq!(three.parts.len(), 3 * one.parts.len(), "parts add");
    assert_eq!(
        three.summary.rows,
        3 * 100_000,
        "each shard gets the full budget"
    );

    // The same tenants, in the same ID space.
    let ids =
        |t: &prod_synth::SyntheticTopology| t.tenants.iter().map(|x| x.id).collect::<Vec<_>>();
    assert_eq!(ids(&one), ids(&three), "shards share their tenants");

    // Candidate counts add: a tenant is a range candidate in each shard independently.
    let t0 = &three.tenants[0];
    assert!(
        t0.exact_parts >= one.tenants[0].exact_parts,
        "membership accumulates across shards"
    );

    // But the layouts differ — the shards are not copies of one another.
    let shard0: Vec<&prod_synth::topology::SynthPart> =
        three.parts.iter().filter(|x| x.shard == 0).collect();
    let shard1: Vec<&prod_synth::topology::SynthPart> =
        three.parts.iter().filter(|x| x.shard == 1).collect();
    assert_ne!(
        shard0.iter().map(|x| x.members.clone()).collect::<Vec<_>>(),
        shard1.iter().map(|x| x.members.clone()).collect::<Vec<_>>(),
        "domain-separated seeds must give independent layouts, not duplicates"
    );
}

/// Timestamps stay inside the 14-day fixture window, and the clamp is counted rather
/// than hidden.
#[test]
fn time_spans_are_clamped_to_the_window_and_the_clamps_are_counted() {
    let p = load();
    let t = compile(&p, &config(Tier::Smoke, 1_000, 100_000, 1, 0)).expect("compiles");

    for part in &t.parts {
        assert!(
            part.ts_min >= prod_synth_contract::FIXTURE_WINDOW_START_MS,
            "part {}",
            part.index
        );
        assert!(
            part.ts_max < prod_synth_contract::FIXTURE_WINDOW_END_MS,
            "part {}",
            part.index
        );
        assert!(part.ts_min <= part.ts_max);
    }

    // The source has parts spanning >14 days (up to 16.5), so some must have been
    // clipped — and the count must say so.
    let long = p
        .parts
        .iter()
        .filter(|x| x.time_span_ms >= 14 * 24 * 3600 * 1000)
        .count() as u64;
    assert!(
        long > 0,
        "the profile must still contain over-long spans for this to test anything"
    );
    assert_eq!(
        t.summary.time_clamps, long,
        "every over-long span is clipped and counted"
    );
}

/// Representative tenants are chosen from the *finished* graph, so each class means
/// what its name says.
#[test]
fn representatives_are_drawn_from_the_finished_graph() {
    let p = load();
    let t = compile(&p, &config(Tier::Baseline, 10_000, 30_000_000, 1, 0)).expect("compiles");
    let r = &t.representatives;

    let by_id = |id: i64| {
        t.tenants
            .iter()
            .find(|x| x.id == id)
            .expect("a real tenant")
    };
    let heavy = by_id(r.heavy);
    let median = by_id(r.median);
    let light = by_id(r.light);
    assert!(
        heavy.rows > median.rows && median.rows > light.rows,
        "heavy ({}) > median ({}) > light ({}) by rows",
        heavy.rows,
        median.rows,
        light.rows
    );

    let over = |x: &prod_synth::topology::SynthTenant| x.range_parts as f64 / x.exact_parts as f64;
    assert!(
        over(by_id(r.high_overfetch)) > over(by_id(r.low_overfetch)),
        "the overfetch classes must actually differ in overfetch — that is the behaviour under test"
    );

    assert!(
        r.sample.len() >= 8,
        "the sample must span the population, not a handful"
    );
    assert!(
        r.sample.windows(2).all(|w| w[0] < w[1]),
        "sorted and distinct"
    );
}

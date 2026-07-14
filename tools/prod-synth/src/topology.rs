//! Compile the profile into one consistent membership graph.
//!
//! This is the heart of plan 45, and the reason the previous fixture answered the
//! wrong question. Everything about keys — a part's `key_min`/`key_max`, its
//! density, a tenant's exact and range fanout, the writer's roaring bitmap, the
//! catalog's Bloom filter — is *derived from one bipartite graph*. Nothing is
//! assigned independently. Draw a part's range from one distribution and its key
//! count from another and you get a fixture that looks like the production numbers
//! and behaves nothing like them.
//!
//! The algorithm, in the plan's order:
//!
//! 1. Scale each part's degree and span by `generated_tenants / estimated_source_tenants`,
//!    with largest-remainder allocation so rounding does not drift the total.
//! 2. Resample complete tenant records — activity and fanout stay joined.
//! 3. Repair the tenant degrees until the two margins agree **and the sequence is
//!    realizable at all** (see [`repair`] — the plan's bound is necessary but not
//!    sufficient, and the naive version simply cannot run).
//! 4. Realize the exact degree sequence with bipartite Havel-Hakimi, then swap
//!    edges to wash out construction order.
//! 5. Draw tenant IDs from a scaled key universe, and only *then* read each part's
//!    range off its actual members. Sparsity emerges; it is never assigned.

use std::collections::HashSet;

use prod_synth_contract::{
    FIXTURE_WINDOW_DAYS, FIXTURE_WINDOW_START_MS, GeneratedSummary, Quantiles, Representatives,
    Tier,
};

use crate::profile::{ProductionProfile, quantile};
use crate::rng::StableRng;

#[derive(Debug, thiserror::Error)]
pub enum TopologyError {
    #[error(
        "the tenant degree sequence cannot be realized against these part capacities even after \
         repair. {0}"
    )]
    NotBigraphic(String),
    #[error(
        "--rows-per-shard {rows} is below the {required} memberships this topology has: every \
         part-tenant membership must carry at least one row, or the graph the fixture claims is \
         not the graph its files hold."
    )]
    TooFewRows { rows: u64, required: u64 },
    #[error(
        "--tenants {tenants} exceeds the key universe of {universe} the profile implies. Widen \
         the universe or lower the tenant count; IDs must stay distinct."
    )]
    UniverseTooSmall { tenants: u64, universe: u64 },
}

type Result<T> = std::result::Result<T, TopologyError>;

#[derive(Debug, Clone)]
pub struct TopologyConfig {
    pub tier: Tier,
    pub tenants: u64,
    pub rows_per_shard: u64,
    pub shards: u32,
    pub seed: u64,
    pub overrides: Vec<String>,
}

/// One part, after realization. Every key field here was *read off the graph*.
#[derive(Debug, Clone)]
pub struct SynthPart {
    pub index: u32,
    pub shard: u32,
    /// Tenant IDs actually present, ascending.
    pub members: Vec<i64>,
    /// Rows per member, aligned with `members`.
    pub rows: Vec<u64>,
    pub key_min: i64,
    pub key_max: i64,
    pub ts_min: i64,
    pub ts_max: i64,
    /// Index into `profile.parts` — provenance, nothing more.
    pub source: usize,
}

impl SynthPart {
    pub fn rows_total(&self) -> u64 {
        self.rows.iter().sum()
    }
    pub fn distinct_keys(&self) -> u64 {
        self.members.len() as u64
    }
    pub fn key_span(&self) -> i64 {
        self.key_max - self.key_min + 1
    }
    pub fn key_density(&self) -> f64 {
        self.distinct_keys() as f64 / self.key_span() as f64
    }
}

#[derive(Debug, Clone)]
pub struct SynthTenant {
    pub id: i64,
    pub rows: u64,
    pub sample_rows: u64,
    pub sample_exact_parts: u64,
    pub exact_parts: u64,
    pub range_parts: u64,
    pub repaired: bool,
}

#[derive(Debug, Clone)]
pub struct SyntheticTopology {
    pub config: TopologyConfig,
    pub key_universe: u64,
    pub tenants: Vec<SynthTenant>,
    pub parts: Vec<SynthPart>,
    pub summary: GeneratedSummary,
    pub representatives: Representatives,
}

/// Compile the profile and config into one realized graph.
pub fn compile(profile: &ProductionProfile, config: &TopologyConfig) -> Result<SyntheticTopology> {
    let source_tenants = profile.estimated_active_tenants();
    let scale = config.tenants as f64 / source_tenants as f64;

    // ---- 1. Key universe -------------------------------------------------
    //
    // Scaled from the source's widest observed key span, so tenant IDs are as
    // sparse relative to each other as production's are. This single number is
    // what makes range pruning over-select: a part's members are spread across
    // it, so the part's [min, max] brackets far more of the ID space than it
    // holds. Assigning ranges directly would be assuming the answer.
    let universe = ((profile.parts.iter().map(|p| p.key_span).max().unwrap_or(1) as f64) * scale)
        .round()
        .max(config.tenants as f64) as u64;
    if config.tenants > universe {
        return Err(TopologyError::UniverseTooSmall {
            tenants: config.tenants,
            universe,
        });
    }

    // ---- 2. Scaled part degrees -----------------------------------------
    let part_degrees = scale_part_degrees(profile, config.tenants, scale);
    let edges: u64 = part_degrees.iter().sum::<u64>();

    // ---- 3. Resampled tenant degrees, then repaired ----------------------
    let samples = resample_tenants(profile, config);
    let mut degrees: Vec<u64> = samples.iter().map(|s| s.exact_parts).collect();
    // The repair count is *reported*, not enforced here. Whether it is acceptable is
    // a question about the generated distribution, and the plan asks that question
    // of `baseline` only — a 1,000-tenant smoke fixture cannot reproduce a
    // distribution and should not be asked to. See `fidelity::check`, which carries
    // the 2% gate, and `generate`, which enforces it at the tier that means it.
    let repaired = repair(&mut degrees, &part_degrees, edges, config.seed)?;

    // ---- 4. Rows: every membership carries at least one -------------------
    if config.rows_per_shard < edges {
        return Err(TopologyError::TooFewRows {
            rows: config.rows_per_shard,
            required: edges,
        });
    }

    // ---- 5. Tenant IDs from the universe ---------------------------------
    //
    // Drawn uniformly, and assigned to tenants in index order. Index order is
    // already a shuffle of the sample (see `resample_tenants`), so a tenant's ID
    // carries no information about its degree or activity — which is what keeps
    // the ID space from accidentally encoding the very skew we want the graph to
    // express.
    let mut id_rng = StableRng::stream(config.seed, "tenant-ids");
    let ids: Vec<i64> = id_rng
        .sample_distinct(1, universe, config.tenants as usize)
        .into_iter()
        .map(|v| v as i64)
        .collect();

    // ---- 6. Realize the graph, per shard ---------------------------------
    let mut parts: Vec<SynthPart> = Vec::new();
    for shard in 0..config.shards {
        // Domain-separated per shard: shards are independent layouts over the
        // *same* tenants, so candidate counts add while densities and ratios do
        // not. Sharing a stream would correlate them.
        let shard_seed = StableRng::stream(config.seed, &format!("shard-{shard}")).next_u64();
        let membership = realize(&part_degrees, &degrees, shard_seed)?;

        for (pi, members) in membership.into_iter().enumerate() {
            let mut member_ids: Vec<i64> = members.iter().map(|&t| ids[t as usize]).collect();
            member_ids.sort_unstable();
            let key_min = *member_ids.first().expect("every part has a member");
            let key_max = *member_ids.last().expect("every part has a member");
            parts.push(SynthPart {
                index: parts.len() as u32,
                shard,
                members: member_ids,
                rows: Vec::new(), // filled below, once the whole shard is known
                key_min,
                key_max,
                ts_min: 0,
                ts_max: 0,
                source: pi,
            });
        }
    }

    // ---- 7. Rows and time, from the joint activity weights ----------------
    let (tenant_rows, time_clamps) = allocate(profile, config, &mut parts, &ids);

    // ---- 8. Derive every reported figure from the finished graph ----------
    let mut tenants: Vec<SynthTenant> = ids
        .iter()
        .enumerate()
        .map(|(i, &id)| SynthTenant {
            id,
            rows: tenant_rows[i],
            sample_rows: samples[i].tenant_rows,
            sample_exact_parts: samples[i].exact_parts,
            exact_parts: 0,
            range_parts: 0,
            repaired: repaired.contains(&i),
        })
        .collect();

    let index_of: std::collections::HashMap<i64, usize> =
        ids.iter().enumerate().map(|(i, &id)| (id, i)).collect();
    for p in &parts {
        for m in &p.members {
            tenants[index_of[m]].exact_parts += 1;
        }
    }
    // Range fanout: how many parts *bracket* the tenant, whether or not they hold
    // it. This is the number the catalog's range index would return, and the gap
    // between it and `exact_parts` is precisely the work the key filter avoids.
    let ranges: Vec<(i64, i64)> = parts.iter().map(|p| (p.key_min, p.key_max)).collect();
    for t in &mut tenants {
        t.range_parts = ranges
            .iter()
            .filter(|(lo, hi)| *lo <= t.id && t.id <= *hi)
            .count() as u64;
    }

    let summary = summarize(
        &tenants,
        &parts,
        universe,
        repaired.len() as u64,
        time_clamps,
    );
    let representatives = choose_representatives(&tenants, config.seed);

    Ok(SyntheticTopology {
        config: config.clone(),
        key_universe: universe,
        tenants,
        parts,
        summary,
        representatives,
    })
}

/// Scale part degrees, preserving the total exactly.
///
/// Largest-remainder allocation: floor everything, then hand the shortfall to the
/// parts with the biggest discarded fractions. Rounding each part independently
/// would drift the total by tens of edges, and the total is one of the two margins
/// the tenant side has to match.
fn scale_part_degrees(profile: &ProductionProfile, tenants: u64, scale: f64) -> Vec<u64> {
    let target = (profile.memberships() as f64 * scale).round() as u64;

    // A part can hold no more keys than its scaled span, and no more than there
    // are tenants; it holds at least one, or it is not a part.
    let bounds: Vec<(u64, u64)> = profile
        .parts
        .iter()
        .map(|p| {
            let span = ((p.key_span as f64 * scale).round() as u64).max(1);
            (1, span.min(tenants))
        })
        .collect();

    let ideal: Vec<f64> = profile
        .parts
        .iter()
        .zip(&bounds)
        .map(|(p, &(lo, hi))| (p.distinct_keys as f64 * scale).clamp(lo as f64, hi as f64))
        .collect();

    let mut out: Vec<u64> = ideal
        .iter()
        .zip(&bounds)
        .map(|(v, &(lo, hi))| (v.floor() as u64).clamp(lo, hi))
        .collect();

    // The shortfall, handed out by descending discarded fraction. Bounded by what
    // each part can still take, so the clamp above cannot be violated.
    let mut order: Vec<usize> = (0..out.len()).collect();
    order.sort_by(|&a, &b| {
        let fa = ideal[a] - ideal[a].floor();
        let fb = ideal[b] - ideal[b].floor();
        fb.partial_cmp(&fa)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(a.cmp(&b)) // ties by index: deterministic, never by float noise
    });

    let mut current: u64 = out.iter().sum();
    let mut i = 0;
    while current < target && i < order.len() * 64 {
        let k = order[i % order.len()];
        if out[k] < bounds[k].1 {
            out[k] += 1;
            current += 1;
        }
        i += 1;
    }
    while current > target {
        let mut moved = false;
        for &k in &order {
            if current == target {
                break;
            }
            if out[k] > bounds[k].0 {
                out[k] -= 1;
                current -= 1;
                moved = true;
            }
        }
        if !moved {
            break; // every part is at its floor; the target is below the minimum.
        }
    }
    out
}

#[derive(Debug, Clone)]
struct Sample {
    tenant_rows: u64,
    exact_parts: u64,
}

/// Deterministic stratified resample of *complete* tenant records.
///
/// Systematic sampling over the records sorted by (exact_parts, tenant_rows): for
/// generated tenant `j`, take source record `floor(j * n / T)`. That reproduces the
/// empirical joint distribution exactly when `T` is a multiple of `n` and to within
/// one record otherwise — no rejection, no acceptance ratio, no seed-dependent
/// wobble in the marginals.
///
/// Then shuffle, so tenant *index* carries no information about degree. Index order
/// becomes ID order downstream, and a fixture whose low IDs were all its heavy
/// tenants would hand the range predicate a correlation production does not have.
fn resample_tenants(profile: &ProductionProfile, config: &TopologyConfig) -> Vec<Sample> {
    let mut sorted: Vec<&crate::profile::TenantObservation> = profile.tenants.iter().collect();
    sorted.sort_by_key(|t| (t.exact_parts, t.tenant_rows, t.range_parts));

    let n = sorted.len() as u64;
    let mut out: Vec<Sample> = (0..config.tenants)
        .map(|j| {
            let src = sorted[((j * n) / config.tenants) as usize];
            Sample {
                tenant_rows: src.tenant_rows.max(1) as u64,
                exact_parts: src.exact_parts.max(1) as u64,
            }
        })
        .collect();

    StableRng::stream(config.seed, "tenant-resample").shuffle(&mut out);
    out
}

/// Bring the tenant degrees to a sequence that (a) sums to the part margin and
/// (b) can actually be realized against these part capacities.
///
/// **The plan specifies only (a), bounded to `1..part_count`. That is necessary
/// but not sufficient, and a generator that implements it literally cannot run.**
///
/// The profile has 7 parts holding exactly one key each — they scale to capacity 1
/// and stay there — and a tenant sample in which ~0.4% of tenants are in 62-63
/// parts. Resampled to 10,000 tenants that is ~37 tenants each needing at least two
/// of those 7 single-key slots. There are 7. The sequence is not bigraphic and
/// Havel-Hakimi fails outright, whatever the sums say.
///
/// (This is a symptom of a real inconsistency in the source: `tenant-fanout.jsonl`
/// reports up to 63 range candidates while `part-geometry.jsonl` holds only 61
/// parts with any range width, so the two files were captured at different part
/// scopes. See `ProductionProfile::scope_inconsistency`.)
///
/// So the bound here is the Gale-Ryser condition — a bipartite degree sequence is
/// realizable iff, for every `k`, the `k` largest tenant degrees sum to no more
/// than `sum_j min(capacity_j, k)`. Repairs are the smallest deterministic changes
/// that satisfy it, they are all recorded, and the 2% ceiling still applies: in
/// practice it costs ~1.2% of tenants at baseline, all of them in the extreme tail,
/// and it moves neither the p50 nor the p90 the fidelity gates are stated in.
fn repair(
    degrees: &mut [u64],
    capacities: &[u64],
    target_sum: u64,
    seed: u64,
) -> Result<HashSet<usize>> {
    let part_count = capacities.len() as u64;
    let mut repaired = HashSet::new();

    // `rhs(k) = sum_j min(capacity_j, k)`: the most edges any k tenants can
    // possibly absorb, given that each part can serve a given tenant at most once.
    let caps: Vec<u64> = capacities.to_vec();
    let rhs = |k: u64| -> u64 { caps.iter().map(|&c| c.min(k)).sum() };

    for d in degrees.iter_mut() {
        *d = (*d).clamp(1, part_count);
    }

    // Gale-Ryser, repaired from the top down. Only the high-degree head can
    // violate it (for small k the right-hand side grows by the multi-key part
    // count per step, while the left grows by a near-maximal degree), so this
    // converges in a few passes over a short prefix.
    loop {
        let mut order: Vec<usize> = (0..degrees.len()).collect();
        order.sort_by_key(|&i| std::cmp::Reverse((degrees[i], i)));

        let mut violated = None;
        let mut running = 0u64;
        for (k, &i) in order.iter().enumerate() {
            running += degrees[i];
            let k = k as u64 + 1;
            if running > rhs(k) {
                violated = Some(i);
                break;
            }
            // Once the prefix sum can no longer catch the right-hand side, no
            // larger k can violate: degrees are sorted descending and bounded by
            // part_count, while rhs keeps growing.
            if k > part_count && running + (degrees.len() as u64 - k) * part_count <= rhs(k) {
                break;
            }
        }
        let Some(i) = violated else { break };
        if degrees[i] <= 1 {
            return Err(TopologyError::NotBigraphic(format!(
                "even a degree-1 tenant cannot be placed: part capacities sum to {}, tenants {}",
                caps.iter().sum::<u64>(),
                degrees.len()
            )));
        }
        degrees[i] -= 1;
        repaired.insert(i);
    }

    // Now the sum. Deterministic visiting order, and every increment is re-checked
    // against feasibility so fixing the sum cannot un-fix realizability.
    let mut order: Vec<usize> = (0..degrees.len()).collect();
    StableRng::stream(seed, "degree-repair").shuffle(&mut order);

    let mut current: u64 = degrees.iter().sum();
    let mut cursor = 0usize;
    let mut stalled = 0usize;
    while current != target_sum {
        if stalled > degrees.len() {
            return Err(TopologyError::NotBigraphic(format!(
                "tenant degrees sum to {current} but the part margin is {target_sum}, and no \
                 feasible single-degree change closes the gap"
            )));
        }
        let i = order[cursor % order.len()];
        cursor += 1;

        if current < target_sum {
            if degrees[i] < part_count {
                degrees[i] += 1;
                if feasible(degrees, &rhs, part_count) {
                    current += 1;
                    repaired.insert(i);
                    stalled = 0;
                    continue;
                }
                degrees[i] -= 1;
            }
        } else if degrees[i] > 1 {
            degrees[i] -= 1;
            current -= 1;
            repaired.insert(i);
            stalled = 0;
            continue;
        }
        stalled += 1;
    }

    Ok(repaired)
}

/// Gale-Ryser, checked over the prefix that can actually violate it.
fn feasible(degrees: &[u64], rhs: &impl Fn(u64) -> u64, part_count: u64) -> bool {
    let mut top: Vec<u64> = degrees.to_vec();
    // Only the head matters, and the head is short: a tenant's degree is bounded
    // by the part count, so beyond ~part_count entries the right-hand side has
    // outrun anything the left can add.
    let head = (part_count as usize * 4).min(top.len());
    top.sort_unstable_by(|a, b| b.cmp(a));
    let mut running = 0u64;
    for (k, d) in top.iter().take(head).enumerate() {
        running += d;
        if running > rhs(k as u64 + 1) {
            return false;
        }
    }
    true
}

/// Bipartite Havel-Hakimi, then degree-preserving edge swaps.
///
/// Havel-Hakimi alone is correct but *ordered*: it hands the highest-degree tenant
/// the highest-capacity parts, so membership ends up correlated with degree in a way
/// nothing in the source implies. The swaps wash that out while preserving both
/// margins exactly — a swap exchanges the parts of two edges, so every degree on
/// both sides is untouched by construction.
fn realize(part_degrees: &[u64], tenant_degrees: &[u64], seed: u64) -> Result<Vec<Vec<u32>>> {
    let p = part_degrees.len();
    let t = tenant_degrees.len();
    let mut capacity: Vec<u64> = part_degrees.to_vec();
    let mut members: Vec<Vec<u32>> = vec![Vec::new(); p];

    // A bitset per tenant: `part_count` is small (68), so membership is two u64s
    // and a swap's "does this edge already exist" check is a couple of ANDs. A
    // HashSet per tenant would dominate the runtime at the shape tier.
    let words = p.div_ceil(64);
    let mut bits: Vec<u64> = vec![0; t * words];
    let has = |bits: &[u64], ti: usize, pi: usize| bits[ti * words + pi / 64] >> (pi % 64) & 1 == 1;
    let set = |bits: &mut [u64], ti: usize, pi: usize| bits[ti * words + pi / 64] |= 1 << (pi % 64);
    let clear =
        |bits: &mut [u64], ti: usize, pi: usize| bits[ti * words + pi / 64] &= !(1 << (pi % 64));

    // Descending tenant degree: the Gale-Ryser proof is constructive in exactly
    // this order, and taking the hardest tenant first is what makes it work.
    let mut order: Vec<usize> = (0..t).collect();
    order.sort_by_key(|&i| std::cmp::Reverse((tenant_degrees[i], i)));

    let mut slot: Vec<usize> = (0..p).collect();
    for &ti in &order {
        let need = tenant_degrees[ti] as usize;
        // The `need` parts with the most capacity left. Ties by index, so the
        // choice never depends on sort stability.
        slot.sort_by_key(|&pi| std::cmp::Reverse((capacity[pi], p - pi)));
        if need > p || slot.iter().take(need).any(|&pi| capacity[pi] == 0) {
            return Err(TopologyError::NotBigraphic(format!(
                "tenant needs {need} parts but only {} have capacity left",
                slot.iter().filter(|&&pi| capacity[pi] > 0).count()
            )));
        }
        for &pi in slot.iter().take(need) {
            capacity[pi] -= 1;
            members[pi].push(ti as u32);
            set(&mut bits, ti, pi);
        }
    }
    if capacity.iter().any(|&c| c != 0) {
        return Err(TopologyError::NotBigraphic(
            "part capacities left unfilled: the two margins do not agree".into(),
        ));
    }

    // Flatten to an edge list and swap. 10 attempts per edge is the usual
    // mixing rule of thumb for degree-preserving rewiring; failures (a swap that
    // would duplicate an edge) are simply skipped, which is what keeps the graph
    // simple.
    let mut edges: Vec<(u32, u32)> = Vec::new();
    for (pi, m) in members.iter().enumerate() {
        for &ti in m {
            edges.push((ti, pi as u32));
        }
    }
    let mut rng = StableRng::stream(seed, "edge-swap");
    let attempts = edges.len().saturating_mul(10);
    for _ in 0..attempts {
        let a = rng.below(edges.len() as u64) as usize;
        let b = rng.below(edges.len() as u64) as usize;
        let (t1, p1) = edges[a];
        let (t2, p2) = edges[b];
        if t1 == t2 || p1 == p2 {
            continue;
        }
        // The swap must not create an edge that already exists, or the graph
        // stops being simple and a "membership" would be double-counted.
        if has(&bits, t1 as usize, p2 as usize) || has(&bits, t2 as usize, p1 as usize) {
            continue;
        }
        clear(&mut bits, t1 as usize, p1 as usize);
        clear(&mut bits, t2 as usize, p2 as usize);
        set(&mut bits, t1 as usize, p2 as usize);
        set(&mut bits, t2 as usize, p1 as usize);
        edges[a] = (t1, p2);
        edges[b] = (t2, p1);
    }

    let mut out: Vec<Vec<u32>> = vec![Vec::new(); p];
    for (ti, pi) in edges {
        out[pi as usize].push(ti);
    }
    for m in &mut out {
        m.sort_unstable();
        debug_assert!(m.windows(2).all(|w| w[0] != w[1]), "no duplicate edges");
    }
    Ok(out)
}

/// Rows and time spans, from the sampled activity weights.
///
/// Every membership gets at least one row — the graph the manifest claims must be
/// the graph the files hold, and a member with zero rows is a member the Parquet
/// file does not contain. The remaining budget goes out by tenant activity weight,
/// so the three-orders-of-magnitude skew survives, and each part keeps its share of
/// the source's `observed_rows` so big parts stay big.
///
/// Returns the per-tenant row totals and the number of parts whose sampled time
/// span had to be clipped to the 14-day window.
fn allocate(
    profile: &ProductionProfile,
    config: &TopologyConfig,
    parts: &mut [SynthPart],
    ids: &[i64],
) -> (Vec<u64>, u64) {
    let index_of: std::collections::HashMap<i64, usize> =
        ids.iter().enumerate().map(|(i, &id)| (id, i)).collect();

    // Activity weight per tenant: its sampled row count, unnormalized (the
    // largest-remainder pass below needs only ratios). This is the *weight*, not
    // the tenant's row count — its actual rows are decided per part, because a
    // tenant's rows have to land in the parts it is really a member of.
    //
    // `resample_tenants` is pure in (profile, config), so calling it again here
    // returns the same records `compile` drew. Threading them through would be a
    // wider signature for no more truth.
    let samples = resample_tenants(profile, config);
    let weights: Vec<f64> = samples.iter().map(|s| s.tenant_rows as f64).collect();

    let source_rows_total: i64 = profile.parts.iter().map(|p| p.observed_rows.max(1)).sum();
    let mut tenant_rows = vec![0u64; ids.len()];
    let mut clamps = 0u64;

    // Per shard, hand each part its share of the shard's row budget.
    for shard in 0..config.shards {
        let shard_parts: Vec<usize> = parts
            .iter()
            .enumerate()
            .filter(|(_, p)| p.shard == shard)
            .map(|(i, _)| i)
            .collect();

        // A part's share of the budget follows its source `observed_rows` share,
        // floored at its membership count.
        let mut budget: Vec<u64> = shard_parts
            .iter()
            .map(|&i| {
                let src = profile.parts[parts[i].source].observed_rows.max(1);
                let share = (src as f64 / source_rows_total as f64) * config.rows_per_shard as f64;
                (share.round() as u64).max(parts[i].members.len() as u64)
            })
            .collect();

        // Largest-remainder the total back onto `rows_per_shard`, never below a
        // part's membership floor.
        let mut total: u64 = budget.iter().sum();
        let mut k = 0usize;
        while total > config.rows_per_shard && k < budget.len() * 64 {
            let i = k % budget.len();
            if budget[i] > parts[shard_parts[i]].members.len() as u64 {
                budget[i] -= 1;
                total -= 1;
            }
            k += 1;
        }
        let n_budget = budget.len();
        let mut k = 0usize;
        while total < config.rows_per_shard {
            budget[k % n_budget] += 1;
            total += 1;
            k += 1;
        }

        for (bi, &pi) in shard_parts.iter().enumerate() {
            let members = parts[pi].members.clone();
            let n = members.len();
            let mut rows = vec![1u64; n]; // the floor: every membership is real
            let mut left = budget[bi] - n as u64;

            // The rest by activity weight, largest remainder.
            let w: Vec<f64> = members.iter().map(|m| weights[index_of[m]]).collect();
            let wsum: f64 = w.iter().sum();
            if left > 0 && wsum > 0.0 {
                let ideal: Vec<f64> = w.iter().map(|x| x / wsum * left as f64).collect();
                for (r, v) in rows.iter_mut().zip(&ideal) {
                    *r += v.floor() as u64;
                }
                let handed: u64 = ideal.iter().map(|v| v.floor() as u64).sum();
                let mut rest = left - handed;
                let mut order: Vec<usize> = (0..n).collect();
                order.sort_by(|&a, &b| {
                    let fa = ideal[a] - ideal[a].floor();
                    let fb = ideal[b] - ideal[b].floor();
                    fb.partial_cmp(&fa)
                        .unwrap_or(std::cmp::Ordering::Equal)
                        .then(a.cmp(&b))
                });
                for &i in order.iter() {
                    if rest == 0 {
                        break;
                    }
                    rows[i] += 1;
                    rest -= 1;
                }
                left = 0;
            }
            debug_assert_eq!(left, 0);

            for (m, r) in members.iter().zip(&rows) {
                tenant_rows[index_of[m]] += r;
            }

            // Time span: the source part's, clipped to the fixture window. The
            // clip is counted, never hidden — a fixture whose time spans were
            // silently truncated would misreport how much of the window a part
            // covers.
            let src = &profile.parts[parts[pi].source];
            let window_ms = FIXTURE_WINDOW_DAYS * 24 * 60 * 60 * 1000;
            let mut span = src.time_span_ms;
            if span >= window_ms {
                span = window_ms - 1;
                clamps += 1;
            }
            let mut rng = StableRng::stream(config.seed, &format!("time-{shard}-{pi}"));
            let start = FIXTURE_WINDOW_START_MS + rng.below((window_ms - span) as u64) as i64;
            parts[pi].rows = rows;
            parts[pi].ts_min = start;
            parts[pi].ts_max = start + span;
        }
    }

    (tenant_rows, clamps)
}

fn summarize(
    tenants: &[SynthTenant],
    parts: &[SynthPart],
    universe: u64,
    repairs: u64,
    time_clamps: u64,
) -> GeneratedSummary {
    // A tenant whose rows equal its membership count got them from the floor, not
    // from its activity weight. See `GeneratedSummary::floor_pinned_tenants`.
    let floor_pinned = tenants.iter().filter(|t| t.rows <= t.exact_parts).count() as u64;
    let q = |mut v: Vec<f64>| -> Quantiles {
        v.sort_by(|a, b| a.partial_cmp(b).unwrap());
        Quantiles {
            p10: quantile(&v, 0.10),
            p50: quantile(&v, 0.50),
            p90: quantile(&v, 0.90),
            p99: quantile(&v, 0.99),
        }
    };

    GeneratedSummary {
        tenants: tenants.len() as u64,
        parts: parts.len() as u64,
        multi_key_parts: parts.iter().filter(|p| p.distinct_keys() > 1).count() as u64,
        memberships: parts.iter().map(|p| p.distinct_keys()).sum(),
        rows: parts.iter().map(|p| p.rows_total()).sum(),
        bytes: 0, // filled by the writer, from the closed files
        key_universe: universe,
        degree_repairs: repairs,
        floor_pinned_tenants: floor_pinned,
        exact_parts: q(tenants.iter().map(|t| t.exact_parts as f64).collect()),
        range_parts: q(tenants.iter().map(|t| t.range_parts as f64).collect()),
        range_overfetch: q(tenants
            .iter()
            .map(|t| t.range_parts as f64 / t.exact_parts.max(1) as f64)
            .collect()),
        tenant_rows: q(tenants.iter().map(|t| t.rows as f64).collect()),
        key_density: q(parts.iter().map(|p| p.key_density()).collect()),
        time_clamps,
    }
}

/// Tenants a benchmark should ask about — chosen from the *finished* graph, so
/// each class means what its name says.
fn choose_representatives(tenants: &[SynthTenant], seed: u64) -> Representatives {
    let by_rows = {
        let mut v: Vec<&SynthTenant> = tenants.iter().collect();
        v.sort_by_key(|t| (t.rows, t.id));
        v
    };
    let by_over = {
        let mut v: Vec<&SynthTenant> = tenants.iter().collect();
        v.sort_by(|a, b| {
            let oa = a.range_parts as f64 / a.exact_parts.max(1) as f64;
            let ob = b.range_parts as f64 / b.exact_parts.max(1) as f64;
            oa.partial_cmp(&ob).unwrap().then(a.id.cmp(&b.id))
        });
        v
    };

    // A spread across the population, not a random handful: 16 tenants at even
    // rank intervals, so a distribution check sees the whole curve rather than
    // whatever the middle happens to look like.
    let mut sample: Vec<i64> = (0..16)
        .map(|i| by_rows[i * (by_rows.len() - 1) / 15].id)
        .collect();
    sample.sort_unstable();
    sample.dedup();
    let _ = seed;

    Representatives {
        heavy: by_rows[by_rows.len() - 1].id,
        median: by_rows[by_rows.len() / 2].id,
        light: by_rows[0].id,
        high_overfetch: by_over[by_over.len() - 1].id,
        low_overfetch: by_over[0].id,
        sample,
    }
}

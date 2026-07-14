//! A bounded, versioned membership filter over a part's packing keys (issue 0014).
//!
//! # Why this exists
//!
//! `live_parts_pruned` filters on the key *range* a part declares, and that is a
//! superset: a packed file whose keys span 100..4500 "contains" tenant 3000 even
//! when it holds none of tenant 3000's rows — which is the ordinary case, because
//! a file holds the tenants that were *active in the window it covers*, not the
//! tenants its endpoints happen to bracket. Measured on a product-shaped fixture
//! (one hypertable, 100k tenants, heavily packed), the median tenant matched
//! **12,866 parts by range and 6 by key set**, and the catalog shipped all 12,866 —
//! each carrying the ~800-byte roaring bitmap that proved it was not needed. Plan 16
//! measured the same effect on real data as 108 planned files falling to 7, and
//! closed it in the *provider*, after those rows had already crossed the wire.
//!
//! This filter closes it in the catalog, where the rows are.
//!
//! # Why a Bloom filter, and not the exact key set
//!
//! Two hard constraints, both measured rather than assumed:
//!
//! * **An inverted index on the keys is unaffordable.** A GIN index over a
//!   `BIGINT[]` of the keys reads beautifully and writes catastrophically: one index
//!   entry *per key per part* collapsed commit throughput from **7,815/s to
//!   2,254/s** and took PostgreSQL from 4.4 to 11.7 of 12 cores, because every part
//!   insert — and every compaction REPLACE — churns an entry for each of its keys.
//!   That is the forbidden `(part, key)` expansion, hiding inside an index.
//! * **The filter has to live in a btree `INCLUDE` payload**, which is how the scan
//!   rejects a part without touching the heap (`Index Only Scan`, `Heap Fetches:
//!   0`). Index tuples cap at ~2704 bytes, so an exact key set cannot go there:
//!   1,024 keys is 8 KB and the insert would simply fail.
//!
//! So: something small, bounded, and *one entry per part*.
//!
//! # Why the sizes are fixed, and why there are three
//!
//! The first cut sized the blob to the key count and hashed inside an SQL function.
//! It read a sixth of the buffers and still came out **slower than the scan it
//! replaced** — 16.5 ms against 12.8 ms — because a per-row plpgsql call at 20,000
//! candidates costs more than the heap fetches it saves. The hashing has to leave
//! the database.
//!
//! It can, but only if the bit count is something the client already knows: the bit
//! a key lands on depends on how many bits there are. So the sizes are fixed, the
//! client computes the probe positions from the **key alone**, and SQL is left with
//! four `get_byte(...) & mask <> 0` tests. No function call, no allocation, and — the
//! part that matters — no second implementation of the hash to keep in step. There is
//! one, here; the database is handed bit offsets. [`sql_predicate`] and [`sql_binds`]
//! are the two halves of that handoff, and every caller uses them.
//!
//! There are **three** sizes because one cannot serve every shape of part: 1024 bits
//! holds under 1% to ~80 keys and is 76% saturated at 500. A part that packs more
//! tenants gets more bits, up to 2048 bytes — where a btree index tuple's ~2704-byte
//! cap stops us. SQL picks the branch by `octet_length`, so a part pays for the size
//! it needs and the per-row cost is four `get_byte`s either way.
//!
//! # What this cannot do, per the production snapshot
//!
//! `docs/prod-info` measures 68 real parts: the median holds **49,300 distinct keys**
//! (10% of its 505,300-key span). At that density *every* tier is 100% saturated, and
//! no bounded filter — none that fits in an index tuple — can prune such a part. So
//! past [`MAX_KEYS_WORTH_FILTERING`] no filter is stored at all: the bytes would buy
//! nothing, and "no filter" already means "keep".
//!
//! That is not a defeat, because the over-fetch this issue is about *is*
//! `tenants / keys-per-part`. A part holding 49,300 of 505,300 tenants produces a
//! 10x fan-out — the production tenant sample measures 5.7x at the median, on 63 rows,
//! which is a rounding error. It is the parts that hold *few* tenants that fan out
//! catastrophically (2,144x, measured), and those are exactly the ones a bounded
//! filter can represent. The filter is sized for the case that hurts.
//!
//! # The safety rule, unchanged
//!
//! A Bloom filter never says "absent" when the key is present. It may say "present"
//! when the key is absent — a false positive — and that keeps a part that could have
//! been skipped. **Keeping is always safe**; skipping wrongly is not. So every
//! ambiguous state resolves to keep: no filter, an unknown size, an unknown version,
//! a malformed blob, a negative key. Even total saturation is safe: it degrades to
//! keeping everything, which is precisely the behaviour that preceded this module.
//!
//! This is plan 16's rule, and the provider's exact bitmap still runs behind the
//! filter as the correctness backstop.

/// Layout version, in byte 0. A blob whose version this reader does not recognise
/// keeps the part rather than guessing at the bits.
pub const KEY_FILTER_V1: u8 = 1;

/// Hash probes per key.
///
/// Four is a read-cost knob, not a free one: each probe is one `get_byte` the
/// database performs per candidate row.
pub const PROBES: usize = 4;

/// The bit-array sizes a filter may take, smallest first, and the key count each
/// holds under a 1% false-positive rate. Measured, not derived — a false-positive
/// rate is a claim about behaviour, so it is behaviour that sets the number.
///
/// The largest is 2048 bytes because a btree index tuple caps at ~2704, and this one
/// has to share that budget with three bigints and the row pointer. There is no
/// bigger tier available at any price.
const TIERS: [Tier; 3] = [
    Tier {
        bytes: 128,
        supported_keys: 80,
    },
    Tier {
        bytes: 1024,
        supported_keys: 640,
    },
    Tier {
        bytes: 2048,
        supported_keys: 1_280,
    },
];

struct Tier {
    /// Bytes of bit array (the encoded blob is one longer — the version byte).
    bytes: usize,
    /// The key count this tier holds under a 1% false-positive rate.
    supported_keys: usize,
}

/// The key count the largest tier is sized for.
pub const SUPPORTED_KEYS: usize = TIERS[TIERS.len() - 1].supported_keys;

/// Past this many keys, a part gets **no filter at all**.
///
/// Not a safety limit — a saturated filter is perfectly safe, it just says "maybe"
/// about everything. It is a *cost* limit, and it exists because the production
/// snapshot in `docs/prod-info` says it must:
///
/// > The median real part holds **49,300 distinct keys** (of a 505,300-key span —
/// > 10% density). At that count every tier above is 100% saturated. A 2 KB blob per
/// > part, in the row and in the index, that keeps every part it is asked about, is
/// > pure cost for no pruning.
///
/// So above the point where the largest tier is more than half saturated, the honest
/// answer is to store nothing and keep the part — which is precisely what the catalog
/// did before this module existed. Degradation is toward the old behaviour, and it
/// costs nothing on the way.
///
/// The parts this filter *does* earn its keep on are the ones that pack a modest
/// number of tenants — which is the regime Ukiel targets, with files far smaller than
/// the 60M-row parts ClickHouse merges to. `over-fetch = tenants / keys-per-part`, so
/// it is exactly the parts with *few* keys that produce a large fan-out, and exactly
/// those a bounded filter can represent.
pub const MAX_KEYS_WORTH_FILTERING: usize = 8_000;

/// Bytes a filter would cost for `keys` keys — 0 when none would be stored. Lets a
/// writer or a benchmark report the price without building one.
pub fn encoded_len_for(keys: usize) -> usize {
    if keys <= 1 || keys > MAX_KEYS_WORTH_FILTERING {
        return 0;
    }
    1 + tier_for(keys).bytes
}

fn tier_for(keys: usize) -> &'static Tier {
    TIERS
        .iter()
        .find(|t| keys <= t.supported_keys)
        .unwrap_or(&TIERS[TIERS.len() - 1])
}

/// One probe: the byte of the **encoded** blob to read, and the bit to demand in it.
/// Offsets already account for the version byte, so they go straight to `get_byte`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Probe {
    pub byte: i32,
    pub mask: i32,
}

/// Builds the filter for a part's distinct keys, in the smallest tier that holds
/// them under 1% false positives.
///
/// `None` when there is nothing useful to say, and `None` means **unknown** — an
/// absent filter keeps the part:
///
/// * no keys, or a key that cannot be represented; or
/// * a **single** key, because then the part's declared `packing_key_min` and
///   `packing_key_max` *are* its key set. The range predicate is already exact for
///   such a part, and a filter would be 129 bytes proving what the two bigints
///   beside it already prove. Dedicated (unpacked) parts are all of this shape; or
/// * **more keys than [`MAX_KEYS_WORTH_FILTERING`]**, where any bounded filter is
///   saturated and would keep every part it was asked about anyway. Storing one would
///   be 2 KB per part, in the row and in the index, to prune nothing.
pub fn build(keys: &[i64]) -> Option<Vec<u8>> {
    if keys.len() <= 1 || keys.len() > MAX_KEYS_WORTH_FILTERING || keys.iter().any(|k| *k < 0) {
        return None;
    }
    let tier = tier_for(keys.len());

    let mut blob = vec![0u8; 1 + tier.bytes];
    blob[0] = KEY_FILTER_V1;
    let bits = (tier.bytes * 8) as u32;
    for key in keys {
        for probe in probes(*key, bits) {
            blob[probe.byte as usize] |= probe.mask as u8;
        }
    }
    Some(blob)
}

/// Could this part hold `key`?
///
/// `true` = maybe (keep the part). `false` = **proven absent** (skip it). Every
/// undecidable state answers `true`: an unknown size, an unknown version, a
/// truncated blob, a negative key.
///
/// This is the same test the catalog's SQL performs — [`sql_predicate`] is generated
/// from the same tiers and fed by [`sql_binds`] — and a test drives real rows through
/// both to prove they agree.
pub fn maybe_contains(filter: &[u8], key: i64) -> bool {
    if key < 0 || filter.len() < 2 || filter[0] != KEY_FILTER_V1 {
        return true;
    }
    let Some(tier) = TIERS.iter().find(|t| filter.len() == 1 + t.bytes) else {
        return true; // a size this reader does not know
    };
    let bits = (tier.bytes * 8) as u32;
    probes(key, bits)
        .iter()
        .all(|p| filter[p.byte as usize] & p.mask as u8 != 0)
}

/// The SQL that asks the same question of a `BYTEA` column, with `$first_bind..`
/// reserved for what [`sql_binds`] returns.
///
/// The `CASE` is load-bearing twice over. SQL does not promise to short-circuit
/// `OR`, and `get_byte` past the end of a blob *raises* — so a malformed filter
/// would fail the query rather than keep the part. And `CASE` is what dispatches on
/// the tier: the arms are tried in order, so an unknown size falls through to the
/// `TRUE` that keeps the part.
pub fn sql_predicate(col: &str, first_bind: usize) -> String {
    let mut arms = String::new();
    let mut bind = first_bind;
    for tier in &TIERS {
        let bits = (0..PROBES)
            .map(|_| {
                let (byte, mask) = (bind, bind + 1);
                bind += 2;
                // Cast, so a caller may bind these as int4 or int8 as suits it —
                // `get_byte` takes only int4, and an int8 bind would fail to resolve.
                format!("(get_byte({col}, ${byte}::int) & ${mask}::int) <> 0")
            })
            .collect::<Vec<_>>()
            .join("\n                        AND ");
        arms.push_str(&format!(
            "\n                   WHEN octet_length({col}) = {} THEN {bits}",
            1 + tier.bytes
        ));
    }
    format!(
        "CASE WHEN {col} IS NULL OR octet_length({col}) < 2 THEN TRUE
                   WHEN get_byte({col}, 0) <> {KEY_FILTER_V1} THEN TRUE{arms}
                   ELSE TRUE END"
    )
}

/// The bytes and bits `key` demands, in the order [`sql_predicate`] binds them: for
/// each tier, `PROBES` pairs of (byte offset, bit mask).
pub fn sql_binds(key: i64) -> Vec<i32> {
    TIERS
        .iter()
        .flat_map(|t| probes(key, (t.bytes * 8) as u32))
        .flat_map(|p| [p.byte, p.mask])
        .collect()
}

/// The probes a key demands of a filter with `bits` bits.
fn probes(key: i64, bits: u32) -> [Probe; PROBES] {
    std::array::from_fn(|probe| {
        let bit = bit_index(key, probe, bits);
        Probe {
            byte: 1 + (bit / 8) as i32,
            mask: (1u32 << (bit % 8)) as i32,
        }
    })
}

/// The bit one probe of one key lands on.
///
/// Carter–Wegman universal hashing: `((a*k + b) mod P) mod bits`, with `P` a prime
/// above the key space and `(a, b)` distinct per probe. It gives a provable `1/bits`
/// collision bound — and, unlike a 64-bit mix, every step stays inside `i64` without
/// overflowing. (A plain `k * a % bits` was tried first and clusters badly on
/// regularly-spaced tenant ids, which is precisely the shape a tenant id space has.)
fn bit_index(key: i64, probe: usize, bits: u32) -> u32 {
    let k = key.rem_euclid(HASH_PRIME);
    let (a, b) = PROBE_PARAMS[probe];
    // k < 2^31 and a < 2^31, so a*k < 2^62 — comfortably inside i64.
    (((a * k + b) % HASH_PRIME) % i64::from(bits)) as u32
}

/// 2^31 - 1, a Mersenne prime, above any packing key a deployment will use and small
/// enough that `a * k` cannot overflow.
const HASH_PRIME: i64 = 2_147_483_647;

/// `(a, b)` per probe. Distinct, odd, and nowhere near the prime's multiples, so the
/// probes are independent.
const PROBE_PARAMS: [(i64, i64); PROBES] = [
    (1_640_531_527, 1_013_904_223),
    (1_103_515_245, 12_345),
    (1_664_525, 1_566_083_941),
    (2_654_435_761 % HASH_PRIME, 362_437),
];

#[cfg(test)]
mod tests {
    use super::*;

    /// The only property that matters for correctness: a key that is present is
    /// **never** reported absent. Everything else is a performance question.
    #[test]
    fn a_present_key_is_never_reported_absent() {
        for n in [2usize, 7, 80, 81, 640, 641, 1_280, 1_281, 8_000] {
            for (stride, off) in [(7i64, 3i64), (1, 0), (1_000, 17)] {
                let keys: Vec<i64> = (0..n as i64).map(|i| i * stride + off).collect();
                let filter = build(&keys).expect("keys are representable");
                for k in &keys {
                    assert!(
                        maybe_contains(&filter, *k),
                        "{n} keys (stride {stride}): key {k} is in the part and the \
                         filter denied it"
                    );
                }
            }
        }
    }

    /// Every tier must actually hold the rate it claims, or `TIERS` is documentation
    /// rather than a specification — and the acceptance criterion for issue 0014 is
    /// stated as a false-positive rate.
    #[test]
    fn each_tier_holds_one_percent_at_the_key_count_it_claims() {
        for tier in &TIERS {
            let mut worst: f64 = 0.0;
            // Several key spacings: a rate that only holds for one layout is an
            // artefact of that layout, and tenant ids are regularly spaced.
            for (stride, off) in [(7i64, 3i64), (1, 0), (1_000, 17), (13, 5)] {
                let keys: Vec<i64> = (0..tier.supported_keys as i64)
                    .map(|i| i * stride + off)
                    .collect();
                let filter = build(&keys).unwrap();
                assert_eq!(filter.len(), 1 + tier.bytes, "the tier must be the one sized");

                // Probes disjoint from every key set above, so none is truly present.
                let n = 50_000i64;
                let kept = (0..n)
                    .filter(|i| maybe_contains(&filter, i * 3 + 100_000_003))
                    .count();
                worst = worst.max(kept as f64 / n as f64);
            }
            assert!(
                worst < 0.01,
                "{} bits at {} keys: false-positive rate {worst:.4} exceeds the 1% the \
                 tier claims",
                tier.bytes * 8,
                tier.supported_keys
            );
        }
    }

    /// Undecidable is keep. A skip must be *proven*, never assumed.
    #[test]
    fn every_ambiguous_state_keeps_the_part() {
        assert!(maybe_contains(&[], 7), "no filter");
        assert!(maybe_contains(&[KEY_FILTER_V1], 7), "header only, no bits");

        let filter = build(&[1, 2, 3]).unwrap();
        let mut unknown_version = filter.clone();
        unknown_version[0] = 99;
        assert!(maybe_contains(&unknown_version, 7), "unknown version");
        assert!(maybe_contains(&filter[..64], 7), "unknown size");
        assert!(maybe_contains(&filter, -1), "a negative key proves nothing");

        assert_eq!(build(&[-5, 1, 2]), None, "a negative key poisons the filter");
        assert_eq!(build(&[]), None, "no keys is not an empty filter");
        assert_eq!(
            build(&[7]),
            None,
            "a single key needs no filter — min == max == the key, so the range \
             predicate is already exact"
        );
    }

    /// A part denser than any bounded filter can represent gets **no filter** — not a
    /// saturated one.
    ///
    /// This is the production case, not a corner: the median part in
    /// `docs/prod-info/part-geometry.jsonl` holds 49,300 distinct keys, where every
    /// tier is 100% saturated. A saturated filter is safe (it keeps everything) but it
    /// is 2 KB per part, in the row and in the index, to prune nothing.
    #[test]
    fn a_part_too_dense_to_prune_gets_no_filter_rather_than_a_useless_one() {
        let dense: Vec<i64> = (0..49_300).map(|i| i * 7 + 3).collect();
        assert_eq!(build(&dense), None, "the median production part");
        assert_eq!(encoded_len_for(dense.len()), 0, "and it costs nothing");

        // Right at the edge, a filter is still stored and still bounded.
        let edge: Vec<i64> = (0..MAX_KEYS_WORTH_FILTERING as i64).map(|i| i * 7 + 3).collect();
        let filter = build(&edge).expect("still worth filtering");
        assert_eq!(
            filter.len(),
            1 + TIERS[TIERS.len() - 1].bytes,
            "it must not grow past the last tier — the payload is bounded"
        );
        for k in edge.iter().take(500) {
            assert!(maybe_contains(&filter, *k), "a present key was denied");
        }
    }

    /// The binds a query sends must line up with the predicate that consumes them,
    /// and with the bits the builder set. This is the seam the database sees.
    #[test]
    fn the_sql_binds_are_the_bits_the_builder_set() {
        let binds = sql_binds(4_242);
        assert_eq!(
            binds.len(),
            TIERS.len() * PROBES * 2,
            "one (byte, mask) pair per probe per tier, in tier order"
        );
        let sql = sql_predicate("key_filter", 3);
        for i in 3..3 + binds.len() {
            assert!(sql.contains(&format!("${i}")), "the predicate must read ${i}");
        }

        // And the offsets must find set bits in a filter that holds the key — at
        // whichever tier that filter landed in.
        for n in [4usize, 200] {
            let mut keys: Vec<i64> = (0..n as i64).map(|i| i * 13 + 1).collect();
            keys.push(4_242);
            let filter = build(&keys).unwrap();
            let tier = TIERS.iter().position(|t| filter.len() == 1 + t.bytes).unwrap();
            for pair in binds[tier * PROBES * 2..(tier + 1) * PROBES * 2].chunks(2) {
                assert!(
                    filter[pair[0] as usize] & pair[1] as u8 != 0,
                    "{n} keys: SQL will demand byte {} bit {:#04x}, and it is not set",
                    pair[0],
                    pair[1]
                );
            }
        }
    }
}

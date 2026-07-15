//! The one property a skip index must never violate: `NoMatch` implies a full scan of the
//! row group finds zero matches. Tested over many deterministic pseudo-random groups and
//! predicates for every supported index and predicate kind.

use parquet_skip_index_core::prefix_set::PrefixSet;
use parquet_skip_index_core::value_set::ValueSet;
use parquet_skip_index_core::zone_map::ZoneMap;
use parquet_skip_index_core::{Decision, Predicate, Value};

/// A tiny deterministic PRNG (SplitMix64) — no `rand`, no wall-clock, fully reproducible.
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E3779B97F4A7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
        z ^ (z >> 31)
    }
    fn range(&mut self, lo: i64, hi: i64) -> i64 {
        lo + (self.next() % ((hi - lo + 1) as u64)) as i64
    }
}

// --- full-scan oracles ------------------------------------------------------

fn scan_matches(values: &[Option<Value>], pred: &Predicate) -> bool {
    values.iter().flatten().any(|v| matches(v, pred))
}

fn matches(v: &Value, pred: &Predicate) -> bool {
    match (v, pred) {
        (Value::Int(x), Predicate::Eq(Value::Int(y))) => x == y,
        (Value::Str(x), Predicate::Eq(Value::Str(y))) => x == y,
        (_, Predicate::In(vs)) => vs.iter().any(|w| v == w),
        (Value::Int(x), Predicate::Range(Value::Int(lo), Value::Int(hi))) => lo <= x && x <= hi,
        (Value::Str(x), Predicate::Range(Value::Str(lo), Value::Str(hi))) => lo <= x && x <= hi,
        (Value::Str(x), Predicate::Prefix(p)) => x.starts_with(p),
        _ => false,
    }
}

// --- integer zone map + value set ------------------------------------------

#[test]
fn integer_zone_map_and_value_set_never_skip_a_matching_group() {
    let mut rng = Rng(1);
    let mut nomatch_seen = 0;
    for _ in 0..4000 {
        let n = (rng.next() % 12) as usize;
        let values: Vec<Option<Value>> = (0..n)
            .map(|_| {
                if rng.next().is_multiple_of(7) {
                    None
                } else {
                    Some(Value::Int(rng.range(-20, 20)))
                }
            })
            .collect();
        let zone = ZoneMap::build(&values);
        let vset = ValueSet::build(&values, 4096).unwrap();

        for _ in 0..4 {
            let pred = match rng.next() % 3 {
                0 => Predicate::Eq(Value::Int(rng.range(-25, 25))),
                1 => Predicate::In(vec![
                    Value::Int(rng.range(-25, 25)),
                    Value::Int(rng.range(-25, 25)),
                ]),
                _ => {
                    let a = rng.range(-25, 25);
                    let b = rng.range(-25, 25);
                    Predicate::Range(Value::Int(a.min(b)), Value::Int(a.max(b)))
                }
            };
            for (name, d) in [
                ("zone", zone.evaluate(&pred)),
                ("vset", vset.evaluate(&pred)),
            ] {
                if d == Decision::NoMatch {
                    nomatch_seen += 1;
                    assert!(
                        !scan_matches(&values, &pred),
                        "{name} returned NoMatch but a full scan found a match: {values:?} {pred:?}"
                    );
                }
            }
        }
    }
    assert!(
        nomatch_seen > 100,
        "the indices should skip sometimes to be useful"
    );
}

// --- string zone map, value set, prefix set --------------------------------

fn rand_str(rng: &mut Rng) -> Vec<u8> {
    let len = (rng.next() % 6) as usize;
    (0..len).map(|_| b'a' + (rng.next() % 4) as u8).collect()
}

#[test]
fn string_indices_never_skip_a_matching_group() {
    let mut rng = Rng(42);
    let mut nomatch_seen = 0;
    for _ in 0..4000 {
        let n = (rng.next() % 10) as usize;
        let values: Vec<Option<Value>> = (0..n)
            .map(|_| {
                if rng.next().is_multiple_of(8) {
                    None
                } else {
                    Some(Value::Str(rand_str(&mut rng)))
                }
            })
            .collect();
        let zone = ZoneMap::build(&values);
        let vset = ValueSet::build(&values, 4096).unwrap();
        let pset = PrefixSet::build(&values, 2, 4096).unwrap();

        for _ in 0..4 {
            let pred = match rng.next() % 3 {
                0 => Predicate::Eq(Value::Str(rand_str(&mut rng))),
                1 => Predicate::Prefix(rand_str(&mut rng)),
                _ => Predicate::In(vec![
                    Value::Str(rand_str(&mut rng)),
                    Value::Str(rand_str(&mut rng)),
                ]),
            };
            for (name, d) in [
                ("zone", zone.evaluate(&pred)),
                ("vset", vset.evaluate(&pred)),
                ("pset", pset.evaluate(&pred)),
            ] {
                if d == Decision::NoMatch {
                    nomatch_seen += 1;
                    assert!(
                        !scan_matches(&values, &pred),
                        "{name} NoMatch but scan matched: {values:?} {pred:?}"
                    );
                }
            }
        }
    }
    assert!(nomatch_seen > 100);
}

// --- explicit edge cases ---------------------------------------------------

#[test]
fn an_all_null_group_is_a_safe_nomatch() {
    let values = vec![None, None, None];
    let zone = ZoneMap::build(&values);
    assert_eq!(
        zone.evaluate(&Predicate::Eq(Value::Int(0))),
        Decision::NoMatch
    );
    assert_eq!(
        zone.evaluate(&Predicate::Prefix(b"a".to_vec())),
        Decision::NoMatch
    );
}

#[test]
fn unsupported_predicates_degrade_to_unknown_not_skip() {
    let values = vec![Some(Value::Int(5)), Some(Value::Int(9))];
    // A prefix predicate on an integer zone map cannot decide.
    assert_eq!(
        ZoneMap::build(&values).evaluate(&Predicate::Prefix(b"x".to_vec())),
        Decision::Unknown
    );
    // A range on a value set is out of its remit.
    assert_eq!(
        ValueSet::build(&values, 4096)
            .unwrap()
            .evaluate(&Predicate::Range(Value::Int(0), Value::Int(3))),
        Decision::Unknown
    );
    // A prefix shorter than the stored length cannot rule the group out.
    let strs = vec![Some(Value::Str(b"alpha".to_vec()))];
    assert_eq!(
        PrefixSet::build(&strs, 3, 4096)
            .unwrap()
            .evaluate(&Predicate::Prefix(b"a".to_vec())),
        Decision::Unknown
    );
}

#[test]
fn a_value_set_over_budget_is_not_built_so_evaluation_abstains() {
    // Many distinct values, tiny budget -> None (the builder omits the group).
    let values: Vec<Option<Value>> = (0..1000).map(|i| Some(Value::Int(i))).collect();
    assert!(ValueSet::build(&values, 64).is_none());
}

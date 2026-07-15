//! External zone map: min/max per row group, to price skipping before a Parquet footer
//! against native footer/page statistics.
//!
//! Conservative by construction: a `NoMatch` is returned only when the *entire* predicate
//! range falls outside `[min, max]`, so a kept-out row group truly holds no match. A null
//! value never satisfies an equality/range/prefix predicate, so an all-null group is a safe
//! `NoMatch` and nulls otherwise do not affect the min/max bound.

use std::cmp::Ordering;

use crate::{Decision, Predicate, Value};

/// A row group's zone map. `None` bounds mean the group had no non-null value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ZoneMap {
    pub bounds: Option<(Value, Value)>,
}

impl ZoneMap {
    /// Build from a row group's values (nulls are `None`).
    pub fn build(values: &[Option<Value>]) -> ZoneMap {
        let mut min: Option<Value> = None;
        let mut max: Option<Value> = None;
        for v in values.iter().flatten() {
            if min
                .as_ref()
                .is_none_or(|m| cmp(v, m) == Some(Ordering::Less))
            {
                min = Some(v.clone());
            }
            if max
                .as_ref()
                .is_none_or(|m| cmp(v, m) == Some(Ordering::Greater))
            {
                max = Some(v.clone());
            }
        }
        ZoneMap {
            bounds: min.zip(max),
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = vec![b'Z'];
        match &self.bounds {
            None => out.push(0),
            Some((lo, hi)) => {
                out.push(1);
                encode_value(lo, &mut out);
                encode_value(hi, &mut out);
            }
        }
        out
    }

    pub fn decode(bytes: &[u8]) -> Option<ZoneMap> {
        let mut p = 0;
        if bytes.first()? != &b'Z' {
            return None;
        }
        p += 1;
        match bytes.get(p)? {
            0 => Some(ZoneMap { bounds: None }),
            1 => {
                p += 1;
                let (lo, np) = decode_value(bytes, p)?;
                let (hi, _) = decode_value(bytes, np)?;
                Some(ZoneMap {
                    bounds: Some((lo, hi)),
                })
            }
            _ => None,
        }
    }

    /// The verdict for this row group under a predicate.
    pub fn evaluate(&self, pred: &Predicate) -> Decision {
        let Some((min, max)) = &self.bounds else {
            // No non-null value: nothing can match an eq/range/prefix predicate.
            return Decision::NoMatch;
        };
        match pred {
            Predicate::Eq(v) => point_decision(v, min, max),
            Predicate::In(vs) => {
                // NoMatch only if every value is provably outside the range.
                let mut any_maybe = false;
                for v in vs {
                    match point_decision(v, min, max) {
                        Decision::NoMatch => {}
                        Decision::Unknown => return Decision::Unknown,
                        Decision::Maybe => any_maybe = true,
                    }
                }
                if any_maybe {
                    Decision::Maybe
                } else {
                    Decision::NoMatch
                }
            }
            Predicate::Range(lo, hi) => {
                match (cmp(hi, min), cmp(lo, max)) {
                    (Some(Ordering::Less), _) => Decision::NoMatch, // hi < min
                    (_, Some(Ordering::Greater)) => Decision::NoMatch, // lo > max
                    (Some(_), Some(_)) => Decision::Maybe,
                    _ => Decision::Unknown, // type mismatch
                }
            }
            Predicate::Prefix(p) => prefix_decision(p, min, max),
        }
    }
}

fn point_decision(v: &Value, min: &Value, max: &Value) -> Decision {
    match (cmp(v, min), cmp(v, max)) {
        (Some(Ordering::Less), _) => Decision::NoMatch,
        (_, Some(Ordering::Greater)) => Decision::NoMatch,
        (Some(_), Some(_)) => Decision::Maybe,
        _ => Decision::Unknown,
    }
}

/// `NoMatch` iff the prefix range `[p, p⁺)` does not overlap `[min, max]`.
fn prefix_decision(p: &[u8], min: &Value, max: &Value) -> Decision {
    let (Value::Str(min), Value::Str(max)) = (min, max) else {
        return Decision::Unknown; // prefix only applies to strings
    };
    if p.is_empty() {
        return Decision::Maybe; // matches everything
    }
    // The range of strings with prefix `p` is [p, upper), where `upper` is `p` with its last
    // non-0xff byte incremented and the tail dropped; if `p` is all 0xff there is no upper
    // bound (everything >= p matches).
    let upper = prefix_upper(p);
    // Overlap of [p, upper) with [min, max]: NoMatch iff max < p, or (upper exists and
    // upper <= min).
    if max.as_slice() < p {
        return Decision::NoMatch;
    }
    if let Some(u) = &upper
        && u.as_slice() <= min.as_slice()
    {
        return Decision::NoMatch;
    }
    Decision::Maybe
}

/// The exclusive upper bound of the set of byte strings starting with `p`. `None` when `p`
/// is all `0xff` (no finite upper bound).
pub fn prefix_upper(p: &[u8]) -> Option<Vec<u8>> {
    let mut u = p.to_vec();
    while let Some(last) = u.last().copied() {
        if last < 0xff {
            *u.last_mut().unwrap() = last + 1;
            return Some(u);
        }
        u.pop();
    }
    None
}

/// Total order used for bounds. `None` on a type mismatch, which the caller turns into
/// `Unknown`.
fn cmp(a: &Value, b: &Value) -> Option<Ordering> {
    match (a, b) {
        (Value::Int(x), Value::Int(y)) => Some(x.cmp(y)),
        (Value::Str(x), Value::Str(y)) => Some(x.cmp(y)),
        _ => None,
    }
}

pub(crate) fn encode_value(v: &Value, out: &mut Vec<u8>) {
    match v {
        Value::Int(i) => {
            out.push(1);
            out.extend_from_slice(&i.to_le_bytes());
        }
        Value::Str(s) => {
            out.push(2);
            out.extend_from_slice(&(s.len() as u32).to_le_bytes());
            out.extend_from_slice(s);
        }
    }
}

pub(crate) fn decode_value(bytes: &[u8], mut p: usize) -> Option<(Value, usize)> {
    match bytes.get(p)? {
        1 => {
            p += 1;
            let raw: [u8; 8] = bytes.get(p..p + 8)?.try_into().ok()?;
            Some((Value::Int(i64::from_le_bytes(raw)), p + 8))
        }
        2 => {
            p += 1;
            let len = u32::from_le_bytes(bytes.get(p..p + 4)?.try_into().ok()?) as usize;
            p += 4;
            let s = bytes.get(p..p + len)?.to_vec();
            Some((Value::Str(s), p + len))
        }
        _ => None,
    }
}

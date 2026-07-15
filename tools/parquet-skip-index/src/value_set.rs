//! Small exact value set: the complete distinct value set of a low-NDV row group, capped by
//! an explicit payload-byte budget.
//!
//! Because the set is *complete*, `NoMatch` for an equality/`IN` predicate is exact — a
//! value absent from the set is absent from the row group. When the encoded set would exceed
//! the budget the index is simply not built for that group, and evaluation abstains
//! (`Unknown` → keep) rather than storing a partial set that could hide a match.

use std::collections::BTreeSet;

use crate::zone_map::encode_value;
use crate::{Decision, Predicate, Value};

/// A row group's complete distinct value set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValueSet {
    pub values: BTreeSet<Vec<u8>>,
}

/// Encode a value to its canonical set key.
fn key(v: &Value) -> Vec<u8> {
    let mut b = Vec::new();
    encode_value(v, &mut b);
    b
}

impl ValueSet {
    /// Build the complete set, or `None` if it would exceed `budget_bytes`.
    pub fn build(values: &[Option<Value>], budget_bytes: usize) -> Option<ValueSet> {
        let mut set = BTreeSet::new();
        let mut bytes = 0usize;
        for v in values.iter().flatten() {
            let k = key(v);
            if set.insert(k.clone()) {
                bytes += k.len() + 4;
                if bytes > budget_bytes {
                    return None;
                }
            }
        }
        Some(ValueSet { values: set })
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = vec![b'V'];
        out.extend_from_slice(&(self.values.len() as u32).to_le_bytes());
        for v in &self.values {
            out.extend_from_slice(&(v.len() as u32).to_le_bytes());
            out.extend_from_slice(v);
        }
        out
    }

    pub fn decode(bytes: &[u8]) -> Option<ValueSet> {
        let mut p = 0;
        if bytes.first()? != &b'V' {
            return None;
        }
        p += 1;
        let n = u32::from_le_bytes(bytes.get(p..p + 4)?.try_into().ok()?) as usize;
        p += 4;
        let mut set = BTreeSet::new();
        for _ in 0..n {
            let len = u32::from_le_bytes(bytes.get(p..p + 4)?.try_into().ok()?) as usize;
            p += 4;
            let v = bytes.get(p..p + len)?.to_vec();
            p += len;
            set.insert(v);
        }
        Some(ValueSet { values: set })
    }

    pub fn evaluate(&self, pred: &Predicate) -> Decision {
        match pred {
            Predicate::Eq(v) => {
                if self.values.contains(&key(v)) {
                    Decision::Maybe
                } else {
                    Decision::NoMatch
                }
            }
            Predicate::In(vs) => {
                if vs.iter().any(|v| self.values.contains(&key(v))) {
                    Decision::Maybe
                } else {
                    Decision::NoMatch
                }
            }
            // A value set answers equality and IN; ranges and prefixes are not its job.
            Predicate::Range(..) | Predicate::Prefix(_) => Decision::Unknown,
        }
    }
}

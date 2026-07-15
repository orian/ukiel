//! Prefix set: the complete set of byte-prefixes (at a declared length) of a row group's
//! string values, for `LIKE 'literal%'`.
//!
//! Each value contributes the key `value[..min(L, len)]`. A `LIKE 'p%'` with `len(p) >= L`
//! is decided by whether `p[..L]` is present — absent means no value starts with `p`, an
//! exact `NoMatch`. When `len(p) < L` the L-byte prefixes cannot rule the group out, so the
//! index abstains (`Unknown` → keep). Equality on a string is decided the same way against
//! the value's own prefix key. Built only up to a byte budget; over it, the group abstains.

use std::collections::BTreeSet;

use crate::{Decision, Predicate, Value};

/// A row group's complete prefix set at length `prefix_len`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrefixSet {
    pub prefix_len: usize,
    pub prefixes: BTreeSet<Vec<u8>>,
}

fn prefix_key(s: &[u8], l: usize) -> Vec<u8> {
    s[..s.len().min(l)].to_vec()
}

impl PrefixSet {
    /// Build the complete prefix set, or `None` if it would exceed `budget_bytes`. Only
    /// string values contribute; a non-string column yields an empty (useless) set that
    /// evaluation treats as abstaining.
    pub fn build(
        values: &[Option<Value>],
        prefix_len: usize,
        budget_bytes: usize,
    ) -> Option<PrefixSet> {
        let mut set = BTreeSet::new();
        let mut bytes = 0usize;
        for v in values.iter().flatten() {
            let Value::Str(s) = v else {
                return None; // not a string column
            };
            let k = prefix_key(s, prefix_len);
            if set.insert(k.clone()) {
                bytes += k.len() + 4;
                if bytes > budget_bytes {
                    return None;
                }
            }
        }
        Some(PrefixSet {
            prefix_len,
            prefixes: set,
        })
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = vec![b'P'];
        out.extend_from_slice(&(self.prefix_len as u32).to_le_bytes());
        out.extend_from_slice(&(self.prefixes.len() as u32).to_le_bytes());
        for p in &self.prefixes {
            out.extend_from_slice(&(p.len() as u32).to_le_bytes());
            out.extend_from_slice(p);
        }
        out
    }

    pub fn decode(bytes: &[u8]) -> Option<PrefixSet> {
        let mut p = 0;
        if bytes.first()? != &b'P' {
            return None;
        }
        p += 1;
        let prefix_len = u32::from_le_bytes(bytes.get(p..p + 4)?.try_into().ok()?) as usize;
        p += 4;
        let n = u32::from_le_bytes(bytes.get(p..p + 4)?.try_into().ok()?) as usize;
        p += 4;
        let mut set = BTreeSet::new();
        for _ in 0..n {
            let len = u32::from_le_bytes(bytes.get(p..p + 4)?.try_into().ok()?) as usize;
            p += 4;
            set.insert(bytes.get(p..p + len)?.to_vec());
            p += len;
        }
        Some(PrefixSet {
            prefix_len,
            prefixes: set,
        })
    }

    pub fn evaluate(&self, pred: &Predicate) -> Decision {
        match pred {
            Predicate::Prefix(p) => {
                if p.len() < self.prefix_len {
                    // Shorter than the stored key length: the L-byte prefixes cannot rule
                    // the group out.
                    return Decision::Unknown;
                }
                let key = p[..self.prefix_len].to_vec();
                if self.prefixes.contains(&key) {
                    Decision::Maybe
                } else {
                    Decision::NoMatch
                }
            }
            Predicate::Eq(Value::Str(s)) => {
                let key = prefix_key(s, self.prefix_len);
                if self.prefixes.contains(&key) {
                    Decision::Maybe
                } else {
                    Decision::NoMatch
                }
            }
            // Ranges, integer equality, and IN are not a prefix set's job.
            _ => Decision::Unknown,
        }
    }
}

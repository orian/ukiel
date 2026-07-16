//! Access-plan generation: turn a set of files into the four registered byte-range plans.
//!
//! The contiguous and sparse plans must have the *same returned-byte total* for the same
//! fraction, so a scattered-vs-sequential comparison is about locality, not volume. That
//! equality is enforced by construction: sparse splits exactly the contiguous total into
//! `SPARSE_CHUNKS` spread ranges.

/// One byte range within one file: `(file_index, offset, length)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Range {
    pub file: usize,
    pub offset: u64,
    pub length: u64,
}

/// How many scattered chunks a sparse plan splits its budget into per file.
pub const SPARSE_CHUNKS: u64 = 16;

/// The registered access plan kinds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Plan {
    /// Every byte of every file, sequentially.
    All,
    /// Every byte of one complete file.
    One,
    /// A single contiguous run of `fraction` of each file.
    Contiguous,
    /// `SPARSE_CHUNKS` scattered ranges per file summing to the same `fraction` total.
    Sparse,
}

/// The byte length that a `fraction`-sized budget resolves to for a file of `len` bytes.
fn budget(len: u64, fraction: f64) -> u64 {
    ((len as f64 * fraction).ceil() as u64).min(len).max(1)
}

/// Build the range list for a plan over files of the given byte lengths.
pub fn build(plan: Plan, file_lengths: &[u64], fraction: f64) -> Vec<Range> {
    match plan {
        Plan::All => file_lengths
            .iter()
            .enumerate()
            .filter(|&(_, &l)| l > 0)
            .map(|(file, &length)| Range { file, offset: 0, length })
            .collect(),
        Plan::One => file_lengths
            .iter()
            .enumerate()
            .find(|&(_, &l)| l > 0)
            .map(|(file, &length)| vec![Range { file, offset: 0, length }])
            .unwrap_or_default(),
        Plan::Contiguous => file_lengths
            .iter()
            .enumerate()
            .filter(|&(_, &l)| l > 0)
            .map(|(file, &len)| Range { file, offset: 0, length: budget(len, fraction) })
            .collect(),
        Plan::Sparse => {
            let mut out = Vec::new();
            for (file, &len) in file_lengths.iter().enumerate() {
                if len == 0 {
                    continue;
                }
                let total = budget(len, fraction);
                let chunks = SPARSE_CHUNKS.min(total).max(1);
                let base = total / chunks;
                let rem = total - base * chunks;
                // Spread chunk start points across the file; the last chunk absorbs the
                // remainder so the returned-byte total exactly equals the contiguous plan.
                let stride = (len / chunks).max(1);
                for c in 0..chunks {
                    let length = if c == chunks - 1 { base + rem } else { base };
                    if length == 0 {
                        continue;
                    }
                    let mut offset = c * stride;
                    if offset + length > len {
                        offset = len - length;
                    }
                    out.push(Range { file, offset, length });
                }
            }
            out
        }
    }
}

/// The total returned bytes a range list will read.
pub fn total_bytes(ranges: &[Range]) -> u64 {
    ranges.iter().map(|r| r.length).sum()
}

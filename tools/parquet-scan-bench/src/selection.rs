//! Compile global row-group ordinals into the four explicit access plans. Selection is by
//! ordinal, not by predicate — this measures the value of reading fewer row groups
//! separately from whether statistics can discover them. Because a reconstruction and its
//! one-variable child share row-group boundaries (same order, same row-group size), the
//! same global ordinals select the same rows in both, which is what makes them comparable.

use anyhow::{Result, bail};

/// One file's global-ordinal span: `[start, start + count)`.
#[derive(Debug, Clone, Copy)]
pub struct FileGroups {
    pub file: usize,
    pub start_ordinal: u64,
    pub row_group_count: u64,
}

/// The four registered selection modes, plus the zero (metadata-only) control.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Selection {
    All,
    One,
    TenPercentContiguous,
    TenPercentSparse,
    Zero,
}

pub fn parse_selection(s: &str) -> Result<Selection> {
    Ok(match s {
        "all" => Selection::All,
        "one" => Selection::One,
        "ten_percent_contiguous" | "contiguous" => Selection::TenPercentContiguous,
        "ten_percent_sparse" | "sparse" => Selection::TenPercentSparse,
        "zero" => Selection::Zero,
        other => bail!("unknown selection '{other}'"),
    })
}

/// The chosen global ordinals for a selection over `total` row groups.
pub fn global_ordinals(selection: Selection, total: u64) -> Vec<u64> {
    if total == 0 {
        return vec![];
    }
    match selection {
        Selection::Zero => vec![],
        Selection::All => (0..total).collect(),
        // Deterministic single group: the middle one, so it is neither the first (often
        // atypically dense) nor the last (often short).
        Selection::One => vec![total / 2],
        Selection::TenPercentContiguous => {
            let count = (total as f64 * 0.1).ceil() as u64;
            let count = count.clamp(1, total);
            (0..count).collect()
        }
        Selection::TenPercentSparse => {
            let count = (total as f64 * 0.1).ceil() as u64;
            let count = count.clamp(1, total);
            let stride = (total / count).max(1);
            let mut out: Vec<u64> = (0..count).map(|i| (i * stride).min(total - 1)).collect();
            out.dedup();
            out
        }
    }
}

/// Map global ordinals to per-file local row-group indices.
pub fn to_local(spans: &[FileGroups], ordinals: &[u64]) -> Vec<(usize, Vec<usize>)> {
    let mut per_file: Vec<Vec<usize>> = vec![Vec::new(); spans.len()];
    for &g in ordinals {
        if let Some(span) = spans
            .iter()
            .find(|s| g >= s.start_ordinal && g < s.start_ordinal + s.row_group_count)
        {
            per_file[span.file].push((g - span.start_ordinal) as usize);
        }
    }
    per_file
        .into_iter()
        .enumerate()
        .filter(|(_, v)| !v.is_empty())
        .collect()
}

/// Build the per-file ordinal spans from each file's row-group count.
pub fn spans(row_group_counts: &[u64]) -> (Vec<FileGroups>, u64) {
    let mut spans = Vec::with_capacity(row_group_counts.len());
    let mut ordinal = 0u64;
    for (file, &count) in row_group_counts.iter().enumerate() {
        spans.push(FileGroups {
            file,
            start_ordinal: ordinal,
            row_group_count: count,
        });
        ordinal += count;
    }
    (spans, ordinal)
}

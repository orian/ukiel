//! Convergence detection and exact output-part inspection (plan 46, task 4).
//!
//! Both commands are **read-only**. `wait-compacted` polls until the fixture is final;
//! `part-shape` reads the final catalog rows and scans the actual objects. Neither
//! mutates anything, and neither infers a key count from min/max, a Bloom filter, or the
//! source topology — the exact distinct-key count comes from scanning the sorted
//! packing-key column of the object that is actually stored.

use std::collections::BTreeMap;

use anyhow::{Context, Result, bail};
use arrow::array::Int64Array;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use prod_synth_contract::PartShapeReceipt;
use prod_synth_integrity::RowMultiset;
use serde::Serialize;
use ukiel_catalog::PostgresCatalog;
use ukiel_core::{HypertableId, Part};

// ---------------------------------------------------------------------------
// Pure helpers — unit-testable without a database or an object store.
// ---------------------------------------------------------------------------

/// The five-number summary a report quotes. Same nearest-rank-ish definition the plan-45
/// generator pins (`round(p*(n-1))`), so a reader compares like with like across plans.
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct Quantiles {
    pub p10: f64,
    pub p50: f64,
    pub p90: f64,
    pub p99: f64,
    pub max: f64,
}

impl Quantiles {
    pub fn of(values: &mut [f64]) -> Quantiles {
        if values.is_empty() {
            return Quantiles {
                p10: 0.0,
                p50: 0.0,
                p90: 0.0,
                p99: 0.0,
                max: 0.0,
            };
        }
        values.sort_by(|a, b| a.partial_cmp(b).expect("no NaN"));
        let q = |p: f64| {
            let idx = (p * (values.len() - 1) as f64 + 0.5) as usize;
            values[idx.min(values.len() - 1)]
        };
        Quantiles {
            p10: q(0.10),
            p50: q(0.50),
            p90: q(0.90),
            p99: q(0.99),
            max: *values.last().unwrap(),
        }
    }
}

/// Count distinct keys in a **sorted** packing-key column by counting transitions, and
/// reject a column that is not nondecreasing.
///
/// The count comes from the values themselves, not from min/max or any index — that is
/// the whole point of scanning the object. `prev` threads across batches so a file read
/// in several batches is still counted as one run of keys.
pub fn count_transitions(values: &[i64], prev: &mut Option<i64>, distinct: &mut u64) -> Result<()> {
    for &v in values {
        match *prev {
            None => *distinct += 1,
            Some(p) if v > p => *distinct += 1,
            Some(p) if v == p => {}
            Some(p) => bail!(
                "packing-key column is not sorted: {v} follows {p}. A final part must be sorted \
                 by the sort key, or the catalog and the scan planner both lie about its order"
            ),
        }
        *prev = Some(v);
    }
    Ok(())
}

/// The key-count band a part falls in — the issue-0014 filter tiers plus the
/// too-dense-to-filter band. Reported so the false-positive guarantee can be stated per
/// band: it holds only through each tier's supported count.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum KeyBand {
    /// A single key: the range predicate is exact; no filter needed.
    Dedicated,
    /// <= 80 keys: the 128-byte tier.
    Le80,
    /// 81..=640: the 1024-byte tier.
    B81To640,
    /// 641..=1280: the 2048-byte tier.
    B641To1280,
    /// 1281..=8000: still filtered, at the largest tier (degrading false-positive rate).
    B1281To8000,
    /// Over 8000 keys: too dense for any bounded filter — no filter is stored, and the
    /// part is always kept. Expected degradation, not a correctness bug.
    Gt8000,
}

impl KeyBand {
    pub fn of(distinct_keys: u64) -> KeyBand {
        match distinct_keys {
            0 | 1 => KeyBand::Dedicated,
            2..=80 => KeyBand::Le80,
            81..=640 => KeyBand::B81To640,
            641..=1280 => KeyBand::B641To1280,
            1281..=8000 => KeyBand::B1281To8000,
            _ => KeyBand::Gt8000,
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            KeyBand::Dedicated => "dedicated (1 key)",
            KeyBand::Le80 => "<=80",
            KeyBand::B81To640 => "81..640",
            KeyBand::B641To1280 => "641..1280",
            KeyBand::B1281To8000 => "1281..8000",
            KeyBand::Gt8000 => ">8000",
        }
    }
}

/// A convergence observation: one read of the fixture's live state.
#[derive(Debug, Clone, PartialEq)]
pub struct Convergence {
    pub live_parts: usize,
    pub l0_parts: usize,
    pub partitions: usize,
    /// Partitions with more than one distinct `created_by_commit` — i.e. more than one
    /// live run. Finalization drives this to zero.
    pub multi_run_partitions: usize,
    pub live_rows: i64,
    /// Sorted live part ids, to compare against the previous poll for stability.
    pub live_part_ids: Vec<i64>,
    /// True iff every live part carries the receipt's partition marker.
    pub all_marked: bool,
}

impl Convergence {
    /// Has the fixture reached one final run per partition and stopped moving?
    ///
    /// Every condition matters: no L0 (the ladder has drained), no multi-run partition
    /// (finalization is done), stable part ids between two polls (nothing in flight), the
    /// row census intact (no row lost or duplicated), and every part still marked (so the
    /// output provably descends from the staged input).
    pub fn converged(prev: &Convergence, cur: &Convergence, expected_rows: i64) -> bool {
        cur.l0_parts == 0
            && cur.multi_run_partitions == 0
            && cur.live_rows == expected_rows
            && cur.all_marked
            && prev.live_part_ids == cur.live_part_ids
            && !cur.live_part_ids.is_empty()
    }

    /// Why an unfinished fixture is still changing — printed by `wait-compacted` so a
    /// slow convergence is legible rather than a silent wait.
    pub fn still_changing(&self, expected_rows: i64) -> String {
        let mut why = Vec::new();
        if self.l0_parts > 0 {
            why.push(format!("{} L0 part(s) still to merge", self.l0_parts));
        }
        if self.multi_run_partitions > 0 {
            why.push(format!(
                "{} partition(s) with >1 run still to finalize",
                self.multi_run_partitions
            ));
        }
        if self.live_rows != expected_rows {
            why.push(format!(
                "row census {} != expected {expected_rows}",
                self.live_rows
            ));
        }
        if !self.all_marked {
            why.push("a live part is missing the artifact marker".to_string());
        }
        if why.is_empty() {
            "settling (waiting for two identical polls)".to_string()
        } else {
            why.join("; ")
        }
    }
}

/// Read one convergence observation from the catalog.
pub async fn observe(
    catalog: &PostgresCatalog,
    ht: HypertableId,
    marker_digest: &str,
) -> Result<Convergence> {
    let parts = catalog.live_parts(ht, None).await?;

    let mut runs_by_partition: BTreeMap<String, std::collections::BTreeSet<i64>> = BTreeMap::new();
    let mut l0 = 0usize;
    let mut rows = 0i64;
    let mut ids = Vec::with_capacity(parts.len());
    let mut all_marked = true;

    for p in &parts {
        if p.meta.level == 0 {
            l0 += 1;
        }
        rows += p.meta.row_count;
        ids.push(p.id.0);
        let pk = p.meta.partition_values.to_string();
        runs_by_partition
            .entry(pk)
            .or_default()
            .insert(p.created_by_commit.0);
        if !part_is_marked(p, marker_digest) {
            all_marked = false;
        }
    }
    ids.sort_unstable();

    let multi_run = runs_by_partition.values().filter(|c| c.len() > 1).count();

    Ok(Convergence {
        live_parts: parts.len(),
        l0_parts: l0,
        partitions: runs_by_partition.len(),
        multi_run_partitions: multi_run,
        live_rows: rows,
        live_part_ids: ids,
        all_marked,
    })
}

/// Does this part carry the receipt's artifact marker? The identity half (the L0 manifest
/// digest) is what proves descent; the day varies per part.
fn part_is_marked(p: &Part, marker_digest: &str) -> bool {
    p.meta
        .partition_values
        .get("l0_manifest")
        .and_then(|v| v.as_str())
        .map(|d| PartShapeReceipt::marker_digest(d) == marker_digest)
        .unwrap_or(false)
}

// ---------------------------------------------------------------------------
// Part-shape report.
// ---------------------------------------------------------------------------

/// One final part, as scanned from its object and its catalog row.
#[derive(Debug, Clone, Serialize)]
pub struct ScannedPart {
    pub path: String,
    pub level: i16,
    pub rows: i64,
    pub bytes: i64,
    pub key_min: i64,
    pub key_max: i64,
    /// Exact distinct keys, counted from the sorted packing-key column of the object.
    pub distinct_keys: u64,
    pub key_density: f64,
    pub band: KeyBand,
    /// Whether the catalog row carries an exact roaring bitmap in column_stats.
    pub has_exact_bitmap: bool,
}

/// The full report. Raw per-part rows plus the aggregates the plan requires.
#[derive(Debug, Clone, Serialize)]
pub struct PartShapeReport {
    pub disclaimer: String,
    pub label: String,
    pub hypertable: String,
    pub placement: String,
    pub receipt_l0_digest: String,
    pub source_manifest_digest: String,

    // Census + integrity.
    pub live_parts: usize,
    pub live_rows: i64,
    pub live_bytes: i64,
    /// The full-row multiset fingerprint of every final object, and whether it matches
    /// the staged input the receipt recorded. Equal counts cannot hide a changed,
    /// duplicated, or lost row.
    pub fingerprint_matches_input: bool,
    pub fingerprint_digest: String,

    // Shape.
    pub file_bytes: Quantiles,
    pub rows_per_part: Quantiles,
    pub distinct_keys: Quantiles,
    pub key_density: Quantiles,

    // Level / run / partition.
    pub level_histogram: Vec<(i16, usize)>,
    pub partitions: usize,
    pub runs_per_partition: Quantiles,
    pub dedicated_fraction: f64,

    // Filter coverage.
    pub exact_bitmap_present: usize,
    pub exact_bitmap_omitted: usize,
    pub key_bands: Vec<(String, usize)>,
    /// From benchmark-local read-only SQL over the `key_filter` column.
    pub key_filter_null: i64,
    pub key_filter_by_size: Vec<(i64, i64)>,
    /// Total `key_filter` bytes, and the parts-table and live-index byte sizes, so the
    /// filter's storage cost can be stated as a fraction.
    pub key_filter_bytes: i64,
    pub parts_table_bytes: i64,
    pub live_index_bytes: i64,

    pub parts: Vec<ScannedPart>,
}

/// The read-only SQL the tool confines to itself: key-filter NULL/size counts and the
/// storage sizes. `key_filter` is not on `PartMeta`, so it has to be read directly.
#[derive(Debug, Clone, Serialize)]
pub struct FilterStorage {
    pub null_count: i64,
    pub by_size: Vec<(i64, i64)>,
    pub total_bytes: i64,
    pub parts_table_bytes: i64,
    pub live_index_bytes: i64,
}

pub async fn filter_storage(catalog: &PostgresCatalog, ht: HypertableId) -> Result<FilterStorage> {
    let pool = catalog.pool_for_tests();

    let null_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM parts WHERE hypertable_id = $1 AND deleted_by_commit IS NULL \
         AND key_filter IS NULL",
    )
    .bind(ht.0)
    .fetch_one(pool)
    .await
    .context("counting NULL key filters")?;

    let by_size: Vec<(i64, i64)> = sqlx::query_as(
        "SELECT octet_length(key_filter)::bigint AS sz, count(*)::bigint FROM parts \
         WHERE hypertable_id = $1 AND deleted_by_commit IS NULL AND key_filter IS NOT NULL \
         GROUP BY sz ORDER BY sz",
    )
    .bind(ht.0)
    .fetch_all(pool)
    .await
    .context("grouping key filters by size")?;

    let total_bytes: i64 = sqlx::query_scalar(
        "SELECT coalesce(sum(octet_length(key_filter)), 0)::bigint FROM parts \
         WHERE hypertable_id = $1 AND deleted_by_commit IS NULL",
    )
    .bind(ht.0)
    .fetch_one(pool)
    .await
    .context("summing key-filter bytes")?;

    // Physical sizes, from PostgreSQL's own accounting. The live index is the one the
    // filter rides in (`parts_live_idx`).
    let parts_table_bytes: i64 = sqlx::query_scalar("SELECT pg_table_size('parts')::bigint")
        .fetch_one(pool)
        .await
        .context("reading parts table size")?;
    let live_index_bytes: i64 =
        sqlx::query_scalar("SELECT pg_relation_size('parts_live_idx')::bigint")
            .fetch_one(pool)
            .await
            .unwrap_or(0);

    Ok(FilterStorage {
        null_count,
        by_size,
        total_bytes,
        parts_table_bytes,
        live_index_bytes,
    })
}

/// Scan one object: count exact distinct keys from the sorted packing-key column, verify
/// sortedness and row/byte agreement, fold the full-row fingerprint, and check the exact
/// bitmap when the catalog row carries one.
pub fn scan_object(
    part: &Part,
    bytes: &[u8],
    packing_key: &str,
    fingerprint: &mut RowMultiset,
) -> Result<ScannedPart> {
    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::copy_from_slice(bytes))
        .with_context(|| format!("opening {}", part.meta.path))?
        .build()
        .with_context(|| format!("reading {}", part.meta.path))?;

    let mut prev: Option<i64> = None;
    let mut distinct = 0u64;
    let mut rows = 0i64;
    let mut key_min = i64::MAX;
    let mut key_max = i64::MIN;

    // The exact bitmap, if the catalog row has one, so we can check the scan against it.
    let bitmap_keys: Option<std::collections::BTreeSet<i64>> = part
        .meta
        .column_stats
        .as_ref()
        .and_then(|s| s.get(ukiel_core::stats::PACKING_KEYS_STAT))
        .and_then(|v| v.as_str())
        .and_then(ukiel_core::stats::bitmap_keys)
        .map(|ks| ks.into_iter().collect());
    let mut seen_keys: std::collections::BTreeSet<i64> = std::collections::BTreeSet::new();

    for batch in reader {
        let batch = batch.with_context(|| format!("decoding {}", part.meta.path))?;
        rows += batch.num_rows() as i64;
        fingerprint
            .update(&batch)
            .map_err(|e| anyhow::anyhow!("{}: fingerprint: {e}", part.meta.path))?;

        let idx = batch
            .schema()
            .index_of(packing_key)
            .map_err(|_| anyhow::anyhow!("{}: no '{packing_key}' column", part.meta.path))?;
        let col = batch
            .column(idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| anyhow::anyhow!("{}: '{packing_key}' is not int64", part.meta.path))?;
        let values: Vec<i64> = col.values().to_vec();
        count_transitions(&values, &mut prev, &mut distinct)
            .with_context(|| part.meta.path.clone())?;
        if let Some(v) = arrow::compute::min(col) {
            key_min = key_min.min(v);
        }
        if let Some(v) = arrow::compute::max(col) {
            key_max = key_max.max(v);
        }
        if bitmap_keys.is_some() {
            seen_keys.extend(values);
        }
    }

    if rows != part.meta.row_count {
        bail!(
            "{}: object holds {rows} rows, catalog says {}",
            part.meta.path,
            part.meta.row_count
        );
    }
    if bytes.len() as i64 != part.meta.size_bytes {
        bail!(
            "{}: object is {} bytes, catalog says {}",
            part.meta.path,
            bytes.len(),
            part.meta.size_bytes
        );
    }
    if key_min != part.meta.packing_key_min || key_max != part.meta.packing_key_max {
        bail!(
            "{}: object key range [{key_min}, {key_max}] != catalog [{}, {}]",
            part.meta.path,
            part.meta.packing_key_min,
            part.meta.packing_key_max
        );
    }
    // Bitmap truth: if the catalog carries an exact bitmap, it must be exactly the keys
    // the object holds. A bitmap that disagrees with its own file is worse than none.
    if let Some(bm) = &bitmap_keys
        && *bm != seen_keys
    {
        bail!(
            "{}: the exact bitmap does not match the object's key set ({} vs {} distinct)",
            part.meta.path,
            bm.len(),
            seen_keys.len()
        );
    }

    let span = (key_max - key_min + 1).max(1);
    Ok(ScannedPart {
        path: part.meta.path.clone(),
        level: part.meta.level,
        rows,
        bytes: bytes.len() as i64,
        key_min,
        key_max,
        distinct_keys: distinct,
        key_density: distinct as f64 / span as f64,
        band: KeyBand::of(distinct),
        has_exact_bitmap: bitmap_keys.is_some(),
    })
}

/// Assemble the aggregate report from the scanned parts and the filter storage.
pub fn build_report(
    receipt: &PartShapeReceipt,
    parts: Vec<ScannedPart>,
    runs_per_partition: Vec<usize>,
    storage: FilterStorage,
    fingerprint: &RowMultiset,
) -> PartShapeReport {
    let live_rows: i64 = parts.iter().map(|p| p.rows).sum();
    let live_bytes: i64 = parts.iter().map(|p| p.bytes).sum();

    let mut level_hist: BTreeMap<i16, usize> = BTreeMap::new();
    let mut band_hist: BTreeMap<&'static str, usize> = BTreeMap::new();
    for p in &parts {
        *level_hist.entry(p.level).or_default() += 1;
        *band_hist.entry(p.band.label()).or_default() += 1;
    }
    let dedicated = parts
        .iter()
        .filter(|p| matches!(p.band, KeyBand::Dedicated))
        .count();
    let with_bitmap = parts.iter().filter(|p| p.has_exact_bitmap).count();

    let fp_matches = receipt.fingerprint.version == prod_synth_integrity::ROW_MULTISET_VERSION
        && receipt.fingerprint.count == fingerprint.count
        && receipt.fingerprint.xor == hex(&fingerprint.xor)
        && receipt.fingerprint.sum == fingerprint.sum;

    PartShapeReport {
        disclaimer: receipt.disclaimer.clone(),
        label: receipt.label.clone(),
        hypertable: receipt.hypertable.clone(),
        placement: receipt.placement.as_str().to_string(),
        receipt_l0_digest: receipt.l0_manifest.digest.clone(),
        source_manifest_digest: receipt.source_manifest.digest.clone(),
        live_parts: parts.len(),
        live_rows,
        live_bytes,
        fingerprint_matches_input: fp_matches,
        fingerprint_digest: fingerprint.digest_hex(),
        file_bytes: Quantiles::of(&mut parts.iter().map(|p| p.bytes as f64).collect::<Vec<_>>()),
        rows_per_part: Quantiles::of(&mut parts.iter().map(|p| p.rows as f64).collect::<Vec<_>>()),
        distinct_keys: Quantiles::of(
            &mut parts
                .iter()
                .map(|p| p.distinct_keys as f64)
                .collect::<Vec<_>>(),
        ),
        key_density: Quantiles::of(&mut parts.iter().map(|p| p.key_density).collect::<Vec<_>>()),
        level_histogram: level_hist.into_iter().collect(),
        partitions: runs_per_partition.len(),
        runs_per_partition: Quantiles::of(
            &mut runs_per_partition
                .iter()
                .map(|&r| r as f64)
                .collect::<Vec<_>>(),
        ),
        dedicated_fraction: if parts.is_empty() {
            0.0
        } else {
            dedicated as f64 / parts.len() as f64
        },
        exact_bitmap_present: with_bitmap,
        exact_bitmap_omitted: parts.len() - with_bitmap,
        key_bands: band_hist
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect(),
        key_filter_null: storage.null_count,
        key_filter_by_size: storage.by_size,
        key_filter_bytes: storage.total_bytes,
        parts_table_bytes: storage.parts_table_bytes,
        live_index_bytes: storage.live_index_bytes,
        parts,
    }
}

fn hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transition_counting_counts_distinct_sorted_keys() {
        let mut prev = None;
        let mut d = 0;
        count_transitions(&[1, 1, 1, 2, 2, 5, 5, 5, 9], &mut prev, &mut d).unwrap();
        assert_eq!(d, 4, "1,2,5,9");

        // Across batches, the run continues: the last key of one batch and the first of
        // the next are the same key, not two.
        let mut prev = None;
        let mut d = 0;
        count_transitions(&[1, 1, 2], &mut prev, &mut d).unwrap();
        count_transitions(&[2, 2, 3], &mut prev, &mut d).unwrap();
        assert_eq!(d, 3, "1,2,3 — the 2 spanning the batch boundary is one key");
    }

    #[test]
    fn an_unsorted_column_is_rejected() {
        let mut prev = None;
        let mut d = 0;
        let e = count_transitions(&[1, 2, 1], &mut prev, &mut d).unwrap_err();
        assert!(e.to_string().contains("not sorted"), "{e}");
    }

    #[test]
    fn key_bands_match_the_filter_tiers() {
        assert_eq!(KeyBand::of(1), KeyBand::Dedicated);
        assert_eq!(KeyBand::of(80), KeyBand::Le80);
        assert_eq!(KeyBand::of(81), KeyBand::B81To640);
        assert_eq!(KeyBand::of(640), KeyBand::B81To640);
        assert_eq!(KeyBand::of(641), KeyBand::B641To1280);
        assert_eq!(KeyBand::of(1280), KeyBand::B641To1280);
        assert_eq!(KeyBand::of(1281), KeyBand::B1281To8000);
        assert_eq!(KeyBand::of(8000), KeyBand::B1281To8000);
        assert_eq!(KeyBand::of(8001), KeyBand::Gt8000);
        assert_eq!(
            KeyBand::of(49_300),
            KeyBand::Gt8000,
            "the median production part"
        );
    }

    #[test]
    fn quantiles_are_the_pinned_definition() {
        let mut v: Vec<f64> = (1..=10).map(|x| x as f64).collect();
        let q = Quantiles::of(&mut v);
        assert_eq!(q.p50, 6.0);
        assert_eq!(q.p90, 9.0);
        assert_eq!(q.max, 10.0);
        assert_eq!(Quantiles::of(&mut []).max, 0.0, "empty is not a panic");
    }

    fn conv(l0: usize, multi: usize, rows: i64, ids: &[i64], marked: bool) -> Convergence {
        Convergence {
            live_parts: ids.len(),
            l0_parts: l0,
            partitions: 3,
            multi_run_partitions: multi,
            live_rows: rows,
            live_part_ids: ids.to_vec(),
            all_marked: marked,
        }
    }

    #[test]
    fn convergence_requires_every_condition() {
        let good = conv(0, 0, 100, &[1, 2, 3], true);
        assert!(
            Convergence::converged(&good, &good, 100),
            "stable, drained, marked"
        );

        // Still has L0.
        assert!(!Convergence::converged(
            &conv(1, 0, 100, &[1, 2, 3], true),
            &conv(1, 0, 100, &[1, 2, 3], true),
            100
        ));
        // Still has a multi-run partition.
        assert!(!Convergence::converged(
            &conv(0, 1, 100, &[1, 2, 3], true),
            &conv(0, 1, 100, &[1, 2, 3], true),
            100
        ));
        // Row census wrong.
        assert!(!Convergence::converged(
            &conv(0, 0, 99, &[1, 2, 3], true),
            &conv(0, 0, 99, &[1, 2, 3], true),
            100
        ));
        // Not marked.
        assert!(!Convergence::converged(
            &conv(0, 0, 100, &[1, 2, 3], false),
            &conv(0, 0, 100, &[1, 2, 3], false),
            100
        ));
        // Part ids changed between polls: not settled.
        assert!(!Convergence::converged(
            &conv(0, 0, 100, &[1, 2, 3], true),
            &conv(0, 0, 100, &[1, 2, 4], true),
            100
        ));
        // Empty is never converged.
        assert!(!Convergence::converged(
            &conv(0, 0, 0, &[], true),
            &conv(0, 0, 0, &[], true),
            0
        ));
    }

    #[test]
    fn still_changing_explains_itself() {
        assert!(
            conv(2, 0, 100, &[1], true)
                .still_changing(100)
                .contains("2 L0")
        );
        assert!(
            conv(0, 1, 100, &[1], true)
                .still_changing(100)
                .contains("finalize")
        );
        assert!(
            conv(0, 0, 99, &[1], true)
                .still_changing(100)
                .contains("census")
        );
        assert!(
            conv(0, 0, 100, &[1], false)
                .still_changing(100)
                .contains("marker")
        );
        assert!(
            conv(0, 0, 100, &[1], true)
                .still_changing(100)
                .contains("settling")
        );
    }
}

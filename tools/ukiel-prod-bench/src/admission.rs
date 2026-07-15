//! The honest admission A/B (plan 46, task 5).
//!
//! Two read-only paths over the same deterministic tenant sequence, run closed-loop by a
//! fixed pool of workers:
//!
//! 1. **range-only** — benchmark-local SQL that reproduces the *pre-issue-0014* predicate
//!    (`packing_key_min <= key AND packing_key_max >= key`) with the **full part-row
//!    projection**, so it ships every range candidate carrying its JSONB `column_stats`;
//! 2. **filtered** — the real `PostgresCatalog::live_parts_pruned`, which applies the
//!    issue-0014 key filter and ships only the survivors.
//!
//! Both paths **deserialize and touch** the returned metadata, because the cost issue
//! 0014 fixed is the JSONB/TOAST/SQLx/network cost of shipping rows nobody needed — a
//! `count(*)` would miss all of it. The comparison is only honest if both arms pay the
//! full per-row price.
//!
//! The range-only SQL lives **here**, in the read-only tool, and never in the product
//! method. The product has no range-only mode, and adding a worse one so a benchmark can
//! measure it would be putting a regression in the product to make a graph.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use serde::Serialize;
use sqlx::Row;
use ukiel_catalog::PostgresCatalog;
use ukiel_core::HypertableId;

/// Which pruning path a run measures.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum AdmissionPath {
    RangeOnly,
    Filtered,
}

impl AdmissionPath {
    pub fn label(self) -> &'static str {
        match self {
            AdmissionPath::RangeOnly => "range-only",
            AdmissionPath::Filtered => "filtered",
        }
    }
}

/// The `VACUUM` posture a run was taken under. The command **records** the phase; it
/// never performs a `VACUUM` — that is an explicit operator step, so a post-vacuum
/// number can never be produced by accident inside a benchmark.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum Phase {
    Unvacuumed,
    Vacuumed,
}

#[derive(Debug, Clone)]
pub struct AdmissionConfig {
    pub workers: usize,
    pub warmup: Duration,
    pub duration: Duration,
    /// Which path runs first, recorded so a report can account for order effects across
    /// interleaved repetitions.
    pub filter_first: bool,
}

/// The outcome of measuring one path.
#[derive(Debug, Clone, Serialize)]
pub struct PathResult {
    pub path: AdmissionPath,
    pub offered: u64,
    pub completed: u64,
    pub failed: u64,
    pub throughput_per_sec: f64,
    pub p50_ms: f64,
    pub p95_ms: f64,
    pub p99_ms: f64,
    pub max_ms: f64,
    /// Mean parts returned per query — the fan-out the path shipped.
    pub mean_parts: f64,
    /// Mean catalog tuple bytes materialized per query (row width incl. JSONB), the cost
    /// issue 0014 removed.
    pub mean_tuple_bytes: f64,
    /// Mean parts the *provider's exact bitmap* would still drop from what this path
    /// shipped — the residue the scan layer catches after the catalog. For the filtered
    /// path this is the Bloom filter's false positives; for range-only it is everything
    /// the filter would have removed.
    pub mean_provider_residue: f64,
}

/// One measured op.
struct Sample {
    latency: Duration,
    parts: u64,
    tuple_bytes: u64,
    provider_residue: u64,
}

/// The columns `live_parts_pruned` returns, replicated so the range-only path ships the
/// same bytes. Kept in step with `ukiel_catalog`'s `PART_COLUMNS` by the equivalence test
/// in `tests/admission.rs`, which proves both paths return the same candidate set before
/// the filter.
const PART_COLUMNS: &str = "id, hypertable_id, path, partition_values, packing_key_min, \
     packing_key_max, row_count, size_bytes, level, column_stats, created_by_commit";

/// One range-only candidate, deserialized so the JSONB cost is really paid.
struct RangeRow {
    partition_values: serde_json::Value,
    column_stats: Option<serde_json::Value>,
    path_len: usize,
}

/// The range-only lookup: the pre-0014 predicate, full projection, JSONB materialized.
async fn range_only(
    catalog: &PostgresCatalog,
    ht: HypertableId,
    key: i64,
) -> Result<Vec<RangeRow>> {
    let sql = format!(
        "SELECT {PART_COLUMNS} FROM parts \
         WHERE hypertable_id = $1 AND deleted_by_commit IS NULL \
           AND packing_key_min <= $2 AND packing_key_max >= $2 \
         ORDER BY id"
    );
    let rows = sqlx::query(sqlx::AssertSqlSafe(sql))
        .bind(ht.0)
        .bind(key)
        .fetch_all(catalog.pool_for_tests())
        .await?;

    // Deserialize every row's JSONB, exactly as the product path does. This is the cost
    // under measurement; skipping it would measure a different, cheaper query.
    let mut out = Vec::with_capacity(rows.len());
    for r in rows {
        let partition_values: serde_json::Value = r.try_get("partition_values")?;
        let column_stats: Option<serde_json::Value> = r.try_get("column_stats")?;
        let path: String = r.try_get("path")?;
        out.push(RangeRow {
            partition_values,
            column_stats,
            path_len: path.len(),
        });
    }
    Ok(out)
}

/// The width of one shipped tuple, approximated from what actually crosses the wire and
/// is materialized: the JSONB blobs and the path string dominate; the fixed scalars are a
/// constant. Enough to compare the two paths honestly — the point is the *ratio*, and the
/// JSONB is where the bytes are.
fn range_row_bytes(r: &RangeRow) -> u64 {
    let stats = r
        .column_stats
        .as_ref()
        .map(|v| v.to_string().len())
        .unwrap_or(0);
    (r.partition_values.to_string().len() + stats + r.path_len + 64) as u64
}

fn part_bytes(p: &ukiel_core::Part) -> u64 {
    let stats = p
        .meta
        .column_stats
        .as_ref()
        .map(|v| v.to_string().len())
        .unwrap_or(0);
    (p.meta.partition_values.to_string().len() + stats + p.meta.path.len() + 64) as u64
}

/// Parts this candidate set holds that the provider's exact bitmap would still drop for
/// `key` — i.e. those whose bitmap proves the key absent. This is the residue the scan
/// layer removes after the catalog.
fn provider_residue(stats: &[Option<serde_json::Value>], key: i64) -> u64 {
    stats
        .iter()
        .filter(|s| {
            // present => keep; the residue is the parts proven absent.
            s.as_ref()
                .and_then(|v| v.get(ukiel_core::stats::PACKING_KEYS_STAT))
                .and_then(|v| v.as_str())
                .and_then(|b| ukiel_core::stats::bitmap_contains(b, key))
                == Some(false)
        })
        .count() as u64
}

/// Run one path closed-loop: warm up, then measure. Returns the aggregated result.
async fn run_path(
    catalog: &PostgresCatalog,
    ht: HypertableId,
    tenants: Arc<Vec<i64>>,
    path: AdmissionPath,
    cfg: &AdmissionConfig,
) -> Result<PathResult> {
    let next = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let measuring = Arc::new(AtomicBool::new(false));

    let mut handles = Vec::with_capacity(cfg.workers);
    for _ in 0..cfg.workers {
        let (catalog, tenants, next, stop, measuring) = (
            catalog.clone(),
            tenants.clone(),
            next.clone(),
            stop.clone(),
            measuring.clone(),
        );
        handles.push(tokio::spawn(async move {
            let mut samples: Vec<Sample> = Vec::new();
            let mut offered = 0u64;
            let mut failed = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let i = next.fetch_add(1, Ordering::Relaxed);
                let key = tenants[(i as usize) % tenants.len()];
                let recording = measuring.load(Ordering::Relaxed);
                if recording {
                    offered += 1;
                }
                let start = Instant::now();
                let sample = match path {
                    AdmissionPath::RangeOnly => range_only(&catalog, ht, key).await.map(|rows| {
                        let stats: Vec<Option<serde_json::Value>> =
                            rows.iter().map(|r| r.column_stats.clone()).collect();
                        Sample {
                            latency: start.elapsed(),
                            parts: rows.len() as u64,
                            tuple_bytes: rows.iter().map(range_row_bytes).sum(),
                            provider_residue: provider_residue(&stats, key),
                        }
                    }),
                    AdmissionPath::Filtered => catalog
                        .live_parts_pruned(ht, Some(key), &[])
                        .await
                        .map_err(anyhow::Error::from)
                        .map(|parts| {
                            let stats: Vec<Option<serde_json::Value>> =
                                parts.iter().map(|p| p.meta.column_stats.clone()).collect();
                            Sample {
                                latency: start.elapsed(),
                                parts: parts.len() as u64,
                                tuple_bytes: parts.iter().map(part_bytes).sum(),
                                provider_residue: provider_residue(&stats, key),
                            }
                        }),
                };
                match sample {
                    Ok(s) if recording => samples.push(s),
                    Ok(_) => {}
                    Err(_) if recording => failed += 1,
                    Err(_) => {}
                }
            }
            (samples, offered, failed)
        }));
    }

    // Warm up (workers run, nothing recorded), then flip on recording, measure, stop.
    tokio::time::sleep(cfg.warmup).await;
    measuring.store(true, Ordering::Relaxed);
    let measure_start = Instant::now();
    tokio::time::sleep(cfg.duration).await;
    stop.store(true, Ordering::Relaxed);
    let elapsed = measure_start.elapsed();

    let mut latencies: Vec<f64> = Vec::new();
    let mut parts = 0u64;
    let mut tuple_bytes = 0u64;
    let mut residue = 0u64;
    let mut offered = 0u64;
    let mut failed = 0u64;
    for h in handles {
        let (samples, off, fail) = h.await.context("admission worker panicked")?;
        offered += off;
        failed += fail;
        for s in samples {
            latencies.push(s.latency.as_secs_f64() * 1000.0);
            parts += s.parts;
            tuple_bytes += s.tuple_bytes;
            residue += s.provider_residue;
        }
    }
    let completed = latencies.len() as u64;
    latencies.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let pct = |p: f64| -> f64 {
        if latencies.is_empty() {
            return 0.0;
        }
        latencies[((p * (latencies.len() - 1) as f64).round() as usize).min(latencies.len() - 1)]
    };

    Ok(PathResult {
        path,
        offered,
        completed,
        failed,
        throughput_per_sec: completed as f64 / elapsed.as_secs_f64(),
        p50_ms: pct(0.50),
        p95_ms: pct(0.95),
        p99_ms: pct(0.99),
        max_ms: latencies.last().copied().unwrap_or(0.0),
        mean_parts: if completed == 0 {
            0.0
        } else {
            parts as f64 / completed as f64
        },
        mean_tuple_bytes: if completed == 0 {
            0.0
        } else {
            tuple_bytes as f64 / completed as f64
        },
        mean_provider_residue: if completed == 0 {
            0.0
        } else {
            residue as f64 / completed as f64
        },
    })
}

/// The full admission comparison, both paths, in the configured order.
#[derive(Debug, Clone, Serialize)]
pub struct AdmissionReport {
    pub phase: Phase,
    pub workers: usize,
    pub warmup_secs: f64,
    pub duration_secs: f64,
    pub path_order: String,
    pub tenants: usize,
    pub range_only: PathResult,
    pub filtered: PathResult,
    /// `range_only.mean_tuple_bytes / filtered.mean_tuple_bytes` — how many times more
    /// bytes the pre-0014 path shipped for the same answers.
    pub bytes_shipped_ratio: f64,
    /// EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) of the filtered path for a representative
    /// key, so buffers and heap fetches are on the record.
    pub filtered_explain: serde_json::Value,
    pub range_only_explain: serde_json::Value,
}

pub async fn run(
    catalog: &PostgresCatalog,
    ht: HypertableId,
    tenants: Vec<i64>,
    phase: Phase,
    cfg: &AdmissionConfig,
) -> Result<AdmissionReport> {
    if tenants.is_empty() {
        bail!("no queryable tenants: the fixture has no logical tables to scope by");
    }
    let tenants = Arc::new(tenants);

    let (first, second) = if cfg.filter_first {
        (AdmissionPath::Filtered, AdmissionPath::RangeOnly)
    } else {
        (AdmissionPath::RangeOnly, AdmissionPath::Filtered)
    };
    let r1 = run_path(catalog, ht, tenants.clone(), first, cfg).await?;
    let r2 = run_path(catalog, ht, tenants.clone(), second, cfg).await?;

    let (range_only, filtered) = match first {
        AdmissionPath::RangeOnly => (r1, r2),
        AdmissionPath::Filtered => (r2, r1),
    };

    let rep_key = tenants[tenants.len() / 2];
    let filtered_explain = explain_filtered(catalog, ht, rep_key).await?;
    let range_only_explain = explain_range_only(catalog, ht, rep_key).await?;

    let ratio = if filtered.mean_tuple_bytes == 0.0 {
        0.0
    } else {
        range_only.mean_tuple_bytes / filtered.mean_tuple_bytes
    };

    Ok(AdmissionReport {
        phase,
        workers: cfg.workers,
        warmup_secs: cfg.warmup.as_secs_f64(),
        duration_secs: cfg.duration.as_secs_f64(),
        path_order: if cfg.filter_first {
            "filter-first".into()
        } else {
            "range-first".into()
        },
        tenants: tenants.len(),
        range_only,
        filtered,
        bytes_shipped_ratio: ratio,
        filtered_explain,
        range_only_explain,
    })
}

async fn explain_range_only(
    catalog: &PostgresCatalog,
    ht: HypertableId,
    key: i64,
) -> Result<serde_json::Value> {
    let sql = format!(
        "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) \
         SELECT {PART_COLUMNS} FROM parts \
         WHERE hypertable_id = $1 AND deleted_by_commit IS NULL \
           AND packing_key_min <= $2 AND packing_key_max >= $2 ORDER BY id"
    );
    let row = sqlx::query(sqlx::AssertSqlSafe(sql))
        .bind(ht.0)
        .bind(key)
        .fetch_one(catalog.pool_for_tests())
        .await?;
    Ok(row.try_get::<serde_json::Value, _>(0)?)
}

/// EXPLAIN of the filtered path. It reruns the real product query shape (the two-phase
/// index-only candidate pass) so the plan nodes, buffers, and heap fetches are recorded.
async fn explain_filtered(
    catalog: &PostgresCatalog,
    ht: HypertableId,
    key: i64,
) -> Result<serde_json::Value> {
    use ukiel_core::keyfilter;
    let binds = keyfilter::sql_binds(key);
    let pred = keyfilter::sql_predicate("key_filter", 3);
    let sql = format!(
        "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) \
         SELECT {PART_COLUMNS} FROM parts WHERE id IN ( \
            SELECT id FROM parts \
            WHERE hypertable_id = $1 AND deleted_by_commit IS NULL \
              AND packing_key_min <= $2 AND packing_key_max >= $2 AND {pred} \
         ) ORDER BY id"
    );
    let mut q = sqlx::query(sqlx::AssertSqlSafe(sql)).bind(ht.0).bind(key);
    for b in &binds {
        q = q.bind(*b);
    }
    let row = q.fetch_one(catalog.pool_for_tests()).await?;
    Ok(row.try_get::<serde_json::Value, _>(0)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provider_residue_counts_proven_absent_parts() {
        let present = ukiel_core::stats::key_bitmap(&one_key_batch(&[10, 20, 30]), "k");
        let stats = vec![
            Some(
                serde_json::json!({ ukiel_core::stats::PACKING_KEYS_STAT: present.clone().unwrap() }),
            ),
            None, // no bitmap: keep (not residue)
        ];
        // key 20 is present in the first, so residue 0; key 999 is absent, residue 1.
        assert_eq!(provider_residue(&stats, 20), 0);
        assert_eq!(provider_residue(&stats, 999), 1);
    }

    fn one_key_batch(keys: &[i64]) -> arrow::array::RecordBatch {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
        arrow::array::RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(keys.to_vec()))])
            .unwrap()
    }

    #[test]
    fn range_row_bytes_dominated_by_jsonb() {
        let big = RangeRow {
            partition_values: serde_json::json!({"utc_day": "2026-07-01"}),
            column_stats: Some(serde_json::json!({"packing_keys": "x".repeat(800)})),
            path_len: 40,
        };
        let small = RangeRow {
            partition_values: serde_json::json!({"utc_day": "2026-07-01"}),
            column_stats: None,
            path_len: 40,
        };
        assert!(
            range_row_bytes(&big) > range_row_bytes(&small) + 700,
            "the bitmap is the weight"
        );
    }
}

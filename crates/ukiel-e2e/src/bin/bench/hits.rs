//! ClickBench `hits` read-benchmark fixture (plan 30, Task 2/3).
//!
//! Loads the partitioned ClickBench parquet files into ukiel parts: a 14-column
//! lowercase subset of the ~100-column source, cast to supported types
//! (`int64`/`timestamp_ms`/`utf8`), day-split, sorted by `(counter_id, ts)`, and
//! committed one-commit-per-file at level 1 with column stats populated. The
//! compactor is deliberately NOT run over this fixture: the source files are
//! row-range slices, so their `counter_id` ranges overlap and a merge would
//! rewrite the whole ~15 GB (and trip the plan-28 cap). Overlap under-measures
//! pruning — a caveat, not a correctness issue (recorded in the runbook note).

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, bail};
use arrow::array::{Array, ArrayRef, Int64Array, RecordBatch, UInt32Array};
use arrow::compute::{cast, concat_batches, take};
use arrow::datatypes::{DataType, Field, Schema};
use object_store::ObjectStoreExt;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Arc;
use ukiel_core::{CommitOp, NamespaceId, PartMeta};
use ukiel_e2e::Stack;

/// (ukiel column, ClickBench source column) for the Int64 subset. `counter_id`
/// (the packing key) and `ts` (from `EventTime`) are handled explicitly.
const INT_COLS: &[(&str, &str)] = &[
    ("watch_id", "WatchID"),
    ("user_id", "UserID"),
    ("region_id", "RegionID"),
    ("adv_engine_id", "AdvEngineID"),
    ("is_refresh", "IsRefresh"),
    ("resolution_width", "ResolutionWidth"),
    ("os", "OS"),
    ("user_agent", "UserAgent"),
];
const UTF8_COLS: &[(&str, &str)] = &[
    ("url", "URL"),
    ("referer", "Referer"),
    ("title", "Title"),
    ("search_phrase", "SearchPhrase"),
];

fn dataset_dir() -> PathBuf {
    PathBuf::from("bench/datasets/hits")
}
fn results_dir() -> PathBuf {
    PathBuf::from("bench/results")
}

/// The ukiel `hits` table schema (JSON, `TableColumns` format).
pub fn hits_schema_json() -> serde_json::Value {
    let mut fields = vec![
        json!({"name": "counter_id", "type": "int64"}),
        json!({"name": "ts", "type": "timestamp_ms"}),
    ];
    for (name, _) in INT_COLS {
        fields.push(json!({"name": name, "type": "int64"}));
    }
    for (name, _) in UTF8_COLS {
        fields.push(json!({"name": name, "type": "utf8"}));
    }
    json!({ "fields": fields })
}

/// Target arrow schema for the written parts (`ts` is Int64 epoch-ms, matching
/// the physical schema `timestamp_ms` maps to).
fn target_schema() -> Arc<Schema> {
    let mut fields = vec![
        Field::new("counter_id", DataType::Int64, true),
        Field::new("ts", DataType::Int64, true),
    ];
    for (name, _) in INT_COLS {
        fields.push(Field::new(*name, DataType::Int64, true));
    }
    for (name, _) in UTF8_COLS {
        fields.push(Field::new(*name, DataType::Utf8, true));
    }
    Arc::new(Schema::new(fields))
}

/// Day partition string (`YYYY-MM-DD`) for an epoch-ms value — the same
/// derivation ingest uses (`chrono::DateTime::from_timestamp_millis`).
fn day_of_ms(ms: i64) -> Option<String> {
    chrono::DateTime::from_timestamp_millis(ms).map(|d| d.date_naive().to_string())
}

fn source_col<'a>(input: &'a RecordBatch, name: &str) -> anyhow::Result<&'a ArrayRef> {
    let idx = input
        .schema()
        .index_of(name)
        .with_context(|| format!("source column '{name}' missing from hits parquet"))?;
    Ok(input.column(idx))
}

/// Casts the ClickBench subset to the ukiel `hits` schema and splits by day.
/// Pure: the sole transform under test. `EventTime` (unix seconds) becomes
/// `ts` epoch-ms; every Int64 source is cast via the arrow kernel.
fn cast_hits_batch(input: &RecordBatch) -> anyhow::Result<Vec<(String, RecordBatch)>> {
    let schema = target_schema();
    let n = input.num_rows();

    // ts = EventTime (seconds) * 1000.
    let evt = cast(source_col(input, "EventTime")?, &DataType::Int64)?;
    let evt = evt
        .as_any()
        .downcast_ref::<Int64Array>()
        .context("EventTime not castable to Int64")?;
    let ts: Int64Array = evt.iter().map(|v| v.map(|s| s * 1000)).collect();

    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    columns.push(cast(source_col(input, "CounterID")?, &DataType::Int64)?);
    columns.push(Arc::new(ts.clone()) as ArrayRef);
    for (_, src) in INT_COLS {
        columns.push(cast(source_col(input, src)?, &DataType::Int64)?);
    }
    for (_, src) in UTF8_COLS {
        columns.push(cast(source_col(input, src)?, &DataType::Utf8)?);
    }
    let full = RecordBatch::try_new(schema.clone(), columns)?;

    // Group row indices by day, then `take` each column per day.
    let mut by_day: HashMap<String, Vec<u32>> = HashMap::new();
    for row in 0..n {
        if let Some(day) = ts.is_valid(row).then(|| day_of_ms(ts.value(row))).flatten() {
            by_day.entry(day).or_default().push(row as u32);
        }
    }
    let mut out = Vec::with_capacity(by_day.len());
    for (day, idx) in by_day {
        let indices = UInt32Array::from(idx);
        let cols: Result<Vec<ArrayRef>, _> = full
            .columns()
            .iter()
            .map(|c| take(c, &indices, None))
            .collect();
        out.push((day, RecordBatch::try_new(schema.clone(), cols?)?));
    }
    Ok(out)
}

fn int64_col<'a>(batch: &'a RecordBatch, name: &str) -> anyhow::Result<&'a Int64Array> {
    let idx = batch.schema().index_of(name)?;
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .with_context(|| format!("{name} is not Int64"))
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TenantCount {
    pub id: i64,
    pub rows: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HitsCensus {
    pub heavy: TenantCount,
    pub median: TenantCount,
    pub light: TenantCount,
    pub top20: Vec<TenantCount>,
}

/// Picks representative tenants from per-`counter_id` row counts: heaviest,
/// median-by-rows, and lightest above 1k rows (falling back to the smallest if
/// none clears the floor). Pure.
fn pick_tenants(counts: &HashMap<i64, i64>) -> anyhow::Result<HitsCensus> {
    if counts.is_empty() {
        bail!("no tenants: empty hits load");
    }
    let mut sorted: Vec<TenantCount> = counts
        .iter()
        .map(|(&id, &rows)| TenantCount { id, rows })
        .collect();
    // Descending by rows, id as a stable tiebreak.
    sorted.sort_by(|a, b| b.rows.cmp(&a.rows).then(a.id.cmp(&b.id)));

    let heavy = sorted[0].clone();
    let median = sorted[sorted.len() / 2].clone();
    let light = sorted
        .iter()
        .rev()
        .find(|t| t.rows > 1000)
        .cloned()
        .unwrap_or_else(|| sorted.last().unwrap().clone());
    let top20 = sorted.iter().take(20).cloned().collect();
    Ok(HitsCensus {
        heavy,
        median,
        light,
        top20,
    })
}

/// Reads one ClickBench parquet file (projecting only the subset columns),
/// concatenating its row groups into one batch.
fn read_hits_file(path: &Path) -> anyhow::Result<RecordBatch> {
    let file = std::fs::File::open(path).with_context(|| format!("open {}", path.display()))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let needed: Vec<&str> = std::iter::once("CounterID")
        .chain(std::iter::once("EventTime"))
        .chain(INT_COLS.iter().map(|(_, s)| *s))
        .chain(UTF8_COLS.iter().map(|(_, s)| *s))
        .collect();
    let mask = ProjectionMask::columns(builder.parquet_schema(), needed.iter().copied());
    let reader = builder
        .with_projection(mask)
        .with_batch_size(65_536)
        .build()?;
    let batches: Vec<RecordBatch> = reader.collect::<Result<_, _>>()?;
    if batches.is_empty() {
        bail!("{} yielded no rows", path.display());
    }
    Ok(concat_batches(&batches[0].schema(), &batches)?)
}

/// `bench hits load [--files N]`: loads present ClickBench files into the
/// `hits` hypertable and writes the tenant census.
pub async fn load(files: Option<usize>) -> anyhow::Result<()> {
    let inputs = present_files(&dataset_dir(), files)?;
    if inputs.is_empty() {
        bail!(
            "no hits parquet files under {} — run `make bench-fetch-hits`",
            dataset_dir().display()
        );
    }
    let stack = Stack::start().await;
    let ht = match stack.catalog.get_hypertable("hits").await {
        Ok(h) => h.id,
        Err(_) => stack
            .catalog
            .create_hypertable(
                "hits",
                &hits_schema_json(),
                &json!({ "columns": ["day"] }),
                &["counter_id".to_string(), "ts".to_string()],
                "counter_id",
            )
            .await
            .context("create hits hypertable")?,
    };

    let mut census: HashMap<i64, i64> = HashMap::new();
    let sort_key = vec!["counter_id".to_string(), "ts".to_string()];
    for (i, path) in inputs.iter().enumerate() {
        let batch = read_hits_file(path)?;
        let day_groups = cast_hits_batch(&batch)?;
        let mut parts = Vec::with_capacity(day_groups.len());
        for (day, group) in day_groups {
            let sorted = ukiel_compactor::rewrite::sort_batch(&group, &sort_key)?;
            let keys = int64_col(&sorted, "counter_id")?;
            for k in keys.iter().flatten() {
                *census.entry(k).or_default() += 1;
            }
            let (kmin, kmax) = (
                arrow::compute::min(keys).unwrap_or(0),
                arrow::compute::max(keys).unwrap_or(0),
            );
            let bytes = ukiel_compactor::rewrite::batch_to_parquet(&sorted, &sort_key)?;
            let path = format!("ht/{}/L1/{}.parquet", ht.0, uuid::Uuid::new_v4());
            let size = bytes.len() as i64;
            stack
                .store
                .put(&object_store::path::Path::from(path.clone()), bytes.into())
                .await?;
            parts.push(PartMeta {
                path,
                partition_values: json!({ "day": day }),
                packing_key_min: kmin,
                packing_key_max: kmax,
                row_count: sorted.num_rows() as i64,
                size_bytes: size,
                level: 1,
                column_stats: ukiel_core::stats::int64_column_stats(&sorted),
            });
        }
        // One commit per input file: each file is its own run.
        stack
            .catalog
            .commit(ht, CommitOp::Add { parts }, None)
            .await
            .with_context(|| format!("commit file {}", path.display()))?;
        println!("loaded {}/{}: {}", i + 1, inputs.len(), path.display());
    }

    let picked = pick_tenants(&census)?;
    // Census tenants get a logical `hits` table in their namespace (= counter_id).
    let mut ns_ids: Vec<i64> = picked.top20.iter().map(|t| t.id).collect();
    for t in [&picked.heavy, &picked.median, &picked.light] {
        if !ns_ids.contains(&t.id) {
            ns_ids.push(t.id);
        }
    }
    for id in ns_ids {
        // Idempotent-ish: ignore "already exists" so re-loads don't abort.
        let _ = stack
            .catalog
            .create_logical_table(NamespaceId(id), "hits", ht)
            .await;
    }
    std::fs::create_dir_all(results_dir())?;
    let out = results_dir().join("hits-tenants.json");
    serde_json::to_writer_pretty(std::fs::File::create(&out)?, &picked)?;
    println!(
        "census: {} tenants; heavy={} ({} rows) median={} light={} → {}",
        census.len(),
        picked.heavy.id,
        picked.heavy.rows,
        picked.median.id,
        picked.light.id,
        out.display()
    );
    Ok(())
}

/// The `hits_*.parquet` files present under `dir`, sorted, capped at `limit`.
fn present_files(dir: &Path, limit: Option<usize>) -> anyhow::Result<Vec<PathBuf>> {
    let mut files: Vec<PathBuf> = match std::fs::read_dir(dir) {
        Ok(rd) => rd
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| p.extension().is_some_and(|x| x == "parquet"))
            .collect(),
        Err(_) => return Ok(Vec::new()),
    };
    files.sort();
    if let Some(n) = limit {
        files.truncate(n);
    }
    Ok(files)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};

    /// A tiny ClickBench-shaped batch: two rows on two different days.
    fn sample_input() -> RecordBatch {
        let mut fields = vec![
            Field::new("CounterID", DataType::Int32, false),
            Field::new("EventTime", DataType::Int64, false),
        ];
        for (_, src) in INT_COLS {
            fields.push(Field::new(*src, DataType::Int32, false));
        }
        for (_, src) in UTF8_COLS {
            fields.push(Field::new(*src, DataType::Utf8, false));
        }
        let schema = Arc::new(Schema::new(fields));
        let mut cols: Vec<ArrayRef> = vec![
            Arc::new(Int32Array::from(vec![42, 42])),
            // 2013-07-01 and 2013-07-02 (unix seconds).
            Arc::new(Int64Array::from(vec![1_372_680_000, 1_372_770_000])),
        ];
        for _ in INT_COLS {
            cols.push(Arc::new(Int32Array::from(vec![1, 2])));
        }
        for _ in UTF8_COLS {
            cols.push(Arc::new(StringArray::from(vec!["a", "b"])));
        }
        RecordBatch::try_new(schema, cols).unwrap()
    }

    #[test]
    fn cast_subset_types_and_ms_conversion() {
        let out = cast_hits_batch(&sample_input()).unwrap();
        // Two distinct days → two groups, one row each.
        assert_eq!(out.len(), 2);
        let total: usize = out.iter().map(|(_, b)| b.num_rows()).sum();
        assert_eq!(total, 2);
        for (day, b) in &out {
            // Target schema: 14 columns, ts is Int64 = seconds*1000.
            assert_eq!(b.num_columns(), 2 + INT_COLS.len() + UTF8_COLS.len());
            assert_eq!(b.column(0).data_type(), &DataType::Int64); // counter_id
            assert_eq!(b.column(1).data_type(), &DataType::Int64); // ts
            let ts = int64_col(b, "ts").unwrap().value(0);
            assert_eq!(&day_of_ms(ts).unwrap(), day);
            assert_eq!(ts % 1000, 0, "ts must be ms (seconds*1000)");
        }
    }

    #[test]
    fn day_split_groups_same_day() {
        // Both rows same day → one group of two.
        let mut fields = vec![
            Field::new("CounterID", DataType::Int32, false),
            Field::new("EventTime", DataType::Int64, false),
        ];
        for (_, src) in INT_COLS {
            fields.push(Field::new(*src, DataType::Int32, false));
        }
        for (_, src) in UTF8_COLS {
            fields.push(Field::new(*src, DataType::Utf8, false));
        }
        let schema = Arc::new(Schema::new(fields));
        let mut cols: Vec<ArrayRef> = vec![
            Arc::new(Int32Array::from(vec![7, 7])),
            Arc::new(Int64Array::from(vec![1_372_680_000, 1_372_680_100])),
        ];
        for _ in INT_COLS {
            cols.push(Arc::new(Int32Array::from(vec![1, 1])));
        }
        for _ in UTF8_COLS {
            cols.push(Arc::new(StringArray::from(vec!["x", "y"])));
        }
        let batch = RecordBatch::try_new(schema, cols).unwrap();
        let out = cast_hits_batch(&batch).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].1.num_rows(), 2);
    }

    #[test]
    fn census_picks_heavy_median_light() {
        let counts = HashMap::from([
            (1, 100_000), // heavy
            (2, 5_000),
            (3, 2_000),
            (4, 500), // below the 1k light floor
        ]);
        let c = pick_tenants(&counts).unwrap();
        assert_eq!(c.heavy.id, 1);
        assert_eq!(c.light.id, 3, "light = smallest above 1k rows");
        assert_eq!(c.top20.len(), 4);
        assert_eq!(c.top20[0].id, 1);
    }
}

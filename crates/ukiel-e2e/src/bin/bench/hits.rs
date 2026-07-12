//! ClickBench read-benchmark fixtures (plan 30 Task 2/3; plan 32 Task 2).
//!
//! Two hypertables are loaded from the same partitioned ClickBench parquet
//! files, by one loader parameterized on a [`Dataset`] descriptor:
//!
//! * **`hits`** (plan 30) — a 14-column *lowercase* subset for the per-tenant
//!   SaaS scenario suite (`counter_id` = tenant, `ts` = epoch-ms).
//! * **`hits_cb`** (plan 32) — the 25 columns the official 43-query ClickBench
//!   suite touches, named **verbatim** (`"CounterID"`, `"EventTime"`, …) so the
//!   upstream file's quoted CamelCase identifiers resolve against it.
//!
//! Both are cast to ukiel's supported types (`int64`/`timestamp_ms`/`utf8` —
//! the source's Int16/Int32/UInt16 widen to int64 and its Binary strings become
//! utf8), day-split, sorted by their sort key, and committed one-commit-per-file
//! at level 1 with column stats populated. The source files are row-range
//! slices, so their `CounterID` ranges overlap and the loaded fixture measures
//! pruning against ~file-count fan-out per tenant. `bench hits compact` collapses
//! that into one run per day-partition: since plan 29 (streaming merge,
//! O(K·batch + row-group) RAM) the ~15 GB fold no longer trips the retired
//! plan-28 cap — running the read suite before and after quantifies the fan-out
//! collapse (bench §5 caveat lifted).

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
use std::time::Instant;
use ukiel_compactor::compactor::{Compactor, CompactorConfig};
use ukiel_core::{CommitOp, HypertableId, NamespaceId, PartMeta};
use ukiel_e2e::Stack;

/// How a source column becomes a ukiel column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Cast {
    /// Arrow-cast to Int64 (Int16/Int32/UInt16 all widen losslessly).
    Int64,
    /// Arrow-cast to Int64, then scale — `hits.ts` is `EventTime` × 1000
    /// (ClickBench stores unix *seconds*; ukiel's `timestamp_ms` is epoch-ms).
    Int64Scaled(i64),
    /// Arrow-cast to Utf8 (the source stores strings as Binary).
    Utf8,
}

/// One column of a fixture: where it comes from and what it becomes.
#[derive(Debug, Clone, Copy)]
pub struct ColSpec {
    /// Column name in the ukiel table (verbatim-CamelCase for `hits_cb`).
    pub ukiel: &'static str,
    /// Column name in the ClickBench source parquet.
    pub src: &'static str,
    pub cast: Cast,
    /// Declared ukiel type — drives the writer's encoding hints.
    pub ukiel_type: &'static str,
}

/// A fixture: one hypertable loaded from the ClickBench parquet files.
pub struct Dataset {
    pub table: &'static str,
    pub cols: &'static [ColSpec],
    pub packing_key: &'static str,
    pub sort_key: &'static [&'static str],
    /// The ukiel column the day partition is derived from, and the factor that
    /// converts its stored value to epoch-ms (`hits.ts` is already ms → 1;
    /// `hits_cb."EventTime"` stays in seconds → 1000).
    pub day_from: (&'static str, i64),
}

impl Dataset {
    pub fn sort_key_vec(&self) -> Vec<String> {
        self.sort_key.iter().map(|s| s.to_string()).collect()
    }

    /// The ukiel table schema (JSON, `TableColumns` format).
    pub fn schema_json(&self) -> serde_json::Value {
        let fields: Vec<serde_json::Value> = self
            .cols
            .iter()
            .map(|c| json!({"name": c.ukiel, "type": c.ukiel_type}))
            .collect();
        json!({ "fields": fields })
    }

    /// Target arrow schema for the written parts. `timestamp_ms` is physically
    /// Int64 (epoch-ms), so every non-utf8 column lands as Int64.
    fn target_schema(&self) -> Arc<Schema> {
        Arc::new(Schema::new(
            self.cols
                .iter()
                .map(|c| {
                    let dt = match c.cast {
                        Cast::Utf8 => DataType::Utf8,
                        _ => DataType::Int64,
                    };
                    Field::new(c.ukiel, dt, true)
                })
                .collect::<Vec<_>>(),
        ))
    }

    /// The source columns to project when reading a ClickBench file.
    fn source_columns(&self) -> Vec<&'static str> {
        let mut v: Vec<&'static str> = self.cols.iter().map(|c| c.src).collect();
        v.sort_unstable();
        v.dedup();
        v
    }
}

/// Plan 30's per-tenant SaaS fixture: a lowercase 14-column subset.
pub const HITS: Dataset = Dataset {
    table: "hits",
    cols: &[
        ColSpec {
            ukiel: "counter_id",
            src: "CounterID",
            cast: Cast::Int64,
            ukiel_type: "int64",
        },
        // ClickBench's EventTime is unix seconds; ukiel's timestamp_ms is epoch-ms.
        ColSpec {
            ukiel: "ts",
            src: "EventTime",
            cast: Cast::Int64Scaled(1000),
            ukiel_type: "timestamp_ms",
        },
        ColSpec {
            ukiel: "watch_id",
            src: "WatchID",
            cast: Cast::Int64,
            ukiel_type: "int64",
        },
        ColSpec {
            ukiel: "user_id",
            src: "UserID",
            cast: Cast::Int64,
            ukiel_type: "int64",
        },
        ColSpec {
            ukiel: "region_id",
            src: "RegionID",
            cast: Cast::Int64,
            ukiel_type: "int64",
        },
        ColSpec {
            ukiel: "adv_engine_id",
            src: "AdvEngineID",
            cast: Cast::Int64,
            ukiel_type: "int64",
        },
        ColSpec {
            ukiel: "is_refresh",
            src: "IsRefresh",
            cast: Cast::Int64,
            ukiel_type: "int64",
        },
        ColSpec {
            ukiel: "resolution_width",
            src: "ResolutionWidth",
            cast: Cast::Int64,
            ukiel_type: "int64",
        },
        ColSpec {
            ukiel: "os",
            src: "OS",
            cast: Cast::Int64,
            ukiel_type: "int64",
        },
        ColSpec {
            ukiel: "user_agent",
            src: "UserAgent",
            cast: Cast::Int64,
            ukiel_type: "int64",
        },
        ColSpec {
            ukiel: "url",
            src: "URL",
            cast: Cast::Utf8,
            ukiel_type: "utf8",
        },
        ColSpec {
            ukiel: "referer",
            src: "Referer",
            cast: Cast::Utf8,
            ukiel_type: "utf8",
        },
        ColSpec {
            ukiel: "title",
            src: "Title",
            cast: Cast::Utf8,
            ukiel_type: "utf8",
        },
        ColSpec {
            ukiel: "search_phrase",
            src: "SearchPhrase",
            cast: Cast::Utf8,
            ukiel_type: "utf8",
        },
    ],
    packing_key: "counter_id",
    sort_key: &["counter_id", "ts"],
    day_from: ("ts", 1),
};

/// Plan 32's official-suite fixture: the 25 columns the 43 ClickBench queries
/// reference, **named verbatim** so the upstream file runs unmodified.
///
/// Type deltas from the source (each an entry in
/// `bench/queries/clickbench/ADAPTATIONS.md`): every integer widens to int64
/// (ukiel has no narrower integer type); Binary strings become utf8 — the same
/// thing upstream's own `create.sql` does with `binary_as_string=true`;
/// `"EventDate"` (UInt16 day number) stays a **day number** as int64 rather than
/// becoming a DATE, since ukiel has no date type — the only adaptation that
/// touches query *text*.
///
/// `"EventTime"` stays unix **seconds** (the queries call
/// `to_timestamp_seconds` on it), so it is declared `int64`, not `timestamp_ms`
/// — which costs the delta-packed-ts encoding hint but keeps the column's
/// meaning honest.
pub const HITS_CB: Dataset = Dataset {
    table: "hits_cb",
    cols: &[
        ColSpec {
            ukiel: "CounterID",
            src: "CounterID",
            cast: Cast::Int64,
            ukiel_type: "int64",
        },
        ColSpec {
            ukiel: "EventTime",
            src: "EventTime",
            cast: Cast::Int64,
            ukiel_type: "int64",
        },
        ColSpec {
            ukiel: "EventDate",
            src: "EventDate",
            cast: Cast::Int64,
            ukiel_type: "int64",
        },
        ColSpec {
            ukiel: "UserID",
            src: "UserID",
            cast: Cast::Int64,
            ukiel_type: "int64",
        },
        ColSpec {
            ukiel: "WatchID",
            src: "WatchID",
            cast: Cast::Int64,
            ukiel_type: "int64",
        },
        ColSpec {
            ukiel: "ClientIP",
            src: "ClientIP",
            cast: Cast::Int64,
            ukiel_type: "int64",
        },
        ColSpec {
            ukiel: "URLHash",
            src: "URLHash",
            cast: Cast::Int64,
            ukiel_type: "int64",
        },
        ColSpec {
            ukiel: "RefererHash",
            src: "RefererHash",
            cast: Cast::Int64,
            ukiel_type: "int64",
        },
        ColSpec {
            ukiel: "AdvEngineID",
            src: "AdvEngineID",
            cast: Cast::Int64,
            ukiel_type: "int64",
        },
        ColSpec {
            ukiel: "ResolutionWidth",
            src: "ResolutionWidth",
            cast: Cast::Int64,
            ukiel_type: "int64",
        },
        ColSpec {
            ukiel: "RegionID",
            src: "RegionID",
            cast: Cast::Int64,
            ukiel_type: "int64",
        },
        ColSpec {
            ukiel: "MobilePhone",
            src: "MobilePhone",
            cast: Cast::Int64,
            ukiel_type: "int64",
        },
        ColSpec {
            ukiel: "SearchEngineID",
            src: "SearchEngineID",
            cast: Cast::Int64,
            ukiel_type: "int64",
        },
        ColSpec {
            ukiel: "IsRefresh",
            src: "IsRefresh",
            cast: Cast::Int64,
            ukiel_type: "int64",
        },
        ColSpec {
            ukiel: "DontCountHits",
            src: "DontCountHits",
            cast: Cast::Int64,
            ukiel_type: "int64",
        },
        ColSpec {
            ukiel: "IsLink",
            src: "IsLink",
            cast: Cast::Int64,
            ukiel_type: "int64",
        },
        ColSpec {
            ukiel: "IsDownload",
            src: "IsDownload",
            cast: Cast::Int64,
            ukiel_type: "int64",
        },
        ColSpec {
            ukiel: "TraficSourceID",
            src: "TraficSourceID",
            cast: Cast::Int64,
            ukiel_type: "int64",
        },
        ColSpec {
            ukiel: "WindowClientWidth",
            src: "WindowClientWidth",
            cast: Cast::Int64,
            ukiel_type: "int64",
        },
        ColSpec {
            ukiel: "WindowClientHeight",
            src: "WindowClientHeight",
            cast: Cast::Int64,
            ukiel_type: "int64",
        },
        ColSpec {
            ukiel: "MobilePhoneModel",
            src: "MobilePhoneModel",
            cast: Cast::Utf8,
            ukiel_type: "utf8",
        },
        ColSpec {
            ukiel: "SearchPhrase",
            src: "SearchPhrase",
            cast: Cast::Utf8,
            ukiel_type: "utf8",
        },
        ColSpec {
            ukiel: "URL",
            src: "URL",
            cast: Cast::Utf8,
            ukiel_type: "utf8",
        },
        ColSpec {
            ukiel: "Title",
            src: "Title",
            cast: Cast::Utf8,
            ukiel_type: "utf8",
        },
        ColSpec {
            ukiel: "Referer",
            src: "Referer",
            cast: Cast::Utf8,
            ukiel_type: "utf8",
        },
    ],
    packing_key: "CounterID",
    sort_key: &["CounterID", "EventTime"],
    day_from: ("EventTime", 1000),
};

fn dataset_dir() -> PathBuf {
    PathBuf::from("bench/datasets/hits")
}
fn results_dir() -> PathBuf {
    PathBuf::from("bench/results")
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

/// Casts a ClickBench batch to `dataset`'s ukiel schema and splits it by day.
/// Pure: the sole transform under test, shared by both fixtures.
fn cast_batch(
    dataset: &Dataset,
    input: &RecordBatch,
) -> anyhow::Result<Vec<(String, RecordBatch)>> {
    let schema = dataset.target_schema();
    let n = input.num_rows();

    let mut columns: Vec<ArrayRef> = Vec::with_capacity(dataset.cols.len());
    for c in dataset.cols {
        let src = source_col(input, c.src)?;
        let arr: ArrayRef = match c.cast {
            Cast::Int64 => cast(src, &DataType::Int64)?,
            Cast::Int64Scaled(factor) => {
                let ints = cast(src, &DataType::Int64)?;
                let ints = ints
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .with_context(|| format!("{} not castable to Int64", c.src))?;
                let scaled: Int64Array = ints.iter().map(|v| v.map(|x| x * factor)).collect();
                Arc::new(scaled) as ArrayRef
            }
            Cast::Utf8 => cast(src, &DataType::Utf8)?,
        };
        columns.push(arr);
    }
    let full = RecordBatch::try_new(schema.clone(), columns)?;

    // Group row indices by day, then `take` each column per day.
    let (day_col, to_ms) = dataset.day_from;
    let time = int64_col(&full, day_col)?;
    let mut by_day: HashMap<String, Vec<u32>> = HashMap::new();
    for row in 0..n {
        if let Some(day) = time
            .is_valid(row)
            .then(|| day_of_ms(time.value(row) * to_ms))
            .flatten()
        {
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

/// Reads one ClickBench parquet file (projecting only the dataset's columns),
/// concatenating its row groups into one batch.
fn read_hits_file(dataset: &Dataset, path: &Path) -> anyhow::Result<RecordBatch> {
    let file = std::fs::File::open(path).with_context(|| format!("open {}", path.display()))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let mask = ProjectionMask::columns(builder.parquet_schema(), dataset.source_columns());
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

/// Loads the present ClickBench files into `dataset`'s hypertable: day-split,
/// sorted, one L1 commit per input file. Returns the per-packing-key row census.
async fn load_dataset(
    stack: &Stack,
    dataset: &Dataset,
    inputs: &[PathBuf],
) -> anyhow::Result<HashMap<i64, i64>> {
    let ht = match stack.catalog.get_hypertable(dataset.table).await {
        Ok(h) => h.id,
        Err(_) => stack
            .catalog
            .create_hypertable(
                dataset.table,
                &dataset.schema_json(),
                &json!({ "columns": ["day"] }),
                &dataset.sort_key_vec(),
                dataset.packing_key,
            )
            .await
            .with_context(|| format!("create {} hypertable", dataset.table))?,
    };

    let mut census: HashMap<i64, i64> = HashMap::new();
    let sort_key = dataset.sort_key_vec();
    for (i, path) in inputs.iter().enumerate() {
        let batch = read_hits_file(dataset, path)?;
        let day_groups = cast_batch(dataset, &batch)?;
        let mut parts = Vec::with_capacity(day_groups.len());
        for (day, group) in day_groups {
            let sorted = ukiel_compactor::rewrite::sort_batch(&group, &sort_key)?;
            let keys = int64_col(&sorted, dataset.packing_key)?;
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
        println!(
            "{}: loaded {}/{}: {}",
            dataset.table,
            i + 1,
            inputs.len(),
            path.display()
        );
    }
    Ok(census)
}

/// The ClickBench files to load, or a loud error pointing at the fetch target.
fn require_inputs(files: Option<usize>) -> anyhow::Result<Vec<PathBuf>> {
    let inputs = present_files(&dataset_dir(), files)?;
    if inputs.is_empty() {
        bail!(
            "no hits parquet files under {} — run `make bench-fetch-hits`",
            dataset_dir().display()
        );
    }
    Ok(inputs)
}

/// `bench clickbench load [--files N]`: loads the official-suite fixture
/// (`hits_cb`, verbatim column names). No census and no logical tables — the
/// 43 queries are whole-table and run through the unscoped operator session.
pub async fn load_clickbench(files: Option<usize>) -> anyhow::Result<()> {
    let inputs = require_inputs(files)?;
    let stack = Stack::start().await;
    let census = load_dataset(&stack, &HITS_CB, &inputs).await?;
    let rows: i64 = census.values().sum();
    println!(
        "hits_cb: {rows} rows over {} CounterIDs from {} file(s) — run `bench clickbench run`",
        census.len(),
        inputs.len()
    );
    Ok(())
}

/// `bench hits load [--files N]`: loads present ClickBench files into the
/// `hits` hypertable and writes the tenant census.
pub async fn load(files: Option<usize>) -> anyhow::Result<()> {
    let inputs = require_inputs(files)?;
    let stack = Stack::start().await;
    let census = load_dataset(&stack, &HITS, &inputs).await?;
    let ht = stack.catalog.get_hypertable(HITS.table).await?.id;

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

/// Live-part fan-out over a hypertable: total parts, how many day-partitions
/// they span, and the worst per-partition run count (the read-side fan-out a
/// whole-partition query pays).
struct RunSummary {
    parts: usize,
    partitions: usize,
    max_per_partition: usize,
}

async fn partition_run_summary(stack: &Stack, ht: HypertableId) -> anyhow::Result<RunSummary> {
    let parts = stack.catalog.live_parts(ht, None).await?;
    let mut per: HashMap<String, usize> = HashMap::new();
    for p in &parts {
        *per.entry(p.meta.partition_values.to_string()).or_default() += 1;
    }
    Ok(RunSummary {
        parts: parts.len(),
        partitions: per.len(),
        max_per_partition: per.values().copied().max().unwrap_or(0),
    })
}

/// `bench {hits,clickbench} compact [--target-mb N]`: drives compaction +
/// finalization to quiescence over `dataset`'s loaded fixture, folding the
/// per-file L1 runs into one run per day-partition (the plan-17 terminal
/// invariant). This is the plan-29 exercise at volume — the ~15 GB fold streams
/// in bounded memory where the retired plan-28 cap once forbade it. With
/// `--target-mb N` the hypertable switches to `SizeTargeted(N MiB)` placement
/// first, so heavy keys **whale-split** into dedicated files instead of
/// collapsing into one big packed file per day (restoring scan parallelism for
/// the whale while the tail still shrinks). Run the matching read suite before
/// and after to quantify the fan-out collapse (lifts the bench §5 caveat).
pub async fn compact(dataset: &Dataset, target_mb: Option<usize>) -> anyhow::Result<()> {
    let stack = Stack::start().await;
    let ht = stack
        .catalog
        .get_hypertable(dataset.table)
        .await
        .with_context(|| format!("{} not loaded — load it first", dataset.table))?
        .id;

    if let Some(mb) = target_mb {
        let bytes = (mb * 1024 * 1024) as i64;
        stack
            .catalog
            .set_placement(ht, ukiel_core::Placement::SizeTargeted(bytes))
            .await?;
        println!("placement: SizeTargeted({mb} MiB) — heavy keys whale-split");
    }

    let before = partition_run_summary(&stack, ht).await?;
    println!(
        "{}: before: {} live parts / {} day-partitions (worst {}/day)",
        dataset.table, before.parts, before.partitions, before.max_per_partition
    );

    // finalize_after_secs: 0 — the loaded fixture is quiet by construction, so
    // every partition is immediately foldable to its single terminal run.
    let drain = Compactor::new(
        stack.catalog.clone(),
        stack.store.clone(),
        CompactorConfig {
            worker_name: "bench-hits-compact".to_string(),
            l0_fanout: 4,
            fanout: 10,
            finalize_after_secs: 0,
            ..CompactorConfig::default()
        },
    );
    let comp_start = Instant::now();
    while drain.run_once().await?.merged_groups > 0 {}
    let compaction_secs = comp_start.elapsed().as_secs_f64();
    let fin_start = Instant::now();
    while drain.finalize_once().await? > 0 {}
    let finalization_secs = fin_start.elapsed().as_secs_f64();

    let after = partition_run_summary(&stack, ht).await?;
    println!(
        "{}: after:  {} live parts / {} day-partitions (worst {}/day)",
        dataset.table, after.parts, after.partitions, after.max_per_partition
    );
    let t = dataset.table;
    println!(
        "PERF {t}/compact/compaction_secs={compaction_secs:.1}\n\
         PERF {t}/compact/finalization_secs={finalization_secs:.1}\n\
         fan-out collapse: worst {}→{} runs/day-partition (terminal invariant: 1 cold run/partition)",
        before.max_per_partition, after.max_per_partition
    );

    // Per-tenant fan-out collapse for the census classes, if a census exists.
    if let Ok(f) = std::fs::File::open(results_dir().join("hits-tenants.json"))
        && let Ok(census) = serde_json::from_reader::<_, HitsCensus>(f)
    {
        for (class, t) in [
            ("heavy", &census.heavy),
            ("median", &census.median),
            ("light", &census.light),
        ] {
            let fo = stack.live_part_count(ht, Some(t.id)).await;
            println!("  {class} tenant {} fan-out now {fo}", t.id);
        }
    }
    Ok(())
}

#[derive(Debug, Serialize)]
struct ScenarioResult {
    name: String,
    median_ms: f64,
}

#[derive(Debug, Serialize)]
struct TenantReport {
    class: String,
    id: i64,
    census_rows: i64,
    observed_rows: i64,
    fan_out: usize,
    scenarios: Vec<ScenarioResult>,
}

#[derive(Debug, Serialize)]
struct HitsQueryReport {
    label: String,
    tenants: Vec<TenantReport>,
}

/// Reads `rows[0]["n"]` from a count query response.
fn scalar_n(rows: &serde_json::Value) -> i64 {
    rows.get(0)
        .and_then(|r| r.get("n"))
        .and_then(|n| n.as_i64())
        .unwrap_or(0)
}

/// `bench hits queries [--iters N] [--label L]`: runs the per-tenant ClickBench
/// scenario suite through the real HTTP query endpoint, reporting warm medians.
pub async fn queries(iters: usize, label: &str) -> anyhow::Result<()> {
    let census: HitsCensus = serde_json::from_reader(std::fs::File::open(
        results_dir().join("hits-tenants.json"),
    )?)
    .context("read hits-tenants.json — run `bench hits load` first")?;
    let stack = Stack::start().await;
    let ht = stack.catalog.get_hypertable("hits").await?.id;

    let classes = [
        ("heavy", &census.heavy),
        ("median", &census.median),
        ("light", &census.light),
    ];
    let mut reports = Vec::new();
    for (class, tenant) in classes {
        let ns = NamespaceId(tenant.id);
        // Windowed scenarios need a real, non-empty ts range for this tenant.
        let range = stack
            .query(ns, "SELECT min(ts) AS lo, max(ts) AS hi FROM hits")
            .await
            .map_err(|e| anyhow::anyhow!("range query for tenant {}: {e}", tenant.id))?;
        let lo = range
            .get(0)
            .and_then(|r| r.get("lo"))
            .and_then(|v| v.as_i64())
            .unwrap_or(0);
        let hi = range
            .get(0)
            .and_then(|r| r.get("hi"))
            .and_then(|v| v.as_i64())
            .unwrap_or(lo);
        let mid = lo + (hi - lo) / 2;
        let (wlo, whi) = (mid, mid + 24 * 3600 * 1000);

        let scenarios: Vec<(&str, String)> = vec![
            ("count", "SELECT count(*) AS n FROM hits".to_string()),
            ("adv_filter", "SELECT count(*) AS n FROM hits WHERE adv_engine_id <> 0".to_string()),
            ("distinct_users", "SELECT count(DISTINCT user_id) AS n FROM hits".to_string()),
            ("top_urls", "SELECT url, count(*) AS c FROM hits GROUP BY url ORDER BY c DESC LIMIT 10".to_string()),
            ("search_phrases", "SELECT search_phrase, count(*) AS c FROM hits WHERE search_phrase <> '' GROUP BY search_phrase ORDER BY c DESC LIMIT 10".to_string()),
            ("ts_window", format!("SELECT count(*) AS n FROM hits WHERE ts BETWEEN {wlo} AND {whi}")),
            ("minutely", format!("SELECT ts / 60000 AS m, count(*) AS c FROM hits WHERE ts BETWEEN {wlo} AND {whi} GROUP BY m ORDER BY m")),
        ];

        let observed_rows = scalar_n(
            &stack
                .query(ns, "SELECT count(*) AS n FROM hits")
                .await
                .map_err(|e| anyhow::anyhow!("count for tenant {}: {e}", tenant.id))?,
        );
        // Namespace isolation at volume: the count must equal the census.
        if observed_rows != tenant.rows {
            bail!(
                "tenant {} count mismatch: query {observed_rows} != census {}",
                tenant.id,
                tenant.rows
            );
        }
        let fan_out = stack.live_part_count(ht, Some(tenant.id)).await;

        let mut results = Vec::new();
        for (name, sql) in &scenarios {
            let ms = crate::report::warm_median_ms(iters, || async {
                stack
                    .query(ns, sql)
                    .await
                    .map_err(|e| anyhow::anyhow!("{name}: {e}"))
            })
            .await?;
            println!("PERF hits/{class}/{name}={ms:.1}");
            results.push(ScenarioResult {
                name: name.to_string(),
                median_ms: ms,
            });
        }
        reports.push(TenantReport {
            class: class.to_string(),
            id: tenant.id,
            census_rows: tenant.rows,
            observed_rows,
            fan_out,
            scenarios: results,
        });
    }

    let report = HitsQueryReport {
        label: label.to_string(),
        tenants: reports,
    };
    let out = crate::report::write_json(&format!("hits-queries-{label}.json"), &report)?;
    println!("wrote {}", out.display());
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
    use arrow::array::{BinaryArray, Int16Array, Int32Array, UInt16Array};

    /// 2013-07-01 and 2013-07-02, in ClickBench's unix seconds.
    const T1: i64 = 1_372_680_000;
    const T2: i64 = 1_372_770_000;

    /// A ClickBench-shaped source batch for `dataset`, with the source's real
    /// types: narrow signed ints, `EventDate` as UInt16, strings as Binary.
    /// `times` are unix seconds; every other column is filler.
    fn sample_input(dataset: &Dataset, keys: Vec<i64>, times: Vec<i64>) -> RecordBatch {
        let n = keys.len();
        let mut fields = Vec::new();
        let mut cols: Vec<ArrayRef> = Vec::new();
        for c in dataset.source_columns() {
            let (dt, arr): (DataType, ArrayRef) = match c {
                "CounterID" => (
                    DataType::Int32,
                    Arc::new(Int32Array::from(
                        keys.iter().map(|k| *k as i32).collect::<Vec<_>>(),
                    )),
                ),
                "EventTime" => (DataType::Int64, Arc::new(Int64Array::from(times.clone()))),
                // UInt16 day number, as in the source parquet.
                "EventDate" => (
                    DataType::UInt16,
                    Arc::new(UInt16Array::from(
                        times
                            .iter()
                            .map(|t| (t / 86_400) as u16)
                            .collect::<Vec<_>>(),
                    )),
                ),
                other => {
                    let is_utf8 = dataset
                        .cols
                        .iter()
                        .any(|c| c.src == other && c.cast == Cast::Utf8);
                    if is_utf8 {
                        (
                            DataType::Binary,
                            Arc::new(BinaryArray::from_iter_values(
                                (0..n).map(|i| format!("s{i}").into_bytes()),
                            )),
                        )
                    } else {
                        (
                            DataType::Int16,
                            Arc::new(Int16Array::from(
                                (0..n).map(|i| i as i16).collect::<Vec<_>>(),
                            )),
                        )
                    }
                }
            };
            fields.push(Field::new(c, dt, false));
            cols.push(arr);
        }
        RecordBatch::try_new(Arc::new(Schema::new(fields)), cols).unwrap()
    }

    #[test]
    fn cast_subset_types_and_ms_conversion() {
        let input = sample_input(&HITS, vec![42, 42], vec![T1, T2]);
        let out = cast_batch(&HITS, &input).unwrap();
        // Two distinct days → two groups, one row each.
        assert_eq!(out.len(), 2);
        let total: usize = out.iter().map(|(_, b)| b.num_rows()).sum();
        assert_eq!(total, 2);
        for (day, b) in &out {
            assert_eq!(b.num_columns(), HITS.cols.len());
            assert_eq!(b.column(0).data_type(), &DataType::Int64); // counter_id
            assert_eq!(b.column(1).data_type(), &DataType::Int64); // ts
            let ts = int64_col(b, "ts").unwrap().value(0);
            assert_eq!(&day_of_ms(ts).unwrap(), day);
            assert_eq!(ts % 1000, 0, "ts must be ms (seconds*1000)");
        }
    }

    #[test]
    fn day_split_groups_same_day() {
        // Both rows the same day → one group of two.
        let input = sample_input(&HITS, vec![7, 7], vec![T1, T1 + 100]);
        let out = cast_batch(&HITS, &input).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].1.num_rows(), 2);
    }

    /// The official-suite fixture is what makes the vendored queries runnable
    /// verbatim: ClickBench's own column names, `EventTime` still in *seconds*
    /// (the queries wrap it in `to_timestamp_seconds`), and `EventDate` as the
    /// raw day number — while the day partition is still derived correctly.
    #[test]
    fn clickbench_descriptor_keeps_verbatim_names_and_seconds() {
        // Names are verbatim ClickBench, and every query column is present.
        let names: Vec<&str> = HITS_CB.cols.iter().map(|c| c.ukiel).collect();
        assert_eq!(names.len(), 25);
        for required in [
            "CounterID",
            "EventTime",
            "EventDate",
            "UserID",
            "SearchPhrase",
            "URL",
            "Title",
            "Referer",
            "MobilePhoneModel",
            "WindowClientHeight",
        ] {
            assert!(names.contains(&required), "{required} missing from hits_cb");
        }
        // The packing key leads the sort key (plan-27 registration invariant).
        assert_eq!(HITS_CB.sort_key[0], HITS_CB.packing_key);

        let input = sample_input(&HITS_CB, vec![62, 62], vec![T1, T2]);
        let out = cast_batch(&HITS_CB, &input).unwrap();
        assert_eq!(out.len(), 2, "two days → two partitions");

        for (day, b) in &out {
            assert_eq!(b.num_columns(), 25);
            let evt = int64_col(b, "EventTime").unwrap().value(0);
            assert!(
                evt == T1 || evt == T2,
                "EventTime must stay unix seconds, got {evt}"
            );
            // The day partition still derives correctly, from seconds.
            assert_eq!(&day_of_ms(evt * 1000).unwrap(), day);
            // EventDate is the raw day number, consistent with EventTime.
            let date = int64_col(b, "EventDate").unwrap().value(0);
            assert_eq!(date, evt / 86_400);
            // Strings survive the Binary → Utf8 cast.
            assert_eq!(
                b.column(b.schema().index_of("URL").unwrap()).data_type(),
                &DataType::Utf8
            );
        }
        // 2013-07-01 is day 15887 — the literal the adapted queries use.
        assert_eq!(T1 / 86_400, 15_887);
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

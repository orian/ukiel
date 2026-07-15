//! Register one laboratory artifact as a DataFusion table behind its logical-projection
//! view, and run declared queries with cold/warm timing, plan capture, and (in
//! object-store mode) truthful I/O accounting.

use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result};
use arrow::array::RecordBatch;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::physical_plan::displayable;
use datafusion::prelude::{SessionConfig, SessionContext};
use object_store::ObjectStore;
use object_store::memory::InMemory;
use object_store::path::Path as ObjPath;

use crate::counting_store::{CounterSnapshot, Counters, CountingObjectStore};

/// The reader switches a run pins, recorded in every report. A writer feature is not
/// credited when its reader is disabled, so these are always reported alongside a result.
#[derive(Debug, Clone, Copy, serde::Serialize)]
pub struct ReaderFlags {
    pub enable_page_index: bool,
    pub pruning: bool,
    pub pushdown_filters: bool,
    pub reorder_filters: bool,
    pub bloom_filter_on_read: bool,
}

impl Default for ReaderFlags {
    fn default() -> Self {
        ReaderFlags {
            enable_page_index: true,
            pruning: true,
            pushdown_filters: true,
            reorder_filters: true,
            bloom_filter_on_read: true,
        }
    }
}

/// One artifact's files, referenced by relative path under a base directory. Bytes are read
/// lazily by the backing store — a publishable mode never preloads a whole part into RAM.
pub struct Artifact {
    pub base_dir: std::path::PathBuf,
    /// Relative paths for every Parquet file.
    pub files: Vec<String>,
}

/// The storage backend a session reads through.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// The decode-only micro mode: the files are preloaded into an in-memory store. Fast,
    /// but not a storage measurement — no I/O is accounted.
    Memory,
    /// A real local-filesystem object store. Reads ranges from disk without preloading; no
    /// I/O accounting (use object-store mode for that).
    Local,
    /// A real object store (a local-filesystem-backed one here, or S3/MinIO via a receipt),
    /// wrapped in the counting store so every request and byte range is accounted. Never
    /// preloads a full file.
    ObjectStore,
}

/// A session over the artifact, plus the counters when instrumented.
pub struct Session {
    pub ctx: SessionContext,
    pub counters: Option<Arc<Counters>>,
}

fn scheme_url(mode: Mode) -> url::Url {
    match mode {
        Mode::Memory => url::Url::parse("memory://").expect("url"),
        Mode::Local | Mode::ObjectStore => url::Url::parse("file://").expect("url"),
    }
}

impl Artifact {
    /// Build a fresh session over the artifact, register it as `events_physical`, and create
    /// the logical `events` view.
    pub async fn session(&self, mode: Mode, flags: ReaderFlags, view_sql: &str) -> Result<Session> {
        // Build the backing store and the listing URLs for the chosen mode.
        let (store, counters, urls): (
            Arc<dyn ObjectStore>,
            Option<Arc<Counters>>,
            Vec<ListingTableUrl>,
        ) = match mode {
            Mode::Memory => {
                // Decode-only: preload into memory (the one mode that legitimately does).
                let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
                for rel in &self.files {
                    let bytes = std::fs::read(self.base_dir.join(rel))
                        .with_context(|| format!("reading {rel}"))?;
                    inner
                        .put_opts(
                            &ObjPath::from(rel.as_str()),
                            bytes.into(),
                            Default::default(),
                        )
                        .await?;
                }
                let urls = self
                    .files
                    .iter()
                    .map(|p| ListingTableUrl::parse(format!("memory:///{p}")))
                    .collect::<Result<_, _>>()?;
                (inner, None, urls)
            }
            Mode::Local | Mode::ObjectStore => {
                // A real filesystem object store: reads ranges from disk, no preload.
                let abs = std::fs::canonicalize(&self.base_dir)
                    .with_context(|| format!("resolving {}", self.base_dir.display()))?;
                let local: Arc<dyn ObjectStore> =
                    Arc::new(object_store::local::LocalFileSystem::new());
                let (store, counters): (Arc<dyn ObjectStore>, Option<Arc<Counters>>) =
                    if mode == Mode::ObjectStore {
                        let (c, ctr) = CountingObjectStore::new(local);
                        (c, Some(ctr))
                    } else {
                        (local, None)
                    };
                let urls = self
                    .files
                    .iter()
                    .map(|p| {
                        let full = abs.join(p);
                        ListingTableUrl::parse(format!("file://{}", full.display()))
                    })
                    .collect::<Result<_, _>>()?;
                (store, counters, urls)
            }
        };

        let mut config = SessionConfig::new();
        {
            let opts = config.options_mut();
            opts.execution.parquet.pushdown_filters = flags.pushdown_filters;
            opts.execution.parquet.reorder_filters = flags.reorder_filters;
            opts.execution.parquet.pruning = flags.pruning;
            opts.execution.parquet.enable_page_index = flags.enable_page_index;
            opts.execution.parquet.bloom_filter_on_read = flags.bloom_filter_on_read;
        }
        let ctx = SessionContext::new_with_config(config);
        ctx.register_object_store(&scheme_url(mode), store);

        let options =
            ListingOptions::new(Arc::new(ParquetFormat::default())).with_file_extension(".parquet");
        let schema = options
            .infer_schema(&ctx.state(), &urls[0])
            .await
            .context("inferring the artifact schema")?;
        let cfg = ListingTableConfig::new_with_multi_paths(urls)
            .with_listing_options(options)
            .with_schema(schema);
        ctx.register_table("events_physical", Arc::new(ListingTable::try_new(cfg)?))?;

        // The logical view every query runs against.
        ctx.sql(view_sql)
            .await
            .context("planning the logical view")?
            .collect()
            .await
            .context("creating the logical view")?;

        Ok(Session { ctx, counters })
    }
}

/// The outcome of running one query once.
pub struct QueryRun {
    pub batches: Vec<RecordBatch>,
    pub schema: Arc<arrow::datatypes::Schema>,
    pub elapsed_ms: f64,
    pub io: Option<CounterSnapshot>,
}

/// Run one query against a session, timing it and (when instrumented) attributing I/O.
pub async fn run_query(session: &Session, sql: &str) -> Result<QueryRun> {
    let before = session.counters.as_ref().map(|c| c.snapshot());
    let start = Instant::now();
    let df = session
        .ctx
        .sql(sql)
        .await
        .with_context(|| format!("planning: {sql}"))?;
    let schema = Arc::new(df.schema().as_arrow().clone());
    let batches = df
        .collect()
        .await
        .with_context(|| format!("executing: {sql}"))?;
    let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
    let io = match (&session.counters, before) {
        (Some(c), Some(before)) => Some(CounterSnapshot::delta(&c.snapshot(), &before)),
        _ => None,
    };
    Ok(QueryRun {
        batches,
        schema,
        elapsed_ms,
        io,
    })
}

/// The formatted physical plan for a query, with metrics — the plan is executed once so
/// the displayed tree carries row counts, elapsed compute, and rows pruned per node.
pub async fn physical_plan(session: &Session, sql: &str) -> Result<String> {
    let df = session.ctx.sql(sql).await?;
    let plan = df.create_physical_plan().await?;
    // Execute so metrics populate, then display with metrics shown.
    let task_ctx = session.ctx.task_ctx();
    let _ = datafusion::physical_plan::collect(plan.clone(), task_ctx)
        .await
        .with_context(|| format!("executing plan for metrics: {sql}"))?;
    Ok(format!("{}", displayable(plan.as_ref()).indent(true)))
}

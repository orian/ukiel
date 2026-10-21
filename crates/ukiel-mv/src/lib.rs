//! One source ADD -> SQL -> one identified target ADD. Compaction REPLACE and
//! DELETE events advance progress without reapplying original append intent.
use arrow::{
    array::{RecordBatch, new_null_array},
    compute::cast,
};
use futures::StreamExt;
use object_store::{ObjectStore, path::Path};
use parquet::arrow::async_reader::{ParquetObjectReader, ParquetRecordBatchStreamBuilder};
use std::{collections::HashSet, error::Error, sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;
use ukiel_catalog::{CatalogError, OperationLookup, PostgresCatalog};
use ukiel_core::{
    ChangeEvent, ColumnKind, CommitId, CommitResult, Endpoint, ErrorPolicy, Hypertable, MvIntent,
    OperationIdentity, Part, PartId, Pipeline, ReadySignal, TableColumns,
};
use ukiel_pipeline::{
    ExecutionLimits, PipelineError, PreparedPipeline, execute_pipeline, prepare_pipeline,
};
use ukiel_write::{AddCommit, L0Writer, WriteError, batches_to_l0_items, validate_output_schema};

type BoxError = Box<dyn Error + Send + Sync>;

pub const MV_TRANSFORMATION_VERSION: u32 = 1;

#[derive(Debug, thiserror::Error)]
pub enum MvError {
    #[error(transparent)]
    Catalog(#[from] CatalogError),
    #[error(transparent)]
    Pipeline(#[from] PipelineError),
    #[error(transparent)]
    Write(#[from] WriteError),
    #[error(transparent)]
    Schema(#[from] ukiel_core::SchemaError),
    #[error(transparent)]
    Expr(#[from] ukiel_expr::ExprError),
    #[error(transparent)]
    Arrow(#[from] arrow::error::ArrowError),
    #[error(transparent)]
    Operation(#[from] ukiel_core::OperationError),
    #[error("invalid MV configuration: {0}")]
    Config(String),
    #[error("source event exceeds its {0}-byte input budget")]
    InputBudget(usize),
    #[error("reading source commit {commit}, part {part}, path {path}: {source}")]
    SourceRead {
        commit: CommitId,
        part: PartId,
        path: String,
        #[source]
        source: BoxError,
    },
    #[error(
        "required source input unavailable: commit {commit}, part {part}, path {path}: {source}"
    )]
    SourceInputUnavailable {
        commit: CommitId,
        part: PartId,
        path: String,
        #[source]
        source: BoxError,
    },
}

impl MvError {
    pub fn catalog_error(&self) -> Option<&CatalogError> {
        match self {
            Self::Catalog(e) | Self::Write(WriteError::Catalog(e)) => Some(e),
            _ => None,
        }
    }
    fn is_data_error(&self) -> bool {
        match self {
            Self::Pipeline(e) => e.is_runtime_data_error(),
            Self::Write(
                WriteError::Arrow(_) | WriteError::Expr(_) | WriteError::InvalidTarget(_),
            ) => true,
            _ => false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct MvConfig {
    pub pipelines: Vec<String>,
    pub poll_interval_ms: u64,
    pub limits: ExecutionLimits,
    pub ready: Option<ReadySignal>,
}

impl Default for MvConfig {
    fn default() -> Self {
        Self {
            pipelines: vec![],
            poll_interval_ms: 1_000,
            limits: ExecutionLimits::default(),
            ready: None,
        }
    }
}

struct Selected {
    pipeline: Pipeline,
    source: Hypertable,
    target: Hypertable,
    prepared: PreparedPipeline,
}

pub struct MvFleet {
    catalog: PostgresCatalog,
    store: Arc<dyn ObjectStore>,
    writer: L0Writer,
    selected: Vec<Selected>,
    config: MvConfig,
}

impl MvFleet {
    /// Resolve only explicit names, validate all selections before registering
    /// any cursor, then register each first cursor under the source feed barrier.
    pub async fn new(
        catalog: PostgresCatalog,
        store: Arc<dyn ObjectStore>,
        config: MvConfig,
    ) -> Result<Self, MvError> {
        if config.pipelines.is_empty() || config.poll_interval_ms == 0 {
            return Err(MvError::Config(
                "a non-empty pipeline list and positive poll interval are required".into(),
            ));
        }
        let mut names = HashSet::new();
        let mut edges = HashSet::new();
        let mut selected = Vec::new();
        for name in &config.pipelines {
            if !names.insert(name.clone()) {
                return Err(MvError::Config(format!(
                    "duplicate selected pipeline {name}"
                )));
            }
            let pipeline = catalog.get_pipeline(name).await?;
            let Endpoint::Parquet(source_id) = pipeline.definition.source else {
                return Err(MvError::Config(format!(
                    "pipeline {name} is not Parquet-sourced"
                )));
            };
            if !edges.insert((source_id, pipeline.definition.target)) {
                return Err(MvError::Config(
                    "duplicate selected source/target pair".into(),
                ));
            }
            let source = catalog.get_hypertable_by_id(source_id).await?;
            let target = catalog
                .get_hypertable_by_id(pipeline.definition.target)
                .await?;
            ukiel_expr::validate_table_schema(&source.table_schema)?;
            ukiel_expr::validate_table_schema(&target.table_schema)?;
            let input = Arc::new(TableColumns::parse(&source.table_schema)?.query_schema());
            let prepared =
                prepare_pipeline(&source.name, &pipeline.definition.sql, input, config.limits)
                    .await?;
            validate_output_schema(prepared.output_schema().as_ref(), &target)?;
            if let Some(column) = &pipeline.definition.event_time_column {
                prepared.output_schema().index_of(column)?;
            }
            selected.push(Selected {
                pipeline,
                source,
                target,
                prepared,
            });
        }
        // API creation rejects all catalog cycles. Also guard selected definitions
        // against direct catalog corruption before readiness.
        for selection in &selected {
            let mut pending = vec![selection.target.id];
            let mut visited = HashSet::new();
            while let Some(node) = pending.pop() {
                if node == selection.source.id {
                    return Err(MvError::Config(
                        "selected pipeline graph contains a cycle".into(),
                    ));
                }
                if visited.insert(node) {
                    pending.extend(
                        selected
                            .iter()
                            .filter(|s| s.source.id == node)
                            .map(|s| s.target.id),
                    );
                }
            }
        }
        for selection in &selected {
            catalog.ensure_mv_cursor(selection.pipeline.id).await?;
        }
        Ok(Self {
            writer: L0Writer::new(catalog.clone(), store.clone()),
            catalog,
            store,
            selected,
            config,
        })
    }

    /// Processes at most one event per selected pipeline, loading fresh durable
    /// progress each time. No cached event page or per-replica cursor survives.
    pub async fn process_once(&self) -> Result<usize, MvError> {
        let mut processed = 0;
        let mut stalled = None;
        for selected in &self.selected {
            let cursor = self.catalog.read_mv_cursor(selected.pipeline.id).await?;
            let events = self
                .catalog
                .changes_since(selected.source.id, cursor, 1)
                .await?;
            if let Some(event) = events.into_iter().next() {
                match self.process_event(selected, event).await {
                    Ok(()) => processed += 1,
                    // A data error holds this pipeline's cursor, while other
                    // selected pipelines can still make progress this pass.
                    Err(error) if error.is_data_error() => {
                        stalled.get_or_insert(error);
                    }
                    Err(error) => return Err(error),
                }
            }
        }
        match stalled {
            Some(error) => Err(error),
            None => Ok(processed),
        }
    }

    pub async fn run(self, shutdown: CancellationToken) -> Result<(), MvError> {
        ukiel_core::signal_ready(&self.config.ready);
        loop {
            if shutdown.is_cancelled() {
                return Ok(());
            }
            // Drain the current attempt on normal shutdown, rather than detach
            // object work or abandon a publication while reporting a clean stop.
            match self.process_once().await {
                Ok(processed) if processed > 0 => continue,
                Ok(_) => {}
                Err(error) if error.is_data_error() => {
                    metrics::counter!("mv_errors_total", "phase" => "transform", "policy" => "stall").increment(1);
                    tracing::error!(%error, "MV source event stalled without advancing its cursor");
                }
                Err(error) => return Err(error),
            }
            tokio::select! {
                _ = shutdown.cancelled() => return Ok(()),
                _ = tokio::time::sleep(Duration::from_millis(self.config.poll_interval_ms)) => {},
            }
        }
    }

    async fn advance(&self, selected: &Selected, commit: CommitId) -> Result<(), MvError> {
        self.catalog
            .set_cursor(
                &format!("mv/{}", selected.pipeline.id),
                selected.source.id,
                commit,
            )
            .await?;
        Ok(())
    }

    async fn completed(
        &self,
        selected: &Selected,
        event: CommitId,
        identity: &OperationIdentity,
        phase: &'static str,
    ) -> Result<bool, MvError> {
        let proof = if self.catalog.read_mv_cursor(selected.pipeline.id).await? >= event {
            Some("cursor")
        } else if matches!(
            self.catalog.lookup_operation(identity).await?,
            OperationLookup::Committed(_)
        ) {
            self.advance(selected, event).await?;
            Some("operation")
        } else {
            None
        };
        if let Some(proof) = proof {
            metrics::counter!("mv_superseded_attempts_total", "phase" => phase, "proof" => proof)
                .increment(1);
            return Ok(true);
        }
        Ok(false)
    }

    async fn process_event(&self, selected: &Selected, event: ChangeEvent) -> Result<(), MvError> {
        if matches!(event.kind.as_str(), "replace" | "delete") {
            return self.advance(selected, event.commit_id).await;
        }
        if event.kind != "add" {
            return Err(MvError::Config(format!("unknown feed kind {}", event.kind)));
        }
        let identity = OperationIdentity::mv(
            selected.target.id,
            MvIntent {
                pipeline_id: selected.pipeline.id,
                source_hypertable_id: selected.source.id,
                source_commit_id: event.commit_id,
                transformation_version: MV_TRANSFORMATION_VERSION,
            },
        )?;
        if self
            .completed(selected, event.commit_id, &identity, "before_read")
            .await?
        {
            return Ok(());
        }
        let started = std::time::Instant::now();
        let input = match read_input(
            &self.store,
            &selected.source,
            &event,
            self.config.limits.input_bytes,
        )
        .await
        {
            Ok(input) => input,
            Err(error) => {
                // All owned source streams/partial batches have been dropped by
                // read_input before asking authority; no detached reads survive.
                if self
                    .completed(selected, event.commit_id, &identity, "source_read")
                    .await?
                {
                    return Ok(());
                }
                return Err(required_source_error(error));
            }
        };
        let input_rows = input.iter().map(RecordBatch::num_rows).sum::<usize>();
        metrics::histogram!("mv_attempt_input_rows").record(input_rows as f64);
        metrics::histogram!("mv_attempt_input_bytes").record(
            input
                .iter()
                .map(RecordBatch::get_array_memory_size)
                .sum::<usize>() as f64,
        );
        // Empty ADDs are completion markers. Never manufacture count(*)=0 rows
        // by executing downstream SQL over a marker with no source parts.
        let result = if event.added.is_empty() {
            Ok(vec![])
        } else {
            execute_pipeline(&selected.prepared, input)
                .await
                .map_err(MvError::from)
        };
        let encoded = match result {
            Ok(output) => {
                metrics::histogram!("mv_attempt_output_rows")
                    .record(output.iter().map(RecordBatch::num_rows).sum::<usize>() as f64);
                metrics::histogram!("mv_attempt_output_bytes").record(
                    output
                        .iter()
                        .map(RecordBatch::get_array_memory_size)
                        .sum::<usize>() as f64,
                );
                batches_to_l0_items(&output, &selected.target, self.config.limits.output_bytes)
                    .map_err(MvError::from)
            }
            Err(error) => Err(error),
        };
        let (items, skipped) = match encoded {
            Ok(items) => (items, false),
            Err(error) => {
                if self
                    .completed(selected, event.commit_id, &identity, "transform")
                    .await?
                {
                    return Ok(());
                }
                if selected.pipeline.definition.error_policy == ErrorPolicy::Skip
                    && error.is_data_error()
                {
                    tracing::warn!(pipeline=%selected.pipeline.definition.name, %error, "MV policy publishes an identified empty skip");
                    (vec![], true)
                } else {
                    return Err(error);
                }
            }
        };
        if self
            .completed(selected, event.commit_id, &identity, "before_publish")
            .await?
        {
            return Ok(());
        }
        let outcome = self
            .writer
            .publish_add(
                &selected.target,
                items,
                AddCommit::Plain {
                    identity: &identity,
                },
            )
            .await?;
        match outcome {
            CommitResult::Committed(_) => {
                metrics::counter!("mv_events_committed_total").increment(1);
                if skipped {
                    metrics::counter!("pipeline_batches_skipped_total", "role" => "mv")
                        .increment(1);
                }
            }
            CommitResult::AlreadyApplied(_) => {
                metrics::counter!("mv_already_applied_total").increment(1);
            }
        }
        self.advance(selected, event.commit_id).await?;
        metrics::histogram!("mv_event_duration_seconds").record(started.elapsed().as_secs_f64());
        Ok(())
    }
}

fn source_error(
    event: CommitId,
    part: &Part,
    error: impl Error + Send + Sync + 'static,
) -> MvError {
    MvError::SourceRead {
        commit: event,
        part: part.id,
        path: part.meta.path.clone(),
        source: Box::new(error),
    }
}

async fn read_input(
    store: &Arc<dyn ObjectStore>,
    source: &Hypertable,
    event: &ChangeEvent,
    limit: usize,
) -> Result<Vec<RecordBatch>, MvError> {
    let cols = TableColumns::parse(&source.table_schema)?;
    let physical = Arc::new(cols.physical_schema());
    let query = Arc::new(cols.query_schema());
    let mut output = Vec::new();
    let mut bytes = 0usize;
    for part in &event.added {
        let reader = ParquetObjectReader::new(store.clone(), Path::from(part.meta.path.clone()));
        let builder = ParquetRecordBatchStreamBuilder::new(reader)
            .await
            .map_err(|e| source_error(event.commit_id, part, e))?;
        let mut stream = builder
            .with_batch_size(8192)
            .build()
            .map_err(|e| source_error(event.commit_id, part, e))?;
        while let Some(batch) = stream.next().await {
            let batch = batch.map_err(|e| source_error(event.commit_id, part, e))?;
            let arrays = physical
                .fields()
                .iter()
                .map(|field| match batch.schema().index_of(field.name()) {
                    Ok(i) => cast(batch.column(i), field.data_type()),
                    Err(_) => Ok(new_null_array(field.data_type(), batch.num_rows())),
                })
                .collect::<Result<Vec<_>, arrow::error::ArrowError>>()
                .map_err(|e| source_error(event.commit_id, part, e))?;
            let stored = RecordBatch::try_new(physical.clone(), arrays)
                .map_err(|e| source_error(event.commit_id, part, e))?;
            let arrays = cols
                .specs
                .iter()
                .map(|spec| {
                    if let ColumnKind::Alias(sql) = &spec.kind {
                        let field = query.field_with_name(&spec.name)?;
                        let expr = ukiel_expr::compile_for_field(sql, &physical, field)?;
                        ukiel_expr::evaluate(&expr, &stored)
                    } else {
                        Ok(stored.column(stored.schema().index_of(&spec.name)?).clone())
                    }
                })
                .collect::<Result<Vec<_>, ukiel_expr::ExprError>>()
                .map_err(|e| source_error(event.commit_id, part, e))?;
            let adapted = RecordBatch::try_new(query.clone(), arrays)
                .map_err(|e| source_error(event.commit_id, part, e))?;
            bytes = bytes.saturating_add(adapted.get_array_memory_size());
            if bytes > limit {
                return Err(MvError::InputBudget(limit));
            }
            output.push(adapted);
        }
    }
    Ok(output)
}

fn required_source_error(error: MvError) -> MvError {
    let MvError::SourceRead {
        commit,
        part,
        path,
        source,
    } = error
    else {
        return error;
    };
    let mut cause: Option<&(dyn Error + 'static)> = Some(source.as_ref());
    while let Some(current) = cause {
        if matches!(
            current.downcast_ref::<object_store::Error>(),
            Some(object_store::Error::NotFound { .. })
        ) {
            return MvError::SourceInputUnavailable {
                commit,
                part,
                path,
                source,
            };
        }
        cause = current.source();
    }
    MvError::SourceRead {
        commit,
        part,
        path,
        source,
    }
}

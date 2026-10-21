//! SQL over one logical Arrow input. Publication and deduplication belong to
//! the caller: fresh attempts may compute different results, including now().

use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{DataFusionError, TableReference};
use datafusion::datasource::MemTable;
use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::{SessionConfig, SessionContext};
use futures::TryStreamExt;

#[derive(Debug, thiserror::Error)]
pub enum PipelineError {
    #[error(transparent)]
    DataFusion(#[from] DataFusionError),
    #[error("pipeline requires one SELECT over exactly one scan of its declared source")]
    InvalidQuery,
    #[error("pipeline input batch does not match its prepared schema")]
    InputSchemaMismatch,
    #[error("pipeline {resource} exceeds its limit of {limit}")]
    BudgetExceeded {
        resource: &'static str,
        limit: usize,
    },
    #[error("pipeline execution limits must all be positive")]
    InvalidLimits,
}

impl PipelineError {
    /// Only execution/data errors participate in a pipeline's stall/skip policy.
    /// Resource limits and malformed contracts never silently skip source work.
    pub fn is_runtime_data_error(&self) -> bool {
        matches!(self, Self::DataFusion(error) if matches!(error.find_root(), DataFusionError::Execution(_) | DataFusionError::ArrowError(..)))
    }
}

/// Limits apply to one SQL execution, not separately to each input batch.
/// DataFusion uses a limited operator memory pool and spilling is disabled.
/// Dropping the execution future drops its owned DataFusion stream.
#[derive(Debug, Clone, Copy)]
pub struct ExecutionLimits {
    pub input_bytes: usize,
    pub output_bytes: usize,
    pub output_rows: usize,
    pub operator_memory_bytes: usize,
}

impl Default for ExecutionLimits {
    fn default() -> Self {
        Self {
            input_bytes: 64 * 1024 * 1024,
            output_bytes: 64 * 1024 * 1024,
            output_rows: 1_000_000,
            operator_memory_bytes: 128 * 1024 * 1024,
        }
    }
}

/// Stores SQL and schemas, never a physical plan or a clock captured at prepare
/// time. Each execution registers its own input and plans with fresh context.
#[derive(Debug, Clone)]
pub struct PreparedPipeline {
    source_name: String,
    sql: String,
    input_schema: SchemaRef,
    output_schema: SchemaRef,
    limits: ExecutionLimits,
}

impl PreparedPipeline {
    pub fn output_schema(&self) -> &SchemaRef {
        &self.output_schema
    }
}

pub async fn prepare_pipeline(
    source_name: &str,
    sql: &str,
    input_schema: SchemaRef,
    limits: ExecutionLimits,
) -> Result<PreparedPipeline, PipelineError> {
    let ctx = context(source_name, input_schema.clone(), vec![], limits)?;
    let plan = ctx.state().create_logical_plan(sql).await?;
    validate_plan(&plan, source_name)?;
    // Validate physical support before a caller installs a consumer GC fence.
    // Discard this plan so now() is still evaluated afresh on actual execution.
    ctx.state().create_physical_plan(&plan).await?;
    let output_schema = Arc::new(plan.schema().as_arrow().clone());
    Ok(PreparedPipeline {
        source_name: source_name.into(),
        sql: sql.into(),
        input_schema,
        output_schema,
        limits,
    })
}

/// Run SQL once across all batches from one logical source event. Keeping
/// batches separate avoids concatenating an entire commit into one allocation;
/// aggregates and HAVING still see every row in that event.
pub async fn execute_pipeline(
    prepared: &PreparedPipeline,
    input: Vec<RecordBatch>,
) -> Result<Vec<RecordBatch>, PipelineError> {
    let mut bytes = 0usize;
    for batch in &input {
        if batch.schema() != prepared.input_schema {
            return Err(PipelineError::InputSchemaMismatch);
        }
        bytes = bytes.saturating_add(batch.get_array_memory_size());
        check_budget("input bytes", bytes, prepared.limits.input_bytes)?;
    }
    let ctx = context(
        &prepared.source_name,
        prepared.input_schema.clone(),
        input,
        prepared.limits,
    )?;
    let plan = ctx.state().create_logical_plan(&prepared.sql).await?;
    validate_plan(&plan, &prepared.source_name)?;
    let frame = ctx.execute_logical_plan(plan).await?;
    let mut stream = frame.execute_stream().await?;
    let mut output = Vec::new();
    let mut output_bytes = 0usize;
    let mut output_rows = 0usize;
    while let Some(batch) = stream.try_next().await? {
        output_bytes = output_bytes.saturating_add(batch.get_array_memory_size());
        output_rows = output_rows.saturating_add(batch.num_rows());
        check_budget("output bytes", output_bytes, prepared.limits.output_bytes)?;
        check_budget("output rows", output_rows, prepared.limits.output_rows)?;
        output.push(batch);
    }
    Ok(output)
}

fn check_budget(resource: &'static str, value: usize, limit: usize) -> Result<(), PipelineError> {
    if value > limit {
        return Err(PipelineError::BudgetExceeded { resource, limit });
    }
    Ok(())
}

fn context(
    source_name: &str,
    schema: SchemaRef,
    input: Vec<RecordBatch>,
    limits: ExecutionLimits,
) -> Result<SessionContext, PipelineError> {
    if [
        limits.input_bytes,
        limits.output_bytes,
        limits.output_rows,
        limits.operator_memory_bytes,
    ]
    .contains(&0)
    {
        return Err(PipelineError::InvalidLimits);
    }
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_limit(limits.operator_memory_bytes, 1.0)
        .with_disk_manager_builder(
            DiskManagerBuilder::default().with_mode(DiskManagerMode::Disabled),
        )
        .build_arc()?;
    let ctx = SessionContext::new_with_config_rt(SessionConfig::new(), runtime);
    let table = MemTable::try_new(schema, vec![input])?;
    ctx.register_table(
        TableReference::bare(source_name.to_owned()),
        Arc::new(table),
    )?;
    Ok(ctx)
}

fn validate_plan(plan: &LogicalPlan, source_name: &str) -> Result<(), PipelineError> {
    let mut scans = 0usize;
    let mut invalid = false;
    // Include scalar subqueries: traversing only plan inputs would miss extra
    // source scans hidden inside expressions.
    plan.apply_with_subqueries(|node| {
        match node {
            LogicalPlan::TableScan(scan) => {
                scans += 1;
                invalid |= scan.table_name.table() != source_name;
            }
            LogicalPlan::Join(_)
            | LogicalPlan::Ddl(_)
            | LogicalPlan::Dml(_)
            | LogicalPlan::Statement(_)
            | LogicalPlan::Copy(_)
            | LogicalPlan::DescribeTable(_)
            | LogicalPlan::Explain(_)
            | LogicalPlan::Analyze(_)
            | LogicalPlan::Extension(_)
            | LogicalPlan::RecursiveQuery(_) => invalid = true,
            _ => {}
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    if invalid || scans != 1 {
        return Err(PipelineError::InvalidQuery);
    }
    Ok(())
}

//! Shared Arrow-to-L0 encoding and identified publication. Source ownership,
//! SQL execution, recovery supervision, and role metrics belong to callers.
//! Expressions use ukiel-expr (and its DataFusion expression dependency).
use arrow::{
    array::{
        Array, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray, UInt32Array,
        new_null_array,
    },
    compute::{cast, concat_batches, take_record_batch},
    datatypes::{DataType, Schema, TimeUnit},
};
use object_store::{ObjectStore, ObjectStoreExt, path::Path};
use parquet::arrow::ArrowWriter;
use serde_json::{Map, Value};
use std::{collections::BTreeMap, sync::Arc};
use ukiel_catalog::{CatalogError, OffsetRange, PostgresCatalog};
use ukiel_core::{
    ColumnKind, CommitOp, CommitResult, Hypertable, OperationIdentity, PartMeta, TableColumns,
    WriteOpts,
};

#[derive(Debug, thiserror::Error)]
pub enum WriteError {
    #[error(transparent)]
    Catalog(#[from] CatalogError),
    #[error(transparent)]
    ObjectStore(#[from] object_store::Error),
    #[error(transparent)]
    Schema(#[from] ukiel_core::SchemaError),
    #[error(transparent)]
    Arrow(#[from] arrow::error::ArrowError),
    #[error(transparent)]
    Parquet(#[from] parquet::errors::ParquetError),
    #[error(transparent)]
    Expr(#[from] ukiel_expr::ExprError),
    #[error(transparent)]
    SortKey(#[from] ukiel_core::SortKeyError),
    #[error("invalid target output: {0}")]
    InvalidTarget(String),
    #[error("write input exceeds its limit of {0} bytes")]
    BudgetExceeded(usize),
    #[error("cannot encode an empty batch")]
    EmptyBatch,
}

#[derive(Debug)]
pub struct EncodedPart {
    pub bytes: Vec<u8>,
    pub row_count: i64,
    pub key_min: i64,
    pub key_max: i64,
    pub column_stats: Option<Value>,
}

pub struct FlushItem {
    pub partition_values: Value,
    pub part: EncodedPart,
}

pub enum AddCommit<'a> {
    Ingest {
        offsets: &'a [OffsetRange],
        identity: &'a OperationIdentity,
    },
    Plain {
        identity: &'a OperationIdentity,
    },
}

pub struct L0Writer {
    catalog: PostgresCatalog,
    store: Arc<dyn ObjectStore>,
}

impl L0Writer {
    pub fn new(catalog: PostgresCatalog, store: Arc<dyn ObjectStore>) -> Self {
        Self { catalog, store }
    }

    /// Empty items still publish the same identified operation, without an
    /// object upload. A late attempt with rows must lose to this empty outcome.
    pub async fn publish_add(
        &self,
        target: &Hypertable,
        items: Vec<FlushItem>,
        commit: AddCommit<'_>,
    ) -> Result<CommitResult, WriteError> {
        let identity = match &commit {
            AddCommit::Ingest { identity, .. } | AddCommit::Plain { identity } => *identity,
        };
        if identity.hypertable_id() != target.id {
            return Err(CatalogError::OperationHypertableMismatch {
                identity: identity.hypertable_id().0,
                mutation: target.id.0,
            }
            .into());
        }
        let prepared: Vec<_> = items
            .into_iter()
            .map(|item| {
                (
                    format!("ht/{}/L0/{}.parquet", target.id, uuid::Uuid::new_v4()),
                    item,
                )
            })
            .collect();
        let paths = prepared
            .iter()
            .map(|(path, _)| path.clone())
            .collect::<Vec<_>>();
        self.catalog
            .register_pending_objects(target.id, &paths)
            .await?;
        let mut parts = Vec::with_capacity(prepared.len());
        for (path, item) in prepared {
            let size_bytes = item.part.bytes.len() as i64;
            self.store
                .put(&Path::from(path.clone()), item.part.bytes.into())
                .await?;
            parts.push(PartMeta {
                path,
                partition_values: item.partition_values,
                packing_key_min: item.part.key_min,
                packing_key_max: item.part.key_max,
                row_count: item.part.row_count,
                size_bytes,
                level: 0,
                column_stats: item.part.column_stats,
            });
        }
        let op = CommitOp::Add { parts };
        Ok(match commit {
            AddCommit::Ingest { offsets, identity } => {
                self.catalog
                    .commit_with_offsets(target.id, op, offsets, identity)
                    .await?
            }
            AddCommit::Plain { identity } => {
                self.catalog.commit(target.id, op, Some(identity)).await?
            }
        })
    }
}

fn partition_columns(target: &Hypertable) -> Result<Vec<String>, WriteError> {
    let Some(columns) = target
        .partition_spec
        .get("columns")
        .and_then(Value::as_array)
    else {
        return Err(WriteError::InvalidTarget(
            "partition spec must contain a columns array".into(),
        ));
    };
    columns
        .iter()
        .map(|c| {
            c.as_str()
                .map(str::to_owned)
                .ok_or_else(|| WriteError::InvalidTarget("partition columns must be names".into()))
        })
        .collect()
}

/// Validate before registration/readiness, rather than first discovering a
/// missing target key after this pipeline has acquired a GC fence.
pub fn validate_output_schema(output: &Schema, target: &Hypertable) -> Result<(), WriteError> {
    let cols = TableColumns::parse(&target.table_schema)?;
    let partitions = partition_columns(target)?;
    let required: Vec<&str> = target
        .sort_key
        .iter()
        .map(String::as_str)
        .chain(partitions.iter().map(String::as_str))
        .chain(std::iter::once(target.packing_key.as_str()))
        .collect();
    for field in output.fields() {
        let Some(spec) = cols.specs.iter().find(|c| &c.name == field.name()) else {
            return Err(WriteError::InvalidTarget(format!(
                "unknown output column {}",
                field.name()
            )));
        };
        if matches!(
            spec.kind,
            ColumnKind::Alias(_) | ColumnKind::Materialized(_)
        ) {
            return Err(WriteError::InvalidTarget(format!(
                "column {} is not writable",
                spec.name
            )));
        }
        if !arrow::compute::can_cast_types(field.data_type(), &spec.data_type) {
            return Err(WriteError::InvalidTarget(format!(
                "column {} cannot be cast to {:?}",
                spec.name, spec.data_type
            )));
        }
    }
    for spec in &cols.specs {
        if matches!(spec.kind, ColumnKind::Stored)
            && (!spec.nullable || required.contains(&spec.name.as_str()))
            && output.index_of(&spec.name).is_err()
        {
            return Err(WriteError::InvalidTarget(format!(
                "missing required column {}",
                spec.name
            )));
        }
    }
    let physical = cols.physical_schema();
    ukiel_core::validate_sort_key(&physical, &target.sort_key, &target.packing_key, None)?;
    for name in required {
        physical.index_of(name)?;
    }
    Ok(())
}

pub fn batches_to_l0_items(
    batches: &[RecordBatch],
    target: &Hypertable,
    max_bytes: usize,
) -> Result<Vec<FlushItem>, WriteError> {
    let cols = TableColumns::parse(&target.table_schema)?;
    let schema = Arc::new(cols.physical_schema());
    // Defaults/materialized values can fill a missing non-null field. Permit
    // temporary NULLs while evaluating them, then enforce the target contract.
    let evaluation_schema = Arc::new(Schema::new_with_metadata(
        schema
            .fields()
            .iter()
            .map(|field| field.as_ref().clone().with_nullable(true))
            .collect::<Vec<_>>(),
        schema.metadata().clone(),
    ));
    let partitions = partition_columns(target)?;
    let mut adapted = Vec::new();
    let mut used = 0usize;
    let mut materialized_bytes = 0usize;
    for batch in batches {
        validate_output_schema(batch.schema().as_ref(), target)?;
        used = used.saturating_add(batch.get_array_memory_size());
        if used > max_bytes {
            return Err(WriteError::BudgetExceeded(max_bytes));
        }
        if batch.num_rows() == 0 {
            continue;
        }
        let arrays = cols
            .specs
            .iter()
            .filter(|c| !matches!(c.kind, ColumnKind::Alias(_)))
            .map(|spec| {
                match batch.schema().index_of(&spec.name) {
                    Ok(i) => {
                        let array = batch.column(i);
                        // Ukiel timestamp_ms is physically Int64. SQL timestamp
                        // output must first change units, rather than cast ns to i64.
                        if spec.ukiel_type == "timestamp_ms"
                            && matches!(array.data_type(), DataType::Timestamp(_, _))
                        {
                            let millis =
                                cast(array, &DataType::Timestamp(TimeUnit::Millisecond, None))?;
                            cast(&millis, &DataType::Int64)
                        } else {
                            cast(array, &spec.data_type)
                        }
                    }
                    Err(_) => Ok(new_null_array(&spec.data_type, batch.num_rows())),
                }
            })
            .collect::<Result<Vec<_>, arrow::error::ArrowError>>()?;
        let filled = ukiel_expr::apply_defaults_and_materialized(
            RecordBatch::try_new(evaluation_schema.clone(), arrays)?,
            &cols,
        )?;
        materialized_bytes = materialized_bytes.saturating_add(filled.get_array_memory_size());
        if materialized_bytes > max_bytes {
            return Err(WriteError::BudgetExceeded(max_bytes));
        }
        for (i, field) in schema.fields().iter().enumerate() {
            if !field.is_nullable() && filled.column(i).null_count() > 0 {
                return Err(WriteError::InvalidTarget(format!(
                    "NULL in non-null column {}",
                    field.name()
                )));
            }
        }
        let key = filled.column(schema.index_of(&target.packing_key)?);
        if key.null_count() > 0 {
            return Err(WriteError::InvalidTarget(
                "packing key contains NULL".into(),
            ));
        }
        adapted.push(RecordBatch::try_new(
            schema.clone(),
            filled.columns().to_vec(),
        )?);
    }
    if adapted.is_empty() {
        return Ok(vec![]);
    }
    let batch = concat_batches(&schema, &adapted)?;
    if batch.num_rows() > u32::MAX as usize {
        return Err(WriteError::InvalidTarget(
            "too many output rows for one write".into(),
        ));
    }
    let mut groups: BTreeMap<String, (Value, Vec<u32>)> = BTreeMap::new();
    for row in 0..batch.num_rows() {
        let mut values = Map::new();
        for name in &partitions {
            let array = batch.column(schema.index_of(name)?);
            let value = if array.is_null(row) {
                Value::Null
            } else {
                match array.data_type() {
                    DataType::Int64 => Value::from(
                        array
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .unwrap()
                            .value(row),
                    ),
                    DataType::Float64 => Value::Number(
                        serde_json::Number::from_f64(
                            array
                                .as_any()
                                .downcast_ref::<Float64Array>()
                                .unwrap()
                                .value(row),
                        )
                        .ok_or_else(|| {
                            WriteError::InvalidTarget(format!(
                                "non-finite partition value in {name}"
                            ))
                        })?,
                    ),
                    DataType::Boolean => Value::from(
                        array
                            .as_any()
                            .downcast_ref::<BooleanArray>()
                            .unwrap()
                            .value(row),
                    ),
                    DataType::Utf8 => Value::from(
                        array
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .unwrap()
                            .value(row),
                    ),
                    _ => {
                        return Err(WriteError::InvalidTarget(format!(
                            "unsupported partition column {name}"
                        )));
                    }
                }
            };
            values.insert(name.clone(), value);
        }
        let value = Value::Object(values);
        let key = value.to_string();
        groups
            .entry(key)
            .or_insert_with(|| (value, Vec::new()))
            .1
            .push(row as u32);
    }
    groups
        .into_values()
        .map(|(partition_values, indices)| {
            let selected = take_record_batch(&batch, &UInt32Array::from(indices))?;
            let sorted = ukiel_core::sort_batch(&selected, &target.sort_key)?;
            let opts = WriteOpts::from_columns(&cols, &target.sort_key, 0);
            let part = encode_batch(sorted, &target.packing_key, &target.sort_key, opts)?;
            Ok(FlushItem {
                partition_values,
                part,
            })
        })
        .collect()
}

/// Encode an already-sorted batch using the same L0 properties and statistics
/// as ingest. Keeping this shared prevents MV files drifting from core layout.
pub fn encode_batch(
    batch: RecordBatch,
    packing_key: &str,
    sort_key: &[String],
    opts: WriteOpts,
) -> Result<EncodedPart, WriteError> {
    let keys = batch
        .column(batch.schema().index_of(packing_key)?)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| WriteError::InvalidTarget("packing key must be Int64".into()))?;
    if keys.null_count() > 0 {
        return Err(WriteError::InvalidTarget(
            "packing key contains NULL".into(),
        ));
    }
    let key_min = arrow::compute::min(keys).ok_or(WriteError::EmptyBatch)?;
    let key_max = arrow::compute::max(keys).ok_or(WriteError::EmptyBatch)?;
    let column_stats = ukiel_core::stats::with_key_index(
        ukiel_core::stats::int64_column_stats(&batch),
        (key_min != key_max)
            .then(|| ukiel_core::stats::key_bitmap(&batch, packing_key))
            .flatten(),
        None,
    );
    let props = ukiel_core::writer_props(&batch.schema(), sort_key, &opts);
    let mut bytes = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut bytes, batch.schema(), Some(props))?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(EncodedPart {
        bytes,
        row_count: batch.num_rows() as i64,
        key_min,
        key_max,
        column_stats,
    })
}

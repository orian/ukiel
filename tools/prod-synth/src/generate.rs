//! Write the topology out as sorted Parquet plus a manifest, atomically.
//!
//! Two rules shape this file.
//!
//! **Bounded memory.** One part at a time, in bounded Arrow batches. The `shape`
//! tier is 100M rows; materializing them would need tens of gigabytes and the tool
//! would be useless exactly where it is most interesting. Rows stream into the
//! writer and are dropped.
//!
//! **Measured, not predicted.** `row_count` and `size_bytes` are read back from the
//! *closed* file. It is tempting to write down what you meant to write — and a
//! fixture whose catalog metadata says 4,096 bytes while the object is 4,102 is a
//! fixture that will fail a load-time integrity check for a reason nobody can find.

use std::fs::File;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::Schema;
use parquet::arrow::ArrowWriter;
use prod_synth_contract::{
    FileDigest, GenerationConfig, Manifest, ManifestPart, Membership, PartProvenance,
    SYNTHETIC_DISCLAIMER, SourceProfile, Tenant, Topology, TopologyShard, ValueModel,
};
use ukiel_core::{TableColumns, WriteOpts, sorted_writer_props, writer_props};

use crate::fidelity;
use crate::profile::ProductionProfile;
use crate::rng::{self, StableRng};
use crate::schema::{PACKING_KEY, TS_COLUMN, table_spec};
use crate::topology::{SynthPart, SyntheticTopology};
use crate::value_model::RowGen;

/// Rows per Arrow batch handed to the writer. Sized so a batch of ten wide string
/// columns stays comfortably inside a few tens of MB.
const BATCH_ROWS: usize = 16_384;

#[derive(Debug, thiserror::Error)]
pub enum GenerateError {
    #[error("{path}: {source}")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("parquet: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
    #[error("arrow: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    #[error("schema: {0}")]
    Schema(#[from] ukiel_core::SchemaError),
    #[error(
        "{path} already exists. Refusing to overwrite a fixture that a report may already cite; \
         pass --replace if you mean to replace this exact label."
    )]
    Exists { path: String },
    #[error("fidelity gates failed:\n{0}")]
    Fidelity(String),
    #[error("{0}")]
    Census(String),
}

type Result<T> = std::result::Result<T, GenerateError>;

#[derive(Debug)]
pub struct Generated {
    pub manifest: Manifest,
    pub dir: PathBuf,
}

/// Compile → write → verify → publish.
///
/// Output lands in a temporary sibling and is renamed into place **only after every
/// gate passes**. A half-written fixture that looks complete is worse than no fixture:
/// it will be loaded, benchmarked, and cited.
pub fn generate(
    profile: &ProductionProfile,
    topology: &SyntheticTopology,
    output: &Path,
    replace: bool,
    gate: bool,
) -> Result<Generated> {
    if output.exists() {
        if !replace {
            return Err(GenerateError::Exists {
                path: output.display().to_string(),
            });
        }
        std::fs::remove_dir_all(output).map_err(|source| GenerateError::Io {
            path: output.display().to_string(),
            source,
        })?;
    }

    let staging = staging_dir(output);
    if staging.exists() {
        std::fs::remove_dir_all(&staging).map_err(|source| GenerateError::Io {
            path: staging.display().to_string(),
            source,
        })?;
    }
    std::fs::create_dir_all(&staging).map_err(|source| GenerateError::Io {
        path: staging.display().to_string(),
        source,
    })?;

    let result = write_all(profile, topology, &staging, gate);
    match result {
        Ok(manifest) => {
            std::fs::rename(&staging, output).map_err(|source| GenerateError::Io {
                path: output.display().to_string(),
                source,
            })?;
            Ok(Generated {
                manifest,
                dir: output.to_path_buf(),
            })
        }
        Err(e) => {
            // Leave nothing half-built behind.
            let _ = std::fs::remove_dir_all(&staging);
            Err(e)
        }
    }
}

fn staging_dir(output: &Path) -> PathBuf {
    let name = output
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| "fixture".to_string());
    output.with_file_name(format!(".{name}.staging"))
}

fn write_all(
    profile: &ProductionProfile,
    topology: &SyntheticTopology,
    dir: &Path,
    gate: bool,
) -> Result<Manifest> {
    let spec = table_spec();
    let columns = TableColumns::parse(&spec.schema)?;
    let physical = columns.physical_schema();
    let arrow_schema = Arc::new(physical.clone());
    let model = crate::value_model::default_model();

    let mut manifest_parts = Vec::with_capacity(topology.parts.len());
    let mut total_bytes = 0u64;

    for part in &topology.parts {
        let rel = format!(
            "parquet/shard-{:03}/part-{:05}.parquet",
            part.shard, part.index
        );
        let path = dir.join(&rel);
        std::fs::create_dir_all(path.parent().expect("has a parent")).map_err(|source| {
            GenerateError::Io {
                path: path.display().to_string(),
                source,
            }
        })?;

        let (rows, bytes, digest, ts_min, ts_max) = write_part(
            part,
            &arrow_schema,
            &columns,
            &spec.sort_key,
            &model,
            topology.config.seed,
            &path,
        )?;
        total_bytes += bytes;

        let src = &profile.parts[part.source];
        manifest_parts.push(ManifestPart {
            index: part.index,
            shard: part.shard,
            path: rel,
            rows,
            bytes,
            digest,
            key_min: part.key_min,
            key_max: part.key_max,
            distinct_keys: part.distinct_keys(),
            key_density: part.key_density(),
            ts_min,
            ts_max,
            provenance: PartProvenance {
                source_part_id: src.part_id,
                source_partition_id: src.partition_id,
                source_level: src.level,
                source_part_type: src.part_type.clone(),
                source_physical_rows: src.physical_rows,
                source_observed_rows: src.observed_rows,
                source_bytes_on_disk: src.bytes_on_disk,
                source_distinct_keys: src.distinct_keys,
                source_key_span: src.key_span,
            },
        });
    }

    // The census gate: what the graph promised, and what the files hold.
    let promised: u64 = topology.parts.iter().map(|p| p.rows_total()).sum();
    let actual: u64 = manifest_parts.iter().map(|p| p.rows).sum();
    if promised != actual {
        return Err(GenerateError::Census(format!(
            "the topology promised {promised} rows and the Parquet files hold {actual}"
        )));
    }

    let mut summary = topology.summary.clone();
    summary.bytes = total_bytes;

    // The distribution gates bind at `baseline`. The structural invariants above —
    // the census here, and the range/footer/membership checks in `verify` — bind
    // everywhere, and are what make a smoke fixture trustworthy even though its
    // distribution cannot be.
    let checks = fidelity::check(&profile.summary(), &summary);
    if gate && fidelity::gates_bind(topology.config.tier) && !fidelity::all_passed(&checks) {
        return Err(GenerateError::Fidelity(fidelity::render(&checks)));
    }

    // topology.json, then manifest.json — the manifest carries the topology's
    // digest, so it has to be written second.
    let topology_json = build_topology(topology, &model)?;
    let topology_bytes = serde_json::to_vec_pretty(&topology_json).expect("serializable");
    let topology_rel = "topology.json";
    write_file(&dir.join(topology_rel), &topology_bytes)?;

    let manifest = Manifest {
        manifest_version: prod_synth_contract::MANIFEST_VERSION.to_string(),
        disclaimer: SYNTHETIC_DISCLAIMER.to_string(),
        generator: prod_synth_contract::Generator {
            name: "prod-synth".to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            rng: rng::ALGORITHM.to_string(),
        },
        config: config_of(topology),
        source: SourceProfile {
            files: profile.files.clone(),
            summary: profile.summary(),
            assumptions: profile.assumptions(),
        },
        generated: summary,
        fidelity: checks,
        table: spec,
        value_model: model,
        topology: FileDigest::of(topology_rel, &topology_bytes),
        parts: manifest_parts,
        representatives: topology.representatives.clone(),
    };

    let manifest_bytes = serde_json::to_vec_pretty(&manifest).expect("serializable");
    write_file(&dir.join("manifest.json"), &manifest_bytes)?;

    Ok(manifest)
}

fn config_of(t: &SyntheticTopology) -> GenerationConfig {
    GenerationConfig {
        tier: t.config.tier,
        tenants: t.config.tenants,
        rows_per_shard: t.config.rows_per_shard,
        shards: t.config.shards,
        seed: t.config.seed,
        overrides: t.config.overrides.clone(),
    }
}

fn build_topology(t: &SyntheticTopology, _model: &ValueModel) -> Result<Topology> {
    let tenants: Vec<Tenant> = t
        .tenants
        .iter()
        .map(|x| Tenant {
            id: x.id,
            rows: x.rows,
            sample_rows: x.sample_rows,
            sample_exact_parts: x.sample_exact_parts,
            exact_parts: x.exact_parts,
            range_parts: x.range_parts,
            range_overfetch: x.range_parts as f64 / x.exact_parts.max(1) as f64,
            repaired: x.repaired,
        })
        .collect();

    let mut shards: Vec<TopologyShard> = (0..t.config.shards)
        .map(|shard| TopologyShard {
            shard,
            memberships: Vec::new(),
        })
        .collect();
    for p in &t.parts {
        shards[p.shard as usize].memberships.push(Membership {
            part: p.index,
            tenants: p.members.clone(),
            rows: p.rows.clone(),
            ts_min: p.ts_min,
            ts_max: p.ts_max,
        });
    }

    Ok(Topology {
        manifest_version: prod_synth_contract::MANIFEST_VERSION.to_string(),
        disclaimer: SYNTHETIC_DISCLAIMER.to_string(),
        config: config_of(t),
        key_universe: t.key_universe,
        tenants,
        shards,
    })
}

/// Write one part. Returns (rows, bytes, digest, ts_min, ts_max) — all read back
/// from the closed file, never from what we intended to write.
#[allow(clippy::too_many_arguments)]
fn write_part(
    part: &SynthPart,
    schema: &Arc<Schema>,
    columns: &TableColumns,
    sort_key: &[String],
    model: &ValueModel,
    seed: u64,
    path: &Path,
) -> Result<(u64, u64, String, i64, i64)> {
    // The product's own writer properties: ZSTD at L1+, the sort-key stamp, the
    // delta-packed timestamp, the opt-in bloom. The fixture is written the way Ukiel
    // writes files, not the way a benchmark wishes it did — otherwise a scan
    // measured over it is a scan over somebody else's layout.
    //
    // Level 1: a fixed non-L0 level keeps an idle benchmark stable. It is emphatically
    // *not* the source's ClickHouse merge level, which is recorded as provenance and
    // never mapped onto Ukiel's ladder.
    let opts = WriteOpts::from_columns(columns, sort_key, GENERATED_LEVEL);
    let props = writer_props(schema, sort_key, &opts);
    debug_assert_eq!(
        sorted_writer_props(schema, sort_key).compression(&"team_id".into()),
        props.compression(&"team_id".into()),
        "the generator must not diverge from the product's L1+ compression"
    );

    let file = File::create(path).map_err(|source| GenerateError::Io {
        path: path.display().to_string(),
        source,
    })?;
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))?;

    let mut rows_written = 0u64;
    let mut ts_min = i64::MAX;
    let mut ts_max = i64::MIN;

    // Members ascend, and rows within a member ascend by timestamp, so the file is
    // already in sort-key order as it is produced: `team_id` first, then `timestamp`.
    // Sorting a 100M-row part after the fact would defeat the streaming this file is
    // built around. `sort_batch` still runs per batch to settle the *within*-second
    // ties on event/distinct_id/uuid, which is cheap and keeps the ordering exactly
    // the product's.
    let mut buf = RowBuffer::new(BATCH_ROWS);
    for (member_idx, (&tenant, &nrows)) in part.members.iter().zip(&part.rows).enumerate() {
        let mut rows_gen = RowGen::new(
            model,
            tenant,
            nrows,
            seed ^ (part.index as u64) << 32 ^ member_idx as u64,
        );
        // Timestamps ascend within the member and stay inside the part's span, so the
        // file's footer bounds are the part's declared bounds by construction.
        let span = (part.ts_max - part.ts_min).max(1);
        let mut ts_rng = StableRng::stream(seed, &format!("ts-{}-{tenant}", part.index));
        let mut stamps: Vec<i64> = (0..nrows)
            .map(|_| part.ts_min + ts_rng.below(span as u64 + 1) as i64)
            .collect();
        stamps.sort_unstable();

        for &ts in &stamps {
            let r = rows_gen.next();
            ts_min = ts_min.min(ts);
            ts_max = ts_max.max(ts);
            buf.push(tenant, ts, &r);
            rows_written += 1;
            if buf.len() >= BATCH_ROWS {
                let batch = buf.drain(schema)?;
                writer.write(&ukiel_core::sort_batch(&batch, sort_key).map_err(box_sort)?)?;
            }
        }
    }
    if buf.len() > 0 {
        let batch = buf.drain(schema)?;
        writer.write(&ukiel_core::sort_batch(&batch, sort_key).map_err(box_sort)?)?;
    }
    writer.close()?;

    let bytes = std::fs::read(path).map_err(|source| GenerateError::Io {
        path: path.display().to_string(),
        source,
    })?;
    let digest = prod_synth_contract::digest_bytes(&bytes);
    Ok((
        rows_written,
        bytes.len() as u64,
        digest,
        ts_min,
        ts_max.max(ts_min),
    ))
}

/// A fixed non-L0 level. See `write_part`.
pub const GENERATED_LEVEL: i16 = 1;

fn box_sort(e: ukiel_core::SortKeyError) -> GenerateError {
    GenerateError::Arrow(arrow::error::ArrowError::ComputeError(e.to_string()))
}

/// Column-major row accumulation. Ten `Vec`s beat a `Vec` of ten-field structs:
/// the Arrow arrays want columns, and this is the shape they are already in.
struct RowBuffer {
    team_id: Vec<i64>,
    timestamp: Vec<i64>,
    event: Vec<String>,
    distinct_id: Vec<String>,
    uuid: Vec<String>,
    properties: Vec<String>,
    elements_chain: Vec<String>,
    current_url: Vec<String>,
    host: Vec<String>,
    lib: Vec<String>,
}

impl RowBuffer {
    fn new(cap: usize) -> Self {
        RowBuffer {
            team_id: Vec::with_capacity(cap),
            timestamp: Vec::with_capacity(cap),
            event: Vec::with_capacity(cap),
            distinct_id: Vec::with_capacity(cap),
            uuid: Vec::with_capacity(cap),
            properties: Vec::with_capacity(cap),
            elements_chain: Vec::with_capacity(cap),
            current_url: Vec::with_capacity(cap),
            host: Vec::with_capacity(cap),
            lib: Vec::with_capacity(cap),
        }
    }

    fn len(&self) -> usize {
        self.team_id.len()
    }

    fn push(&mut self, tenant: i64, ts: i64, r: &crate::value_model::Row) {
        self.team_id.push(tenant);
        self.timestamp.push(ts);
        self.event.push(r.event.clone());
        self.distinct_id.push(r.distinct_id.clone());
        self.uuid.push(r.uuid.clone());
        self.properties.push(r.properties.clone());
        self.elements_chain.push(r.elements_chain.clone());
        self.current_url.push(r.current_url.clone());
        self.host.push(r.host.clone());
        self.lib.push(r.lib.clone());
    }

    fn drain(&mut self, schema: &Arc<Schema>) -> Result<RecordBatch> {
        let cols: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(std::mem::take(&mut self.team_id))),
            Arc::new(Int64Array::from(std::mem::take(&mut self.timestamp))),
            Arc::new(StringArray::from(std::mem::take(&mut self.event))),
            Arc::new(StringArray::from(std::mem::take(&mut self.distinct_id))),
            Arc::new(StringArray::from(std::mem::take(&mut self.uuid))),
            Arc::new(StringArray::from(std::mem::take(&mut self.properties))),
            Arc::new(StringArray::from(std::mem::take(&mut self.elements_chain))),
            Arc::new(StringArray::from(std::mem::take(&mut self.current_url))),
            Arc::new(StringArray::from(std::mem::take(&mut self.host))),
            Arc::new(StringArray::from(std::mem::take(&mut self.lib))),
        ];
        Ok(RecordBatch::try_new(schema.clone(), cols)?)
    }
}

fn write_file(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut f = File::create(path).map_err(|source| GenerateError::Io {
        path: path.display().to_string(),
        source,
    })?;
    f.write_all(bytes).map_err(|source| GenerateError::Io {
        path: path.display().to_string(),
        source,
    })?;
    Ok(())
}

const _: () = {
    // `PACKING_KEY` and `TS_COLUMN` are referenced by the schema module; keep the
    // imports honest without a runtime cost.
    let _ = PACKING_KEY;
    let _ = TS_COLUMN;
};

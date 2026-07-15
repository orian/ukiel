//! Restage a plan-45 source artifact as Ukiel-shaped L0 files.
//!
//! The transformation is a **regrouping**, not a rewrite: the same rows come out, in
//! different files. Read the source rows in manifest/file order, cut them into
//! deterministic flushes of `flush_rows`, split each flush by the UTC day of its
//! timestamp column, sort each day slice by the table sort key, and write each slice as
//! one level-0 Parquet file with the product's own L0 writer properties.
//!
//! Two invariants make it trustworthy, and both are checked before the manifest is
//! written:
//!
//! * **census** — every source row appears in exactly one output file; and
//! * **fingerprint** — the order-independent row multiset of the output equals that of
//!   the input. `take` and sort are permutations, so they *should* preserve it; folding
//!   both sides and asserting equality is what catches the bug where they do not.

use std::collections::BTreeMap;
use std::fs::File;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, UInt32Array};
use arrow::compute::concat_batches;
use arrow::datatypes::Schema;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use prod_synth_contract::{
    FileDigest, L0File, L0Manifest, L0StagingConfig, Manifest, PROD_SYNTH_L0_VERSION,
    RowFingerprint, SYNTHETIC_DISCLAIMER,
};
use prod_synth_integrity::{ROW_MULTISET_VERSION, RowMultiset};
use ukiel_core::{TableColumns, WriteOpts, writer_props};

/// Rows per Arrow read batch. The flush buffer holds at most `flush_rows` plus one of
/// these, so peak memory is bounded regardless of tier — the shape tier's 100M rows are
/// never all resident.
const READ_BATCH_ROWS: usize = 8_192;

#[derive(Debug, thiserror::Error)]
pub enum StageError {
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
    #[error(transparent)]
    Contract(#[from] prod_synth_contract::ContractError),
    #[error("fingerprint: {0}")]
    Fingerprint(String),
    #[error(
        "{path} already exists. Refusing to overwrite a staged artifact a load may already \
         reference; pass --replace to replace this exact output."
    )]
    Exists { path: String },
    #[error(
        "row at {ts} ms is outside the fixture window [{start}, {end}); a staged row cannot fall \
         outside the window the source declares"
    )]
    TimestampOutOfWindow { ts: i64, start: i64, end: i64 },
    #[error(
        "the source artifact holds {source_rows} rows but staging wrote {staged_rows}: staging \
         regroups rows, it must never add or drop one"
    )]
    CensusMismatch { source_rows: u64, staged_rows: u64 },
    #[error(
        "the staged row multiset does not match the source's. Regrouping permuted the rows but \
         did not preserve them — this is a bug in take/sort/day-grouping, not a tolerance"
    )]
    FingerprintMismatch,
}

type Result<T> = std::result::Result<T, StageError>;

#[derive(Debug)]
pub struct Staged {
    pub manifest: L0Manifest,
    pub dir: PathBuf,
}

/// Stage `manifest` (already parsed and its parts digest-verified by the caller) from
/// `source_dir` into `output`.
pub fn stage(
    manifest: &Manifest,
    source_manifest_bytes: &[u8],
    source_topology: FileDigest,
    source_dir: &Path,
    output: &Path,
    flush_rows: u64,
    replace: bool,
) -> Result<Staged> {
    if output.exists() {
        if !replace {
            return Err(StageError::Exists {
                path: output.display().to_string(),
            });
        }
        std::fs::remove_dir_all(output).map_err(|source| StageError::Io {
            path: output.display().to_string(),
            source,
        })?;
    }
    let staging = staging_dir(output);
    let _ = std::fs::remove_dir_all(&staging);
    std::fs::create_dir_all(&staging).map_err(|source| StageError::Io {
        path: staging.display().to_string(),
        source,
    })?;

    let result = write_all(
        manifest,
        source_manifest_bytes,
        source_topology,
        source_dir,
        &staging,
        flush_rows,
    );
    match result {
        Ok(l0) => {
            std::fs::rename(&staging, output).map_err(|source| StageError::Io {
                path: output.display().to_string(),
                source,
            })?;
            Ok(Staged {
                manifest: l0,
                dir: output.to_path_buf(),
            })
        }
        Err(e) => {
            let _ = std::fs::remove_dir_all(&staging);
            Err(e)
        }
    }
}

fn staging_dir(output: &Path) -> PathBuf {
    let name = output
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| "l0".to_string());
    output.with_file_name(format!(".{name}.staging"))
}

struct Writer<'a> {
    dir: &'a Path,
    schema: Arc<Schema>,
    columns: &'a TableColumns,
    sort_key: &'a [String],
    ts_idx: usize,
    key_idx: usize,
    files: Vec<L0File>,
    input_fp: RowMultiset,
    output_fp: RowMultiset,
    input_rows: u64,
    output_rows: u64,
}

fn write_all(
    manifest: &Manifest,
    source_manifest_bytes: &[u8],
    source_topology: FileDigest,
    source_dir: &Path,
    dir: &Path,
    flush_rows: u64,
) -> Result<L0Manifest> {
    let columns = TableColumns::parse(&manifest.table.schema)?;
    let physical = columns.physical_schema();
    let schema = Arc::new(physical.clone());
    let ts_idx = schema.index_of(&manifest.table.ts_column).map_err(|_| {
        StageError::Schema(ukiel_core::SchemaError::Invalid(format!(
            "ts_column '{}' not in schema",
            manifest.table.ts_column
        )))
    })?;
    let key_idx = schema.index_of(&manifest.table.packing_key).map_err(|_| {
        StageError::Schema(ukiel_core::SchemaError::Invalid(format!(
            "packing_key '{}' not in schema",
            manifest.table.packing_key
        )))
    })?;

    let mut w = Writer {
        dir,
        schema: schema.clone(),
        columns: &columns,
        sort_key: &manifest.table.sort_key,
        ts_idx,
        key_idx,
        files: Vec::new(),
        input_fp: RowMultiset::default(),
        output_fp: RowMultiset::default(),
        input_rows: 0,
        output_rows: 0,
    };

    // The flush buffer: at most `flush_rows` + one read batch resident at any time.
    let mut pending: Vec<RecordBatch> = Vec::new();
    let mut pending_rows: usize = 0;
    let mut flush_index: u32 = 0;

    for part in &manifest.parts {
        let path = source_dir.join(&part.path);
        let raw = std::fs::read(&path).map_err(|source| StageError::Io {
            path: path.display().to_string(),
            source,
        })?;
        // The manifest already carries this part's digest; the caller verified it. We
        // read the bytes, not the promise.
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(raw))?
            .with_batch_size(READ_BATCH_ROWS)
            .build()?;
        for batch in reader {
            let batch = batch?;
            w.input_rows += batch.num_rows() as u64;
            w.input_fp
                .update(&batch)
                .map_err(|e| StageError::Fingerprint(e.to_string()))?;
            pending_rows += batch.num_rows();
            pending.push(batch);

            while pending_rows >= flush_rows as usize {
                let all = concat_batches(&schema, &pending)?;
                let flush = all.slice(0, flush_rows as usize);
                let rest = all.slice(flush_rows as usize, all.num_rows() - flush_rows as usize);
                w.flush(flush_index, &flush)?;
                flush_index += 1;
                pending = if rest.num_rows() > 0 {
                    vec![rest]
                } else {
                    Vec::new()
                };
                pending_rows = pending.first().map_or(0, |b| b.num_rows());
            }
        }
    }
    if pending_rows > 0 {
        let all = concat_batches(&schema, &pending)?;
        w.flush(flush_index, &all)?;
    }

    if w.input_rows != w.output_rows {
        return Err(StageError::CensusMismatch {
            source_rows: w.input_rows,
            staged_rows: w.output_rows,
        });
    }
    if w.input_fp != w.output_fp {
        return Err(StageError::FingerprintMismatch);
    }

    let fingerprint = RowFingerprint {
        version: ROW_MULTISET_VERSION.to_string(),
        count: w.output_fp.count,
        xor: hex(&w.output_fp.xor),
        sum: w.output_fp.sum,
        digest: w.output_fp.digest_hex(),
    };

    let l0 = L0Manifest {
        manifest_version: PROD_SYNTH_L0_VERSION.to_string(),
        disclaimer: SYNTHETIC_DISCLAIMER.to_string(),
        source_manifest: FileDigest::of("manifest.json", source_manifest_bytes),
        source_topology,
        staging: L0StagingConfig {
            flush_rows,
            day_policy: "utc-day-from-timestamp".to_string(),
            seed: manifest.config.seed,
        },
        table: manifest.table.clone(),
        representatives: manifest.representatives.clone(),
        input_rows: w.input_rows,
        output_rows: w.output_rows,
        fingerprint,
        files: w.files,
    };

    let bytes = serde_json::to_vec_pretty(&l0).expect("serializable");
    let mut f = File::create(dir.join("l0-manifest.json")).map_err(|source| StageError::Io {
        path: "l0-manifest.json".to_string(),
        source,
    })?;
    f.write_all(&bytes).map_err(|source| StageError::Io {
        path: "l0-manifest.json".to_string(),
        source,
    })?;

    Ok(l0)
}

impl Writer<'_> {
    /// One flush: split by UTC day, sort each day, write one file per day.
    fn flush(&mut self, flush_index: u32, flush: &RecordBatch) -> Result<()> {
        let ts = flush
            .column(self.ts_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("timestamp is int64");

        // Row indices per UTC day. A BTreeMap keeps days in ascending order, so the file
        // list — and therefore the manifest — is deterministic.
        let mut by_day: BTreeMap<i32, Vec<u32>> = BTreeMap::new();
        for row in 0..flush.num_rows() {
            let t = ts.value(row);
            if !(prod_synth_contract::FIXTURE_WINDOW_START_MS
                ..prod_synth_contract::FIXTURE_WINDOW_END_MS)
                .contains(&t)
            {
                return Err(StageError::TimestampOutOfWindow {
                    ts: t,
                    start: prod_synth_contract::FIXTURE_WINDOW_START_MS,
                    end: prod_synth_contract::FIXTURE_WINDOW_END_MS,
                });
            }
            by_day
                .entry(days_since_epoch(t))
                .or_default()
                .push(row as u32);
        }

        for (day, rows) in by_day {
            let indices = UInt32Array::from(rows);
            let day_batch = arrow::compute::take_record_batch(flush, &indices)?;
            // The product's own canonical sort. L0 files must be sorted by the sort key
            // or the catalog and the query provider both lie about the ordering.
            let sorted = ukiel_core::sort_batch(&day_batch, self.sort_key).map_err(|e| {
                StageError::Arrow(arrow::error::ArrowError::ComputeError(e.to_string()))
            })?;

            self.output_rows += sorted.num_rows() as u64;
            self.output_fp
                .update(&sorted)
                .map_err(|e| StageError::Fingerprint(e.to_string()))?;

            let day_str = civil_from_days(day);
            let rel = format!("parquet/{day_str}/flush-{flush_index:05}.parquet");
            let path = self.dir.join(&rel);
            std::fs::create_dir_all(path.parent().expect("has parent")).map_err(|source| {
                StageError::Io {
                    path: path.display().to_string(),
                    source,
                }
            })?;

            let (bytes, digest, kmin, kmax, tmin, tmax) = self.write_file(&sorted, &path)?;
            self.files.push(L0File {
                path: rel,
                day: day_str,
                flush_index,
                rows: sorted.num_rows() as u64,
                bytes,
                digest,
                key_min: kmin,
                key_max: kmax,
                ts_min: tmin,
                ts_max: tmax,
            });
        }
        Ok(())
    }

    fn write_file(
        &self,
        batch: &RecordBatch,
        path: &Path,
    ) -> Result<(u64, String, i64, i64, i64, i64)> {
        // Level 0: LZ4_RAW, the sort-key stamp, the delta-packed timestamp, the opt-in
        // bloom columns — exactly what ingest writes, from ukiel-core. A staged L0 file
        // must be indistinguishable from one the ingest path would have produced, or the
        // compactor is fed a different input than production.
        let opts = WriteOpts::from_columns(self.columns, self.sort_key, 0);
        let props = writer_props(&self.schema, self.sort_key, &opts);

        let file = File::create(path).map_err(|source| StageError::Io {
            path: path.display().to_string(),
            source,
        })?;
        let mut writer = ArrowWriter::try_new(file, self.schema.clone(), Some(props))?;
        writer.write(batch)?;
        writer.close()?;

        let raw = std::fs::read(path).map_err(|source| StageError::Io {
            path: path.display().to_string(),
            source,
        })?;
        let key = batch
            .column(self.key_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("packing key is int64");
        let ts = batch
            .column(self.ts_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("timestamp is int64");
        Ok((
            raw.len() as u64,
            prod_synth_contract::digest_bytes(&raw),
            arrow::compute::min(key).expect("non-empty"),
            arrow::compute::max(key).expect("non-empty"),
            arrow::compute::min(ts).expect("non-empty"),
            arrow::compute::max(ts).expect("non-empty"),
        ))
    }
}

fn hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

/// Whole UTC days since the Unix epoch for an epoch-millisecond timestamp.
///
/// `div_euclid`, not `/`: it floors toward negative infinity, so a pre-epoch timestamp
/// (which the generator never produces, but a corrupt input might) lands on the correct
/// day rather than one day late.
fn days_since_epoch(ms: i64) -> i32 {
    ms.div_euclid(86_400_000) as i32
}

/// `YYYY-MM-DD` for a whole-day count since 1970-01-01, by the standard civil-calendar
/// algorithm (Howard Hinnant's `civil_from_days`). Pure arithmetic, no calendar library
/// — the tool stays offline and dependency-light, and there is no timezone subtlety in
/// UTC.
fn civil_from_days(z: i32) -> String {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as i64; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365; // [0, 399]
    let year = yoe as i32 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let day = (doy - (153 * mp + 2) / 5 + 1) as i32; // [1, 31]
    let month = if mp < 10 { mp + 3 } else { mp - 9 } as i32; // [1, 12]
    let year = if month <= 2 { year + 1 } else { year };
    format!("{year:04}-{month:02}-{day:02}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn civil_dates_are_correct() {
        assert_eq!(civil_from_days(0), "1970-01-01");
        // 2026-07-01: the fixture window start.
        assert_eq!(
            civil_from_days(days_since_epoch(
                prod_synth_contract::FIXTURE_WINDOW_START_MS
            )),
            "2026-07-01"
        );
        // One millisecond before the window end is the window's last day.
        assert_eq!(
            civil_from_days(days_since_epoch(
                prod_synth_contract::FIXTURE_WINDOW_END_MS - 1
            )),
            "2026-07-14"
        );
    }

    #[test]
    fn day_boundaries_are_utc_midnight() {
        let midnight = prod_synth_contract::FIXTURE_WINDOW_START_MS;
        assert_eq!(
            days_since_epoch(midnight),
            days_since_epoch(midnight + 86_399_999)
        );
        assert_ne!(
            days_since_epoch(midnight),
            days_since_epoch(midnight + 86_400_000)
        );
    }
}

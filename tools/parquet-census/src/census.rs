//! The physical Parquet census: raw per-file/row-group/column-chunk records plus
//! per-column aggregates, read footer-first with bounded optional column scans.
//!
//! The whole point is to report what was *actually written* — the encodings the writer
//! chose (dictionary fallback included), the codec it applied, whether page/offset
//! indices and Bloom filters are present, and where the bytes live — rather than what a
//! spec requested. Where the footer cannot supply a metric (NDV, value width), the tool
//! streams that column and labels the number `scanned`; it never invents one from min/max.

use std::collections::{BTreeMap, HashSet};

use anyhow::{Context, Result};
use arrow::array::Array;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::metadata::RowGroupMetaData;
use serde::Serialize;

/// Where a metric came from. A number that could not be sourced honestly is
/// `Unavailable`, never a guess.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Provenance {
    /// Read directly from the column-chunk footer statistics.
    Footer,
    /// Read from the page (offset/column) index.
    PageIndex,
    /// Computed by streaming the column's values.
    Scanned,
    /// The footer did not carry it and no scan was requested.
    Unavailable,
}

/// One column chunk's physical record.
#[derive(Debug, Clone, Serialize)]
pub struct ColumnChunkCensus {
    pub column: String,
    pub physical_type: String,
    /// The Parquet logical/converted type annotation, if any.
    pub parquet_logical_type: Option<String>,
    /// The declared laboratory logical type from the snapshot manifest, if known.
    pub declared_logical_type: Option<String>,
    pub compression: String,
    /// The encodings the writer actually emitted — dictionary fallback shows here.
    pub encodings: Vec<String>,
    pub has_dictionary_page: bool,
    pub compressed_bytes: i64,
    pub uncompressed_bytes: i64,
    pub num_values: i64,
    pub null_count: Option<i64>,
    pub null_count_source: Provenance,
    pub distinct_count: Option<i64>,
    pub distinct_count_source: Provenance,
    /// Whether the footer's min and max are exact (not truncated).
    pub min_exact: Option<bool>,
    pub max_exact: Option<bool>,
    pub bloom_filter_offset: Option<i64>,
    pub bloom_filter_length: Option<i64>,
    pub has_column_index: bool,
    pub has_offset_index: bool,
    /// Mean serialized value width in bytes, when scanned.
    pub mean_value_bytes: Option<f64>,
    pub value_width_source: Provenance,
}

/// One row group's record.
#[derive(Debug, Clone, Serialize)]
pub struct RowGroupCensus {
    pub ordinal: usize,
    pub rows: i64,
    pub compressed_bytes: i64,
    pub uncompressed_bytes: i64,
    pub sorting_columns: Vec<SortingColumnRecord>,
    pub columns: Vec<ColumnChunkCensus>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SortingColumnRecord {
    pub column_index: i32,
    pub descending: bool,
    pub nulls_first: bool,
}

/// One file's record.
#[derive(Debug, Clone, Serialize)]
pub struct FileCensus {
    pub path: String,
    pub file_bytes: u64,
    pub rows: i64,
    pub row_groups: usize,
    /// File bytes not accounted for by compressed column data: footer, page indices,
    /// Bloom filters, page headers, and the magic bytes. An overhead ceiling, labeled
    /// as such rather than pretended to be exactly the thrift footer.
    pub metadata_overhead_bytes: i64,
    pub row_group_records: Vec<RowGroupCensus>,
}

/// Per-column rollup across every file and row group.
#[derive(Debug, Clone, Serialize)]
pub struct ColumnAggregate {
    pub column: String,
    pub compressed_bytes: i64,
    pub uncompressed_bytes: i64,
    /// This column's share of all compressed column bytes.
    pub compressed_fraction: f64,
    pub num_values: i64,
    pub null_count: i64,
    /// Every distinct encoding this column used anywhere, sorted.
    pub encodings: Vec<String>,
    /// Row-group chunks that carried a dictionary page.
    pub dictionary_chunks: usize,
    pub total_chunks: usize,
    pub bloom_chunks: usize,
}

/// The whole report.
#[derive(Debug, Clone, Serialize)]
pub struct CensusReport {
    pub report_kind: String,
    /// The parent manifest's digest, binding the census to exact bytes.
    pub manifest_digest: String,
    pub files: usize,
    pub total_rows: i64,
    pub total_compressed_bytes: i64,
    pub total_uncompressed_bytes: i64,
    pub total_metadata_overhead_bytes: i64,
    /// Whether columns were scanned for NDV/value width.
    pub scanned_columns: bool,
    pub file_records: Vec<FileCensus>,
    pub column_aggregates: Vec<ColumnAggregate>,
}

/// Census one file's bytes. `declared` maps a column name to its declared laboratory
/// logical type. When `scan` is set, columns are streamed for NDV and value width.
pub fn census_file(
    path: &str,
    bytes: &[u8],
    declared: &BTreeMap<String, String>,
    scan: bool,
) -> Result<FileCensus> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::copy_from_slice(bytes))
        .with_context(|| format!("opening {path}"))?;
    let meta = builder.metadata().clone();
    let file_rows = meta.file_metadata().num_rows();

    // Optional scan: distinct counts and mean value widths per column, computed once from
    // a fresh reader over the same bytes.
    let scanned = if scan {
        Some(scan_columns(bytes).with_context(|| format!("scanning {path}"))?)
    } else {
        None
    };

    let mut compressed_total = 0i64;
    let mut row_group_records = Vec::with_capacity(meta.num_row_groups());
    for (rg_ord, rg) in meta.row_groups().iter().enumerate() {
        let (rg_census, rg_compressed) = census_row_group(rg_ord, rg, declared, scanned.as_ref());
        compressed_total += rg_compressed;
        row_group_records.push(rg_census);
    }

    let metadata_overhead = (bytes.len() as i64 - compressed_total - 4).max(0);
    Ok(FileCensus {
        path: path.to_string(),
        file_bytes: bytes.len() as u64,
        rows: file_rows,
        row_groups: meta.num_row_groups(),
        metadata_overhead_bytes: metadata_overhead,
        row_group_records,
    })
}

fn census_row_group(
    ordinal: usize,
    rg: &RowGroupMetaData,
    declared: &BTreeMap<String, String>,
    scanned: Option<&ScannedColumns>,
) -> (RowGroupCensus, i64) {
    let mut columns = Vec::with_capacity(rg.num_columns());
    let mut rg_compressed = 0i64;
    let mut rg_uncompressed = 0i64;
    for col in rg.columns() {
        let name = col.column_path().string();
        let compressed = col.compressed_size();
        let uncompressed = col.uncompressed_size();
        rg_compressed += compressed;
        rg_uncompressed += uncompressed;

        let (null_count, null_src, distinct_footer, min_exact, max_exact) = match col.statistics() {
            Some(s) => (
                s.null_count_opt().map(|n| n as i64),
                if s.null_count_opt().is_some() {
                    Provenance::Footer
                } else {
                    Provenance::Unavailable
                },
                s.distinct_count_opt().map(|n| n as i64),
                Some(s.min_is_exact()),
                Some(s.max_is_exact()),
            ),
            None => (None, Provenance::Unavailable, None, None, None),
        };

        // NDV: footer if present, else scanned if we scanned, else unavailable.
        let scan_col = scanned.and_then(|s| s.by_column.get(&name));
        let (distinct_count, distinct_src) = match (distinct_footer, scan_col) {
            (Some(n), _) => (Some(n), Provenance::Footer),
            (None, Some(sc)) => (Some(sc.distinct as i64), Provenance::Scanned),
            (None, None) => (None, Provenance::Unavailable),
        };
        let (mean_value_bytes, value_src) = match scan_col {
            Some(sc) => (Some(sc.mean_value_bytes), Provenance::Scanned),
            None => (None, Provenance::Unavailable),
        };

        columns.push(ColumnChunkCensus {
            column: name.clone(),
            physical_type: format!("{:?}", col.column_type()),
            parquet_logical_type: col
                .column_descr()
                .logical_type_ref()
                .map(|lt| format!("{lt:?}")),
            declared_logical_type: declared.get(&name).cloned(),
            compression: format!("{:?}", col.compression()),
            encodings: col.encodings().map(|e| format!("{e:?}")).collect(),
            has_dictionary_page: col.dictionary_page_offset().is_some(),
            compressed_bytes: compressed,
            uncompressed_bytes: uncompressed,
            num_values: col.num_values(),
            null_count,
            null_count_source: null_src,
            distinct_count,
            distinct_count_source: distinct_src,
            min_exact,
            max_exact,
            bloom_filter_offset: col.bloom_filter_offset(),
            bloom_filter_length: col.bloom_filter_length().map(|l| l as i64),
            has_column_index: col.column_index_offset().is_some(),
            has_offset_index: col.offset_index_offset().is_some(),
            mean_value_bytes,
            value_width_source: value_src,
        });
    }

    let sorting_columns = rg
        .sorting_columns()
        .map(|scs| {
            scs.iter()
                .map(|sc| SortingColumnRecord {
                    column_index: sc.column_idx,
                    descending: sc.descending,
                    nulls_first: sc.nulls_first,
                })
                .collect()
        })
        .unwrap_or_default();

    (
        RowGroupCensus {
            ordinal,
            rows: rg.num_rows(),
            compressed_bytes: rg_compressed,
            uncompressed_bytes: rg_uncompressed,
            sorting_columns,
            columns,
        },
        rg_compressed,
    )
}

/// A scanned column's derived metrics.
struct ScannedColumn {
    distinct: u64,
    mean_value_bytes: f64,
}

struct ScannedColumns {
    by_column: BTreeMap<String, ScannedColumn>,
}

/// Stream every column once, computing distinct counts and mean serialized value widths.
/// Bounded by holding one batch at a time, but it does visit every value — only run when
/// a metric actually needs it.
fn scan_columns(bytes: &[u8]) -> Result<ScannedColumns> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::copy_from_slice(bytes))?;
    let schema = builder.schema().clone();
    let reader = builder.build().context("building scan reader")?;

    let mut sets: Vec<HashSet<Vec<u8>>> =
        (0..schema.fields().len()).map(|_| HashSet::new()).collect();
    let mut byte_totals = vec![0u64; schema.fields().len()];
    let mut value_counts = vec![0u64; schema.fields().len()];

    for batch in reader {
        let batch = batch?;
        for (i, col) in batch.columns().iter().enumerate() {
            for row in 0..col.len() {
                if col.is_null(row) {
                    continue;
                }
                let key = value_bytes(col.as_ref(), row);
                byte_totals[i] += key.len() as u64;
                value_counts[i] += 1;
                sets[i].insert(key);
            }
        }
    }

    let mut by_column = BTreeMap::new();
    for (i, field) in schema.fields().iter().enumerate() {
        let mean = if value_counts[i] == 0 {
            0.0
        } else {
            byte_totals[i] as f64 / value_counts[i] as f64
        };
        by_column.insert(
            field.name().clone(),
            ScannedColumn {
                distinct: sets[i].len() as u64,
                mean_value_bytes: mean,
            },
        );
    }
    Ok(ScannedColumns { by_column })
}

/// A stable byte key for one non-null value, used for distinct counting and width.
fn value_bytes(col: &dyn Array, row: usize) -> Vec<u8> {
    use arrow::array::*;
    use arrow::datatypes::DataType;
    match col.data_type() {
        DataType::Int64 => col
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(row)
            .to_le_bytes()
            .to_vec(),
        DataType::Int32 => col
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(row)
            .to_le_bytes()
            .to_vec(),
        DataType::Float64 => col
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(row)
            .to_le_bytes()
            .to_vec(),
        DataType::Boolean => vec![
            col.as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .value(row) as u8,
        ],
        DataType::Utf8 => col
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(row)
            .as_bytes()
            .to_vec(),
        DataType::Utf8View => col
            .as_any()
            .downcast_ref::<StringViewArray>()
            .unwrap()
            .value(row)
            .as_bytes()
            .to_vec(),
        // Anything else: use the one-row slice's debug form so distinct counting still
        // works and width is approximate. The event/ClickBench columns are covered above.
        _ => format!("{:?}", col.slice(row, 1)).into_bytes(),
    }
}

/// Roll per-file records up into per-column aggregates and totals.
pub fn aggregate(
    report_kind: &str,
    manifest_digest: &str,
    scanned: bool,
    file_records: Vec<FileCensus>,
) -> CensusReport {
    let mut total_rows = 0i64;
    let mut total_compressed = 0i64;
    let mut total_uncompressed = 0i64;
    let mut total_overhead = 0i64;

    // column name -> running aggregate.
    let mut cols: BTreeMap<String, ColumnAggregate> = BTreeMap::new();
    let mut col_encodings: BTreeMap<String, std::collections::BTreeSet<String>> = BTreeMap::new();

    for f in &file_records {
        total_rows += f.rows;
        total_overhead += f.metadata_overhead_bytes;
        for rg in &f.row_group_records {
            total_compressed += rg.compressed_bytes;
            total_uncompressed += rg.uncompressed_bytes;
            for c in &rg.columns {
                let agg = cols
                    .entry(c.column.clone())
                    .or_insert_with(|| ColumnAggregate {
                        column: c.column.clone(),
                        compressed_bytes: 0,
                        uncompressed_bytes: 0,
                        compressed_fraction: 0.0,
                        num_values: 0,
                        null_count: 0,
                        encodings: Vec::new(),
                        dictionary_chunks: 0,
                        total_chunks: 0,
                        bloom_chunks: 0,
                    });
                agg.compressed_bytes += c.compressed_bytes;
                agg.uncompressed_bytes += c.uncompressed_bytes;
                agg.num_values += c.num_values;
                agg.null_count += c.null_count.unwrap_or(0);
                agg.total_chunks += 1;
                if c.has_dictionary_page {
                    agg.dictionary_chunks += 1;
                }
                if c.bloom_filter_offset.is_some() {
                    agg.bloom_chunks += 1;
                }
                let set = col_encodings.entry(c.column.clone()).or_default();
                for e in &c.encodings {
                    set.insert(e.clone());
                }
            }
        }
    }

    let mut column_aggregates: Vec<ColumnAggregate> = cols
        .into_values()
        .map(|mut a| {
            a.compressed_fraction = if total_compressed == 0 {
                0.0
            } else {
                a.compressed_bytes as f64 / total_compressed as f64
            };
            a.encodings = col_encodings
                .get(&a.column)
                .map(|s| s.iter().cloned().collect())
                .unwrap_or_default();
            a
        })
        .collect();
    column_aggregates.sort_by_key(|a| std::cmp::Reverse(a.compressed_bytes));

    CensusReport {
        report_kind: report_kind.to_string(),
        manifest_digest: manifest_digest.to_string(),
        files: file_records.len(),
        total_rows,
        total_compressed_bytes: total_compressed,
        total_uncompressed_bytes: total_uncompressed,
        total_metadata_overhead_bytes: total_overhead,
        scanned_columns: scanned,
        file_records,
        column_aggregates,
    }
}

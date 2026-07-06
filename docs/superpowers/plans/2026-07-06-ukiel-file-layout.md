# Ukiel Plan 14: Parquet File Layout & Encoding (Tier-3 write-side) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Tier 3 of `docs/notes/2026-07-06-parquet-read-performance.md` (items #7–#10): (7) compaction output row groups aligned to packing-key runs so row-group key stats become perfectly selective for the isolation predicate, (8) row-group size discipline (128k-row cap everywhere), (9) `DELTA_BINARY_PACKED` for the sorted `ts` column plus per-level compression (L0 = LZ4_RAW for fast decode of short-lived files, L1+ = ZSTD), (10) opt-in per-column bloom filters declared in the table schema.

**Architecture:** All write-side layout policy moves into ONE new shared module, `crates/ukiel-core/src/parquet_props.rs` (`WriteOpts` + `writer_props()`); the ingest writer (`crates/ukiel-ingest/src/writer.rs`) and the compactor rewrite engine (`crates/ukiel-compactor/src/rewrite.rs`) become its only two consumers. Key alignment happens only in the compactor's **packed** placement path (separated placement already yields one key per file); it is implemented as `rewrite::batch_to_parquet_key_aligned`, which streams per-key slices from `rewrite::split_by_key` into one `ArrowWriter`, calling `flush()` (= end the current row group) before any key run that would overflow the row-group cap — so boundaries land on key boundaries whenever a key fits, tiny keys still share row groups, and a huge key spans several. Two invariants must survive every task: **readers are layout-agnostic** (every existing ingest/compactor/query test stays green — they are the behavioral safety net), and **layout claims are verified by reading footers back** (`SerializedFileReader` over the produced bytes: row-group counts, per-row-group key min/max stats, column-chunk encodings/compression, bloom-filter offsets).

**Tech Stack:** arrow + parquet 58.3 (pinned — the versions DataFusion 54 links against; never bump independently). Parquet-crate API names drift between majors — the code below uses `parquet::basic::{Compression, Encoding}`, `parquet::schema::types::ColumnPath`, `parquet::format::SortingColumn`, `WriterProperties` builder methods (`set_column_encoding`, `set_column_dictionary_enabled`, `set_column_bloom_filter_enabled`, `set_sorting_columns`, `set_max_row_group_size`), `ArrowWriter::{flush, in_progress_rows}`, and metadata readers (`ColumnChunkMetaData::{compression, encodings, dictionary_page_offset, bloom_filter_offset, statistics}`, `RowGroupMetaData::{num_rows, sorting_columns}`, `Statistics::Int64` with `min_opt()`/`max_opt()`). On any name/signature mismatch check docs.rs for the workspace's exact parquet version and adapt minimally, keeping the plan shape (e.g. if `parquet::format` no longer exists in 58.3, `SortingColumn` lives where docs.rs says — same three fields).

**Prerequisites:** Plans 1–7 executed and green. Plan 11 (`2026-07-06-ukiel-scan-pushdown.md`) is **not** required, but see the adaptation constraint below if it has landed.

## Global Constraints

- Rust edition 2024, toolchain ≥ 1.96.
- Component tests via `make test` (`cargo test -- --test-threads=2`, Docker required for testcontainers); the unit tests added by Tasks 1–3 must pass without Docker (`cargo test -p <crate> --lib`).
- **Every existing ingest, compactor, and query test must stay green after every task.** Readers are layout-agnostic; any red reader test means the layout change broke data, not just layout.
- **Adapt to the writers' current state.** Plan 11's Task 4 also touches `finish_batch` and `batch_to_parquet` (ad-hoc `sorting_columns` props). As of writing (2026-07-06) it has NOT landed: `finish_batch(batch, packing_key)` builds plain ZSTD props and `batch_to_parquet(batch)` takes no sort key. If plan 11 Task 4 lands first, `finish_batch` will already take a `ts_column` parameter and set `sorting_columns`, and `batch_to_parquet` will take `(batch, sort_key: &[String])` — in that case delete the ad-hoc props code in favor of the shared `writer_props` (this plan's `WriteOpts.sort_columns` preserves the `sorting_columns` metadata plan 11 relies on) and adjust the signatures to this plan's shapes; plan 11's declared-ordering regression tests must stay green.
- `Compression::LZ4_RAW` and bloom-filter writing are in parquet's default feature set; if the build errors with a disabled-feature message, add the named feature to the workspace `parquet` dependency in the root `Cargo.toml` — do not bump the version.
- Commit messages: conventional style, no Claude/AI attribution.
- Every task ends with `cargo fmt && cargo clippy --all-targets -- -D warnings` clean.

---

### Task 1: Shared writer properties in ukiel-core + `bloom_filter` schema attribute

**Files:**
- Modify: `crates/ukiel-core/Cargo.toml`
- Modify: `crates/ukiel-core/src/schema.rs`
- Create: `crates/ukiel-core/src/parquet_props.rs`
- Modify: `crates/ukiel-core/src/lib.rs`

**Interfaces:**
- Produces: `ColumnSpec.bloom_filter: bool` (JSON attr `"bloom_filter": true`, default false, rejected on alias columns — they are never stored) and `TableColumns::bloom_columns() -> Vec<String>`.
- Produces: `ukiel_core::{WriteOpts, writer_props, DEFAULT_MAX_ROW_GROUP_ROWS}` — the single place encoding Ukiel's write-side layout policy. No Docker needed anywhere in this task.

- [ ] **Step 1: Add dependencies**

Replace the entire contents of `crates/ukiel-core/Cargo.toml` with:

```toml
[package]
name = "ukiel-core"
version.workspace = true
edition.workspace = true

[dependencies]
serde = { workspace = true }
serde_json = { workspace = true }
arrow = { workspace = true }
parquet = { workspace = true }
thiserror = { workspace = true }

[dev-dependencies]
bytes = { workspace = true }
```

- [ ] **Step 2: Write the failing schema tests**

Append inside the `mod tests` block of `crates/ukiel-core/src/schema.rs`:

```rust
    #[test]
    fn parses_bloom_filter_attribute_and_lists_bloom_columns() {
        let cols = TableColumns::parse(&json!({"fields": [
            {"name": "tenant_id", "type": "int64"},
            {"name": "ts", "type": "timestamp_ms"},
            {"name": "device_id", "type": "utf8", "bloom_filter": true},
            {"name": "amount", "type": "float64", "default": "0.0", "bloom_filter": true},
            {"name": "payload", "type": "utf8"}
        ]}))
        .unwrap();

        assert!(!cols.specs[0].bloom_filter); // default false
        assert!(cols.specs[2].bloom_filter);
        assert!(cols.specs[3].bloom_filter); // orthogonal to kind
        assert_eq!(cols.bloom_columns(), vec!["device_id", "amount"]);

        // bloom_filter is orthogonal to kind but meaningless on alias
        // columns (never stored) -> rejected.
        let err = TableColumns::parse(&json!({"fields": [
            {"name": "amount", "type": "float64"},
            {"name": "half", "type": "float64", "alias": "amount / 2.0", "bloom_filter": true}
        ]}))
        .unwrap_err();
        assert!(matches!(err, SchemaError::Invalid(_)));
    }
```

- [ ] **Step 3: Verify the test fails**

Run: `cargo test -p ukiel-core`
Expected: compile error (`bloom_filter` field and `bloom_columns` method don't exist yet).

- [ ] **Step 4: Implement the schema attribute**

In `crates/ukiel-core/src/schema.rs`:

1. Add the field to `ColumnSpec` (after `kind`):

```rust
    pub kind: ColumnKind,
    /// Write a Parquet bloom filter for this column (schema attr
    /// `"bloom_filter": true`). Orthogonal to `kind`, but invalid on alias
    /// columns — they are never stored. Default false.
    pub bloom_filter: bool,
```

2. In `TableColumns::parse`, replace the block from the `let kind = match (` statement through `specs.push(ColumnSpec { ... });` with:

```rust
            let kind = match (
                expr_of("default"),
                expr_of("materialized"),
                expr_of("alias"),
            ) {
                (None, None, None) => ColumnKind::Stored,
                (Some(e), None, None) => ColumnKind::Default(e),
                (None, Some(e), None) => ColumnKind::Materialized(e),
                (None, None, Some(e)) => ColumnKind::Alias(e),
                _ => {
                    return Err(SchemaError::Invalid(format!(
                        "field '{name}': default/materialized/alias are mutually exclusive"
                    )));
                }
            };
            let bloom_filter = f
                .get("bloom_filter")
                .and_then(|b| b.as_bool())
                .unwrap_or(false);
            if bloom_filter && matches!(kind, ColumnKind::Alias(_)) {
                return Err(SchemaError::Invalid(format!(
                    "field '{name}': bloom_filter is invalid on alias columns (never stored)"
                )));
            }
            specs.push(ColumnSpec {
                name: name.to_string(),
                data_type,
                nullable,
                kind,
                bloom_filter,
                ukiel_type: type_name.to_string(),
            });
```

3. Add the accessor to `impl TableColumns` (after `query_schema`):

```rust
    /// Stored columns (stored/default/materialized) flagged for bloom
    /// filters, in schema order.
    pub fn bloom_columns(&self) -> Vec<String> {
        self.specs
            .iter()
            .filter(|s| s.bloom_filter && !matches!(s.kind, ColumnKind::Alias(_)))
            .map(|s| s.name.clone())
            .collect()
    }
```

- [ ] **Step 5: Verify the schema tests pass**

Run: `cargo test -p ukiel-core`
Expected: all pass (existing schema tests included — the attribute defaults to false everywhere).

- [ ] **Step 6: Create `parquet_props.rs` with tests and a stub**

Create `crates/ukiel-core/src/parquet_props.rs` — full file, with `writer_props` as a deliberate stub so the tests fail first:

```rust
//! Shared Parquet writer properties: the ONE place encoding Ukiel's
//! write-side layout policy (perf note Tier 3): compression per LSM level,
//! declared sort metadata, delta-packed timestamps, opt-in bloom filters,
//! row-group sizing. Both writers (ingest L0, compactor rewrites) build
//! their `WriterProperties` here.

use parquet::basic::{Compression, Encoding, ZstdLevel};
use parquet::file::properties::WriterProperties;
use parquet::format::SortingColumn;
use parquet::schema::types::ColumnPath;

/// Default row-group cap (perf note Tier-3 #8): large enough to amortize
/// metadata, small enough to prune.
pub const DEFAULT_MAX_ROW_GROUP_ROWS: usize = 128 * 1024;

/// Write-side layout options for one output file.
#[derive(Debug, Clone)]
pub struct WriteOpts {
    /// Output LSM level. Level 0 gets LZ4_RAW (3-5x faster decode; L0 files
    /// are short-lived), level >= 1 gets ZSTD (read-many, space wins).
    pub level: i16,
    /// Indices (into the file schema) of the columns the data is sorted by,
    /// in order. Written as Parquet `sorting_columns` metadata
    /// (ascending, nulls last — matching how the writers sort).
    pub sort_columns: Vec<usize>,
    /// The timestamp column, written DELTA_BINARY_PACKED with dictionary
    /// disabled: sorted epoch-ms delta-packs spectacularly; dictionary on a
    /// near-unique column only adds indirection. Name-based because the
    /// parquet builder keys per-column overrides by `ColumnPath`, not index.
    pub ts_column: Option<String>,
    /// Columns to write bloom filters for (schema attr `"bloom_filter"`).
    pub bloom_columns: Vec<String>,
    /// Row-group row cap (`set_max_row_group_size`).
    pub max_row_group_rows: usize,
}

impl WriteOpts {
    /// Options for a plain write at `level` with no layout hints.
    pub fn for_level(level: i16) -> Self {
        Self {
            level,
            sort_columns: Vec::new(),
            ts_column: None,
            bloom_columns: Vec::new(),
            max_row_group_rows: DEFAULT_MAX_ROW_GROUP_ROWS,
        }
    }
}

/// Builds `WriterProperties` implementing the layout policy in `opts`.
pub fn writer_props(opts: &WriteOpts) -> WriterProperties {
    let _ = opts;
    WriterProperties::builder().build() // stub: replaced in the next step
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow::array::{Int64Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use bytes::Bytes;
    use parquet::arrow::ArrowWriter;
    use parquet::file::reader::{FileReader, SerializedFileReader};

    /// Writes `rows` rows of (tenant_id, ts, payload) with `opts` and
    /// returns the file bytes for footer inspection.
    fn sample_bytes(opts: &WriteOpts, rows: usize) -> Bytes {
        let schema = Arc::new(Schema::new(vec![
            Field::new("tenant_id", DataType::Int64, false),
            Field::new("ts", DataType::Int64, false),
            Field::new("payload", DataType::Utf8, true),
        ]));
        let tenants = Int64Array::from_iter_values((0..rows as i64).map(|i| i / 4));
        let ts = Int64Array::from_iter_values(0..rows as i64);
        let payload: StringArray = (0..rows).map(|i| Some(format!("p{i}"))).collect();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(tenants), Arc::new(ts), Arc::new(payload)],
        )
        .unwrap();
        let mut bytes = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut bytes, schema, Some(writer_props(opts))).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        Bytes::from(bytes)
    }

    #[test]
    fn level_zero_writes_lz4_raw_and_level_one_zstd() {
        let l0 = SerializedFileReader::new(sample_bytes(&WriteOpts::for_level(0), 8)).unwrap();
        assert_eq!(
            l0.metadata().row_group(0).column(0).compression(),
            Compression::LZ4_RAW
        );

        let l1 = SerializedFileReader::new(sample_bytes(&WriteOpts::for_level(1), 8)).unwrap();
        assert!(matches!(
            l1.metadata().row_group(0).column(0).compression(),
            Compression::ZSTD(_)
        ));
    }

    #[test]
    fn ts_column_is_delta_packed_without_dictionary() {
        let mut opts = WriteOpts::for_level(1);
        opts.ts_column = Some("ts".to_string());
        let reader = SerializedFileReader::new(sample_bytes(&opts, 8)).unwrap();
        let rg = reader.metadata().row_group(0);

        let encodings: Vec<Encoding> = rg.column(1).encodings().iter().copied().collect();
        assert!(
            encodings.contains(&Encoding::DELTA_BINARY_PACKED),
            "ts encodings: {encodings:?}"
        );
        assert!(
            rg.column(1).dictionary_page_offset().is_none(),
            "ts must not be dictionary-encoded"
        );
        // Other columns keep dictionary encoding (utf8 payload).
        assert!(rg.column(2).dictionary_page_offset().is_some());
    }

    #[test]
    fn sorting_columns_bloom_filters_and_row_group_cap() {
        let mut opts = WriteOpts::for_level(1);
        opts.sort_columns = vec![0, 1];
        opts.bloom_columns = vec!["payload".to_string()];
        opts.max_row_group_rows = 4;
        let reader = SerializedFileReader::new(sample_bytes(&opts, 10)).unwrap();
        let meta = reader.metadata();

        // 10 rows / cap 4 -> row groups of 4, 4, 2.
        assert_eq!(meta.num_row_groups(), 3);
        let rows: Vec<i64> = meta.row_groups().iter().map(|rg| rg.num_rows()).collect();
        assert_eq!(rows, vec![4, 4, 2]);

        let rg = meta.row_group(0);
        let sorting = rg.sorting_columns().expect("sorting_columns metadata");
        assert_eq!(sorting.len(), 2);
        assert_eq!(
            (sorting[0].column_idx, sorting[0].descending, sorting[0].nulls_first),
            (0, false, false)
        );
        assert_eq!(
            (sorting[1].column_idx, sorting[1].descending, sorting[1].nulls_first),
            (1, false, false)
        );

        // Bloom filter present exactly where asked.
        assert!(rg.column(2).bloom_filter_offset().is_some(), "payload bloom");
        assert!(rg.column(0).bloom_filter_offset().is_none());
        assert!(rg.column(1).bloom_filter_offset().is_none());
    }
}
```

Register the module in `crates/ukiel-core/src/lib.rs` — add after `pub mod part;`:

```rust
pub mod parquet_props;
```

and extend the re-exports (after the `pub use part::...` line):

```rust
pub use parquet_props::{DEFAULT_MAX_ROW_GROUP_ROWS, WriteOpts, writer_props};
```

- [ ] **Step 7: Verify the new tests fail**

Run: `cargo test -p ukiel-core parquet_props`
Expected: compiles, all three tests FAIL (stub props: ZSTD-less default compression, no sorting metadata, no blooms, default row-group size).

- [ ] **Step 8: Implement `writer_props`**

Replace the stub body in `crates/ukiel-core/src/parquet_props.rs`:

```rust
/// Builds `WriterProperties` implementing the layout policy in `opts`.
pub fn writer_props(opts: &WriteOpts) -> WriterProperties {
    let compression = if opts.level == 0 {
        Compression::LZ4_RAW
    } else {
        Compression::ZSTD(ZstdLevel::default())
    };
    let mut builder = WriterProperties::builder()
        .set_compression(compression)
        .set_max_row_group_size(opts.max_row_group_rows);
    if !opts.sort_columns.is_empty() {
        let sorting: Vec<SortingColumn> = opts
            .sort_columns
            .iter()
            .map(|&idx| SortingColumn {
                column_idx: idx as i32,
                descending: false,
                nulls_first: false,
            })
            .collect();
        builder = builder.set_sorting_columns(Some(sorting));
    }
    if let Some(ts) = &opts.ts_column {
        let path = ColumnPath::new(vec![ts.clone()]);
        builder = builder
            .set_column_encoding(path.clone(), Encoding::DELTA_BINARY_PACKED)
            .set_column_dictionary_enabled(path, false);
    }
    for name in &opts.bloom_columns {
        builder =
            builder.set_column_bloom_filter_enabled(ColumnPath::new(vec![name.clone()]), true);
    }
    builder.build()
}
```

(Remove the `let _ = opts;` line and the stub comment.)

- [ ] **Step 9: Verify pass**

Run: `cargo test -p ukiel-core && cargo build --workspace`
Expected: all ukiel-core tests pass; the workspace still builds (nothing else consumes the new API yet, and `ColumnSpec` is only constructed inside `schema.rs`).

- [ ] **Step 10: Lint and commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add crates/ukiel-core
git commit -m "feat: shared parquet writer properties and bloom_filter column attribute"
```

---

### Task 2: Ingest L0 writer uses the shared layout

**Files:**
- Modify: `crates/ukiel-ingest/src/writer.rs`

**Interfaces:**
- Changes `finish_batch(batch, packing_key)` to `finish_batch(batch, packing_key, ts_column, bloom_columns)`; L0 files get LZ4_RAW, `(packing_key, ts)` sorting metadata, delta-packed `ts`, blooms where the schema asks. `rows_to_parquet` / `encode_rows` public signatures unchanged. No Docker needed (`--lib` tests).

- [ ] **Step 1: Write the failing footer test**

Append inside `mod tests` in `crates/ukiel-ingest/src/writer.rs`:

```rust
    #[test]
    fn l0_layout_lz4_delta_ts_sorting_and_blooms() {
        use parquet::basic::{Compression, Encoding};
        use parquet::file::reader::{FileReader, SerializedFileReader};

        let cols = ukiel_core::TableColumns::parse(&json!({"fields": [
            {"name": "tenant_id", "type": "int64"},
            {"name": "ts", "type": "timestamp_ms"},
            {"name": "payload", "type": "utf8", "bloom_filter": true}
        ]}))
        .unwrap();
        let rows = vec![
            json!({"tenant_id": 2, "ts": 20, "payload": "b"}),
            json!({"tenant_id": 1, "ts": 10, "payload": "a"}),
            json!({"tenant_id": 1, "ts": 30, "payload": "c"}),
        ];
        let part = encode_rows(&cols, "tenant_id", "ts", rows).unwrap();

        let reader = SerializedFileReader::new(Bytes::from(part.bytes)).unwrap();
        let rg = reader.metadata().row_group(0);

        // L0 compression: LZ4_RAW on every chunk (short-lived, decode-fast).
        for i in 0..rg.num_columns() {
            assert_eq!(rg.column(i).compression(), Compression::LZ4_RAW, "column {i}");
        }
        // ts (idx 1): delta-packed, dictionary off.
        let encodings: Vec<Encoding> = rg.column(1).encodings().iter().copied().collect();
        assert!(
            encodings.contains(&Encoding::DELTA_BINARY_PACKED),
            "ts encodings: {encodings:?}"
        );
        assert!(rg.column(1).dictionary_page_offset().is_none());
        // Declared sort: (tenant_id, ts) ascending, nulls last.
        let sorting = rg.sorting_columns().expect("sorting_columns metadata");
        assert_eq!(sorting.len(), 2);
        assert_eq!(
            (sorting[0].column_idx, sorting[0].descending, sorting[0].nulls_first),
            (0, false, false)
        );
        assert_eq!(
            (sorting[1].column_idx, sorting[1].descending, sorting[1].nulls_first),
            (1, false, false)
        );
        // Bloom filter only where the schema asked.
        assert!(rg.column(2).bloom_filter_offset().is_some(), "payload bloom");
        assert!(rg.column(0).bloom_filter_offset().is_none());
        assert!(rg.column(1).bloom_filter_offset().is_none());
    }
```

- [ ] **Step 2: Verify it fails**

Run: `cargo test -p ukiel-ingest --lib`
Expected: `l0_layout_lz4_delta_ts_sorting_and_blooms` FAILS on the compression assertion (current writer is ZSTD, no sorting metadata, no blooms). All other writer tests still pass.

- [ ] **Step 3: Implement**

In `crates/ukiel-ingest/src/writer.rs`:

1. Replace the parquet/core imports at the top:

```rust
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use ukiel_core::TableColumns;
```

with:

```rust
use parquet::arrow::ArrowWriter;
use ukiel_core::{DEFAULT_MAX_ROW_GROUP_ROWS, TableColumns, WriteOpts, writer_props};
```

(Update the module doc comment's first line to: `//! Encodes buffered JSON rows into a sorted Parquet file with the shared write-side layout (ukiel_core::parquet_props).`)

2. Update the two callers:

In `rows_to_parquet`, change the last line to:

```rust
    finish_batch(batch, packing_key, ts_column, Vec::new())
```

In `encode_rows`, change the last line to:

```rust
    finish_batch(batch, packing_key, ts_column, cols.bloom_columns())
```

3. Replace `finish_batch` entirely with:

```rust
/// Computes the packing-key range from the batch and writes L0 Parquet with
/// the shared layout: LZ4_RAW (L0 files are short-lived and decode-heavy),
/// declared (packing_key, ts) sort, delta-packed ts, opt-in bloom filters.
fn finish_batch(
    batch: RecordBatch,
    packing_key: &str,
    ts_column: &str,
    bloom_columns: Vec<String>,
) -> Result<EncodedPart, IngestError> {
    let missing = |column: &str| IngestError::MissingColumn {
        row: 0,
        column: column.to_string(),
    };
    let key_idx = batch
        .schema()
        .index_of(packing_key)
        .map_err(|_| missing(packing_key))?;
    let ts_idx = batch
        .schema()
        .index_of(ts_column)
        .map_err(|_| missing(ts_column))?;
    let keys = batch
        .column(key_idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| missing(packing_key))?;
    let key_min = arrow::compute::min(keys).ok_or(IngestError::EmptyFlush)?;
    let key_max = arrow::compute::max(keys).ok_or(IngestError::EmptyFlush)?;

    let opts = WriteOpts {
        level: 0,
        sort_columns: vec![key_idx, ts_idx],
        ts_column: Some(ts_column.to_string()),
        bloom_columns,
        max_row_group_rows: DEFAULT_MAX_ROW_GROUP_ROWS,
    };
    let mut bytes = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut bytes, batch.schema(), Some(writer_props(&opts)))?;
    writer.write(&batch)?;
    writer.close()?;

    Ok(EncodedPart {
        bytes,
        row_count: batch.num_rows() as i64,
        key_min,
        key_max,
    })
}
```

(If plan 11 Task 4 already landed: `finish_batch` will already take `ts_column` and build ad-hoc sorting props — replace that whole body with the above; the `sorting_columns` metadata plan 11 added is preserved via `WriteOpts.sort_columns`.)

- [ ] **Step 4: Verify pass**

Run: `cargo test -p ukiel-ingest --lib && cargo test -p ukiel-compactor --lib`
Expected: all pass — including `sorts_rows_and_reports_key_range` and `encode_rows_fills_defaults_and_materialized` (readers decode LZ4_RAW transparently) and the compactor's `merge_sort_split_round_trip` (it reads L0 files written by `rows_to_parquet`).

- [ ] **Step 5: Lint and commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add crates/ukiel-ingest
git commit -m "feat: ingest L0 layout via shared props - lz4_raw, delta ts, sort metadata, blooms"
```

---

### Task 3: Compactor — shared L1 props + key-aligned row groups

**Files:**
- Modify: `crates/ukiel-compactor/src/rewrite.rs`
- Modify: `crates/ukiel-compactor/src/compactor.rs` (`merge_group`)
- Modify: `crates/ukiel-compactor/src/deletion.rs` (`delete_key`)

**Interfaces:**
- Produces: `rewrite::write_opts(cols, sort_key, level) -> WriteOpts` (sort indices from the physical schema, ts = first `timestamp_ms` sort-key column, blooms from the schema).
- Changes: `rewrite::batch_to_parquet(batch, props: WriterProperties)`.
- Produces: `rewrite::batch_to_parquet_key_aligned(batch, packing_key, props) -> Result<Vec<u8>, CompactorError>` — ONE output file whose row-group boundaries land on packing-key boundaries whenever a key fits under the cap (perf note #7). Used by `merge_group` for **packed** placement (separated output is one key per file already — plain `batch_to_parquet`) and by `delete_key` rewrites (packed files by definition).

- [ ] **Step 1: Write the failing layout test**

Append inside `mod tests` in `crates/ukiel-compactor/src/rewrite.rs`:

```rust
    #[test]
    fn key_aligned_row_groups_land_on_key_boundaries() {
        use parquet::basic::{Compression, Encoding};
        use parquet::file::reader::{FileReader, SerializedFileReader};
        use parquet::file::statistics::Statistics;

        // Uneven key runs: 1 x3, 2 x3, 3 x6, 4 x1 rows; row-group cap 4.
        // Expected grouping: [k1 x3] | [k2 x3] | [k3 x4] | [k3 x2, k4 x1]
        //  - k1 doesn't fit with k2 (3+3 > 4): flush on the key boundary.
        //  - k3 (6 rows) exceeds the cap alone: ArrowWriter splits it at 4.
        //  - k4 (tiny) shares the last group with k3's tail (2+1 <= 4).
        let arrow_schema = Arc::new(arrow_schema_from_json(&schema_json()).unwrap());
        let mut tenants: Vec<i64> = Vec::new();
        let mut ts: Vec<i64> = Vec::new();
        for (key, n) in [(1i64, 3usize), (2, 3), (3, 6), (4, 1)] {
            for i in 0..n {
                tenants.push(key);
                ts.push(i as i64);
            }
        }
        let payload: arrow::array::StringArray =
            tenants.iter().map(|k| Some(format!("p{k}"))).collect();
        let batch = RecordBatch::try_new(
            arrow_schema,
            vec![
                Arc::new(Int64Array::from(tenants)),
                Arc::new(Int64Array::from(ts)),
                Arc::new(payload),
            ],
        )
        .unwrap();

        let cols = ukiel_core::TableColumns::parse(&schema_json()).unwrap();
        let mut opts = write_opts(&cols, &["tenant_id".to_string(), "ts".to_string()], 1);
        opts.max_row_group_rows = 4;
        let bytes =
            batch_to_parquet_key_aligned(&batch, "tenant_id", ukiel_core::writer_props(&opts))
                .unwrap();

        let reader = SerializedFileReader::new(bytes::Bytes::from(bytes)).unwrap();
        let meta = reader.metadata();

        let rows: Vec<i64> = meta.row_groups().iter().map(|rg| rg.num_rows()).collect();
        assert_eq!(rows, vec![3, 3, 4, 3]);

        // Per-row-group packing-key min/max stats (column 0 = tenant_id).
        let key_stats = |i: usize| -> (i64, i64) {
            match meta.row_group(i).column(0).statistics().expect("key stats") {
                Statistics::Int64(s) => (*s.min_opt().unwrap(), *s.max_opt().unwrap()),
                other => panic!("expected int64 stats, got {other:?}"),
            }
        };
        let ranges: Vec<(i64, i64)> = (0..meta.num_row_groups()).map(key_stats).collect();
        // Keys 1 and 2 each own a whole row group (min == max); key 3 spans
        // groups only because it exceeds the cap alone; key 4 rides along.
        assert_eq!(ranges, vec![(1, 1), (2, 2), (3, 3), (3, 4)]);
        // No straddling: consecutive groups' key ranges never interleave.
        for w in ranges.windows(2) {
            assert!(w[0].1 <= w[1].0, "row-group key ranges out of order: {ranges:?}");
        }

        // L1 layout from the shared props: ZSTD + delta-packed ts + sort metadata.
        let rg = meta.row_group(0);
        assert!(matches!(rg.column(0).compression(), Compression::ZSTD(_)));
        let encodings: Vec<Encoding> = rg.column(1).encodings().iter().copied().collect();
        assert!(
            encodings.contains(&Encoding::DELTA_BINARY_PACKED),
            "ts encodings: {encodings:?}"
        );
        let sorting = rg.sorting_columns().expect("sorting_columns metadata");
        assert_eq!(sorting.len(), 2);
        assert_eq!((sorting[0].column_idx, sorting[1].column_idx), (0, 1));
    }
```

- [ ] **Step 2: Verify it fails**

Run: `cargo test -p ukiel-compactor --lib`
Expected: compile error (`write_opts` and `batch_to_parquet_key_aligned` don't exist).

- [ ] **Step 3: Implement the rewrite engine changes**

In `crates/ukiel-compactor/src/rewrite.rs`:

1. Replace the imports

```rust
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use ukiel_core::Part;
```

with:

```rust
use parquet::file::properties::WriterProperties;
use ukiel_core::{DEFAULT_MAX_ROW_GROUP_ROWS, Part, TableColumns, WriteOpts};
```

2. Replace the whole `batch_to_parquet` function (and its doc comment) with:

```rust
/// The shared write options for a rewrite output at `level`: sorted by the
/// hypertable sort key, first timestamp_ms sort column delta-packed, blooms
/// per the table schema, default row-group cap.
pub fn write_opts(cols: &TableColumns, sort_key: &[String], level: i16) -> WriteOpts {
    let physical = cols.physical_schema();
    let sort_columns = sort_key
        .iter()
        .filter_map(|n| physical.index_of(n).ok())
        .collect();
    let ts_column = sort_key
        .iter()
        .find(|n| {
            cols.specs
                .iter()
                .any(|s| &s.name == *n && s.ukiel_type == "timestamp_ms")
        })
        .cloned();
    WriteOpts {
        level,
        sort_columns,
        ts_column,
        bloom_columns: cols.bloom_columns(),
        max_row_group_rows: DEFAULT_MAX_ROW_GROUP_ROWS,
    }
}

/// Parquet bytes for one batch with the given writer properties.
pub fn batch_to_parquet(
    batch: &RecordBatch,
    props: WriterProperties,
) -> Result<Vec<u8>, CompactorError> {
    let mut bytes = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut bytes, batch.schema(), Some(props))?;
    writer.write(batch)?;
    writer.close()?;
    Ok(bytes)
}

/// Parquet bytes for a batch whose primary sort column is `packing_key`,
/// with row-group boundaries aligned to key boundaries (perf note #7): the
/// per-key slices from `split_by_key` are streamed into ONE writer, and the
/// current row group is closed before any key run that would overflow the
/// cap. Tiny keys share a row group; a key larger than the cap spans
/// several (ArrowWriter splits it at max_row_group_size on its own). The
/// result: row-group key min/max stats are perfectly selective for the
/// isolation predicate whenever a key fits.
pub fn batch_to_parquet_key_aligned(
    batch: &RecordBatch,
    packing_key: &str,
    props: WriterProperties,
) -> Result<Vec<u8>, CompactorError> {
    let max_rows = props.max_row_group_size();
    let mut bytes = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut bytes, batch.schema(), Some(props))?;
    for (_, slice) in split_by_key(batch, packing_key)? {
        let in_progress = writer.in_progress_rows();
        if in_progress > 0 && in_progress + slice.num_rows() > max_rows {
            writer.flush()?; // end the row group on the key boundary
        }
        writer.write(&slice)?;
    }
    writer.close()?;
    Ok(bytes)
}
```

(`ArrowWriter::flush` ends the in-progress row group; `in_progress_rows` reports its buffered row count. If either name differs in the pinned parquet version, check docs.rs and adapt minimally.)

3. In the existing `merge_sort_split_round_trip` test, replace the line

```rust
        let bytes = batch_to_parquet(&splits[2].1).unwrap();
```

with:

```rust
        let cols = ukiel_core::TableColumns::parse(&schema_json()).unwrap();
        let opts = write_opts(&cols, &["tenant_id".to_string(), "ts".to_string()], 1);
        let bytes =
            batch_to_parquet(&splits[2].1, ukiel_core::writer_props(&opts)).unwrap();
```

(If plan 11 Task 4 already landed, `batch_to_parquet` takes `(batch, sort_key)` and builds ad-hoc sorting props — replace with the shapes above; its callers and test are updated the same way as below.)

- [ ] **Step 4: Update the two callers**

In `crates/ukiel-compactor/src/compactor.rs` `merge_group`, replace the block

```rust
        let mut new_parts = Vec::with_capacity(outputs.len());
        for (key_min, key_max, batch) in &outputs {
            let bytes = rewrite::batch_to_parquet(batch)?;
```

with:

```rust
        let opts = rewrite::write_opts(&cols, &hypertable.sort_key, 1);
        let mut new_parts = Vec::with_capacity(outputs.len());
        for (key_min, key_max, batch) in &outputs {
            let props = ukiel_core::writer_props(&opts);
            let bytes = match hypertable.placement {
                // Packed output carries many keys: align row groups to key
                // runs so row-group stats isolate tenants.
                Placement::Packed => rewrite::batch_to_parquet_key_aligned(
                    batch,
                    &hypertable.packing_key,
                    props,
                )?,
                // Separated output is a single key per file already.
                Placement::Separated => rewrite::batch_to_parquet(batch, props)?,
            };
```

In `crates/ukiel-compactor/src/deletion.rs` `delete_key`, replace

```rust
        let bytes = rewrite::batch_to_parquet(&kept)?;
```

with:

```rust
        // Rewritten packed files keep the level they had and stay
        // key-aligned (filtering preserves the (key, ts) sort).
        let opts = rewrite::write_opts(&cols, &hypertable.sort_key, part.meta.level);
        let bytes = rewrite::batch_to_parquet_key_aligned(
            &kept,
            &hypertable.packing_key,
            ukiel_core::writer_props(&opts),
        )?;
```

- [ ] **Step 5: Verify pass (unit, then full component suite)**

Run: `cargo test -p ukiel-compactor --lib`
Expected: `key_aligned_row_groups_land_on_key_boundaries` and `merge_sort_split_round_trip` pass.

Run (Docker required): `cargo test -p ukiel-compactor -- --test-threads=2 && cargo test -p ukiel-query -- --test-threads=2`
Expected: all compactor component tests (merge, races, deletion) and all query tests green — readers are layout-agnostic, and the query isolation tests double as proof the rewritten files still carry every tenant's rows correctly.

- [ ] **Step 6: Lint and commit**

```bash
cargo fmt && cargo clippy --all-targets -- -D warnings
git add crates/ukiel-compactor
git commit -m "feat: key-aligned row groups and shared writer props in compactor rewrites"
```

---

### Task 4: Docs — perf-note Tier-3 status + roadmap row 14

**Files:**
- Modify: `docs/notes/2026-07-06-parquet-read-performance.md`
- Modify: `docs/superpowers/plans/2026-07-05-ukiel-v1-roadmap.md`

- [ ] **Step 1: Full verification first**

Run: `cargo fmt --check && cargo clippy --all-targets -- -D warnings && make test`
Expected: all green. Optionally `make e2e` (write-path changes touch every scenario's data — cheap insurance).

- [ ] **Step 2: Update the perf note**

In `docs/notes/2026-07-06-parquet-read-performance.md`, replace the heading

```
## Tier 3 — write-side layout & encoding (rides the compactor)
```

with:

```
## Tier 3 — write-side layout & encoding *(plan 14)* (rides the compactor)
```

and append directly after item 10 (before the `## Tier 4` heading):

```
Plan-14 status: #7 done for packed compaction output
(`rewrite::batch_to_parquet_key_aligned` — row groups end on key boundaries
up to the cap; a key bigger than the cap spans several groups; separated
placement needs no alignment). #8 done (`DEFAULT_MAX_ROW_GROUP_ROWS =
128 * 1024` on every writer). #9 done (ts = `DELTA_BINARY_PACKED` with
dictionary off; L0 = LZ4_RAW, L1+ = ZSTD — both writers). #10 done (schema
attr `"bloom_filter": true`, rejected on alias columns;
`TableColumns::bloom_columns()` feeds both writers). All layout policy lives
in `ukiel-core::parquet_props` (`WriteOpts` / `writer_props`).
```

- [ ] **Step 3: Update roadmap row 14**

In `docs/superpowers/plans/2026-07-05-ukiel-v1-roadmap.md`, plan 14's row already exists — set its status column to **Executed**.

- [ ] **Step 4: Commit**

```bash
git add docs/
git commit -m "docs: record tier-3 parquet layout status, roadmap row 14 executed"
```

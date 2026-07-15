//! The `from-files` adapter: freeze an explicit, sorted local Parquet file list.
//!
//! Fully offline — no catalog, object store, or service of any kind. It requires an
//! explicit declared schema and an explicit sorted file list; it never lists a directory
//! implicitly, because a snapshot is a *chosen* set of bytes, not whatever happened to be
//! in a folder. This adapter exists for the bounded ClickBench slice and for anyone using
//! the tools outside Ukiel.

use std::path::Path;

use anyhow::{Context, Result, bail};
use parquet_lab_contract::{
    FileDigest, LogicalProjection, SNAPSHOT_MANIFEST_VERSION, SnapshotFile, SnapshotManifest,
    SourceKind,
};
use parquet_lab_integrity::{LogicalColumn, LogicalSchema, LogicalType};
use serde::Deserialize;

use crate::{PendingFile, SnapshotBuilder};

/// The declared logical schema file: an ordered list matching the physical Parquet
/// column order, each with a logical type name `LogicalType::parse` accepts.
#[derive(Debug, Deserialize)]
struct LogicalSchemaFile {
    columns: Vec<LogicalColumnDecl>,
}

#[derive(Debug, Deserialize)]
struct LogicalColumnDecl {
    name: String,
    logical: String,
}

fn parse_logical_schema(json: &[u8], path: &str) -> Result<(LogicalSchema, LogicalProjection)> {
    let decl: LogicalSchemaFile =
        serde_json::from_slice(json).with_context(|| format!("parsing logical schema {path}"))?;
    if decl.columns.is_empty() {
        bail!("{path}: logical schema declares no columns");
    }
    let mut columns = Vec::with_capacity(decl.columns.len());
    let mut proj = serde_json::Map::new();
    for c in &decl.columns {
        let logical = LogicalType::parse(&c.logical)
            .map_err(|e| anyhow::anyhow!("{path}: column '{}': {e}", c.name))?;
        columns.push(LogicalColumn {
            name: c.name.clone(),
            logical,
        });
        proj.insert(c.name.clone(), serde_json::json!(c.logical));
    }
    Ok((
        LogicalSchema::new(columns),
        LogicalProjection {
            logical_types: proj,
        },
    ))
}

/// Read a `--files-from` list: one path per non-empty, non-comment line, in order. The
/// order is the caller's declared sort order and is preserved.
fn read_file_list(path: &Path) -> Result<Vec<std::path::PathBuf>> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("reading file list {}", path.display()))?;
    let mut files = Vec::new();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        files.push(std::path::PathBuf::from(line));
    }
    if files.is_empty() {
        bail!("{}: file list is empty", path.display());
    }
    Ok(files)
}

/// Freeze an explicit file list into a snapshot.
pub fn freeze(
    schema_file: &Path,
    files_from: &Path,
    logical_schema_file: &Path,
    output_dir: &Path,
    creation_command: String,
    replace: bool,
) -> Result<std::path::PathBuf> {
    let declared_schema: serde_json::Value = serde_json::from_slice(
        &std::fs::read(schema_file)
            .with_context(|| format!("reading {}", schema_file.display()))?,
    )
    .with_context(|| format!("parsing {}", schema_file.display()))?;

    let logical_bytes = std::fs::read(logical_schema_file)
        .with_context(|| format!("reading {}", logical_schema_file.display()))?;
    let (logical_schema, projection) =
        parse_logical_schema(&logical_bytes, &logical_schema_file.display().to_string())?;

    let list = read_file_list(files_from)?;

    let mut builder =
        SnapshotBuilder::new(output_dir, logical_schema, /* want_physical */ false);
    builder.begin(replace)?;

    let mut seen = std::collections::HashSet::new();
    for (i, src) in list.iter().enumerate() {
        if !seen.insert(src.clone()) {
            bail!("{}: appears more than once in the file list", src.display());
        }
        let bytes = std::fs::read(src).with_context(|| format!("reading {}", src.display()))?;
        let basename = src
            .file_name()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_else(|| format!("file-{i}"));
        let pending = PendingFile {
            rel_path: format!("parquet/{i:05}-{basename}"),
            object_key: None,
            source_part: Some(src.display().to_string()),
            expect_rows: None,
            expect_bytes: None,
        };
        builder.add_file(&pending, &bytes)?;
    }

    let logical_mirror = crate::logical_fingerprint(builder.logical());
    let files: Vec<SnapshotFile> = builder.files().to_vec();
    let physical_schema = builder.physical_schema_json();
    let total_rows = builder.total_rows();
    let total_bytes: u64 = files.iter().map(|f| f.bytes).sum();

    let manifest = SnapshotManifest {
        manifest_version: SNAPSHOT_MANIFEST_VERSION.to_string(),
        source_kind: SourceKind::ExplicitFiles,
        source_digests: vec![FileDigest::of(
            schema_file.display().to_string(),
            &std::fs::read(schema_file)?,
        )],
        tool_versions: crate::tool_versions(),
        creation_command,
        logical_schema: declared_schema,
        physical_schema,
        // An explicit dataset declares no packing/sort key; the matrix that needs one
        // supplies a predicate against a named column, not a stored key policy.
        packing_key: String::new(),
        sort_key: Vec::new(),
        logical_projection: Some(projection),
        files,
        total_rows,
        total_bytes,
        // No physical `row-multiset/v1`: there is no plan-46 receipt to compare against,
        // and an explicit dataset's physical schema is not the Ukiel one.
        physical_fingerprint: None,
        logical_fingerprint: logical_mirror,
        // A non-synthetic dataset carries no synthetic disclaimer.
        disclaimer: None,
    };
    builder.finish(manifest)
}

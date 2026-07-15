//! `parquet-lab-snapshot` — freeze an explicit Parquet file set into a verified,
//! immutable laboratory snapshot.
//!
//! A snapshot is the product control for the whole plan-47 matrix: the *exact bytes*
//! a source produced, copied unchanged, with everything a downstream tool needs to
//! prove it is reading those bytes and nothing else. Three subcommands:
//!
//! * `from-ukiel` — the only service-aware adapter. It revalidates a plan-46 receipt
//!   against a live catalog and object store, downloads each converged final object
//!   unchanged, and publishes atomically only after local digests equal the bytes it
//!   downloaded. It compacts, vacuums, queries, and mutates nothing.
//! * `from-files` — an offline adapter over an explicit schema and sorted file list.
//!   It never lists a directory implicitly; it exists for the bounded ClickBench
//!   slice and for anyone using these tools outside Ukiel.
//! * `verify` — re-checks a published snapshot against its manifest, offline.
//!
//! The canonical logical encoder lives in `parquet-lab-integrity`; the physical
//! `row-multiset/v1` lives in `prod-synth-integrity`. This crate computes both while
//! it copies, so a `from-ukiel` snapshot can be checked against a plan-46 receipt's
//! physical fingerprint *and* carry the logical fingerprint every variant reproduces.

pub mod from_files;
pub mod from_ukiel;

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet_lab_contract::{
    Digest, FileDigest, Fingerprint, SnapshotFile, SnapshotManifest, ToolVersions, digest_bytes,
};
use parquet_lab_integrity::{LogicalRowMultiset, LogicalSchema};
use prod_synth_integrity::RowMultiset;

/// The pinned toolchain, stamped into every snapshot. `git_sha` is read from the
/// environment when present (a CI build sets it) and is otherwise `unknown` — never a
/// fabricated value.
pub fn tool_versions() -> ToolVersions {
    ToolVersions {
        git_sha: std::env::var("UKIEL_GIT_SHA").unwrap_or_else(|_| "unknown".to_string()),
        arrow: "58.3".to_string(),
        parquet: "58.3".to_string(),
        datafusion: "54".to_string(),
    }
}

fn hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

/// A physical `row-multiset/v1` accumulator as the contract mirror. Comparable to a
/// plan-46 receipt's fingerprint field.
pub fn physical_fingerprint(m: &RowMultiset) -> Fingerprint {
    Fingerprint {
        version: prod_synth_integrity::ROW_MULTISET_VERSION.to_string(),
        count: m.count,
        xor: hex(&m.xor),
        sum: m.sum,
        digest: m.digest_hex(),
    }
}

/// A `logical-row-multiset/v1` accumulator as the contract mirror. The invariant every
/// variant must reproduce.
pub fn logical_fingerprint(m: &LogicalRowMultiset) -> Fingerprint {
    Fingerprint {
        version: parquet_lab_integrity::LOGICAL_ROW_MULTISET_VERSION.to_string(),
        count: m.count,
        xor: m.xor_hex(),
        sum: m.sum,
        digest: m.digest_hex(),
    }
}

/// One file to be frozen, before it is read. `bytes` is fetched lazily by the builder
/// so a whole part is never held longer than the one being processed.
pub struct PendingFile {
    /// The path this file will occupy inside the snapshot, relative to the manifest.
    pub rel_path: String,
    /// The original object-store key, when frozen from Ukiel.
    pub object_key: Option<String>,
    /// The source part identity (catalog part id, or explicit list index).
    pub source_part: Option<String>,
    /// The row and byte census the catalog claims, cross-checked against the object.
    /// `None` for an explicit file list with no external census.
    pub expect_rows: Option<u64>,
    pub expect_bytes: Option<u64>,
}

/// Accumulates frozen files into a temporary directory and publishes atomically.
///
/// One file is read at a time: its Parquet metadata gives rows/row-groups/schema, its
/// batches feed the physical and logical fingerprints, and its bytes are written to the
/// temp tree. Nothing is published until every file has closed and (for `from-ukiel`)
/// the physical fingerprint has been checked against the receipt.
pub struct SnapshotBuilder {
    output_dir: PathBuf,
    tmp_dir: PathBuf,
    logical_schema: LogicalSchema,
    /// The physical fingerprint, computed only when a receipt will be checked against
    /// it. `from-files` leaves it `None`.
    physical: Option<RowMultiset>,
    logical: LogicalRowMultiset,
    files: Vec<SnapshotFile>,
    total_rows: u64,
    total_bytes: u64,
    physical_arrow_schema: Option<serde_json::Value>,
}

impl SnapshotBuilder {
    pub fn new(output_dir: &Path, logical_schema: LogicalSchema, want_physical: bool) -> Self {
        // A sibling temp directory so the atomic rename is same-filesystem.
        let tmp_dir = output_dir.with_file_name(format!(
            "{}.tmp-snapshot",
            output_dir
                .file_name()
                .map(|s| s.to_string_lossy().into_owned())
                .unwrap_or_else(|| "snapshot".to_string())
        ));
        SnapshotBuilder {
            output_dir: output_dir.to_path_buf(),
            tmp_dir,
            logical_schema,
            physical: want_physical.then(RowMultiset::default),
            logical: LogicalRowMultiset::default(),
            files: Vec::new(),
            total_rows: 0,
            total_bytes: 0,
            physical_arrow_schema: None,
        }
    }

    /// Prepare the temp tree. Refuses a non-empty output directory unless `replace`.
    pub fn begin(&self, output_exists_ok: bool) -> Result<()> {
        if self.output_dir.exists() {
            let non_empty = std::fs::read_dir(&self.output_dir)
                .map(|mut d| d.next().is_some())
                .unwrap_or(false);
            if non_empty && !output_exists_ok {
                bail!(
                    "output directory {} already exists and is not empty; pass --replace to \
                     overwrite (a snapshot is immutable, so this refuses by default)",
                    self.output_dir.display()
                );
            }
            if output_exists_ok {
                std::fs::remove_dir_all(&self.output_dir).ok();
            }
        }
        if self.tmp_dir.exists() {
            std::fs::remove_dir_all(&self.tmp_dir).ok();
        }
        std::fs::create_dir_all(&self.tmp_dir)
            .with_context(|| format!("creating {}", self.tmp_dir.display()))?;
        Ok(())
    }

    /// Freeze one file: census it, fold its rows into both fingerprints, and write its
    /// bytes into the temp tree.
    pub fn add_file(&mut self, pending: &PendingFile, bytes: &[u8]) -> Result<()> {
        let (rows, row_groups) = self
            .census_and_fold(&pending.rel_path, bytes)
            .with_context(|| format!("reading {}", pending.rel_path))?;

        if let Some(expect) = pending.expect_rows
            && expect != rows
        {
            bail!(
                "{}: object holds {rows} rows, source census says {expect}",
                pending.rel_path
            );
        }
        if let Some(expect) = pending.expect_bytes
            && expect != bytes.len() as u64
        {
            bail!(
                "{}: object is {} bytes, source census says {expect}",
                pending.rel_path,
                bytes.len()
            );
        }

        // Write the exact bytes to the temp tree, creating parent dirs.
        let dst = self.tmp_dir.join(&pending.rel_path);
        if let Some(parent) = dst.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("creating {}", parent.display()))?;
        }
        std::fs::write(&dst, bytes).with_context(|| format!("writing {}", dst.display()))?;

        // Re-read what we just wrote and prove it is byte-identical before we record its
        // digest — a snapshot that trusts the write it did not verify is not a control.
        let written =
            std::fs::read(&dst).with_context(|| format!("re-reading {}", dst.display()))?;
        if written != bytes {
            bail!(
                "{}: written bytes differ from source bytes",
                pending.rel_path
            );
        }

        self.total_rows += rows;
        self.total_bytes += bytes.len() as u64;
        self.files.push(SnapshotFile {
            path: pending.rel_path.clone(),
            object_key: pending.object_key.clone(),
            digest: digest_bytes(bytes),
            bytes: bytes.len() as u64,
            rows,
            row_groups,
            source_part: pending.source_part.clone(),
        });
        Ok(())
    }

    /// Read one file's Parquet metadata and stream its batches into both fingerprints.
    fn census_and_fold(&mut self, rel_path: &str, bytes: &[u8]) -> Result<(u64, u32)> {
        let builder =
            ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::copy_from_slice(bytes))?;
        let meta = builder.metadata();
        let rows = meta.file_metadata().num_rows() as u64;
        let row_groups = meta.num_row_groups() as u32;
        if self.physical_arrow_schema.is_none() {
            self.physical_arrow_schema = Some(arrow_schema_json(builder.schema()));
        }
        let reader = builder.build()?;
        for batch in reader {
            let batch = batch?;
            if let Some(p) = &mut self.physical {
                p.update(&batch)
                    .map_err(|e| anyhow::anyhow!("{rel_path}: physical fingerprint: {e}"))?;
            }
            self.logical
                .update(&batch, &self.logical_schema)
                .map_err(|e| anyhow::anyhow!("{rel_path}: logical fingerprint: {e}"))?;
        }
        Ok((rows, row_groups))
    }

    /// The physical fingerprint accumulated so far, for a receipt cross-check before
    /// publishing.
    pub fn physical(&self) -> Option<&RowMultiset> {
        self.physical.as_ref()
    }

    pub fn logical(&self) -> &LogicalRowMultiset {
        &self.logical
    }

    /// The Arrow schema of the frozen files, as JSON.
    pub fn physical_schema_json(&self) -> serde_json::Value {
        self.physical_arrow_schema
            .clone()
            .unwrap_or(serde_json::Value::Null)
    }

    pub fn total_rows(&self) -> u64 {
        self.total_rows
    }

    /// Finish: build the manifest, write it into the temp tree, then rename the temp tree
    /// onto the output path so the snapshot appears atomically and complete.
    #[allow(clippy::too_many_arguments)]
    pub fn finish(self, manifest: SnapshotManifest) -> Result<PathBuf> {
        // Structural validation before anything is published.
        manifest.validate("manifest.json")?;
        let manifest_bytes = serde_json::to_vec_pretty(&manifest)?;
        std::fs::write(self.tmp_dir.join("manifest.json"), &manifest_bytes)
            .context("writing manifest.json")?;

        // Atomic publish: the fully-populated temp tree becomes the output in one rename.
        if self.output_dir.exists() {
            std::fs::remove_dir_all(&self.output_dir).ok();
        }
        std::fs::rename(&self.tmp_dir, &self.output_dir).with_context(|| {
            format!(
                "publishing {} -> {}",
                self.tmp_dir.display(),
                self.output_dir.display()
            )
        })?;
        Ok(self.output_dir.join("manifest.json"))
    }

    pub fn files(&self) -> &[SnapshotFile] {
        &self.files
    }
}

/// Serialize an Arrow schema to a compact JSON description (name + Arrow type per field).
pub fn arrow_schema_json(schema: &arrow::datatypes::Schema) -> serde_json::Value {
    serde_json::json!({
        "fields": schema
            .fields()
            .iter()
            .map(|f| serde_json::json!({
                "name": f.name(),
                "type": format!("{:?}", f.data_type()),
                "nullable": f.is_nullable(),
            }))
            .collect::<Vec<_>>()
    })
}

/// Verify a published snapshot against its manifest, offline.
///
/// Re-checks every file's digest and size, refuses any file present on disk that the
/// manifest does not list (an added file), and recomputes the logical fingerprint over
/// all files so an alteration that also rewrote the manifest digest still fails.
pub fn verify(manifest_path: &Path) -> Result<VerifyReport> {
    let dir = manifest_path.parent().unwrap_or(Path::new("."));
    let bytes = std::fs::read(manifest_path)
        .with_context(|| format!("reading {}", manifest_path.display()))?;
    let manifest = SnapshotManifest::parse(&manifest_path.display().to_string(), &bytes)?;

    // Every listed file must be present, unchanged, and the right size.
    let mut listed: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    let schema = from_ukiel::logical_schema_from_manifest(&manifest)?;
    let mut logical = LogicalRowMultiset::default();
    let mut total_rows = 0u64;
    for f in &manifest.files {
        listed.insert(f.path.clone());
        let path = dir.join(&f.path);
        let fb =
            std::fs::read(&path).with_context(|| format!("{}: missing snapshot file", f.path))?;
        FileDigest {
            path: f.path.clone(),
            bytes: f.bytes,
            digest: f.digest.clone(),
        }
        .verify(&f.path, &fb)?;
        let (rows, _rg) = fold_logical(&f.path, &fb, &schema, &mut logical)?;
        total_rows += rows;
    }

    // No file on disk (under the tracked subtrees) may be absent from the manifest.
    let extra = extra_files(dir, &listed);
    if !extra.is_empty() {
        bail!(
            "snapshot directory holds files the manifest does not list: {extra:?}. A snapshot is \
             an explicit set; an added file is a tampered snapshot."
        );
    }

    if total_rows != manifest.total_rows {
        bail!(
            "row census {total_rows} != manifest total_rows {}",
            manifest.total_rows
        );
    }
    let recomputed = logical_fingerprint(&logical);
    if !recomputed.agrees_with(&manifest.logical_fingerprint) {
        bail!(
            "the snapshot's rows do not reproduce the manifest's logical fingerprint — a file has \
             been altered even though its recorded digest may have been rewritten to match"
        );
    }
    Ok(VerifyReport {
        files: manifest.files.len(),
        rows: total_rows,
        logical_digest: recomputed.digest,
    })
}

/// What `verify` proved.
#[derive(Debug)]
pub struct VerifyReport {
    pub files: usize,
    pub rows: u64,
    pub logical_digest: Digest,
}

fn fold_logical(
    rel_path: &str,
    bytes: &[u8],
    schema: &LogicalSchema,
    logical: &mut LogicalRowMultiset,
) -> Result<(u64, u32)> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::copy_from_slice(bytes))
        .with_context(|| format!("opening {rel_path}"))?;
    let rows = builder.metadata().file_metadata().num_rows() as u64;
    let row_groups = builder.metadata().num_row_groups() as u32;
    let reader = builder.build()?;
    for batch in reader {
        let batch = batch?;
        logical
            .update(&batch, schema)
            .map_err(|e| anyhow::anyhow!("{rel_path}: logical fingerprint: {e}"))?;
    }
    Ok((rows, row_groups))
}

/// Every regular file under `dir` except the manifest itself, relative to `dir`.
fn extra_files(dir: &Path, listed: &std::collections::BTreeSet<String>) -> Vec<String> {
    let mut extra = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&d) else {
            continue;
        };
        for e in entries.flatten() {
            let p = e.path();
            if p.is_dir() {
                stack.push(p);
                continue;
            }
            let rel = p
                .strip_prefix(dir)
                .unwrap_or(&p)
                .to_string_lossy()
                .into_owned();
            if rel == "manifest.json" {
                continue;
            }
            if !listed.contains(&rel) {
                extra.push(rel);
            }
        }
    }
    extra.sort();
    extra
}

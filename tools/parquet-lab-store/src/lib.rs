//! `parquet-lab-store` — publish a laboratory artifact to a disposable object-store
//! namespace and verify it read-only.
//!
//! Publishing uploads an artifact's exact files to an explicit disposable prefix with
//! bounded buffers, verifies each object's HEAD/size, records its SHA-256, and writes a
//! `ukiel-parquet-store/v1` receipt atomically. It refuses an existing prefix, a root/empty
//! prefix, path traversal, and overwrite. Verification is read-only. There is no delete.
//! Credentials come only from the environment/config and never enter a receipt.

pub mod publish;
pub mod verify;

use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use object_store::ObjectStore;
use parquet_lab_contract::{SnapshotManifest, VariantManifest, digest_bytes};
use serde::Deserialize;

/// A store target, from a TOML config. Credentials for S3 come from the environment, never
/// from this file's serialized form in a receipt.
#[derive(Debug, Clone, Deserialize)]
pub struct StoreConfig {
    /// `local` or `s3`.
    pub kind: String,
    /// Local filesystem base directory (kind = `local`).
    #[serde(default)]
    pub base_dir: Option<String>,
    /// S3-compatible endpoint (kind = `s3`), e.g. a MinIO URL.
    #[serde(default)]
    pub endpoint: Option<String>,
    #[serde(default)]
    pub bucket: Option<String>,
    #[serde(default)]
    pub region: Option<String>,
    /// Allow HTTP (MinIO on localhost). Never for production S3.
    #[serde(default)]
    pub allow_http: bool,
}

impl StoreConfig {
    pub fn load(path: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("reading store config {}", path.display()))?;
        toml::from_str(&text).map_err(|e| anyhow::anyhow!("{}: {e}", path.display()))
    }

    /// Build the object store and its non-secret endpoint identity.
    pub fn build(&self) -> Result<(Arc<dyn ObjectStore>, String, String)> {
        match self.kind.as_str() {
            "local" => {
                let base = self
                    .base_dir
                    .as_ref()
                    .context("local store config needs base_dir")?;
                std::fs::create_dir_all(base)?;
                let store = object_store::local::LocalFileSystem::new_with_prefix(base)?;
                Ok((Arc::new(store), "local".to_string(), base.clone()))
            }
            "s3" => {
                let endpoint = self.endpoint.clone().unwrap_or_default();
                let bucket = self
                    .bucket
                    .as_ref()
                    .context("s3 store config needs bucket")?;
                let mut b = object_store::aws::AmazonS3Builder::from_env()
                    .with_bucket_name(bucket)
                    .with_allow_http(self.allow_http);
                if !endpoint.is_empty() {
                    b = b.with_endpoint(endpoint.clone());
                }
                if let Some(r) = &self.region {
                    b = b.with_region(r);
                }
                let store = b.build().context("building the S3 store")?;
                let identity = if endpoint.is_empty() {
                    format!("s3://{bucket}")
                } else {
                    format!("{endpoint}/{bucket}")
                };
                Ok((Arc::new(store), "s3".to_string(), identity))
            }
            other => bail!("unknown store kind '{other}' (local|s3)"),
        }
    }
}

/// A resolved artifact: its files (relative path + local bytes) and its manifest digest.
pub struct Artifact {
    pub files: Vec<(String, Vec<u8>)>,
    pub artifact_digest: String,
}

/// Read a snapshot *or* variant manifest and its files from disk.
pub fn read_artifact(manifest_path: &Path) -> Result<Artifact> {
    let dir = manifest_path.parent().unwrap_or(Path::new("."));
    let mb = std::fs::read(manifest_path)
        .with_context(|| format!("reading {}", manifest_path.display()))?;
    let artifact_digest = digest_bytes(&mb);
    let name = manifest_path.display().to_string();
    let rels: Vec<(String, String)> = match SnapshotManifest::parse(&name, &mb) {
        Ok(s) => s
            .files
            .iter()
            .map(|f| (f.path.clone(), f.digest.clone()))
            .collect(),
        Err(_) => VariantManifest::parse(&name, &mb)
            .context("manifest is neither a snapshot nor a variant")?
            .files
            .iter()
            .map(|f| (f.output.path.clone(), f.output.digest.clone()))
            .collect(),
    };
    let mut files = Vec::with_capacity(rels.len());
    for (rel, digest) in rels {
        let bytes = std::fs::read(dir.join(&rel)).with_context(|| format!("reading {rel}"))?;
        // The manifest's own digest binds the bytes we publish to the artifact.
        if digest_bytes(&bytes) != digest {
            bail!(
                "{rel}: local bytes do not match the artifact manifest digest; refusing to publish a mutated artifact"
            );
        }
        files.push((rel, bytes));
    }
    if files.is_empty() {
        bail!("the artifact lists no files");
    }
    Ok(Artifact {
        files,
        artifact_digest,
    })
}

/// Reject a root, empty, or traversing prefix.
pub fn check_prefix(prefix: &str) -> Result<()> {
    let trimmed = prefix.trim();
    if trimmed.is_empty() || trimmed == "/" || trimmed == "." {
        bail!("refusing an empty/root prefix '{prefix}'");
    }
    if trimmed.split('/').any(|seg| seg == "..") {
        bail!("refusing a traversing prefix '{prefix}'");
    }
    if trimmed.starts_with('/') {
        bail!("refusing an absolute prefix '{prefix}'");
    }
    Ok(())
}

/// SHA-256 of bytes, lowercase hex — what an S3-compatible store can attest to.
pub fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(bytes);
    let out = h.finalize();
    let mut s = String::with_capacity(64);
    for b in out {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

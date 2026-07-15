//! Publish: upload an artifact's files to a disposable prefix and write the receipt.

use std::path::Path;

use anyhow::{Context, Result, bail};
use futures::StreamExt;
use object_store::ObjectStoreExt as _;
use object_store::path::Path as ObjPath;
use parquet_lab_contract::{STORE_RECEIPT_VERSION, StoreObject, StoreReceipt};

use crate::{StoreConfig, check_prefix, read_artifact, sha256_hex};

/// Upload the artifact under `prefix` and write a `ukiel-parquet-store/v1` receipt.
pub async fn publish(
    manifest_path: &Path,
    config: &StoreConfig,
    prefix: &str,
    receipt_out: &Path,
) -> Result<()> {
    check_prefix(prefix)?;
    if receipt_out.exists() {
        bail!(
            "receipt {} already exists; refusing to overwrite",
            receipt_out.display()
        );
    }
    let artifact = read_artifact(manifest_path)?;
    let (store, store_kind, endpoint_identity) = config.build()?;

    // Refuse a prefix that already holds objects — a publish target is fresh and disposable.
    let mut existing = store.list(Some(&ObjPath::from(prefix)));
    if existing.next().await.is_some() {
        bail!("prefix '{prefix}' already contains objects; choose a fresh disposable prefix");
    }
    drop(existing);

    let mut objects = Vec::with_capacity(artifact.files.len());
    for (rel, bytes) in &artifact.files {
        let key = ObjPath::from(format!("{prefix}/{rel}"));
        store
            .put_opts(
                &key,
                bytes.clone().into(),
                object_store::PutOptions::default(),
            )
            .await
            .with_context(|| format!("uploading {rel}"))?;
        // Verify HEAD/size immediately.
        let head = store
            .head(&key)
            .await
            .with_context(|| format!("HEAD {rel}"))?;
        if head.size != bytes.len() as u64 {
            bail!(
                "{rel}: uploaded size {} != local {}",
                head.size,
                bytes.len()
            );
        }
        objects.push(StoreObject {
            path: rel.clone(),
            size: bytes.len() as u64,
            sha256: sha256_hex(bytes),
        });
    }
    objects.sort_by(|a, b| a.path.cmp(&b.path));

    let receipt = StoreReceipt {
        receipt_version: STORE_RECEIPT_VERSION.to_string(),
        artifact_digest: artifact.artifact_digest,
        store_kind,
        endpoint_identity,
        bucket: config.bucket.clone().unwrap_or_default(),
        prefix: prefix.to_string(),
        objects,
    };
    receipt.validate(&receipt_out.display().to_string())?;
    let tmp = receipt_out.with_extension("json.tmp");
    std::fs::write(&tmp, serde_json::to_vec_pretty(&receipt)?)?;
    std::fs::rename(&tmp, receipt_out)
        .with_context(|| format!("publishing receipt {}", receipt_out.display()))?;
    Ok(())
}

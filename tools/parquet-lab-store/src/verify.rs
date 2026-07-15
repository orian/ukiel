//! Verify: read-only re-check of a published namespace against its receipt.

use std::path::Path;

use anyhow::{Context, Result, bail};
use object_store::ObjectStoreExt as _;
use object_store::path::Path as ObjPath;
use parquet_lab_contract::StoreReceipt;

use crate::{StoreConfig, sha256_hex};

/// What verification proved.
#[derive(Debug)]
pub struct VerifyReport {
    pub objects: usize,
    pub total_bytes: u64,
}

/// Re-check every object's size and SHA-256 against the receipt. Read-only: it issues only
/// HEAD and GET, never a put, delete, or copy.
pub async fn verify(receipt_path: &Path, config: &StoreConfig) -> Result<VerifyReport> {
    let rb = std::fs::read(receipt_path)
        .with_context(|| format!("reading {}", receipt_path.display()))?;
    let receipt = StoreReceipt::parse(&receipt_path.display().to_string(), &rb)?;
    let (store, _kind, _identity) = config.build()?;

    let mut total = 0u64;
    for o in &receipt.objects {
        let key = ObjPath::from(format!("{}/{}", receipt.prefix, o.path));
        let head = store
            .head(&key)
            .await
            .with_context(|| format!("HEAD {}", o.path))?;
        if head.size != o.size {
            bail!("{}: store size {} != receipt {}", o.path, head.size, o.size);
        }
        let bytes = store
            .get(&key)
            .await
            .with_context(|| format!("GET {}", o.path))?
            .bytes()
            .await
            .with_context(|| format!("reading {}", o.path))?;
        if sha256_hex(&bytes) != o.sha256 {
            bail!(
                "{}: SHA-256 mismatch — the stored object differs from the receipt",
                o.path
            );
        }
        total += o.size;
    }
    Ok(VerifyReport {
        objects: receipt.objects.len(),
        total_bytes: total,
    })
}

//! `ukiel-parquet-store/v1` — an immutable object-namespace receipt.
//!
//! Binds one artifact manifest digest to one immutable object namespace (a store kind, an
//! endpoint *identity* with no secrets, a bucket/prefix, and the sorted object
//! path/size/SHA-256 tuples). It is what lets the read-only benchmark read a published
//! artifact through a real object store and prove it read the exact bytes that were
//! published — without ever carrying a credential.
//!
//! Credentials and mutable client configuration NEVER appear in a receipt. There is no
//! field for an access key, secret, or session token, and the golden test in
//! `tests/contracts.rs` proves a serialized receipt cannot contain one.

use serde::{Deserialize, Serialize};

use crate::{ContractError, check_relative_path, check_version};

/// The store receipt format. A reader that does not recognise it fails closed.
pub const STORE_RECEIPT_VERSION: &str = "ukiel-parquet-store/v1";

/// One object in the immutable namespace.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoreObject {
    /// The object key relative to `prefix`. Never absolute, never traversing.
    pub path: String,
    pub size: u64,
    /// Lowercase-hex SHA-256 of the object bytes. (SHA-256, not BLAKE3: this is what an
    /// S3-compatible store can attest to independently.)
    pub sha256: String,
}

/// The immutable-namespace receipt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoreReceipt {
    pub receipt_version: String,
    /// The artifact manifest (snapshot or variant) whose files these objects are, by digest.
    pub artifact_digest: crate::Digest,
    /// `s3`, `minio`, `local`, … — the store kind, never a client secret.
    pub store_kind: String,
    /// A non-secret endpoint identity, e.g. `http://127.0.0.1:9000` or `s3://region`. No
    /// access key, secret, or token — those live only in the operator's environment.
    pub endpoint_identity: String,
    pub bucket: String,
    /// The disposable prefix the artifact was published under.
    pub prefix: String,
    /// Every object, sorted by path so the receipt (and its digest) is deterministic.
    pub objects: Vec<StoreObject>,
}

impl StoreReceipt {
    pub fn parse(path: &str, bytes: &[u8]) -> Result<Self, ContractError> {
        let value: serde_json::Value =
            serde_json::from_slice(bytes).map_err(|source| ContractError::Parse {
                path: path.to_string(),
                source,
            })?;
        check_version(path, &value, STORE_RECEIPT_VERSION, "receipt_version")?;
        let receipt: StoreReceipt =
            serde_json::from_value(value).map_err(|source| ContractError::Parse {
                path: path.to_string(),
                source,
            })?;
        receipt.validate(path)?;
        Ok(receipt)
    }

    /// Enforce the invariants: relative non-traversing keys, unique objects, sorted order.
    pub fn validate(&self, path: &str) -> Result<(), ContractError> {
        let mut seen = std::collections::HashSet::new();
        let mut prev: Option<&str> = None;
        for o in &self.objects {
            check_relative_path(path, &o.path)?;
            if !seen.insert(o.path.as_str()) {
                return Err(ContractError::DuplicateFile {
                    context: path.to_string(),
                    duplicate: o.path.clone(),
                });
            }
            if let Some(p) = prev
                && p > o.path.as_str()
            {
                return Err(ContractError::UnsortedObjects {
                    context: path.to_string(),
                    offending: o.path.clone(),
                });
            }
            prev = Some(&o.path);
        }
        Ok(())
    }

    /// Whether this receipt describes the given artifact.
    pub fn is_for_artifact(&self, artifact_digest: &str) -> bool {
        self.artifact_digest == artifact_digest
    }
}

use serde::{Deserialize, Serialize};

use crate::ids::{CommitId, PartId};
use crate::part::{Part, PartMeta};

/// An atomic catalog mutation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum CommitOp {
    /// Register new parts (ingest).
    Add { parts: Vec<PartMeta> },
    /// Atomically tombstone `old` and register `new` (compaction, mutation rewrite).
    /// Fails with a conflict if any `old` part is no longer live.
    Replace {
        old: Vec<PartId>,
        new: Vec<PartMeta>,
    },
    /// Tombstone parts without replacement (retention/GDPR).
    Delete { parts: Vec<PartId> },
}

impl CommitOp {
    pub fn kind(&self) -> &'static str {
        match self {
            CommitOp::Add { .. } => "add",
            CommitOp::Replace { .. } => "replace",
            CommitOp::Delete { .. } => "delete",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitResult {
    /// The commit was applied by this call.
    Committed(CommitId),
    /// A commit with the same idempotency key already existed; nothing was changed.
    AlreadyApplied(CommitId),
}

impl CommitResult {
    pub fn commit_id(&self) -> CommitId {
        match self {
            CommitResult::Committed(id) | CommitResult::AlreadyApplied(id) => *id,
        }
    }
}

/// One change-feed entry: what a commit added and removed.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChangeEvent {
    pub commit_id: CommitId,
    /// "add" | "replace" | "delete"
    pub kind: String,
    pub added: Vec<Part>,
    pub removed: Vec<PartId>,
}

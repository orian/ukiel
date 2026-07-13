//! A deterministic fault point at the commit boundary — **test builds only**
//! (plan 43, task 7).
//!
//! Reconciliation exists for one event: the transaction is durable and the
//! acknowledgement never arrives. That event cannot be produced by cutting the
//! network from outside, because from outside you cannot tell — and cannot
//! choose — which side of `COMMIT` the cut lands on. Toxiproxy can prove the
//! system survives an outage (S10 does); only a seam *inside* the response path
//! can prove it resolves a lost acknowledgement, because only there can the test
//! say "this one committed, and the caller will never learn it".
//!
//! Three properties, all load-bearing:
//!
//! * **Feature-gated.** Compiled out of any build that does not ask for it, so
//!   it cannot become an operator-reachable knob. There is no environment
//!   variable and no TOML key: the only way to arm it is to hold the catalog
//!   handle and call a method that does not exist in production.
//! * **Instance-scoped.** The armed fault lives in the `PostgresCatalog` (and
//!   its clones), not in a global. Two catalogs in one test process do not
//!   interfere.
//! * **One-shot.** It fires once and disarms. A fault that fired on every commit
//!   would not model a lost acknowledgement; it would model a broken database.

use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

/// Where to fail, relative to `COMMIT`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitFault {
    /// Fail with a transport error *before* the transaction commits. It rolls
    /// back: the operation is definitely **absent**, and the caller cannot know
    /// that. Reconciliation must discover it.
    BeforeCommit,
    /// Commit, then fail on the way home. The transaction is **durable** and the
    /// caller receives the same transport error as above — indistinguishable to
    /// it, decidable only by asking the catalog.
    AfterCommitBeforeReturn,
}

const NONE: u8 = 0;
const BEFORE: u8 = 1;
const AFTER: u8 = 2;

/// The armed fault, shared with every clone of the catalog handle.
#[derive(Debug, Clone, Default)]
pub(crate) struct CommitFaults(Arc<AtomicU8>);

impl CommitFaults {
    pub(crate) fn arm(&self, fault: CommitFault) {
        let code = match fault {
            CommitFault::BeforeCommit => BEFORE,
            CommitFault::AfterCommitBeforeReturn => AFTER,
        };
        self.0.store(code, Ordering::SeqCst);
    }

    /// Takes the fault if it matches, disarming it. Returns false otherwise, so
    /// the two call sites never consume each other's arming.
    pub(crate) fn take(&self, fault: CommitFault) -> bool {
        let code = match fault {
            CommitFault::BeforeCommit => BEFORE,
            CommitFault::AfterCommitBeforeReturn => AFTER,
        };
        self.0
            .compare_exchange(code, NONE, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
    }
}

/// The error a cut connection produces at the boundary. Transport class, so it
/// travels the real code path: `AmbiguousMutation` when the mutation is
/// identified, `UnidentifiedAmbiguousMutation` when it is not.
pub(crate) fn injected_transport_error() -> sqlx::Error {
    sqlx::Error::Io(std::io::Error::new(
        std::io::ErrorKind::ConnectionReset,
        "injected fault: connection reset at the commit boundary",
    ))
}

impl crate::PostgresCatalog {
    /// Arms a one-shot fault at the next commit boundary on this handle.
    ///
    /// Test-only, and deliberately awkward to reach: you must own the catalog
    /// the process will use. See the module docs for why this cannot be done
    /// from outside the process.
    pub fn arm_commit_fault(&self, fault: CommitFault) {
        self.faults.arm(fault);
    }
}

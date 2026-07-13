//! The worker-ready handshake (plan 42).
//!
//! A worker is *ready* when it has reloaded its authoritative state — not when
//! its future was spawned, and not when a `SELECT 1` answered. The distinction
//! is the whole point: ingest must reload catalog offsets and re-seek Kafka
//! before it consumes anything; the compactor must rediscover live state before
//! it merges; GC must reread a candidate and its fence before it deletes.
//!
//! A process that reports Healthy on a successful ping is a process that will
//! act on state it has not reloaded. So the worker says so itself, once, through
//! this signal.

use std::fmt;
use std::sync::Arc;

/// A one-way "I have reloaded from authority and am doing my job" callback.
/// Cheap to clone; firing it more than once is harmless (the health registry
/// treats it as idempotent).
#[derive(Clone)]
pub struct ReadySignal(Arc<dyn Fn() + Send + Sync>);

impl ReadySignal {
    pub fn new(f: impl Fn() + Send + Sync + 'static) -> Self {
        ReadySignal(Arc::new(f))
    }

    pub fn fire(&self) {
        (self.0)()
    }
}

impl fmt::Debug for ReadySignal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ReadySignal")
    }
}

impl PartialEq for ReadySignal {
    /// Two signals are never meaningfully equal; this exists only so worker
    /// configs can keep deriving `PartialEq` for their tests.
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

/// Fires the signal if one was configured.
pub fn signal_ready(ready: &Option<ReadySignal>) {
    if let Some(r) = ready {
        r.fire();
    }
}

//! Role and catalog health (plan 42).
//!
//! Two questions that are constantly conflated, kept apart here:
//!
//! - **Is the catalog reachable?** A successful `ping` answers this.
//! - **Is this role able to do its job?** A ping does *not* answer this. Ingest
//!   must reload its authoritative offsets and re-seek Kafka; the compactor must
//!   rediscover live state; GC must reread a candidate and its fence. A role
//!   that goes `Healthy` because a `SELECT 1` succeeded is a role that will act
//!   on state it has not reloaded.
//!
//! So `CatalogState` and `RoleState` are separate, and a role leaves
//! `Reconciling` only when it says so itself — never when the probe says so.

use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use metrics::gauge;
use serde::Serialize;
use ukiel_catalog::CatalogErrorClass;

use crate::config::Role;

/// Reachability of the catalog, as last observed. Says nothing about whether any
/// role has *reconciled* with it.
#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum CatalogState {
    Healthy,
    Unavailable,
}

/// A role's own lifecycle.
///
/// `Degraded` = it hit a recoverable catalog failure and its worker is gone.
/// `Reconciling` = the catalog is reachable again and the worker is rebuilding
/// from authority. `Healthy` = that rebuild *finished*. The gap between the last
/// two is the whole point of this enum.
#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum RoleState {
    Starting,
    Healthy,
    Degraded,
    Reconciling,
    Failed,
    Stopped,
}

impl RoleState {
    fn as_label(self) -> &'static str {
        match self {
            RoleState::Starting => "starting",
            RoleState::Healthy => "healthy",
            RoleState::Degraded => "degraded",
            RoleState::Reconciling => "reconciling",
            RoleState::Failed => "failed",
            RoleState::Stopped => "stopped",
        }
    }

    const ALL: [RoleState; 6] = [
        RoleState::Starting,
        RoleState::Healthy,
        RoleState::Degraded,
        RoleState::Reconciling,
        RoleState::Failed,
        RoleState::Stopped,
    ];

    /// A role that has permanently failed or been stopped never transitions
    /// again — the process is on its way down and a late worker must not
    /// resurrect its state.
    fn is_terminal(self) -> bool {
        matches!(self, RoleState::Failed | RoleState::Stopped)
    }
}

pub fn role_label(role: Role) -> &'static str {
    match role {
        Role::Ingest => "ingest",
        Role::Query => "query",
        Role::Compactor => "compactor",
        Role::Gc => "gc",
    }
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct RoleSnapshot {
    pub state: RoleState,
    /// Seconds this role has been in its current state.
    pub state_age_seconds: u64,
    /// Consecutive recovery attempts since it last went `Healthy`.
    pub recovery_attempts: u32,
    /// The *class* of the last failure, never its text. A class is a fact an
    /// operator can act on; a driver's error string is a fact that changes on a
    /// dependency bump — and may carry a URL, a credential, or a tenant id.
    pub last_error_class: Option<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct HealthSnapshot {
    /// True only when every configured role is `Healthy` and the catalog is
    /// reachable — i.e. this process can take new work.
    pub ready: bool,
    pub catalog: CatalogState,
    /// Seconds since the catalog last answered. `None` = it never has (the
    /// process started during an outage).
    pub catalog_last_success_seconds: Option<u64>,
    pub roles: BTreeMap<String, RoleSnapshot>,
}

#[derive(Debug)]
struct RoleEntry {
    state: RoleState,
    since: std::time::Instant,
    recovery_attempts: u32,
    last_error_class: Option<CatalogErrorClass>,
}

#[derive(Debug)]
struct Inner {
    catalog: CatalogState,
    catalog_last_success: Option<std::time::Instant>,
    roles: BTreeMap<&'static str, RoleEntry>,
}

/// Shared, lock-light health state. Every mutation is a tiny in-memory write;
/// the lock is **never** held across an `await`, so a slow reader can never
/// stall a worker's transition.
#[derive(Clone)]
pub struct HealthRegistry {
    inner: Arc<RwLock<Inner>>,
}

impl HealthRegistry {
    pub fn new(roles: &[Role]) -> Self {
        let mut map = BTreeMap::new();
        for role in roles {
            let label = role_label(*role);
            map.insert(
                label,
                RoleEntry {
                    state: RoleState::Starting,
                    since: std::time::Instant::now(),
                    recovery_attempts: 0,
                    last_error_class: None,
                },
            );
            set_state_gauge(label, RoleState::Starting);
        }
        // The catalog starts Unavailable, not Healthy: nothing has proven it is
        // reachable yet, and a process that starts during a failover must not
        // claim readiness it has not earned.
        Self {
            inner: Arc::new(RwLock::new(Inner {
                catalog: CatalogState::Unavailable,
                catalog_last_success: None,
                roles: map,
            })),
        }
    }

    /// A handle one role uses to move its own state. Roles cannot move each
    /// other's: a compactor failure must not be able to mark ingest degraded.
    pub fn role(&self, role: Role) -> RoleHandle {
        RoleHandle {
            registry: self.clone(),
            label: role_label(role),
        }
    }

    pub fn catalog_reachable(&self) {
        let mut inner = self.inner.write().expect("health lock");
        inner.catalog = CatalogState::Healthy;
        inner.catalog_last_success = Some(std::time::Instant::now());
        gauge!("catalog_last_success_timestamp").set(unix_secs());
        gauge!("catalog_outage_seconds").set(0.0);
    }

    pub fn catalog_unavailable(&self) {
        let mut inner = self.inner.write().expect("health lock");
        inner.catalog = CatalogState::Unavailable;
    }

    pub fn snapshot(&self) -> HealthSnapshot {
        let inner = self.inner.read().expect("health lock");
        let now = std::time::Instant::now();
        let roles: BTreeMap<String, RoleSnapshot> = inner
            .roles
            .iter()
            .map(|(label, entry)| {
                (
                    (*label).to_string(),
                    RoleSnapshot {
                        state: entry.state,
                        state_age_seconds: now.duration_since(entry.since).as_secs(),
                        recovery_attempts: entry.recovery_attempts,
                        last_error_class: entry.last_error_class.map(|c| c.as_label().to_string()),
                    },
                )
            })
            .collect();
        HealthSnapshot {
            ready: inner.catalog == CatalogState::Healthy
                && roles.values().all(|r| r.state == RoleState::Healthy),
            catalog: inner.catalog,
            catalog_last_success_seconds: inner
                .catalog_last_success
                .map(|t| now.duration_since(t).as_secs()),
            roles,
        }
    }
}

/// One role's capability to move its own state.
#[derive(Clone)]
pub struct RoleHandle {
    registry: HealthRegistry,
    label: &'static str,
}

impl RoleHandle {
    pub fn label(&self) -> &'static str {
        self.label
    }

    /// The role has reloaded from catalog authority and is doing its job.
    /// Called by the worker itself — never by a probe (see the module docs).
    pub fn healthy(&self) {
        self.transition(RoleState::Healthy, None, |e| e.recovery_attempts = 0);
    }

    /// A recoverable catalog failure took the worker down. `class` is recorded
    /// for the snapshot; the error text is deliberately dropped.
    pub fn degraded(&self, class: CatalogErrorClass) {
        self.transition(RoleState::Degraded, Some(class), |_| {});
    }

    /// The catalog is reachable again and the worker is rebuilding from
    /// authority. Not yet ready for work.
    pub fn reconciling(&self) {
        self.transition(RoleState::Reconciling, None, |e| e.recovery_attempts += 1);
    }

    /// Terminal: this role cannot be fixed by waiting.
    pub fn failed(&self, class: Option<CatalogErrorClass>) {
        self.transition(RoleState::Failed, class, |_| {});
    }

    pub fn stopped(&self) {
        self.transition(RoleState::Stopped, None, |_| {});
    }

    pub fn state(&self) -> RoleState {
        self.registry
            .inner
            .read()
            .expect("health lock")
            .roles
            .get(self.label)
            .map(|e| e.state)
            .unwrap_or(RoleState::Stopped)
    }

    fn transition(
        &self,
        to: RoleState,
        class: Option<CatalogErrorClass>,
        mut adjust: impl FnMut(&mut RoleEntry),
    ) {
        let mut inner = self.registry.inner.write().expect("health lock");
        let Some(entry) = inner.roles.get_mut(self.label) else {
            return;
        };
        // Failed/Stopped are terminal. Without this, a worker future that is
        // still unwinding after shutdown can flip a stopped role back to
        // Degraded and make a clean exit look like an outage.
        if entry.state.is_terminal() {
            return;
        }
        if entry.state != to {
            entry.since = std::time::Instant::now();
        }
        entry.state = to;
        if class.is_some() {
            entry.last_error_class = class;
        }
        adjust(entry);
        set_state_gauge(self.label, to);
    }
}

/// One-hot: the new state is 1 and every other state for this role is cleared.
/// Without the clear, a role that recovered would keep reporting `degraded=1`
/// forever and every alert built on it would be a lie.
fn set_state_gauge(role: &'static str, state: RoleState) {
    for candidate in RoleState::ALL {
        let value = if candidate == state { 1.0 } else { 0.0 };
        gauge!("ukield_role_state", "role" => role, "state" => candidate.as_label()).set(value);
    }
}

fn unix_secs() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn registry() -> HealthRegistry {
        HealthRegistry::new(&[Role::Query, Role::Compactor])
    }

    #[test]
    fn a_process_starts_unready_and_unproven() {
        // Nothing has proven the catalog reachable yet. A process that came up
        // during a failover must not claim readiness it has not earned.
        let snap = registry().snapshot();
        assert!(!snap.ready);
        assert_eq!(snap.catalog, CatalogState::Unavailable);
        assert_eq!(snap.catalog_last_success_seconds, None);
        assert_eq!(snap.roles["query"].state, RoleState::Starting);
        assert_eq!(snap.roles.len(), 2, "only configured roles are reported");
    }

    #[test]
    fn the_recovery_cycle_is_healthy_degraded_reconciling_healthy() {
        let health = registry();
        health.catalog_reachable();
        let query = health.role(Role::Query);
        let compactor = health.role(Role::Compactor);
        query.healthy();
        compactor.healthy();
        assert!(health.snapshot().ready);

        // A transport failure takes one role down...
        query.degraded(CatalogErrorClass::Transport);
        let snap = health.snapshot();
        assert_eq!(snap.roles["query"].state, RoleState::Degraded);
        assert_eq!(
            snap.roles["query"].last_error_class.as_deref(),
            Some("transport")
        );
        assert!(!snap.ready, "one degraded role makes the process unready");
        assert_eq!(
            snap.roles["compactor"].state,
            RoleState::Healthy,
            "and leaves the others alone"
        );

        // ...the catalog comes back, the worker rebuilds from authority...
        query.reconciling();
        let snap = health.snapshot();
        assert_eq!(snap.roles["query"].state, RoleState::Reconciling);
        assert_eq!(snap.roles["query"].recovery_attempts, 1);
        assert!(
            !snap.ready,
            "reconciling is NOT ready: the reload has not finished"
        );

        // ...and only the worker itself can say it is done.
        query.healthy();
        let snap = health.snapshot();
        assert!(snap.ready);
        assert_eq!(snap.roles["query"].recovery_attempts, 0, "counter resets");
    }

    #[test]
    fn a_repeated_transient_while_reconciling_returns_to_degraded() {
        let health = registry();
        let query = health.role(Role::Query);
        query.healthy();
        query.degraded(CatalogErrorClass::Transport);
        query.reconciling();
        // The catalog flapped: the rebuild itself failed.
        query.degraded(CatalogErrorClass::Transport);
        assert_eq!(query.state(), RoleState::Degraded);
        query.reconciling();
        assert_eq!(health.snapshot().roles["query"].recovery_attempts, 2);
    }

    #[test]
    fn failed_and_stopped_are_terminal() {
        let health = registry();
        let query = health.role(Role::Query);
        query.failed(Some(CatalogErrorClass::PermanentDatabase));
        // A worker future still unwinding after a permanent failure must not be
        // able to flip the role back to something that looks recoverable.
        query.degraded(CatalogErrorClass::Transport);
        query.healthy();
        assert_eq!(query.state(), RoleState::Failed);

        let compactor = health.role(Role::Compactor);
        compactor.stopped();
        compactor.degraded(CatalogErrorClass::Transport);
        assert_eq!(
            compactor.state(),
            RoleState::Stopped,
            "a clean shutdown must not read as an outage"
        );
    }

    #[test]
    fn snapshots_carry_a_class_never_an_error_string() {
        let health = registry();
        let query = health.role(Role::Query);
        // Nothing here can leak a URL, a credential, a tenant id, or SQL: the
        // only thing recorded is the class.
        query.degraded(CatalogErrorClass::Transport);
        let json = serde_json::to_string(&health.snapshot()).unwrap();
        assert!(json.contains("\"transport\""), "{json}");
        assert!(!json.contains("postgres://"), "{json}");
        assert!(!json.contains("password"), "{json}");
        assert!(!json.contains("SELECT"), "{json}");
    }

    #[test]
    fn readiness_needs_both_the_catalog_and_every_role() {
        let health = registry();
        health.role(Role::Query).healthy();
        health.role(Role::Compactor).healthy();
        assert!(
            !health.snapshot().ready,
            "roles healthy but the catalog was never proven reachable"
        );
        health.catalog_reachable();
        assert!(health.snapshot().ready);
        health.catalog_unavailable();
        assert!(!health.snapshot().ready);
    }
}

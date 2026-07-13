//! Plan 42: role state must be readable from Prometheus, and a role that
//! recovered must stop reporting that it is degraded.
//!
//! Its own integration test (its own process): the recorder is process-wide, so
//! sharing it with the parallel unit tests would make these assertions depend on
//! what else happened to run.

use ukiel_catalog::CatalogErrorClass;
use ukield::config::Role;
use ukield::health::{HealthRegistry, RoleState};

/// The rendered value of `ukield_role_state{role,state}`, if present.
fn state_gauge(body: &str, role: &str, state: &str) -> Option<f64> {
    body.lines()
        .find(|l| {
            l.starts_with("ukield_role_state{")
                && l.contains(&format!("role=\"{role}\""))
                && l.contains(&format!("state=\"{state}\""))
        })
        .and_then(|l| l.rsplit(' ').next())
        .and_then(|v| v.parse().ok())
}

#[test]
fn role_state_is_one_hot_and_a_recovered_role_stops_reporting_degraded() {
    let prom = ukield::metrics::handle();
    let health = HealthRegistry::new(&[Role::Query, Role::Compactor]);
    let query = health.role(Role::Query);

    query.healthy();
    let body = prom.render();
    assert_eq!(state_gauge(&body, "query", "healthy"), Some(1.0));
    assert_eq!(state_gauge(&body, "query", "starting"), Some(0.0));

    query.degraded(CatalogErrorClass::Transport);
    let body = prom.render();
    assert_eq!(state_gauge(&body, "query", "degraded"), Some(1.0));
    assert_eq!(
        state_gauge(&body, "query", "healthy"),
        Some(0.0),
        "the old state must be cleared, not left standing"
    );
    // Another role's state is its own.
    assert_eq!(state_gauge(&body, "compactor", "starting"), Some(1.0));
    assert_eq!(state_gauge(&body, "compactor", "degraded"), Some(0.0));

    query.reconciling();
    query.healthy();
    let body = prom.render();
    assert_eq!(state_gauge(&body, "query", "healthy"), Some(1.0));
    // The whole point of the one-hot clear: without it, a role that recovered
    // keeps reporting degraded=1 forever and every alert built on it is a lie.
    assert_eq!(state_gauge(&body, "query", "degraded"), Some(0.0));
    assert_eq!(state_gauge(&body, "query", "reconciling"), Some(0.0));
    assert_eq!(query.state(), RoleState::Healthy);
}

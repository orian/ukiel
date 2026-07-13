//! Bounded, jittered recovery from a transient catalog outage (plan 42).
//!
//! The failure this exists for is a managed writer failover: every process in
//! the fleet loses its connections at the same instant, and every process wants
//! them back at the same instant. Retrying without jitter turns the recovery into
//! a synchronized stampede against a primary that has only just been promoted —
//! the fleet then keeps it down itself.
//!
//! So: exponential backoff, a hard ceiling, and per-process jitter derived from
//! the process identity, so two processes do not agree on when to knock. No
//! connection is held while sleeping, and every wait is cancellable — a SIGTERM
//! during an outage must exit promptly, not after the full backoff.

use std::time::Duration;

use metrics::{counter, gauge};
use tokio_util::sync::CancellationToken;
use ukiel_catalog::PostgresCatalog;

use crate::config::CatalogConfig;
use crate::health::{HealthRegistry, RoleHandle};

/// The process asked to shut down while we were waiting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Shutdown;

#[derive(Debug, Clone)]
pub struct RecoveryPolicy {
    pub base: Duration,
    pub max: Duration,
    /// Symmetric jitter as a fraction of the delay: 0.2 = ±20%.
    pub jitter_ratio: f64,
    pub probe_timeout: Duration,
    /// Per-process seed. Two processes with different seeds do not retry in
    /// lockstep; the same process is deterministic, so a test can pin it.
    pub seed: u64,
}

impl RecoveryPolicy {
    pub fn from_config(cfg: &CatalogConfig, seed: u64) -> Self {
        Self {
            base: Duration::from_millis(cfg.retry_base_ms),
            max: Duration::from_millis(cfg.retry_max_ms),
            jitter_ratio: cfg.retry_jitter_ratio,
            probe_timeout: Duration::from_millis(cfg.recovery_probe_timeout_ms),
            seed,
        }
    }

    pub fn backoff(&self) -> Backoff {
        Backoff {
            policy: self.clone(),
            attempt: 0,
        }
    }
}

/// Exponential backoff with saturation and symmetric jitter.
#[derive(Debug, Clone)]
pub struct Backoff {
    policy: RecoveryPolicy,
    attempt: u32,
}

impl Backoff {
    pub fn attempt(&self) -> u32 {
        self.attempt
    }

    /// The next delay: `base * 2^attempt`, capped at `max`, then jittered by
    /// ±`jitter_ratio`. Saturating throughout — a long outage must not overflow
    /// its way to a zero (or absurd) delay.
    pub fn next_delay(&mut self) -> Duration {
        let exp = self
            .policy
            .base
            .saturating_mul(1u32.checked_shl(self.attempt.min(31)).unwrap_or(u32::MAX));
        let capped = exp.min(self.policy.max);
        self.attempt = self.attempt.saturating_add(1);
        jitter(
            capped,
            self.policy.jitter_ratio,
            self.policy.seed,
            self.attempt,
        )
    }

    pub fn reset(&mut self) {
        self.attempt = 0;
    }
}

/// Deterministic ±ratio jitter. A hash of (seed, attempt) rather than a random
/// number generator: reproducible in tests, and still decorrelated between
/// processes because the seed is the process identity.
fn jitter(delay: Duration, ratio: f64, seed: u64, attempt: u32) -> Duration {
    if ratio <= 0.0 || delay.is_zero() {
        return delay;
    }
    // SplitMix64 finalizer: cheap, and mixes adjacent seeds/attempts well enough
    // that two processes started a millisecond apart do not land together.
    let mut x = seed ^ (u64::from(attempt).wrapping_mul(0x9E37_79B9_7F4A_7C15));
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^= x >> 31;
    // Map to [-1, 1], scale by the ratio, and clamp the factor at 0 so a ratio
    // of exactly 1.0 can shorten a delay to zero but never invert it.
    let unit = (x as f64 / u64::MAX as f64) * 2.0 - 1.0;
    let factor = (1.0 + unit * ratio.min(1.0)).max(0.0);
    delay.mul_f64(factor)
}

/// Waits until the catalog answers, or the process is shutting down.
///
/// Each probe is bounded by `probe_timeout` — an unreachable database must fail
/// the probe, not hang it — and the pool connection is released before every
/// sleep. Retries are unbounded in *count* (a role waits as long as the outage
/// lasts) but bounded in *rate*, which is the property that protects the writer.
pub async fn wait_for_catalog(
    catalog: &PostgresCatalog,
    health: &HealthRegistry,
    role: &RoleHandle,
    policy: &RecoveryPolicy,
    shutdown: &CancellationToken,
) -> Result<(), Shutdown> {
    let mut backoff = policy.backoff();
    let outage_started = std::time::Instant::now();
    loop {
        if shutdown.is_cancelled() {
            return Err(Shutdown);
        }

        let probe = tokio::time::timeout(policy.probe_timeout, catalog.ping());
        match probe.await {
            Ok(Ok(())) => {
                health.catalog_reachable();
                gauge!("catalog_outage_seconds").set(0.0);
                return Ok(());
            }
            // A failed probe and a probe that never answered are the same fact:
            // the catalog is not usable. Neither is a reason to stop waiting.
            Ok(Err(e)) => {
                health.catalog_unavailable();
                counter!("catalog_retry_total",
                    "role" => role.label(), "reason" => e.class().as_label())
                .increment(1);
            }
            Err(_elapsed) => {
                health.catalog_unavailable();
                counter!("catalog_retry_total",
                    "role" => role.label(), "reason" => "probe_timeout")
                .increment(1);
            }
        }

        gauge!("catalog_outage_seconds").set(outage_started.elapsed().as_secs_f64());
        let delay = backoff.next_delay();
        tracing::warn!(
            role = role.label(),
            attempt = backoff.attempt(),
            delay_ms = delay.as_millis() as u64,
            "catalog unavailable; backing off"
        );
        // Cancellable: SIGTERM during an outage exits now, not in five seconds.
        tokio::select! {
            () = shutdown.cancelled() => return Err(Shutdown),
            () = tokio::time::sleep(delay) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy(seed: u64) -> RecoveryPolicy {
        RecoveryPolicy {
            base: Duration::from_millis(100),
            max: Duration::from_millis(5_000),
            jitter_ratio: 0.2,
            probe_timeout: Duration::from_millis(500),
            seed,
        }
    }

    #[test]
    fn backoff_grows_exponentially_and_saturates_at_the_cap() {
        let p = policy(1);
        let mut b = p.backoff();
        // Each delay stays inside ±ratio of its ideal, and the ideal doubles
        // until the cap. Without the cap, a long outage would back off to hours.
        for ideal_ms in [100u64, 200, 400, 800, 1600, 3200, 5000, 5000, 5000, 5000] {
            let d = b.next_delay().as_millis() as f64;
            let ideal = ideal_ms as f64;
            assert!(
                d >= ideal * 0.8 - 1.0 && d <= ideal * 1.2 + 1.0,
                "delay {d} outside ±20% of {ideal}"
            );
        }
        assert!(b.attempt() >= 10);
    }

    #[test]
    fn reset_returns_to_the_first_delay() {
        let mut b = policy(7).backoff();
        for _ in 0..6 {
            b.next_delay();
        }
        b.reset();
        assert_eq!(b.attempt(), 0);
        let d = b.next_delay().as_millis() as f64;
        assert!((80.0..=121.0).contains(&d), "after reset: {d}");
    }

    #[test]
    fn two_processes_do_not_retry_in_lockstep() {
        // The failover stampede this whole module exists to prevent: every
        // process loses its connections at the same instant, so if they also
        // agree on when to retry, they keep the new primary down themselves.
        let (mut a, mut b) = (policy(11).backoff(), policy(999_331).backoff());
        let mut identical = 0;
        for _ in 0..10 {
            if a.next_delay() == b.next_delay() {
                identical += 1;
            }
        }
        assert_eq!(identical, 0, "two seeds produced the same delay schedule");
    }

    #[test]
    fn zero_jitter_is_exact_and_still_capped() {
        let mut b = RecoveryPolicy {
            jitter_ratio: 0.0,
            ..policy(3)
        }
        .backoff();
        assert_eq!(b.next_delay(), Duration::from_millis(100));
        assert_eq!(b.next_delay(), Duration::from_millis(200));
        for _ in 0..20 {
            assert!(b.next_delay() <= Duration::from_millis(5_000));
        }
    }

    #[test]
    fn jitter_never_produces_a_negative_or_inverted_delay() {
        // ratio 1.0 is the extreme the config allows: it may shorten a delay to
        // zero, but a delay that wrapped negative would panic on `sleep`.
        let mut b = RecoveryPolicy {
            jitter_ratio: 1.0,
            ..policy(42)
        }
        .backoff();
        for _ in 0..50 {
            let d = b.next_delay();
            assert!(d <= Duration::from_millis(10_000), "{d:?}");
        }
    }
}

#[cfg(test)]
mod probe_tests {
    use super::*;
    use crate::config::Role;
    use crate::health::{CatalogState, RoleState};
    use ukiel_catalog::{CatalogPoolConfig, PostgresCatalog};

    /// A catalog pointed at a port nothing listens on: every probe fails fast
    /// and classifiably, which is what an outage looks like from inside.
    fn unreachable_catalog() -> PostgresCatalog {
        PostgresCatalog::connect_lazy_with_config(
            "postgres://u:p@127.0.0.1:1/db",
            &CatalogPoolConfig {
                acquire_timeout: Duration::from_millis(50),
                ..Default::default()
            },
        )
        .unwrap()
    }

    #[tokio::test]
    async fn shutdown_interrupts_the_backoff_immediately() {
        // A SIGTERM during a catalog outage must exit now, not after the full
        // backoff. Without this the fleet takes its maximum delay to drain.
        let catalog = unreachable_catalog();
        let health = HealthRegistry::new(&[Role::Query]);
        let role = health.role(Role::Query);
        let policy = RecoveryPolicy {
            base: Duration::from_secs(30), // long enough that only cancellation ends this
            max: Duration::from_secs(30),
            jitter_ratio: 0.0,
            probe_timeout: Duration::from_millis(200),
            seed: 1,
        };
        let shutdown = CancellationToken::new();
        let token = shutdown.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            token.cancel();
        });

        let started = std::time::Instant::now();
        let result = wait_for_catalog(&catalog, &health, &role, &policy, &shutdown).await;
        assert_eq!(result, Err(Shutdown));
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "waited {:?} — the sleep was not cancellable",
            started.elapsed()
        );
        assert_eq!(health.snapshot().catalog, CatalogState::Unavailable);
    }

    #[tokio::test]
    async fn an_already_cancelled_token_never_probes() {
        let catalog = unreachable_catalog();
        let health = HealthRegistry::new(&[Role::Query]);
        let role = health.role(Role::Query);
        let shutdown = CancellationToken::new();
        shutdown.cancel();
        let policy = RecoveryPolicy {
            base: Duration::from_millis(10),
            max: Duration::from_millis(10),
            jitter_ratio: 0.0,
            probe_timeout: Duration::from_secs(30),
            seed: 1,
        };
        assert_eq!(
            wait_for_catalog(&catalog, &health, &role, &policy, &shutdown).await,
            Err(Shutdown)
        );
    }

    #[tokio::test]
    async fn a_reachable_catalog_marks_it_healthy_without_touching_role_state() {
        // The distinction this plan turns on: reachability is not readiness. The
        // probe proves the catalog answers; only the worker can say it has
        // reloaded its authoritative state.
        let pg = testcontainers_modules::testcontainers::runners::AsyncRunner::start(
            testcontainers_modules::postgres::Postgres::default(),
        )
        .await
        .expect("postgres");
        let port = pg.get_host_port_ipv4(5432).await.unwrap();
        let catalog = PostgresCatalog::connect(&format!(
            "postgres://postgres:postgres@127.0.0.1:{port}/postgres"
        ))
        .await
        .unwrap();

        let health = HealthRegistry::new(&[Role::Query]);
        let role = health.role(Role::Query);
        role.degraded(ukiel_catalog::CatalogErrorClass::Transport);

        let policy = RecoveryPolicy::from_config(&crate::config::CatalogConfig::default(), 1);
        wait_for_catalog(&catalog, &health, &role, &policy, &CancellationToken::new())
            .await
            .expect("catalog is up");

        let snap = health.snapshot();
        assert_eq!(snap.catalog, CatalogState::Healthy);
        assert_eq!(snap.catalog_last_success_seconds, Some(0));
        assert_eq!(
            snap.roles["query"].state,
            RoleState::Degraded,
            "a ping does not make a role healthy — only its own reload does"
        );
        assert!(!snap.ready);
    }
}

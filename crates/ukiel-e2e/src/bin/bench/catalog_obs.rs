//! Saturation attribution (plan 40, Task 3).
//!
//! A throughput knee with no cause attached is a number, not a finding: "one
//! primary does N QPS" cannot decide between a pool knob, PgBouncer, a replica,
//! and sharding. This samples PostgreSQL from a **side connection outside every
//! measured pool** — if the observer had to queue for the same connections the
//! workload is saturating, it would go blind exactly when it matters.
//!
//! The classifier says which resource is the wall. It is deliberately allowed to
//! answer **Ambiguous**: a benchmark that always names a culprit will name the
//! wrong one, and the raw samples are kept so the classification can be argued
//! with rather than believed.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Row};

/// One sample of the primary's state.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Sample {
    pub t_ms: f64,
    /// Backends actively running a query.
    pub active: i64,
    /// Backends idle inside a transaction — a lock/latency smell.
    pub idle_in_txn: i64,
    /// Backends waiting on a lock.
    pub waiting_on_lock: i64,
    /// Backends waiting on I/O.
    pub waiting_on_io: i64,
    pub total_backends: i64,
    /// Cumulative counters; deltas are taken across the run.
    pub xact_commit: i64,
    pub xact_rollback: i64,
    pub blks_read: i64,
    pub blks_hit: i64,
    pub tup_returned: i64,
    pub temp_bytes: i64,
    pub deadlocks: i64,
    /// Postgres backend CPU seconds, from the container's cgroup.
    pub pg_cpu_s: f64,
    /// This driver process's CPU seconds — if the driver is burning more than a
    /// couple of cores, the point is invalid.
    pub driver_cpu_s: f64,
}

/// The environment a run happened in. A run whose recorded settings differ from
/// its label is not reproducible and must be discarded.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Env {
    pub pg_version: String,
    pub max_connections: String,
    pub shared_buffers: String,
    pub work_mem: String,
    pub effective_cache_size: String,
    pub synchronous_commit: String,
    pub fsync: String,
    /// Cores visible to the Postgres container (cgroup quota, or host cores).
    pub pg_cpu_limit: String,
    pub host_cores: usize,
    pub host_ram_gb: f64,
    /// Bytes in `parts` + its indexes vs host RAM — whether the working set fits.
    pub dataset_bytes: i64,
    pub dataset_to_ram_ratio: f64,
}

/// What actually ran out.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Saturation {
    /// The client pool queued while Postgres had headroom — a pool knob's case,
    /// but only with a fleet-wide connection budget.
    PoolQueue,
    /// Backends waiting on locks.
    Lock,
    /// Backends waiting on I/O — the working set does not fit.
    DataIo,
    /// Postgres burned its cores.
    PostgresCpu,
    /// The *driver* was the wall. The point is invalid, not a finding.
    Driver,
    /// Nothing dominated. Said out loud rather than guessed at.
    Ambiguous,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Attribution {
    pub class: Saturation,
    pub evidence: Vec<String>,
    pub pg_cpu_cores_used: f64,
    pub driver_cpu_cores_used: f64,
    pub cache_hit_ratio: f64,
    pub peak_active_backends: i64,
    pub peak_waiting_on_lock: i64,
    pub peak_waiting_on_io: i64,
    pub deadlocks: i64,
    pub rollbacks: i64,
}

/// Classify from the sampled window.
///
/// The thresholds are stated, not hidden, so a reader can disagree with them.
///
/// **The driver rule is a *share*, not a flat core count.** The plan's original
/// "driver > 2 cores ⇒ invalid" is the right intent — a bench that measures
/// itself is worthless — but the wrong test on a big box: driver and Postgres are
/// co-located here, and at 30k QPS the driver legitimately burns ~6 cores
/// deserializing ~670k part rows/s while Postgres burns ~5.75, with 24 cores in
/// the machine. Neither is the wall. So the driver invalidates a point when it is
/// *plausibly the limiter*: it has eaten most of the cores Postgres is not using,
/// or it is doing far more work than the database. Driver CPU is reported on every
/// run regardless, because "the client needs ~1 core per 5k catalog QPS" is itself
/// a fleet-sizing fact, not a bench artifact.
///
/// Postgres is CPU bound above 80% of its quota; I/O and lock waits win when they
/// dominate the active backends. When nothing dominates we say **Ambiguous** — a
/// classifier that always names a culprit will name the wrong one, and that is how
/// a follow-up row gets justified by noise.
pub fn classify(
    samples: &[Sample],
    pg_cores: f64,
    pool_waiters_peak: usize,
    duration_s: f64,
) -> Attribution {
    let mut ev = Vec::new();
    if samples.len() < 2 {
        return Attribution {
            class: Saturation::Ambiguous,
            evidence: vec!["too few samples to attribute".to_string()],
            pg_cpu_cores_used: 0.0,
            driver_cpu_cores_used: 0.0,
            cache_hit_ratio: 0.0,
            peak_active_backends: 0,
            peak_waiting_on_lock: 0,
            peak_waiting_on_io: 0,
            deadlocks: 0,
            rollbacks: 0,
        };
    }
    let (first, last) = (samples.first().unwrap(), samples.last().unwrap());
    let secs = duration_s.max(f64::MIN_POSITIVE);

    // CPU from counter *deltas*, never a point-in-time `docker stats` sample —
    // one instant of a bursty workload tells you nothing.
    let pg_cores_used = (last.pg_cpu_s - first.pg_cpu_s) / secs;
    let driver_cores_used = (last.driver_cpu_s - first.driver_cpu_s) / secs;

    let hits = (last.blks_hit - first.blks_hit) as f64;
    let reads = (last.blks_read - first.blks_read) as f64;
    let hit_ratio = if hits + reads > 0.0 {
        hits / (hits + reads)
    } else {
        1.0
    };

    let peak_active = samples.iter().map(|s| s.active).max().unwrap_or(0);
    let peak_lock = samples.iter().map(|s| s.waiting_on_lock).max().unwrap_or(0);
    let peak_io = samples.iter().map(|s| s.waiting_on_io).max().unwrap_or(0);
    let deadlocks = last.deadlocks - first.deadlocks;
    let rollbacks = last.xact_rollback - first.xact_rollback;

    let cpu_saturated = pg_cores > 0.0 && pg_cores_used / pg_cores >= 0.80;
    let lock_bound = peak_lock > 0 && peak_lock as f64 >= 0.30 * peak_active.max(1) as f64;
    let io_bound = peak_io as f64 >= 0.30 * peak_active.max(1) as f64;
    // The pool is the wall when clients queued while Postgres was NOT busy.
    let pool_bound = pool_waiters_peak > 0 && !cpu_saturated && !io_bound && !lock_bound;

    // Cores the driver could plausibly have used: the machine, minus what
    // Postgres ACTUALLY consumed. Not minus its *quota* — an unlimited Postgres
    // is nominally entitled to every core while using six of them, which would
    // make the driver's budget zero and flag every run as driver-bound.
    let host = std::thread::available_parallelism()
        .map(|n| n.get() as f64)
        .unwrap_or(1.0);
    let driver_budget = (host - pg_cores_used).max(1.0);
    let driver_bound = driver_cores_used > 2.0
        && (driver_cores_used >= 0.80 * driver_budget || driver_cores_used > 2.0 * pg_cores_used);

    let class = if driver_bound {
        ev.push(format!(
            "the DRIVER burned {driver_cores_used:.2} of its ~{driver_budget:.0} available cores \
             (postgres used {pg_cores_used:.2}) — this point measures the bench, not the database"
        ));
        Saturation::Driver
    } else if cpu_saturated {
        ev.push(format!(
            "postgres used {pg_cores_used:.2} of {pg_cores:.1} cores ({:.0}%)",
            100.0 * pg_cores_used / pg_cores
        ));
        Saturation::PostgresCpu
    } else if lock_bound {
        ev.push(format!(
            "{peak_lock} backends peaked waiting on locks vs {peak_active} active"
        ));
        Saturation::Lock
    } else if io_bound {
        ev.push(format!(
            "{peak_io} backends peaked waiting on I/O; buffer hit ratio {:.3}",
            hit_ratio
        ));
        Saturation::DataIo
    } else if pool_bound {
        ev.push(format!(
            "clients queued for connections ({pool_waiters_peak} waiters peak) while postgres used \
             only {pg_cores_used:.2} of {pg_cores:.1} cores — the wall is the POOL, not the database"
        ));
        Saturation::PoolQueue
    } else {
        ev.push(format!(
            "nothing dominated: pg {pg_cpu:.2}/{pg_cores:.1} cores, {peak_lock} lock-waiters, \
             {peak_io} io-waiters, {pool_waiters_peak} pool waiters — not saturated, or not saturated here",
            pg_cpu = pg_cores_used
        ));
        Saturation::Ambiguous
    };

    if deadlocks > 0 {
        ev.push(format!("{deadlocks} deadlocks"));
    }
    if rollbacks > 0 {
        ev.push(format!(
            "{rollbacks} rollbacks (conflict retries land here)"
        ));
    }
    if hit_ratio < 0.99 {
        ev.push(format!(
            "buffer cache hit ratio {hit_ratio:.3} — the working set does not fully fit"
        ));
    }

    Attribution {
        class,
        evidence: ev,
        pg_cpu_cores_used: pg_cores_used,
        driver_cpu_cores_used: driver_cores_used,
        cache_hit_ratio: hit_ratio,
        peak_active_backends: peak_active,
        peak_waiting_on_lock: peak_lock,
        peak_waiting_on_io: peak_io,
        deadlocks,
        rollbacks,
    }
}

/// Samples the primary every ~250 ms from a side connection until stopped.
pub struct Observer {
    stop: Arc<AtomicBool>,
    handle: tokio::task::JoinHandle<Vec<Sample>>,
}

impl Observer {
    pub async fn start(url: &str) -> anyhow::Result<Self> {
        // ONE connection, outside every measured pool. An observer sharing the
        // workload's pool would go blind at exactly the moment the pool
        // saturates — which is the moment we need it.
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect(url)
            .await?;
        let stop = Arc::new(AtomicBool::new(false));
        let s = stop.clone();
        let started = Instant::now();
        let handle = tokio::spawn(async move {
            let mut out = Vec::new();
            while !s.load(Ordering::Relaxed) {
                if let Ok(mut sample) = sample_once(&pool).await {
                    sample.t_ms = started.elapsed().as_secs_f64() * 1000.0;
                    sample.driver_cpu_s = driver_cpu_seconds();
                    out.push(sample);
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
            out
        });
        Ok(Self { stop, handle })
    }

    pub async fn stop(self) -> Vec<Sample> {
        self.stop.store(true, Ordering::Relaxed);
        self.handle.await.unwrap_or_default()
    }
}

async fn sample_once(pool: &PgPool) -> anyhow::Result<Sample> {
    let a = sqlx::query(
        r#"
        SELECT
          count(*) FILTER (WHERE state = 'active') AS active,
          count(*) FILTER (WHERE state = 'idle in transaction') AS idle_in_txn,
          count(*) FILTER (WHERE wait_event_type = 'Lock') AS lock_wait,
          count(*) FILTER (WHERE wait_event_type = 'IO') AS io_wait,
          count(*) AS total
        FROM pg_stat_activity WHERE datname = current_database()
        "#,
    )
    .fetch_one(pool)
    .await?;

    let d = sqlx::query(
        r#"
        SELECT xact_commit, xact_rollback, blks_read, blks_hit,
               tup_returned, temp_bytes, deadlocks
        FROM pg_stat_database WHERE datname = current_database()
        "#,
    )
    .fetch_one(pool)
    .await?;

    Ok(Sample {
        t_ms: 0.0,
        active: a.get::<i64, _>("active"),
        idle_in_txn: a.get::<i64, _>("idle_in_txn"),
        waiting_on_lock: a.get::<i64, _>("lock_wait"),
        waiting_on_io: a.get::<i64, _>("io_wait"),
        total_backends: a.get::<i64, _>("total"),
        xact_commit: d.get::<i64, _>("xact_commit"),
        xact_rollback: d.get::<i64, _>("xact_rollback"),
        blks_read: d.get::<i64, _>("blks_read"),
        blks_hit: d.get::<i64, _>("blks_hit"),
        tup_returned: d.get::<i64, _>("tup_returned"),
        temp_bytes: d.get::<i64, _>("temp_bytes"),
        deadlocks: d.get::<i64, _>("deadlocks"),
        pg_cpu_s: pg_container_cpu_seconds(),
        driver_cpu_s: 0.0,
    })
}

/// Postgres's CPU seconds from the container's cgroup counter.
///
/// A counter delta, not a `docker stats` snapshot: a single instant of a bursty
/// workload says nothing about what it consumed over a window.
fn pg_container_cpu_seconds() -> f64 {
    let Ok(out) = std::process::Command::new("docker")
        .args(["inspect", "-f", "{{.Id}}", "ukiel-postgres-1"])
        .output()
    else {
        return 0.0;
    };
    let id = String::from_utf8_lossy(&out.stdout).trim().to_string();
    if id.is_empty() {
        return 0.0;
    }
    for path in [
        format!("/sys/fs/cgroup/system.slice/docker-{id}.scope/cpu.stat"),
        format!("/sys/fs/cgroup/docker/{id}/cpu.stat"),
    ] {
        if let Ok(text) = std::fs::read_to_string(&path) {
            for line in text.lines() {
                if let Some(v) = line.strip_prefix("usage_usec ") {
                    return v.trim().parse::<f64>().unwrap_or(0.0) / 1e6;
                }
            }
        }
    }
    0.0
}

/// This process's CPU seconds (utime + stime) from `/proc/self/stat`.
fn driver_cpu_seconds() -> f64 {
    let Ok(stat) = std::fs::read_to_string("/proc/self/stat") else {
        return 0.0;
    };
    // Split after the `(comm)` field — the command name can itself contain
    // spaces and parentheses, so the only safe anchor is the LAST ')'.
    let after = match stat.rfind(')') {
        Some(i) => &stat[i + 2..],
        None => return 0.0,
    };
    let f: Vec<&str> = after.split_whitespace().collect();
    let ticks = 100.0; // _SC_CLK_TCK on Linux
    let utime: f64 = f.get(11).and_then(|v| v.parse().ok()).unwrap_or(0.0);
    let stime: f64 = f.get(12).and_then(|v| v.parse().ok()).unwrap_or(0.0);
    (utime + stime) / ticks
}

/// Record the environment once per report. A run whose recorded settings differ
/// from its label is not reproducible, and the label is a claim about the
/// environment.
pub async fn snapshot_env(pool: &PgPool, dataset_bytes: i64) -> anyhow::Result<Env> {
    let mut env = Env {
        host_cores: std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(0),
        dataset_bytes,
        ..Default::default()
    };
    // `SHOW <name>` cannot be parameterized, and sqlx rejects a format!()'d SQL
    // string on principle — rightly. `current_setting($1)` is the parameterized
    // equivalent and does the same job.
    let show = |k: &'static str| async move {
        sqlx::query_scalar::<_, String>("SELECT current_setting($1)")
            .bind(k)
            .fetch_one(pool)
            .await
            .unwrap_or_default()
    };
    env.pg_version = sqlx::query_scalar::<_, String>("SELECT version()")
        .fetch_one(pool)
        .await
        .unwrap_or_default();
    env.max_connections = show("max_connections").await;
    env.shared_buffers = show("shared_buffers").await;
    env.work_mem = show("work_mem").await;
    env.effective_cache_size = show("effective_cache_size").await;
    env.synchronous_commit = show("synchronous_commit").await;
    env.fsync = show("fsync").await;
    env.pg_cpu_limit = pg_cpu_limit();

    if let Ok(mem) = std::fs::read_to_string("/proc/meminfo")
        && let Some(l) = mem.lines().find(|l| l.starts_with("MemTotal:"))
        && let Some(kb) = l
            .split_whitespace()
            .nth(1)
            .and_then(|v| v.parse::<f64>().ok())
    {
        env.host_ram_gb = kb / 1048576.0;
    }
    env.dataset_to_ram_ratio = if env.host_ram_gb > 0.0 {
        dataset_bytes as f64 / (env.host_ram_gb * 1e9)
    } else {
        0.0
    };
    Ok(env)
}

/// The Postgres container's CPU quota, as the classifier's denominator. **This is
/// read, never set** — the bench does not mutate Docker limits, so a sweep that
/// forgets to restore one cannot silently poison every later run.
pub fn pg_cpu_limit() -> String {
    let Ok(out) = std::process::Command::new("docker")
        .args([
            "inspect",
            "-f",
            "{{.HostConfig.NanoCpus}}",
            "ukiel-postgres-1",
        ])
        .output()
    else {
        return "unknown".to_string();
    };
    let nano: f64 = String::from_utf8_lossy(&out.stdout)
        .trim()
        .parse()
        .unwrap_or(0.0);
    if nano <= 0.0 {
        "unlimited".to_string()
    } else {
        format!("{:.1}", nano / 1e9)
    }
}

/// Cores available to Postgres, for the classifier.
pub fn pg_cores() -> f64 {
    match pg_cpu_limit().as_str() {
        "unlimited" | "unknown" => std::thread::available_parallelism()
            .map(|n| n.get() as f64)
            .unwrap_or(1.0),
        s => s.parse().unwrap_or(1.0),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(t: f64, active: i64, lock: i64, io: i64, pg_cpu: f64, drv_cpu: f64) -> Sample {
        Sample {
            t_ms: t,
            active,
            waiting_on_lock: lock,
            waiting_on_io: io,
            pg_cpu_s: pg_cpu,
            driver_cpu_s: drv_cpu,
            blks_hit: 1_000_000,
            ..Default::default()
        }
    }

    /// CPU comes from counter *deltas* over the window, never a point sample.
    #[test]
    fn cpu_is_a_delta_over_the_window() {
        let a = classify(
            &[s(0.0, 8, 0, 0, 10.0, 1.0), s(1000.0, 8, 0, 0, 14.0, 1.1)],
            8.0,
            0,
            1.0,
        );
        assert!((a.pg_cpu_cores_used - 4.0).abs() < 1e-6, "{a:?}");
        assert!((a.driver_cpu_cores_used - 0.1).abs() < 1e-6);
    }

    /// The driver rule is a SHARE, not a flat core count.
    ///
    /// A driver burning 6 cores next to a Postgres burning 5.75, on a 24-core
    /// box, is not the wall — it is an expensive client, which is a fleet-sizing
    /// fact, not an invalid measurement. A driver that has eaten nearly all the
    /// cores Postgres is not using IS the wall, and then the point is discarded.
    #[test]
    fn the_driver_invalidates_a_point_only_when_it_is_plausibly_the_limiter() {
        let host = std::thread::available_parallelism()
            .map(|n| n.get() as f64)
            .unwrap_or(1.0);
        if host < 8.0 {
            return; // the distinction is meaningless on a tiny box
        }

        // Expensive but not limiting: pg 5.75 cores, driver 6.3, plenty spare.
        let ok = classify(
            &[s(0.0, 8, 0, 0, 0.0, 0.0), s(1000.0, 8, 0, 0, 5.75, 6.3)],
            8.0,
            0,
            1.0,
        );
        assert_ne!(ok.class, Saturation::Driver, "{ok:?}");

        // Actually limiting: the driver ate ~everything Postgres did not have.
        let starved = classify(
            &[
                s(0.0, 2, 0, 0, 0.0, 0.0),
                s(1000.0, 2, 0, 0, 0.2, host - 2.0),
            ],
            2.0,
            0,
            1.0,
        );
        assert_eq!(starved.class, Saturation::Driver, "{starved:?}");
        assert!(starved.evidence[0].contains("DRIVER"));

        // Or doing far more work than the database it is supposedly measuring.
        let lopsided = classify(
            &[s(0.0, 2, 0, 0, 0.0, 0.0), s(1000.0, 2, 0, 0, 0.5, 4.0)],
            8.0,
            0,
            1.0,
        );
        assert_eq!(lopsided.class, Saturation::Driver, "{lopsided:?}");
    }

    #[test]
    fn postgres_cpu_lock_and_io_are_distinguished() {
        // 90% of a 4-core quota.
        let cpu = classify(
            &[s(0.0, 8, 0, 0, 0.0, 0.0), s(1000.0, 8, 0, 0, 3.6, 0.1)],
            4.0,
            0,
            1.0,
        );
        assert_eq!(cpu.class, Saturation::PostgresCpu);

        // Locks dominate while CPU is idle.
        let lock = classify(
            &[s(0.0, 10, 6, 0, 0.0, 0.0), s(1000.0, 10, 6, 0, 0.1, 0.1)],
            8.0,
            0,
            1.0,
        );
        assert_eq!(lock.class, Saturation::Lock);

        // I/O waits dominate.
        let io = classify(
            &[s(0.0, 10, 0, 6, 0.0, 0.0), s(1000.0, 10, 0, 6, 0.1, 0.1)],
            8.0,
            0,
            1.0,
        );
        assert_eq!(io.class, Saturation::DataIo);
    }

    /// The pool is only the wall when clients queued while the DATABASE had
    /// headroom. That distinction is the whole reason "just raise the pool size"
    /// is sometimes right and usually wrong.
    #[test]
    fn pool_queue_requires_an_idle_database() {
        let pool = classify(
            &[s(0.0, 2, 0, 0, 0.0, 0.0), s(1000.0, 2, 0, 0, 0.2, 0.1)],
            8.0,
            40,
            1.0,
        );
        assert_eq!(pool.class, Saturation::PoolQueue);

        // Same waiters, but Postgres is pinned: the pool is a symptom, not the cause.
        let cpu = classify(
            &[s(0.0, 8, 0, 0, 0.0, 0.0), s(1000.0, 8, 0, 0, 7.5, 0.1)],
            8.0,
            40,
            1.0,
        );
        assert_eq!(cpu.class, Saturation::PostgresCpu);
    }

    /// A classifier that always names a culprit will name the wrong one.
    #[test]
    fn nothing_dominating_is_reported_as_ambiguous() {
        let a = classify(
            &[s(0.0, 2, 0, 0, 0.0, 0.0), s(1000.0, 2, 0, 0, 0.3, 0.1)],
            8.0,
            0,
            1.0,
        );
        assert_eq!(a.class, Saturation::Ambiguous);
        assert!(a.evidence[0].contains("nothing dominated"));

        // And too few samples never fabricates a cause.
        assert_eq!(classify(&[], 8.0, 0, 1.0).class, Saturation::Ambiguous);
    }
}

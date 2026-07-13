//! The catalog workload engine (plan 40, Task 2): closed-loop knees, open-loop
//! surges, and process-equivalent pool topologies.
//!
//! Three things here are not incidental, because getting any of them wrong
//! produces a capacity number that does not exist:
//!
//! **Open loop measures from the scheduled arrival, not from when a worker got
//! around to it.** That is the whole point of open loop. If latency is timed from
//! the moment the database call begins, a saturated system reports beautiful
//! service times while its queue grows without bound — the classic *coordinated
//! omission*, and the reason "closed-loop only" benchmarks are cheerful liars. A
//! request that waited 4 s to be picked up waited 4 s.
//!
//! **Every operation has a deadline, and a timeout is a failure.** Throughput
//! measured while quietly dropping deadlines is not throughput.
//!
//! **A pool is per process.** N ukield processes each open their own pool; the
//! primary sees N × connections. Modelling a fleet as one big pool hides the
//! global connection wall, which is precisely the thing a "just raise the pool
//! size" fix would walk into.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::bail;
use serde::{Deserialize, Serialize};
use ukiel_catalog::PostgresCatalog;

use crate::catalog::{Demand, capacity_verdict, pg_url};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// N workers, each issuing the next request when the last completes. Finds
    /// the service-capacity knee.
    Closed,
    /// A fixed arrival rate, regardless of whether the system is keeping up.
    /// Exposes queueing, timeouts, shedding and recovery.
    Open,
}

impl Mode {
    pub fn parse(s: &str) -> anyhow::Result<Self> {
        match s {
            "closed" => Ok(Mode::Closed),
            "open" => Ok(Mode::Open),
            other => bail!("unknown --mode '{other}' (closed | open)"),
        }
    }
}

/// How packing keys are chosen. Uniform is the friendly case; **Zipf is the real
/// one** — tenant activity is never uniform, and a hot key concentrates work on
/// the parts that cover it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyDist {
    Uniform,
    Zipf,
}

impl KeyDist {
    pub fn parse(s: &str) -> anyhow::Result<Self> {
        match s {
            "uniform" => Ok(KeyDist::Uniform),
            "zipf" => Ok(KeyDist::Zipf),
            other => bail!("unknown --key-dist '{other}' (uniform | zipf)"),
        }
    }

    /// `n` in [0, 1) → a key in [0, keys). Zipf here is the cheap
    /// inverse-power form: heavily skewed toward low keys, which is all the
    /// benchmark needs (a hot minority), not a distributional claim.
    pub fn pick(self, n: f64, keys: u64) -> i64 {
        let f = match self {
            KeyDist::Uniform => n,
            KeyDist::Zipf => n.powf(3.0),
        };
        ((f * keys as f64) as u64).min(keys.saturating_sub(1)) as i64
    }
}

#[derive(Debug, Clone)]
pub struct LoadConfig {
    pub label: String,
    pub mode: Mode,
    /// Closed loop: worker count. Open loop: ignored.
    pub workers: usize,
    /// Open loop: offered arrivals per second. Closed loop: ignored.
    pub rate: f64,
    pub duration_s: f64,
    pub warmup_s: f64,
    pub timeout_ms: u64,
    /// Process-equivalent pools — each is its own `PostgresCatalog`.
    pub pools: usize,
    pub connections_per_pool: u32,
    pub key_dist: KeyDist,
    pub packing_keys: u64,
    pub scenario: String,
    /// Open loop: the bounded in-flight window. A *policy*, so it is explicit and
    /// reported — too small and the driver sheds load the database would have
    /// taken; too large and every operation queues for a connection and the
    /// latency measured is the driver's, not Postgres's.
    pub max_inflight: usize,
}

/// Everything a run must record for its numbers to be challengeable.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoadReport {
    pub label: String,
    pub workload: String,
    pub mode: String,
    pub scenario: String,
    pub demand: Demand,
    pub derived_read_qps: f64,
    pub derived_write_tps: f64,

    pub pools: usize,
    pub connections_per_pool: u32,
    pub total_connections: usize,
    pub workers: usize,
    pub offered_rate: f64,
    pub duration_s: f64,
    pub warmup_s: f64,
    pub timeout_ms: u64,
    pub key_dist: String,
    pub max_inflight: usize,

    /// Open loop: arrivals the scheduler *offered*. Closed loop: == started.
    pub offered: u64,
    pub started: u64,
    pub completed: u64,
    pub failed: u64,
    pub timed_out: u64,
    /// Open loop: arrivals the bounded queue could not admit. A non-zero value
    /// means the *driver* shed load, and the point is suspect.
    pub dropped: u64,

    pub achieved_qps: f64,
    pub p50_ms: f64,
    pub p95_ms: f64,
    pub p99_ms: f64,
    pub max_ms: f64,
    /// Open loop only: how long arrivals sat before a worker began them. This is
    /// where coordinated omission would hide.
    pub queue_wait_p99_ms: f64,
    /// The database call alone. If this stays flat while `p99_ms` climbs, the
    /// queue is the story and the database is not yet the wall.
    pub service_p50_ms: f64,
    pub service_p99_ms: f64,

    /// Rows the workload actually pulled back — a latency claim without a result
    /// size is not comparable.
    pub result_rows: u64,
    pub result_bytes_est: u64,

    pub verdict_passes: bool,
    pub verdict_reasons: Vec<String>,
    pub achieved_headroom: f64,

    /// What ran out, and the evidence for it. A knee with no cause cannot decide
    /// between a pool knob, a replica and sharding.
    pub saturation: Option<crate::catalog_obs::Attribution>,
    pub env: Option<crate::catalog_obs::Env>,
    /// Raw samples, kept so the classification can be argued with.
    pub samples: Vec<crate::catalog_obs::Sample>,
}

#[derive(Default)]
struct Counters {
    offered: AtomicU64,
    started: AtomicU64,
    completed: AtomicU64,
    /// Completions **inside the measured window** only. Throughput is
    /// `completed_measured / duration`, never `completed / duration` — the
    /// latter divides warmup completions by the post-warmup duration and inflates
    /// every rate by (warmup + duration) / duration. It reported 1,250 op/s for a
    /// 1,000/s offered rate, which is how it was caught.
    completed_measured: AtomicU64,
    failed: AtomicU64,
    timed_out: AtomicU64,
    dropped: AtomicU64,
    rows: AtomicU64,
    bytes: AtomicU64,
}

/// One completed operation: total latency **from scheduled arrival**, and how
/// much of that was spent queued.
struct Sample {
    total_ms: f64,
    queue_ms: f64,
    /// The database call itself. Reported separately so a slow *driver* can never
    /// be mistaken for a slow database — if service is flat while total climbs,
    /// the queue is the story.
    service_ms: f64,
}

/// Nearest-rank percentile: the smallest value at or above which `p`% of the
/// samples fall. `p99` of 100 samples is the 99th, not an interpolation — a tail
/// number that quietly interpolates is a tail number that under-reports.
pub fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let rank = ((p / 100.0) * sorted.len() as f64).ceil() as usize;
    sorted[rank.saturating_sub(1).min(sorted.len() - 1)]
}

/// Build `pools` independent catalogs — **process-equivalents**, not one pool.
pub async fn build_pools(n: usize, per_pool: u32) -> anyhow::Result<Vec<PostgresCatalog>> {
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        out.push(PostgresCatalog::connect_with_pool_size(&pg_url(), per_pool).await?);
    }
    Ok(out)
}

/// Runs one workload under `cfg`. `op` performs a single unit of work and
/// returns the rows it pulled back.
///
/// The closed and open arms differ in exactly one way — when the clock starts —
/// and that difference is the entire reason both exist.
pub async fn run_load<F, Fut>(
    workload: &str,
    cfg: &LoadConfig,
    pools: Arc<Vec<PostgresCatalog>>,
    op: F,
) -> anyhow::Result<LoadReport>
where
    F: Fn(Arc<Vec<PostgresCatalog>>, u64) -> Fut + Send + Sync + Clone + 'static,
    Fut: std::future::Future<Output = anyhow::Result<(u64, u64)>> + Send,
{
    let demand = Demand::named(&cfg.scenario)?;
    // A side connection, outside every measured pool — an observer that queued
    // for the same connections would go blind exactly when the pool saturates.
    let observer = crate::catalog_obs::Observer::start(&pg_url()).await.ok();
    let counters = Arc::new(Counters::default());
    // A std Mutex, not a tokio one: `record` never awaits while holding it, and a
    // tokio Mutex under thousands of concurrent completions serializes the whole
    // driver through the scheduler (measured: it capped the driver at ~1.5k op/s
    // while the database was doing 26k).
    let samples: Arc<std::sync::Mutex<Vec<Sample>>> = Arc::new(std::sync::Mutex::new(Vec::new()));

    let started_at = Instant::now();
    let warmup = Duration::from_secs_f64(cfg.warmup_s);
    let total = Duration::from_secs_f64(cfg.warmup_s + cfg.duration_s);
    let timeout = Duration::from_millis(cfg.timeout_ms);

    // Warmup is excluded by *timestamp*, not by discarding the first N — the
    // system is warming up in wall-clock, not in operation count.
    let measuring = move |t: Instant| t.duration_since(started_at) >= warmup;

    match cfg.mode {
        Mode::Closed => {
            let mut handles = Vec::new();
            for w in 0..cfg.workers {
                let (c, s, p, o) = (counters.clone(), samples.clone(), pools.clone(), op.clone());
                handles.push(tokio::spawn(async move {
                    let mut seq = w as u64;
                    loop {
                        let now = Instant::now();
                        if now.duration_since(started_at) >= total {
                            break;
                        }
                        seq = seq.wrapping_add(0x9E37_79B9_7F4A_7C15);
                        c.started.fetch_add(1, Ordering::Relaxed);
                        c.offered.fetch_add(1, Ordering::Relaxed);
                        let began = Instant::now();
                        let res = tokio::time::timeout(timeout, o(p.clone(), seq)).await;
                        let elapsed = began.elapsed().as_secs_f64() * 1000.0;
                        record(&c, &s, res, elapsed, 0.0, elapsed, measuring(began));
                    }
                }));
            }
            for h in handles {
                let _ = h.await;
            }
        }
        Mode::Open => {
            // A bounded channel feeding a **fixed** worker pool. Spawning a task
            // per arrival does not survive 30k/s: the spawn plus wake cost lands
            // on the driver, and the driver ends up measuring itself (observed:
            // it capped at ~1.5k op/s while the database was serving 26k).
            //
            // The channel bound is the in-flight policy, and it is explicit:
            // too small and the driver sheds load Postgres would have taken;
            // too large and every arrival queues for a connection and the latency
            // reported is the driver's, not the database's.
            const TICK: Duration = Duration::from_micros(250);
            let n_workers = cfg.workers.max(1);
            // ONE QUEUE PER WORKER, not one shared receiver behind a mutex: a
            // worker holding that mutex across `recv().await` blocks every other
            // worker from even waiting, so exactly one worker runs at a time.
            // (Measured: it capped the driver at ~800 op/s while the database was
            // serving 26k, and the queue-wait it reported was entirely the
            // driver's own.) The scheduler round-robins arrivals across them.
            let per_worker_bound = (cfg.max_inflight / n_workers).max(1);
            let mut txs = Vec::with_capacity(n_workers);
            let mut handles = Vec::with_capacity(n_workers);
            for _ in 0..n_workers {
                let (tx, mut rx) = tokio::sync::mpsc::channel::<(Instant, u64)>(per_worker_bound);
                txs.push(tx);
                let (c, s, p, o) = (counters.clone(), samples.clone(), pools.clone(), op.clone());
                handles.push(tokio::spawn(async move {
                    loop {
                        let Some((scheduled, seq)) = rx.recv().await else {
                            break;
                        };
                        let began = Instant::now();
                        // Scheduled -> actually started: where coordinated
                        // omission would hide, so it is measured, not assumed 0.
                        let queue_ms =
                            began.saturating_duration_since(scheduled).as_secs_f64() * 1000.0;
                        c.started.fetch_add(1, Ordering::Relaxed);
                        // The deadline runs from ARRIVAL — queue time counts
                        // against it, exactly as it does for a real client.
                        let budget =
                            timeout.saturating_sub(began.saturating_duration_since(scheduled));
                        let res = tokio::time::timeout(budget, o(p.clone(), seq)).await;
                        let now = Instant::now();
                        let total_ms =
                            now.saturating_duration_since(scheduled).as_secs_f64() * 1000.0;
                        let service_ms =
                            now.saturating_duration_since(began).as_secs_f64() * 1000.0;
                        record(
                            &c,
                            &s,
                            res,
                            total_ms,
                            queue_ms,
                            service_ms,
                            measuring(began),
                        );
                    }
                }));
            }

            // The scheduler: emit every arrival that has come *due*, each stamped
            // with its own exact scheduled instant. One sleep per arrival cannot
            // work above a few thousand/s — the timer granularity alone is many
            // times the inter-arrival gap at 30k/s.
            let interval = 1.0 / cfg.rate.max(f64::MIN_POSITIVE);
            let mut seq = 0u64;
            let mut emitted = 0u64;
            loop {
                let elapsed = Instant::now().duration_since(started_at);
                if elapsed >= total {
                    break;
                }
                let due = (elapsed.as_secs_f64() / interval) as u64;
                while emitted < due {
                    let scheduled = started_at + Duration::from_secs_f64(emitted as f64 * interval);
                    emitted += 1;
                    seq = seq.wrapping_add(0x9E37_79B9_7F4A_7C15);
                    counters.offered.fetch_add(1, Ordering::Relaxed);
                    // Round-robin, falling through to the next worker whose queue
                    // has room. Only when *every* worker is full is the arrival
                    // shed — and then it is shed loudly.
                    let start = (emitted as usize) % txs.len();
                    let mut placed = false;
                    for i in 0..txs.len() {
                        if txs[(start + i) % txs.len()]
                            .try_send((scheduled, seq))
                            .is_ok()
                        {
                            placed = true;
                            break;
                        }
                    }
                    if !placed {
                        counters.dropped.fetch_add(1, Ordering::Relaxed);
                    }
                }
                tokio::time::sleep(TICK).await;
            }
            drop(txs);
            // Work offered inside the window still has to finish; abandoning it
            // would discard exactly the tail we came for.
            for h in handles {
                let _ = h.await;
            }
        }
    }

    let obs_samples = match observer {
        Some(o) => o.stop().await,
        None => Vec::new(),
    };
    let saturation = Some(crate::catalog_obs::classify(
        &obs_samples,
        crate::catalog_obs::pg_cores(),
        0, // pool waiters: sqlx does not expose them; the canary is separate
        cfg.duration_s + cfg.warmup_s,
    ));
    let env = crate::catalog_obs::snapshot_env(pools[0].pool_for_tests(), 0)
        .await
        .ok();

    let mut samples = samples.lock().expect("samples lock");
    let mut totals: Vec<f64> = samples.iter().map(|s| s.total_ms).collect();
    let mut queues: Vec<f64> = samples.iter().map(|s| s.queue_ms).collect();
    let mut services: Vec<f64> = samples.iter().map(|s| s.service_ms).collect();
    samples.clear();
    totals.sort_by(|a, b| a.partial_cmp(b).unwrap());
    queues.sort_by(|a, b| a.partial_cmp(b).unwrap());
    services.sort_by(|a, b| a.partial_cmp(b).unwrap());

    let completed = counters.completed.load(Ordering::Relaxed);
    let completed_measured = counters.completed_measured.load(Ordering::Relaxed);
    let achieved_qps = completed_measured as f64 / cfg.duration_s.max(f64::MIN_POSITIVE);
    let p99 = percentile(&totals, 99.0);
    let timed_out = counters.timed_out.load(Ordering::Relaxed);
    let verdict = capacity_verdict(&demand, achieved_qps, p99, timed_out);
    let load = demand.derive();

    Ok(LoadReport {
        label: cfg.label.clone(),
        workload: workload.to_string(),
        mode: format!("{:?}", cfg.mode).to_lowercase(),
        scenario: cfg.scenario.clone(),
        demand: demand.clone(),
        derived_read_qps: load.read_qps,
        derived_write_tps: load.write_tps,
        pools: cfg.pools,
        connections_per_pool: cfg.connections_per_pool,
        total_connections: cfg.pools * cfg.connections_per_pool as usize,
        workers: cfg.workers,
        offered_rate: cfg.rate,
        duration_s: cfg.duration_s,
        warmup_s: cfg.warmup_s,
        timeout_ms: cfg.timeout_ms,
        key_dist: format!("{:?}", cfg.key_dist).to_lowercase(),
        max_inflight: cfg.max_inflight,
        offered: counters.offered.load(Ordering::Relaxed),
        started: counters.started.load(Ordering::Relaxed),
        completed,
        failed: counters.failed.load(Ordering::Relaxed),
        timed_out,
        dropped: counters.dropped.load(Ordering::Relaxed),
        achieved_qps,
        p50_ms: percentile(&totals, 50.0),
        p95_ms: percentile(&totals, 95.0),
        p99_ms: p99,
        max_ms: totals.last().copied().unwrap_or(0.0),
        queue_wait_p99_ms: percentile(&queues, 99.0),
        service_p50_ms: percentile(&services, 50.0),
        service_p99_ms: percentile(&services, 99.0),
        result_rows: counters.rows.load(Ordering::Relaxed),
        result_bytes_est: counters.bytes.load(Ordering::Relaxed),
        verdict_passes: verdict.passes,
        verdict_reasons: verdict.reasons,
        achieved_headroom: verdict.achieved_headroom,
        saturation,
        env,
        samples: obs_samples,
    })
}

/// Conservation: every started operation lands in exactly one of completed /
/// failed / timed_out. A benchmark whose counters do not add up is not
/// measuring anything.
#[allow(clippy::too_many_arguments)]
fn record(
    c: &Counters,
    samples: &std::sync::Mutex<Vec<Sample>>,
    res: Result<anyhow::Result<(u64, u64)>, tokio::time::error::Elapsed>,
    total_ms: f64,
    queue_ms: f64,
    service_ms: f64,
    measuring: bool,
) {
    match res {
        Err(_) => {
            c.timed_out.fetch_add(1, Ordering::Relaxed);
        }
        Ok(Err(_)) => {
            c.failed.fetch_add(1, Ordering::Relaxed);
        }
        Ok(Ok((rows, bytes))) => {
            c.completed.fetch_add(1, Ordering::Relaxed);
            if measuring {
                c.completed_measured.fetch_add(1, Ordering::Relaxed);
            }
            c.rows.fetch_add(rows, Ordering::Relaxed);
            c.bytes.fetch_add(bytes, Ordering::Relaxed);
        }
    }
    if measuring {
        samples.lock().expect("samples lock").push(Sample {
            total_ms,
            queue_ms,
            service_ms,
        });
    }
}

pub fn print_report(r: &LoadReport) {
    println!(
        "\n{} / {} / {} — {} pools x {} conns = {} total\n\
         offered {} | started {} | completed {} | failed {} | TIMED OUT {} | driver-dropped {}\n\
         achieved {:.0} op/s   p50 {:.1} | p95 {:.1} | p99 {:.1} | max {:.1} ms\n\
         \x20 service p50 {:.1} / p99 {:.1} ms   queue-wait p99 {:.1} ms\n\
         rows returned {}   demand {:.0} QPS -> headroom {:.2}x  [{}]",
        r.label,
        r.workload,
        r.mode,
        r.pools,
        r.connections_per_pool,
        r.total_connections,
        r.offered,
        r.started,
        r.completed,
        r.failed,
        r.timed_out,
        r.dropped,
        r.achieved_qps,
        r.p50_ms,
        r.p95_ms,
        r.p99_ms,
        r.max_ms,
        r.service_p50_ms,
        r.service_p99_ms,
        r.queue_wait_p99_ms,
        r.result_rows,
        r.derived_read_qps,
        r.achieved_headroom,
        if r.verdict_passes { "PASS" } else { "FAIL" },
    );
    if let Some(a) = &r.saturation {
        println!(
            "  saturation: {:?}   (pg {:.2} cores, driver {:.2} cores, buffer-hit {:.3}, \
             peak active {})",
            a.class,
            a.pg_cpu_cores_used,
            a.driver_cpu_cores_used,
            a.cache_hit_ratio,
            a.peak_active_backends
        );
        for e in &a.evidence {
            println!("    - {e}");
        }
    }
    for reason in &r.verdict_reasons {
        println!("  ! {reason}");
    }
    if r.dropped > 0 {
        println!(
            "  ! {} arrivals were shed by the DRIVER, not the database — this point is invalid",
            r.dropped
        );
    }
}

// ---------------------------------------------------------------------------
// Workloads
// ---------------------------------------------------------------------------

/// `bench catalog read …` — the query-admission path.
///
/// This is exactly what every product query does before a single byte of Parquet
/// is read: `live_parts_pruned` on the tenant's key. If this saturates, query
/// admission saturates, however fast the scan layer is.
pub async fn read(cfg: LoadConfig, ht_name: &str) -> anyhow::Result<()> {
    let pools = Arc::new(build_pools(cfg.pools, cfg.connections_per_pool).await?);
    let ht = pools[0].get_hypertable(ht_name).await?;
    let (ht_id, keys, dist) = (ht.id, cfg.packing_keys, cfg.key_dist);

    let report = run_load("read", &cfg, pools, move |pools, seq| async move {
        // Spread across process-equivalent pools, as a fleet would.
        let cat = &pools[(seq as usize) % pools.len()];
        let n = (seq >> 11) as f64 / (u64::MAX >> 11) as f64;
        let key = dist.pick(n, keys);
        let parts = cat.live_parts_pruned(ht_id, Some(key), &[]).await?;
        // Result size travels with every latency claim: 3 ms returning 2 rows and
        // 3 ms returning 2,000 are not the same measurement.
        let bytes: u64 = parts
            .iter()
            .map(|p| {
                p.meta.path.len() as u64
                    + p.meta
                        .column_stats
                        .as_ref()
                        .map_or(0, |s| s.to_string().len() as u64)
            })
            .sum();
        Ok((parts.len() as u64, bytes))
    })
    .await?;

    print_report(&report);
    crate::report::write_json(&format!("catalog-read-{}.json", cfg.label), &report)?;
    if report.dropped > 0 {
        bail!("the driver shed load — fix the driver before trusting this point");
    }
    Ok(())
}

/// `bench catalog write …` — the durable-write path.
///
/// Uses the **real** `commit_with_offsets`, per-part INSERT loop and all. The
/// point is to price what the product actually does, including the parts of it we
/// might want to change; a hand-rolled fast INSERT would measure a system that
/// does not exist.
///
/// Each worker owns a distinct lane (Kafka-partition-equivalent), so commits do
/// not conflict — conflict is its own workload.
pub async fn write(cfg: LoadConfig, ht_name: &str, parts_per_commit: usize) -> anyhow::Result<()> {
    use ukiel_catalog::OffsetRange;
    use ukiel_core::{CommitOp, PartMeta};

    let pools = Arc::new(build_pools(cfg.pools, cfg.connections_per_pool).await?);
    let ht = pools[0].get_hypertable(ht_name).await?;
    let ht_id = ht.id;
    let topic = format!("bench-cat-{}", cfg.label);

    // The write workload GROWS the catalog it is measuring — every commit adds
    // live parts, and a later read workload against the same fixture would be
    // reading a bigger table than the one it was seeded to. (Measured the hard
    // way: an unrecorded write run inflated the hot hypertable from 600 to ~80k
    // live parts, and every read after it looked 25x slower for no reason.)
    //
    // So: record the cardinality this run started and ended with, print the
    // delta, and tell the operator to reseed. The fixture is a measurement input,
    // not a scratchpad.
    let live_before = pools[0].live_parts(ht_id, None).await?.len() as i64;

    // A FIXED set of lanes, each with a monotonically advancing offset — which is
    // what ingest actually looks like (one lane per Kafka partition). A fresh
    // lane per commit would be conflict-free too, but it would grow
    // `ingest_offsets` without bound and measure a system nobody runs.
    //
    // The offset CAS (`WHERE next_offset <= range.first`) is per (topic,
    // partition), so distinct lanes never contend: conflict is its own workload.
    let lanes: usize = cfg.workers.max(1);
    let offsets: Arc<Vec<AtomicU64>> = Arc::new((0..lanes).map(|_| AtomicU64::new(0)).collect());
    let seq_no = Arc::new(AtomicU64::new(0));
    let report = {
        let topic = topic.clone();
        run_load("write", &cfg, pools.clone(), move |pools, seq| {
            let topic = topic.clone();
            let offsets = offsets.clone();
            let seq_no = seq_no.clone();
            async move {
                let cat = &pools[(seq as usize) % pools.len()];
                let n = seq_no.fetch_add(1, Ordering::Relaxed) as i64;
                let my_lane = (n as usize % offsets.len()) as i32;
                // Claim this lane's next offset slot. `fetch_add` is what keeps
                // two commits on the same lane from racing the CAS.
                let first = offsets[my_lane as usize].fetch_add(1, Ordering::Relaxed) as i64;

                let parts: Vec<PartMeta> = (0..parts_per_commit)
                    .map(|i| PartMeta {
                        path: format!("ht/{}/L0/{n}-{i}.parquet", ht_id.0),
                        partition_values: serde_json::json!({"day": "2026-01-01"}),
                        packing_key_min: n % 1_000_000,
                        packing_key_max: (n % 1_000_000) + 50,
                        row_count: 100_000,
                        size_bytes: 50_000_000,
                        level: 0,
                        column_stats: Some(serde_json::json!({
                            "tenant_id": {"min": n % 1_000_000, "max": (n % 1_000_000) + 50},
                            "ts": {"min": 1767225600000i64, "max": 1769817600000i64}
                        })),
                    })
                    .collect();

                let ranges = [OffsetRange {
                    topic: topic.clone(),
                    partition: my_lane,
                    first,
                    last: first,
                }];
                cat.commit_with_offsets(ht_id, CommitOp::Add { parts }, &ranges)
                    .await?;
                Ok((parts_per_commit as u64, 0))
            }
        })
        .await?
    };

    let live_after = pools[0].live_parts(ht_id, None).await?.len() as i64;

    print_report(&report);
    println!(
        "  commit path: the real commit_with_offsets, {parts_per_commit} parts/commit, one INSERT\n\
         \x20 per part — that per-part loop is exactly what a batching follow-up would target.\n\
         \x20 catalog grew {live_before} -> {live_after} live parts (+{}). RESEED before the next\n\
         \x20 read point, or it will measure this run's leftovers.",
        live_after - live_before
    );
    let mut value = serde_json::to_value(&report)?;
    value["live_parts_before"] = live_before.into();
    value["live_parts_after"] = live_after.into();
    value["parts_per_commit"] = parts_per_commit.into();
    crate::report::write_json(&format!("catalog-write-{}.json", cfg.label), &value)?;
    Ok(())
}

/// `bench catalog conflict …` — the optimistic-REPLACE storm.
///
/// This is the compactor's real shape: read the live parts of a partition,
/// commit `REPLACE(old → new)`, and lose the race if another worker tombstoned
/// any input first. The losing side re-reads and replans — so throughput here is
/// not "commits/s" but **useful swaps/s**, and the difference between them is
/// rollback tax.
///
/// A conflict benchmark that reports attempts as throughput is measuring how fast
/// it can fail.
pub async fn conflict(
    cfg: LoadConfig,
    ht_name: &str,
    shape: &str,
    coordination: &str,
) -> anyhow::Result<()> {
    use ukiel_core::{CommitOp, PartMeta};

    // `optimistic` is plan 40's arm: every contender does the work and loses at
    // REPLACE. `partition-lease` is plan 41's: a contender that does not own the
    // partition never plans a merge at all. In the catalog-only bench the
    // "work" it skips is a live-parts read; in the product it is a Parquet
    // read, a k-way merge, and an upload — which is why the two arms understate
    // the real difference.
    let leased = match coordination {
        "optimistic" => false,
        "partition-lease" => true,
        other => {
            anyhow::bail!("unknown --coordination '{other}' (expected optimistic|partition-lease)")
        }
    };
    let coordination = coordination.to_string();

    let pools = Arc::new(build_pools(cfg.pools, cfg.connections_per_pool).await?);
    let ht = pools[0].get_hypertable(ht_name).await?;
    let ht_id = ht.id;

    // Each contender stands in for one compactor process, so each gets its own
    // owner identity. Sharing an owner across contenders would defeat the lease
    // rather than test it: reacquisition by the *same* owner is deliberately
    // idempotent (a worker retrying must not invalidate the fence its own
    // in-flight merge holds), so same-owner contenders are all let straight
    // through. Measured, not assumed: the first version of this arm handed every
    // worker in a pool one uuid and reported 603 conflicts *under a lease*.
    let lease_ttl = Duration::from_secs(60);
    // A contender that finds the partition owned: the whole point of the lease.
    let skipped = Arc::new(AtomicU64::new(0));
    let acquired = Arc::new(AtomicU64::new(0));

    // Contention shape. `same-partition` is the worst case (every contender
    // targets one partition); `spread` gives each its own; `zipf` makes one
    // partition hot while others are quiet — the realistic middle.
    let shape = shape.to_string();
    let attempts = Arc::new(AtomicU64::new(0));
    let conflicts = Arc::new(AtomicU64::new(0));
    // USEFUL swaps are counted here, not taken from `completed`. A merge that
    // found nothing left to merge is a no-op, and counting it as a swap is how a
    // conflict benchmark reports 62,000 useful merges/s from a partition that
    // only ever held 25 parts. Ask me how I know.
    let useful = Arc::new(AtomicU64::new(0));
    let noops = Arc::new(AtomicU64::new(0));

    let report = {
        let (attempts, conflicts, useful, noops, shape, skipped, acquired) = (
            attempts.clone(),
            conflicts.clone(),
            useful.clone(),
            noops.clone(),
            shape.clone(),
            skipped.clone(),
            acquired.clone(),
        );
        run_load("conflict", &cfg, pools.clone(), move |pools, seq| {
            let (attempts, conflicts, useful, noops, shape, skipped, acquired) = (
                attempts.clone(),
                conflicts.clone(),
                useful.clone(),
                noops.clone(),
                shape.clone(),
                skipped.clone(),
                acquired.clone(),
            );
            async move {
                let cat = &pools[(seq as usize) % pools.len()];
                let owner = uuid::Uuid::new_v4();
                let day = match shape.as_str() {
                    "same-partition" => 0,
                    "zipf" => {
                        let n = (seq >> 11) as f64 / (u64::MAX >> 11) as f64;
                        (n.powf(3.0) * 30.0) as u64
                    }
                    _ => seq % 30,
                };
                let partition = serde_json::json!({
                    "day": format!("2026-01-{:02}", day + 1)
                });

                // Leased arm: claim the partition BEFORE planning anything. A
                // contender that does not own it stops here — one cheap upsert
                // instead of a merge it would have thrown away at REPLACE.
                let lease = if leased {
                    match cat
                        .try_acquire_compaction_lease(ht_id, &partition, owner, lease_ttl)
                        .await?
                    {
                        Some(lease) => {
                            acquired.fetch_add(1, Ordering::Relaxed);
                            Some(lease)
                        }
                        None => {
                            skipped.fetch_add(1, Ordering::Relaxed);
                            return Ok((0, 0));
                        }
                    }
                } else {
                    None
                };

                // The compactor's loop: read fresh state, plan, swap, and on a
                // conflict re-read and try again. Bounded, because an unbounded
                // retry loop would report a latency that is really a livelock.
                let mut result = Ok((0, 0));
                for _ in 0..5 {
                    attempts.fetch_add(1, Ordering::Relaxed);
                    // The partition read the real compactor does (plan 41), not
                    // a whole-hypertable scan filtered in Rust. On a fixture deep
                    // enough to have merge headroom, the scan is what the run
                    // would end up measuring — and it is not what the product
                    // does. Both arms read the same way, so the A/B stays fair.
                    let live = cat.live_partition_parts(ht_id, &partition).await?;
                    let inputs: Vec<_> = live.iter().take(4).collect();
                    if inputs.len() < 2 {
                        // The partition is merged out. NOT a useful swap — and if
                        // this dominates, the fixture was exhausted and the point
                        // is about the fixture, not about conflict.
                        noops.fetch_add(1, Ordering::Relaxed);
                        break;
                    }
                    let old: Vec<_> = inputs.iter().map(|p| p.id).collect();
                    let new = vec![PartMeta {
                        path: format!("ht/{}/L1/{}.parquet", ht_id.0, uuid::Uuid::new_v4()),
                        partition_values: partition.clone(),
                        packing_key_min: inputs
                            .iter()
                            .map(|p| p.meta.packing_key_min)
                            .min()
                            .unwrap(),
                        packing_key_max: inputs
                            .iter()
                            .map(|p| p.meta.packing_key_max)
                            .max()
                            .unwrap(),
                        row_count: inputs.iter().map(|p| p.meta.row_count).sum(),
                        size_bytes: inputs.iter().map(|p| p.meta.size_bytes).sum(),
                        level: 1,
                        column_stats: None,
                    }];
                    // Same REPLACE either way — the leased arm adds the fence,
                    // it does not remove the optimistic check. A conflict in the
                    // leased arm would mean a NON-compactor writer raced us,
                    // which is exactly what it is still there to catch.
                    let committed = match &lease {
                        Some(lease) => {
                            cat.commit_compaction_replace(ht_id, lease, old, new, None)
                                .await
                        }
                        None => {
                            cat.commit(ht_id, CommitOp::Replace { old, new }, None)
                                .await
                        }
                    };
                    match committed {
                        Ok(_) => {
                            useful.fetch_add(1, Ordering::Relaxed);
                            result = Ok((inputs.len() as u64, 0));
                            break;
                        }
                        Err(ukiel_catalog::CatalogError::Conflict { .. }) => {
                            // Lost the race. Every byte of work in this attempt is
                            // thrown away — that is the rollback tax, and it is
                            // what a "commits attempted" number would hide.
                            conflicts.fetch_add(1, Ordering::Relaxed);
                            tokio::time::sleep(Duration::from_millis(2)).await;
                        }
                        Err(e) => {
                            result = Err(e.into());
                            break;
                        }
                    }
                }
                // Hand the partition back so a peer can take it immediately;
                // otherwise the TTL, not the work, would set the pace.
                if let Some(lease) = &lease {
                    cat.release_compaction_lease(lease).await?;
                }
                result
            }
        })
        .await?
    };

    let a = attempts.load(Ordering::Relaxed);
    let c = conflicts.load(Ordering::Relaxed);
    let u = useful.load(Ordering::Relaxed);
    let no = noops.load(Ordering::Relaxed);
    let sk = skipped.load(Ordering::Relaxed);
    let acq = acquired.load(Ordering::Relaxed);
    let secs = report.duration_s.max(1.0);
    print_report(&report);
    println!(
        "  conflict '{shape}' [{coordination}]: {a} attempts -> {u} USEFUL swaps, {c} conflicts, {no} no-ops\n  \
         useful swaps/s {:.1}   |   attempted commits/s {:.1} (the number that would flatter us)\n  \
         rollback tax: {:.1}% of attempts thrown away",
        u as f64 / secs,
        a as f64 / secs,
        if a > 0 {
            100.0 * c as f64 / a as f64
        } else {
            0.0
        },
    );
    if leased {
        // The coordination cost, priced: what the lease charges Postgres per
        // second, against the merges it stopped from being thrown away.
        println!(
            "  leases: {acq} acquired + {sk} skipped (owned by a peer) = {:.1} lease ops/s\n  \
             {sk} contenders never planned a merge — in the product, that is the Parquet\n  \
             read/merge/upload they did not pay for before losing at REPLACE",
            (acq + sk) as f64 / secs,
        );
    }
    if no > u * 2 {
        println!(
            "  ! {no} no-ops vs {u} real swaps — the partition merged OUT during the run, so this\n  \
             point measures an exhausted fixture, not contention. Seed more parts per partition."
        );
    }
    let mut v = serde_json::to_value(&report)?;
    v["conflict_shape"] = shape.into();
    v["coordination"] = coordination.clone().into();
    v["lease_acquired"] = acq.into();
    v["lease_skipped"] = sk.into();
    v["lease_ops_per_s"] = ((acq + sk) as f64 / secs).into();
    v["attempts"] = a.into();
    v["conflicts"] = c.into();
    v["useful_swaps"] = u.into();
    v["noops"] = no.into();
    v["useful_swaps_per_s"] = (u as f64 / secs).into();
    crate::report::write_json(&format!("catalog-conflict-{}.json", cfg.label), &v)?;
    Ok(())
}

/// `bench catalog mixed …` — the demand envelope with the background workers on.
///
/// Reads and writes at the scenario's derived rates, **plus** everything the
/// fleet does whether or not a customer is looking: feed polls, finalization
/// candidates, the L0 backpressure probe, GC's pending-object sweep, and the
/// collector's aggregates. A capacity number measured with the background off is
/// a number for a system nobody runs.
pub async fn mixed(cfg: LoadConfig, ht_name: &str) -> anyhow::Result<()> {
    use ukiel_core::{CommitOp, PartMeta};

    let pools = Arc::new(build_pools(cfg.pools, cfg.connections_per_pool).await?);
    let ht = pools[0].get_hypertable(ht_name).await?;
    let ht_id = ht.id;
    let demand = Demand::named(&cfg.scenario)?;
    let load = demand.derive();

    // The op mix mirrors the derived demand: reads dominate, writes are steady,
    // background is a small constant. Sampled per operation from the sequence, so
    // the ratio holds without a scheduler per stream.
    let total = load.read_qps + load.write_tps + load.background_qps;
    let read_share = load.read_qps / total;
    let write_share = (load.read_qps + load.write_tps) / total;
    let topic = format!("bench-mixed-{}", cfg.label);
    let (keys, dist) = (cfg.packing_keys, cfg.key_dist);
    let seq_no = Arc::new(AtomicU64::new(0));

    let report = run_load("mixed", &cfg, pools.clone(), move |pools, seq| {
        let topic = topic.clone();
        let seq_no = seq_no.clone();
        async move {
            let cat = &pools[(seq as usize) % pools.len()];
            let n = (seq >> 11) as f64 / (u64::MAX >> 11) as f64;

            if n < read_share {
                // Query admission.
                let key = dist.pick(n / read_share, keys);
                let parts = cat.live_parts_pruned(ht_id, Some(key), &[]).await?;
                Ok((parts.len() as u64, 0))
            } else if n < write_share {
                // Ingest flush: the real commit path plus the guardrail probe the
                // flusher actually runs before every flush.
                let _ = cat.max_live_l0_parts(ht_id, &[]).await?;
                let i = seq_no.fetch_add(1, Ordering::Relaxed) as i64;
                let parts = vec![PartMeta {
                    path: format!("ht/{}/L0/mix-{i}.parquet", ht_id.0),
                    partition_values: serde_json::json!({"day": "2026-01-01"}),
                    packing_key_min: i % 1_000_000,
                    packing_key_max: (i % 1_000_000) + 50,
                    row_count: 100_000,
                    size_bytes: 50_000_000,
                    level: 0,
                    column_stats: None,
                }];
                let ranges = [ukiel_catalog::OffsetRange {
                    topic: topic.clone(),
                    partition: (i % 64) as i32,
                    first: i,
                    last: i,
                }];
                cat.commit_with_offsets(ht_id, CommitOp::Add { parts }, &ranges)
                    .await?;
                Ok((1, 0))
            } else {
                // Background: what the fleet does regardless of customers.
                match (seq >> 3) % 3 {
                    0 => {
                        let ev = cat
                            .changes_since(ht_id, ukiel_core::CommitId(0), 100)
                            .await?;
                        Ok((ev.len() as u64, 0))
                    }
                    1 => {
                        let c = cat.finalize_candidates(ht_id).await?;
                        Ok((c.len() as u64, 0))
                    }
                    _ => {
                        let n = cat.max_live_l0_parts(ht_id, &[]).await?;
                        Ok((n as u64, 0))
                    }
                }
            }
        }
    })
    .await?;

    print_report(&report);
    println!(
        "  mix: {:.0}% read / {:.0}% write / {:.0}% background (from the '{}' demand envelope)",
        read_share * 100.0,
        (write_share - read_share) * 100.0,
        (1.0 - write_share) * 100.0,
        cfg.scenario
    );
    crate::report::write_json(&format!("catalog-mixed-{}.json", cfg.label), &report)?;
    Ok(())
}

/// `bench catalog connections …` — the fleet arriving at once.
///
/// Not "can one pool open 16 connections", but: **P process-equivalent pools all
/// starting together**, as a deploy or a restart does. The wall this finds is
/// global (`max_connections`), and no amount of per-process pool tuning moves it —
/// which is exactly why a pool knob without a fleet-wide connection budget can
/// make things worse.
pub async fn connections(pools_n: usize, per_pool: u32, ht_name: &str) -> anyhow::Result<()> {
    let max_conn: String = {
        let probe = PostgresCatalog::connect_with_pool_size(&pg_url(), 1).await?;
        sqlx::query_scalar("SELECT current_setting('max_connections')")
            .fetch_one(probe.pool_for_tests())
            .await?
    };
    println!(
        "connection surge: {pools_n} process-equivalent pools x {per_pool} conns = {} total, \
         against max_connections = {max_conn}",
        pools_n * per_pool as usize
    );

    let started = Instant::now();
    let mut handles = Vec::new();
    for _ in 0..pools_n {
        handles.push(tokio::spawn(async move {
            let t = Instant::now();
            let r = PostgresCatalog::connect_with_pool_size(&pg_url(), per_pool).await;
            (t.elapsed().as_secs_f64() * 1000.0, r)
        }));
    }

    let mut establish_ms = Vec::new();
    let mut failed = 0usize;
    let mut cats = Vec::new();
    for h in handles {
        match h.await {
            Ok((ms, Ok(c))) => {
                establish_ms.push(ms);
                cats.push(c);
            }
            Ok((_, Err(e))) => {
                failed += 1;
                if failed == 1 {
                    println!("  first failure: {e}");
                }
            }
            Err(_) => failed += 1,
        }
    }
    establish_ms.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let surge_ms = started.elapsed().as_secs_f64() * 1000.0;

    // **sqlx pools connect lazily.** Constructing P pools of N proves nothing — it
    // opens almost no sockets, and a surge test reporting "128 connections, 0
    // failures" against `max_connections = 100` is reporting a fiction.
    //
    // Force every pool to hold ALL of its connections at once, behind a barrier so
    // the peak is simultaneous rather than smeared across a ramp. That is what a
    // fleet under load does, and it is the moment the global wall exists.
    let ht = cats[0].get_hypertable(ht_name).await?;
    let ht_id = ht.id;
    let wanted = cats.len() * per_pool as usize;
    let gate = Arc::new(tokio::sync::Barrier::new(wanted));
    let mut probes = Vec::new();
    for c in &cats {
        for _ in 0..per_pool {
            let (c, gate) = (c.clone(), gate.clone());
            probes.push(tokio::spawn(async move {
                gate.wait().await;
                c.live_parts_pruned(ht_id, Some(1), &[]).await.is_ok()
            }));
        }
    }
    let mut admitted = 0usize;
    let mut refused = 0usize;
    for p in probes {
        match p.await {
            Ok(true) => admitted += 1,
            _ => refused += 1,
        }
    }

    println!(
        "  established {} / {pools_n} pools in {surge_ms:.0} ms (p50 {:.0} / p99 {:.0} ms), {failed} failed to construct\n  \
         under SIMULTANEOUS load: {admitted} / {wanted} concurrent connections admitted, {refused} refused",
        cats.len(),
        percentile(&establish_ms, 50.0),
        percentile(&establish_ms, 99.0),
    );
    let limit: i64 = max_conn.parse().unwrap_or(0);
    if refused > 0 || wanted as i64 > limit {
        println!(
            "  ! the wall here is GLOBAL: max_connections = {max_conn}, this fleet wants {wanted}.\n  \
             No per-process pool setting moves it — a pool knob without a fleet-wide connection\n  \
             budget just walks a bigger fleet into the same wall, faster."
        );
    }
    crate::report::write_json(
        &format!("catalog-connections-{pools_n}x{per_pool}.json"),
        &serde_json::json!({
            "pools": pools_n, "connections_per_pool": per_pool,
            "total_connections": pools_n * per_pool as usize,
            "max_connections": max_conn,
            "established": cats.len(), "failed_to_construct": failed,
            "concurrent_admitted": admitted, "concurrent_refused": refused,
            "concurrent_wanted": cats.len() * per_pool as usize,
            "surge_ms": surge_ms,
            "establish_p50_ms": percentile(&establish_ms, 50.0),
            "establish_p99_ms": percentile(&establish_ms, 99.0),
        }),
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percentiles_are_exact_at_the_edges() {
        let v: Vec<f64> = (1..=100).map(|i| i as f64).collect();
        assert_eq!(percentile(&v, 50.0), 50.0);
        assert_eq!(percentile(&v, 99.0), 99.0);
        assert_eq!(percentile(&v, 100.0), 100.0);
        assert_eq!(percentile(&[], 99.0), 0.0);
    }

    /// Zipf must actually concentrate, or the "hot key" workload is uniform in
    /// disguise and the whole skew story is untested.
    #[test]
    fn zipf_concentrates_where_uniform_spreads() {
        let keys = 1_000_000u64;
        let n = 10_000;
        let hot = |d: KeyDist| {
            (0..n)
                .map(|i| d.pick(i as f64 / n as f64, keys))
                .filter(|&k| k < (keys / 100) as i64) // the top 1% of keys
                .count()
        };
        let (u, z) = (hot(KeyDist::Uniform), hot(KeyDist::Zipf));
        assert!(
            z > u * 4,
            "zipf must concentrate on the hot 1% ({z} vs uniform {u})"
        );
        // And it must stay in range.
        for d in [KeyDist::Uniform, KeyDist::Zipf] {
            for i in 0..100 {
                let k = d.pick(i as f64 / 100.0, keys);
                assert!((0..keys as i64).contains(&k));
            }
        }
        assert!(KeyDist::parse("nope").is_err());
    }

    /// Coordinated omission is the failure this engine exists to avoid, so the
    /// definition is pinned: an open-loop operation's latency runs from its
    /// *scheduled arrival*, and the queue wait is part of it.
    #[test]
    fn open_loop_latency_includes_the_queue_wait() {
        let scheduled = Instant::now();
        let began = scheduled + Duration::from_millis(400);
        let finished = began + Duration::from_millis(10);

        let queue_ms = began.duration_since(scheduled).as_secs_f64() * 1000.0;
        let total_ms = finished.duration_since(scheduled).as_secs_f64() * 1000.0;
        let service_ms = finished.duration_since(began).as_secs_f64() * 1000.0;

        assert!((queue_ms - 400.0).abs() < 1.0);
        assert!((service_ms - 10.0).abs() < 1.0);
        // The number a coordinated-omitting benchmark would report is 10 ms.
        // The number a client experienced is 410 ms. We report 410.
        assert!((total_ms - 410.0).abs() < 1.0);
        assert!(total_ms > service_ms * 10.0);
    }

    #[test]
    fn mode_parsing_is_strict() {
        assert_eq!(Mode::parse("closed").unwrap(), Mode::Closed);
        assert_eq!(Mode::parse("open").unwrap(), Mode::Open);
        assert!(Mode::parse("half-open").is_err());
    }
}

//! Shared reporting helpers for the macro perf bench (plan 30): warm-median
//! timing (plan-11 convention) and machine-readable JSON output under
//! `bench/results/`.

use std::path::PathBuf;
use std::time::Instant;

use serde::Serialize;

fn results_dir() -> PathBuf {
    PathBuf::from("bench/results")
}

/// Median of a sample (lower-middle for even counts — a bench, not a stats
/// package). Empty input is 0.
pub fn median(mut xs: Vec<f64>) -> f64 {
    if xs.is_empty() {
        return 0.0;
    }
    xs.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    xs[xs.len() / 2]
}

/// Times `f` once as warm-up (discarded), then `iters` times, returning the
/// median wall-clock milliseconds. The plan-11 warm-median convention.
pub async fn warm_median_ms<F, Fut, T>(iters: usize, mut f: F) -> anyhow::Result<f64>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<T>>,
{
    f().await?; // warm-up
    let mut samples = Vec::with_capacity(iters);
    for _ in 0..iters.max(1) {
        let start = Instant::now();
        f().await?;
        samples.push(start.elapsed().as_secs_f64() * 1000.0);
    }
    Ok(median(samples))
}

/// Writes `value` as pretty JSON to `bench/results/<name>` and returns the path.
pub fn write_json<T: Serialize>(name: &str, value: &T) -> anyhow::Result<PathBuf> {
    std::fs::create_dir_all(results_dir())?;
    let path = results_dir().join(name);
    serde_json::to_writer_pretty(std::fs::File::create(&path)?, value)?;
    Ok(path)
}

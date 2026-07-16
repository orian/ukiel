//! Wall / CPU / peak-RSS sampling around exactly the timed region.
//!
//! `getrusage(RUSAGE_SELF)` is process-cumulative, so a sample is the *delta* across the
//! timed write; the decode and projection that happen during preparation are outside the
//! clock and never counted. Peak RSS is `ru_maxrss` (KiB on Linux) — a process-lifetime
//! high-water mark, recorded as-is because a per-sample peak is not observable this way.

use std::time::Instant;

/// A resource-usage snapshot.
#[derive(Debug, Clone, Copy)]
pub struct Rusage {
    pub user_seconds: f64,
    pub sys_seconds: f64,
    /// Peak resident set size in bytes, or `None` if unobservable.
    pub max_rss_bytes: Option<u64>,
}

/// Read the current process resource usage.
pub fn rusage() -> Rusage {
    // SAFETY: getrusage writes a fully-initialised rusage into the zeroed struct.
    unsafe {
        let mut ru: libc::rusage = std::mem::zeroed();
        if libc::getrusage(libc::RUSAGE_SELF, &mut ru) != 0 {
            return Rusage {
                user_seconds: 0.0,
                sys_seconds: 0.0,
                max_rss_bytes: None,
            };
        }
        Rusage {
            user_seconds: ru.ru_utime.tv_sec as f64 + ru.ru_utime.tv_usec as f64 / 1e6,
            sys_seconds: ru.ru_stime.tv_sec as f64 + ru.ru_stime.tv_usec as f64 / 1e6,
            // Linux reports ru_maxrss in KiB.
            max_rss_bytes: Some((ru.ru_maxrss.max(0) as u64) * 1024),
        }
    }
}

/// One timed measurement of a closure. The clock brackets exactly `f`; nothing before or
/// after it is counted.
#[derive(Debug, Clone, Copy)]
pub struct Timing {
    pub wall_seconds: f64,
    pub user_seconds: f64,
    pub sys_seconds: f64,
    pub max_rss_bytes: Option<u64>,
}

/// Time `f`, returning both its result and the wall/CPU/peak-RSS it consumed.
pub fn time<T>(f: impl FnOnce() -> T) -> (T, Timing) {
    let before = rusage();
    let start = Instant::now();
    let out = f();
    let wall = start.elapsed().as_secs_f64();
    let after = rusage();
    (
        out,
        Timing {
            wall_seconds: wall,
            user_seconds: (after.user_seconds - before.user_seconds).max(0.0),
            sys_seconds: (after.sys_seconds - before.sys_seconds).max(0.0),
            max_rss_bytes: after.max_rss_bytes,
        },
    )
}

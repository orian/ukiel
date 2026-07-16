//! Wall / CPU / peak-RSS sampling around exactly the timed region (getrusage delta).

use std::time::Instant;

#[derive(Debug, Clone, Copy)]
pub struct Timing {
    pub wall_seconds: f64,
    pub user_seconds: f64,
    pub sys_seconds: f64,
    pub max_rss_bytes: Option<u64>,
}

fn rusage() -> (f64, f64, Option<u64>) {
    // SAFETY: getrusage fills the zeroed struct.
    unsafe {
        let mut ru: libc::rusage = std::mem::zeroed();
        if libc::getrusage(libc::RUSAGE_SELF, &mut ru) != 0 {
            return (0.0, 0.0, None);
        }
        let user = ru.ru_utime.tv_sec as f64 + ru.ru_utime.tv_usec as f64 / 1e6;
        let sys = ru.ru_stime.tv_sec as f64 + ru.ru_stime.tv_usec as f64 / 1e6;
        (user, sys, Some((ru.ru_maxrss.max(0) as u64) * 1024))
    }
}

/// Time `f`, returning its result and the wall/CPU/peak-RSS it consumed.
pub fn time<T>(f: impl FnOnce() -> T) -> (T, Timing) {
    let (u0, s0, _) = rusage();
    let start = Instant::now();
    let out = f();
    let wall = start.elapsed().as_secs_f64();
    let (u1, s1, rss) = rusage();
    (
        out,
        Timing {
            wall_seconds: wall,
            user_seconds: (u1 - u0).max(0.0),
            sys_seconds: (s1 - s0).max(0.0),
            max_rss_bytes: rss,
        },
    )
}

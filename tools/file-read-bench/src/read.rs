//! Execute a range plan: read exactly the requested bytes into a BLAKE3 checksum sink,
//! counting read syscalls. Never opens a Parquet footer — this is raw I/O only.

use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

use anyhow::{Context, Result, bail};

use crate::ranges::Range;

/// What one execution of a range plan read.
pub struct ReadOutcome {
    pub requested_bytes: u64,
    pub returned_bytes: u64,
    pub ranges: u64,
    pub reads: u64,
    /// The checksum over the returned bytes, so an accidentally-empty or reordered read is
    /// caught and repeated samples can be proven identical.
    pub checksum: String,
}

/// Read every range from the given files (indexed by the plan) into a checksum sink.
pub fn execute(files: &[std::path::PathBuf], plan_ranges: &[Range]) -> Result<ReadOutcome> {
    // Open each referenced file once.
    let mut handles: Vec<Option<std::fs::File>> = (0..files.len()).map(|_| None).collect();
    let mut hasher = blake3::Hasher::new();
    let mut buf = vec![0u8; 1 << 20];
    let mut requested = 0u64;
    let mut returned = 0u64;
    let mut reads = 0u64;

    for r in plan_ranges {
        requested += r.length;
        let file = &files[r.file];
        if handles[r.file].is_none() {
            handles[r.file] = Some(
                std::fs::File::open(file).with_context(|| format!("opening {}", file.display()))?,
            );
        }
        let fh = handles[r.file].as_mut().unwrap();
        fh.seek(SeekFrom::Start(r.offset))
            .with_context(|| format!("seeking {} to {}", file.display(), r.offset))?;
        let mut remaining = r.length as usize;
        while remaining > 0 {
            let want = remaining.min(buf.len());
            let n = fh
                .read(&mut buf[..want])
                .with_context(|| format!("reading {} at {}", file.display(), r.offset))?;
            reads += 1;
            if n == 0 {
                bail!(
                    "short read on {}: wanted {} more bytes at range offset {}",
                    file.display(),
                    remaining,
                    r.offset
                );
            }
            hasher.update(&buf[..n]);
            returned += n as u64;
            remaining -= n;
        }
    }

    Ok(ReadOutcome {
        requested_bytes: requested,
        returned_bytes: returned,
        ranges: plan_ranges.len() as u64,
        reads,
        checksum: hasher.finalize().to_hex().to_string(),
    })
}

/// The byte lengths of a list of files, in order.
pub fn file_lengths(files: &[std::path::PathBuf]) -> Result<Vec<u64>> {
    files
        .iter()
        .map(|p| {
            std::fs::metadata(p)
                .map(|m| m.len())
                .with_context(|| format!("stat {}", p.display()))
        })
        .collect()
}

/// Guard: refuse a path that is not a plain file.
pub fn assert_regular(path: &Path) -> Result<()> {
    let m = std::fs::metadata(path).with_context(|| format!("stat {}", path.display()))?;
    if !m.is_file() {
        bail!("{} is not a regular file", path.display());
    }
    Ok(())
}

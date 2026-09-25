//! Linux page-cache primitives: probe residency with `mincore`, warm by reading, evict
//! with `posix_fadvise(POSIX_FADV_DONTNEED)`. Never `drop_caches` — only the explicit
//! benchmark files are touched, so another process's files are left resident.

use std::path::Path;

use anyhow::{Result, bail};

/// Resident and total 4 KiB-ish pages of one file. A zero-length file is `(0, 0)`.
#[cfg(target_os = "linux")]
pub fn page_residency(path: &Path) -> Result<(u64, u64)> {
    use std::os::unix::io::AsRawFd;

    let file = std::fs::File::open(path)
        .map_err(|e| anyhow::anyhow!("opening {} for mincore: {e}", path.display()))?;
    let len = file.metadata()?.len() as usize;
    if len == 0 {
        return Ok((0, 0));
    }
    let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) } as usize;
    if page == 0 {
        bail!("could not read page size");
    }
    let n_pages = len.div_ceil(page);
    // SAFETY: a read-only shared mapping of the whole file; unmapped before returning.
    let addr = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            len,
            libc::PROT_READ,
            libc::MAP_SHARED,
            file.as_raw_fd(),
            0,
        )
    };
    if addr == libc::MAP_FAILED {
        bail!("mmap of {} failed", path.display());
    }
    let mut vec = vec![0u8; n_pages];
    let rc = unsafe { libc::mincore(addr, len, vec.as_mut_ptr()) };
    let resident = if rc == 0 {
        vec.iter().filter(|b| *b & 1 == 1).count() as u64
    } else {
        0
    };
    unsafe {
        libc::munmap(addr, len);
    }
    if rc != 0 {
        bail!("mincore of {} failed", path.display());
    }
    Ok((resident, n_pages as u64))
}

/// Pull a file's bytes into the OS page cache by reading it end to end.
pub fn warm(path: &Path) -> Result<u64> {
    let bytes =
        std::fs::read(path).map_err(|e| anyhow::anyhow!("warming {}: {e}", path.display()))?;
    Ok(bytes.len() as u64)
}

/// Advise the kernel to drop this file's pages from the page cache. Read-only immutable
/// files carry no dirty pages, so `DONTNEED` is sufficient; there is no `drop_caches`.
#[cfg(target_os = "linux")]
pub fn evict(path: &Path) -> Result<()> {
    use std::os::unix::io::AsRawFd;

    let file = std::fs::File::open(path)
        .map_err(|e| anyhow::anyhow!("opening {} to evict: {e}", path.display()))?;
    let rc = unsafe { libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_DONTNEED) };
    if rc != 0 {
        bail!(
            "posix_fadvise(DONTNEED) on {} returned {rc}",
            path.display()
        );
    }
    Ok(())
}

/// Whether this platform supports the residency primitives.
pub const SUPPORTED: bool = cfg!(target_os = "linux");

#[cfg(not(target_os = "linux"))]
pub fn page_residency(_path: &Path) -> Result<(u64, u64)> {
    bail!("mincore page-residency probing is only implemented on Linux")
}

#[cfg(not(target_os = "linux"))]
pub fn evict(_path: &Path) -> Result<()> {
    bail!("posix_fadvise eviction is only implemented on Linux")
}

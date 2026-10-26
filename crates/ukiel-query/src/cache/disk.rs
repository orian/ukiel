//! One owner and one budget for published files, pinned reads and temporary fills.
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::ops::Range;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use futures::{StreamExt, stream::BoxStream};
use lru::LruCache;
use object_store::{PutPayload, Result};

use super::{CacheConfig, io_err};

const LOCK: &str = ".ukiel-cache.lock";

#[derive(Debug, Default, Clone, Copy)]
pub struct CacheStats {
    pub resident_bytes: u64,
    pub reserved_bytes: u64,
    pub entries: usize,
    pub inflight_entries: usize,
    pub size_entries: usize,
}

#[derive(Debug)]
struct Entry {
    bytes: u64,
    readers: usize,
}

#[derive(Debug)]
struct State {
    entries: LruCache<PathBuf, Entry>,
    pending: HashMap<PathBuf, u64>,
    resident: u64,
    reserved: u64,
}

impl State {
    fn report(&self) {
        metrics::gauge!("cache_resident_bytes").set(self.resident as f64);
        metrics::gauge!("cache_reserved_bytes").set(self.reserved as f64);
        metrics::gauge!("cache_entries").set(self.entries.len() as f64);
        metrics::gauge!("cache_inflight_entries").set(self.pending.len() as f64);
    }
}

#[derive(Debug)]
pub(super) struct DiskCache {
    dir: PathBuf,
    max_bytes: u64,
    max_entries: usize,
    state: Mutex<State>,
    // Also held by active fills/read streams after the store itself is dropped.
    _owner: File,
}

pub(super) fn rejected(reason: &'static str) {
    metrics::counter!("cache_admission_failures_total", "reason" => reason).increment(1);
}

fn cache_error(operation: &'static str) {
    metrics::counter!("cache_errors_total", "operation" => operation).increment(1);
}

fn managed_name(name: &str) -> bool {
    let (base, temporary) = match name.rsplit_once(".tmp-") {
        Some((base, id)) if uuid::Uuid::parse_str(id).is_ok() => (base, true),
        Some(_) => return false,
        None => (name, false),
    };
    let hash = |s: &str| s.len() == 64 && s.bytes().all(|b| b.is_ascii_hexdigit());
    if base.strip_suffix(".file").is_some_and(hash) {
        return true;
    }
    let object = if let Some((object, chunk)) = base.rsplit_once(".chunk-") {
        if chunk.parse::<u64>().is_err() {
            return false;
        }
        if let Some((key, size)) = object.rsplit_once('.')
            && hash(key)
            && size.parse::<u64>().is_ok_and(|size| size > 0)
        {
            return true;
        }
        object
    } else {
        base
    };
    // Legacy production names are flattened immutable Parquet part paths.
    // Arbitrary old/custom filenames require a fresh dedicated directory.
    let Some(part) = object
        .strip_suffix(".parquet")
        .or_else(|| temporary.then_some(object))
    else {
        return false;
    };
    let fields: Vec<_> = part.split("__").collect();
    fields.len() == 4
        && fields[0] == "ht"
        && fields[1].parse::<i64>().is_ok()
        && fields[2]
            .strip_prefix('L')
            .is_some_and(|s| s.parse::<u16>().is_ok())
        && uuid::Uuid::parse_str(fields[3]).is_ok()
}

impl DiskCache {
    pub(super) fn open(dir: PathBuf, config: &CacheConfig) -> std::io::Result<Arc<Self>> {
        if config.max_bytes == 0 || config.max_entries == 0 || config.chunk_size == 0 {
            return Err(std::io::Error::other("cache capacities must be positive"));
        }
        std::fs::create_dir_all(&dir)?;
        let owner = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(dir.join(LOCK))?;
        owner.try_lock().map_err(std::io::Error::other)?;
        // Validate before removing anything: a typo in cache_dir must never
        // turn other application state into eviction candidates.
        for item in std::fs::read_dir(&dir)? {
            let item = item?;
            if item.file_name() == LOCK {
                continue;
            }
            if !item.file_type()?.is_file() || !managed_name(&item.file_name().to_string_lossy()) {
                return Err(std::io::Error::other(
                    "unrecognized contents in cache directory",
                ));
            }
        }
        let cache = Arc::new(Self {
            dir,
            max_bytes: config.max_bytes,
            max_entries: config.max_entries,
            state: Mutex::new(State {
                entries: LruCache::unbounded(),
                pending: HashMap::new(),
                resident: 0,
                reserved: 0,
            }),
            _owner: owner,
        });
        // A dedicated flat cache directory contains only disposable cache data
        // and our owner lock. Reindex incrementally, keeping startup memory bounded.
        for item in std::fs::read_dir(&cache.dir)? {
            let item = item?;
            if item.file_name() == LOCK {
                continue;
            }
            if !item.file_type()?.is_file() {
                return Err(std::io::Error::other(
                    "cache directory must contain only regular cache files",
                ));
            }
            let path = item.path();
            let name = item.file_name().to_string_lossy().into_owned();
            let temporary = name
                .rsplit_once(".tmp-")
                .is_some_and(|(_, id)| uuid::Uuid::parse_str(id).is_ok());
            let bytes = item.metadata()?.len();
            let mut state = cache.state.lock().expect("cache state");
            if temporary || bytes > cache.max_bytes {
                std::fs::remove_file(path)?;
            } else {
                if !cache.make_room(&mut state, bytes, true) {
                    return Err(std::io::Error::other(
                        "cannot trim cache to configured limits",
                    ));
                }
                state.resident += bytes;
                state.entries.put(path, Entry { bytes, readers: 0 });
            }
        }
        cache.state.lock().expect("cache state").report();
        Ok(cache)
    }

    fn make_room(&self, state: &mut State, bytes: u64, entry: bool) -> bool {
        if bytes > self.max_bytes {
            rejected("oversize");
            return false;
        }
        while bytes
            > self
                .max_bytes
                .saturating_sub(state.resident + state.reserved)
            || state
                .entries
                .len()
                .saturating_add(state.pending.len())
                .saturating_add(usize::from(entry))
                > self.max_entries
        {
            let Some(path) = state
                .entries
                .iter()
                .rev()
                .find(|(_, e)| e.readers == 0)
                .map(|(p, _)| p.clone())
            else {
                rejected("capacity");
                return false;
            };
            if let Err(error) = std::fs::remove_file(&path)
                && error.kind() != std::io::ErrorKind::NotFound
            {
                cache_error("evict");
                rejected("io");
                return false;
            }
            let evicted = state.entries.pop(&path).expect("indexed entry");
            state.resident -= evicted.bytes;
            metrics::counter!("cache_evictions_total").increment(1);
            state.report();
        }
        true
    }

    pub(super) fn stats(&self) -> CacheStats {
        let state = self.state.lock().expect("cache state");
        CacheStats {
            resident_bytes: state.resident,
            reserved_bytes: state.reserved,
            entries: state.entries.len(),
            inflight_entries: state.pending.len(),
            size_entries: 0,
        }
    }

    pub(super) fn read(self: &Arc<Self>, name: &str) -> Option<CachedRead> {
        let path = self.dir.join(name);
        let mut state = self.state.lock().expect("cache state");
        let entry = state.entries.get_mut(&path)?;
        let file = match File::open(&path).and_then(|f| {
            if f.metadata()?.len() != entry.bytes {
                return Err(std::io::Error::other("invalid cache file length"));
            }
            Ok(f)
        }) {
            Ok(file) => file,
            Err(_) => {
                cache_error("read");
                if std::fs::remove_file(&path).is_ok() || !path.exists() {
                    let entry = state.entries.pop(&path).expect("indexed entry");
                    state.resident -= entry.bytes;
                    state.report();
                }
                return None;
            }
        };
        entry.readers += 1;
        Some(CachedRead {
            file,
            bytes: entry.bytes,
            _lease: ReadLease {
                cache: self.clone(),
                path,
            },
        })
    }

    pub(super) fn reserve(self: &Arc<Self>, name: &str, bytes: u64) -> Option<Fill> {
        let path = self.dir.join(name);
        let mut state = self.state.lock().expect("cache state");
        if state.entries.contains(&path) || state.pending.contains_key(&path) {
            rejected("duplicate");
            return None;
        }
        if !self.make_room(&mut state, bytes, true) {
            return None;
        }
        // Append instead of replacing the extension; whole and chunk names must
        // never share a staging path. create_new also refuses preexisting links.
        let tmp = self
            .dir
            .join(format!("{name}.tmp-{}", uuid::Uuid::new_v4()));
        let file = match OpenOptions::new().write(true).create_new(true).open(&tmp) {
            Ok(file) => file,
            Err(_) => {
                cache_error("create");
                rejected("io");
                return None;
            }
        };
        state.pending.insert(path.clone(), bytes);
        state.reserved += bytes;
        state.report();
        Some(Fill {
            file: Some(file),
            cache: self.clone(),
            path,
            tmp,
            bytes,
            published: false,
        })
    }
}

#[derive(Debug)]
struct ReadLease {
    cache: Arc<DiskCache>,
    path: PathBuf,
}

impl Drop for ReadLease {
    fn drop(&mut self) {
        if let Some(entry) = self
            .cache
            .state
            .lock()
            .expect("cache state")
            .entries
            .peek_mut(&self.path)
        {
            entry.readers -= 1;
        }
    }
}

#[derive(Debug)]
pub(super) struct CachedRead {
    file: File,
    pub(super) bytes: u64,
    _lease: ReadLease,
}

impl CachedRead {
    pub(super) async fn range(mut self, range: Range<u64>) -> Result<Bytes> {
        // The blocking task owns the read lease, including after cancellation.
        tokio::task::spawn_blocking(move || {
            self.file.seek(SeekFrom::Start(range.start))?;
            let mut bytes = vec![0; (range.end - range.start) as usize];
            self.file.read_exact(&mut bytes)?;
            drop(self);
            Ok::<_, std::io::Error>(Bytes::from(bytes))
        })
        .await
        .map_err(super::generic_err)?
        .map_err(io_err)
    }

    pub(super) fn stream(self, range: Range<u64>) -> BoxStream<'static, Result<Bytes>> {
        futures::stream::try_unfold((self, range), |(mut read, mut range)| async move {
            if range.start == range.end {
                return Ok(None);
            }
            tokio::task::spawn_blocking(move || {
                read.file.seek(SeekFrom::Start(range.start))?;
                let len = (range.end - range.start).min(1024 * 1024) as usize;
                let mut bytes = vec![0; len];
                read.file.read_exact(&mut bytes)?;
                range.start += len as u64;
                Ok::<_, std::io::Error>(Some((Bytes::from(bytes), (read, range))))
            })
            .await
            .map_err(super::generic_err)?
            .map_err(io_err)
        })
        .boxed()
    }
}

#[derive(Debug)]
pub(super) struct Fill {
    // Closed before releasing the reservation in Drop (including abandoned I/O).
    file: Option<File>,
    cache: Arc<DiskCache>,
    path: PathBuf,
    tmp: PathBuf,
    bytes: u64,
    published: bool,
}

impl Fill {
    pub(super) fn grow(&mut self, bytes: u64) -> bool {
        let mut state = self.cache.state.lock().expect("cache state");
        if !self.cache.make_room(&mut state, bytes, false) {
            return false;
        }
        self.bytes += bytes;
        state.reserved += bytes;
        *state.pending.get_mut(&self.path).expect("pending fill") = self.bytes;
        state.report();
        true
    }

    pub(super) fn write_at(&self, payload: &PutPayload, mut offset: u64) -> std::io::Result<()> {
        use std::os::unix::fs::FileExt;
        if offset
            .checked_add(payload.content_length() as u64)
            .is_none_or(|end| end > self.bytes)
        {
            return Err(std::io::Error::other("cache write exceeds reservation"));
        }
        for bytes in payload {
            self.file
                .as_ref()
                .expect("open fill")
                .write_all_at(bytes, offset)?;
            offset += bytes.len() as u64;
        }
        Ok(())
    }

    pub(super) fn publish(mut self) -> std::io::Result<()> {
        self.file.as_mut().expect("open fill").flush()?;
        if self.file.as_ref().expect("open fill").metadata()?.len() != self.bytes {
            return Err(std::io::Error::other("incomplete cache fill"));
        }
        drop(self.file.take());
        let mut state = self.cache.state.lock().expect("cache state");
        std::fs::rename(&self.tmp, &self.path)?;
        state.pending.remove(&self.path);
        state.reserved -= self.bytes;
        state.resident += self.bytes;
        state.entries.put(
            self.path.clone(),
            Entry {
                bytes: self.bytes,
                readers: 0,
            },
        );
        self.published = true;
        state.report();
        Ok(())
    }
}

impl Drop for Fill {
    fn drop(&mut self) {
        if self.published {
            return;
        }
        drop(self.file.take());
        // If unlink fails, retain the charge and block further admission rather
        // than undercounting disk usage. Startup retries orphan cleanup.
        if let Err(error) = std::fs::remove_file(&self.tmp)
            && error.kind() != std::io::ErrorKind::NotFound
        {
            cache_error("cleanup");
            return;
        }
        let mut state = self.cache.state.lock().expect("cache state");
        state.pending.remove(&self.path);
        state.reserved -= self.bytes;
        state.report();
    }
}

//! The disk store: what `http` calls, and a naive implementation behind it.
//!
//! # The contract (frozen: a signature change goes through the lead)
//!
//! `http` calls [`Store::read`], [`Store::stat`] and [`Store::admit`], then
//! [`Reservation::write`]. The `store` lot replaces the implementation without changing any
//! signature in this file. What any implementation must guarantee:
//!
//! * **Bounded size (D12).** Bytes of stored objects + bytes reserved by PUTs in flight + bytes
//!   evicted but not yet deleted never exceed `max_size`. Sizes are charged rounded up to 4 KiB.
//!   [`Store::admit`] evicts least recently used entries until the reservation fits, and refuses
//!   with [`DropReason::NoSpace`] when even evicting everything evictable would not make it fit or
//!   when the disk would drop below `min_free_disk`. [`Store::open`] does the same at start-up:
//!   when the scanned objects already exceed `max_size` (a smaller `max_size` than last run), it
//!   evicts the least recently used entries until they fit, before [`Store::is_ready`] turns
//!   `true`. The naive implementation does not, and serves a store above its limit.
//! * **Pinned keys (D12).** A key is not evictable from its reservation until it is stored or the
//!   reservation is dropped. An eviction decided before an object was stored never deletes it: a
//!   deletion still pending carries a generation, so it cannot remove a newer version.
//! * **Atomic writes (D13).** The body goes to `tmp/<unique>`, then is renamed to the object path.
//!   The object is visible only after the rename, and `201` follows it. Concurrent writes of one
//!   key all succeed, the last rename wins and the key is counted once. A reader that already
//!   opened the file keeps the old version. The data directory is `flock`ed at [`Store::open`]
//!   before `tmp/` is emptied.
//! * **Durability (D32).** A `state` file in the data directory records the durability mode of
//!   the run and `clean = true|false`. [`Store::open`], under the `flock` and before any mutation
//!   or served read, loads the previous state, then writes the new one with `clean = false` and
//!   syncs it (file, then directory). If the previous state is absent, unreadable, or
//!   `clean = false` after a `purge-on-crash` run, it purges **all** objects before serving
//!   anything; an interrupted purge leaves `clean = false` and starts over at the next start. It
//!   counts the purge with [`Metrics::crash_purge`] and logs a `warn`. A run in `fsync` mode that
//!   stops badly does not purge, unless the run before it was `purge-on-crash` and not clean.
//!   [`Store::close`] closes admissions, waits for every writer and background task (a missed
//!   shutdown deadline forbids `clean = true`), runs `syncfs` on the objects' file system and
//!   **checks that it succeeded**, then writes `clean = true` and syncs it. A `syncfs` or I/O error
//!   leaves `clean = false`. With [`Durability::Fsync`] a write does `flush`, then `sync_all` of
//!   the file before the rename, and never syncs the directory: a recovered name always denotes a
//!   complete version, and the last PUT may vanish. With [`Durability::PurgeOnCrash`] a write
//!   syncs nothing. This is Linux-only (macOS has no `syncfs`, which the configuration refuses).
//!   The boot identifier is not consulted. Crash tests cover each persistent transition: before
//!   and after `state` is written, mid-purge, during shutdown, and a failing `syncfs`.
//! * **Index and readiness (D14).** The index is rebuilt from `objects/` at start-up, the recency
//!   from `mtime`. Until it is built [`Store::is_ready`] is `false`, [`Store::admit`] answers
//!   [`DropReason::NotReady`] without waiting, and reads are served straight from disk. Recency
//!   is decided under the lock and written back to `mtime` outside it, at most once per
//!   `touch_interval`, and for every late entry in [`Store::close`].
//! * **Reads (D14, D33).** [`Store::read`] retries once when the file vanishes between the lookup
//!   and the open, then answers `None`. [`Object::into_stream`] has two paths: an object of
//!   64 KiB or less is read by one blocking task into a buffer of its size and leaves as a single
//!   `Bytes`; a larger one streams in blocks of 64 KiB, bounded by its known length (no read past
//!   the end). `ReaderStream::new` (4 KiB blocks) is forbidden: it divides throughput by 6 to 10.
//! * **Writes (D34).** The admitted body goes through a bounded channel (depth 4) to one blocking
//!   task that owns the temporary file, writes through a 64 KiB `BufWriter`, then validates
//!   (`rename`, and `sync_all` before it in `fsync` mode). No `tokio::fs` write per frame.
//!   Consuming the body starts without waiting (D7).
//! * **No waiting between headers and body (D7).** [`Store::admit`] is synchronous and does no
//!   I/O. Nothing it takes can be held by a writer across I/O, and nothing in
//!   [`Reservation::write`] waits for the index, an eviction or a key lock before it starts
//!   consuming the body.
//! * **Index and locking (D15, D35).** One `std::sync::Mutex` over the index, with O(1) critical
//!   sections: never held across an `.await`, and **never across any I/O, [`Store::open`]
//!   included**. Competing validations of one key are serialised by a separate lock, which only
//!   validations take. Eviction deletions and `mtime` touches are decided under the lock and
//!   executed outside it. File I/O runs on tokio's blocking pool.
//! * **Metrics.** The store records `written_bytes` when an object becomes visible, `eviction` for
//!   each eviction and `crash_purge`, through the [`Metrics`] in its [`Options`]. `http` records
//!   everything else.
//!
//! # The naive implementation
//!
//! Files live at `objects/<2 hex>/<64 hex>` ([`Key::relative_path`]). There is no index and no
//! eviction: a full store refuses writes with `no_space`. Totals are exact because validations
//! are serialised by their own lock, apart from the accounting lock. Writes use `tokio::fs`, not
//! the D34 task. Recency, `touch_interval`, `min_free_disk` and the crash recovery are not
//! implemented: a [`Durability::PurgeOnCrash`] store simply does not sync.

#![expect(
    dead_code,
    reason = "the http lot is the caller; removed once it uses the whole API"
)]

use std::io;
use std::path::{Path, PathBuf};
use std::pin::pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use bytes::Bytes;
use futures_util::{Stream, StreamExt};
use thiserror::Error;
use tokio::fs::{self, File};
use tokio::io::AsyncWriteExt;
use tokio::runtime::Handle;
use tokio_util::io::ReaderStream;
use tracing::{error, warn};

use crate::config::Durability;
use crate::key::Key;
use crate::metrics::Metrics;

/// Granularity of the space accounting (D12).
const BLOCK: u64 = 4096;
/// Size of the chunks a read streams, so memory does not depend on object size (D15).
const READ_CHUNK: usize = 64 * 1024;

/// Limits and durability settings the store is opened with.
#[derive(Debug, Clone)]
pub(crate) struct Options {
    /// Ceiling for objects, reservations and pending deletions, in bytes.
    pub(crate) max_size: u64,
    /// Largest accepted object, in bytes. Never above `max_size`.
    pub(crate) max_object_size: u64,
    /// Free space the disk must keep after a reservation, in bytes.
    #[expect(dead_code, reason = "read by the disk store, not by the naive one")]
    pub(crate) min_free_disk: u64,
    /// What a write spends to survive a crash.
    pub(crate) durability: Durability,
    /// Minimum delay between two persisted recency updates of one entry.
    #[expect(dead_code, reason = "read by the disk store, not by the naive one")]
    pub(crate) touch_interval: Duration,
    /// Where the store records what it writes, evicts and purges.
    pub(crate) metrics: Metrics,
}

/// A store failure the caller can only report: the disk or the data directory misbehaved.
#[derive(Debug, Error)]
pub(crate) enum Error {
    /// A file system call failed.
    #[error("{op} {}: {source}", path.display())]
    Io {
        /// What was being done.
        op: &'static str,
        /// The file or directory involved.
        path: PathBuf,
        /// The underlying error.
        source: io::Error,
    },
    /// Another process holds the lock of the data directory.
    #[error("data directory {} is in use by another process", .0.display())]
    Locked(PathBuf),
}

impl Error {
    fn io(op: &'static str, path: &Path) -> impl FnOnce(io::Error) -> Self {
        let path = path.to_path_buf();
        move |source| Self::Io { op, path, source }
    }
}

/// Why a PUT was answered 200 without being stored (D5). The label of `puts_dropped_total`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DropReason {
    /// The announced size is above `max_object_size`.
    TooLarge,
    /// The reservation does not fit under `max_size` or `min_free_disk`.
    NoSpace,
    /// The index is still being built.
    NotReady,
    /// An I/O error happened after admission (D13).
    Io,
}

impl DropReason {
    /// Every reason, to publish the metric series up front.
    pub(crate) const ALL: [Self; 4] = [Self::TooLarge, Self::NoSpace, Self::NotReady, Self::Io];

    /// The metric label and log value.
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::TooLarge => "too_large",
            Self::NoSpace => "no_space",
            Self::NotReady => "not_ready",
            Self::Io => "io",
        }
    }
}

/// An opened object, ready to stream.
#[derive(Debug)]
pub(crate) struct Object {
    /// The open file. A concurrent replacement or eviction does not affect it.
    pub(crate) file: File,
    /// Exact size in bytes, for `Content-Length`.
    pub(crate) size: u64,
    /// Last modification time, for `Last-Modified`.
    pub(crate) modified: SystemTime,
}

impl Object {
    /// Streams the file in bounded chunks.
    pub(crate) fn into_stream(self) -> impl Stream<Item = io::Result<Bytes>> + Send {
        ReaderStream::with_capacity(self.file, READ_CHUNK)
    }
}

/// What `HEAD` and `PROPFIND` need to know about an object.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Meta {
    /// Exact size in bytes.
    pub(crate) size: u64,
    /// Last modification time.
    pub(crate) modified: SystemTime,
}

/// The counters behind the size and entry gauges.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Stats {
    /// Bytes charged to stored objects.
    pub(crate) size_bytes: u64,
    /// The configured `max_size`.
    pub(crate) size_limit_bytes: u64,
    /// Stored objects.
    pub(crate) entries: u64,
    /// Age of the least recently used entry, if the store tracks recency.
    pub(crate) oldest_entry_age: Option<Duration>,
}

/// The answer to [`Store::admit`].
#[derive(Debug)]
pub(crate) enum Admission {
    /// Room is reserved: pass the body to [`Reservation::write`].
    Reserved(Reservation),
    /// The PUT will not be stored. Answer 200 (D5) after reading and discarding the body (D7).
    Dropped(DropReason),
}

/// How [`Reservation::write`] ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Outcome {
    /// The object is stored and visible. Answer 201.
    Stored,
    /// An I/O error ended the write; the cause is logged at `error`. The body has been read to
    /// its end, so the connection is healthy: answer 200 (D5).
    Dropped(DropReason),
    /// The body ended before the announced size, failed, or exceeded it. Nothing was stored.
    /// There is no client left to answer, or its request is malformed.
    Aborted,
}

#[derive(Debug, Default)]
struct Accounts {
    size: u64,
    entries: u64,
    reserved: u64,
}

#[derive(Debug)]
struct Inner {
    root: PathBuf,
    options: Options,
    /// O(1) accounting; held for a few instructions, never across I/O (D35).
    accounts: Mutex<Accounts>,
    /// Serialises validations so a replaced version is counted exactly once. Only
    /// [`commit_blocking`] takes it, so a validation stuck in I/O cannot stall an admission.
    validation: Mutex<()>,
    ready: AtomicBool,
    next_tmp: AtomicU64,
    /// Held for the life of the process: the `flock` of D13.
    _lock: std::fs::File,
}

/// Handle to the store. Cloning is cheap and every clone shares the same state.
#[derive(Debug, Clone)]
pub(crate) struct Store {
    inner: Arc<Inner>,
}

const _: () = {
    const fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<Store>();
    assert_send_sync::<Reservation>();
};

/// Bytes charged for an object of `size` bytes: a whole number of blocks, at least one.
fn charge(size: u64) -> u64 {
    size.div_ceil(BLOCK).max(1).saturating_mul(BLOCK)
}

fn len_u64(chunk: &Bytes) -> u64 {
    u64::try_from(chunk.len()).unwrap_or(u64::MAX)
}

async fn blocking<T: Send + 'static>(work: impl FnOnce() -> T + Send + 'static) -> T {
    tokio::task::spawn_blocking(work)
        .await
        .expect("blocking tasks are not cancelled while the runtime runs")
}

impl Store {
    /// Opens the store on `dir`, creating it if needed.
    ///
    /// # Errors
    ///
    /// [`Error::Locked`] when another process uses the directory, [`Error::Io`] when it cannot be
    /// prepared or scanned.
    pub(crate) async fn open(dir: &Path, options: Options) -> Result<Self, Error> {
        let dir = dir.to_path_buf();
        let inner = blocking(move || open_blocking(dir, options)).await?;
        inner.ready.store(true, Ordering::Release);
        Ok(Self {
            inner: Arc::new(inner),
        })
    }

    /// Whether the index is built (`/healthz` answers 200, PUTs are admitted).
    pub(crate) fn is_ready(&self) -> bool {
        self.inner.ready.load(Ordering::Acquire)
    }

    /// Opens the object of `key` for streaming, or `None` when it is not stored.
    ///
    /// # Errors
    ///
    /// [`Error::Io`] when the file exists but cannot be opened or inspected.
    pub(crate) async fn read(&self, key: &Key) -> Result<Option<Object>, Error> {
        let path = self.inner.root.join(key.relative_path());
        let file = match File::open(&path).await {
            Ok(file) => file,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(Error::io("open", &path)(e)),
        };
        let meta = file.metadata().await.map_err(Error::io("stat", &path))?;
        let modified = meta.modified().map_err(Error::io("stat", &path))?;
        Ok(Some(Object {
            file,
            size: meta.len(),
            modified,
        }))
    }

    /// Size and modification time of the object of `key`, without opening it.
    ///
    /// # Errors
    ///
    /// [`Error::Io`] when the file exists but cannot be inspected.
    pub(crate) async fn stat(&self, key: &Key) -> Result<Option<Meta>, Error> {
        let path = self.inner.root.join(key.relative_path());
        match fs::metadata(&path).await {
            Ok(meta) => {
                let modified = meta.modified().map_err(Error::io("stat", &path))?;
                Ok(Some(Meta {
                    size: meta.len(),
                    modified,
                }))
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(Error::io("stat", &path)(e)),
        }
    }

    /// Decides, from the key and the announced size alone, whether a PUT is stored. Synchronous:
    /// no waiting, no I/O.
    ///
    /// A size above `max_object_size` is [`DropReason::TooLarge`], checked first. Then an index
    /// still being built is [`DropReason::NotReady`], and a reservation that does not fit is
    /// [`DropReason::NoSpace`].
    pub(crate) fn admit(&self, key: &Key, size: u64) -> Admission {
        let inner = &self.inner;
        if size > inner.options.max_object_size {
            return Admission::Dropped(DropReason::TooLarge);
        }
        if !self.is_ready() {
            return Admission::Dropped(DropReason::NotReady);
        }
        let charged = charge(size);
        let mut accounts = inner.accounts.lock().expect("accounts lock poisoned");
        if accounts
            .size
            .saturating_add(accounts.reserved)
            .saturating_add(charged)
            > inner.options.max_size
        {
            return Admission::Dropped(DropReason::NoSpace);
        }
        accounts.reserved += charged;
        drop(accounts);
        Admission::Reserved(Reservation {
            inner: Arc::clone(inner),
            destination: inner.root.join(key.relative_path()),
            size,
            charged,
            temporary: None,
            settled: false,
        })
    }

    /// The counters behind the gauges.
    pub(crate) fn stats(&self) -> Stats {
        let accounts = self.inner.accounts.lock().expect("accounts lock poisoned");
        Stats {
            size_bytes: accounts.size,
            size_limit_bytes: self.inner.options.max_size,
            entries: accounts.entries,
            oldest_entry_age: None,
        }
    }

    /// Flushes what shutdown must persist (late recency updates). Call once the server drained.
    ///
    /// # Errors
    ///
    /// [`Error::Io`] when a flush fails.
    #[expect(
        clippy::unused_async,
        clippy::unused_async_trait_impl,
        reason = "nothing to flush without recency"
    )]
    pub(crate) async fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}

fn open_blocking(root: PathBuf, options: Options) -> Result<Inner, Error> {
    let objects = root.join("objects");
    std::fs::create_dir_all(&objects).map_err(Error::io("create", &objects))?;
    let lock_path = root.join("lock");
    let lock = std::fs::File::options()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&lock_path)
        .map_err(Error::io("open", &lock_path))?;
    match lock.try_lock() {
        Ok(()) => {}
        Err(std::fs::TryLockError::WouldBlock) => return Err(Error::Locked(root)),
        Err(std::fs::TryLockError::Error(e)) => return Err(Error::io("lock", &lock_path)(e)),
    }
    let tmp = root.join("tmp");
    match std::fs::remove_dir_all(&tmp) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => return Err(Error::io("clear", &tmp)(e)),
    }
    std::fs::create_dir_all(&tmp).map_err(Error::io("create", &tmp))?;
    let accounts = scan(&objects)?;
    Ok(Inner {
        root,
        options,
        accounts: Mutex::new(accounts),
        validation: Mutex::new(()),
        ready: AtomicBool::new(false),
        next_tmp: AtomicU64::new(0),
        _lock: lock,
    })
}

fn scan(objects: &Path) -> Result<Accounts, Error> {
    let mut accounts = Accounts::default();
    let read_dir = |dir: &Path| std::fs::read_dir(dir).map_err(Error::io("read", dir));
    for bucket in read_dir(objects)? {
        let bucket = bucket.map_err(Error::io("read", objects))?;
        if !bucket
            .file_type()
            .map_err(Error::io("stat", &bucket.path()))?
            .is_dir()
        {
            continue;
        }
        for entry in read_dir(&bucket.path())? {
            let entry = entry.map_err(Error::io("read", &bucket.path()))?;
            let meta = entry.metadata().map_err(Error::io("stat", &entry.path()))?;
            if meta.is_file() {
                accounts.size = accounts.size.saturating_add(charge(meta.len()));
                accounts.entries += 1;
            }
        }
    }
    Ok(accounts)
}

/// Room reserved for one PUT, from admission to its end. Dropping it without a stored object
/// gives the room back and removes the temporary file.
#[derive(Debug)]
pub(crate) struct Reservation {
    inner: Arc<Inner>,
    destination: PathBuf,
    size: u64,
    charged: u64,
    temporary: Option<PathBuf>,
    settled: bool,
}

enum Failure {
    Aborted,
    Io(Error),
}

impl Reservation {
    /// Reads `body` to its end, stores it under the reserved key and says how it went.
    ///
    /// The announced size must match the bytes received exactly. After an I/O error the rest of
    /// the body is read and discarded before returning (D7), so the client's connection stays
    /// usable. The cause of an I/O failure is logged here at `error`. Dropping the future
    /// cancels the write.
    pub(crate) async fn write<S, E>(mut self, body: S) -> Outcome
    where
        S: Stream<Item = Result<Bytes, E>>,
    {
        let mut body = pin!(body);
        match self.receive(body.as_mut()).await {
            Ok(()) => {}
            Err(Failure::Aborted) => return Outcome::Aborted,
            Err(Failure::Io(e)) => {
                error!(error = %e, "PUT dropped after admission");
                while let Some(Ok(_)) = body.next().await {}
                return Outcome::Dropped(DropReason::Io);
            }
        }
        match self.commit().await {
            Ok(()) => Outcome::Stored,
            Err(e) => {
                error!(error = %e, "PUT dropped after admission");
                Outcome::Dropped(DropReason::Io)
            }
        }
    }

    async fn receive<S, E>(&mut self, mut body: std::pin::Pin<&mut S>) -> Result<(), Failure>
    where
        S: Stream<Item = Result<Bytes, E>>,
    {
        let id = self.inner.next_tmp.fetch_add(1, Ordering::Relaxed);
        let path = self.inner.root.join("tmp").join(id.to_string());
        let mut file = File::create(&path)
            .await
            .map_err(Error::io("create", &path))
            .map_err(Failure::Io)?;
        self.temporary = Some(path.clone());
        let mut received = 0_u64;
        while let Some(chunk) = body.next().await {
            let Ok(chunk) = chunk else {
                return Err(Failure::Aborted);
            };
            received = received.saturating_add(len_u64(&chunk));
            if received > self.size {
                return Err(Failure::Aborted);
            }
            file.write_all(&chunk)
                .await
                .map_err(Error::io("write", &path))
                .map_err(Failure::Io)?;
        }
        if received != self.size {
            return Err(Failure::Aborted);
        }
        file.flush()
            .await
            .map_err(Error::io("write", &path))
            .map_err(Failure::Io)?;
        if self.inner.options.durability == Durability::Fsync {
            file.sync_all()
                .await
                .map_err(Error::io("fsync", &path))
                .map_err(Failure::Io)?;
        }
        Ok(())
    }

    async fn commit(&mut self) -> Result<(), Error> {
        let temporary = self
            .temporary
            .clone()
            .expect("receive created the temporary file");
        let destination = self.destination.clone();
        let inner = Arc::clone(&self.inner);
        let charged = self.charged;
        blocking(move || commit_blocking(&inner, &temporary, &destination, charged)).await?;
        self.temporary = None;
        self.settled = true;
        self.inner.options.metrics.written_bytes(self.size);
        Ok(())
    }
}

/// Moves the temporary file into place and updates the totals.
///
/// The reservation stays charged until the rename is done. Validations take `validation` for the
/// lookup of the replaced version and the rename, so totals stay exact when two writers race on
/// one key; `accounts` is taken afterwards, only for the O(1) update, so an admission never waits
/// for the disk. The disk store replaces this with its index.
fn commit_blocking(
    inner: &Inner,
    temporary: &Path,
    destination: &Path,
    charged: u64,
) -> Result<(), Error> {
    let parent = destination
        .parent()
        .ok_or_else(|| Error::io("commit", destination)(io::ErrorKind::InvalidInput.into()))?;
    std::fs::create_dir_all(parent).map_err(Error::io("create", parent))?;
    let _validation = inner.validation.lock().expect("validation lock poisoned");
    let replaced = match std::fs::metadata(destination) {
        Ok(meta) => Some(charge(meta.len())),
        Err(e) if e.kind() == io::ErrorKind::NotFound => None,
        Err(e) => return Err(Error::io("stat", destination)(e)),
    };
    std::fs::rename(temporary, destination).map_err(Error::io("rename", destination))?;
    let mut accounts = inner.accounts.lock().expect("accounts lock poisoned");
    accounts.reserved = accounts.reserved.saturating_sub(charged);
    accounts.size = accounts
        .size
        .saturating_sub(replaced.unwrap_or(0))
        .saturating_add(charged);
    if replaced.is_none() {
        accounts.entries += 1;
    }
    Ok(())
}

impl Drop for Reservation {
    /// Cancels an unsettled reservation. The temporary file is removed on the blocking pool, and
    /// the room stays charged until it is gone, so the bound of D12 holds meanwhile.
    fn drop(&mut self) {
        if self.settled {
            return;
        }
        let inner = Arc::clone(&self.inner);
        let temporary = self.temporary.take();
        let charged = self.charged;
        let cancel = move || {
            if let Some(path) = temporary {
                match std::fs::remove_file(&path) {
                    Ok(()) => {}
                    Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                    Err(e) => {
                        warn!(path = %path.display(), error = %e, "cannot remove temporary file");
                    }
                }
            }
            let mut accounts = inner.accounts.lock().expect("accounts lock poisoned");
            accounts.reserved = accounts.reserved.saturating_sub(charged);
        };
        match Handle::try_current() {
            Ok(runtime) => drop(runtime.spawn_blocking(cancel)),
            Err(_) => cancel(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;

    use futures_util::stream;
    use tempfile::TempDir;
    use tokio::io::AsyncReadExt;

    use super::*;

    const MIB: u64 = 1024 * 1024;

    fn options(max_size: u64) -> Options {
        Options {
            max_size,
            max_object_size: max_size,
            min_free_disk: 0,
            durability: Durability::Fsync,
            touch_interval: Duration::from_secs(300),
            metrics: Metrics::detached(),
        }
    }

    fn key(text: &str) -> Key {
        Key::new(text).expect("test key is valid")
    }

    fn body(chunks: &[&'static [u8]]) -> impl Stream<Item = Result<Bytes, Infallible>> {
        stream::iter(
            chunks
                .iter()
                .map(|chunk| Ok(Bytes::from_static(chunk)))
                .collect::<Vec<_>>(),
        )
    }

    fn assert_send<T: Send>(value: T) -> T {
        value
    }

    async fn put(store: &Store, key: &Key, data: &'static [u8]) -> Outcome {
        let size = u64::try_from(data.len()).unwrap();
        match store.admit(key, size) {
            // The write future must be Send: http awaits it inside axum handlers.
            Admission::Reserved(reservation) => assert_send(reservation.write(body(&[data]))).await,
            Admission::Dropped(reason) => panic!("dropped: {reason:?}"),
        }
    }

    async fn contents(store: &Store, key: &Key) -> Option<Vec<u8>> {
        let object = store.read(key).await.unwrap()?;
        let mut data = Vec::new();
        let mut file = object.file;
        file.read_to_end(&mut data).await.unwrap();
        assert_eq!(u64::try_from(data.len()).unwrap(), object.size);
        Some(data)
    }

    fn temporaries(dir: &TempDir) -> usize {
        std::fs::read_dir(dir.path().join("tmp")).unwrap().count()
    }

    /// Waits for `condition`, which a cancelled write reaches on the blocking pool.
    async fn eventually(what: &str, condition: impl Fn() -> bool) {
        for _ in 0..200 {
            if condition() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("{what} did not happen within 2s");
    }

    #[tokio::test]
    async fn written_bytes_read_back() {
        let dir = TempDir::new().unwrap();
        let store = Store::open(dir.path(), options(MIB)).await.unwrap();
        let k = key("a/b/c");
        assert!(store.is_ready());
        assert!(contents(&store, &k).await.is_none());
        assert_eq!(put(&store, &k, b"hello cache").await, Outcome::Stored);
        assert_eq!(
            contents(&store, &k).await.as_deref(),
            Some(&b"hello cache"[..])
        );
        let meta = store.stat(&k).await.unwrap().unwrap();
        assert_eq!(meta.size, 11);
        assert_eq!(temporaries(&dir), 0);
        assert!(dir.path().join(k.relative_path()).is_file());
    }

    #[tokio::test]
    async fn replacing_a_key_counts_it_once() {
        let dir = TempDir::new().unwrap();
        let store = Store::open(dir.path(), options(MIB)).await.unwrap();
        let k = key("same");
        assert_eq!(put(&store, &k, b"first").await, Outcome::Stored);
        assert_eq!(put(&store, &k, b"second version").await, Outcome::Stored);
        assert_eq!(
            contents(&store, &k).await.as_deref(),
            Some(&b"second version"[..])
        );
        let stats = store.stats();
        assert_eq!((stats.entries, stats.size_bytes), (1, BLOCK));
    }

    #[tokio::test]
    async fn refused_keys_create_nothing_on_disk() {
        let dir = TempDir::new().unwrap();
        let _store = Store::open(dir.path(), options(MIB)).await.unwrap();
        for raw in ["", "/", "/a/../b", "/a/./b", "/a%00b", "/%ff"] {
            assert!(Key::from_uri_path(raw).is_err(), "{raw:?} was accepted");
        }
        let files: Vec<_> = walk(dir.path());
        assert_eq!(
            files,
            [dir.path().join("lock")],
            "unexpected files: {files:?}"
        );
    }

    fn walk(dir: &Path) -> Vec<PathBuf> {
        let mut files = Vec::new();
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                files.extend(walk(&path));
            } else {
                files.push(path);
            }
        }
        files
    }

    #[tokio::test]
    async fn admission_follows_d12() {
        let dir = TempDir::new().unwrap();
        let mut opts = options(2 * BLOCK);
        opts.max_object_size = BLOCK;
        let store = Store::open(dir.path(), opts).await.unwrap();
        let k = key("k");
        assert!(matches!(
            store.admit(&k, BLOCK + 1),
            Admission::Dropped(DropReason::TooLarge)
        ));
        let first = store.admit(&k, 1);
        let second = store.admit(&key("k2"), BLOCK);
        assert!(matches!(first, Admission::Reserved(_)));
        assert!(matches!(second, Admission::Reserved(_)));
        assert!(matches!(
            store.admit(&key("k3"), 1),
            Admission::Dropped(DropReason::NoSpace)
        ));
        drop((first, second));
        eventually("the rooms given back", || {
            matches!(store.admit(&key("k3"), 1), Admission::Reserved(_))
        })
        .await;
    }

    #[tokio::test]
    async fn dropped_reservation_cleans_up() {
        let dir = TempDir::new().unwrap();
        let store = Store::open(dir.path(), options(BLOCK)).await.unwrap();
        let k = key("k");
        let Admission::Reserved(reservation) = store.admit(&k, 4) else {
            panic!("not admitted")
        };
        let stream =
            stream::iter([Ok::<_, Infallible>(Bytes::from_static(b"ab"))]).chain(stream::pending());
        let write = reservation.write(stream);
        tokio::time::timeout(Duration::from_millis(200), write)
            .await
            .unwrap_err();
        eventually("temporary file removal", || temporaries(&dir) == 0).await;
        eventually("room given back", || {
            matches!(store.admit(&k, 4), Admission::Reserved(_))
        })
        .await;
        assert!(store.read(&k).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn cancelled_write_keeps_its_room_until_cleanup_is_done() {
        let dir = TempDir::new().unwrap();
        let store = Store::open(dir.path(), options(BLOCK)).await.unwrap();
        let Admission::Reserved(reservation) = store.admit(&key("k"), 4) else {
            panic!("not admitted")
        };
        // No runtime context: cleanup runs inline, so the room is back as soon as drop returns.
        let outside = std::thread::spawn(move || drop(reservation));
        outside.join().unwrap();
        assert!(matches!(store.admit(&key("k2"), 4), Admission::Reserved(_)));
    }

    #[tokio::test]
    async fn admission_does_not_wait_for_a_validation_stuck_in_io() {
        let dir = TempDir::new().unwrap();
        let store = Store::open(dir.path(), options(MIB)).await.unwrap();
        let (locked_tx, locked_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let inner = Arc::clone(&store.inner);
        // Stands for a rename that takes a long time: it owns the validation lock.
        let stuck = std::thread::spawn(move || {
            let _validation = inner.validation.lock().unwrap();
            locked_tx.send(()).unwrap();
            release_rx.recv().unwrap();
        });
        locked_rx.recv().unwrap();

        let writer = {
            let (store, k) = (store.clone(), key("slow"));
            tokio::spawn(async move { put(&store, &k, b"data").await })
        };
        let shard = dir.path().join("objects");
        eventually("the write to reach its validation", || {
            std::fs::read_dir(&shard).unwrap().count() > 0
        })
        .await;

        let (answer_tx, answer_rx) = std::sync::mpsc::channel();
        let prober = store.clone();
        std::thread::spawn(move || {
            let admitted = matches!(prober.admit(&key("other"), 4), Admission::Reserved(_));
            answer_tx.send(admitted).unwrap();
        });
        let admitted = answer_rx.recv_timeout(Duration::from_secs(2));
        release_tx.send(()).unwrap();
        stuck.join().unwrap();
        assert_eq!(admitted, Ok(true), "admission waited for the validation");
        assert_eq!(writer.await.unwrap(), Outcome::Stored);
    }

    #[tokio::test]
    async fn short_and_failed_bodies_store_nothing() {
        let dir = TempDir::new().unwrap();
        let store = Store::open(dir.path(), options(MIB)).await.unwrap();
        let k = key("k");
        let Admission::Reserved(reservation) = store.admit(&k, 10) else {
            panic!("not admitted")
        };
        assert_eq!(reservation.write(body(&[b"short"])).await, Outcome::Aborted);
        let Admission::Reserved(reservation) = store.admit(&k, 10) else {
            panic!("not admitted")
        };
        let failing = stream::iter([Ok(Bytes::from_static(b"12345")), Err("reset")]);
        assert_eq!(reservation.write(failing).await, Outcome::Aborted);
        assert!(store.read(&k).await.unwrap().is_none());
        eventually("temporary files removed", || temporaries(&dir) == 0).await;
        assert_eq!(store.stats().entries, 0);
    }

    #[tokio::test]
    async fn io_error_drains_the_body_and_gives_the_room_back() {
        let dir = TempDir::new().unwrap();
        let store = Store::open(dir.path(), options(MIB)).await.unwrap();
        let k = key("k");
        let Admission::Reserved(reservation) = store.admit(&k, 4) else {
            panic!("not admitted")
        };
        std::fs::remove_dir_all(dir.path().join("tmp")).unwrap();
        let drained = Arc::new(AtomicU64::new(0));
        let counter = Arc::clone(&drained);
        let chunks = stream::iter(0..3).map(move |_| {
            counter.fetch_add(1, Ordering::SeqCst);
            Ok::<_, Infallible>(Bytes::from_static(b"ab"))
        });
        assert_eq!(
            reservation.write(chunks).await,
            Outcome::Dropped(DropReason::Io)
        );
        assert_eq!(
            drained.load(Ordering::SeqCst),
            3,
            "body not read to its end"
        );
        assert!(matches!(store.admit(&k, 4), Admission::Reserved(_)));
    }

    #[tokio::test]
    async fn survives_a_restart_and_refuses_a_second_opener() {
        let dir = TempDir::new().unwrap();
        let store = Store::open(dir.path(), options(MIB)).await.unwrap();
        let k = key("persisted");
        assert_eq!(put(&store, &k, b"data").await, Outcome::Stored);
        assert!(matches!(
            Store::open(dir.path(), options(MIB)).await,
            Err(Error::Locked(_))
        ));
        drop(store);
        let reopened = Store::open(dir.path(), options(MIB)).await.unwrap();
        assert_eq!(contents(&reopened, &k).await.as_deref(), Some(&b"data"[..]));
        assert_eq!(reopened.stats().entries, 1);
    }
}

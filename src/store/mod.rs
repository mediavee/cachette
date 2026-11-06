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
//!   when the disk would drop below `min_free_disk`.
//! * **Pinned keys (D12).** A key is not evictable from its reservation until it is stored or the
//!   reservation is dropped. An eviction decided before an object was stored never deletes it: a
//!   deletion still pending carries a generation, so it cannot remove a newer version.
//! * **Atomic writes (D13).** The body goes to `tmp/<unique>`, is flushed (and `fsync`ed when
//!   `fsync` is set), then renamed to the object path, then the directory is `fsync`ed. The object
//!   is visible only after the rename. Concurrent writes of one key all succeed, the last rename
//!   wins and the key is counted once. A reader that already opened the file keeps the old
//!   version. The data directory is `flock`ed at [`Store::open`] before `tmp/` is emptied.
//! * **Index and readiness (D14).** The index is rebuilt from `objects/` at start-up, the recency
//!   from `mtime`. Until it is built [`Store::is_ready`] is `false`, [`Store::admit`] answers
//!   [`DropReason::NotReady`] without waiting, and reads are served straight from disk. Recency
//!   is written back to `mtime` at most once per `touch_interval`, and for every late entry in
//!   [`Store::close`].
//! * **Reads never fail on eviction (D14).** [`Store::read`] retries once when the file vanishes
//!   between the lookup and the open, then answers `None`.
//! * **No waiting between headers and body (D7).** [`Store::admit`] is synchronous, takes no
//!   lock a writer can hold across I/O, and does no I/O. Nothing in [`Reservation::write`]
//!   waits for the index, an eviction or a key lock before it starts consuming the body.
//! * **Concurrency (D15).** One `std::sync::Mutex` over the index, never held across an `.await`
//!   or an I/O call. File I/O runs on tokio's blocking pool and reads stream in bounded chunks.
//! * **Metrics.** The store records `written_bytes` when an object becomes visible, and
//!   `eviction` for each eviction. `http` records everything else.
//!
//! # The naive implementation
//!
//! Files live at `objects/<2 hex>/<64 hex>` ([`Key::relative_path`]). There is no index and no
//! eviction: a full store refuses writes with `no_space`. Totals are exact because commits are
//! serialised under one lock. Recency, `touch_interval` and `min_free_disk` are not used.

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
use tokio_util::io::ReaderStream;
use tracing::{error, warn};

use crate::key::Key;
use crate::metrics;

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
    /// Whether to `fsync` files and directories before answering a write.
    pub(crate) fsync: bool,
    /// Minimum delay between two persisted recency updates of one entry.
    #[expect(dead_code, reason = "read by the disk store, not by the naive one")]
    pub(crate) touch_interval: Duration,
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
    accounts: Mutex<Accounts>,
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
        if self.inner.options.fsync {
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
        let (renamed, result) =
            blocking(move || commit_blocking(&inner, &temporary, &destination, charged)).await;
        if renamed {
            self.temporary = None;
            self.settled = true;
            if result.is_ok() {
                metrics::written_bytes(self.size);
            }
        }
        result
    }
}

/// Moves the temporary file into place and updates the totals. Returns whether the object is now
/// visible, and the outcome of the whole commit.
///
/// The accounts lock is held from the lookup of the replaced version to the end of the rename so
/// that totals stay exact when two writers race on one key. The lock lives in a blocking task and
/// is never held across an `.await`; the disk store replaces this with its index.
fn commit_blocking(
    inner: &Inner,
    temporary: &Path,
    destination: &Path,
    charged: u64,
) -> (bool, Result<(), Error>) {
    let Some(parent) = destination.parent() else {
        return (
            false,
            Err(Error::io("commit", destination)(
                io::ErrorKind::InvalidInput.into(),
            )),
        );
    };
    if let Err(e) = std::fs::create_dir_all(parent) {
        return (false, Err(Error::io("create", parent)(e)));
    }
    {
        let mut accounts = inner.accounts.lock().expect("accounts lock poisoned");
        let replaced = match std::fs::metadata(destination) {
            Ok(meta) => Some(charge(meta.len())),
            Err(e) if e.kind() == io::ErrorKind::NotFound => None,
            Err(e) => return (false, Err(Error::io("stat", destination)(e))),
        };
        if let Err(e) = std::fs::rename(temporary, destination) {
            return (false, Err(Error::io("rename", destination)(e)));
        }
        accounts.reserved = accounts.reserved.saturating_sub(charged);
        accounts.size = accounts
            .size
            .saturating_sub(replaced.unwrap_or(0))
            .saturating_add(charged);
        if replaced.is_none() {
            accounts.entries += 1;
        }
    }
    if inner.options.fsync {
        let synced = std::fs::File::open(parent).and_then(|dir| dir.sync_all());
        return (true, synced.map_err(Error::io("fsync", parent)));
    }
    (true, Ok(()))
}

impl Drop for Reservation {
    fn drop(&mut self) {
        if self.settled {
            return;
        }
        if let Some(path) = self.temporary.take() {
            match std::fs::remove_file(&path) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => warn!(path = %path.display(), error = %e, "cannot remove temporary file"),
            }
        }
        let mut accounts = self.inner.accounts.lock().expect("accounts lock poisoned");
        accounts.reserved = accounts.reserved.saturating_sub(self.charged);
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
            fsync: true,
            touch_interval: Duration::from_secs(300),
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
        assert!(matches!(store.admit(&key("k3"), 1), Admission::Reserved(_)));
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
        assert_eq!(temporaries(&dir), 0, "temporary file left behind");
        assert!(
            matches!(store.admit(&k, 4), Admission::Reserved(_)),
            "room not given back"
        );
        assert!(store.read(&k).await.unwrap().is_none());
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
        assert_eq!(temporaries(&dir), 0);
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

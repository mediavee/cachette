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
//! # The implementation
//!
//! Files live at `objects/<2 hex>/<64 hex>` (the blake3 digest of the key). The in-memory
//! index ([`index::Index`]) maps that digest to a compact entry and keeps the LRU order in an
//! intrusive list, behind the one `Mutex` of D15. Admission decides evictions under that lock;
//! deletions, `mtime` touches, file opens and every other I/O run outside it, on the blocking
//! pool. Validations and pending deletions are serialised against each other by the separate
//! validation lock, so a deletion decided against an old generation can never unlink a version
//! renamed after it.

#![expect(
    dead_code,
    reason = "the http lot is the caller; removed once it uses the whole API"
)]

mod index;

use std::fs::File as StdFile;
use std::io::{self, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::pin::pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use bytes::Bytes;
use futures_util::{Stream, StreamExt};
use thiserror::Error;
use tokio::fs::File;
use tokio::runtime::Handle;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tracing::{error, warn};

use crate::config::Durability;
use crate::key::Key;
use crate::metrics::Metrics;
use index::{Digest, Index};

/// Granularity of the space accounting (D12).
const BLOCK: u64 = 4096;
/// Size of the chunks a read streams, so memory does not depend on object size (D15, D33).
const READ_CHUNK: usize = 64 * 1024;
/// Objects at most this large are read by one blocking task into a single `Bytes` (D33).
const SMALL_OBJECT: u64 = 64 * 1024;
/// Depth of the channel feeding a write's blocking task (D34).
const WRITE_CHANNEL_DEPTH: usize = 4;
/// `BufWriter` capacity of a write's blocking task (D34).
const WRITE_BUFFER: usize = 64 * 1024;
/// Chunks a streaming read may have in flight: at most twice [`READ_CHUNK`] bytes per reader.
const READ_CHANNEL_DEPTH: usize = 2;
/// Shards under `objects/`: the first digest byte, in hex.
const SHARDS: u32 = 256;

/// Limits and durability settings the store is opened with.
#[derive(Debug, Clone)]
pub(crate) struct Options {
    /// Ceiling for objects, reservations and pending deletions, in bytes.
    pub(crate) max_size: u64,
    /// Largest accepted object, in bytes. Never above `max_size`.
    pub(crate) max_object_size: u64,
    /// Free space the disk must keep after a reservation, in bytes.
    pub(crate) min_free_disk: u64,
    /// What a write spends to survive a crash.
    pub(crate) durability: Durability,
    /// Minimum delay between two persisted recency updates of one entry.
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
    /// Streams the file in bounded chunks (D33): one blocking task reads a small object
    /// whole into a single `Bytes`; a larger one is read in [`READ_CHUNK`] blocks, never
    /// past the known length.
    pub(crate) fn into_stream(self) -> impl Stream<Item = io::Result<Bytes>> + Send {
        let (sender, receiver) = mpsc::channel(READ_CHANNEL_DEPTH);
        let size = self.size;
        match self.file.try_into_std() {
            Ok(file) => spawn_background(move || read_file(file, size, &sender)),
            Err(file) => match Handle::try_current() {
                // A read is in flight on this handle: wait it out off the request path.
                Ok(runtime) => drop(runtime.spawn(async move {
                    let file = file.into_std().await;
                    spawn_background(move || read_file(file, size, &sender));
                })),
                Err(_) => {
                    let _ = sender
                        .blocking_send(Err(io::Error::other("no runtime to read the object on")));
                }
            },
        }
        futures_util::stream::unfold(receiver, |mut receiver| async move {
            receiver.recv().await.map(|chunk| (chunk, receiver))
        })
    }
}

/// Reads exactly `size` bytes from the start of `file`, sending each block before reading
/// the next. Runs on the blocking pool; stops when the reader goes away.
fn read_file(mut file: StdFile, size: u64, sender: &mpsc::Sender<io::Result<Bytes>>) {
    if size <= SMALL_OBJECT {
        let len = usize::try_from(size).unwrap_or(usize::MAX);
        let mut buffer = vec![0_u8; len];
        let chunk = match file.read_exact(&mut buffer) {
            Ok(()) => Ok(Bytes::from(buffer)),
            Err(e) => Err(e),
        };
        let _ = sender.blocking_send(chunk);
        return;
    }
    let mut remaining = size;
    while remaining > 0 {
        let want = usize::try_from(remaining.min(u64::try_from(READ_CHUNK).unwrap_or(u64::MAX)))
            .unwrap_or(READ_CHUNK);
        let mut buffer = vec![0_u8; want];
        if let Err(e) = file.read_exact(&mut buffer) {
            let _ = sender.blocking_send(Err(e));
            return;
        }
        remaining = remaining.saturating_sub(u64::try_from(want).unwrap_or(u64::MAX));
        if sender.blocking_send(Ok(Bytes::from(buffer))).is_err() {
            return;
        }
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

/// Test-only fault and timing injection. Every knob is inert in a normal build.
#[derive(Debug, Default)]
struct Hooks {
    /// Pending deletions wait until this is cleared (criterion 1b).
    hold_deletions: AtomicBool,
    /// The start-up scan waits until this is cleared (`not_ready` window).
    hold_scan: AtomicBool,
    /// `syncfs` at close reports an error (criterion 9).
    fail_syncfs: AtomicBool,
    /// One read unlinks the object file between its lookup and its open (criterion 1c):
    /// 1 = just unlink, 2 = unlink, then (3) a replacement appears before the retry.
    vanish_on_read: AtomicU64,
}

#[derive(Debug)]
struct Inner {
    root: PathBuf,
    options: Options,
    /// The index; held for a few O(1) instructions, never across I/O or an `.await` (D35).
    index: Mutex<Index>,
    /// Serialises validations against each other and against pending deletions, so a
    /// deletion decided before a rename cannot unlink the renamed version. Only writer
    /// tasks and deletion tasks take it, so I/O under it cannot stall an admission.
    validation: Mutex<()>,
    ready: AtomicBool,
    closed: AtomicBool,
    next_tmp: AtomicU64,
    /// Background tasks (scan, deletions, touches, writers) still running; `close` waits
    /// for zero beyond itself before it may call the run clean.
    background: AtomicU64,
    /// Last free-disk sample, refreshed off the request path so `admit` does no I/O (D7).
    /// Admissions subtract the bytes reserved since (in `reserved`), failing closed.
    free_disk: AtomicU64,
    hooks: Hooks,
    /// The `objects/` directory, kept open from start-up: the `syncfs` observation handle
    /// of D32, so writeback errors since start-up are reported to `close`.
    objects_dir: StdFile,
    /// Held for the life of the process: the `flock` of D13.
    _lock: StdFile,
}

/// An eviction decided under the index lock, to execute outside it.
#[derive(Debug)]
struct PendingDelete {
    digest: Digest,
    generation: u64,
    charged: u64,
    path: PathBuf,
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

/// Runs `work` on the blocking pool, or on a fresh thread when no runtime is at hand (an
/// admission from a plain thread must still be able to start a deletion).
fn spawn_background(work: impl FnOnce() + Send + 'static) {
    match Handle::try_current() {
        Ok(runtime) => drop(runtime.spawn_blocking(work)),
        Err(_) => drop(std::thread::spawn(work)),
    }
}

/// [`spawn_background`], counted in `background` so [`Store::close`] can wait for it.
fn spawn_tracked(inner: &Arc<Inner>, work: impl FnOnce() + Send + 'static) {
    inner.background.fetch_add(1, Ordering::SeqCst);
    let tracked = Arc::clone(inner);
    spawn_background(move || {
        work();
        tracked.background.fetch_sub(1, Ordering::SeqCst);
    });
}

fn digest_of(key: &Key) -> Digest {
    Digest(*blake3::hash(key.as_str().as_bytes()).as_bytes())
}

/// `objects/<2 hex>/<64 hex>` for a digest. Every component is a plain name.
fn object_path(root: &Path, digest: Digest) -> PathBuf {
    let hex = blake3::Hash::from(digest.0).to_hex();
    let hex = hex.as_str();
    root.join("objects").join(&hex[..2]).join(hex)
}

/// Free bytes on the file system holding `path`, for the `min_free_disk` check. A failure
/// reads as no free space: fail closed, never admit blind. Sampled off the request path.
fn sample_free_disk(path: &Path) -> u64 {
    match rustix::fs::statvfs(path) {
        Ok(stats) => stats.f_bavail.saturating_mul(stats.f_frsize.max(1)),
        Err(e) => {
            warn!(path = %path.display(), error = %e, "cannot read free disk space");
            0
        }
    }
}

fn refresh_free_disk(inner: &Inner) {
    inner
        .free_disk
        .store(sample_free_disk(&inner.root), Ordering::Release);
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
        let inner = Arc::new(inner);
        // The scan rebuilds the index in the background (D14, D36): reads are served from
        // disk meanwhile, admissions answer `not_ready`.
        let scan = Arc::clone(&inner);
        spawn_tracked(&inner, move || scan_blocking(&scan));
        Ok(Self { inner })
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
        let digest = digest_of(key);
        let path = object_path(&self.inner.root, digest);
        // Two attempts: an eviction (or a rewrite) between the lookup and the open answers
        // absent, or the newer version's bytes — never an error (D14).
        for _attempt in 0..2 {
            if self.is_ready() {
                let hit = {
                    let mut index = self.inner.index.lock().expect("index lock poisoned");
                    index.hit(digest, SystemTime::now(), self.inner.options.touch_interval)
                };
                let Some(hit) = hit else { return Ok(None) };
                if let Some(when) = hit.persist_recency {
                    let inner = Arc::clone(&self.inner);
                    let touch_path = path.clone();
                    spawn_tracked(&self.inner, move || {
                        touch(&inner, &touch_path, when);
                    });
                }
                self.vanish_hook(&path);
            }
            match open_object(&path).await? {
                Some(object) => return Ok(Some(object)),
                None if !self.is_ready() => return Ok(None),
                None => {}
            }
        }
        Ok(None)
    }

    /// Test hook: make the object file vanish (and optionally be replaced) after the index
    /// lookup, as an eviction landing exactly then would.
    fn vanish_hook(&self, path: &Path) {
        match self.inner.hooks.vanish_on_read.swap(0, Ordering::SeqCst) {
            0 => {}
            1 => drop(std::fs::remove_file(path)),
            2 => {
                // Unlink for this attempt; the replacement lands before the retry, as a
                // rewrite committing between the two opens would.
                drop(std::fs::remove_file(path));
                self.inner.hooks.vanish_on_read.store(3, Ordering::SeqCst);
            }
            _ => drop(std::fs::write(path, b"replacement")),
        }
    }

    /// Size and modification time of the object of `key`, without opening it.
    ///
    /// # Errors
    ///
    /// [`Error::Io`] when the file exists but cannot be inspected.
    pub(crate) async fn stat(&self, key: &Key) -> Result<Option<Meta>, Error> {
        let digest = digest_of(key);
        if self.is_ready() {
            // From the index, without I/O — and without refreshing recency: a `HEAD` probe
            // is not a hit (D14 reserves that to reads).
            let index = self.inner.index.lock().expect("index lock poisoned");
            return Ok(index
                .peek(digest)
                .map(|(size, modified)| Meta { size, modified }));
        }
        let path = object_path(&self.inner.root, digest);
        blocking(move || match std::fs::metadata(&path) {
            Ok(meta) => {
                let modified = meta.modified().map_err(Error::io("stat", &path))?;
                Ok(Some(Meta {
                    size: meta.len(),
                    modified,
                }))
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(Error::io("stat", &path)(e)),
        })
        .await
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
        if inner.closed.load(Ordering::Acquire) || !self.is_ready() {
            return Admission::Dropped(DropReason::NotReady);
        }
        let digest = digest_of(key);
        let charged = charge(size);
        let mut evictions: Vec<PendingDelete> = Vec::new();
        let mut evicted_sizes: Vec<u64> = Vec::new();
        {
            let mut index = inner.index.lock().expect("index lock poisoned");
            let free = inner
                .free_disk
                .load(Ordering::Acquire)
                .saturating_sub(index.reserved);
            if free < inner.options.min_free_disk.saturating_add(charged) {
                return Admission::Dropped(DropReason::NoSpace);
            }
            // The bytes this admission must free. Its own evictions move from `size` to
            // `pending` and are spent by this reservation; `pending` from earlier
            // admissions stays charged until its unlinks land, so no later admission can
            // spend the same bytes twice.
            let over = index
                .charged_total()
                .saturating_add(charged)
                .saturating_sub(inner.options.max_size);
            if over > 0 {
                // Collect first: an admission that cannot fit evicts nothing (D12).
                index.pin(digest);
                let candidates = index.eviction_candidates(over);
                let mut freed = 0_u64;
                let mut chosen: Vec<Digest> = Vec::new();
                for candidate in candidates {
                    if freed >= over {
                        break;
                    }
                    if let Some((_, candidate_charged)) =
                        index.peek(candidate).map(|(size, _)| (size, charge(size)))
                    {
                        freed = freed.saturating_add(candidate_charged);
                        chosen.push(candidate);
                    }
                }
                if freed < over {
                    index.unpin(digest);
                    return Admission::Dropped(DropReason::NoSpace);
                }
                for candidate in chosen {
                    if let Some(entry) = index.remove(candidate) {
                        index.pending = index.pending.saturating_add(entry.charged);
                        evicted_sizes.push(entry.size);
                        evictions.push(PendingDelete {
                            digest: entry.digest,
                            generation: entry.generation,
                            charged: entry.charged,
                            path: object_path(&inner.root, entry.digest),
                        });
                    }
                }
            } else {
                index.pin(digest);
            }
            index.reserved = index.reserved.saturating_add(charged);
        }
        for (pending, evicted) in evictions.into_iter().zip(evicted_sizes) {
            inner.options.metrics.eviction(evicted);
            let delete_inner = Arc::clone(inner);
            spawn_tracked(inner, move || delete_pending(&delete_inner, &pending));
        }
        Admission::Reserved(Reservation {
            inner: Arc::clone(inner),
            digest,
            destination: object_path(&inner.root, digest),
            size,
            charged,
            settled: false,
        })
    }

    /// The counters behind the gauges.
    pub(crate) fn stats(&self) -> Stats {
        let index = self.inner.index.lock().expect("index lock poisoned");
        Stats {
            size_bytes: index.size,
            size_limit_bytes: self.inner.options.max_size,
            entries: index.entries(),
            oldest_entry_age: index
                .oldest_access()
                .and_then(|access| SystemTime::now().duration_since(access).ok()),
        }
    }

    /// Flushes what shutdown must persist (late recency updates). Call once the server drained.
    ///
    /// Closes admissions, waits for every writer and background task, persists late
    /// recency, then — `purge-on-crash` only — runs a checked `syncfs` before writing
    /// `clean = true` (D32). There is no deadline here by design: the server's shutdown
    /// deadline killing the process before this returns leaves `clean = false`, which is
    /// exactly the "missed deadline forbids clean" rule.
    ///
    /// # Errors
    ///
    /// [`Error::Io`] when a flush fails. The state stays `clean = false`.
    pub(crate) async fn close(&self) -> Result<(), Error> {
        let inner = Arc::clone(&self.inner);
        inner.closed.store(true, Ordering::Release);
        blocking(move || close_blocking(&inner)).await
    }
}

/// Opens an object file and its metadata on the blocking pool. `Ok(None)` is `ENOENT`.
async fn open_object(path: &Path) -> Result<Option<Object>, Error> {
    let path = path.to_path_buf();
    blocking(move || match StdFile::open(&path) {
        Ok(file) => {
            let meta = file.metadata().map_err(Error::io("stat", &path))?;
            let modified = meta.modified().map_err(Error::io("stat", &path))?;
            Ok(Some(Object {
                file: File::from_std(file),
                size: meta.len(),
                modified,
            }))
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(Error::io("open", &path)(e)),
    })
    .await
}

/// Sets a file's `mtime` to `when` (the persisted recency, D14). Background task body.
fn touch(inner: &Inner, path: &Path, when: SystemTime) {
    let _ = inner;
    match StdFile::options().write(true).open(path) {
        Ok(file) => {
            if let Err(e) = file.set_modified(when) {
                warn!(path = %path.display(), error = %e, "cannot persist recency");
            }
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => warn!(path = %path.display(), error = %e, "cannot persist recency"),
    }
}

/// Deletes one evicted object, unless a newer version was committed after the eviction
/// was decided. The validation lock makes the generation check and the unlink atomic
/// against a commit's rename.
fn delete_pending(inner: &Arc<Inner>, pending: &PendingDelete) {
    while inner.hooks.hold_deletions.load(Ordering::Acquire) {
        std::thread::sleep(Duration::from_millis(1));
    }
    {
        let _validation = inner.validation.lock().expect("validation lock poisoned");
        let stale = {
            let index = inner.index.lock().expect("index lock poisoned");
            matches!(index.generation(pending.digest), Some(g) if g != pending.generation)
        };
        if !stale {
            match std::fs::remove_file(&pending.path) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => {
                    warn!(path = %pending.path.display(), error = %e, "cannot delete evicted object");
                }
            }
        }
    }
    let mut index = inner.index.lock().expect("index lock poisoned");
    index.pending = index.pending.saturating_sub(pending.charged);
    drop(index);
    refresh_free_disk(inner);
}

/// The persisted run state of D32: the durability mode of the run that wrote it, and
/// whether that run shut down cleanly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct State {
    mode: Durability,
    clean: bool,
}

fn mode_name(mode: Durability) -> &'static str {
    match mode {
        Durability::PurgeOnCrash => "purge-on-crash",
        Durability::Fsync => "fsync",
    }
}

/// Reads `state`; absent, unreadable and malformed all read as `None` (each purges, D32).
/// The second value says whether a state file exists at all: a first boot on an empty
/// directory purges nothing and is initialisation, not a crash purge.
fn read_state(root: &Path) -> (Option<State>, bool) {
    let path = root.join("state");
    let existed = path.try_exists().unwrap_or(false);
    (parse_state(&path), existed)
}

fn parse_state(path: &Path) -> Option<State> {
    let text = std::fs::read_to_string(path).ok()?;
    let mut mode = None;
    let mut clean = None;
    for line in text.lines() {
        let (field, value) = line.split_once('=')?;
        match (field.trim(), value.trim()) {
            ("mode", "purge-on-crash") => mode = Some(Durability::PurgeOnCrash),
            ("mode", "fsync") => mode = Some(Durability::Fsync),
            ("clean", "true") => clean = Some(true),
            ("clean", "false") => clean = Some(false),
            _ => return None,
        }
    }
    Some(State {
        mode: mode?,
        clean: clean?,
    })
}

/// Whether the previous run's objects may be served. Only a readable state that is clean,
/// or unclean in `fsync` mode (whose objects are each synced), keeps them.
fn purge_needed(previous: Option<State>) -> bool {
    match previous {
        None => true,
        Some(state) => !state.clean && state.mode == Durability::PurgeOnCrash,
    }
}

/// Writes `state` via a synced temporary plus rename, then syncs the directory, so a crash
/// leaves the old state or the new one, never a torn one.
fn write_state(root: &Path, state: State) -> Result<(), Error> {
    let temporary = root.join("state.tmp");
    let destination = root.join("state");
    {
        let mut file = StdFile::create(&temporary).map_err(Error::io("create", &temporary))?;
        write!(
            file,
            "mode={}\nclean={}\n",
            mode_name(state.mode),
            state.clean
        )
        .map_err(Error::io("write", &temporary))?;
        file.sync_all().map_err(Error::io("fsync", &temporary))?;
    }
    std::fs::rename(&temporary, &destination).map_err(Error::io("rename", &destination))?;
    sync_dir(root)
}

fn sync_dir(dir: &Path) -> Result<(), Error> {
    StdFile::open(dir)
        .and_then(|file| file.sync_all())
        .map_err(Error::io("fsync", dir))
}

/// Deletes every object file, shard by shard, returning how many. Names outside the
/// object scheme are left alone here too: a purge removes what the store could have
/// served, nothing else.
fn purge_objects(objects: &Path) -> Result<u64, Error> {
    let mut removed = 0_u64;
    for shard in shard_dirs(objects)? {
        let read = std::fs::read_dir(&shard).map_err(Error::io("read", &shard))?;
        for entry in read {
            let entry = entry.map_err(Error::io("read", &shard))?;
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            if parse_digest(name).is_some() {
                let path = entry.path();
                std::fs::remove_file(&path).map_err(Error::io("delete", &path))?;
                removed += 1;
            }
        }
    }
    Ok(removed)
}

/// The existing `objects/<2 hex>` directories. Anything else directly under `objects/` is
/// unexpected: logged, ignored, never deleted.
fn shard_dirs(objects: &Path) -> Result<Vec<PathBuf>, Error> {
    let mut shards = Vec::new();
    let read = match std::fs::read_dir(objects) {
        Ok(read) => read,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(shards),
        Err(e) => return Err(Error::io("read", objects)(e)),
    };
    for entry in read {
        let entry = entry.map_err(Error::io("read", objects))?;
        let path = entry.path();
        let is_dir = entry
            .file_type()
            .map_err(Error::io("stat", &path))?
            .is_dir();
        let name = entry.file_name();
        let valid = name.to_str().is_some_and(is_shard_name);
        if is_dir && valid {
            shards.push(path);
        } else {
            warn!(path = %path.display(), "unexpected entry under objects/, ignored");
        }
    }
    Ok(shards)
}

fn is_shard_name(name: &str) -> bool {
    name.len() == 2
        && name
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

fn parse_digest(name: &str) -> Option<Digest> {
    if name.len() != 64 {
        return None;
    }
    let mut digest = [0_u8; 32];
    for (index, pair) in digest.iter_mut().enumerate() {
        *pair = u8::from_str_radix(name.get(index * 2..index * 2 + 2)?, 16).ok()?;
    }
    // blake3 hex is lowercase; uppercase names are not ours.
    if name.bytes().any(|b| b.is_ascii_uppercase()) {
        return None;
    }
    Some(Digest(digest))
}

fn open_blocking(root: PathBuf, options: Options) -> Result<Inner, Error> {
    let objects = root.join("objects");
    std::fs::create_dir_all(&objects).map_err(Error::io("create", &objects))?;
    let lock_path = root.join("lock");
    let lock = StdFile::options()
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
    // D32, under the flock and before any mutation or served read. While a purge runs, the
    // state on disk still demands one (`purge-on-crash`, not clean), so an interrupted
    // purge starts over at the next start instead of serving what it had not reached.
    let (previous, state_existed) = read_state(&root);
    if purge_needed(previous) {
        write_state(
            &root,
            State {
                mode: Durability::PurgeOnCrash,
                clean: false,
            },
        )?;
        let removed = purge_objects(&objects)?;
        if removed > 0 || state_existed {
            options.metrics.crash_purge();
            warn!(
                removed,
                "purged all objects: the previous run was not clean"
            );
        }
    }
    write_state(
        &root,
        State {
            mode: options.durability,
            clean: false,
        },
    )?;
    let tmp = root.join("tmp");
    match std::fs::remove_dir_all(&tmp) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => return Err(Error::io("clear", &tmp)(e)),
    }
    std::fs::create_dir_all(&tmp).map_err(Error::io("create", &tmp))?;
    let objects_dir = StdFile::open(&objects).map_err(Error::io("open", &objects))?;
    let free_disk = sample_free_disk(&root);
    Ok(Inner {
        root,
        options,
        index: Mutex::new(Index::with_capacity(0)),
        validation: Mutex::new(()),
        ready: AtomicBool::new(false),
        closed: AtomicBool::new(false),
        next_tmp: AtomicU64::new(0),
        background: AtomicU64::new(0),
        free_disk: AtomicU64::new(free_disk),
        hooks: Hooks::default(),
        objects_dir,
        _lock: lock,
    })
}

/// One object found by the start-up scan.
#[derive(Debug)]
struct Scanned {
    digest: Digest,
    size: u64,
    modified: SystemTime,
}

fn scan_shard(shard: &Path, out: &mut Vec<Scanned>) -> Result<(), Error> {
    let shard_name = shard
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default()
        .to_owned();
    let read = std::fs::read_dir(shard).map_err(Error::io("read", shard))?;
    for entry in read {
        let entry = entry.map_err(Error::io("read", shard))?;
        let path = entry.path();
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            warn!(path = %path.display(), "unexpected entry under objects/, ignored");
            continue;
        };
        let Some(digest) = parse_digest(name) else {
            warn!(path = %path.display(), "unexpected entry under objects/, ignored");
            continue;
        };
        if !name.starts_with(&shard_name) {
            warn!(path = %path.display(), "unexpected entry under objects/, ignored");
            continue;
        }
        let meta = entry.metadata().map_err(Error::io("stat", &path))?;
        if !meta.is_file() {
            warn!(path = %path.display(), "unexpected entry under objects/, ignored");
            continue;
        }
        let modified = meta.modified().map_err(Error::io("stat", &path))?;
        out.push(Scanned {
            digest,
            size: meta.len(),
            modified,
        });
    }
    Ok(())
}

/// Rebuilds the index from `objects/` (D36): shards scanned in parallel with local
/// accumulation, one sort by `mtime`, the map built at exact capacity. Objects above
/// `max_size` (a shrunk configuration) are evicted, oldest first, before readiness.
/// Test gate: while set, start-up scans wait, opening the `not_ready` window (D14).
#[cfg(test)]
static HOLD_SCAN: AtomicBool = AtomicBool::new(false);

fn scan_blocking(inner: &Arc<Inner>) {
    while inner.hooks.hold_scan.load(Ordering::Acquire) {
        std::thread::sleep(Duration::from_millis(1));
    }
    #[cfg(test)]
    while HOLD_SCAN.load(Ordering::Acquire) {
        std::thread::sleep(Duration::from_millis(1));
    }
    let objects = inner.root.join("objects");
    let shards = match shard_dirs(&objects) {
        Ok(shards) => shards,
        Err(e) => {
            error!(error = %e, "start-up scan failed; the store stays not ready");
            return;
        }
    };
    let workers = std::thread::available_parallelism()
        .map_or(1, usize::from)
        .clamp(1, 8);
    let scanned: Option<Vec<Scanned>> = std::thread::scope(|scope| {
        let chunks: Vec<&[PathBuf]> = shards
            .chunks(shards.len().div_ceil(workers).max(1))
            .collect();
        let mut handles = Vec::new();
        for chunk in chunks {
            handles.push(scope.spawn(move || {
                let mut local = Vec::new();
                let mut failure = None;
                for shard in chunk {
                    if let Err(e) = scan_shard(shard, &mut local) {
                        failure = Some(e);
                        break;
                    }
                }
                (local, failure)
            }));
        }
        let mut all = Vec::new();
        for handle in handles {
            let (local, failure) = handle.join().unwrap_or_else(|_| (Vec::new(), None));
            if let Some(e) = failure {
                error!(error = %e, "start-up scan failed; the store stays not ready");
                return None;
            }
            all.extend(local);
        }
        Some(all)
    });
    let Some(mut scanned) = scanned else { return };
    // Deterministic order for equal mtimes: by digest. Oldest first.
    scanned.sort_by(|a, b| {
        a.modified
            .cmp(&b.modified)
            .then(a.digest.0.cmp(&b.digest.0))
    });
    let mut evictions: Vec<PendingDelete> = Vec::new();
    let mut evicted_sizes: Vec<u64> = Vec::new();
    {
        let mut index = Index::with_capacity(scanned.len());
        for object in &scanned {
            index.commit(
                object.digest,
                object.size,
                charge(object.size),
                object.modified,
            );
        }
        // The fit measure here is the committed size: entries decided here move to
        // `pending`, which bounds later admissions until their unlinks land.
        while index.size > inner.options.max_size {
            let over = index.size - inner.options.max_size;
            let candidates = index.eviction_candidates(over);
            if candidates.is_empty() {
                break;
            }
            for candidate in candidates {
                if let Some(entry) = index.remove(candidate) {
                    index.pending = index.pending.saturating_add(entry.charged);
                    evicted_sizes.push(entry.size);
                    evictions.push(PendingDelete {
                        digest: entry.digest,
                        generation: entry.generation,
                        charged: entry.charged,
                        path: object_path(&inner.root, entry.digest),
                    });
                }
            }
        }
        *inner.index.lock().expect("index lock poisoned") = index;
        inner.ready.store(true, Ordering::Release);
    }
    for (pending, evicted) in evictions.into_iter().zip(evicted_sizes) {
        inner.options.metrics.eviction(evicted);
        let delete_inner = Arc::clone(inner);
        spawn_tracked(inner, move || delete_pending(&delete_inner, &pending));
    }
    refresh_free_disk(inner);
}

fn close_blocking(inner: &Arc<Inner>) -> Result<(), Error> {
    // Writers and background tasks must all be done; spawning none from here on (the late
    // touches below run inline, not as background tasks).
    while inner.background.load(Ordering::Acquire) > 0 {
        std::thread::sleep(Duration::from_millis(1));
    }
    let late = {
        let mut index = inner.index.lock().expect("index lock poisoned");
        index.take_late_touches()
    };
    for (digest, when) in late {
        touch(inner, &object_path(&inner.root, digest), when);
    }
    if inner.options.durability == Durability::PurgeOnCrash {
        syncfs_checked(inner)?;
    }
    write_state(
        &inner.root,
        State {
            mode: inner.options.durability,
            clean: true,
        },
    )
}

/// `syncfs` on the objects' file system, success checked (D32). Linux only: elsewhere a
/// `purge-on-crash` run can never be certified clean, so close fails and the next start
/// purges — the configuration already refuses the mode off Linux.
fn syncfs_checked(inner: &Inner) -> Result<(), Error> {
    let objects = inner.root.join("objects");
    if inner.hooks.fail_syncfs.load(Ordering::Acquire) {
        return Err(Error::io("syncfs", &objects)(io::Error::other(
            "injected syncfs failure",
        )));
    }
    #[cfg(target_os = "linux")]
    {
        rustix::fs::syncfs(&inner.objects_dir)
            .map_err(io::Error::from)
            .map_err(Error::io("syncfs", &objects))
    }
    #[cfg(not(target_os = "linux"))]
    {
        Err(Error::io("syncfs", &objects)(io::Error::new(
            io::ErrorKind::Unsupported,
            "syncfs is not available on this platform",
        )))
    }
}

/// Room reserved for one PUT, from admission to its end. Dropping it without a stored object
/// gives the room back.
#[derive(Debug)]
pub(crate) struct Reservation {
    inner: Arc<Inner>,
    digest: Digest,
    destination: PathBuf,
    size: u64,
    charged: u64,
    /// True once the writer task owns settling (inside [`Reservation::write`]), or the
    /// object is stored. An unsettled reservation is cancelled by `Drop`.
    settled: bool,
}

/// One message to a write's blocking task (D34).
enum WriteMsg {
    Chunk(Bytes),
    /// The body stream failed; abandon the write.
    Abort,
}

/// How a writer task ended.
enum WriteResult {
    Stored,
    Aborted,
    Io(Error),
}

/// Gives a reservation's room back and unpins its key.
fn release_reservation(inner: &Inner, digest: Digest, charged: u64) {
    let mut index = inner.index.lock().expect("index lock poisoned");
    index.reserved = index.reserved.saturating_sub(charged);
    index.unpin(digest);
}

fn remove_temporary(path: &Path) {
    match std::fs::remove_file(path) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => warn!(path = %path.display(), error = %e, "cannot remove temporary file"),
    }
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
        // From here the writer task owns settling the reservation on every path,
        // including this future being dropped (its channel then closes early).
        self.settled = true;
        let (sender, receiver) = mpsc::channel(WRITE_CHANNEL_DEPTH);
        let (result_sender, result_receiver) = oneshot::channel();
        let inner = Arc::clone(&self.inner);
        let (digest, destination, size, charged) = (
            self.digest,
            self.destination.clone(),
            self.size,
            self.charged,
        );
        spawn_tracked(&self.inner, move || {
            let result = writer_blocking(&inner, receiver, digest, &destination, size, charged);
            drop(result_sender.send(result));
        });
        let mut body = pin!(body);
        let mut writer_gone = false;
        while let Some(chunk) = body.next().await {
            let Ok(chunk) = chunk else {
                drop(sender.send(WriteMsg::Abort).await);
                break;
            };
            if sender.send(WriteMsg::Chunk(chunk)).await.is_err() {
                writer_gone = true;
                break;
            }
        }
        drop(sender);
        match result_receiver.await {
            Ok(WriteResult::Stored) => Outcome::Stored,
            Ok(WriteResult::Aborted) => Outcome::Aborted,
            Ok(WriteResult::Io(e)) => {
                error!(error = %e, "PUT dropped after admission");
                if writer_gone {
                    while let Some(Ok(_)) = body.next().await {}
                }
                Outcome::Dropped(DropReason::Io)
            }
            Err(_) => {
                error!("writer task ended without a result");
                Outcome::Dropped(DropReason::Io)
            }
        }
    }
}

/// The write's one blocking task (D34): owns the temporary file, writes through a
/// [`WRITE_BUFFER`] `BufWriter`, then validates in the same task.
fn writer_blocking(
    inner: &Arc<Inner>,
    mut receiver: mpsc::Receiver<WriteMsg>,
    digest: Digest,
    destination: &Path,
    size: u64,
    charged: u64,
) -> WriteResult {
    let id = inner.next_tmp.fetch_add(1, Ordering::Relaxed);
    let temporary = inner
        .root
        .join("tmp")
        .join(format!("{}-{id}", std::process::id()));
    let file = match StdFile::create(&temporary) {
        Ok(file) => file,
        Err(e) => {
            release_reservation(inner, digest, charged);
            return WriteResult::Io(Error::io("create", &temporary)(e));
        }
    };
    let abandon = |result: WriteResult| -> WriteResult {
        remove_temporary(&temporary);
        release_reservation(inner, digest, charged);
        result
    };
    let mut writer = BufWriter::with_capacity(WRITE_BUFFER, file);
    let mut bytes_seen = 0_u64;
    loop {
        match receiver.blocking_recv() {
            Some(WriteMsg::Chunk(chunk)) => {
                bytes_seen = bytes_seen.saturating_add(len_u64(&chunk));
                if bytes_seen > size {
                    return abandon(WriteResult::Aborted);
                }
                if let Err(e) = writer.write_all(&chunk) {
                    return abandon(WriteResult::Io(Error::io("write", &temporary)(e)));
                }
            }
            Some(WriteMsg::Abort) => return abandon(WriteResult::Aborted),
            None => break,
        }
    }
    if bytes_seen != size {
        return abandon(WriteResult::Aborted);
    }
    if let Err(e) = writer.flush() {
        return abandon(WriteResult::Io(Error::io("write", &temporary)(e)));
    }
    if inner.options.durability == Durability::Fsync
        && let Err(e) = writer.get_ref().sync_all()
    {
        return abandon(WriteResult::Io(Error::io("fsync", &temporary)(e)));
    }
    drop(writer);
    // The parent is created before the validation lock, so a validation stuck in I/O is
    // observable (and admissions never wait for either).
    if let Some(parent) = destination.parent()
        && let Err(e) = std::fs::create_dir_all(parent)
    {
        return abandon(WriteResult::Io(Error::io("create", parent)(e)));
    }
    let modified = {
        let _validation = inner.validation.lock().expect("validation lock poisoned");
        if let Err(e) = std::fs::rename(&temporary, destination) {
            return abandon(WriteResult::Io(Error::io("rename", destination)(e)));
        }
        // The rename already made the object visible: a failed stat must not lose it.
        std::fs::metadata(destination)
            .and_then(|meta| meta.modified())
            .unwrap_or_else(|_| SystemTime::now())
    };
    // The object is visible on disk; the index catches up here, still holding the
    // reservation until this O(1) section converts it into the stored entry.
    {
        let mut index = inner.index.lock().expect("index lock poisoned");
        index.commit(digest, size, charged, modified);
        index.reserved = index.reserved.saturating_sub(charged);
        index.unpin(digest);
    }
    inner.options.metrics.written_bytes(size);
    refresh_free_disk(inner);
    WriteResult::Stored
}

impl Drop for Reservation {
    /// Cancels an unsettled reservation (one [`Reservation::write`] never took): no file
    /// exists yet, only the room and the pin are held. The release is handed to the
    /// blocking pool and the room stays charged until it runs, so D12 holds meanwhile and
    /// the dropping thread never does the store's bookkeeping synchronously (P3). Without
    /// a runtime there is nowhere to hand it to; the O(1) release runs inline, file-free.
    fn drop(&mut self) {
        if self.settled {
            return;
        }
        let inner = Arc::clone(&self.inner);
        let (digest, charged) = (self.digest, self.charged);
        match Handle::try_current() {
            Ok(_) => spawn_tracked(&self.inner, move || {
                release_reservation(&inner, digest, charged);
            }),
            Err(_) => release_reservation(&inner, digest, charged),
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

    /// Waits for the background start-up scan to make the store ready.
    async fn ready(store: &Store) {
        eventually("store readiness", || store.is_ready()).await;
    }

    /// Reopens after a drop; the previous store's last background task may hold the
    /// `flock` for a moment after the handle is gone.
    async fn open_retried(dir: &Path, opts: Options) -> Store {
        for _ in 0..200 {
            match Store::open(dir, opts.clone()).await {
                Ok(store) => return store,
                Err(Error::Locked(_)) => {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(e) => panic!("open failed: {e}"),
            }
        }
        panic!("store stayed locked for 2s");
    }

    async fn put(store: &Store, key: &Key, data: &'static [u8]) -> Outcome {
        ready(store).await;
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
        ready(&store).await;
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
        let mut files: Vec<_> = walk(dir.path());
        files.sort();
        assert_eq!(
            files,
            [dir.path().join("lock"), dir.path().join("state")],
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
        ready(&store).await;
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
        ready(&store).await;
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
        ready(&store).await;
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
        ready(&store).await;
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
        ready(&store).await;
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
    async fn eviction_lru_makes_room() {
        let dir = TempDir::new().unwrap();
        let store = Store::open(dir.path(), options(2 * BLOCK)).await.unwrap();
        assert_eq!(put(&store, &key("a"), b"aaaa").await, Outcome::Stored);
        assert_eq!(put(&store, &key("b"), b"bbbb").await, Outcome::Stored);
        // Reading `a` makes `b` the least recently used; `c` must evict `b`.
        assert!(contents(&store, &key("a")).await.is_some());
        assert_eq!(put(&store, &key("c"), b"cccc").await, Outcome::Stored);
        assert!(contents(&store, &key("a")).await.is_some(), "a evicted");
        assert!(contents(&store, &key("b")).await.is_none(), "b not evicted");
        assert!(contents(&store, &key("c")).await.is_some(), "c missing");
    }

    #[tokio::test]
    async fn min_free_disk_refuses() {
        let dir = TempDir::new().unwrap();
        let mut opts = options(MIB);
        opts.min_free_disk = u64::MAX / 2;
        let store = Store::open(dir.path(), opts).await.unwrap();
        ready(&store).await;
        assert!(matches!(
            store.admit(&key("k"), 4),
            Admission::Dropped(DropReason::NoSpace)
        ));
    }

    #[tokio::test]
    async fn unclean_purge_on_crash_state_purges() {
        let dir = TempDir::new().unwrap();
        let mut opts = options(MIB);
        opts.durability = Durability::PurgeOnCrash;
        let store = Store::open(dir.path(), opts.clone()).await.unwrap();
        assert_eq!(put(&store, &key("k"), b"data").await, Outcome::Stored);
        drop(store); // No close(): an unclean purge-on-crash run.
        let reopened = open_retried(dir.path(), opts).await;
        assert!(
            contents(&reopened, &key("k")).await.is_none(),
            "object of an unclean purge-on-crash run was served"
        );
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
        let reopened = open_retried(dir.path(), options(MIB)).await;
        assert_eq!(contents(&reopened, &k).await.as_deref(), Some(&b"data"[..]));
        ready(&reopened).await;
        assert_eq!(reopened.stats().entries, 1);
    }

    async fn put_vec(store: &Store, key: &Key, data: Vec<u8>) -> Outcome {
        ready(store).await;
        let size = u64::try_from(data.len()).unwrap();
        match store.admit(key, size) {
            Admission::Reserved(reservation) => {
                let body = stream::iter([Ok::<_, Infallible>(Bytes::from(data))]);
                reservation.write(body).await
            }
            Admission::Dropped(reason) => Outcome::Dropped(reason),
        }
    }

    /// Charged bytes of every file under `objects/` and `tmp/`: the D12 disk measure.
    fn disk_charged(dir: &Path) -> u64 {
        let mut total = 0_u64;
        for sub in ["objects", "tmp"] {
            for path in walk(&dir.join(sub)) {
                if let Ok(meta) = std::fs::metadata(&path) {
                    total = total.saturating_add(charge(meta.len()));
                }
            }
        }
        total
    }

    fn object_files(dir: &Path) -> usize {
        walk(&dir.join("objects")).len()
    }

    // Criterion 1: the measured disk occupation never exceeds `max_size`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_puts_never_exceed_max_size() {
        let dir = TempDir::new().unwrap();
        let max_size = 64 * BLOCK;
        let store = Store::open(dir.path(), options(max_size)).await.unwrap();
        ready(&store).await;
        let peak = Arc::new(AtomicU64::new(0));
        let stop = Arc::new(AtomicBool::new(false));
        let sampler = {
            let (peak, stop, root) = (
                Arc::clone(&peak),
                Arc::clone(&stop),
                dir.path().to_path_buf(),
            );
            tokio::spawn(async move {
                while !stop.load(Ordering::Acquire) {
                    peak.fetch_max(disk_charged(&root), Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_micros(200)).await;
                }
                peak.fetch_max(disk_charged(&root), Ordering::SeqCst);
            })
        };
        let mut writers = Vec::new();
        for i in 0_u8..64 {
            let store = store.clone();
            writers.push(tokio::spawn(async move {
                let len = (usize::from(i) * 37 % 40 + 1) * 1024;
                put_vec(&store, &key(&format!("k{i}")), vec![i; len]).await
            }));
        }
        for writer in writers {
            writer.await.unwrap();
        }
        stop.store(true, Ordering::Release);
        sampler.await.unwrap();
        let peak = peak.load(Ordering::SeqCst);
        assert!(
            peak <= max_size,
            "disk occupation peaked at {peak}, above max_size {max_size}"
        );
        eventually("the index to equal the disk", || {
            let stats = store.stats();
            stats.size_bytes == disk_charged(dir.path())
                && stats.entries == u64::try_from(object_files(dir.path())).unwrap()
                && temporaries(&dir) == 0
        })
        .await;
    }

    // Criterion 1b: a pending deletion of the old version must not remove the new one.
    #[tokio::test]
    async fn evicted_then_rewritten_key_survives_pending_delete() {
        let dir = TempDir::new().unwrap();
        let store = Store::open(dir.path(), options(3 * BLOCK)).await.unwrap();
        assert_eq!(put(&store, &key("a"), b"old!").await, Outcome::Stored);
        assert_eq!(put(&store, &key("b"), b"bbbb").await, Outcome::Stored);
        assert_eq!(put(&store, &key("c"), b"cccc").await, Outcome::Stored);
        store
            .inner
            .hooks
            .hold_deletions
            .store(true, Ordering::Release);
        // Evicts `a` (LRU); its deletion stays pending behind the hook.
        assert_eq!(put(&store, &key("d"), b"dddd").await, Outcome::Stored);
        assert!(
            dir.path().join(key("a").relative_path()).is_file(),
            "pending deletion ran too early"
        );
        // Rewriting `a` now renames over the file the pending deletion targets.
        assert_eq!(put(&store, &key("a"), b"new!").await, Outcome::Stored);
        store
            .inner
            .hooks
            .hold_deletions
            .store(false, Ordering::Release);
        eventually("pending deletions to drain", || {
            store.inner.background.load(Ordering::Acquire) == 0
        })
        .await;
        assert_eq!(
            contents(&store, &key("a")).await.as_deref(),
            Some(&b"new!"[..]),
            "the pending deletion removed the rewritten version"
        );
    }

    // Criterion 1c: eviction between lookup and open is absent or the new bytes, never Err.
    #[tokio::test]
    async fn read_racing_an_eviction_is_absent_or_new_bytes() {
        let dir = TempDir::new().unwrap();
        let store = Store::open(dir.path(), options(MIB)).await.unwrap();
        assert_eq!(put(&store, &key("gone"), b"orig").await, Outcome::Stored);
        store.inner.hooks.vanish_on_read.store(1, Ordering::Release);
        assert!(
            store.read(&key("gone")).await.unwrap().is_none(),
            "a vanished file must read as absent"
        );
        assert_eq!(put(&store, &key("swapped"), b"orig").await, Outcome::Stored);
        store.inner.hooks.vanish_on_read.store(2, Ordering::Release);
        assert_eq!(
            contents(&store, &key("swapped")).await.as_deref(),
            Some(&b"replacement"[..]),
            "the retry must open the replacement"
        );
    }

    // Criterion 2: one key, many writers, exactly one whole content wins.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_writes_of_one_key_never_mix() {
        let dir = TempDir::new().unwrap();
        let store = Store::open(dir.path(), options(MIB)).await.unwrap();
        ready(&store).await;
        let bodies: Vec<Vec<u8>> = (0_u8..16)
            .map(|i| vec![i; 1000 + usize::from(i) * 100])
            .collect();
        let mut writers = Vec::new();
        for body_bytes in &bodies {
            let (store, data) = (store.clone(), body_bytes.clone());
            writers.push(tokio::spawn(async move {
                put_vec(&store, &key("same"), data).await
            }));
        }
        for writer in writers {
            assert_eq!(writer.await.unwrap(), Outcome::Stored);
        }
        let got = contents(&store, &key("same")).await.unwrap();
        assert!(
            bodies.contains(&got),
            "the stored content is a mixture: {} bytes of {}",
            got.len(),
            got.first().copied().unwrap_or(0)
        );
        let stats = store.stats();
        assert_eq!(stats.entries, 1);
        assert_eq!(stats.size_bytes, charge(u64::try_from(got.len()).unwrap()));
    }

    // Criterion 3: a reader keeps its version across an eviction (POSIX open file).
    #[tokio::test]
    async fn eviction_during_a_large_read_loses_nothing() {
        let dir = TempDir::new().unwrap();
        let store = Store::open(dir.path(), options(MIB)).await.unwrap();
        let big: Vec<u8> = (0..512 * 1024)
            .map(|i| u8::try_from(i % 251).unwrap())
            .collect();
        assert_eq!(
            put_vec(&store, &key("big"), big.clone()).await,
            Outcome::Stored
        );
        let object = store.read(&key("big")).await.unwrap().unwrap();
        let big_path = dir.path().join(key("big").relative_path());
        for i in 0..8 {
            assert_eq!(
                put_vec(&store, &key(&format!("filler{i}")), vec![7; 128 * 1024]).await,
                Outcome::Stored
            );
        }
        eventually("the evicted file to be unlinked", || !big_path.exists()).await;
        let chunks: Vec<_> = pin!(object.into_stream())
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .map(|chunk| chunk.unwrap())
            .collect();
        assert!(chunks.iter().all(|chunk| chunk.len() <= READ_CHUNK));
        let streamed: Vec<u8> = chunks.concat();
        assert_eq!(streamed, big);
    }

    // Criterion 4: recency survives a restart through `mtime`.
    #[tokio::test]
    async fn recency_persists_across_a_reopen() {
        let dir = TempDir::new().unwrap();
        let mut opts = options(2 * BLOCK);
        opts.touch_interval = Duration::ZERO;
        let store = Store::open(dir.path(), opts.clone()).await.unwrap();
        assert_eq!(put(&store, &key("a"), b"aaaa").await, Outcome::Stored);
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(put(&store, &key("b"), b"bbbb").await, Outcome::Stored);
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(contents(&store, &key("a")).await.is_some());
        store.close().await.unwrap();
        drop(store);
        let reopened = open_retried(dir.path(), opts).await;
        ready(&reopened).await;
        assert_eq!(put(&reopened, &key("c"), b"cccc").await, Outcome::Stored);
        assert!(
            contents(&reopened, &key("a")).await.is_some(),
            "touched a was evicted"
        );
        assert!(
            contents(&reopened, &key("b")).await.is_none(),
            "stale b was kept"
        );
    }

    // Criterion 5: crash leftovers in `tmp/` do not survive or get served.
    #[tokio::test]
    async fn crash_leftovers_do_not_survive_a_restart() {
        let dir = TempDir::new().unwrap();
        let store = Store::open(dir.path(), options(MIB)).await.unwrap();
        assert_eq!(put(&store, &key("keep"), b"data").await, Outcome::Stored);
        store.close().await.unwrap();
        drop(store);
        std::fs::write(dir.path().join("tmp").join("partial"), b"truncated upload").unwrap();
        let reopened = open_retried(dir.path(), options(MIB)).await;
        assert_eq!(temporaries(&dir), 0, "tmp/ was not emptied at start-up");
        ready(&reopened).await;
        assert_eq!(reopened.stats().entries, 1);
        assert_eq!(
            contents(&reopened, &key("keep")).await.as_deref(),
            Some(&b"data"[..])
        );
    }

    #[tokio::test]
    async fn unexpected_files_are_ignored_never_deleted() {
        let dir = TempDir::new().unwrap();
        let store = Store::open(dir.path(), options(MIB)).await.unwrap();
        assert_eq!(put(&store, &key("keep"), b"data").await, Outcome::Stored);
        store.close().await.unwrap();
        drop(store);
        let junk = dir.path().join("objects").join("junk.txt");
        std::fs::write(&junk, b"not ours").unwrap();
        let off_scheme = dir.path().join("objects").join("zz");
        std::fs::create_dir_all(&off_scheme).unwrap();
        std::fs::write(off_scheme.join("whatever"), b"not ours").unwrap();
        let reopened = open_retried(dir.path(), options(MIB)).await;
        ready(&reopened).await;
        assert!(junk.is_file(), "an unknown file was deleted");
        assert!(
            off_scheme.join("whatever").is_file(),
            "an unknown file was deleted"
        );
        assert_eq!(reopened.stats().entries, 1);
    }

    #[tokio::test]
    async fn startup_evicts_down_to_a_shrunk_max_size() {
        let dir = TempDir::new().unwrap();
        let store = Store::open(dir.path(), options(MIB)).await.unwrap();
        for name in ["a", "b", "c"] {
            assert_eq!(put(&store, &key(name), b"xxxx").await, Outcome::Stored);
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        store.close().await.unwrap();
        drop(store);
        let reopened = open_retried(dir.path(), options(2 * BLOCK)).await;
        ready(&reopened).await;
        assert_eq!(reopened.stats().entries, 2);
        assert!(
            contents(&reopened, &key("a")).await.is_none(),
            "oldest kept"
        );
        assert!(
            contents(&reopened, &key("c")).await.is_some(),
            "newest evicted"
        );
        eventually("the evicted file to be deleted", || {
            object_files(dir.path()) == 2
        })
        .await;
    }

    #[tokio::test]
    async fn not_ready_during_the_startup_scan() {
        let dir = TempDir::new().unwrap();
        let store = Store::open(dir.path(), options(MIB)).await.unwrap();
        assert_eq!(put(&store, &key("k"), b"data").await, Outcome::Stored);
        store.close().await.unwrap();
        drop(store);
        HOLD_SCAN.store(true, Ordering::Release);
        let reopened = open_retried(dir.path(), options(MIB)).await;
        let during = (
            reopened.is_ready(),
            matches!(
                reopened.admit(&key("k"), 4),
                Admission::Dropped(DropReason::NotReady)
            ),
            matches!(
                reopened.admit(&key("k"), MIB + 1),
                Admission::Dropped(DropReason::TooLarge)
            ),
            contents(&reopened, &key("k"))
                .await
                .as_deref()
                .map(<[u8]>::to_vec),
        );
        HOLD_SCAN.store(false, Ordering::Release);
        assert_eq!(during, (false, true, true, Some(b"data".to_vec())));
        ready(&reopened).await;
        assert!(matches!(
            reopened.admit(&key("k"), 4),
            Admission::Reserved(_)
        ));
    }

    #[tokio::test]
    async fn pinned_key_is_not_evicted() {
        let dir = TempDir::new().unwrap();
        let store = Store::open(dir.path(), options(2 * BLOCK)).await.unwrap();
        assert_eq!(put(&store, &key("a"), b"aaaa").await, Outcome::Stored);
        assert_eq!(put(&store, &key("b"), b"bbbb").await, Outcome::Stored);
        // Reserving `a` again pins its stored version; making room evicts `b`.
        let Admission::Reserved(reservation) = store.admit(&key("a"), 4) else {
            panic!("not admitted")
        };
        assert_eq!(
            contents(&store, &key("a")).await.as_deref(),
            Some(&b"aaaa"[..])
        );
        assert!(contents(&store, &key("b")).await.is_none());
        assert_eq!(reservation.write(body(&[b"new!"])).await, Outcome::Stored);
        assert_eq!(
            contents(&store, &key("a")).await.as_deref(),
            Some(&b"new!"[..])
        );
    }

    // D33: small objects are one `Bytes`; large ones are 64 KiB blocks.
    #[tokio::test]
    async fn read_stream_shapes_follow_d33() {
        let dir = TempDir::new().unwrap();
        let store = Store::open(dir.path(), options(MIB)).await.unwrap();
        assert_eq!(
            put_vec(&store, &key("small"), vec![3; 1000]).await,
            Outcome::Stored
        );
        let small = store.read(&key("small")).await.unwrap().unwrap();
        let chunks: Vec<_> = pin!(small.into_stream()).collect::<Vec<_>>().await;
        assert_eq!(chunks.len(), 1, "a small object must be a single Bytes");
        assert_eq!(
            put_vec(&store, &key("large"), vec![4; 200 * 1024]).await,
            Outcome::Stored
        );
        let large = store.read(&key("large")).await.unwrap().unwrap();
        let chunks: Vec<_> = pin!(large.into_stream())
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .map(|chunk| chunk.unwrap())
            .collect();
        assert_eq!(chunks.len(), 4);
        assert!(chunks.iter().all(|chunk| chunk.len() <= READ_CHUNK));
        assert_eq!(chunks.concat(), vec![4; 200 * 1024]);
    }

    // Criterion 7: a 1 GiB object streams in bounded blocks (sparse file on disk).
    #[tokio::test]
    async fn one_gib_object_streams_in_bounded_blocks() {
        const GIB: u64 = 1024 * 1024 * 1024;
        let dir = TempDir::new().unwrap();
        let store = Store::open(dir.path(), options(4 * GIB)).await.unwrap();
        store.close().await.unwrap();
        drop(store);
        let path = dir.path().join(key("huge").relative_path());
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        StdFile::create(&path).unwrap().set_len(GIB).unwrap();
        let reopened = open_retried(dir.path(), options(4 * GIB)).await;
        ready(&reopened).await;
        let object = reopened.read(&key("huge")).await.unwrap().unwrap();
        assert_eq!(object.size, GIB);
        let mut stream = pin!(object.into_stream());
        let (mut total, mut blocks) = (0_u64, 0_u64);
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.unwrap();
            assert!(chunk.len() <= READ_CHUNK, "oversized block {}", chunk.len());
            total += u64::try_from(chunk.len()).unwrap();
            blocks += 1;
        }
        assert_eq!(total, GIB);
        assert_eq!(blocks, GIB / u64::try_from(READ_CHUNK).unwrap());
    }

    // Criterion 9: the D32 decision table, state by crafted state.
    #[test]
    fn purge_decision_table() {
        assert!(purge_needed(None), "absent state must purge");
        assert!(purge_needed(Some(State {
            mode: Durability::PurgeOnCrash,
            clean: false
        })));
        assert!(!purge_needed(Some(State {
            mode: Durability::PurgeOnCrash,
            clean: true
        })));
        assert!(!purge_needed(Some(State {
            mode: Durability::Fsync,
            clean: false
        })));
        assert!(!purge_needed(Some(State {
            mode: Durability::Fsync,
            clean: true
        })));
    }

    fn craft_object(dir: &Path, name: &str) -> PathBuf {
        let path = dir.join(key(name).relative_path());
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, b"crafted").unwrap();
        path
    }

    #[tokio::test]
    async fn absent_or_malformed_state_purges() {
        for state in [None, Some("not a state\n")] {
            let dir = TempDir::new().unwrap();
            let object = craft_object(dir.path(), "k");
            if let Some(text) = state {
                std::fs::write(dir.path().join("state"), text).unwrap();
            }
            let store = Store::open(dir.path(), options(MIB)).await.unwrap();
            assert!(
                !object.exists(),
                "an object under state {state:?} survived the purge"
            );
            ready(&store).await;
            assert_eq!(store.stats().entries, 0);
        }
    }

    #[tokio::test]
    async fn interrupted_purge_repeats_even_switching_to_fsync() {
        let dir = TempDir::new().unwrap();
        let first = craft_object(dir.path(), "k1");
        let second = craft_object(dir.path(), "k2");
        // A purge interrupted halfway leaves exactly this: purge demanded, objects left.
        std::fs::write(
            dir.path().join("state"),
            "mode=purge-on-crash\nclean=false\n",
        )
        .unwrap();
        let store = Store::open(dir.path(), options(MIB)).await.unwrap();
        assert!(
            !first.exists() && !second.exists(),
            "the repeated purge missed an object"
        );
        let state = std::fs::read_to_string(dir.path().join("state")).unwrap();
        assert_eq!(state, "mode=fsync\nclean=false\n");
        drop(store);
    }

    #[tokio::test]
    async fn clean_purge_on_crash_state_keeps_objects() {
        let dir = TempDir::new().unwrap();
        let object = craft_object(dir.path(), "k");
        std::fs::write(
            dir.path().join("state"),
            "mode=purge-on-crash\nclean=true\n",
        )
        .unwrap();
        let store = Store::open(dir.path(), options(MIB)).await.unwrap();
        assert!(object.exists(), "a clean previous run's object was purged");
        ready(&store).await;
        assert_eq!(
            contents(&store, &key("k")).await.as_deref(),
            Some(&b"crafted"[..])
        );
    }

    #[tokio::test]
    async fn syncfs_failure_forbids_clean_and_purges_next_start() {
        let dir = TempDir::new().unwrap();
        let mut opts = options(MIB);
        opts.durability = Durability::PurgeOnCrash;
        let store = Store::open(dir.path(), opts.clone()).await.unwrap();
        assert_eq!(put(&store, &key("k"), b"data").await, Outcome::Stored);
        store.inner.hooks.fail_syncfs.store(true, Ordering::Release);
        assert!(
            store.close().await.is_err(),
            "a failing syncfs must fail close"
        );
        let state = std::fs::read_to_string(dir.path().join("state")).unwrap();
        assert!(
            state.contains("clean=false"),
            "state wrongly clean: {state}"
        );
        drop(store);
        let reopened = open_retried(dir.path(), opts).await;
        ready(&reopened).await;
        assert!(contents(&reopened, &key("k")).await.is_none());
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn clean_close_keeps_a_purge_on_crash_store() {
        let dir = TempDir::new().unwrap();
        let mut opts = options(MIB);
        opts.durability = Durability::PurgeOnCrash;
        let store = Store::open(dir.path(), opts.clone()).await.unwrap();
        assert_eq!(put(&store, &key("k"), b"data").await, Outcome::Stored);
        store.close().await.unwrap();
        let state = std::fs::read_to_string(dir.path().join("state")).unwrap();
        assert_eq!(state, "mode=purge-on-crash\nclean=true\n");
        drop(store);
        let reopened = open_retried(dir.path(), opts).await;
        ready(&reopened).await;
        assert_eq!(
            contents(&reopened, &key("k")).await.as_deref(),
            Some(&b"data"[..])
        );
    }

    // Criterion 8 (local, not CI): parallel 1 MiB PUT/GET throughput, so the review can
    // see the cost of the one index mutex. Run in release:
    // `cargo test --release bench_ -- --ignored --nocapture`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "local microbench"]
    async fn bench_put_get_1mib_parallel() {
        use std::time::Instant;
        const TASKS: u64 = 16;
        const OPS: u64 = 25;
        let dir = TempDir::new().unwrap();
        let mut opts = options(8 * 1024 * MIB);
        opts.durability = Durability::PurgeOnCrash;
        let store = Store::open(dir.path(), opts).await.unwrap();
        ready(&store).await;
        let payload = Bytes::from(vec![9_u8; usize::try_from(MIB).unwrap()]);
        let started = Instant::now();
        let mut handles = Vec::new();
        for t in 0..TASKS {
            let (store, payload) = (store.clone(), payload.clone());
            handles.push(tokio::spawn(async move {
                for i in 0..OPS {
                    let k = key(&format!("bench/{t}/{i}"));
                    let Admission::Reserved(reservation) = store.admit(&k, MIB) else {
                        panic!("bench admission dropped")
                    };
                    let body = stream::iter([Ok::<_, Infallible>(payload.clone())]);
                    assert_eq!(reservation.write(body).await, Outcome::Stored);
                    let object = store.read(&k).await.unwrap().unwrap();
                    let mut streamed = pin!(object.into_stream());
                    let mut total = 0_u64;
                    while let Some(chunk) = streamed.next().await {
                        total += u64::try_from(chunk.unwrap().len()).unwrap();
                    }
                    assert_eq!(total, MIB);
                }
            }));
        }
        for handle in handles {
            handle.await.unwrap();
        }
        let elapsed = started.elapsed();
        let pairs = f64::from(u32::try_from(TASKS * OPS).unwrap());
        println!(
            "PUT+GET 1MiB x{} ({} tasks): {:?} -> {:.0} pairs/s, {:.0} MiB/s each way",
            TASKS * OPS,
            TASKS,
            elapsed,
            pairs / elapsed.as_secs_f64(),
            pairs / elapsed.as_secs_f64()
        );
    }

    // D33 (local, not CI): one-shot whole read vs 64 KiB streaming at 64/128/256 KiB.
    #[tokio::test]
    #[ignore = "local microbench"]
    async fn bench_read_one_shot_vs_streaming() {
        use std::time::Instant;
        const READS: u32 = 300;
        let dir = TempDir::new().unwrap();
        let store = Store::open(dir.path(), options(64 * MIB)).await.unwrap();
        for kib in [64_u64, 128, 256] {
            let size = kib * 1024;
            let name = format!("obj{kib}");
            assert_eq!(
                put_vec(&store, &key(&name), vec![5; usize::try_from(size).unwrap()]).await,
                Outcome::Stored
            );
            let path = dir.path().join(key(&name).relative_path());
            let started = Instant::now();
            for _ in 0..READS {
                let read_path = path.clone();
                let data = blocking(move || std::fs::read(&read_path).unwrap()).await;
                assert_eq!(u64::try_from(data.len()).unwrap(), size);
            }
            let whole = started.elapsed();
            let started = Instant::now();
            for _ in 0..READS {
                let object = store.read(&key(&name)).await.unwrap().unwrap();
                let mut streamed = pin!(object.into_stream());
                let mut total = 0_u64;
                while let Some(chunk) = streamed.next().await {
                    total += u64::try_from(chunk.unwrap().len()).unwrap();
                }
                assert_eq!(total, size);
            }
            let streamed = started.elapsed();
            println!(
                "{kib} KiB: whole-read {:?}/op, into_stream {:?}/op ({} reads)",
                whole / READS,
                streamed / READS,
                READS
            );
        }
    }

    // D36 (local, not CI): time to readiness over a prepared tree. Point
    // `CACHETTE_BENCH_SCAN_DIR` at a data directory holding `objects/` and a clean
    // `state`; prints entries and scan time.
    #[tokio::test]
    #[ignore = "local microbench"]
    async fn bench_startup_scan() {
        use std::time::Instant;
        let dir = std::env::var_os("CACHETTE_BENCH_SCAN_DIR")
            .expect("CACHETTE_BENCH_SCAN_DIR must name the prepared data directory");
        let started = Instant::now();
        let store = Store::open(Path::new(&dir), options(u64::MAX / 4))
            .await
            .unwrap();
        ready(&store).await;
        println!(
            "scan of {} entries ready in {:?}",
            store.stats().entries,
            started.elapsed()
        );
    }
}

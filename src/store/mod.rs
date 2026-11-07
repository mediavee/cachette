//! The disk store: what `http` calls.
//!
//! # The contract
//!
//! `http` calls [`Store::read`], [`Store::stat`] and [`Store::admit`], then
//! [`Reservation::write`]. What the store guarantees:
//!
//! * **Bounded size.** Live bytes (stored objects + reservations in flight) plus bytes
//!   evicted but not yet deleted never exceed `max_size`, at any instant. One POSIX exception:
//!   a file evicted or replaced while a GET reads it keeps its blocks until that read ends, a
//!   transient excess bounded by the reads in flight (bazel-remote behaves the same). Sizes are charged
//!   rounded up to 4 KiB. [`Store::admit`] evicts least recently used entries only for the
//!   reservation's own room in *live* bytes — never to cover deletions already in flight — and
//!   refuses with [`DropReason::NoSpace`] when the reservation would push live + being-deleted
//!   past `max_size`, when even evicting everything evictable would not fit it in live bytes, or
//!   when the disk would drop below `min_free_disk`. A sweep after each commit keeps live bytes
//!   at or below 95 % of `max_size`, so admissions in a full store usually find their room.
//!   [`Store::open`] evicts at start-up the same way, deleting synchronously before
//!   [`Store::is_ready`] turns `true`: when the scanned objects exceed `max_size` (a smaller
//!   `max_size` than last run), the least recently used go first. A deletion that fails keeps
//!   its charge and is retried with backoff; only a successful unlink, an `ENOENT`, or a
//!   superseding version releases it (same rule for temporary files). A closing store stops
//!   retrying, so [`Store::close`] never waits on a refusal; the file is left for the next start.
//! * **Pinned keys.** A key is not evictable from its reservation until it is stored or the
//!   reservation is dropped. An eviction decided before an object was stored never deletes it: a
//!   deletion still pending carries a generation, so it cannot remove a newer version.
//! * **Atomic writes.** The body goes to `objects/<2 hex>/.tmp-<unique>`, in the shard
//!   directory of its destination, then is renamed to the object path: a rename within one
//!   directory takes no file-system-wide lock, while one between two directories serialises
//!   every PUT. The `.tmp-` prefix can never be an object name, which is hex. The object is
//!   visible only after the rename, and `201` follows it. Concurrent writes of one
//!   key all succeed, the last rename wins and the key is counted once. A reader that already
//!   opened the file keeps the old version. The data directory is `flock`ed at [`Store::open`]
//!   before the start-up walk of `objects/` deletes every leftover `.tmp-*` file and indexes
//!   none.
//! * **Durability.** A `state` file in the data directory records the durability mode of
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
//! * **Index and readiness.** The index is rebuilt from `objects/` at start-up, the recency
//!   from `mtime`. Until it is built [`Store::is_ready`] is `false`, [`Store::admit`] answers
//!   [`DropReason::NotReady`] without waiting, and reads are served straight from disk. Recency
//!   is decided under the lock and written back to `mtime` outside it, at most once per
//!   `touch_interval`, and for every late entry in [`Store::close`].
//! * **Reads.** [`Store::read`] retries once when the file vanishes between the lookup
//!   and the open, then answers `None`. [`Object::into_stream`] has two paths: an object of
//!   64 KiB or less is read by one blocking task into a buffer of its size and leaves as a single
//!   `Bytes`; a larger one streams as `ReaderStream::with_capacity(file.take(length), 64 KiB)`,
//!   bounded by its known length, each block returning the blocking thread so a slow consumer
//!   holds none of the shared pool. `ReaderStream::new` (4 KiB blocks) is forbidden: it divides
//!   throughput by 6 to 10.
//! * **Writes.** An object of at most 64 KiB is read into memory, bounded by its
//!   announced size, then written, synced, renamed and published in one blocking pass: no
//!   channel, no owner task. A larger body goes through a bounded channel (depth 4) to one
//!   blocking task that owns the temporary file, writes through a 64 KiB `BufWriter`, then
//!   validates (`rename`, and `sync_all` before it in `fsync` mode). No `tokio::fs` write per
//!   frame. Consuming the body starts without waiting.
//! * **No waiting between headers and body.** [`Store::admit`] is synchronous and does no
//!   I/O. Nothing it takes can be held by a writer across I/O, and nothing in
//!   [`Reservation::write`] waits for the index, an eviction or a key lock before it starts
//!   consuming the body.
//! * **Index and locking.** One `std::sync::Mutex` over the index, with O(1) critical
//!   sections: never held across an `.await`, and **never across any I/O, [`Store::open`]
//!   included**. One validation lock per shard of the digest (256; not per key) serialises
//!   every rename of that shard, its index publication, and every pending deletion and `mtime`
//!   touch against each other; only writer and background tasks take it, always before the index
//!   lock. Eviction deletions and `mtime` touches are decided
//!   under the index lock and executed outside it. File I/O runs on tokio's blocking pool.
//! * **Metrics.** The store records `written_bytes` when an object becomes visible, `eviction` for
//!   each eviction and `crash_purge`, through the [`Metrics`] in its [`Options`]. `http` records
//!   everything else.
//!
//! # The implementation
//!
//! Files live at `objects/<2 hex>/<64 hex>` (the blake3 digest of the key). The in-memory
//! index ([`index::Index`]) maps that digest to a compact entry and keeps the LRU order in an
//! intrusive list, behind the one `Mutex`. Admission decides evictions under that lock;
//! deletions, `mtime` touches, file opens and every other I/O run outside it, on the blocking
//! pool. Validations and pending deletions of one shard are serialised against each other by the
//! separate validation lock of that shard, so a deletion decided against an old generation can never unlink a version
//! renamed after it.

mod index;
#[cfg(test)]
mod put_bench;

use std::fs::File as StdFile;
use std::io::{self, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::pin::pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, SystemTime};

use bytes::Bytes;
use futures_util::{Stream, StreamExt};
use thiserror::Error;
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tokio::runtime::Handle;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio_util::io::ReaderStream;
use tracing::{error, warn};

use crate::config::Durability;
use crate::key::{Key, object_path};
use crate::metrics::Metrics;
use index::{Digest, Index};

/// Granularity of the space accounting.
const BLOCK: u64 = 4096;
/// Size of the chunks a read streams, so memory does not depend on object size.
const READ_CHUNK: usize = 64 * 1024;
/// Objects at most this large are read by one blocking task into a single `Bytes`.
const SMALL_OBJECT: u64 = 64 * 1024;
/// Depth of the channel feeding a write's blocking task.
const WRITE_CHANNEL_DEPTH: usize = 4;
/// `BufWriter` capacity of a write's blocking task.
const WRITE_BUFFER: usize = 64 * 1024;
/// Objects at most this large are written in one blocking pass.
const SMALL_WRITE: u64 = 64 * 1024;
/// Validation locks: one per leading digest byte, like the fan-out of `objects/`.
const VALIDATION_SHARDS: usize = 256;
/// Name prefix of a file being written. Object names are hex, so it never collides.
const TEMP_PREFIX: &str = ".tmp-";
/// Chunks a streaming read may have in flight: at most twice [`READ_CHUNK`] bytes per reader.
const READ_CHANNEL_DEPTH: usize = 2;

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

/// Why a PUT was not stored. The label of `puts_dropped_total`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DropReason {
    /// The announced size is above `max_object_size`.
    TooLarge,
    /// The reservation does not fit under `max_size` or `min_free_disk`.
    NoSpace,
    /// The index is still being built.
    NotReady,
    /// An I/O error happened after admission.
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
    #[expect(
        dead_code,
        reason = "part of the API `http` calls; http sends no Last-Modified on GET"
    )]
    pub(crate) modified: SystemTime,
}

impl Object {
    /// Streams the file in bounded chunks: one blocking task reads a small object
    /// whole into a single `Bytes`; a larger one streams through a `ReaderStream` of
    /// [`READ_CHUNK`] blocks bounded by the known length, so the blocking thread is
    /// returned between blocks and a slow consumer holds none of the shared pool.
    pub(crate) fn into_stream(self) -> impl Stream<Item = io::Result<Bytes>> + Send {
        if self.size > SMALL_OBJECT {
            let stream = ReaderStream::with_capacity(self.file.take(self.size), READ_CHUNK);
            let pinned: std::pin::Pin<Box<dyn Stream<Item = io::Result<Bytes>> + Send>> =
                Box::pin(stream);
            return pinned;
        }
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
        Box::pin(futures_util::stream::unfold(
            receiver,
            |mut receiver| async move { receiver.recv().await.map(|chunk| (chunk, receiver)) },
        ))
    }
}

/// Reads exactly `size` bytes (a small object) from the start of `file` and sends them
/// as one block. Runs on the blocking pool.
fn read_file(mut file: StdFile, size: u64, sender: &mpsc::Sender<io::Result<Bytes>>) {
    let len = usize::try_from(size).unwrap_or(usize::MAX);
    let mut buffer = vec![0_u8; len];
    let chunk = match file.read_exact(&mut buffer) {
        Ok(()) => Ok(Bytes::from(buffer)),
        Err(e) => Err(e),
    };
    let _ = sender.blocking_send(chunk);
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
    /// The PUT will not be stored: answer 413, or 200 to Gradle, after reading and discarding
    /// the body.
    Dropped(DropReason),
}

/// How [`Reservation::write`] ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Outcome {
    /// The object is stored and visible. Answer 201.
    Stored,
    /// An I/O error ended the write; the cause is logged at `error`. The body has been read to
    /// its end, so the connection is healthy: answer 413, or 200 to Gradle.
    Dropped(DropReason),
    /// The body ended before the announced size, failed, or exceeded it. Nothing was stored.
    /// There is no client left to answer, or its request is malformed.
    Aborted,
}

/// Test-only fault and timing injection. Compiled out of normal builds entirely, so
/// nothing test-only sits on a hot path.
#[cfg(test)]
#[derive(Debug, Default)]
struct Hooks {
    /// Pending deletions wait until this is cleared (criterion 1b).
    hold_deletions: AtomicBool,
    /// `syncfs` at close reports an error (criterion 9).
    fail_syncfs: AtomicBool,
    /// One read unlinks the object file between its lookup and its open (criterion 1c):
    /// 1 = just unlink, 2 = unlink, then (3) a replacement appears before the retry.
    vanish_on_read: AtomicU64,
    /// The next writer to reach publication pauses there until `publish_release`.
    pause_publish: AtomicBool,
    publish_reached: AtomicBool,
    publish_release: AtomicBool,
    /// Recency touches pause before applying.
    pause_touch: AtomicBool,
    touch_reached: AtomicBool,
    /// The start-up scan of a shard reports an error.
    fail_scan: AtomicBool,
    /// The start-up scan of a shard panics.
    panic_scan: AtomicBool,
    /// The start-up scan waits while this is set, holding the `not_ready` window.
    hold_scan: AtomicBool,
    /// Free-disk samples wait while this is set, keeping the last one stale.
    hold_free_disk: AtomicBool,
    /// Free-disk samples report this instead of asking the file system.
    free_disk: Mutex<Option<u64>>,
    /// `close` pauses at this point until `close_release`.
    pause_close_at: Mutex<Option<ClosePoint>>,
    close_reached: AtomicBool,
    close_release: AtomicBool,
    /// `close_blocking` returned.
    close_done: AtomicBool,
    /// Work handed to the blocking pool or a thread, by any task of the store.
    dispatches: AtomicU64,
    /// Body chunks sent to a write's owner task over its channel.
    channel_sends: AtomicU64,
}

/// Where a test may pause [`close_blocking`], to cancel the close there.
#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClosePoint {
    Drained,
    Touched,
    Synced,
    Prepared,
}

#[cfg(test)]
fn close_point(inner: &Inner, point: ClosePoint) {
    if *inner
        .hooks
        .pause_close_at
        .lock()
        .expect("close hook poisoned")
        == Some(point)
    {
        inner.hooks.close_reached.store(true, Ordering::Release);
        while !inner.hooks.close_release.load(Ordering::Acquire) {
            std::thread::sleep(Duration::from_millis(1));
        }
    }
}

/// Whether a clean state may still be published: decided once, under one lock.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CloseDecision {
    Pending,
    /// The close was cancelled or ran past the shutdown deadline: never `clean = true`.
    Abandoned,
    Published,
}

#[derive(Debug)]
struct Inner {
    root: PathBuf,
    options: Options,
    /// The index; held for a few O(1) instructions, never across I/O or an `.await`.
    index: Mutex<Index>,
    /// One lock per shard of the digest, not per key: serialises every validation (rename
    /// and index publication) of a shard against the others, against pending deletions and
    /// against recency touches, so a deletion decided before a rename cannot unlink the
    /// renamed version. Two digests of different shards never wait for each other.
    /// Only background tasks take it, always before `index`, so I/O under it cannot stall
    /// an admission or a read.
    validation: Validation,
    ready: AtomicBool,
    /// Set by [`Store::begin_close`]: admissions refused, refused deletions not retried.
    closed: AtomicBool,
    /// Taken by the clean-state publication and by a cancelled close, so a cancellation
    /// lands either before the publication (which then never happens) or after it.
    close_decision: Mutex<CloseDecision>,
    next_tmp: AtomicU64,
    /// Background tasks (scan, deletions, touches, writers) still running; `close` waits
    /// for zero beyond itself before it may call the run clean.
    background: AtomicU64,
    /// A free-disk refresh task is running (single-flight).
    free_refreshing: AtomicBool,
    #[cfg(test)]
    hooks: Hooks,
    /// The `objects/` directory, kept open from start-up: the `syncfs` observation handle,
    /// so writeback errors since start-up are reported to `close`.
    #[cfg(target_os = "linux")]
    objects_dir: StdFile,
    /// Held for the life of the process: the `flock` on the data directory.
    _lock: StdFile,
}

/// An eviction decided under the index lock, to execute outside it.
#[derive(Debug)]
struct PendingDelete {
    digest: Digest,
    generation: u64,
    charged: u64,
    /// Exact object bytes, for the eviction metric.
    size: u64,
    path: PathBuf,
}

/// A validation lock on its own cache line: neighbouring shards do not share one.
#[derive(Debug, Default)]
#[repr(align(64))]
struct Shard(Mutex<()>);

/// The validation locks, sharded by the leading byte of the digest.
#[derive(Debug)]
struct Validation {
    shards: Box<[Shard]>,
}

impl Validation {
    fn new() -> Self {
        Self {
            shards: (0..VALIDATION_SHARDS).map(|_| Shard::default()).collect(),
        }
    }

    /// Takes the lock of `digest`'s shard. Callers take it before the index lock.
    fn lock(&self, digest: Digest) -> MutexGuard<'_, ()> {
        let shard = self
            .shards
            .get(usize::from(digest.0[0]))
            .expect("one shard per leading digest byte");
        shard.0.lock().expect("validation lock poisoned")
    }
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
    #[cfg(test)]
    inner.hooks.dispatches.fetch_add(1, Ordering::Relaxed);
    inner.background.fetch_add(1, Ordering::SeqCst);
    let tracked = Arc::clone(inner);
    spawn_background(move || {
        work();
        tracked.background.fetch_sub(1, Ordering::SeqCst);
    });
}

/// [`spawn_tracked`] for work whose result the caller awaits, with no channel in between.
async fn blocking_tracked<T: Send + 'static>(
    inner: &Arc<Inner>,
    work: impl FnOnce() -> T + Send + 'static,
) -> T {
    #[cfg(test)]
    inner.hooks.dispatches.fetch_add(1, Ordering::Relaxed);
    inner.background.fetch_add(1, Ordering::SeqCst);
    let tracked = Arc::clone(inner);
    blocking(move || {
        let result = work();
        tracked.background.fetch_sub(1, Ordering::SeqCst);
        result
    })
    .await
}

fn digest_of(key: &Key) -> Digest {
    Digest(key.digest())
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

fn free_disk_now(inner: &Inner) -> u64 {
    #[cfg(test)]
    if let Some(free) = *inner
        .hooks
        .free_disk
        .lock()
        .expect("free disk hook poisoned")
    {
        return free;
    }
    sample_free_disk(&inner.root)
}

/// Samples free disk space off the request path, so `admit` does no I/O. The mark is
/// read before the sample, so every reservation the sample may miss stays debited.
fn refresh_free_disk(inner: &Inner) {
    #[cfg(test)]
    while inner.hooks.hold_free_disk.load(Ordering::Acquire) {
        std::thread::sleep(Duration::from_millis(1));
    }
    let mark = inner
        .index
        .lock()
        .expect("index lock poisoned")
        .free_disk_mark();
    let free = free_disk_now(inner);
    inner
        .index
        .lock()
        .expect("index lock poisoned")
        .record_free_disk(free, mark);
}

/// Starts one background free-disk refresh if none is running, so a store refusing for
/// `min_free_disk` recovers as soon as space returns.
fn refresh_free_disk_async(inner: &Arc<Inner>) {
    if inner
        .free_refreshing
        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
        .is_ok()
    {
        let refresh = Arc::clone(inner);
        spawn_tracked(inner, move || {
            refresh_free_disk(&refresh);
            refresh.free_refreshing.store(false, Ordering::Release);
        });
    }
}

/// The high watermark a background sweep keeps `live` at or below: 95 % of
/// `max_size`, so admissions in a full store find room without waiting for unlinks.
fn watermark(max_size: u64) -> u64 {
    max_size.saturating_sub(max_size / 20)
}

/// Evicts down to the watermark, decided under the index lock, deletions spawned
/// outside it. Runs in writer tasks after a commit; `just_committed` is never a victim
/// of its own sweep (a single object may legitimately exceed the watermark).
fn sweep_to_watermark(inner: &Arc<Inner>, just_committed: Digest) {
    let evictions: Vec<PendingDelete> = {
        let mut index = inner.index.lock().expect("index lock poisoned");
        index.pin(just_committed);
        let over = index
            .live()
            .saturating_sub(watermark(inner.options.max_size));
        let removed = if over > 0 {
            index.evict_least_recent(over)
        } else {
            Vec::new()
        };
        index.unpin(just_committed);
        removed
            .into_iter()
            .map(|entry| {
                index.pending = index.pending.saturating_add(entry.charged);
                inner.options.metrics.eviction(entry.size);
                PendingDelete {
                    digest: entry.digest,
                    generation: entry.generation,
                    charged: entry.charged,
                    size: entry.size,
                    path: object_path(&inner.root, &entry.digest.0),
                }
            })
            .collect()
    };
    for pending in evictions {
        let delete_inner = Arc::clone(inner);
        spawn_tracked(inner, move || delete_pending(&delete_inner, &pending));
    }
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
        Ok(Self::start(inner))
    }

    /// Starts the scan that rebuilds the index in the background: reads are
    /// served from disk meanwhile, admissions answer `not_ready`.
    fn start(inner: Inner) -> Self {
        let inner = Arc::new(inner);
        let scan = Arc::clone(&inner);
        spawn_tracked(&inner, move || scan_blocking(&scan));
        Self { inner }
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
        let path = object_path(&self.inner.root, &digest.0);
        // Two attempts: an eviction (or a rewrite) between the lookup and the open answers
        // absent, or the newer version's bytes — never an error.
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
                    let generation = hit.generation;
                    spawn_tracked(&self.inner, move || {
                        touch(&inner, digest, &touch_path, when, generation);
                    });
                }
                #[cfg(test)]
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
    #[cfg(test)]
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
            // is not a hit; only reads refresh recency.
            let index = self.inner.index.lock().expect("index lock poisoned");
            return Ok(index
                .peek(digest)
                .map(|(size, modified)| Meta { size, modified }));
        }
        let path = object_path(&self.inner.root, &digest.0);
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
        if !self.is_ready() {
            return Admission::Dropped(DropReason::NotReady);
        }
        let digest = digest_of(key);
        let charged = charge(size);
        let mut evictions: Vec<PendingDelete> = Vec::new();
        let mut disk_refused = false;
        let admitted = {
            let mut index = inner.index.lock().expect("index lock poisoned");
            if inner.closed.load(Ordering::Acquire) {
                return Admission::Dropped(DropReason::NotReady);
            }
            if index.known_free_disk() < inner.options.min_free_disk.saturating_add(charged) {
                disk_refused = true;
                false
            } else {
                // Evict only for this reservation's own room in `live` — never to
                // cover deletions already in flight. Evictions are collected first, so
                // an admission that cannot fit in `live` evicts nothing.
                let over = index
                    .live()
                    .saturating_add(charged)
                    .saturating_sub(inner.options.max_size);
                index.pin(digest);
                let mut fits_live = true;
                if over > 0 {
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
                        fits_live = false;
                    } else {
                        for candidate in chosen {
                            if let Some(entry) = index.remove(candidate) {
                                index.pending = index.pending.saturating_add(entry.charged);
                                evictions.push(PendingDelete {
                                    digest: entry.digest,
                                    generation: entry.generation,
                                    charged: entry.charged,
                                    size: entry.size,
                                    path: object_path(&inner.root, &entry.digest.0),
                                });
                            }
                        }
                    }
                }
                // The hard bound: bytes being deleted are still on disk, so a
                // reservation that would push live + pending past `max_size` waits for
                // a later PUT — the room is coming, it is not here yet.
                let fits_hard =
                    index.charged_total().saturating_add(charged) <= inner.options.max_size;
                if fits_live && fits_hard {
                    index.reserve(charged);
                    true
                } else {
                    index.unpin(digest);
                    false
                }
            }
        };
        for pending in evictions {
            inner.options.metrics.eviction(pending.size);
            let delete_inner = Arc::clone(inner);
            spawn_tracked(inner, move || delete_pending(&delete_inner, &pending));
        }
        if disk_refused {
            refresh_free_disk_async(inner);
            return Admission::Dropped(DropReason::NoSpace);
        }
        if !admitted {
            return Admission::Dropped(DropReason::NoSpace);
        }
        Admission::Reserved(Reservation {
            inner: Arc::clone(inner),
            digest,
            destination: object_path(&inner.root, &digest.0),
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

    /// The first step of a shutdown, synchronous and without I/O: closes admissions and
    /// stops retrying refused deletions, so no background task outlives the shutdown
    /// deadline even when draining spends it all and [`Store::close`] never runs.
    pub(crate) fn begin_close(&self) {
        // Under the index lock, so no admission can slip between the check there and this
        // store: every reservation is either in `reserved` (and waited for) or refused.
        let _index = self.inner.index.lock().expect("index lock poisoned");
        self.inner.closed.store(true, Ordering::Release);
    }

    /// Flushes what shutdown must persist (late recency updates). Call once the server drained.
    ///
    /// Closes admissions, waits for every writer, background task and outstanding
    /// reservation, persists late recency, then — `purge-on-crash` only — runs a checked
    /// `syncfs` before writing `clean = true`. The caller bounds it with the shutdown
    /// deadline by dropping this future: a close dropped before it finished never writes
    /// `clean = true`, though its blocking half runs on until it notices.
    ///
    /// # Errors
    ///
    /// [`Error::Io`] when a flush fails. The state stays `clean = false`.
    pub(crate) async fn close(&self) -> Result<(), Error> {
        self.begin_close();
        let inner = Arc::clone(&self.inner);
        let mut abandon = AbandonCloseOnDrop(Some(Arc::clone(&inner)));
        let result = blocking(move || close_blocking(&inner)).await;
        abandon.0 = None;
        result
    }
}

/// Abandons the close unless disarmed: [`Store::close`] dropped before it finished was
/// cancelled or ran past the shutdown deadline, which forbids `clean = true`.
struct AbandonCloseOnDrop(Option<Arc<Inner>>);

impl Drop for AbandonCloseOnDrop {
    fn drop(&mut self) {
        if let Some(inner) = &self.0 {
            // Waits out a publication in progress (a rename and a directory sync), so
            // the outcome logged here is the one on disk.
            let mut decision = inner
                .close_decision
                .lock()
                .expect("close decision poisoned");
            if *decision == CloseDecision::Pending {
                *decision = CloseDecision::Abandoned;
                warn!("store close abandoned at the shutdown deadline; the run stays not clean");
            }
        }
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

/// Runs `attempt` (one unlink of `path`) until the file is gone, sleeping with backoff
/// between refusals: only a successful unlink or `ENOENT` ends it, so the caller releases
/// a charge only once its file is gone. A closing store admits nothing more and
/// stops retrying instead, so `close` never waits on a refusal that may not lift; the file
/// left behind is deleted (a `.tmp-*` file) or indexed again (an object) at the next start-up.
fn retry_unlink(inner: &Inner, path: &Path, mut attempt: impl FnMut() -> io::Result<()>) {
    let mut delay = Duration::from_millis(10);
    loop {
        match attempt() {
            Ok(()) => return,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return,
            Err(e) if inner.closed.load(Ordering::Acquire) => {
                warn!(path = %path.display(), error = %e, "cannot delete file; the store is closing, leaving it for the next start");
                return;
            }
            Err(e) => {
                warn!(path = %path.display(), error = %e, retry_in = ?delay, "cannot delete file; retrying");
                sleep_unless_closing(inner, delay);
                delay = (delay * 2).min(Duration::from_secs(5));
            }
        }
    }
}

/// Sleeps `delay`, in slices short enough that a store starting to close is noticed at
/// once: the runtime waits for blocking tasks at exit, so a backoff must not hold it.
fn sleep_unless_closing(inner: &Inner, delay: Duration) {
    const SLICE: Duration = Duration::from_millis(10);
    let mut left = delay;
    while !left.is_zero() && !inner.closed.load(Ordering::Acquire) {
        let nap = left.min(SLICE);
        std::thread::sleep(nap);
        left = left.saturating_sub(nap);
    }
}

fn unlink_with_retry(inner: &Inner, path: &Path) {
    retry_unlink(inner, path, || std::fs::remove_file(path));
}

/// Sets a file's `mtime` to `when` (the persisted recency), unless the version it
/// was decided for is gone. The generation is rechecked under the validation lock,
/// which also keeps a rename from landing between the check and the update. Only a
/// successful write counts as persisted; a failed one stays late for `close` to retry.
fn touch(inner: &Inner, digest: Digest, path: &Path, when: SystemTime, generation: u64) {
    #[cfg(test)]
    {
        inner.hooks.touch_reached.store(true, Ordering::Release);
        while inner.hooks.pause_touch.load(Ordering::Acquire) {
            std::thread::sleep(Duration::from_millis(1));
        }
    }
    let _validation = inner.validation.lock(digest);
    let applies = {
        let index = inner.index.lock().expect("index lock poisoned");
        index.touch_applies(digest, generation, when)
    };
    let persisted = applies
        && match StdFile::options()
            .write(true)
            .open(path)
            .and_then(|file| file.set_modified(when))
        {
            Ok(()) => true,
            Err(e) if e.kind() == io::ErrorKind::NotFound => false,
            Err(e) => {
                warn!(path = %path.display(), error = %e, "cannot persist recency");
                false
            }
        };
    inner
        .index
        .lock()
        .expect("index lock poisoned")
        .touch_settled(digest, generation, persisted.then_some(when));
}

/// Deletes one evicted object, unless a newer version was committed after the eviction
/// was decided. The validation lock makes the generation check and the unlink atomic
/// against a commit's rename. A failed unlink keeps the charge in `pending` and is
/// retried with backoff, the generation rechecked on every attempt; only a
/// successful unlink, `ENOENT`, or a superseding version releases the charge.
fn delete_pending(inner: &Arc<Inner>, pending: &PendingDelete) {
    #[cfg(test)]
    while inner.hooks.hold_deletions.load(Ordering::Acquire) {
        std::thread::sleep(Duration::from_millis(1));
    }
    retry_unlink(inner, &pending.path, || {
        let _validation = inner.validation.lock(pending.digest);
        let superseded = {
            let index = inner.index.lock().expect("index lock poisoned");
            matches!(index.generation(pending.digest), Some(g) if g != pending.generation)
        };
        // A newer version's rename already replaced the evicted file.
        if superseded {
            Ok(())
        } else {
            std::fs::remove_file(&pending.path)
        }
    });
    let mut index = inner.index.lock().expect("index lock poisoned");
    index.pending = index.pending.saturating_sub(pending.charged);
    drop(index);
    refresh_free_disk(inner);
}

/// The persisted run state: the durability mode of the run that wrote it, and
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

/// Reads `state`; absent, unreadable and malformed all read as `None` (each purges).
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
    let candidate = prepare_state(root, state)?;
    publish_state(root, &candidate)
}

/// Writes and syncs `state.tmp`, returning its path: the candidate [`publish_state`] renames.
fn prepare_state(root: &Path, state: State) -> Result<PathBuf, Error> {
    let temporary = root.join("state.tmp");
    let mut file = StdFile::create(&temporary).map_err(Error::io("create", &temporary))?;
    write!(
        file,
        "mode={}\nclean={}\n",
        mode_name(state.mode),
        state.clean
    )
    .map_err(Error::io("write", &temporary))?;
    file.sync_all().map_err(Error::io("fsync", &temporary))?;
    Ok(temporary)
}

fn publish_state(root: &Path, candidate: &Path) -> Result<(), Error> {
    let destination = root.join("state");
    std::fs::rename(candidate, &destination).map_err(Error::io("rename", &destination))?;
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
        let shard_name = shard
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or_default()
            .to_owned();
        let read = std::fs::read_dir(&shard).map_err(Error::io("read", &shard))?;
        for entry in read {
            let entry = entry.map_err(Error::io("read", &shard))?;
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            if parse_digest(name).is_some() && name.starts_with(&shard_name) {
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
        let is_dir = match entry.file_type() {
            Ok(kind) => kind.is_dir(),
            Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
            Err(e) => return Err(Error::io("stat", &path)(e)),
        };
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
    // Under the flock and before any mutation or served read. While a purge runs, the
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
    #[cfg(target_os = "linux")]
    let objects_dir = StdFile::open(&objects).map_err(Error::io("open", &objects))?;
    Ok(Inner {
        root,
        options,
        index: Mutex::new(Index::with_capacity(0)),
        validation: Validation::new(),
        ready: AtomicBool::new(false),
        closed: AtomicBool::new(false),
        close_decision: Mutex::new(CloseDecision::Pending),
        next_tmp: AtomicU64::new(0),
        background: AtomicU64::new(0),
        free_refreshing: AtomicBool::new(false),
        #[cfg(test)]
        hooks: Hooks::default(),
        #[cfg(target_os = "linux")]
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

/// Injected scan faults for tests: a panic or an I/O error in a shard worker.
#[cfg(test)]
#[expect(
    clippy::panic_in_result_fn,
    reason = "the panic is the injected fault under test"
)]
fn scan_test_hook(inner: &Inner, shard: &Path) -> Result<(), Error> {
    assert!(
        !inner.hooks.panic_scan.load(Ordering::Acquire),
        "injected scan panic"
    );
    if inner.hooks.fail_scan.load(Ordering::Acquire) {
        return Err(Error::io("read", shard)(io::Error::other(
            "injected scan failure",
        )));
    }
    Ok(())
}

fn scan_shard(shard: &Path, out: &mut Vec<Scanned>) -> Result<(), Error> {
    let shard_name = shard
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default()
        .to_owned();
    // Something gone since it was listed holds nothing to index: `ENOENT` means absent.
    let read = match std::fs::read_dir(shard) {
        Ok(read) => read,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(Error::io("read", shard)(e)),
    };
    for entry in read {
        let entry = entry.map_err(Error::io("read", shard))?;
        let path = entry.path();
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            warn!(path = %path.display(), "unexpected entry under objects/, ignored");
            continue;
        };
        if name.starts_with(TEMP_PREFIX) {
            // An upload a previous run never finished. Nothing writes during the scan:
            // admissions answer `not_ready` until it ends.
            match std::fs::remove_file(&path) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => return Err(Error::io("delete", &path)(e)),
            }
            continue;
        }
        let Some(digest) = parse_digest(name) else {
            warn!(path = %path.display(), "unexpected entry under objects/, ignored");
            continue;
        };
        if !name.starts_with(&shard_name) {
            warn!(path = %path.display(), "unexpected entry under objects/, ignored");
            continue;
        }
        let meta = match entry.metadata() {
            Ok(meta) => meta,
            Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
            Err(e) => return Err(Error::io("stat", &path)(e)),
        };
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

/// Collects every object under `objects/`: shards scanned in parallel with local
/// accumulation. An I/O error or a panicked scan thread is an error, never a partial
/// index: serving from half a tree would under-count the disk silently.
fn collect_scan(inner: &Inner) -> Result<Vec<Scanned>, Error> {
    let objects = inner.root.join("objects");
    let shards = shard_dirs(&objects)?;
    let workers = std::thread::available_parallelism()
        .map_or(1, usize::from)
        .clamp(1, 8);
    std::thread::scope(|scope| {
        let chunks: Vec<&[PathBuf]> = shards
            .chunks(shards.len().div_ceil(workers).max(1))
            .collect();
        let mut handles = Vec::new();
        for chunk in chunks {
            handles.push(scope.spawn(move || {
                let mut local = Vec::new();
                for shard in chunk {
                    #[cfg(test)]
                    scan_test_hook(inner, shard)?;
                    scan_shard(shard, &mut local)?;
                }
                Ok(local)
            }));
        }
        let mut all = Vec::new();
        for handle in handles {
            match handle.join() {
                Ok(Ok(local)) => all.extend(local),
                Ok(Err(e)) => return Err(e),
                Err(_) => {
                    return Err(Error::io("scan", &objects)(io::Error::other(
                        "a scan thread panicked",
                    )));
                }
            }
        }
        Ok(all)
    })
}

/// Rebuilds the index from `objects/`: one sort by `mtime`, the map built at
/// exact capacity. Objects above `max_size` (a shrunk configuration) are evicted,
/// oldest first, and deleted here before readiness — admissions answer `not_ready`
/// meanwhile, so no pending bytes outlive the scan.
fn scan_blocking(inner: &Arc<Inner>) {
    #[cfg(test)]
    while inner.hooks.hold_scan.load(Ordering::Acquire) {
        std::thread::sleep(Duration::from_millis(1));
    }
    let mut scanned = match collect_scan(inner) {
        Ok(scanned) => scanned,
        Err(e) => {
            // A store that cannot rebuild its index cannot bound its disk. Fail the
            // start loudly instead of serving half a tree as not ready forever.
            error!(error = %e, "start-up scan failed; aborting");
            // The log writer is not flushed by `exit`: say it on stderr directly too.
            let _ = writeln!(io::stderr().lock(), "start-up scan failed: {e}");
            std::process::exit(1);
        }
    };
    // Deterministic order for equal mtimes: by digest. Oldest first.
    scanned.sort_by(|a, b| {
        a.modified
            .cmp(&b.modified)
            .then(a.digest.0.cmp(&b.digest.0))
    });
    let mut index = Index::with_capacity(scanned.len());
    for object in &scanned {
        index.commit(
            object.digest,
            object.size,
            charge(object.size),
            object.modified,
        );
    }
    while index.size > inner.options.max_size {
        let over = index.size - inner.options.max_size;
        let removed = index.evict_least_recent(over);
        if removed.is_empty() {
            break;
        }
        for entry in removed {
            inner.options.metrics.eviction(entry.size);
            unlink_with_retry(inner, &object_path(&inner.root, &entry.digest.0));
        }
    }
    *inner.index.lock().expect("index lock poisoned") = index;
    refresh_free_disk(inner);
    inner.ready.store(true, Ordering::Release);
}

fn close_blocking(inner: &Arc<Inner>) -> Result<(), Error> {
    let result = close_steps(inner);
    #[cfg(test)]
    inner.hooks.close_done.store(true, Ordering::Release);
    result
}

fn close_steps(inner: &Arc<Inner>) -> Result<(), Error> {
    // Writers, background tasks and reservations not yet in `write` must all be done;
    // spawning none from here on (the late touches below run inline).
    while inner.background.load(Ordering::Acquire) > 0
        || inner.index.lock().expect("index lock poisoned").reserved > 0
    {
        abandoned(inner)?;
        std::thread::sleep(Duration::from_millis(1));
    }
    #[cfg(test)]
    close_point(inner, ClosePoint::Drained);
    let late = inner
        .index
        .lock()
        .expect("index lock poisoned")
        .late_touches();
    for (digest, when, generation) in late {
        touch(
            inner,
            digest,
            &object_path(&inner.root, &digest.0),
            when,
            generation,
        );
    }
    #[cfg(test)]
    close_point(inner, ClosePoint::Touched);
    if inner.options.durability == Durability::PurgeOnCrash {
        syncfs_checked(inner)?;
    }
    #[cfg(test)]
    close_point(inner, ClosePoint::Synced);
    let candidate = prepare_state(
        &inner.root,
        State {
            mode: inner.options.durability,
            clean: true,
        },
    )?;
    #[cfg(test)]
    close_point(inner, ClosePoint::Prepared);
    // The decision and the rename are one step against a cancellation: once abandoned,
    // nothing renames a clean state in, however late this thread gets here.
    let mut decision = inner
        .close_decision
        .lock()
        .expect("close decision poisoned");
    if *decision == CloseDecision::Abandoned {
        return Err(abandoned_error(inner));
    }
    publish_state(&inner.root, &candidate)?;
    *decision = CloseDecision::Published;
    Ok(())
}

/// Fails once [`Store::close`] was abandoned (cancelled, or past the shutdown deadline).
fn abandoned(inner: &Inner) -> Result<(), Error> {
    if *inner
        .close_decision
        .lock()
        .expect("close decision poisoned")
        == CloseDecision::Abandoned
    {
        return Err(abandoned_error(inner));
    }
    Ok(())
}

fn abandoned_error(inner: &Inner) -> Error {
    Error::io("close", &inner.root)(io::Error::new(
        io::ErrorKind::TimedOut,
        "close abandoned at the shutdown deadline; the run stays not clean",
    ))
}

/// `syncfs` on the objects' file system, success checked. Linux only: elsewhere a
/// `purge-on-crash` run can never be certified clean, so close fails and the next start
/// purges — the configuration already refuses the mode off Linux.
fn syncfs_checked(inner: &Inner) -> Result<(), Error> {
    let objects = inner.root.join("objects");
    #[cfg(test)]
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

/// One message to a write's blocking task.
enum WriteMsg {
    Chunk(Bytes),
    /// The body stream ended normally; the writer may commit. A channel that closes
    /// without this message means the write was abandoned (a dropped future, a failed
    /// body) and must commit nothing, even at exactly the announced size.
    Finish,
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

fn remove_temporary(inner: &Inner, path: &Path) {
    // Same rule as object deletions: the reservation is released only once the
    // temporary is actually gone.
    unlink_with_retry(inner, path);
}

impl Reservation {
    /// Reads `body` to its end, stores it under the reserved key and says how it went.
    ///
    /// The announced size must match the bytes received exactly. After an I/O error the rest of
    /// the body is read and discarded before returning, so the client's connection stays
    /// usable. The cause of an I/O failure is logged here at `error`. Dropping the future
    /// cancels the write.
    pub(crate) async fn write<S, E>(mut self, body: S) -> Outcome
    where
        S: Stream<Item = Result<Bytes, E>>,
    {
        if self.size <= SMALL_WRITE {
            return self.write_small(body).await;
        }
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
            let stored = matches!(result, WriteResult::Stored);
            drop(result_sender.send(result));
            // After the answer: the 201 does not wait for the sweep or a `statvfs`.
            if stored {
                sweep_to_watermark(&inner, digest);
                refresh_free_disk(&inner);
            }
        });
        let mut body = pin!(body);
        let mut writer_gone = false;
        let mut body_ended = true;
        while let Some(chunk) = body.next().await {
            let Ok(chunk) = chunk else {
                body_ended = false;
                break;
            };
            if sender.send(WriteMsg::Chunk(chunk)).await.is_err() {
                writer_gone = true;
                body_ended = false;
                break;
            }
            #[cfg(test)]
            self.inner
                .hooks
                .channel_sends
                .fetch_add(1, Ordering::Relaxed);
        }
        if body_ended {
            drop(sender.send(WriteMsg::Finish).await);
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

/// The path of a fresh temporary file next to `destination`.
fn temporary_path(inner: &Inner, destination: &Path) -> PathBuf {
    let id = inner.next_tmp.fetch_add(1, Ordering::Relaxed);
    destination.with_file_name(format!("{TEMP_PREFIX}{id}"))
}

/// Creates the temporary, and its shard directory the first time the shard is written.
fn create_temporary(temporary: &Path) -> io::Result<StdFile> {
    match StdFile::create(temporary) {
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            if let Some(shard) = temporary.parent() {
                std::fs::create_dir_all(shard)?;
            }
            StdFile::create(temporary)
        }
        created => created,
    }
}

/// Ends a write that stored nothing. The temporary's removal (retried until it lands) and
/// the reservation release run as a tracked background task: the charge stays held until
/// the file is gone, but a stuck unlink must not hold this write's outcome hostage.
fn abandon(
    inner: &Arc<Inner>,
    temporary: PathBuf,
    digest: Digest,
    charged: u64,
    result: WriteResult,
) -> WriteResult {
    let cleanup = Arc::clone(inner);
    spawn_tracked(inner, move || {
        remove_temporary(&cleanup, &temporary);
        release_reservation(&cleanup, digest, charged);
    });
    result
}

/// The write's one blocking task: owns the temporary file, writes through a
/// [`WRITE_BUFFER`] `BufWriter`, then validates in the same task.
fn writer_blocking(
    inner: &Arc<Inner>,
    mut receiver: mpsc::Receiver<WriteMsg>,
    digest: Digest,
    destination: &Path,
    size: u64,
    charged: u64,
) -> WriteResult {
    let temporary = temporary_path(inner, destination);
    let file = match create_temporary(&temporary) {
        Ok(file) => file,
        Err(e) => {
            release_reservation(inner, digest, charged);
            return WriteResult::Io(Error::io("create", &temporary)(e));
        }
    };
    let mut writer = BufWriter::with_capacity(WRITE_BUFFER, file);
    let mut bytes_seen = 0_u64;
    loop {
        match receiver.blocking_recv() {
            Some(WriteMsg::Chunk(chunk)) => {
                bytes_seen = bytes_seen.saturating_add(len_u64(&chunk));
                if bytes_seen > size {
                    return abandon(inner, temporary, digest, charged, WriteResult::Aborted);
                }
                if let Err(e) = writer.write_all(&chunk) {
                    let result = WriteResult::Io(Error::io("write", &temporary)(e));
                    return abandon(inner, temporary, digest, charged, result);
                }
            }
            Some(WriteMsg::Finish) => break,
            None => return abandon(inner, temporary, digest, charged, WriteResult::Aborted),
        }
    }
    if bytes_seen != size {
        return abandon(inner, temporary, digest, charged, WriteResult::Aborted);
    }
    match writer.into_inner() {
        Ok(file) => validate(inner, file, temporary, destination, digest, size, charged),
        Err(e) => {
            let result = WriteResult::Io(Error::io("write", &temporary)(e.into_error()));
            abandon(inner, temporary, digest, charged, result)
        }
    }
}

/// The whole write of a small object in one blocking pass: `data` is the complete
/// body, already checked against the announced size, so there is nothing left to wait for.
/// Runs the post-store sweep and free-disk sample here too, since the pass has no later
/// stage to defer them to.
fn write_small_blocking(
    inner: &Arc<Inner>,
    data: &[u8],
    digest: Digest,
    destination: &Path,
    size: u64,
    charged: u64,
) -> WriteResult {
    let temporary = temporary_path(inner, destination);
    let mut file = match create_temporary(&temporary) {
        Ok(file) => file,
        Err(e) => {
            release_reservation(inner, digest, charged);
            return WriteResult::Io(Error::io("create", &temporary)(e));
        }
    };
    if let Err(e) = file.write_all(data) {
        let result = WriteResult::Io(Error::io("write", &temporary)(e));
        return abandon(inner, temporary, digest, charged, result);
    }
    let result = validate(inner, file, temporary, destination, digest, size, charged);
    if matches!(result, WriteResult::Stored) {
        sweep_to_watermark(inner, digest);
        refresh_free_disk(inner);
    }
    result
}

/// Syncs the finished temporary (`fsync` mode), renames it into place and publishes
/// it: the part of a write both paths share.
fn validate(
    inner: &Arc<Inner>,
    file: StdFile,
    temporary: PathBuf,
    destination: &Path,
    digest: Digest,
    size: u64,
    charged: u64,
) -> WriteResult {
    if inner.options.durability == Durability::Fsync
        && let Err(e) = file.sync_all()
    {
        let result = WriteResult::Io(Error::io("fsync", &temporary)(e));
        return abandon(inner, temporary, digest, charged, result);
    }
    drop(file);
    {
        // One validation section covers the rename and its publication (index commit,
        // reservation conversion, unpin), in validation → index order: a pending
        // deletion or a competing writer of the same shard can only run before the rename
        // or after the entry exists, never between them.
        let _validation = inner.validation.lock(digest);
        if let Err(e) = std::fs::rename(&temporary, destination) {
            let result = WriteResult::Io(Error::io("rename", destination)(e));
            return abandon(inner, temporary, digest, charged, result);
        }
        // The rename already made the object visible: a failed stat must not lose it.
        let modified = std::fs::metadata(destination)
            .and_then(|meta| meta.modified())
            .unwrap_or_else(|_| SystemTime::now());
        #[cfg(test)]
        if inner.hooks.pause_publish.swap(false, Ordering::SeqCst) {
            inner.hooks.publish_reached.store(true, Ordering::Release);
            while !inner.hooks.publish_release.load(Ordering::Acquire) {
                std::thread::sleep(Duration::from_millis(1));
            }
            inner.hooks.publish_release.store(false, Ordering::Release);
        }
        let mut index = inner.index.lock().expect("index lock poisoned");
        index.commit(digest, size, charged, modified);
        index.reserved = index.reserved.saturating_sub(charged);
        index.unpin(digest);
    }
    inner.options.metrics.written_bytes(size);
    WriteResult::Stored
}

impl Reservation {
    /// [`Reservation::write`] for an object of at most [`SMALL_WRITE`] bytes: the body is
    /// read into memory first, bounded by the announced size, then one blocking pass stores it.
    /// Until that pass starts the reservation is unsettled, so a dropped future, a failed body
    /// or a wrong length cancel it through `Drop` and nothing was ever created.
    async fn write_small<S, E>(mut self, body: S) -> Outcome
    where
        S: Stream<Item = Result<Bytes, E>>,
    {
        let announced = usize::try_from(self.size).unwrap_or(usize::MAX);
        let mut data = Vec::with_capacity(announced);
        let mut body = pin!(body);
        while let Some(chunk) = body.next().await {
            let Ok(chunk) = chunk else {
                return Outcome::Aborted;
            };
            if chunk.len() > announced - data.len() {
                return Outcome::Aborted;
            }
            data.extend_from_slice(&chunk);
        }
        if data.len() != announced {
            return Outcome::Aborted;
        }
        // From here the blocking pass owns settling the reservation on every path,
        // including this future being dropped while it runs.
        self.settled = true;
        let inner = Arc::clone(&self.inner);
        let (digest, destination, size, charged) = (
            self.digest,
            self.destination.clone(),
            self.size,
            self.charged,
        );
        let pass = Arc::clone(&inner);
        let result = blocking_tracked(&inner, move || {
            write_small_blocking(&pass, &data, digest, &destination, size, charged)
        })
        .await;
        match result {
            WriteResult::Stored => Outcome::Stored,
            WriteResult::Aborted => Outcome::Aborted,
            WriteResult::Io(e) => {
                error!(error = %e, "PUT dropped after admission");
                Outcome::Dropped(DropReason::Io)
            }
        }
    }
}

impl Drop for Reservation {
    /// Cancels an unsettled reservation (one [`Reservation::write`] never took): no file
    /// exists yet, only the room and the pin are held. The release is handed to the
    /// blocking pool and the room stays charged until it runs, so the size bound holds
    /// meanwhile and the dropping thread never does the store's bookkeeping synchronously. Without
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

    fn is_temporary(path: &Path) -> bool {
        path.file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.starts_with(TEMP_PREFIX))
    }

    /// The only shard directory that holds a file being written.
    fn temporary_dir(dir: &TempDir) -> PathBuf {
        walk(&dir.path().join("objects"))
            .iter()
            .find(|path| is_temporary(path))
            .and_then(|path| path.parent())
            .expect("a temporary exists")
            .to_path_buf()
    }

    /// Files being written, which live next to their destination under `objects/`.
    fn temporaries(dir: &TempDir) -> usize {
        walk(&dir.path().join("objects"))
            .iter()
            .filter(|path| is_temporary(path))
            .count()
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
        assert!(object_path(dir.path(), &k.digest()).is_file());
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
    async fn admission_is_bounded_by_size_and_object_size() {
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
        let slow = digest_of(&key("slow"));
        // Stands for a rename that takes a long time: it owns the validation lock.
        let stuck = std::thread::spawn(move || {
            let _validation = inner.validation.lock(slow);
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
        let Admission::Reserved(reservation) = store.admit(&k, 6) else {
            panic!("not admitted")
        };
        // A file where the shard directory must be: the temporary cannot be created.
        let shard = object_path(dir.path(), &k.digest())
            .parent()
            .unwrap()
            .to_path_buf();
        std::fs::write(&shard, b"").unwrap();
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
        let store = Store::open(dir.path(), options(10 * BLOCK)).await.unwrap();
        assert_eq!(
            put_vec(&store, &key("a"), vec![1; 9000]).await,
            Outcome::Stored
        );
        assert_eq!(
            put_vec(&store, &key("b"), vec![2; 9000]).await,
            Outcome::Stored
        );
        // Reading `a` makes `b` the least recently used.
        assert!(contents(&store, &key("a")).await.is_some());
        assert_eq!(
            put_vec(&store, &key("c"), vec![3; 9000]).await,
            Outcome::Stored
        );
        // `d` first evicts `b` for its own room, then drops: the pending bytes still
        // occupy the disk. Once they are deleted the retry is stored.
        assert_eq!(
            put_vec(&store, &key("d"), vec![4; 9000]).await,
            Outcome::Dropped(DropReason::NoSpace)
        );
        eventually("the pending deletion to drain", || {
            store.inner.background.load(Ordering::Acquire) == 0
        })
        .await;
        assert_eq!(
            put_vec(&store, &key("d"), vec![4; 9000]).await,
            Outcome::Stored
        );
        assert!(contents(&store, &key("a")).await.is_some(), "a evicted");
        assert!(contents(&store, &key("b")).await.is_none(), "b not evicted");
        assert!(contents(&store, &key("d")).await.is_some(), "d missing");
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

    // After an unclean `purge-on-crash` run nothing is left: no object, and no
    // temporary an interrupted upload left in a shard.
    #[tokio::test]
    async fn crash_purge_start_leaves_no_object_and_no_temporary() {
        let dir = TempDir::new().unwrap();
        let mut opts = options(MIB);
        opts.durability = Durability::PurgeOnCrash;
        let store = Store::open(dir.path(), opts.clone()).await.unwrap();
        assert_eq!(put(&store, &key("k"), b"data").await, Outcome::Stored);
        let shard = object_path(dir.path(), &key("k").digest())
            .parent()
            .unwrap()
            .to_path_buf();
        std::fs::write(shard.join(".tmp-3"), b"interrupted upload").unwrap();
        drop(store); // No close(): an unclean purge-on-crash run.
        let reopened = open_retried(dir.path(), opts).await;
        ready(&reopened).await;
        assert_eq!(reopened.stats().entries, 0);
        assert_eq!(walk(&dir.path().join("objects")), Vec::<PathBuf>::new());
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

    /// Charged bytes of every file under `objects/`, temporaries included: what the size bound
    /// counts on disk.
    fn disk_charged(dir: &Path) -> u64 {
        let mut total = 0_u64;
        for path in walk(&dir.join("objects")) {
            if let Ok(meta) = std::fs::metadata(&path) {
                total = total.saturating_add(charge(meta.len()));
            }
        }
        total
    }

    fn object_files(dir: &Path) -> usize {
        walk(&dir.join("objects"))
            .iter()
            .filter(|path| !is_temporary(path))
            .count()
    }

    // The measured disk occupation never exceeds `max_size`.
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

    // A pending deletion of the old version must not remove the new one.
    #[tokio::test]
    async fn evicted_then_rewritten_key_survives_pending_delete() {
        let dir = TempDir::new().unwrap();
        let store = Store::open(dir.path(), options(10 * BLOCK)).await.unwrap();
        assert_eq!(
            put_vec(&store, &key("a"), vec![1; 100]).await,
            Outcome::Stored
        );
        for i in 0..8 {
            assert_eq!(
                put_vec(&store, &key(&format!("f{i}")), vec![2; 100]).await,
                Outcome::Stored
            );
        }
        store
            .inner
            .hooks
            .hold_deletions
            .store(true, Ordering::Release);
        // Evicts `a` (LRU) into a pending deletion, held by the hook.
        put_vec(&store, &key("z"), vec![3; 5000]).await;
        assert!(
            object_path(dir.path(), &key("a").digest()).is_file(),
            "pending deletion ran too early"
        );
        // Rewriting `a` now renames over the file the pending deletion targets.
        assert_eq!(
            put_vec(&store, &key("a"), vec![9; 100]).await,
            Outcome::Stored
        );
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
            contents(&store, &key("a")).await.unwrap(),
            vec![9; 100],
            "the pending deletion removed the rewritten version"
        );
    }

    // Eviction between lookup and open is absent or the new bytes, never Err.
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

    // One key, many writers, exactly one whole content wins.
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

    // A reader keeps its version across an eviction (POSIX open file).
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
        let big_path = object_path(dir.path(), &key("big").digest());
        // Fillers evict `big`; under the hard ceiling some drop `no_space` while its
        // deletion is pending, which is fine — the pressure is the point.
        for i in 0..16 {
            if !big_path.exists() {
                break;
            }
            put_vec(&store, &key(&format!("filler{i}")), vec![7; 128 * 1024]).await;
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

    // Recency survives a restart through `mtime`.
    #[tokio::test]
    async fn recency_persists_across_a_reopen() {
        let dir = TempDir::new().unwrap();
        let mut opts = options(10 * BLOCK);
        opts.touch_interval = Duration::ZERO;
        let store = Store::open(dir.path(), opts.clone()).await.unwrap();
        assert_eq!(
            put_vec(&store, &key("a"), vec![1; 9000]).await,
            Outcome::Stored
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(
            put_vec(&store, &key("b"), vec![2; 9000]).await,
            Outcome::Stored
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(contents(&store, &key("a")).await.is_some());
        store.close().await.unwrap();
        drop(store);
        let reopened = open_retried(dir.path(), opts).await;
        ready(&reopened).await;
        assert_eq!(
            put_vec(&reopened, &key("c"), vec![3; 9000]).await,
            Outcome::Stored
        );
        // Eviction pressure after the reopen must take `b` (older `mtime`) before `a`.
        put_vec(&reopened, &key("d"), vec![4; 9000]).await;
        eventually("the pending deletion to drain", || {
            reopened.inner.background.load(Ordering::Acquire) == 0
        })
        .await;
        assert!(
            contents(&reopened, &key("a")).await.is_some(),
            "touched a was evicted"
        );
        assert!(
            contents(&reopened, &key("b")).await.is_none(),
            "stale b was kept"
        );
    }

    // Crash leftovers in `tmp/` do not survive or get served.
    #[tokio::test]
    async fn crash_leftovers_do_not_survive_a_restart() {
        let dir = TempDir::new().unwrap();
        let store = Store::open(dir.path(), options(MIB)).await.unwrap();
        assert_eq!(put(&store, &key("keep"), b"data").await, Outcome::Stored);
        store.close().await.unwrap();
        drop(store);
        let shard = object_path(dir.path(), &key("keep").digest())
            .parent()
            .unwrap()
            .to_path_buf();
        std::fs::write(shard.join(".tmp-7"), b"truncated upload").unwrap();
        let reopened = open_retried(dir.path(), options(MIB)).await;
        ready(&reopened).await;
        assert_eq!(
            temporaries(&dir),
            0,
            "a leftover survived the start-up walk"
        );
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
        let reopened = open_scan_held(dir.path(), options(MIB)).await;
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
        release_scan(&reopened);
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
        let store = Store::open(dir.path(), options(20 * BLOCK)).await.unwrap();
        for i in 0..9 {
            assert_eq!(
                put_vec(&store, &key(&format!("p{i}")), vec![1; 5000]).await,
                Outcome::Stored
            );
        }
        // Reserving `p0` (the LRU) again pins its stored version; a further admission
        // needing room must evict `p1`, never the pinned `p0`.
        ready(&store).await;
        let Admission::Reserved(reservation) = store.admit(&key("p0"), 5000) else {
            panic!("not admitted")
        };
        assert!(matches!(
            store.admit(&key("q"), 5000),
            Admission::Dropped(DropReason::NoSpace)
        ));
        assert!(
            contents(&store, &key("p1")).await.is_none(),
            "the unpinned entry was not evicted"
        );
        assert_eq!(contents(&store, &key("p0")).await.unwrap(), vec![1; 5000]);
        let written = reservation
            .write(stream::iter([Ok::<_, Infallible>(Bytes::from(vec![
                9;
                5000
            ]))]))
            .await;
        assert_eq!(written, Outcome::Stored);
        assert_eq!(contents(&store, &key("p0")).await.unwrap(), vec![9; 5000]);
    }

    // Small objects are one `Bytes`; large ones are 64 KiB blocks.
    #[tokio::test]
    async fn read_stream_shapes_follow_object_size() {
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

    // A 1 GiB object streams in bounded blocks (sparse file on disk).
    #[tokio::test]
    async fn one_gib_object_streams_in_bounded_blocks() {
        const GIB: u64 = 1024 * 1024 * 1024;
        let dir = TempDir::new().unwrap();
        let store = Store::open(dir.path(), options(4 * GIB)).await.unwrap();
        store.close().await.unwrap();
        drop(store);
        let path = object_path(dir.path(), &key("huge").digest());
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

    // The purge decision table, state by crafted state.
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
        let path = object_path(dir, &key(name).digest());
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

    // Local, not CI: parallel 1 MiB PUT/GET throughput, to see the cost of the one index
    // mutex. Run in release: `cargo test --release bench_ -- --ignored --nocapture`.
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

    // Local, not CI: one-shot whole read vs 64 KiB streaming at 64/128/256 KiB.
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
            let path = object_path(dir.path(), &key(&name).digest());
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

    // Local, not CI: time to readiness over a prepared tree. Point
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

    /// [`open_retried`], with the start-up scan held until `hooks.hold_scan` is cleared.
    async fn open_scan_held(dir: &Path, opts: Options) -> Store {
        for _ in 0..200 {
            let (dir, opts) = (dir.to_path_buf(), opts.clone());
            match blocking(move || open_blocking(dir, opts)).await {
                Ok(inner) => {
                    inner.hooks.hold_scan.store(true, Ordering::Release);
                    return Store::start(inner);
                }
                Err(Error::Locked(_)) => tokio::time::sleep(Duration::from_millis(10)).await,
                Err(e) => panic!("open failed: {e}"),
            }
        }
        panic!("store stayed locked for 2s");
    }

    fn release_scan(store: &Store) {
        store.inner.hooks.hold_scan.store(false, Ordering::Release);
    }

    fn accounted_total(store: &Store) -> u64 {
        store.inner.index.lock().unwrap().charged_total()
    }

    // With deletions held, accounting and disk never exceed the ceiling, and the
    // PUT that would spend not-yet-freed bytes is dropped `no_space`.
    #[tokio::test]
    async fn ceiling_holds_with_pending_deletions_held() {
        let dir = TempDir::new().unwrap();
        let store = Store::open(dir.path(), options(2 * BLOCK)).await.unwrap();
        assert_eq!(put(&store, &key("a"), b"aaaa").await, Outcome::Stored);
        store
            .inner
            .hooks
            .hold_deletions
            .store(true, Ordering::Release);
        assert_eq!(put(&store, &key("b"), b"bbbb").await, Outcome::Stored);
        let outcome = put_vec(&store, &key("c"), b"cccc".to_vec()).await;
        let (total, disk) = (accounted_total(&store), disk_charged(dir.path()));
        store
            .inner
            .hooks
            .hold_deletions
            .store(false, Ordering::Release);
        assert_eq!(
            outcome,
            Outcome::Dropped(DropReason::NoSpace),
            "C spent pending bytes"
        );
        assert!(total <= 2 * BLOCK, "accounted {total} above the ceiling");
        assert!(disk <= 2 * BLOCK, "disk {disk} above the ceiling");
    }

    // Under sustained load the accounted total, sampled at every admission, stays
    // under the ceiling, and the index equals the disk once drained.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn sustained_load_never_exceeds_the_ceiling() {
        let dir = TempDir::new().unwrap();
        let store = Store::open(dir.path(), options(64 * BLOCK)).await.unwrap();
        ready(&store).await;
        let peak = Arc::new(AtomicU64::new(0));
        let mut jobs = Vec::new();
        for t in 0..64_usize {
            let (store, peak) = (store.clone(), Arc::clone(&peak));
            jobs.push(tokio::spawn(async move {
                for i in 0..32 {
                    let name = key(&format!("sustain/{t}/{i}"));
                    let data = vec![1_u8; (t * 37 % 12 + 1) * 1024];
                    let size = u64::try_from(data.len()).unwrap();
                    let admission = store.admit(&name, size);
                    peak.fetch_max(accounted_total(&store), Ordering::SeqCst);
                    if let Admission::Reserved(reservation) = admission {
                        let input = stream::iter([Ok::<_, Infallible>(Bytes::from(data))]);
                        reservation.write(input).await;
                    }
                    tokio::task::yield_now().await;
                }
            }));
        }
        for job in jobs {
            job.await.unwrap();
        }
        eventually("background work to drain", || {
            store.inner.background.load(Ordering::Acquire) == 0
        })
        .await;
        assert!(
            peak.load(Ordering::SeqCst) <= 64 * BLOCK,
            "accounting peaked at {} above {}",
            peak.load(Ordering::SeqCst),
            64 * BLOCK
        );
        assert_eq!(accounted_total(&store), disk_charged(dir.path()));
    }

    // After a restart with a reduced `max_size`, one small PUT must not evict the
    // live entries to cover deletions already in flight.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn reduced_max_size_put_does_not_evict_live_for_pending() {
        let dir = TempDir::new().unwrap();
        let store = Store::open(dir.path(), options(20 * BLOCK)).await.unwrap();
        for i in 0..10 {
            assert_eq!(
                put_vec(&store, &key(&format!("k{i}")), vec![1; 100]).await,
                Outcome::Stored
            );
            tokio::time::sleep(Duration::from_millis(3)).await;
        }
        store.close().await.unwrap();
        drop(store);
        let reopened = open_scan_held(dir.path(), options(6 * BLOCK)).await;
        reopened
            .inner
            .hooks
            .hold_deletions
            .store(true, Ordering::Release);
        release_scan(&reopened);
        ready(&reopened).await;
        assert_eq!(reopened.stats().entries, 6);
        let outcome = put_vec(&reopened, &key("new"), vec![2; 100]).await;
        let entries = reopened.stats().entries;
        reopened
            .inner
            .hooks
            .hold_deletions
            .store(false, Ordering::Release);
        eventually("deletions to drain", || {
            reopened.inner.background.load(Ordering::Acquire) == 0
        })
        .await;
        assert_eq!(outcome, Outcome::Dropped(DropReason::NoSpace));
        assert!(
            entries >= 5,
            "one small PUT left {entries} of 6 live entries"
        );
        assert_eq!(
            put_vec(&reopened, &key("new"), vec![2; 100]).await,
            Outcome::Stored
        );
    }

    // An admission evicts only for its own room in live bytes. With an earlier
    // eviction still pending, a PUT that fits live but not live + pending is dropped
    // without evicting anything.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn admission_never_evicts_to_cover_pending_deletions() {
        let dir = TempDir::new().unwrap();
        let store = Store::open(dir.path(), options(10 * BLOCK)).await.unwrap();
        for i in 0..8 {
            assert_eq!(
                put_vec(&store, &key(&format!("k{i}")), vec![1; 100]).await,
                Outcome::Stored
            );
        }
        store
            .inner
            .hooks
            .hold_deletions
            .store(true, Ordering::Release);
        // Live reaches 10 blocks; the 95 % sweep then evicts one into a held deletion.
        assert_eq!(
            put_vec(&store, &key("two"), vec![2; 5000]).await,
            Outcome::Stored
        );
        eventually("the sweep's deletion to be pending", || {
            store.inner.index.lock().unwrap().pending == BLOCK
        })
        .await;
        let entries = store.stats().entries;
        let outcome = put_vec(&store, &key("new"), vec![3; 100]).await;
        let after = store.stats().entries;
        store
            .inner
            .hooks
            .hold_deletions
            .store(false, Ordering::Release);
        assert_eq!(outcome, Outcome::Dropped(DropReason::NoSpace));
        assert_eq!(
            after, entries,
            "a live entry was evicted to cover a pending one"
        );
    }

    // A deletion decided before a rewrite must not unlink the rewritten file while its
    // writer sits between `rename` and the index commit.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn pending_delete_cannot_remove_rewrite_in_publish_window() {
        let dir = TempDir::new().unwrap();
        let store = Store::open(dir.path(), options(10 * BLOCK)).await.unwrap();
        assert_eq!(
            put_vec(&store, &key("a"), vec![1; 100]).await,
            Outcome::Stored
        );
        for i in 0..8 {
            assert_eq!(
                put_vec(&store, &key(&format!("f{i}")), vec![2; 100]).await,
                Outcome::Stored
            );
        }
        store
            .inner
            .hooks
            .hold_deletions
            .store(true, Ordering::Release);
        // Evicts `a` (and possibly a filler) into pending, held by the hook.
        put_vec(&store, &key("z"), vec![3; 5000]).await;
        store
            .inner
            .hooks
            .pause_publish
            .store(true, Ordering::Release);
        let writer = {
            let store = store.clone();
            tokio::spawn(async move { put_vec(&store, &key("a"), vec![9; 100]).await })
        };
        eventually("the writer to reach publication", || {
            store.inner.hooks.publish_reached.load(Ordering::Acquire)
        })
        .await;
        store
            .inner
            .hooks
            .hold_deletions
            .store(false, Ordering::Release);
        tokio::time::sleep(Duration::from_millis(100)).await;
        store
            .inner
            .hooks
            .publish_release
            .store(true, Ordering::Release);
        assert_eq!(writer.await.unwrap(), Outcome::Stored);
        eventually("deletions to drain", || {
            store.inner.background.load(Ordering::Acquire) == 0
        })
        .await;
        assert_eq!(
            contents(&store, &key("a")).await.unwrap(),
            vec![9; 100],
            "a pending deletion removed the rewritten version (stat: {:?})",
            store.stat(&key("a")).await.unwrap()
        );
    }

    // Two writers of one key: the index must describe the version the renames left on
    // disk, so HEAD (`stat`) and GET agree.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn rename_order_matches_index_publication() {
        let dir = TempDir::new().unwrap();
        let store = Store::open(dir.path(), options(MIB)).await.unwrap();
        ready(&store).await;
        store
            .inner
            .hooks
            .pause_publish
            .store(true, Ordering::Release);
        let first = {
            let store = store.clone();
            tokio::spawn(async move { put_vec(&store, &key("k"), vec![1; 4]).await })
        };
        eventually("the first writer to reach publication", || {
            store.inner.hooks.publish_reached.load(Ordering::Acquire)
        })
        .await;
        let second = {
            let store = store.clone();
            tokio::spawn(async move { put_vec(&store, &key("k"), vec![2; 8192]).await })
        };
        tokio::time::sleep(Duration::from_millis(100)).await;
        store
            .inner
            .hooks
            .publish_release
            .store(true, Ordering::Release);
        assert_eq!(first.await.unwrap(), Outcome::Stored);
        assert_eq!(second.await.unwrap(), Outcome::Stored);
        let body = contents(&store, &key("k")).await.unwrap();
        let meta = store.stat(&key("k")).await.unwrap().unwrap();
        assert_eq!(
            meta.size,
            u64::try_from(body.len()).unwrap(),
            "stat and GET disagree: the index describes another version"
        );
        assert!(body == vec![1; 4] || body == vec![2; 8192], "mixed content");
    }

    // A temporary whose unlink is refused keeps its reservation until the retried
    // unlink lands, the same rule as for evicted objects.
    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn refused_temporary_unlink_keeps_its_reservation() {
        use std::os::unix::fs::PermissionsExt;
        let dir = TempDir::new().unwrap();
        let store = Store::open(dir.path(), options(32 * BLOCK)).await.unwrap();
        ready(&store).await;
        // Above the one-pass threshold: only the streaming path has a temporary while the
        // body is still being read.
        let size = SMALL_WRITE + 1;
        let Admission::Reserved(reservation) = store.admit(&key("t"), size) else {
            panic!("not admitted")
        };
        let (sender, receiver) = mpsc::channel::<Result<Bytes, Infallible>>(1);
        let input = stream::unfold(receiver, |mut receiver| async move {
            receiver.recv().await.map(|chunk| (chunk, receiver))
        });
        let writing = tokio::spawn(reservation.write(input));
        sender.send(Ok(Bytes::from_static(b"short"))).await.unwrap();
        eventually("the temporary to exist", || temporaries(&dir) == 1).await;
        let tmp = temporary_dir(&dir);
        std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o500)).unwrap();
        // The body ends short of the announced size: the write aborts.
        drop(sender);
        let outcome = writing.await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        let (held, present) = (accounted_total(&store), temporaries(&dir));
        std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o700)).unwrap();
        assert_eq!(outcome, Outcome::Aborted);
        assert_eq!(present, 1, "the refused unlink was never exercised");
        assert_eq!(
            held,
            charge(size),
            "the reservation was released while its temporary remained"
        );
        eventually("the retried unlink to release the reservation", || {
            temporaries(&dir) == 0 && accounted_total(&store) == 0
        })
        .await;
    }

    // Same with a real file-system refusal: while the shard is read-only the failed
    // unlink keeps its charge (accounted == present); once writable again the retry
    // lands and releases it. Sol's probe, with the refusal phase bounded.
    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn unlink_failure_on_disk_retains_charge_until_it_lands() {
        use std::os::unix::fs::PermissionsExt;
        let dir = TempDir::new().unwrap();
        let store = Store::open(dir.path(), options(2 * BLOCK)).await.unwrap();
        assert_eq!(put(&store, &key("a"), b"aaaa").await, Outcome::Stored);
        let shard = object_path(dir.path(), &key("a").digest())
            .parent()
            .unwrap()
            .to_path_buf();
        std::fs::set_permissions(&shard, std::fs::Permissions::from_mode(0o500)).unwrap();
        // Storing `b` sweeps `a` out; its unlink is refused and retries in background.
        assert_eq!(put(&store, &key("b"), b"bbbb").await, Outcome::Stored);
        put_vec(&store, &key("c"), b"cccc".to_vec()).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        let (total, disk) = (accounted_total(&store), disk_charged(dir.path()));
        let refused_file_present = object_path(dir.path(), &key("a").digest()).exists();
        std::fs::set_permissions(&shard, std::fs::Permissions::from_mode(0o700)).unwrap();
        assert!(
            refused_file_present,
            "the refused unlink was never exercised"
        );
        assert_eq!(total, disk, "a refused unlink vanished from the accounting");
        assert_eq!(total, 2 * BLOCK, "the refused charge was not retained");
        eventually("the retried unlink to land", || {
            accounted_total(&store) == disk_charged(dir.path())
                && store.inner.background.load(Ordering::Acquire) == 0
        })
        .await;
    }

    // Cancelling `write` after exactly the announced bytes still stores nothing:
    // only an explicit end-of-body may commit.
    #[tokio::test]
    async fn cancel_after_exact_bytes_does_not_commit() {
        let dir = TempDir::new().unwrap();
        let store = Store::open(dir.path(), options(MIB)).await.unwrap();
        ready(&store).await;
        let Admission::Reserved(reservation) = store.admit(&key("k"), 4) else {
            panic!("not admitted")
        };
        let input = stream::iter([Ok::<_, Infallible>(Bytes::from_static(b"data"))])
            .chain(stream::pending());
        let write = reservation.write(input);
        assert!(
            tokio::time::timeout(Duration::from_millis(200), write)
                .await
                .is_err()
        );
        eventually("the cancelled write to clean up", || {
            store.inner.background.load(Ordering::Acquire) == 0
        })
        .await;
        assert!(
            store.read(&key("k")).await.unwrap().is_none(),
            "a cancelled write committed"
        );
        assert_eq!(temporaries(&dir), 0);
    }

    // A recency touch decided for an old version must not age its replacement.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn old_touch_does_not_age_replacement() {
        let dir = TempDir::new().unwrap();
        let mut opts = options(10 * BLOCK);
        opts.touch_interval = Duration::ZERO;
        let store = Store::open(dir.path(), opts).await.unwrap();
        assert_eq!(
            put_vec(&store, &key("a"), vec![1; 9000]).await,
            Outcome::Stored
        );
        store.inner.hooks.pause_touch.store(true, Ordering::Release);
        assert!(contents(&store, &key("a")).await.is_some());
        eventually("the old touch to queue", || {
            store.inner.hooks.touch_reached.load(Ordering::Acquire)
        })
        .await;
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(
            put_vec(&store, &key("b"), vec![2; 9000]).await,
            Outcome::Stored
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(
            put_vec(&store, &key("a"), vec![3; 9000]).await,
            Outcome::Stored
        );
        let a_path = object_path(dir.path(), &key("a").digest());
        let fresh = std::fs::metadata(&a_path).unwrap().modified().unwrap();
        store
            .inner
            .hooks
            .pause_touch
            .store(false, Ordering::Release);
        eventually("the touch to drain", || {
            store.inner.background.load(Ordering::Acquire) == 0
        })
        .await;
        let after = std::fs::metadata(&a_path).unwrap().modified().unwrap();
        assert_eq!(
            after, fresh,
            "a stale touch rewound the replacement's mtime"
        );
    }

    // Slow readers must not park the shared blocking pool.
    #[test]
    fn large_reads_do_not_starve_the_blocking_pool() {
        use std::pin::Pin;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .max_blocking_threads(4)
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            type ChunkStream = Pin<Box<dyn Stream<Item = io::Result<Bytes>> + Send>>;
            let dir = TempDir::new().unwrap();
            let store = Store::open(dir.path(), options(16 * MIB)).await.unwrap();
            for i in 0..5 {
                let data = vec![7_u8; usize::try_from(MIB).unwrap()];
                assert_eq!(
                    put_vec(&store, &key(&format!("big{i}")), data).await,
                    Outcome::Stored
                );
            }
            let mut streams: Vec<ChunkStream> = Vec::new();
            for i in 0..4 {
                let object = store.read(&key(&format!("big{i}"))).await.unwrap().unwrap();
                let mut stream: ChunkStream = Box::pin(object.into_stream());
                assert!(stream.next().await.is_some(), "first block missing");
                streams.push(stream);
            }
            let fifth =
                tokio::time::timeout(Duration::from_millis(500), store.read(&key("big4"))).await;
            assert!(
                matches!(fifth, Ok(Ok(Some(_)))),
                "a fifth open starved behind four unconsumed streams"
            );
            drop(streams);
        });
    }

    // Once the disk recovers, `min_free_disk` refusals stop without a restart.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn min_free_disk_recovers_after_space_is_freed() {
        const MIB128: u64 = 128 * 1024 * 1024;
        let dir = TempDir::new().unwrap();
        let mut opts = options(1024 * MIB);
        let min_free_disk = 2 * MIB128;
        opts.min_free_disk = min_free_disk;
        let store = Store::open(dir.path(), opts).await.unwrap();
        ready(&store).await;
        // The host's own free space never enters the test: every sample reads this.
        let set_free = |free: u64| {
            *store.inner.hooks.free_disk.lock().unwrap() = Some(free);
        };
        set_free(4 * MIB128);
        refresh_free_disk(&store.inner);
        assert_eq!(
            put_vec(&store, &key("x"), vec![1; 10]).await,
            Outcome::Stored
        );
        set_free(MIB128);
        // Admitted on the stale sample; its commit re-samples after answering.
        put_vec(&store, &key("y"), vec![1; 10]).await;
        eventually("the sample to see the lost space", || {
            store.inner.index.lock().unwrap().known_free_disk() < min_free_disk
        })
        .await;
        assert_eq!(
            put_vec(&store, &key("z"), vec![1; 10]).await,
            Outcome::Dropped(DropReason::NoSpace)
        );
        set_free(4 * MIB128);
        let mut recovered = false;
        for _ in 0..100 {
            if put_vec(&store, &key("w"), vec![1; 10]).await == Outcome::Stored {
                recovered = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(
            recovered,
            "the store stayed in no_space after the disk recovered"
        );
    }

    // `close` waits for a reservation that has not started writing yet.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn close_waits_for_an_outstanding_reservation() {
        let dir = TempDir::new().unwrap();
        let store = Store::open(dir.path(), options(MIB)).await.unwrap();
        ready(&store).await;
        let Admission::Reserved(reservation) = store.admit(&key("late"), 4) else {
            panic!("not admitted")
        };
        let closing = {
            let store = store.clone();
            tokio::spawn(async move { store.close().await })
        };
        tokio::time::sleep(Duration::from_millis(100)).await;
        let finished_early = closing.is_finished();
        let state = std::fs::read_to_string(dir.path().join("state")).unwrap();
        assert_eq!(reservation.write(body(&[b"late"])).await, Outcome::Stored);
        closing.await.unwrap().unwrap();
        assert!(
            !finished_early,
            "close completed behind an open reservation"
        );
        assert!(
            state.contains("clean=false"),
            "clean was written early: {state}"
        );
        let state = std::fs::read_to_string(dir.path().join("state")).unwrap();
        assert!(
            state.contains("clean=true"),
            "close did not finish clean: {state}"
        );
    }

    // A closing store stops retrying a refused unlink, so `close` finishes; the file
    // left behind is a complete object, indexed again at the next start.
    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn close_gives_up_a_refused_unlink() {
        use std::os::unix::fs::PermissionsExt;
        let dir = TempDir::new().unwrap();
        let store = Store::open(dir.path(), options(2 * BLOCK)).await.unwrap();
        assert_eq!(put(&store, &key("a"), b"aaaa").await, Outcome::Stored);
        let a_path = object_path(dir.path(), &key("a").digest());
        let shard = a_path.parent().unwrap().to_path_buf();
        std::fs::set_permissions(&shard, std::fs::Permissions::from_mode(0o500)).unwrap();
        // Storing `b` sweeps `a` out; its unlink is refused and retried.
        assert_eq!(put(&store, &key("b"), b"bbbb").await, Outcome::Stored);
        eventually("the eviction of `a`", || {
            store
                .inner
                .index
                .lock()
                .unwrap()
                .peek(digest_of(&key("a")))
                .is_none()
        })
        .await;
        let closed = tokio::time::timeout(Duration::from_secs(2), store.close()).await;
        std::fs::set_permissions(&shard, std::fs::Permissions::from_mode(0o700)).unwrap();
        assert!(
            matches!(closed, Ok(Ok(()))),
            "close waited on a refused unlink: {closed:?}"
        );
        assert!(a_path.exists(), "the refused unlink was never exercised");
        drop(store);
        let reopened = open_retried(dir.path(), options(2 * BLOCK)).await;
        ready(&reopened).await;
        assert_eq!(
            contents(&reopened, &key("a")).await.as_deref(),
            Some(&b"aaaa"[..])
        );
    }

    // Until a sample covers it, every admitted reservation is debited from the known
    // free space, so a stale sample never admits below `min_free_disk`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn stale_free_sample_never_admits_below_min_free_disk() {
        const SIXTEEN: u64 = 16 * MIB;
        let dir = TempDir::new().unwrap();
        let mut opts = options(1024 * MIB);
        opts.min_free_disk = 1024 * MIB;
        let store = Store::open(dir.path(), opts).await.unwrap();
        ready(&store).await;
        // Freeze the sample at room for one 16 MiB object, not two.
        store
            .inner
            .hooks
            .hold_free_disk
            .store(true, Ordering::Release);
        {
            let mut index = store.inner.index.lock().unwrap();
            let mark = index.free_disk_mark();
            index.record_free_disk(1024 * MIB + 24 * MIB, mark);
        }
        let data = vec![1_u8; usize::try_from(SIXTEEN).unwrap()];
        assert_eq!(put_vec(&store, &key("first"), data).await, Outcome::Stored);
        let second = store.admit(&key("second"), SIXTEEN);
        store
            .inner
            .hooks
            .hold_free_disk
            .store(false, Ordering::Release);
        assert!(
            matches!(second, Admission::Dropped(DropReason::NoSpace)),
            "admitted below min_free_disk on a stale sample: {second:?}"
        );
    }

    // A refused deletion retrying with backoff does not hold the runtime at exit once the
    // store began closing, even if `close` never runs (the drain spent the deadline).
    #[cfg(unix)]
    #[test]
    fn begin_close_bounds_exit_with_a_refused_unlink() {
        use std::os::unix::fs::PermissionsExt;
        use std::time::Instant;
        let dir = TempDir::new().unwrap();
        let shard = object_path(dir.path(), &key("a").digest())
            .parent()
            .unwrap()
            .to_path_buf();
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let store = Store::open(dir.path(), options(2 * BLOCK)).await.unwrap();
            assert_eq!(put(&store, &key("a"), b"aaaa").await, Outcome::Stored);
            std::fs::set_permissions(&shard, std::fs::Permissions::from_mode(0o500)).unwrap();
            // Storing `b` sweeps `a` out; its unlink is refused, and the backoff grows.
            assert_eq!(put(&store, &key("b"), b"bbbb").await, Outcome::Stored);
            tokio::time::sleep(Duration::from_secs(1)).await;
            assert_eq!(
                store.inner.index.lock().unwrap().pending,
                BLOCK,
                "the refused unlink was never exercised"
            );
            store.begin_close();
        });
        // Lets a failing run end instead of hanging: the refusal lifts after 1.5 s.
        let restore = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(1500));
            std::fs::set_permissions(&shard, std::fs::Permissions::from_mode(0o700)).unwrap();
        });
        let started = Instant::now();
        drop(runtime);
        let exit = started.elapsed();
        restore.join().unwrap();
        assert!(
            exit < Duration::from_millis(200),
            "runtime exit waited {exit:?} on a refused unlink"
        );
    }

    // A close cancelled at any point of its path never publishes `clean = true`,
    // even once its blocking half reaches the publication.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn cancelled_close_never_publishes_clean() {
        for point in [
            ClosePoint::Drained,
            ClosePoint::Touched,
            ClosePoint::Synced,
            ClosePoint::Prepared,
        ] {
            let dir = TempDir::new().unwrap();
            let store = Store::open(dir.path(), options(MIB)).await.unwrap();
            assert_eq!(put(&store, &key("k"), b"data").await, Outcome::Stored);
            *store.inner.hooks.pause_close_at.lock().unwrap() = Some(point);
            let closing = {
                let store = store.clone();
                tokio::spawn(async move { store.close().await })
            };
            eventually("close to reach the point", || {
                store.inner.hooks.close_reached.load(Ordering::Acquire)
            })
            .await;
            closing.abort();
            assert!(closing.await.is_err(), "close finished at {point:?}");
            store
                .inner
                .hooks
                .close_release
                .store(true, Ordering::Release);
            eventually("the blocking close to return", || {
                store.inner.hooks.close_done.load(Ordering::Acquire)
            })
            .await;
            let state = std::fs::read_to_string(dir.path().join("state")).unwrap();
            assert!(
                state.contains("clean=false"),
                "a close cancelled at {point:?} published: {state}"
            );
        }
    }

    // A touch whose `mtime` write fails is not persisted; `close` writes it.
    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn failed_touch_is_retried_by_close() {
        use std::os::unix::fs::PermissionsExt;
        let dir = TempDir::new().unwrap();
        let mut opts = options(MIB);
        opts.touch_interval = Duration::ZERO;
        let store = Store::open(dir.path(), opts).await.unwrap();
        assert_eq!(put(&store, &key("a"), b"aaaa").await, Outcome::Stored);
        let path = object_path(dir.path(), &key("a").digest());
        let old = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000_000);
        StdFile::options()
            .write(true)
            .open(&path)
            .unwrap()
            .set_modified(old)
            .unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o444)).unwrap();
        assert!(contents(&store, &key("a")).await.is_some());
        eventually("the refused touch", || {
            store.inner.background.load(Ordering::Acquire) == 0
        })
        .await;
        let refused = std::fs::metadata(&path).unwrap().modified().unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        store.close().await.unwrap();
        let closed = std::fs::metadata(&path).unwrap().modified().unwrap();
        assert_eq!(refused, old, "the touch was never refused");
        assert!(closed > old, "close did not retry the failed touch");
    }

    // A close dropped at the shutdown deadline never writes `clean = true`, even
    // once what it was waiting for completes.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn abandoned_close_never_writes_clean() {
        let dir = TempDir::new().unwrap();
        let store = Store::open(dir.path(), options(MIB)).await.unwrap();
        ready(&store).await;
        let Admission::Reserved(reservation) = store.admit(&key("late"), 4) else {
            panic!("not admitted")
        };
        let closing = tokio::time::timeout(Duration::from_millis(50), store.close()).await;
        assert!(
            closing.is_err(),
            "close finished behind an open reservation"
        );
        assert_eq!(reservation.write(body(&[b"late"])).await, Outcome::Stored);
        eventually("background work to finish", || {
            store.inner.background.load(Ordering::Acquire) == 0
        })
        .await;
        tokio::time::sleep(Duration::from_millis(200)).await;
        let state = std::fs::read_to_string(dir.path().join("state")).unwrap();
        assert!(
            state.contains("clean=false"),
            "an abandoned close wrote clean: {state}"
        );
    }

    // A scan error or a panicked scan thread propagates as an error (which
    // `scan_blocking` turns into a loud exit), never a partial index.
    #[tokio::test]
    async fn scan_failures_propagate() {
        let dir = TempDir::new().unwrap();
        let store = Store::open(dir.path(), options(MIB)).await.unwrap();
        assert_eq!(put(&store, &key("k"), b"data").await, Outcome::Stored);
        store.inner.hooks.fail_scan.store(true, Ordering::Release);
        assert!(
            collect_scan(&store.inner).is_err(),
            "a scan error was swallowed"
        );
        store.inner.hooks.fail_scan.store(false, Ordering::Release);
        store.inner.hooks.panic_scan.store(true, Ordering::Release);
        assert!(
            collect_scan(&store.inner).is_err(),
            "a scan panic was swallowed"
        );
        store.inner.hooks.panic_scan.store(false, Ordering::Release);
        assert!(collect_scan(&store.inner).is_ok());
    }

    // A shard removed between its listing and its read holds nothing: the scan goes on
    // instead of failing the start.
    #[test]
    fn scan_skips_a_vanished_shard() {
        let dir = TempDir::new().unwrap();
        let mut found = Vec::new();
        assert!(scan_shard(&dir.path().join("objects").join("ab"), &mut found).is_ok());
        assert!(found.is_empty());
    }

    // A purge removes only files inside the naming scheme, shard included.
    #[tokio::test]
    async fn purge_keeps_digest_files_in_the_wrong_shard() {
        let dir = TempDir::new().unwrap();
        let correct = craft_object(dir.path(), "k1");
        let name = object_path(dir.path(), &key("k2").digest())
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .to_owned();
        let wrong_shard = if name.starts_with("00") { "ff" } else { "00" };
        let wrong_dir = dir.path().join("objects").join(wrong_shard);
        std::fs::create_dir_all(&wrong_dir).unwrap();
        let wrong = wrong_dir.join(&name);
        std::fs::write(&wrong, b"off scheme").unwrap();
        std::fs::write(
            dir.path().join("state"),
            "mode=purge-on-crash\nclean=false\n",
        )
        .unwrap();
        let store = Store::open(dir.path(), options(MIB)).await.unwrap();
        assert!(!correct.exists(), "the in-scheme object survived the purge");
        assert!(
            wrong.exists(),
            "the purge deleted outside the naming scheme"
        );
        drop(store);
    }

    /// Waits until every background task, writer passes included, has ended.
    async fn quiesce(store: &Store) {
        eventually("background tasks to end", || {
            store.inner.background.load(Ordering::Acquire) == 0
        })
        .await;
    }

    // An object of at most 64 KiB is stored by one blocking pass with no channel; a
    // larger one keeps the streaming path.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn small_put_is_one_blocking_pass() {
        let dir = TempDir::new().unwrap();
        let store = Store::open(dir.path(), options(MIB)).await.unwrap();
        ready(&store).await;
        quiesce(&store).await;
        let hooks = &store.inner.hooks;
        let (dispatched, sent) = (&hooks.dispatches, &hooks.channel_sends);
        let before = dispatched.load(Ordering::Relaxed);
        let exact = usize::try_from(SMALL_WRITE).unwrap();
        for (name, len) in [("tiny", 4), ("exact", exact)] {
            assert_eq!(
                put_vec(&store, &key(name), vec![7; len]).await,
                Outcome::Stored
            );
            quiesce(&store).await;
            assert_eq!(contents(&store, &key(name)).await.unwrap(), vec![7; len]);
        }
        assert_eq!(
            dispatched.load(Ordering::Relaxed) - before,
            2,
            "a small PUT is not one blocking pass"
        );
        assert_eq!(
            sent.load(Ordering::Relaxed),
            0,
            "a small body used a channel"
        );

        assert_eq!(
            put_vec(&store, &key("large"), vec![7; exact + 1]).await,
            Outcome::Stored
        );
        assert!(
            sent.load(Ordering::Relaxed) > 0,
            "a body above 64 KiB left the streaming path"
        );
    }

    /// Two keys whose digests start with different bytes, and two that share one.
    fn keys_by_shard() -> ((Key, Key), (Key, Key)) {
        let keys: Vec<Key> = (0..8192).map(|i| key(&format!("shard/{i}"))).collect();
        let shard = |k: &Key| k.digest()[0];
        let apart = keys
            .iter()
            .find(|k| shard(k) != shard(&keys[0]))
            .expect("8192 digests span several shards");
        let same = keys[1..]
            .iter()
            .find(|k| shard(k) == shard(&keys[0]))
            .expect("8192 digests collide on 256 shards");
        (
            (keys[0].clone(), apart.clone()),
            (keys[0].clone(), same.clone()),
        )
    }

    /// Starts a PUT of `data` under `name` and returns once it holds its validation lock,
    /// paused before the index commit.
    async fn put_paused_in_validation(
        store: &Store,
        name: &Key,
    ) -> tokio::task::JoinHandle<Outcome> {
        store
            .inner
            .hooks
            .pause_publish
            .store(true, Ordering::Release);
        let writer = {
            let (store, name) = (store.clone(), name.clone());
            tokio::spawn(async move { put_vec(&store, &name, vec![1; 100]).await })
        };
        eventually("the writer to reach publication", || {
            store.inner.hooks.publish_reached.load(Ordering::Acquire)
        })
        .await;
        writer
    }

    // A validation held in its critical section does not delay another shard's.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn validations_of_different_shards_run_in_parallel() {
        let dir = TempDir::new().unwrap();
        let store = Store::open(dir.path(), options(MIB)).await.unwrap();
        ready(&store).await;
        let ((held, other), _) = keys_by_shard();
        let first = put_paused_in_validation(&store, &held).await;
        let second = {
            let (store, other) = (store.clone(), other.clone());
            tokio::spawn(async move { put_vec(&store, &other, vec![2; 100]).await })
        };
        let finished = tokio::time::timeout(Duration::from_secs(2), second).await;
        store
            .inner
            .hooks
            .publish_release
            .store(true, Ordering::Release);
        assert_eq!(
            finished
                .expect("a validation waited for another shard's")
                .unwrap(),
            Outcome::Stored
        );
        assert_eq!(first.await.unwrap(), Outcome::Stored);
    }

    // Two validations of one shard still exclude each other.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn validations_of_one_shard_exclude_each_other() {
        let dir = TempDir::new().unwrap();
        let store = Store::open(dir.path(), options(MIB)).await.unwrap();
        ready(&store).await;
        let (_, (held, same)) = keys_by_shard();
        let first = put_paused_in_validation(&store, &held).await;
        let second = {
            let (store, same) = (store.clone(), same.clone());
            tokio::spawn(async move { put_vec(&store, &same, vec![2; 100]).await })
        };
        tokio::time::sleep(Duration::from_millis(200)).await;
        let overtook = second.is_finished();
        store
            .inner
            .hooks
            .publish_release
            .store(true, Ordering::Release);
        assert!(
            !overtook,
            "a validation ran inside another's critical section"
        );
        assert_eq!(first.await.unwrap(), Outcome::Stored);
        assert_eq!(second.await.unwrap(), Outcome::Stored);
    }

    // A small PUT that fails after its temporary exists removes it and gives the room back.
    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn small_put_io_failure_cleans_up() {
        let dir = TempDir::new().unwrap();
        let store = Store::open(dir.path(), options(MIB)).await.unwrap();
        ready(&store).await;
        let k = key("blocked");
        // A directory where the object must go makes the rename fail.
        std::fs::create_dir_all(object_path(dir.path(), &k.digest())).unwrap();
        assert_eq!(
            put_vec(&store, &k, vec![1; 100]).await,
            Outcome::Dropped(DropReason::Io)
        );
        eventually("the temporary and the room to be released", || {
            temporaries(&dir) == 0 && accounted_total(&store) == 0
        })
        .await;
        assert!(store.read(&k).await.unwrap().is_none());
    }

    // A small body longer than announced stores nothing and creates no file.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn small_body_over_its_announced_size_stores_nothing() {
        let dir = TempDir::new().unwrap();
        let store = Store::open(dir.path(), options(MIB)).await.unwrap();
        ready(&store).await;
        let k = key("long");
        let Admission::Reserved(reservation) = store.admit(&k, 4) else {
            panic!("not admitted")
        };
        let outcome = reservation.write(body(&[b"abc", b"def"])).await;
        assert_eq!(outcome, Outcome::Aborted);
        eventually("the room to be released", || accounted_total(&store) == 0).await;
        assert_eq!(temporaries(&dir), 0);
        assert!(store.read(&k).await.unwrap().is_none());
    }
}

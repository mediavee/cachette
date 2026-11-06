//! The in-memory index: a digest map plus an intrusive LRU list (D15, D35).
//!
//! Every operation is O(1). The map key is the blake3 digest itself with a trivial hasher,
//! because a digest is already uniform; no key text or path is copied into an entry.

use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hash, Hasher};
use std::time::{Duration, SystemTime};

/// The blake3 digest of a [`Key`](crate::key::Key), and the index's map key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Digest(pub(super) [u8; 32]);

impl Hash for Digest {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // The first eight digest bytes are as good a hash as any function of them.
        let mut prefix = [0_u8; 8];
        prefix.copy_from_slice(&self.0[..8]);
        state.write_u64(u64::from_le_bytes(prefix));
    }
}

/// A hasher that keeps the value [`Digest`] hands it, unchanged.
#[derive(Debug, Default)]
struct TrivialHasher(u64);

impl Hasher for TrivialHasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        // Only reached if a future key type hashes a slice; fold the first eight bytes.
        let mut prefix = [0_u8; 8];
        let len = bytes.len().min(8);
        if let Some(slot) = prefix.get_mut(..len) {
            slot.copy_from_slice(&bytes[..len]);
        }
        self.0 = u64::from_le_bytes(prefix);
    }

    fn write_u64(&mut self, value: u64) {
        self.0 = value;
    }
}

type DigestMap<V> = HashMap<Digest, V, BuildHasherDefault<TrivialHasher>>;

const NONE: u32 = u32::MAX;

/// One stored object, as the index knows it. About 100 bytes; there is one per entry.
#[derive(Debug, Clone)]
pub(super) struct Entry {
    pub(super) digest: Digest,
    /// Exact object size in bytes.
    pub(super) size: u64,
    /// Bytes charged against `max_size`: `size` rounded up to a 4 KiB block.
    pub(super) charged: u64,
    /// Commit order, from a store-wide counter. A pending deletion carries the generation it
    /// was decided against and must not remove an entry with a different one.
    pub(super) generation: u64,
    /// The file's `mtime`, which is also the persisted recency.
    pub(super) modified: SystemTime,
    /// Last hit in memory; ahead of `modified` until the next persisted touch.
    pub(super) last_access: SystemTime,
    /// Last recency written back to `mtime`.
    pub(super) last_persisted: SystemTime,
    newer: u32,
    older: u32,
}

/// What a hit decided: whether the `mtime` must be rewritten outside the lock, and for
/// which version.
#[derive(Debug, Clone, Copy)]
pub(super) struct Hit {
    /// The version hit; a recency touch carries it and is dropped if it changed.
    pub(super) generation: u64,
    pub(super) persist_recency: Option<SystemTime>,
}

/// The index behind the store's one `Mutex`.
///
/// A mutex rather than an actor (D15): every critical section is a handful of O(1) map and
/// list operations, so a channel round-trip per request would cost more than the work.
#[derive(Debug)]
pub(super) struct Index {
    map: DigestMap<u32>,
    slots: Vec<Option<Entry>>,
    free: Vec<u32>,
    /// Most recently used slot.
    head: u32,
    /// Least recently used slot.
    tail: u32,
    /// Charged bytes of stored objects.
    pub(super) size: u64,
    /// Charged bytes reserved by PUTs in flight.
    pub(super) reserved: u64,
    /// Charged bytes evicted but not yet deleted.
    pub(super) pending: u64,
    /// Keys with a PUT in flight, by digest, with their reservation count. Never evictable.
    pins: DigestMap<u32>,
    next_generation: u64,
}

impl Index {
    pub(super) fn with_capacity(capacity: usize) -> Self {
        Self {
            map: DigestMap::with_capacity_and_hasher(capacity, BuildHasherDefault::default()),
            slots: Vec::with_capacity(capacity),
            free: Vec::new(),
            head: NONE,
            tail: NONE,
            size: 0,
            reserved: 0,
            pending: 0,
            pins: DigestMap::default(),
            next_generation: 1,
        }
    }

    pub(super) fn entries(&self) -> u64 {
        u64::try_from(self.map.len()).unwrap_or(u64::MAX)
    }

    /// `vivant` in D12: committed plus reserved bytes.
    pub(super) fn live(&self) -> u64 {
        self.size.saturating_add(self.reserved)
    }

    /// Every byte the D12 hard bound counts: `vivant` plus bytes being deleted.
    pub(super) fn charged_total(&self) -> u64 {
        self.live().saturating_add(self.pending)
    }

    pub(super) fn generation(&self, digest: Digest) -> Option<u64> {
        self.entry(digest).map(|entry| entry.generation)
    }

    /// Size and `mtime` without touching recency, for `HEAD`.
    pub(super) fn peek(&self, digest: Digest) -> Option<(u64, SystemTime)> {
        self.entry(digest).map(|entry| (entry.size, entry.modified))
    }

    fn entry(&self, digest: Digest) -> Option<&Entry> {
        let slot = *self.map.get(&digest)?;
        self.slot(slot)
    }

    fn slot(&self, slot: u32) -> Option<&Entry> {
        self.slots.get(usize::try_from(slot).ok()?)?.as_ref()
    }

    fn slot_mut(&mut self, slot: u32) -> Option<&mut Entry> {
        self.slots.get_mut(usize::try_from(slot).ok()?)?.as_mut()
    }

    /// Records a hit: the entry becomes most recently used, and its recency is to be
    /// persisted when the last persisted touch is at least `touch_interval` old.
    pub(super) fn hit(
        &mut self,
        digest: Digest,
        now: SystemTime,
        touch_interval: Duration,
    ) -> Option<Hit> {
        let slot = *self.map.get(&digest)?;
        self.move_to_head(slot);
        let entry = self.slot_mut(slot)?;
        entry.last_access = now;
        let due = now
            .duration_since(entry.last_persisted)
            .is_ok_and(|age| age >= touch_interval);
        let persist_recency = if due {
            entry.last_persisted = now;
            entry.modified = now;
            Some(now)
        } else {
            None
        };
        Some(Hit {
            generation: entry.generation,
            persist_recency,
        })
    }

    /// Inserts or replaces a committed object at the most-recently-used end and returns the
    /// generation assigned to it. `size` accounting is adjusted here; the caller releases the
    /// reservation and the pin.
    pub(super) fn commit(
        &mut self,
        digest: Digest,
        size: u64,
        charged: u64,
        modified: SystemTime,
    ) -> u64 {
        let generation = self.next_generation;
        self.next_generation = self.next_generation.saturating_add(1);
        if let Some(&slot) = self.map.get(&digest) {
            self.move_to_head(slot);
            let old_charged = self.slot(slot).map_or(0, |entry| entry.charged);
            self.size = self
                .size
                .saturating_sub(old_charged)
                .saturating_add(charged);
            if let Some(entry) = self.slot_mut(slot) {
                entry.size = size;
                entry.charged = charged;
                entry.generation = generation;
                entry.modified = modified;
                entry.last_access = modified;
                entry.last_persisted = modified;
            }
            return generation;
        }
        let entry = Entry {
            digest,
            size,
            charged,
            generation,
            modified,
            last_access: modified,
            last_persisted: modified,
            newer: NONE,
            older: NONE,
        };
        let slot = self.alloc(entry);
        self.map.insert(digest, slot);
        self.link_head(slot);
        self.size = self.size.saturating_add(charged);
        generation
    }

    /// Removes an entry, returning it so the caller can count and delete it outside the lock.
    pub(super) fn remove(&mut self, digest: Digest) -> Option<Entry> {
        let slot = self.map.remove(&digest)?;
        self.unlink(slot);
        let entry = self.slots.get_mut(usize::try_from(slot).ok()?)?.take()?;
        self.free.push(slot);
        self.size = self.size.saturating_sub(entry.charged);
        Some(entry)
    }

    /// The least recently used evictable digests, oldest first, collecting entries until
    /// their charged bytes reach `want` (or the evictable entries run out). Pinned keys are
    /// skipped. Collecting first, rather than evicting as we go, lets an admission that
    /// cannot fit evict nothing at all.
    pub(super) fn eviction_candidates(&self, want: u64) -> Vec<Digest> {
        let mut candidates = Vec::new();
        let mut freed = 0_u64;
        let mut slot = self.tail;
        while slot != NONE && freed < want {
            let Some(entry) = self.slot(slot) else { break };
            let (digest, charged, newer) = (entry.digest, entry.charged, entry.newer);
            if !self.is_pinned(digest) {
                candidates.push(digest);
                freed = freed.saturating_add(charged);
            }
            slot = newer;
        }
        candidates
    }

    pub(super) fn pin(&mut self, digest: Digest) {
        *self.pins.entry(digest).or_default() += 1;
    }

    pub(super) fn unpin(&mut self, digest: Digest) {
        if let Some(count) = self.pins.get_mut(&digest) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                self.pins.remove(&digest);
            }
        }
    }

    fn is_pinned(&self, digest: Digest) -> bool {
        self.pins.contains_key(&digest)
    }

    /// Age base of the least recently used entry, for the `oldest_entry_age` gauge.
    pub(super) fn oldest_access(&self) -> Option<SystemTime> {
        self.slot(self.tail).map(|entry| entry.last_access)
    }

    /// Every entry whose in-memory recency was never persisted, marked persisted. The caller
    /// writes the `mtime`s outside the lock; used at a clean shutdown (D14).
    pub(super) fn take_late_touches(&mut self) -> Vec<(Digest, SystemTime, u64)> {
        let mut late = Vec::new();
        for entry in self.slots.iter_mut().flatten() {
            if entry.last_access > entry.last_persisted {
                entry.last_persisted = entry.last_access;
                entry.modified = entry.last_access;
                late.push((entry.digest, entry.last_access, entry.generation));
            }
        }
        late
    }

    /// Whether a recency touch decided for `generation` at `when` still applies: the
    /// version is unchanged and no newer touch was persisted since.
    pub(super) fn touch_applies(&self, digest: Digest, generation: u64, when: SystemTime) -> bool {
        self.entry(digest)
            .is_some_and(|entry| entry.generation == generation && entry.last_persisted <= when)
    }

    /// Removes least-recently-used evictable entries freeing at least `want` charged
    /// bytes (or every evictable entry). The caller charges them to `pending`.
    pub(super) fn evict_least_recent(&mut self, want: u64) -> Vec<Entry> {
        self.eviction_candidates(want)
            .into_iter()
            .filter_map(|digest| self.remove(digest))
            .collect()
    }

    fn alloc(&mut self, entry: Entry) -> u32 {
        if let Some(slot) = self.free.pop() {
            if let Some(cell) = self
                .slots
                .get_mut(usize::try_from(slot).unwrap_or(usize::MAX))
            {
                *cell = Some(entry);
            }
            return slot;
        }
        let slot = u32::try_from(self.slots.len()).expect("fewer than u32::MAX entries");
        self.slots.push(Some(entry));
        slot
    }

    fn link_head(&mut self, slot: u32) {
        let old_head = self.head;
        if let Some(entry) = self.slot_mut(slot) {
            entry.newer = NONE;
            entry.older = old_head;
        }
        if let Some(head) = self.slot_mut(old_head) {
            head.newer = slot;
        }
        self.head = slot;
        if self.tail == NONE {
            self.tail = slot;
        }
    }

    fn unlink(&mut self, slot: u32) {
        let Some((newer, older)) = self.slot(slot).map(|entry| (entry.newer, entry.older)) else {
            return;
        };
        if let Some(entry) = self.slot_mut(newer) {
            entry.older = older;
        }
        if let Some(entry) = self.slot_mut(older) {
            entry.newer = newer;
        }
        if self.head == slot {
            self.head = older;
        }
        if self.tail == slot {
            self.tail = newer;
        }
        if let Some(entry) = self.slot_mut(slot) {
            entry.newer = NONE;
            entry.older = NONE;
        }
    }

    fn move_to_head(&mut self, slot: u32) {
        if self.head == slot {
            return;
        }
        self.unlink(slot);
        self.link_head(slot);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn digest(byte: u8) -> Digest {
        Digest([byte; 32])
    }

    fn at(secs: u64) -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_secs(secs)
    }

    #[test]
    fn lru_order_and_candidates() {
        let mut index = Index::with_capacity(4);
        index.commit(digest(1), 10, 4096, at(1));
        index.commit(digest(2), 10, 4096, at(2));
        index.commit(digest(3), 10, 4096, at(3));
        assert_eq!(index.eviction_candidates(4096), [digest(1)]);
        // A hit on the oldest makes the second the candidate, and a pin skips an entry.
        assert!(index.hit(digest(1), at(4), Duration::ZERO).is_some());
        assert_eq!(index.eviction_candidates(4096), [digest(2)]);
        index.pin(digest(2));
        assert_eq!(index.eviction_candidates(4096), [digest(3)]);
        index.unpin(digest(2));
        assert_eq!(index.eviction_candidates(8192), [digest(2), digest(3)]);
    }

    #[test]
    fn replacing_counts_once_and_bumps_generation() {
        let mut index = Index::with_capacity(2);
        let first = index.commit(digest(1), 10, 4096, at(1));
        let second = index.commit(digest(1), 5000, 8192, at(2));
        assert!(second > first, "generation did not grow");
        assert_eq!((index.entries(), index.size), (1, 8192));
        let removed = index.remove(digest(1)).expect("entry present");
        assert_eq!(removed.generation, second);
        assert_eq!((index.entries(), index.size), (0, 0));
        assert!(index.remove(digest(1)).is_none());
    }

    // A touch decided for a replaced version never applies to the new one, even with a
    // later date: a version committed from a file written before the hit is newer.
    #[test]
    fn touch_of_a_replaced_version_does_not_apply() {
        let mut index = Index::with_capacity(1);
        let old = index.commit(digest(1), 10, 4096, at(100));
        let hit = index.hit(digest(1), at(300), Duration::ZERO).expect("hit");
        assert_eq!((hit.generation, hit.persist_recency), (old, Some(at(300))));
        let new = index.commit(digest(1), 10, 4096, at(200));
        assert!(index.touch_applies(digest(1), new, at(300)));
        assert!(!index.touch_applies(digest(1), old, at(300)));
    }

    #[test]
    fn touch_interval_gates_persisted_recency() {
        let mut index = Index::with_capacity(1);
        index.commit(digest(1), 10, 4096, at(100));
        let hit = index
            .hit(digest(1), at(200), Duration::from_secs(300))
            .expect("hit");
        assert_eq!(hit.persist_recency, None);
        let hit = index
            .hit(digest(1), at(500), Duration::from_secs(300))
            .expect("hit");
        assert_eq!(hit.persist_recency, Some(at(500)));
        // A late hit inside the interval is flushed by take_late_touches, once.
        assert!(
            index
                .hit(digest(1), at(600), Duration::from_secs(300))
                .is_some()
        );
        assert_eq!(index.take_late_touches(), [(digest(1), at(600), 1)]);
        assert_eq!(
            index.take_late_touches(),
            Vec::<(Digest, SystemTime, u64)>::new()
        );
    }
}

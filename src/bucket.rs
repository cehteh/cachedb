#![allow(clippy::type_complexity)]
use std::collections::{hash_map::DefaultHasher, HashSet};
use std::hash::{Hash, Hasher};
use std::pin::Pin;
use std::sync::atomic::{AtomicU32, AtomicU8, AtomicUsize, Ordering};

use intrusive_collections::LinkedList;
#[allow(unused_imports)]
pub use log::{debug, error, info, trace, warn};
use parking_lot::{Mutex, MutexGuard};

use crate::entry::EntryAdapter;
use crate::Entry;
use crate::KeyTraits;
use crate::UnsafeRef;

/// The internal representation of a Bucket.
///
/// The LRU eviction is per bucket, this is most efficient and catches the corner cases where
/// one bucket sees more entries than others.
///
/// The eviction caclculation adapts itself and works as follows:
///
/// The maximum number of elements in use (locked) at any time is tracked. This maximum number
/// becomes decremented after 'max_cooldown' operations to catch the case when requirements
/// drop over time. The number of elements in the LRU list is known as well. These two values
/// are used to determine how percent the cached part makes up. Then the 'low_water' and
/// 'high_water' settings come in. Below 'low_water' the cache fills up without evicting any
/// entries, above 'low_water' entries from the head of the LRU list are slowly purged (one
/// per every inserts_per_evict). Above 'high_water' entries become more aggressively purged
/// ('evicts_per_insert').
///
/// The actual used 'low_water' and 'high_water' are derived from the '*_max' and '*_min'
/// settings by linear interpolation from 0 to 'entries_limit'. Thus allowing a high cache
/// ratio when memory requirements are modest and reduce the memory used for caching at higher
/// loads.
pub(crate) struct Bucket<K, V>
where
    K: KeyTraits,
{
    map:      Mutex<HashSet<Pin<Box<Entry<K, V>>>>>,
    lru_list: Mutex<LinkedList<EntryAdapter<K, V>>>,

    // Stats section
    cold:    AtomicUsize,
    maxused: AtomicUsize,

    // State section
    maxused_countdown:  AtomicU32,
    high_water:         AtomicU8,
    low_water:          AtomicU8,
    lowwater_countdown: AtomicU8,

    // Configuration
    pub(crate) maxused_cooldown: AtomicU32,
    pub(crate) entries_limit:    AtomicUsize,

    pub(crate) high_water_max:    AtomicU8,
    pub(crate) high_water_min:    AtomicU8,
    pub(crate) evicts_per_insert: AtomicU8,

    pub(crate) low_water_max:     AtomicU8,
    pub(crate) low_water_min:     AtomicU8,
    pub(crate) inserts_per_evict: AtomicU8,
}

impl<K, V> Bucket<K, V>
where
    K: KeyTraits,
{
    pub(crate) fn new() -> Self {
        Self {
            map:                Mutex::new(HashSet::new()),
            lru_list:           Mutex::new(LinkedList::new(EntryAdapter::new())),
            cold:               AtomicUsize::new(0),
            maxused:            AtomicUsize::new(0),
            maxused_countdown:  AtomicU32::new(0),
            high_water:         AtomicU8::new(60),
            low_water:          AtomicU8::new(30),
            lowwater_countdown: AtomicU8::new(2),
            maxused_cooldown:   AtomicU32::new(1024),
            entries_limit:      AtomicUsize::new(10000000),
            high_water_max:     AtomicU8::new(60),
            high_water_min:     AtomicU8::new(10),
            evicts_per_insert:  AtomicU8::new(2),
            low_water_max:      AtomicU8::new(30),
            low_water_min:      AtomicU8::new(5),
            inserts_per_evict:  AtomicU8::new(2),
        }
    }

    pub(crate) fn lock_map(&self) -> MutexGuard<HashSet<Pin<Box<Entry<K, V>>>>> {
        self.map.lock()
    }

    pub(crate) fn use_entry(
        &self,
        entry: &Entry<K, V>,
        map_lock: &MutexGuard<HashSet<Pin<Box<Entry<K, V>>>>>,
    ) {
        let mut lru_lock = self.lru_list.lock();
        if entry.lru_link.is_linked() {
            unsafe { lru_lock.cursor_mut_from_ptr(&*entry).remove() };
            self.cold.fetch_sub(1, Ordering::Relaxed);
            self.update_maxused(map_lock);
        }
        entry.use_count.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn unuse_entry(&self, entry: &Entry<K, V>) {
        let mut lru_lock = self.lru_list.lock();
        if entry.use_count.fetch_sub(1, Ordering::Relaxed) == 0 {
            self.cold.fetch_add(1, Ordering::Relaxed);
            lru_lock.push_back(unsafe { UnsafeRef::from_raw(entry) });
        }
    }

    /// takes the len from the locked map
    pub(crate) fn update_maxused(&self, map_lock: &MutexGuard<HashSet<Pin<Box<Entry<K, V>>>>>) {
        // since we got the map locked we can be sloppy with atomics

        // update maxused
        self.maxused.fetch_max(
            map_lock.len() - self.cold.load(Ordering::Relaxed),
            Ordering::Relaxed,
        );

        // maxused_countdown handling
        let countdown = self.maxused_countdown.load(Ordering::Relaxed);
        if countdown > 0 {
            // just keep counting down
            self.maxused_countdown
                .store(countdown - 1, Ordering::Relaxed)
        } else {
            // Do some work, reset it to cooldown period, decrement maxused
            self.maxused_countdown.store(
                self.maxused_cooldown.load(Ordering::Relaxed),
                Ordering::Relaxed,
            );
            let mut maxused = self.maxused.load(Ordering::Relaxed);
            if maxused > 0 {
                maxused -= 1;
                self.maxused.store(maxused, Ordering::Relaxed);
            }

            // TODO: recalculate low/highwater
        }
    }
}

/// Defines into which bucket a key falls. The default implementation uses the Hash trait for
/// this. Custom implementations can override this to something more simple. It is recommended
/// to implement this because very good distribution of the resulting value is not as
/// important as for the hashmap.
pub trait Bucketize: Hash {
    // Must return an value 0..N-1 otherwise CacheDb will panic with array access out of bounds.
    fn bucket<const N: usize>(&self) -> usize {
        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish() as usize % N
    }
}

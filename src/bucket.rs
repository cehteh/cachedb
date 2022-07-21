#![allow(clippy::type_complexity)]
use std::collections::{hash_map::DefaultHasher, HashSet};
use std::hash::{Hash, Hasher};
use std::pin::Pin;
use std::sync::atomic::{AtomicU32, AtomicU8, AtomicUsize, Ordering};
use std::fmt::{self, Debug, Formatter};
use std::mem::ManuallyDrop;

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
/// The eviction caclculation adapts itself based on the current capacity of the underlying
/// hash map and some configuration variables. Every 'target_cooldown' inserts the
/// 'cache_target' is recalcuated. As long the capacity is below 'min_capacity_limit' cache
/// just fills up. Between 'min_capacity_limit' and 'max_capacity_limit' the 'cache_target' is
/// linearly interpolated between 'max_cache_percent' and 'min_cache_percent', thus allowing a
/// high cache ratio when memory requirements are modest and reduce the memory usage for
/// caching at higher memory loads. When the cached entries exceed the 'cache_target' up to
/// 'evict_batch' entries are removed from the cache.
pub(crate) struct Bucket<K, V>
where
    K: KeyTraits,
{
    map:      ManuallyDrop<Mutex<HashSet<Pin<Box<Entry<K, V>>>>>>,
    lru_list: ManuallyDrop<Mutex<LinkedList<EntryAdapter<K, V>>>>,

    // Stats section
    pub(crate) cached: AtomicUsize,

    // State section
    pub(crate) cache_target:     AtomicU8,
    pub(crate) target_countdown: AtomicU32,

    // Configuration
    pub(crate) target_cooldown: AtomicU32,

    pub(crate) highwater:          AtomicUsize,
    pub(crate) max_capacity_limit: AtomicUsize,
    pub(crate) min_capacity_limit: AtomicUsize,
    pub(crate) max_cache_percent:  AtomicU8,
    pub(crate) min_cache_percent:  AtomicU8,

    pub(crate) evict_batch: AtomicU8,
}

impl<K, V> Drop for Bucket<K, V>
where
    K: KeyTraits,
{
    fn drop(&mut self) {
        // The lru_list contains a number of pointers into map, which it walks and turns into
        // references in its Drop impl. Therefore, we must ensure that lru_list is dropped before
        // the map or we have a use-after-free.
        unsafe {
            ManuallyDrop::drop(&mut self.lru_list);
            ManuallyDrop::drop(&mut self.map);
        }
    }
}

impl<K, V> Bucket<K, V>
where
    K: KeyTraits,
{
    pub(crate) fn new() -> Self {
        Self {
            map:                ManuallyDrop::new(Mutex::new(HashSet::new())),
            lru_list:           ManuallyDrop::new(Mutex::new(LinkedList::new(EntryAdapter::new()))),
            cached:             AtomicUsize::new(0),
            cache_target:       AtomicU8::new(50),
            highwater:          AtomicUsize::new(usize::MAX),
            target_countdown:   AtomicU32::new(0),
            target_cooldown:    AtomicU32::new(100),
            max_capacity_limit: AtomicUsize::new(10000000),
            min_capacity_limit: AtomicUsize::new(1000),
            max_cache_percent:  AtomicU8::new(60),
            min_cache_percent:  AtomicU8::new(5),
            evict_batch:        AtomicU8::new(16),
        }
    }

    pub(crate) fn lock_map(&self) -> MutexGuard<HashSet<Pin<Box<Entry<K, V>>>>> {
        self.map.lock()
    }

    pub(crate) fn lock_lru(&self) -> MutexGuard<LinkedList<EntryAdapter<K, V>>> {
        self.lru_list.lock()
    }

    pub(crate) fn use_entry(
        &self,
        mut lru_lock: MutexGuard<LinkedList<EntryAdapter<K, V>>>,
        entry: &Entry<K, V>,
    ) {
        if entry.lru_link.is_linked() {
            unsafe { lru_lock.cursor_mut_from_ptr(&*entry).remove() };
            self.cached.fetch_sub(1, Ordering::Relaxed);
        }
        entry.expire.store(false, Ordering::Relaxed);
    }

    pub(crate) fn unuse_entry(
        &self,
        mut lru_lock: MutexGuard<LinkedList<EntryAdapter<K, V>>>,
        entry: &Entry<K, V>,
    ) {
        if !entry.value.is_locked() && !entry.lru_link.is_linked() {
            self.cached.fetch_add(1, Ordering::Relaxed);
            if !entry.expire.load(Ordering::Relaxed) {
                lru_lock.push_back(unsafe { UnsafeRef::from_raw(entry) });
            } else {
                lru_lock.push_front(unsafe { UnsafeRef::from_raw(entry) });
            }
        }
    }

    pub(crate) fn enlist_entry(
        &self,
        mut lru_lock: MutexGuard<LinkedList<EntryAdapter<K, V>>>,
        entry: &Entry<K, V>,
    ) {
        if !entry.lru_link.is_linked() {
            self.cached.fetch_add(1, Ordering::Relaxed);
            lru_lock.push_back(unsafe { UnsafeRef::from_raw(entry) });
        }
    }

    /// recalculates the 'cache_target' and evicts entries from the LRU when above target
    pub(crate) fn maybe_evict(&self, map_lock: &mut MutexGuard<HashSet<Pin<Box<Entry<K, V>>>>>) {
        let min_capacity_limit = self.min_capacity_limit.load(Ordering::Relaxed);
        let min_cache_percent = self.min_cache_percent.load(Ordering::Relaxed);
        let capacity = map_lock.capacity();

        // recalculate the cache_target
        let countdown = self.target_countdown.load(Ordering::Relaxed);
        if countdown > 0 {
            // just keep counting down
            self.target_countdown
                .store(countdown - 1, Ordering::Relaxed)
        } else {
            let max_capacity_limit = self.max_capacity_limit.load(Ordering::Relaxed);
            let max_cache_percent = self.max_cache_percent.load(Ordering::Relaxed);

            self.target_countdown.store(
                self.target_cooldown.load(Ordering::Relaxed),
                Ordering::Relaxed,
            );

            // linear interpolation between the min/max points
            let cache_target = if capacity > max_capacity_limit {
                min_cache_percent
            } else if capacity < min_capacity_limit {
                max_cache_percent
            } else {
                ((max_cache_percent as usize * (max_capacity_limit - capacity)
                    + min_cache_percent as usize * (capacity - min_capacity_limit))
                    / (max_capacity_limit - min_capacity_limit)) as u8
            };
            self.cache_target.store(cache_target, Ordering::Relaxed);
        }

        let cached = self.cached.load(Ordering::Relaxed);
        let percent_cached = if capacity > 0 {
            (cached * 100 / capacity) as u8
        } else {
            0
        };

        if map_lock.len() > self.highwater.load(Ordering::Relaxed)
            || (capacity > min_capacity_limit
                && percent_cached > self.cache_target.load(Ordering::Relaxed))
        {
            // lets evict some entries
            self.evict(self.evict_batch.load(Ordering::Relaxed) as usize, map_lock);
        }
    }

    /// evicts up to 'n' entries from the LRU list. Returns the number of evicted entries which
    /// may be less than 'n' in case the list got depleted.
    pub(crate) fn evict(
        &self,
        n: usize,
        map_lock: &mut MutexGuard<HashSet<Pin<Box<Entry<K, V>>>>>,
    ) -> usize {
        #[cfg(feature = "logging")]
        debug!("evicting {} elements", n);
        for i in 0..n {
            if let Some(entry) = self.lru_list.lock().pop_front() {
                map_lock.remove(&entry.key);
                self.cached.fetch_sub(1, Ordering::Relaxed);
            } else {
                return i;
            }
        }
        n
    }
}

impl<K, V> Debug for Bucket<K, V>
where
    K: KeyTraits,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let map_lock = self.lock_map();
        f.debug_struct("Bucket")
            .field("map.len()", &map_lock.len())
            .field("map.capacity()", &map_lock.capacity())
            .field("cached", &self.cached.load(Ordering::Relaxed))
            .field("cache_target", &self.cache_target.load(Ordering::Relaxed))
            .field(
                "max_capacity_limit",
                &self.max_capacity_limit.load(Ordering::Relaxed),
            )
            .field(
                "min_capacity_limit",
                &self.min_capacity_limit.load(Ordering::Relaxed),
            )
            .field(
                "max_cache_percent",
                &self.max_cache_percent.load(Ordering::Relaxed),
            )
            .field(
                "min_cache_percent",
                &self.min_cache_percent.load(Ordering::Relaxed),
            )
            .field("evict_batch", &self.evict_batch.load(Ordering::Relaxed))
            .finish()
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

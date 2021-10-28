use std::collections::{hash_map::DefaultHasher, HashMap};
use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};

use intrusive_collections::LinkedList;
#[allow(unused_imports)]
pub use log::{debug, error, info, trace, warn};
use parking_lot::{Mutex, MutexGuard};

use crate::entry::EntryAdapter;
use crate::Entry;
use crate::UnsafeRef;

/// The internal representation of a Bucket.
#[derive(Debug)]
pub(crate) struct Bucket<K: Eq + Bucketize + Debug, V> {
    map:      Mutex<HashMap<K, Pin<Box<Entry<V>>>>>,
    lru_list: Mutex<LinkedList<EntryAdapter<V>>>,

    // Stats section
    cold: AtomicUsize,
}

impl<K, V> Bucket<K, V>
where
    K: Eq + Clone + Bucketize + Debug,
{
    pub(crate) fn new() -> Self {
        Self {
            map:      Mutex::new(HashMap::new()),
            lru_list: Mutex::new(LinkedList::new(EntryAdapter::new())),
            cold:     AtomicUsize::new(0),
        }
    }

    pub(crate) fn lock_map(&self) -> MutexGuard<HashMap<K, Pin<Box<Entry<V>>>>> {
        self.map.lock()
    }

    pub(crate) fn use_entry(&self, entry: &Entry<V>) {
        let mut lru_lock = self.lru_list.lock();
        if entry.lru_link.is_linked() {
            unsafe { lru_lock.cursor_mut_from_ptr(&*entry).remove() };
            self.cold.fetch_sub(1, Ordering::Relaxed);
        }
        entry.use_count.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn unuse_entry(&self, entry: &Entry<V>) {
        let mut lru_lock = self.lru_list.lock();
        if entry.use_count.fetch_sub(1, Ordering::Relaxed) == 0 {
            self.cold.fetch_add(1, Ordering::Relaxed);
            lru_lock.push_back(unsafe { UnsafeRef::from_raw(entry) });
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

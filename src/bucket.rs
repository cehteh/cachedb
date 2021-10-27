use std::collections::{hash_map::DefaultHasher, HashMap};
use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::pin::Pin;

use intrusive_collections::LinkedList;
#[allow(unused_imports)]
pub use log::{debug, error, info, trace, warn};
use parking_lot::{Mutex, MutexGuard};

use crate::entry::EntryAdapter;
use crate::Entry;

/// The internal representation of a Bucket.
#[derive(Debug)]
pub(crate) struct Bucket<K: Eq + Bucketize + Debug, V> {
    map:      Mutex<HashMap<K, Pin<Box<Entry<V>>>>>,
    lru_list: Mutex<LinkedList<EntryAdapter<V>>>,
}

impl<K, V> Bucket<K, V>
where
    K: Eq + Clone + Bucketize + Debug,
{
    pub(crate) fn new() -> Self {
        Self {
            map:      Mutex::new(HashMap::new()),
            lru_list: Mutex::new(LinkedList::new(EntryAdapter::new())),
        }
    }

    pub(crate) fn lock_map(&self) -> MutexGuard<HashMap<K, Pin<Box<Entry<V>>>>> {
        self.map.lock()
    }

    pub(crate) fn lock_lru(&self) -> MutexGuard<LinkedList<EntryAdapter<V>>> {
        self.lru_list.lock()
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

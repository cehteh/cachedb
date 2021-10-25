//! In memory Key/Value store with LRU expire and concurrent access
//!
//! Items are stored in N bucketized HashMaps to improve concurrency.  Every Item is always
//! behind a RwLock.  Quering an item will return a guard associated to this lock.  Items that
//! are not locked in any way are kept in a list to implement a least-recent-used expire
//! policy.  Locked items re removed for that lru list and pushed on its back when they become
//! unlocked.  When items become locked the lock on the hosting HashMap becomes released.
//! Thus locking items can not block any other access on the map.  This is obtained with some
//! 'unsafe' code.
//!
//!
//! Implementation Discussion
//! =========================
//!
//! The HashMap storing the Items in Boxed entries.  Entries protect the actual item by a
//! RwLock.  The API allows access to items only over these locks, returning wraped guards
//! thereof.
//!
//! New Items are constructed in an atomic way by passing a closure producing the item to the
//! respective lookup function.  While an Item is constructed it has a write lock which
//! ensures that on concurrent construction/queries only one contructor wins and any other
//! will acquire the newly constructed item.
//!
//!
//! Proof that no lifetime guarantees are violated
//! ----------------------------------------------
//!
//! Is actually simple, the returned guard has a rust lifetime bound to the CacheDB
//! object.  Thus no access can outlive the hosting collection.
//!
//!
//! Proof that no data races exist
//! ------------------------------
//!
//! In most parts the Mutex and RwLock ensures that no data races can happen, this is
//! validated by rust.
//!
//! The unsafe part of the implementation detaches a LockGuard from its hosting collection to
//! free the mutex on the HashMap.  This could lead to potential UB when the HashMap drops a
//! value that is still in use/locked.  However this can never be happen because there is no
//! way to drop Entries in a uncontrolled way.  The guard lifetimes are tied to the hosting
//! hashmap the can not outlive it.  Dropping items from the hash map is normally only done
//! from the LRU list which will never contain locked (and thus in-use) Entries. The
//! 'remove(key)' member function checks explicitly that an Entry is not in use or delays the
//! removal until all locks on the Item are released.
//!
//! While the HashMap may reallocate the tables and thus move the Boxes containing the Entries
//! around, this is not a problem since the lock guards contain references to Entries
//! directly, not to the outer Box.
//!
//!
//! Proof that locking is deadlock free
//! -----------------------------------
//!
//! Locks acquired in the same order can never deadlock.  Deadlocks happen only when 2 or more
//! threads wait on a resource while already holding resource another theread is trying to
//! obtain.
//!
//! On lookup the hashmap will be locked. When the element is found the LRU list is locked and
//! the element may be removed from it (when it was not in use). Once done with the LRU list
//! its lock is released.
//!
//! It is worth to mention that code using the cachedb can still deadlock when it acquires
//! locks in ill order. The simplest advise is to have only one single exclusive lock at all
//! time per thread. When is impractical one need to carefully consider locking order or
//! employ other tactics to counter deadlocks.
//!
//!
//! TESTS
//! =====
//!
//! The 'test::multithreaded_stress' test can be controlled by environment variables
//!
//!  * 'STRESS_THREADS' sets the number of threads to spawn.  Defaults to 10.
//!  * 'STRESS_WAIT' threads randomly wait up to this much milliseconds to fake some work.  Defaults to 5.
//!  * 'STRESS_ITERATIONS' how many iterations each thread shall do.  Defaults to 100.
//!  * 'STRESS_RANGE' how many unique keys the test uses.  Defaults to 1000.
//!
//! The default values are rather small to make the test suite complete fast. For dedicated
//! stress testing at least STRESS_ITERATIONS and STRESS_THREADS has to be incresed significantly.
//!
//!
//! ISSUES
//! ======
//!
//! * Until a full lock_transpose() which transfers locks automically becomes implemented,
//!   waiting for a lock will block the whole bucket. This can be mitigated by finer grained
//!   locking but a definitive solution would be the lock transfer. The workaround is not
//!   planned to be implemented yet.
//! * LRU list is not implemented yet
//!
use std::collections::{hash_map::DefaultHasher, HashMap};
use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::marker::PhantomPinned;
use std::ops::Deref;
use std::pin::Pin;
//use std::sync::atomic::{AtomicUsize, Ordering};

#[allow(unused_imports)]
pub use log::{debug, error, info, trace, warn};

//use intrusive_collections::linked_list::{Link, LinkedList};
use parking_lot::{Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};
pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

pub fn type_name<T>(_: &T) -> &str {
    std::any::type_name::<T>()
}

/// CacheDb implements the concurrent (bucketed) Key/Value store.  Keys must implement
/// 'Bucketize' which has more lax requirments than a full hash implmementation.  'N' is the
/// number of buckets to use. This is const because less dereferencing and management
/// overhead.  Buckets by themself are not very expensive thus it is recommended to use a
/// generous large enough number here.  Think about expected number of concurrenct accesses
/// times four.
#[derive(Debug)]
pub struct CacheDb<K, V, const N: usize>
where
    K: Eq + Clone + Bucketize + Debug,
{
    buckets: [Bucket<K, V>; N],
}

/// The internal representation of a Bucket.
#[derive(Debug)]
struct Bucket<K: Eq + Bucketize + Debug, V> {
    map: Mutex<HashMap<K, Pin<Box<Entry<V>>>>>,
}

impl<K, V> Bucket<K, V>
where
    K: Eq + Clone + Bucketize + Debug,
{
    fn new() -> Self {
        Self {
            map: Mutex::new(HashMap::new()),
        }
    }

    fn lock(&self) -> MutexGuard<HashMap<K, Pin<Box<Entry<V>>>>> {
        self.map.lock()
    }
}

/// User data is stored behind RwLocks in an entry. Furthermore some management information
/// like the LRU list node are stored here. Entries have stable addresses and can't be moved
/// in memory.
#[derive(Debug)]
struct Entry<V> {
    // PLANNED: implement atomic lock transititon between two locks (as is, waiting on the rwlock will block the hashmap)
    // The Option is only used for delaying the construction.
    data: RwLock<Option<V>>,
    _pin: PhantomPinned,
}

impl<V> Default for Entry<V> {
    fn default() -> Self {
        Entry {
            data: RwLock::new(None),
            _pin: PhantomPinned,
        }
    }
}

impl<K, V, const N: usize> CacheDb<K, V, N>
where
    K: Eq + Clone + Bucketize + Debug,
{
    /// Create a new CacheDb
    pub fn new() -> CacheDb<K, V, N> {
        CacheDb {
            // highwater_mark: AtomicUsize::new(50),
            // evicts_per_insert: AtomicUsize::new(2),
            // lowwater_mark: AtomicUsize::new(10),
            // inserts_per_evict: AtomicUsize::new(2),
            // cold: AtomicUsize::new(0),
            // lru_list: RwLock<LinkedList>
            buckets: [(); N].map(|()| Bucket::new()),
        }
    }

    /// Query the Entry associated with key for reading
    pub fn get<'a>(&'a self, key: &K) -> Option<EntryReadGuard<K, V, N>> {
        self.buckets[key.bucket::<N>()]
            .lock()
            .get(key)
            .map(|entry| {
                let entry_ptr: *const Entry<V> = &**entry;
                trace!("read lock: {:?}", key);
                EntryReadGuard {
                    cachedb: self,
                    entry: unsafe { &*entry_ptr },
                    guard: unsafe { (*entry_ptr).data.read() },
                }
            })
    }

    // TODO: The ctor function may become double nested Fn() -> Result(Fn() -> Result(Value)) The
    //       outer can acquire resouces while the cachedb is (temporary) unlocked and returns the
    //       real ctor then.
    /// Query an Entry for reading or construct it (atomically)
    pub fn get_or<'a, F>(&'a self, key: &K, ctor: F) -> Result<EntryReadGuard<K, V, N>>
    where
        F: FnOnce(&K) -> Result<V>,
    {
        let mut bucket = self.buckets[key.bucket::<N>()].lock();

        match bucket.get(&key) {
            Some(entry) => {
                // Entry exists, return a locked ReadGuard to it
                let entry_ptr: *const Entry<V> = &**entry;
                trace!("read lock (existing): {:?}", key);
                Ok(EntryReadGuard {
                    cachedb: self,
                    entry: unsafe { &*entry_ptr },
                    guard: unsafe { (*entry_ptr).data.read() },
                })
            }
            None => {
                // Entry does not exist, we create an empty (data == None) entry and holding a
                // write lock on it
                let new_entry = Box::pin(Entry::default());
                let entry_ptr: *const Entry<V> = &*new_entry;
                let mut wguard = unsafe { (*entry_ptr).data.write() };
                // insert the entry into the bucket
                trace!("create for reading: {:?}", &key);
                bucket.insert(key.clone(), new_entry);
                // release the bucket lock, we dont need it anymore
                drop(bucket);

                // but we have wguard here which allows us to constuct the inner guts
                *wguard = Some(ctor(key)?);

                // Finally downgrade the lock to a readlock and return the Entry
                Ok(EntryReadGuard {
                    cachedb: self,
                    entry: unsafe { &*entry_ptr },
                    guard: RwLockWriteGuard::downgrade(wguard),
                })
            }
        }
    }
}

/// RAII Guard for the read lock. Manages to put unused entries into the LRU list.
#[derive(Debug)]
pub struct EntryReadGuard<'a, K, V, const N: usize>
where
    K: Eq + Clone + Bucketize + Debug,
{
    cachedb: &'a CacheDb<K, V, N>,
    entry: &'a Entry<V>,
    guard: RwLockReadGuard<'a, Option<V>>,
}

impl<'a, K, V, const N: usize> Drop for EntryReadGuard<'_, K, V, N>
where
    K: Eq + Clone + Bucketize + Debug,
{
    fn drop(&mut self) {
        trace!("dropping lock");
    }
}

impl<'a, K, V, const N: usize> Deref for EntryReadGuard<'_, K, V, N>
where
    K: Eq + Clone + Bucketize + Debug,
{
    type Target = V;
    fn deref(&self) -> &Self::Target {
        // unwrap is safe, the option is only None for a short time while constructing a new value
        &(*self.guard).as_ref().unwrap()
    }
}

/// RAII Guard for the write lock. Manages to put unused entries into the LRU list.
#[derive(Debug)]
pub struct EntryWriteGuard<'a, K, V, const N: usize>
where
    K: Eq + Clone + Bucketize + Debug,
{
    cachedb: &'a CacheDb<K, V, N>,
    guard: RwLockWriteGuard<'a, V>,
}

impl<'a, K, V, const N: usize> Drop for EntryWriteGuard<'_, K, V, N>
where
    K: Eq + Clone + Bucketize + Debug,
{
    fn drop(&mut self) {
        trace!("dropping lock");
    }
}

impl<'a, K, V, const N: usize> Deref for EntryWriteGuard<'_, K, V, N>
where
    K: Eq + Clone + Bucketize + Debug,
{
    type Target = V;
    fn deref(&self) -> &Self::Target {
        &(*self.guard)
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

#[cfg(test)]
mod test {
    use crate::*;
    use parking_lot::Once;
    use rand::Rng;
    use std::env;
    use std::{thread, time};

    static INIT: Once = Once::new();

    fn init() {
        INIT.call_once(|| simple_logger::init_with_env().unwrap());
    }

    // using the default hash based implementation for tests here
    impl Bucketize for String {}
    impl Bucketize for u16 {
        fn bucket<const N: usize>(&self) -> usize {
            let r = *self as usize % N;
            trace!("key {} falls into bucket {}", self, r);
            r
        }
    }

    #[test]
    fn create() {
        init();
        let cdb = CacheDb::<String, String, 16>::new();

        assert!(cdb.get(&"foo".to_string()).is_none());
    }

    #[test]
    fn insert_foobar() {
        init();
        let cdb = CacheDb::<String, String, 16>::new();

        assert!(cdb
            .get_or(&"foo".to_string(), |_| Ok("bar".to_string()))
            .is_ok());
        assert_eq!(*cdb.get(&"foo".to_string()).unwrap(), "bar".to_string());
    }

    #[test]
    pub fn multithreaded_stress() {
        init();
        let cdb = CacheDb::<u16, u16, 64>::new();
        let mut rng = rand::thread_rng();

        let num_threads: u16 = env::var("STRESS_THREADS")
            .unwrap_or("10".to_string())
            .parse()
            .unwrap();
        let wait_millis: u64 = env::var("STRESS_WAIT")
            .unwrap_or("5".to_string())
            .parse()
            .unwrap();
        let iterations: u64 = env::var("STRESS_ITERATIONS")
            .unwrap_or("100".to_string())
            .parse()
            .unwrap();
        let range: u16 = env::var("STRESS_RANGE")
            .unwrap_or("1000".to_string())
            .parse()
            .unwrap();

        // The per thread function

        let mut locked = HashMap::<u16, EntryReadGuard<u16, u16, 64>>::new();

        for _ in 1..iterations {
            // r is the key we handle
            let r = rng.gen_range(0..range);
            // p is the probability of some operation
            let p = rng.gen_range(1..100);
            // w is the wait time to simulate thread work
            let w = time::Duration::from_millis(rng.gen_range(0..wait_millis));

            match locked.remove(&r) {
                // thread had no lock stored, create a new entry
                None => {
                    // TODO: chance for touch
                    if p <= 15 {
                        // TODO: remove
                    } else if p <= 30 {
                        // TODO: touch
                    } else if p <= 50 {
                        trace!("get_or {} and keep it", r);
                        locked.insert(r, cdb.get_or(&r, |_| Ok(!r)).unwrap());
                    } else if p <= 55 {
                        // TODO: get_mut_or work
                    } else if p <= 60 {
                        // TODO: work get_mut_or
                    } else if p <= 80 {
                        trace!("get_or {} and then wait/work for {:?}", r, w);
                        let lock = cdb.get_or(&r, |_| Ok(!r)).unwrap();
                        thread::sleep(w);
                    } else {
                        trace!("wait/work for {:?} and then get_or {}", w, r);
                        thread::sleep(w);
                        let lock = cdb.get_or(&r, |_| Ok(!r)).unwrap();
                    }
                }

                // locked already for reading, lets drop it
                Some(read_guard) => {
                    if p <= 95 {
                        trace!("unlock kept readguard {}", r);
                        drop(read_guard);
                    } else {
                        // TODO: drop-remove
                    }
                }
            };
        }

        drop(locked);

        // after all threads joined
        // TODO: finally assert that nothing is locked
    }
}

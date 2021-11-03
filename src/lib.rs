//! In memory Key/Value store with LRU expire and concurrent access
//!
//!
//! Description
//! ===========
//!
//! Items are stored in N sharded/bucketized HashMaps to improve concurrency.  Every Item is
//! always behind a RwLock.  Quering an item will return a guard associated to this lock.
//! Items that are not locked are kept in a list to implement a least-recent-used expire
//! policy.  Locked items are removed from that lru list and put into the lru-list when they
//! become unlocked.  Locked Items will not block the hosting HashMap.
//!
//!
//! Implementation Discussion
//! =========================
//!
//! The HashMap storing the Items in Boxed entries.  Entries protect the actual item by a
//! RwLock.  The API allows access to items only over these locks, returning wraped guards
//! thereof. Since cConcurrent access to the Entries will not block the Hashmap, some
//! 'unsafe' code is required which is hidden behind an safe API.
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
//! LRU List and expire configuration
//! =================================
//!
//! Items that are not in use are pushed onto the tail of an least-recently-used
//! list. Whenever a CacheDb decides to expire Items these are taken from the head of the
//! lru-list and dropped.
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
//! The default values are rather small to make the test suite complete in short time. For dedicated
//! stress testing at least STRESS_ITERATIONS and STRESS_THREADS has to be incresed significantly.
//! Try 'STRESS_ITERATIONS=10000 STRESS_RANGE=10000 STRESS_THREADS=10000' for some harder test.
//!
//!
//! ISSUES
//! ======
//!
//! * Until a full lock_transpose() which transfers locks automically becomes implemented,
//!   waiting for a lock will block the whole bucket. This can be mitigated by finer grained
//!   locking but a definitive solution would be the lock transpose.
use std::sync::atomic::Ordering;

#[allow(unused_imports)]
pub use log::{debug, error, info, trace, warn};
use intrusive_collections::UnsafeRef;
use parking_lot::RwLockWriteGuard;

mod entry;
use crate::entry::Entry;
pub use crate::entry::{EntryReadGuard, EntryWriteGuard, KeyTraits};

mod bucket;
use crate::bucket::Bucket;
pub use crate::bucket::Bucketize;
pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

/// CacheDb implements the concurrent (bucketed) Key/Value store.  Keys must implement
/// 'Bucketize' which has more lax requirments than a full hash implmementation.  'N' is the
/// number of buckets to use. This is const because less dereferencing and management
/// overhead.  Buckets by themself are not very expensive thus it is recommended to use a
/// generous large enough number here.  Think about expected number of concurrenct accesses
/// times four.
pub struct CacheDb<K, V, const N: usize>
where
    K: KeyTraits,
{
    buckets: [Bucket<K, V>; N],
}

impl<K, V, const N: usize> CacheDb<K, V, N>
where
    K: KeyTraits,
{
    /// Create a new CacheDb
    pub fn new() -> CacheDb<K, V, N> {
        CacheDb {
            buckets: [(); N].map(|()| Bucket::new()),
        }
    }

    /// Query the Entry associated with key for reading
    pub fn get<'a>(&'a self, key: &K) -> Option<EntryReadGuard<K, V, N>> {
        let bucket = &self.buckets[key.bucket::<N>()];
        let map_lock = bucket.lock_map();

        map_lock.get(key).map(|entry| {
            bucket.use_entry(entry);

            let entry_ptr: *const Entry<K, V> = &**entry;
            #[cfg(feature = "logging")]
            trace!("read lock: {:?}", key);
            EntryReadGuard {
                bucket,
                entry: unsafe { &*entry_ptr },
                guard: unsafe { (*entry_ptr).value.read() },
            }
        })
    }

    /// Query the Entry associated with key for writing
    pub fn get_mut<'a>(&'a self, key: &K) -> Option<EntryWriteGuard<K, V, N>> {
        let bucket = &self.buckets[key.bucket::<N>()];
        let map_lock = bucket.lock_map();

        map_lock.get(key).map(|entry| {
            bucket.use_entry(entry);

            let entry_ptr: *const Entry<K, V> = &**entry;
            #[cfg(feature = "logging")]
            trace!("write lock: {:?}", key);
            EntryWriteGuard {
                bucket,
                entry: unsafe { &*entry_ptr },
                guard: unsafe { (*entry_ptr).value.write() },
            }
        })
    }

    // TODO: The ctor function may become double nested Fn() -> Result(Fn() -> Result(Value)) The
    //       outer can acquire resouces while the cachedb is (temporary) unlocked and returns the
    //       real ctor then.
    /// Query an Entry for reading or construct it (atomically)
    pub fn get_or_insert<'a, F>(&'a self, key: &K, ctor: F) -> Result<EntryReadGuard<K, V, N>>
    where
        F: FnOnce(&K) -> Result<V>,
    {
        let bucket = &self.buckets[key.bucket::<N>()];
        let mut map_lock = bucket.lock_map();

        match map_lock.get(key) {
            Some(entry) => {
                bucket.use_entry(entry);
                // Entry exists, return a locked ReadGuard to it
                let entry_ptr: *const Entry<K, V> = &**entry;
                #[cfg(feature = "logging")]
                trace!("read lock (existing): {:?}", key);
                Ok(EntryReadGuard {
                    bucket,
                    entry: unsafe { &*entry_ptr },
                    guard: unsafe { (*entry_ptr).value.read() },
                })
            }
            None => {
                // Entry does not exist, we create an empty (data == None) entry and holding a
                // write lock on it
                let new_entry = Box::pin(Entry::new(key.clone()));
                let entry_ptr: *const Entry<K, V> = &*new_entry;
                let mut wguard = unsafe { (*entry_ptr).value.write() };

                bucket.maybe_evict(&mut map_lock);

                // insert the entry into the bucket
                #[cfg(feature = "logging")]
                trace!("create for reading: {:?}", &key);
                map_lock.insert(new_entry);

                // release the map_lock, we dont need it anymore
                drop(map_lock);

                // but we have wguard here which allows us to constuct the inner guts
                *wguard = Some(ctor(key)?);

                // Finally downgrade the lock to a readlock and return the Entry
                Ok(EntryReadGuard {
                    bucket,
                    entry: unsafe { &*entry_ptr },
                    guard: RwLockWriteGuard::downgrade(wguard),
                })
            }
        }
    }

    /// Query an Entry for writing or construct it (atomically)
    pub fn get_or_insert_mut<'a, F>(&'a self, key: &K, ctor: F) -> Result<EntryWriteGuard<K, V, N>>
    where
        F: FnOnce(&K) -> Result<V>,
    {
        let bucket = &self.buckets[key.bucket::<N>()];
        let mut map_lock = bucket.lock_map();

        match map_lock.get(key) {
            Some(entry) => {
                bucket.use_entry(entry);
                // Entry exists, return a locked ReadGuard to it
                let entry_ptr: *const Entry<K, V> = &**entry;
                #[cfg(feature = "logging")]
                trace!("write lock (existing): {:?}", key);
                Ok(EntryWriteGuard {
                    bucket,
                    entry: unsafe { &*entry_ptr },
                    guard: unsafe { (*entry_ptr).value.write() },
                })
            }
            None => {
                // Entry does not exist, we create an empty (data == None) entry and holding a
                // write lock on it
                let new_entry = Box::pin(Entry::new(key.clone()));
                let entry_ptr: *const Entry<K, V> = &*new_entry;
                let mut wguard = unsafe { (*entry_ptr).value.write() };

                bucket.maybe_evict(&mut map_lock);

                // insert the entry into the bucket
                #[cfg(feature = "logging")]
                trace!("create for reading: {:?}", &key);
                map_lock.insert(new_entry);

                // release the map_lock, we dont need it anymore
                drop(map_lock);

                // but we have wguard here which allows us to constuct the inner guts
                *wguard = Some(ctor(key)?);

                // Finally downgrade the lock to a readlock and return the Entry
                Ok(EntryWriteGuard {
                    bucket,
                    entry: unsafe { &*entry_ptr },
                    guard: wguard,
                })
            }
        }
    }

    /// The 'cache_target' will only recalculated after this many inserts. Should be in the
    /// lower hundreds.
    pub fn config_target_cooldown(&self, target_cooldown: u32) {
        for bucket in &self.buckets {
            bucket
                .target_cooldown
                .store(target_cooldown, Ordering::Relaxed);
        }
    }

    /// Sets the lower limit for the 'cache_target' linear interpolation region.  Some
    /// hundreds to thousands of entries are recommended. Should be less than
    /// 'max_capacity_limit'.
    pub fn config_min_capacity_limit(&self, min_capacity_limit: usize) {
        for bucket in &self.buckets {
            // divide by N so that each bucket gets its share
            bucket
                .min_capacity_limit
                .store(min_capacity_limit / N, Ordering::Relaxed);
        }
    }

    /// Sets the upper limit for the 'cache_target' linear interpolation region.
    /// Should be fine around the maximum expected number of entries.
    pub fn config_max_capacity_limit(&self, max_capacity_limit: usize) {
        for bucket in &self.buckets {
            // divide by N so that each bucket gets its share
            bucket
                .max_capacity_limit
                .store(max_capacity_limit / N, Ordering::Relaxed);
        }
    }

    /// Sets the lower limit for the 'cache_target' in percent at 'max_capacity_limit'. Since
    /// when very much entries are stored it is desireable to have a lower percentage of
    /// cached items for wasting less memory. Note that this counts against the 'capacity' of
    /// the underlying container, not the stored entries. Recommended values are around 5%,
    /// but may vary on the access patterns. Should be lower than 'max_cache_percent'
    pub fn config_min_cache_percent(&self, min_cache_percent: u8) {
        assert!(min_cache_percent < 100);
        for bucket in &self.buckets {
            bucket
                .min_cache_percent
                .store(min_cache_percent, Ordering::Relaxed);
        }
    }

    /// Sets the upper limit for the 'cache_target' in percent at 'min_capacity_limit'. When
    /// only few entries are stored in a CacheDb it is reasonable to use a lot space for
    /// caching. Note that this counts against the 'capacity' of the underlying container,
    /// thus it should be not significantly over 60% at most.
    pub fn config_max_cache_percent(&self, max_cache_percent: u8) {
        assert!(max_cache_percent < 100);
        for bucket in &self.buckets {
            bucket
                .max_cache_percent
                .store(max_cache_percent, Ordering::Relaxed);
        }
    }

    /// Sets the number of entries removed at once when evicting entries from the cache. Since
    /// evicting branches into the code parts for removing the entries and calling their
    /// destructors it is a bit more cache friendly to batch a few such things together.
    pub fn config_evict_batch(&self, evict_batch: u8) {
        for bucket in &self.buckets {
            bucket.evict_batch.store(evict_batch, Ordering::Relaxed);
        }
    }
}

impl<K, V, const N: usize> Default for CacheDb<K, V, N>
where
    K: KeyTraits,
{
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;
    use std::env;
    use std::sync::{Arc, Barrier};
    use std::{thread, time};

    #[cfg(feature = "logging")]
    use parking_lot::Once;
    use rand::Rng;

    use crate::*;

    #[cfg(feature = "logging")]
    static INIT: Once = Once::new();

    fn init() {
        #[cfg(feature = "logging")]
        INIT.call_once(|| env_logger::init());
    }

    // using the default hash based implementation for tests here
    impl Bucketize for String {}
    impl Bucketize for u16 {
        fn bucket<const N: usize>(&self) -> usize {
            let r = *self as usize % N;
            #[cfg(feature = "logging")]
            trace!("key {} falls into bucket {}", self, r);
            r
        }
    }

    impl KeyTraits for String {}
    impl KeyTraits for u16 {}

    #[test]
    fn create() {
        init();
        let cdb = CacheDb::<String, String, 16>::new();

        assert!(cdb.get(&"foo".to_string()).is_none());
    }

    #[test]
    fn insert_foobar_onebucket() {
        init();
        let cdb = CacheDb::<String, String, 1>::new();

        assert!(
            cdb.get_or_insert(&"foo".to_string(), |_| Ok("bar".to_string()))
                .is_ok()
        );
        assert_eq!(*cdb.get(&"foo".to_string()).unwrap(), "bar".to_string());
        assert!(
            cdb.get_or_insert(&"bar".to_string(), |_| Ok("foo".to_string()))
                .is_ok()
        );
        assert_eq!(*cdb.get(&"bar".to_string()).unwrap(), "foo".to_string());
        assert!(
            cdb.get_or_insert(&"foo2".to_string(), |_| Ok("bar2".to_string()))
                .is_ok()
        );
        assert_eq!(*cdb.get(&"foo2".to_string()).unwrap(), "bar2".to_string());
        assert!(
            cdb.get_or_insert(&"bar2".to_string(), |_| Ok("foo2".to_string()))
                .is_ok()
        );
        assert_eq!(*cdb.get(&"bar2".to_string()).unwrap(), "foo2".to_string());
    }

    #[test]
    fn insert_foobar() {
        init();
        let cdb = CacheDb::<String, String, 16>::new();

        assert!(
            cdb.get_or_insert(&"foo".to_string(), |_| Ok("bar".to_string()))
                .is_ok()
        );
        assert_eq!(*cdb.get(&"foo".to_string()).unwrap(), "bar".to_string());
        assert!(
            cdb.get_or_insert(&"bar".to_string(), |_| Ok("foo".to_string()))
                .is_ok()
        );
        assert_eq!(*cdb.get(&"bar".to_string()).unwrap(), "foo".to_string());
        assert!(
            cdb.get_or_insert(&"foo2".to_string(), |_| Ok("bar2".to_string()))
                .is_ok()
        );
        assert_eq!(*cdb.get(&"foo2".to_string()).unwrap(), "bar2".to_string());
        assert!(
            cdb.get_or_insert(&"bar2".to_string(), |_| Ok("foo2".to_string()))
                .is_ok()
        );
        assert_eq!(*cdb.get(&"bar2".to_string()).unwrap(), "foo2".to_string());
    }

    #[test]
    fn mutate() {
        init();
        let cdb = CacheDb::<String, String, 16>::new();

        cdb.get_or_insert(&"foo".to_string(), |_| Ok("bar".to_string()))
            .unwrap();

        *cdb.get_mut(&"foo".to_string()).unwrap() = "baz".to_string();
        assert_eq!(*cdb.get(&"foo".to_string()).unwrap(), "baz".to_string());
    }

    #[test]
    fn insert_mutate() {
        init();
        let cdb = CacheDb::<String, String, 16>::new();

        let mut foo = cdb
            .get_or_insert_mut(&"foo".to_string(), |_| Ok("bar".to_string()))
            .unwrap();
        assert_eq!(*foo, "bar".to_string());
        *foo = "baz".to_string();
        assert_eq!(*foo, "baz".to_string());
        drop(foo);
        assert_eq!(*cdb.get(&"foo".to_string()).unwrap(), "baz".to_string());
    }

    #[test]
    pub fn multithreaded_stress() {
        const BUCKETS: usize = 64;
        init();
        let cdb = Arc::new(CacheDb::<u16, u16, BUCKETS>::new());

        let num_threads: usize = env::var("STRESS_THREADS")
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

        let mut handles = Vec::with_capacity(num_threads);
        let barrier = Arc::new(Barrier::new(num_threads));
        for _ in 0..num_threads {
            let c = Arc::clone(&barrier);
            let cdb = Arc::clone(&cdb);

            handles.push(thread::spawn(
                // The per thread function
                move || {
                    let mut rng = rand::thread_rng();
                    c.wait();

                    let mut locked = HashMap::<u16, EntryReadGuard<u16, u16, BUCKETS>>::new();

                    for _ in 0..iterations {
                        // r is the key we handle
                        let r = rng.gen_range(0..range);
                        // p is the probability of some operation
                        let p = rng.gen_range(0..100);
                        // w is the wait time to simulate thread work
                        let w = time::Duration::from_millis(rng.gen_range(0..wait_millis));
                        match locked.remove(&r) {
                            // thread had no lock stored, create a new entry
                            None => {
                                if p == 0 {
                                    #[cfg(feature = "logging")]
                                    trace!("drop all stored locks");
                                    locked.clear();
                                } else if p < 15 {
                                    // TODO: remove
                                } else if p < 30 {
                                    // TODO: touch
                                } else if p < 50 {
                                    #[cfg(feature = "logging")]
                                    trace!("get_or_insert {} and keep it", r);
                                    locked.insert(r, cdb.get_or_insert(&r, |_| Ok(!r)).unwrap());
                                } else if p < 55 {
                                    // TODO: get_mut_or work
                                } else if p < 60 {
                                    // TODO: work get_mut_or
                                } else if p < 80 {
                                    #[cfg(feature = "logging")]
                                    trace!("get_or {} and then wait/work for {:?}", r, w);
                                    let lock = cdb.get_or_insert(&r, |_| Ok(!r)).unwrap();
                                    thread::sleep(w);
                                    drop(lock);
                                } else {
                                    #[cfg(feature = "logging")]
                                    trace!("wait/work for {:?} and then get_or {}", w, r);
                                    thread::sleep(w);
                                    let lock = cdb.get_or_insert(&r, |_| Ok(!r)).unwrap();
                                    drop(lock);
                                }
                            }

                            // locked already for reading, lets drop it
                            Some(read_guard) => {
                                if p < 95 {
                                    #[cfg(feature = "logging")]
                                    trace!("unlock kept readguard {}", r);
                                    drop(read_guard);
                                } else {
                                    // TODO: drop-remove
                                }
                            }
                        };
                    }
                    drop(locked);
                },
            ));
        }

        // TODO: finally assert that nothing is locked

        for handle in handles {
            handle.join().unwrap();
        }
    }
}

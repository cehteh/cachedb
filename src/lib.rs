#![doc = include_str!("../README.md")]
#![warn(missing_docs)]
#![warn(rustdoc::missing_crate_level_docs)]
//! # LRU List and expire configuration
//!
//! Items that are not in use are pushed onto the tail of an least-recently-used
//! list. Whenever a CacheDb decides to expire Items these are taken from the head of the
//! lru-list and dropped.
//!
//! There are some configuration options to tune the caching behavior. These configurations
//! are all per-bucket and not global. Thus one has to take the number of buckets into account
//! when configuring the cachedb. When exact accounting (for example for 'highwater') is
//! needed one must use a single bucket cachedb.
//!
//! Its is also important to know that min/max capacity configurations account against the
//! *capacity* of the underlying containers, not the used number of entries. This allows for
//! better memory utilization after a container got resized but should be respected in a way
//! that caching won't make the containers grow overly large.
//!
//! The default configuration is choosen to create an rather generic cache that may grow
//! pretty huge and being conservative with expiring entries. By this it should be useable as
//! is for many use cases. If required the `max_capacity_limit` or `highwater` are the first
//! knobs to tune.
//!
//!
//! # Implementation Discussion
//!
//! The HashMap storing the Items in Boxed entries.  Entries protect the actual item by a
//! RwLock.  The API allows access to items only over these locks, returning wraped guards
//! thereof. Since concurrent access to the Entries shall not block the Hashmap, some 'unsafe'
//! code is required to implement hand over hand locking which is hidden behind an safe API.
//!
//! New Items are constructed in an atomic way by passing a closure producing the item to the
//! respective lookup function.  While an Item is constructed it has a write lock which
//! ensures that on concurrent construction/queries only one contructor wins and any other
//! will acquire the newly constructed item.
//!
//!
//! ## Proof that no lifetime guarantees are violated
//!
//! Is actually simple, the returned guard has a rust lifetime bound to the CacheDB
//! object.  Thus no access can outlive the hosting collection.
//!
//!
//! ## Proof that no data races exist
//!
//! In most parts the Mutex and RwLock ensures that no data races can happen, this is
//! validated by rust.
//!
//! The unsafe part of the implementation detaches a LockGuard from its hosting collection to
//! free the mutex on the HashMap.  This could lead to potential UB when the HashMap drops a
//! value that is still in use/locked.  However this can never be happen because there is no
//! way to drop Entries in a uncontrolled way.  The guard lifetimes are tied to the hosting
//! hashmap the can not outlive it.  Dropping items from the hash map only done from the LRU
//! list which will never contain locked (and thus in-use) Entries. The 'remove(key)' member
//! function checks explicitly that an Entry is not in use or delays the removal until all
//! locks on the Item are released.
//!
//! While the HashMap may reallocate the tables and thus move the Boxes containing the Entries
//! around, this is not a problem since the lock guards contain references to Entries
//! directly, not to the outer Box.
//!
//!
//! ## Proof that locking is deadlock free
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
//! # TESTS
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
#![allow(clippy::type_complexity)]
use std::sync::atomic::{AtomicU32, Ordering};
use std::collections::HashSet;
use std::pin::Pin;
use std::fmt::Debug;
use std::fmt::Formatter;

#[allow(unused_imports)]
use log::{debug, error, info, trace, warn};
use intrusive_collections::UnsafeRef;
use parking_method::*;

mod entry;
use crate::entry::Entry;
pub use crate::entry::{EntryReadGuard, EntryWriteGuard, KeyTraits};

mod bucket;
use crate::bucket::Bucket;
pub use crate::bucket::Bucketize;

/// CacheDb implements the concurrent (bucketed) Key/Value store.  Keys must implement
/// 'Bucketize' which has more lax requirments than a full hash implmementation.  'N' is the
/// number of buckets to use. This is const because less dereferencing and management
/// overhead.  Buckets by themself are not very expensive thus it is recommended to use a
/// generous large enough number here.  Think about expected number of concurrenct accesses
/// times four.
pub struct CacheDb<K, V, const N: usize>
where
    V: 'static,
    K: KeyTraits,
{
    buckets:      [Bucket<K, V>; N],
    lru_disabled: AtomicU32,
    ctor:         Option<&'static (dyn Fn(&K) -> DynResult<V> + Sync + Send)>,
}

impl<K, V, const N: usize> Debug for CacheDb<K, V, N>
where
    K: KeyTraits,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        f.debug_struct("CacheDb")
            .field("buckets", &self.buckets)
            .field("lru_disabled", &self.lru_disabled)
            .field("ctor", &self.ctor.is_some())
            .finish()
    }
}

impl<K, V, const N: usize> CacheDb<K, V, N>
where
    K: KeyTraits,
{
    /// Create a new CacheDb
    pub fn new() -> CacheDb<K, V, N> {
        CacheDb {
            buckets:      [(); N].map(|()| Bucket::new()),
            lru_disabled: AtomicU32::new(0),
            ctor:         None,
        }
    }

    /// queries an entry and detaches it from the LRU
    fn query_entry(&self, key: &K) -> Result<(&Bucket<K, V>, *const Entry<K, V>), Error> {
        let bucket = &self.buckets[key.bucket_range::<N>()];
        let map_lock = bucket.lock_map();

        if let Some(entry) = map_lock.get(key) {
            bucket.use_entry(bucket.lock_lru(), entry);
            Ok((bucket, &**entry))
        } else {
            Err(Error::NoEntry)
        }
    }

    /// Query the Entry associated with key for reading.  On success a EntryReadGuard
    /// protecting the looked up value is returned.  When a default constructor is configured
    /// (see `with_constructor()`) it tries to construct missing entries, with `get_or_insert()`.
    /// When that fails the constructors error is returned. Otherwise
    /// `Error::NoEntry` will be returned when the queried item is not in the cache.
    ///
    /// The 'method' defines how entries are locked and can be one of:
    ///   * Blocking: normal blocking lock, returns when the lock is acquired
    ///   * TryLock: tries to lock the entry, returns 'Error::LockUnavailable'
    ///     when the lock can't be obtained instantly.
    ///   * Duration: tries to lock the entry with a timeout, returns 'Error::LockUnavailable'
    ///     when the lock can't be obtained within this time.
    ///   * Instant: tries to lock the entry until some point in time, returns 'Error::LockUnavailable'
    ///     when the lock can't be obtained in time.
    ///   All of the can be wraped in 'Recursive()' to allow a thread to relock any lock it already helds.
    pub fn get<'a, M>(&'a self, method: M, key: &K) -> DynResult<EntryReadGuard<K, V, N>>
    where
        M: 'a + RwLockMethod<'a, Option<V>>,
    {
        match &self.ctor {
            Some(ctor) => self.get_or_insert(method, key, ctor),
            None => {
                let (bucket, entry_ptr) = self.query_entry(key)?;

                let guard = unsafe {
                    RwLockMethod::read(&method, &(*entry_ptr).value)
                        .ok_or(Error::LockUnavailable)?
                };

                if guard.is_some() {
                    Ok(EntryReadGuard {
                        bucket,
                        entry: unsafe { &*entry_ptr },
                        guard: Some(guard),
                    })
                } else {
                    Err(Error::NoEntry.into())
                }
            }
        }
    }

    /// Query the Entry associated with key for writing.  On success a EntryWriteGuard
    /// protecting the looked up value is returned.  When a default constructor is configured
    /// (see `with_constructor()`) it tries to construct missing entries with
    /// `get_or_insert_mut()`. When that fails the constructors error is returned. Otherwise
    /// `Error::NoEntry` will be returned when the queried item is not in the cache.
    ///
    /// For locking methods see `get()`.
    pub fn get_mut<'a, M>(&'a self, method: M, key: &K) -> DynResult<EntryWriteGuard<K, V, N>>
    where
        M: 'a + RwLockMethod<'a, Option<V>>,
    {
        match &self.ctor {
            Some(ctor) => self.get_or_insert_mut(method, key, ctor),
            None => {
                let (bucket, entry_ptr) = self.query_entry(key)?;

                let guard = unsafe {
                    RwLockMethod::write(&method, &(*entry_ptr).value)
                        .ok_or(Error::LockUnavailable)?
                };

                if guard.is_some() {
                    Ok(EntryWriteGuard {
                        bucket,
                        entry: unsafe { &*entry_ptr },
                        guard: Some(guard),
                    })
                } else {
                    Err(Error::NoEntry.into())
                }
            }
        }
    }

    // queries an entry and detaches it from the LRU or creates a new one
    fn query_or_insert_entry(
        &self,
        key: &K,
    ) -> std::result::Result<
        (&Bucket<K, V>, *const Entry<K, V>),
        (
            &Bucket<K, V>,
            *const Entry<K, V>,
            MutexGuard<HashSet<Pin<Box<entry::Entry<K, V>>>>>,
        ),
    > {
        let bucket = &self.buckets[key.bucket_range::<N>()];
        let mut map_lock = bucket.lock_map();

        match map_lock.get(key) {
            Some(entry) if entry.value.read().is_none() => {
                let entry_ptr: *const Entry<K, V> = &**entry;
                Err((bucket, entry_ptr, map_lock))
            }
            Some(entry) => Ok((bucket, &**entry)),
            None => {
                let entry = Box::pin(Entry::new(key.clone()));
                let entry_ptr: *const Entry<K, V> = &*entry;
                map_lock.insert(entry);
                Err((bucket, entry_ptr, map_lock))
            }
        }
    }

    /// Tries to insert an entry with the given constructor.  Returns Ok(true) when the
    /// constructor was called, Ok(false) when and item is already present under the given key
    /// or an Err() in case the constructor failed.
    pub fn insert<F>(&self, key: &K, ctor: F) -> DynResult<bool>
    where
        F: FnOnce(&K) -> DynResult<V>,
    {
        match self.query_or_insert_entry(key) {
            Ok(_) => Ok(false),
            Err((bucket, entry_ptr, mut map_lock)) => {
                if self.lru_disabled.load(Ordering::Relaxed) == 0 {
                    bucket.maybe_evict(&mut map_lock);
                }

                // need write lock for the ctor, before releasing the map to avoid a race.
                let mut wguard = unsafe {
                    // Safety: Blocking can not fail
                    RwLockMethod::write(&Blocking, &(*entry_ptr).value).unwrap_unchecked()
                };

                // release the map_lock, we dont need it anymore
                drop(map_lock);

                // Put it into the lru list
                bucket.enlist_entry(bucket.lock_lru(), unsafe { &*entry_ptr });
                // but we have wguard here which allows us to construct the inner guts
                *wguard = Some(ctor(key)?);

                Ok(true)
            }
        }
    }

    /// Query an Entry for reading or construct it (atomically).
    ///
    /// For locking methods see `get()`.
    pub fn get_or_insert<'a, M, F>(
        &'a self,
        method: M,
        key: &K,
        ctor: F,
    ) -> DynResult<EntryReadGuard<K, V, N>>
    where
        F: FnOnce(&K) -> DynResult<V>,
        M: 'a + RwLockMethod<'a, Option<V>>,
    {
        match self.query_or_insert_entry(key) {
            Ok((bucket, entry_ptr)) => Ok(EntryReadGuard {
                bucket,
                entry: unsafe { &*entry_ptr },
                guard: unsafe {
                    Some(
                        RwLockMethod::read(&method, &(*entry_ptr).value)
                            .ok_or(Error::LockUnavailable)?,
                    )
                },
            }),
            Err((bucket, entry_ptr, mut map_lock)) => {
                if self.lru_disabled.load(Ordering::Relaxed) == 0 {
                    bucket.maybe_evict(&mut map_lock);
                }

                // need write lock for the ctor, before releasing the map to avoid a race.
                let mut wguard = unsafe {
                    // Safety: Blocking can not fail
                    RwLockMethod::write(&Blocking, &(*entry_ptr).value).unwrap_unchecked()
                };

                // release the map_lock, we dont need it anymore
                drop(map_lock);

                // but we have wguard here which allows us to constuct the inner guts
                *wguard = Some(ctor(key)?);

                // Finally downgrade the lock to a readlock and return the Entry
                Ok(EntryReadGuard {
                    bucket,
                    entry: unsafe { &*entry_ptr },
                    guard: Some(RwLockWriteGuard::downgrade(wguard)),
                })
            }
        }
    }

    /// Query an Entry for writing or construct it (atomically).
    ///
    /// For locking methods see `get()`.
    pub fn get_or_insert_mut<'a, M, F>(
        &'a self,
        method: M,
        key: &K,
        ctor: F,
    ) -> DynResult<EntryWriteGuard<K, V, N>>
    where
        F: FnOnce(&K) -> DynResult<V>,
        M: 'a + RwLockMethod<'a, Option<V>>,
    {
        match self.query_or_insert_entry(key) {
            Ok((bucket, entry_ptr)) => Ok(EntryWriteGuard {
                bucket,
                entry: unsafe { &*entry_ptr },
                guard: unsafe {
                    Some(
                        RwLockMethod::write(&method, &(*entry_ptr).value)
                            .ok_or(Error::LockUnavailable)?,
                    )
                },
            }),
            Err((bucket, entry_ptr, mut map_lock)) => {
                if self.lru_disabled.load(Ordering::Relaxed) == 0 {
                    bucket.maybe_evict(&mut map_lock);
                }

                // need write lock for the ctor, before releasing the map to avoid a race.
                let mut wguard = unsafe {
                    // Safety: Blocking can not fail
                    RwLockMethod::write(&Blocking, &(*entry_ptr).value).unwrap_unchecked()
                };

                // release the map_lock, we dont need it anymore
                drop(map_lock);

                // but we have wguard here which allows us to constuct the inner guts
                *wguard = Some(ctor(key)?);

                // Finally downgrade the lock to a readlock and return the Entry
                Ok(EntryWriteGuard {
                    bucket,
                    entry: unsafe { &*entry_ptr },
                    guard: Some(wguard),
                })
            }
        }
    }

    /// Removes an element from the cache. When the element is not in use it will become
    /// dropped immediately. When it is in use then the expire bit gets set, thus it will be
    /// evicted with priority.
    pub fn remove(&self, key: &K) {
        self.buckets[key.bucket_range::<N>()].remove(key);
    }

    /// Disable the LRU eviction. Can be called multiple times, every call should be paired
    /// with a 'enable_lru()' call to reenable the LRU finally. Failing to do so may keep the
    /// CacheDb filling up forever. However this might be intentional to disable the LRU
    /// expiration entirely.
    pub fn disable_lru_eviction(&self) -> &Self {
        self.lru_disabled.fetch_add(1, Ordering::Relaxed);
        self
    }

    /// Re-Enables the LRU eviction after it was disabled. every call must be preceeded by a call to
    /// 'disable_lru()'. Calling it without an matching 'disable_lru()' will panic with an integer underflow.
    pub fn enable_lru_eviction(&self) -> &Self {
        self.lru_disabled.fetch_sub(1, Ordering::Relaxed);
        self
    }

    /// Checks if the CacheDb has the given key stored. Note that this can be racy when other
    /// threads access the CacheDb at the same time.
    pub fn contains_key(&self, key: &K) -> bool {
        self.buckets[key.bucket_range::<N>()]
            .lock_map()
            .contains(key)
    }

    /// Registers a default constructor to the cachedb. When present cachedb.get() and
    /// cachedb.get_mut() will try to construct missing items.
    pub fn with_constructor(
        mut self,
        ctor: &'static (dyn Fn(&K) -> DynResult<V> + Sync + Send),
    ) -> Self {
        self.ctor = Some(ctor);
        self
    }

    /// Get some basic stats about utilization.  Returns a tuple of `(capacity, len, cached)`
    /// summed from all buckets.  The result are approximate values because other threads may
    /// modify the underlying cache at the same time.
    pub fn stats(&self) -> (usize, usize, usize) {
        let mut capacity: usize = 0;
        let mut len: usize = 0;
        let mut cached: usize = 0;
        for bucket in &self.buckets {
            let lock = bucket.lock_map();
            capacity += lock.capacity();
            len += lock.len();
            cached += bucket.cached.load(Ordering::Relaxed);
        }
        (capacity, len, cached)
    }

    /// The 'highwater' limit, thats the maximum number of elements each bucket may hold.
    /// When this is exceeded *unused* elements are evicted from the lru list.  Note that the
    /// number of elements *in use* can still exceed this limit.  For performance reasons this
    /// is per-bucket when exact accounting is needed use a one-shard cachedb.  Defaults to
    /// 'usize::MAX', means no upper limit is set.
    pub fn config_highwater(&self, highwater: usize) -> &Self {
        for bucket in &self.buckets {
            bucket.highwater.store(highwater, Ordering::Relaxed);
        }
        self
    }

    /// The 'cache_target' will only recalculated after this many inserts in a bucket. Should
    /// be in the lower hundreds. Defaults to `100`.
    pub fn config_target_cooldown(&self, target_cooldown: u32) -> &Self {
        for bucket in &self.buckets {
            bucket
                .target_cooldown
                .store(target_cooldown, Ordering::Relaxed);
        }
        self
    }

    /// Sets the lower limit for the 'cache_target' linear interpolation region. Some
    /// hundreds to thousands of entries are recommended. Should be less than
    /// 'max_capacity_limit'. Defaults to `1000`.
    pub fn config_min_capacity_limit(&self, min_capacity_limit: usize) -> &Self {
        for bucket in &self.buckets {
            // divide by N so that each bucket gets its share
            bucket
                .min_capacity_limit
                .store(min_capacity_limit / N, Ordering::Relaxed);
        }
        self
    }

    /// Sets the upper limit for the 'cache_target' linear interpolation region. The
    /// recommended value should be around the maximum expected number of entries. Defaults to
    /// `10000000`.
    pub fn config_max_capacity_limit(&self, max_capacity_limit: usize) -> &Self {
        for bucket in &self.buckets {
            // divide by N so that each bucket gets its share
            bucket
                .max_capacity_limit
                .store(max_capacity_limit / N, Ordering::Relaxed);
        }
        self
    }

    /// Sets the lower limit for the 'cache_target' in percent at 'max_capacity_limit'. Since
    /// when a high number of entries are stored it is desireable to have a lower percentage of
    /// cached items for wasting less memory. Note that this counts against the 'capacity' of
    /// the underlying container, not the stored entries. Recommended values are around 5%,
    /// but may vary on the access patterns. Should be lower than 'max_cache_percent'
    /// Defautls to `5%`.
    pub fn config_min_cache_percent(&self, min_cache_percent: u8) -> &Self {
        assert!(min_cache_percent <= 100);
        for bucket in &self.buckets {
            bucket
                .min_cache_percent
                .store(min_cache_percent, Ordering::Relaxed);
        }
        self
    }

    /// Sets the upper limit for the 'cache_target' in percent at 'min_capacity_limit'. When
    /// only few entries are stored in a CacheDb it is reasonable to use a lot space for
    /// caching. Note that this counts against the 'capacity' of the underlying container,
    /// thus it should be not significantly over 60% at most. Defaults to `60%`.
    pub fn config_max_cache_percent(&self, max_cache_percent: u8) -> &Self {
        assert!(max_cache_percent <= 100);
        for bucket in &self.buckets {
            bucket
                .max_cache_percent
                .store(max_cache_percent, Ordering::Relaxed);
        }
        self
    }

    /// Sets the number of entries removed at once when evicting entries from the cache. Since
    /// evicting branches into the code parts for removing the entries and calling their
    /// destructors it is a bit more cache friendly to batch a few such things together.
    /// Defaults to `16`.
    pub fn config_evict_batch(&self, evict_batch: u8) -> &Self {
        for bucket in &self.buckets {
            bucket.evict_batch.store(evict_batch, Ordering::Relaxed);
        }
        self
    }

    /// Evicts up to number entries. The implementation is pretty simple trying to evict number/N from
    /// each bucket. Thus when the distribution is not optimal fewer elements will be removed.
    /// Will not remove any entries when the lru eviction is disabled.
    /// Returns the number of items that got evicted.
    pub fn evict(&self, number: usize) -> usize {
        if self.lru_disabled.load(Ordering::Relaxed) == 0 {
            let mut evicted = number;
            for bucket in &self.buckets {
                evicted -= bucket.evict(number / N, &mut bucket.lock_map());
            }
            evicted
        } else {
            0
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

/// Result type that boxes the error. Allows constructors to return arbitrary errors.
pub type DynResult<T> = std::result::Result<T, Box<dyn std::error::Error>>;

/// The errors that CacheDb implements itself. Note that the constructors can return other
/// errors as well ('DynResult' is returned in those case).
#[derive(Debug)]
pub enum Error {
    /// The Entry was not found
    NoEntry,
    /// Locking an entry failed
    LockUnavailable,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::NoEntry => write!(f, "Entry not found"),
            Error::LockUnavailable => write!(f, "Trying to lock failed"),
        }
    }
}

impl std::error::Error for Error {}

#[cfg(test)]
mod test {
    use std::collections::HashMap;
    use std::env;
    use std::sync::{Arc, Barrier};
    use std::{thread, time};
    #[cfg(feature = "logging")]
    use std::io::Write;

    use rand::Rng;

    use crate::*;

    #[cfg(feature = "logging")]
    fn init() {
        static LOGGER: std::sync::Once = std::sync::Once::new();

        let counter = std::sync::atomic::AtomicU64::new(0);
        let seq_num = move || counter.fetch_add(1, Ordering::SeqCst);

        LOGGER.call_once(|| {
            env_logger::Builder::from_default_env()
                .format(move |buf, record| {
                    writeln!(
                        buf,
                        "{:0>12}: {:>5}: {}:{}: {}: {}",
                        seq_num(),
                        record.level().as_str(),
                        record.file().unwrap_or(""),
                        record.line().unwrap_or(0),
                        std::thread::current().name().unwrap_or("UNKNOWN"),
                        record.args()
                    )
                })
                .try_init()
                .unwrap();
        });

        init_segv_handler();
    }

    #[cfg(not(feature = "logging"))]
    fn init() {
        init_segv_handler();
    }

    fn init_segv_handler() {
        use libc::*;
        unsafe extern "C" fn handler(signum: c_int) {
            let mut sigs = std::mem::MaybeUninit::uninit();
            sigemptyset(sigs.as_mut_ptr());
            sigaddset(sigs.as_mut_ptr(), signum);
            sigprocmask(SIG_UNBLOCK, sigs.as_ptr(), std::ptr::null_mut());
            panic!("SEGV!");
        }
        unsafe {
            signal(SIGSEGV, handler as sighandler_t);
        }
    }

    // using the default hash based implementation for tests here
    impl Bucketize for String {}
    impl Bucketize for u16 {
        fn bucket(&self) -> usize {
            let r = *self as usize;
            r
        }
    }

    impl KeyTraits for String {}
    impl KeyTraits for u16 {}

    #[test]
    fn create() {
        init();
        let cdb = CacheDb::<String, String, 16>::new();

        println!("Debug {:?}", &cdb);
        assert!(cdb.get(Blocking, &"foo".to_string()).is_err());
    }

    #[test]
    fn insert_is_caching() {
        init();
        let cdb = CacheDb::<String, String, 16>::new();

        assert!(
            cdb.insert(&"foo".to_string(), |_| Ok("bar".to_string()))
                .unwrap()
        );

        let (_capacity, len, cached) = cdb.stats();
        assert_eq!(len, 1);
        assert_eq!(cached, 1);
    }

    #[test]
    fn drop_is_caching() {
        init();
        let cdb = CacheDb::<String, String, 16>::new();

        let entry = cdb
            .get_or_insert(Blocking, &"foo".to_string(), |_| Ok("bar".to_string()))
            .unwrap();

        let (_capacity, len, cached) = cdb.stats();
        assert_eq!(len, 1);
        assert_eq!(cached, 0);

        drop(entry);

        let (_capacity, len, cached) = cdb.stats();
        assert_eq!(len, 1);
        assert_eq!(cached, 1);
    }

    #[test]
    fn get_removes_from_cache() {
        init();
        let cdb = CacheDb::<String, String, 16>::new();

        cdb.insert(&"foo".to_string(), |_| Ok("bar".to_string()))
            .unwrap();

        let entry = cdb.get(Blocking, &"foo".to_string()).unwrap();

        let (_capacity, len, cached) = cdb.stats();
        assert_eq!(len, 1);
        assert_eq!(cached, 0);
        assert_eq!(*entry, "bar".to_string());
    }

    #[test]
    fn reget_removes_from_cache_again() {
        init();
        let cdb = CacheDb::<String, String, 16>::new();

        let entry = cdb
            .get_or_insert(Blocking, &"foo".to_string(), |_| Ok("bar".to_string()))
            .unwrap();

        let (_capacity, len, cached) = cdb.stats();
        assert_eq!(len, 1);
        assert_eq!(cached, 0);
        assert_eq!(*entry, "bar".to_string());

        drop(entry);

        let (_capacity, len, cached) = cdb.stats();
        assert_eq!(len, 1);
        assert_eq!(cached, 1);

        let entry = cdb.get(Blocking, &"foo".to_string()).unwrap();

        let (_capacity, len, cached) = cdb.stats();
        assert_eq!(len, 1);
        assert_eq!(cached, 0);
        assert_eq!(*entry, "bar".to_string());
    }

    #[test]
    fn insert_foobar_onebucket() {
        init();
        let cdb = CacheDb::<String, String, 1>::new();

        assert!(
            cdb.get_or_insert(Blocking, &"foo".to_string(), |_| Ok("bar".to_string()))
                .is_ok()
        );
        assert_eq!(
            *cdb.get(Blocking, &"foo".to_string()).unwrap(),
            "bar".to_string()
        );
        assert!(
            cdb.get_or_insert(Blocking, &"bar".to_string(), |_| Ok("foo".to_string()))
                .is_ok()
        );
        assert_eq!(
            *cdb.get(Blocking, &"bar".to_string()).unwrap(),
            "foo".to_string()
        );
        assert!(
            cdb.get_or_insert(Blocking, &"foo2".to_string(), |_| Ok("bar2".to_string()))
                .is_ok()
        );
        assert_eq!(
            *cdb.get(Blocking, &"foo2".to_string()).unwrap(),
            "bar2".to_string()
        );
        assert!(
            cdb.get_or_insert(Blocking, &"bar2".to_string(), |_| Ok("foo2".to_string()))
                .is_ok()
        );
        assert_eq!(
            *cdb.get(Blocking, &"bar2".to_string()).unwrap(),
            "foo2".to_string()
        );
    }

    #[test]
    fn caching_works() {
        init();
        let cdb = CacheDb::<String, String, 1>::new();

        let entry = cdb
            .get_or_insert(Blocking, &"foo".to_string(), |_| Ok("bar".to_string()))
            .unwrap();

        let (_capacity, len, cached) = cdb.stats();
        assert_eq!(len, 1);
        assert_eq!(cached, 0);

        let entry_again = cdb.get(Blocking, &"foo".to_string()).unwrap();

        let (_capacity, len, cached) = cdb.stats();
        assert_eq!(len, 1);
        assert_eq!(cached, 0);

        drop(entry);
        let (_capacity, len, cached) = cdb.stats();
        assert_eq!(len, 1);
        assert_eq!(cached, 0);

        drop(entry_again); // all references dropped, should be cached now

        let (_capacity, len, cached) = cdb.stats();
        assert_eq!(len, 1);
        assert_eq!(cached, 1);
    }

    #[test]
    fn insert_foobar() {
        init();
        let cdb = CacheDb::<String, String, 16>::new();

        assert!(
            cdb.get_or_insert(Blocking, &"foo".to_string(), |_| Ok("bar".to_string()))
                .is_ok()
        );
        assert_eq!(
            *cdb.get(Blocking, &"foo".to_string()).unwrap(),
            "bar".to_string()
        );
        assert!(
            cdb.get_or_insert(Blocking, &"bar".to_string(), |_| Ok("foo".to_string()))
                .is_ok()
        );
        assert_eq!(
            *cdb.get(Blocking, &"bar".to_string()).unwrap(),
            "foo".to_string()
        );
        assert!(
            cdb.get_or_insert(Blocking, &"foo2".to_string(), |_| Ok("bar2".to_string()))
                .is_ok()
        );
        assert_eq!(
            *cdb.get(Blocking, &"foo2".to_string()).unwrap(),
            "bar2".to_string()
        );
        assert!(
            cdb.get_or_insert(Blocking, &"bar2".to_string(), |_| Ok("foo2".to_string()))
                .is_ok()
        );
        assert_eq!(
            *cdb.get(Blocking, &"bar2".to_string()).unwrap(),
            "foo2".to_string()
        );
    }

    #[test]
    fn insert_unit() {
        init();
        let cdb = CacheDb::<String, (), 16>::new();

        assert!(cdb.insert(&"foo".to_string(), |_| Ok(())).is_ok());
        assert_eq!(*cdb.get(Blocking, &"foo".to_string()).unwrap(), ());

        assert!(cdb.insert(&"bar".to_string(), |_| Ok(())).is_ok());
        assert_eq!(*cdb.get(Blocking, &"bar".to_string()).unwrap(), ());

        assert_eq!(cdb.contains_key(&"foo".to_string()), true);
        assert_eq!(cdb.contains_key(&"bar".to_string()), true);
        assert_eq!(cdb.contains_key(&"baz".to_string()), false);
    }

    #[test]
    fn trylocks() {
        init();
        let cdb = CacheDb::<String, String, 16>::new();

        assert!(
            cdb.get_or_insert(Blocking, &"foo".to_string(), |_| Ok("bar".to_string()))
                .is_ok()
        );
        assert_eq!(
            *cdb.get(TryLock, &"foo".to_string()).unwrap(),
            "bar".to_string()
        );
        assert_eq!(
            *cdb.get(Duration::from_millis(100), &"foo".to_string())
                .unwrap(),
            "bar".to_string()
        );
        assert_eq!(
            *cdb.get(
                Instant::now() + Duration::from_millis(100),
                &"foo".to_string()
            )
            .unwrap(),
            "bar".to_string()
        );
    }

    #[test]
    fn recursivelocks() {
        init();
        let cdb = CacheDb::<String, String, 16>::new();

        assert!(
            cdb.get_or_insert(Blocking, &"foo".to_string(), |_| Ok("bar".to_string()))
                .is_ok()
        );

        let l1 = cdb.get(Recursive(Blocking), &"foo".to_string()).unwrap();
        assert_eq!(*l1, "bar".to_string());

        let l2 = cdb.get(Recursive(TryLock), &"foo".to_string()).unwrap();
        assert_eq!(*l2, "bar".to_string());

        let l3 = cdb
            .get(Recursive(Duration::from_millis(100)), &"foo".to_string())
            .unwrap();
        assert_eq!(*l3, "bar".to_string());

        let l4 = cdb
            .get(
                Recursive(Instant::now() + Duration::from_millis(100)),
                &"foo".to_string(),
            )
            .unwrap();
        assert_eq!(*l4, "bar".to_string());
    }

    #[test]
    fn mutate() {
        init();
        let cdb = CacheDb::<String, String, 16>::new();

        cdb.get_or_insert(Blocking, &"foo".to_string(), |_| Ok("bar".to_string()))
            .unwrap();

        *cdb.get_mut(Blocking, &"foo".to_string()).unwrap() = "baz".to_string();
        assert_eq!(
            *cdb.get(Blocking, &"foo".to_string()).unwrap(),
            "baz".to_string()
        );
    }

    #[test]
    fn insert_mutate() {
        init();
        let cdb = CacheDb::<String, String, 16>::new();

        let mut foo = cdb
            .get_or_insert_mut(Blocking, &"foo".to_string(), |_| Ok("bar".to_string()))
            .unwrap();
        assert_eq!(*foo, "bar".to_string());
        *foo = "baz".to_string();
        assert_eq!(*foo, "baz".to_string());
        drop(foo);
        assert_eq!(
            *cdb.get(Blocking, &"foo".to_string()).unwrap(),
            "baz".to_string()
        );
    }

    #[test]
    fn failing_ctor() {
        init();

        #[derive(Debug)]
        struct TestError(&'static str);
        impl std::error::Error for TestError {}
        impl std::fmt::Display for TestError {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
                write!(f, "{}", self.0)
            }
        }

        fn failing_ctor(_key: &String) -> DynResult<String> {
            Err(Box::new(TestError("nope")))
        }

        fn successing_ctor(_key: &String) -> DynResult<String> {
            Ok("value".to_string())
        }

        let cdb = CacheDb::<String, String, 16>::new();

        assert!(cdb.insert(&"key".to_string(), failing_ctor).is_err());

        // fail again
        assert!(cdb.insert(&"key".to_string(), failing_ctor).is_err());

        // get fails too
        assert!(cdb.get(Blocking, &"key".to_string()).is_err());

        // get_or_insert fails too
        assert!(
            cdb.get_or_insert(Blocking, &"key".to_string(), failing_ctor)
                .is_err()
        );

        // succeeding ctor
        assert!(cdb.insert(&"key".to_string(), successing_ctor).is_ok());

        // get success
        assert!(cdb.get(Blocking, &"key".to_string()).is_ok());
    }

    #[test]
    fn exact_highwater() {
        init();

        // example about an cache limited at exact highwater level.

        // only one bucket for precise accounting
        let cdb = CacheDb::<String, String, 1>::new();

        cdb
        // no more than 1000 passive entries
            .config_highwater(1000)
        // check for limit on *every* insert and evict only one as well
            .config_target_cooldown(1)
            .config_evict_batch(1)
        // set limits to allow 100% cache utilization.
            .config_min_capacity_limit(1000)
            .config_max_cache_percent(100)
            .config_max_capacity_limit(1000)
            .config_min_cache_percent(100);

        // insert 1000 things should make them all cached
        for n in 0..1000 {
            let _ = cdb.insert(&format!("key_{}", n), |key| Ok(format!("value_of_{}", key)));
        }

        // They are still cached
        for n in 0..1000 {
            assert!(cdb.get(Blocking, &format!("key_{}", n)).is_ok());
        }

        let (_capacity, _len, cached) = cdb.stats();
        assert_eq!(cached, 1000);

        // adding one excess element will now drop the oldest
        let _ = cdb.insert(&String::from("key_1001"), |_| {
            Ok(String::from("1001'st element"))
        });
        let (_capacity, _len, cached) = cdb.stats();
        assert_eq!(cached, 1000);

        assert_eq!(cdb.contains_key(&String::from("key_0")), false);
    }

    #[test]
    fn remove() {
        init();
        let cdb = CacheDb::<String, String, 16>::new();

        // insert one element
        let _ = cdb.insert(&String::from("element"), |_| Ok(String::from("value")));

        let (_capacity, _len, cached) = cdb.stats();
        assert_eq!(cached, 1);

        // remove it
        cdb.remove(&String::from("element"));
        let (_capacity, _len, cached) = cdb.stats();
        assert_eq!(cached, 0);
    }

    #[test]
    fn downgrade() {
        init();
        let cdb = CacheDb::<String, String, 16>::new();

        let mut writeguard = cdb
            .get_or_insert_mut(Blocking, &String::from("element"), |_| {
                Ok(String::from("value"))
            })
            .unwrap();

        *writeguard = String::from("newvalue");

        let readguard = writeguard.downgrade();

        assert_eq!(*readguard, "newvalue");
    }

    #[test]
    fn registered_ctor() {
        init();
        let cdb = CacheDb::<String, (), 16>::new().with_constructor(&|_| Ok(()));
        assert_eq!(*cdb.get(Blocking, &"foo".to_string()).unwrap(), ());
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
        for thread_num in 0..num_threads {
            let c = Arc::clone(&barrier);
            let cdb = Arc::clone(&cdb);

            handles.push(
                thread::Builder::new()
                    .name(thread_num.to_string())
                    .spawn(
                        // The per thread function
                        move || {
                            let mut rng = rand::thread_rng();
                            c.wait();

                            let mut locked = HashMap::<u16, EntryReadGuard<u16, u16, BUCKETS>>::new();
                            let mut maxlocked: u16 = 0;

                            for _ in 0..iterations {
                                // r is the key we handle
                                let r = rng.gen_range(0..range);
                                // p is the probability of some operation
                                let p = rng.gen_range(0..100);
                                // w is the wait time to simulate thread work
                                let w = if wait_millis > 0 {
                                    Some(time::Duration::from_millis(rng.gen_range(0..wait_millis)))
                                } else {
                                    None
                                };
                                match locked.remove(&r) {
                                    // thread had no lock stored, create a new entry
                                    None => {
                                        if p < 15 {
                                            // TODO: remove
                                        } else if p < 30 {
                                            // TODO: touch
                                        } else if p < 50 {
                                            // #[cfg(feature = "logging")]
                                            // trace!("get_or_insert {} and keep it", r);
                                            // locked.insert(
                                            //     r,
                                            //     cdb.get_or_insert(&r, |_| Ok(!r)).unwrap(),
                                            // );
                                            // #[cfg(feature = "logging")]
                                            // trace!("got {}", r);
                                        } else if p < 55 {
                                            if r > maxlocked {
                                                maxlocked = r;
                                                #[cfg(feature = "logging")]
                                                trace!("get_or_insert_mut {} and then wait/work for {:?}", r, w);
                                                let lock =
                                                    cdb.get_or_insert_mut(Duration::from_millis(500), &r, |_| Ok(!r));
                                                #[cfg(feature = "logging")]
                                                trace!("got {}", r);
                                                if let Some(w) = w {
                                                    thread::sleep(w)
                                                }
                                                drop(lock);
                                            } else {
                                                maxlocked = 0;
                                                #[cfg(feature = "logging")]
                                                trace!("drop all stored locks");
                                                locked.clear();
                                            }
                                        } else if p < 60 {
                                            #[cfg(feature = "logging")]
                                            trace!("wait/work for {:?} and then get_or_insert_mut {}", w, r);
                                            if let Some(w) = w {
                                                thread::sleep(w)
                                            }
                                            let lock =
                                                cdb.get_or_insert_mut(Duration::from_millis(500), &r, |_| Ok(!r));
                                            #[cfg(feature = "logging")]
                                            trace!("got {}", r);
                                            drop(lock);
                                        } else if p < 80 {
                                            #[cfg(feature = "logging")]
                                            trace!("get_or_insert {} and then wait/work for {:?}", r, w);
                                            let lock = cdb.get_or_insert(Blocking, &r, |_| Ok(!r)).unwrap();
                                            #[cfg(feature = "logging")]
                                            trace!("got {}", r);
                                            if let Some(w) = w {
                                                thread::sleep(w)
                                            }
                                            drop(lock);
                                        } else {
                                            #[cfg(feature = "logging")]
                                            trace!("wait/work for {:?} and then get_or_insert {}", w, r);
                                            if let Some(w) = w {
                                                thread::sleep(w)
                                            }
                                            let lock = cdb.get_or_insert(Blocking, &r, |_| Ok(!r)).unwrap();
                                            #[cfg(feature = "logging")]
                                            trace!("got {}", r);
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
                                            drop(read_guard);
                                        }
                                    }
                                };
                            }
                            drop(locked);
                        },
                    )
                    .unwrap(),
            );
        }

        // TODO: finally assert that nothing is locked

        for handle in handles {
            handle.join().unwrap();
        }
    }
}

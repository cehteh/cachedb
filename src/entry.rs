//use std::sync::atomic::{AtomicUsize, Ordering};
use std::fmt::Debug;
use std::marker::PhantomPinned;
use std::ops::Deref;

use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::{Bucketize, CacheDb};

/// User data is stored behind RwLocks in an entry. Furthermore some management information
/// like the LRU list node are stored here. Entries have stable addresses and can't be moved
/// in memory.
#[derive(Debug)]
pub(crate) struct Entry<V> {
    // PLANNED: implement atomic lock transititon between two locks (as is, waiting on the rwlock will block the hashmap)
    // The Option is only used for delaying the construction.
    pub(crate) data: RwLock<Option<V>>,
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

/// RAII Guard for the read lock. Manages to put unused entries into the LRU list.
#[derive(Debug)]
pub struct EntryReadGuard<'a, K, V, const N: usize>
where
    K: Eq + Clone + Bucketize + Debug,
{
    pub(crate) cachedb: &'a CacheDb<K, V, N>,
    pub(crate) entry: &'a Entry<V>,
    pub(crate) guard: RwLockReadGuard<'a, Option<V>>,
}

impl<'a, K, V, const N: usize> Drop for EntryReadGuard<'_, K, V, N>
where
    K: Eq + Clone + Bucketize + Debug,
{
    fn drop(&mut self) {
        #[cfg(feature = "logging")]
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
        (*self.guard).as_ref().unwrap()
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
        #[cfg(feature = "logging")]
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

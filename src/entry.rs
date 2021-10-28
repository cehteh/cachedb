use std::sync::atomic::AtomicUsize;
use std::fmt::Debug;
use std::marker::PhantomPinned;
use std::ops::Deref;
use std::pin::Pin;
use std::hash::{Hash, Hasher};
use std::borrow::Borrow;

use intrusive_collections::{intrusive_adapter, LinkedListLink, UnsafeRef};
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::{bucket::Bucket, Bucketize};

/// Collects the traits a Key must implement, any user defined Key type must implement this
/// trait and any traits it derives from.
/// The 'Debug' trait is only required when the feature 'logging' is enabled.
#[cfg(feature = "logging")]
pub trait KeyTraits: Eq + Clone + Bucketize + Debug {}
#[cfg(not(feature = "logging"))]
pub trait KeyTraits: Eq + Clone + Bucketize {}

/// User data is stored behind RwLocks in an entry. Furthermore some management information
/// like the LRU list node are stored here. Entries have stable addresses and can't be moved
/// in memory.
#[derive(Debug)]
pub(crate) struct Entry<K, V> {
    // PLANNED: implement atomic lock transititon between two locks (as is, waiting on the rwlock will block the hashmap)
    // The Option is only used for delaying the construction.
    pub(crate) key:       K,
    pub(crate) value:     RwLock<Option<V>>,
    pub(crate) lru_link:  LinkedListLink, // protected by lru_list mutex
    pub(crate) use_count: AtomicUsize,
    _pin:                 PhantomPinned,
}

intrusive_adapter!(pub(crate) EntryAdapter<K, V> = UnsafeRef<Entry<K, V>>: Entry<K, V> { lru_link: LinkedListLink });

impl<K: KeyTraits, V> Entry<K, V> {
    pub(crate) fn new(key: K) -> Self {
        Entry {
            key,
            value: RwLock::new(None),
            lru_link: LinkedListLink::new(),
            use_count: AtomicUsize::new(1),
            _pin: PhantomPinned,
        }
    }
}

// Hashes only over the key part.
impl<K: KeyTraits, V> Hash for Entry<K, V> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.key.hash(state);
    }
}

// Compares only the key.
impl<K: PartialEq, V> PartialEq for Entry<K, V> {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl<K: PartialEq, V> Eq for Entry<K, V> {}

// We need this to be able to lookup a Key in a HashSet containing pinboxed entries.
impl<K, V> Borrow<K> for Pin<Box<Entry<K, V>>>
where
    K: KeyTraits,
{
    fn borrow(&self) -> &K {
        &self.key
    }
}

/// RAII Guard for the read lock. Manages to put unused entries into the LRU list.
#[derive(Debug)]
pub struct EntryReadGuard<'a, K, V, const N: usize>
where
    K: KeyTraits,
{
    pub(crate) bucket: &'a Bucket<K, V>,
    pub(crate) entry:  &'a Entry<K, V>,
    pub(crate) guard:  RwLockReadGuard<'a, Option<V>>,
}

impl<'a, K, V, const N: usize> Drop for EntryReadGuard<'_, K, V, N>
where
    K: KeyTraits,
{
    fn drop(&mut self) {
        self.bucket.unuse_entry(self.entry);
    }
}

impl<'a, K, V, const N: usize> Deref for EntryReadGuard<'_, K, V, N>
where
    K: KeyTraits,
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
    K: KeyTraits,
{
    pub(crate) bucket: &'a Bucket<K, V>,
    pub(crate) entry:  &'a Entry<K, V>,
    guard:             RwLockWriteGuard<'a, V>,
}

impl<'a, K, V, const N: usize> Drop for EntryWriteGuard<'_, K, V, N>
where
    K: KeyTraits,
{
    fn drop(&mut self) {
        self.bucket.unuse_entry(self.entry);
    }
}

impl<'a, K, V, const N: usize> Deref for EntryWriteGuard<'_, K, V, N>
where
    K: KeyTraits,
{
    type Target = V;

    fn deref(&self) -> &Self::Target {
        &(*self.guard)
    }
}

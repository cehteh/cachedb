use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(feature = "logging")]
use std::fmt::Debug;
use std::marker::PhantomPinned;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::hash::{Hash, Hasher};
use std::borrow::Borrow;

use intrusive_collections::{intrusive_adapter, LinkedListLink, UnsafeRef};
use parking_method::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::{bucket::Bucket, Bucketize};

/// Collects the traits a Key must implement, any user defined Key type must implement this
/// trait and any traits it derives from.
/// The 'Debug' trait is only required when the feature 'logging' is enabled.
#[cfg(feature = "logging")]
pub trait KeyTraits: Eq + Clone + Bucketize + Debug + 'static {}
#[cfg(not(feature = "logging"))]
#[allow(missing_docs)]
pub trait KeyTraits: Eq + Clone + Bucketize + 'static {}

/// User data is stored behind RwLocks in an entry. Furthermore some management information
/// like the LRU list node are stored here. Entries have stable addresses and can't be moved
/// in memory.
pub(crate) struct Entry<K, V> {
    pub(crate) key:      K,
    // The Option is used for delaying the construction with write lock held as well when
    // leaving stale objects behind when the ctor failed. Such stale objects will expire first
    // and never be handed to the user.
    pub(crate) value:    RwLock<Option<V>>,
    pub(crate) lru_link: LinkedListLink, // protected by lru_list mutex
    pub(crate) expire:   AtomicBool,
    _pin:                PhantomPinned,
}

intrusive_adapter!(pub(crate) EntryAdapter<K, V> = UnsafeRef<Entry<K, V>>: Entry<K, V> { lru_link: LinkedListLink });

impl<K: KeyTraits, V> Entry<K, V> {
    pub(crate) fn new(key: K) -> Self {
        Entry {
            key,
            value: RwLock::new(None),
            lru_link: LinkedListLink::new(),
            expire: AtomicBool::new(false),
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

/// Guard for the read lock. Puts unused entries into the LRU list.
pub struct EntryReadGuard<'a, K, V, const N: usize>
where
    K: KeyTraits,
{
    pub(crate) bucket: &'a Bucket<K, V>,
    pub(crate) entry:  &'a Entry<K, V>,
    pub(crate) guard:  Option<RwLockReadGuard<'a, Option<V>>>,
}

impl<K, V, const N: usize> EntryReadGuard<'_, K, V, N>
where
    K: KeyTraits,
{
    /// Mark the entry for expiration. When dropped it will be put in front of the LRU list
    /// and by that evicted soon. Use with care, when many entries become pushed to the front,
    /// they eventually bubble up again.
    pub fn expire(&mut self) {
        self.entry.expire.store(true, Ordering::Relaxed);
    }
}

impl<K, V, const N: usize> Drop for EntryReadGuard<'_, K, V, N>
where
    K: KeyTraits,
{
    fn drop(&mut self) {
        let lru_lock = self.bucket.lock_lru();
        unsafe {
            debug_assert!(self.guard.is_some());
            drop(self.guard.take().unwrap_unchecked());
        }
        self.bucket.unuse_entry(lru_lock, self.entry);
    }
}

impl<K, V, const N: usize> Deref for EntryReadGuard<'_, K, V, N>
where
    K: KeyTraits,
{
    type Target = V;

    fn deref(&self) -> &Self::Target {
        unsafe {
            debug_assert!(self.guard.is_some());
            let guard = self.guard.as_ref().unwrap_unchecked();

            debug_assert!(guard.is_some());
            guard.as_ref().unwrap_unchecked()
        }
    }
}

/// Guard for the write lock. Puts unused entries into the LRU list.
pub struct EntryWriteGuard<'a, K, V, const N: usize>
where
    K: KeyTraits,
{
    pub(crate) bucket: &'a Bucket<K, V>,
    pub(crate) entry:  &'a Entry<K, V>,
    pub(crate) guard:  Option<RwLockWriteGuard<'a, Option<V>>>,
}

impl<'a, K, V, const N: usize> EntryWriteGuard<'a, K, V, N>
where
    K: KeyTraits,
{
    /// Mark the entry for expiration. When dropped it will be put in front of the LRU list
    /// and by that evicted soon. Use with care, when many entries become pushed to the front,
    /// they eventually bubble up again.
    pub fn expire(&mut self) {
        self.entry.expire.store(true, Ordering::Relaxed);
    }

    /// Downgrade a write lock to a read lock without releasing it.
    pub fn downgrade(mut self) -> EntryReadGuard<'a, K, V, N> {
        debug_assert!(self.guard.is_some());

        let bucket = self.bucket;
        let entry = self.entry;
        let guard = Some(RwLockWriteGuard::downgrade(unsafe {
            self.guard.take().unwrap_unchecked()
        }));

        // We must not run the destructor of the gutted out write lock
        std::mem::forget(self);

        EntryReadGuard {
            bucket,
            entry,
            guard,
        }
    }
}

impl<K, V, const N: usize> Drop for EntryWriteGuard<'_, K, V, N>
where
    K: KeyTraits,
{
    fn drop(&mut self) {
        let lru_lock = self.bucket.lock_lru();
        unsafe {
            debug_assert!(self.guard.is_some());
            drop(self.guard.take().unwrap_unchecked());
        }
        self.bucket.unuse_entry(lru_lock, self.entry);
    }
}

impl<K, V, const N: usize> Deref for EntryWriteGuard<'_, K, V, N>
where
    K: KeyTraits,
{
    type Target = V;

    fn deref(&self) -> &Self::Target {
        unsafe {
            debug_assert!(self.guard.is_some());
            let guard = self.guard.as_ref().unwrap_unchecked();

            debug_assert!(guard.is_some());
            guard.as_ref().unwrap_unchecked()
        }
    }
}

impl<K, V, const N: usize> DerefMut for EntryWriteGuard<'_, K, V, N>
where
    K: KeyTraits,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe {
            debug_assert!(self.guard.is_some());
            let guard = self.guard.as_mut().unwrap_unchecked();

            debug_assert!(guard.is_some());
            guard.as_mut().unwrap_unchecked()
        }
    }
}

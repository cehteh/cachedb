//! There are plenty flavors on how a lock can be obtained. The normal blocking way, trying to
//! obtain a lock, possibly with timeouts, allow a thread to lock a single RwLock multiple
//! times. These are (zero-cost) abstracted here.
pub use std::time::{Duration, Instant};

use crate::Error;

/// Blocking waits until the lock is obtained.
pub struct Blocking;
/// Try to lock or return an error if the lock is not available.
pub struct TryLock;
/// Allows a thread to lock a single RwLock multiple times.
pub struct Recursive<T>(pub T);
// PLANNED: Async<T>(pub T)

/// The locking methods for reading
pub trait LockingMethod<'a, V> {
    fn read(
        &self,
        rwlock: &'a parking_lot::RwLock<Option<V>>,
    ) -> Result<parking_lot::RwLockReadGuard<'a, Option<V>>, Error>;
    fn write(
        &self,
        rwlock: &'a parking_lot::RwLock<Option<V>>,
    ) -> Result<parking_lot::RwLockWriteGuard<'a, Option<V>>, Error>;
}

impl<'a, V> LockingMethod<'a, V> for Blocking {
    #[inline(always)]
    fn read(
        &self,
        rwlock: &'a parking_lot::RwLock<Option<V>>,
    ) -> Result<parking_lot::RwLockReadGuard<'a, Option<V>>, Error> {
        Ok(rwlock.read())
    }

    #[inline(always)]
    fn write(
        &self,
        rwlock: &'a parking_lot::RwLock<Option<V>>,
    ) -> Result<parking_lot::RwLockWriteGuard<'a, Option<V>>, Error> {
        Ok(rwlock.write())
    }
}

impl<'a, V> LockingMethod<'a, V> for TryLock {
    #[inline(always)]
    fn read(
        &self,
        rwlock: &'a parking_lot::RwLock<Option<V>>,
    ) -> Result<parking_lot::RwLockReadGuard<'a, Option<V>>, Error> {
        rwlock.try_read().ok_or(Error::LockUnavailable)
    }

    #[inline(always)]
    fn write(
        &self,
        rwlock: &'a parking_lot::RwLock<Option<V>>,
    ) -> Result<parking_lot::RwLockWriteGuard<'a, Option<V>>, Error> {
        rwlock.try_write().ok_or(Error::LockUnavailable)
    }
}

/// Tries to obtain the lock within a timeout.
impl<'a, V> LockingMethod<'a, V> for Duration {
    #[inline(always)]
    fn read(
        &self,
        rwlock: &'a parking_lot::RwLock<Option<V>>,
    ) -> Result<parking_lot::RwLockReadGuard<'a, Option<V>>, Error> {
        rwlock.try_read_for(*self).ok_or(Error::LockUnavailable)
    }

    #[inline(always)]
    fn write(
        &self,
        rwlock: &'a parking_lot::RwLock<Option<V>>,
    ) -> Result<parking_lot::RwLockWriteGuard<'a, Option<V>>, Error> {
        rwlock.try_write_for(*self).ok_or(Error::LockUnavailable)
    }
}

/// Tries to obtain the lock until a target time expired.
impl<'a, V> LockingMethod<'a, V> for Instant {
    #[inline(always)]
    fn read(
        &self,
        rwlock: &'a parking_lot::RwLock<Option<V>>,
    ) -> Result<parking_lot::RwLockReadGuard<'a, Option<V>>, Error> {
        rwlock.try_read_until(*self).ok_or(Error::LockUnavailable)
    }

    #[inline(always)]
    fn write(
        &self,
        rwlock: &'a parking_lot::RwLock<Option<V>>,
    ) -> Result<parking_lot::RwLockWriteGuard<'a, Option<V>>, Error> {
        rwlock.try_write_until(*self).ok_or(Error::LockUnavailable)
    }
}

impl<'a, V> LockingMethod<'a, V> for Recursive<Blocking> {
    #[inline(always)]
    fn read(
        &self,
        rwlock: &'a parking_lot::RwLock<Option<V>>,
    ) -> Result<parking_lot::RwLockReadGuard<'a, Option<V>>, Error> {
        Ok(rwlock.read_recursive())
    }

    #[inline(always)]
    fn write(
        &self,
        rwlock: &'a parking_lot::RwLock<Option<V>>,
    ) -> Result<parking_lot::RwLockWriteGuard<'a, Option<V>>, Error> {
        Ok(rwlock.write())
    }
}

impl<'a, V> LockingMethod<'a, V> for Recursive<TryLock> {
    #[inline(always)]
    fn read(
        &self,
        rwlock: &'a parking_lot::RwLock<Option<V>>,
    ) -> Result<parking_lot::RwLockReadGuard<'a, Option<V>>, Error> {
        rwlock.try_read_recursive().ok_or(Error::LockUnavailable)
    }

    #[inline(always)]
    fn write(
        &self,
        rwlock: &'a parking_lot::RwLock<Option<V>>,
    ) -> Result<parking_lot::RwLockWriteGuard<'a, Option<V>>, Error> {
        rwlock.try_write().ok_or(Error::LockUnavailable)
    }
}

impl<'a, V> LockingMethod<'a, V> for Recursive<Duration> {
    #[inline(always)]
    fn read(
        &self,
        rwlock: &'a parking_lot::RwLock<Option<V>>,
    ) -> Result<parking_lot::RwLockReadGuard<'a, Option<V>>, Error> {
        rwlock
            .try_read_recursive_for(self.0)
            .ok_or(Error::LockUnavailable)
    }

    #[inline(always)]
    fn write(
        &self,
        rwlock: &'a parking_lot::RwLock<Option<V>>,
    ) -> Result<parking_lot::RwLockWriteGuard<'a, Option<V>>, Error> {
        rwlock.try_write_for(self.0).ok_or(Error::LockUnavailable)
    }
}

impl<'a, V> LockingMethod<'a, V> for Recursive<Instant> {
    #[inline(always)]
    fn read(
        &self,
        rwlock: &'a parking_lot::RwLock<Option<V>>,
    ) -> Result<parking_lot::RwLockReadGuard<'a, Option<V>>, Error> {
        rwlock
            .try_read_recursive_until(self.0)
            .ok_or(Error::LockUnavailable)
    }

    #[inline(always)]
    fn write(
        &self,
        rwlock: &'a parking_lot::RwLock<Option<V>>,
    ) -> Result<parking_lot::RwLockWriteGuard<'a, Option<V>>, Error> {
        rwlock.try_write_until(self.0).ok_or(Error::LockUnavailable)
    }
}

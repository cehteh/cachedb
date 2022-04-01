//! There are plenty flavors on how a lock can be obtained. The normal blocking way, trying to
//! obtain a lock, possibly with timeouts, allow a thread to lock a single RwLock multiple
//! times. These are (zero-cost) abstracted here.

pub use std::time::{Duration, Instant};

use crate::Error;

/// Marker for blocking locks,
/// waits until the Lock becomes available.
pub struct Blocking;

/// Marker for trying locks,
/// will error with 'LockUnavailable' when the lock can't be obtained.
pub struct TryLock;

/// Marker for recursive locking. Allows to obtain a read-lock multiple times by a single
/// thread.  Note that write locks will fall back to non recursive locking and may deadlock
/// when tried to be obtained recursively.
pub struct Recursive<T>(pub T);

// FIXME: how to generate Doc for Duration and Instant
// Tries to obtain the lock within a timeout.
// Tries to obtain the lock until a target time expired.

// PLANNED: Async<T>(pub T)

/// Trait for implementing read/write flavors of locking methods.
pub trait LockingMethod<'a, V> {
    // Obtain a read lock.
    fn read(
        &self,
        rwlock: &'a parking_lot::RwLock<Option<V>>,
    ) -> Result<parking_lot::RwLockReadGuard<'a, Option<V>>, Error>;

    // Obtain a write lock.
    fn write(
        &self,
        rwlock: &'a parking_lot::RwLock<Option<V>>,
    ) -> Result<parking_lot::RwLockWriteGuard<'a, Option<V>>, Error>;
}

macro_rules! impl_locking_method {
    ($policy:ty, $read:expr, $write:expr) => {
        impl<'a, V> LockingMethod<'a, V> for $policy {
            #[inline(always)]
            fn read(
                &self,
                rwlock: &'a parking_lot::RwLock<Option<V>>,
            ) -> Result<parking_lot::RwLockReadGuard<'a, Option<V>>, Error> {
                #[allow(unused_macros)]
                macro_rules! method {
                    () => {
                        self
                    };
                }
                macro_rules! lock {
                    () => {
                        rwlock
                    };
                }
                $read
            }

            #[inline(always)]
            fn write(
                &self,
                rwlock: &'a parking_lot::RwLock<Option<V>>,
            ) -> Result<parking_lot::RwLockWriteGuard<'a, Option<V>>, Error> {
                #[allow(unused_macros)]
                macro_rules! method {
                    () => {
                        self
                    };
                }
                macro_rules! lock {
                    () => {
                        rwlock
                    };
                }
                $write
            }
        }
    };
}

impl_locking_method!(Blocking, Ok(lock!().read()), Ok(lock!().write()));

impl_locking_method!(
    TryLock,
    lock!().try_read().ok_or(Error::LockUnavailable),
    lock!().try_write().ok_or(Error::LockUnavailable)
);

impl_locking_method!(
    Duration,
    lock!()
        .try_read_for(*method!())
        .ok_or(Error::LockUnavailable),
    lock!()
        .try_write_for(*method!())
        .ok_or(Error::LockUnavailable)
);

impl_locking_method!(
    Instant,
    lock!()
        .try_read_until(*method!())
        .ok_or(Error::LockUnavailable),
    lock!()
        .try_write_until(*method!())
        .ok_or(Error::LockUnavailable)
);

impl_locking_method!(
    Recursive<Blocking>,
    Ok(lock!().read_recursive()),
    Ok(lock!().write())
);

impl_locking_method!(
    Recursive<TryLock>,
    lock!().try_read_recursive().ok_or(Error::LockUnavailable),
    lock!().try_write().ok_or(Error::LockUnavailable)
);

impl_locking_method!(
    Recursive<Duration>,
    lock!()
        .try_read_recursive_for(method!().0)
        .ok_or(Error::LockUnavailable),
    lock!()
        .try_write_for(method!().0)
        .ok_or(Error::LockUnavailable)
);

impl_locking_method!(
    Recursive<Instant>,
    lock!()
        .try_read_recursive_until(method!().0)
        .ok_or(Error::LockUnavailable),
    lock!()
        .try_write_until(method!().0)
        .ok_or(Error::LockUnavailable)
);

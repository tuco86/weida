//! The one way this crate takes a `std` mutex.
//!
//! Every lock here guards state that is replaced or `take`n whole — an
//! `Option` a call empties, a snapshot a call overwrites — so a panic while one
//! is held cannot leave it half written, and the next caller may use it. A
//! poisoned lock that gave up instead would turn one panic, which PyO3 already
//! reports as a `PanicException`, into the same panic from every later call on
//! the object: including the `reply` and `refuse` a pending request needs to be
//! answered at all, so that its `Drop` would be the only way out.

use std::sync::{Mutex, MutexGuard, PoisonError};

/// Takes `mutex`, whether or not a thread panicked while it held it.
pub(crate) fn locked<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    #[test]
    fn a_lock_that_a_panic_poisoned_is_still_taken() {
        let mutex = Arc::new(Mutex::new(Some(7u32)));
        let holder = Arc::clone(&mutex);
        let panicked = std::thread::spawn(move || {
            let _guard = holder.lock().expect("not yet poisoned");
            panic!("a panic while the guard is held");
        })
        .join();
        assert!(panicked.is_err());
        assert!(mutex.is_poisoned(), "the premise: the lock is poisoned");

        assert_eq!(locked(&mutex).take(), Some(7));
        assert_eq!(*locked(&mutex), None);
    }
}

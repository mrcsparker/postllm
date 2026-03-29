#![expect(
    clippy::redundant_pub_crate,
    reason = "this module exposes crate-private helpers across sibling modules"
)]

use crate::error::{Error, Result};
use std::sync::atomic::{AtomicBool, Ordering};

/// Checks for a pending `PostgreSQL` query interrupt on the current backend thread.
#[cfg(not(test))]
pub(crate) fn checkpoint() {
    pgrx::check_for_interrupts!();
}

/// Unit tests run outside a live `PostgreSQL` backend, so interrupt polling is a no-op there.
#[cfg(test)]
pub(crate) const fn checkpoint() {}

/// Marks a shared cancellation flag when `PostgreSQL` has signaled an interrupt, then aborts.
#[cfg(not(test))]
pub(crate) fn checkpoint_with_cancellation(cancelled: &AtomicBool) {
    if pgrx::pg_sys::elog::interrupt_pending() {
        cancelled.store(true, Ordering::Relaxed);
    }

    pgrx::check_for_interrupts!();
}

/// Unit tests run outside a live `PostgreSQL` backend, so interrupt polling is a no-op there.
#[cfg(test)]
pub(crate) const fn checkpoint_with_cancellation(_cancelled: &AtomicBool) {}

/// Returns an interrupt-style error when a shared cancellation flag has been raised.
pub(crate) fn ensure_not_cancelled(cancelled: &AtomicBool) -> Result<()> {
    if cancelled.load(Ordering::Relaxed) {
        Err(Error::Interrupted)
    } else {
        Ok(())
    }
}

//! A thread-safe oneshot channel implementation for single-value communication.
//!
//! This module provides a channel that can transmit exactly one value from one or more
//! senders to one or more receivers. Unlike standard channels that can send multiple values,
//! oneshot channels are designed for scenarios where you need to communicate a single result,
//! signal completion, or pass a computed value between threads.
//!
//! ## Key Features
//!
//! - Multiple senders: Can be cloned to create multiple senders, but only the first
//!   successful send will be delivered
//! - Multiple receivers: Can be cloned to create multiple receivers, but only one
//!   receiver will consume the value
//! - Non-blocking options: Supports both blocking and non-blocking (`try_recv`) receive operations
//! - Timeout support: Can wait for a value with a specified timeout
//! - Automatic cleanup: Channel is automatically closed when all senders are dropped
//!
//! ## Channel Lifecycle
//!
//! The channel follows these state transitions:
//!
//! 1. Pending: Initial state, waiting for a value to be sent
//! 2. Ready: A value has been sent and is available for consumption
//! 3. Consumed: The value has been taken or the channel has been closed
//!
//! The channel is automatically closed when:
//! - All senders are dropped without sending a value
//! - A value is sent and subsequently consumed by a receiver
//! - The channel is explicitly closed using `force_close()`
//!
//! ## Thread Safety
//!
//! All types in this module (`OneshotSender<T>` and `OneshotReceiver<T>`) implement
//! `Send` and `Sync` when `T: Send`, making them safe to use across thread boundaries.
//! The internal synchronization is handled using `Mutex`, `Condvar`, and atomic operations.

use std::{
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

/// Creates a new oneshot channel, returning a sender and receiver pair.
///
/// The sender can be cloned to create multiple senders that can all attempt
/// to send a value, but only the first successful send will be delivered.
/// The receiver can also be cloned to allow multiple receivers, but only one
/// of them will receive and consume the value when it is sent.
pub fn channel<T>() -> (OneshotSender<T>, OneshotReceiver<T>) {
    let cell = Arc::new(OneshotCell::new());
    (OneshotSender(cell.clone()), OneshotReceiver(cell))
}

/// Creates a oneshot receiver that is already resolved with the given value.
///
/// This is useful for creating a receiver that immediately returns a value
/// without needing to go through the channel communication process.
pub fn ready<T>(result: T) -> OneshotReceiver<T> {
    let cell = Arc::new(OneshotCell::ready(result));
    OneshotReceiver(cell)
}

/// The sending half of a oneshot channel.
///
/// This type can be cloned to create multiple senders. When all senders are dropped
/// without sending a value, the receiver will be notified that no value will arrive.
/// Only the first successful send will be delivered to receivers.
pub struct OneshotSender<T>(Arc<OneshotCell<T>>);

impl<T> OneshotSender<T> {
    /// Attempts to send a value through the channel.
    ///
    /// Returns `Ok(())` if the value was successfully sent, or `Err(value)`
    /// if the channel was already closed or a value was already sent.
    pub fn send(&self, value: T) -> Result<(), T> {
        self.0.set(value)
    }

    /// Checks if the channel is still pending (no value sent yet).
    ///
    /// Returns `true` if no value has been sent and the channel is still open,
    /// `false` otherwise.
    pub fn is_pending(&self) -> bool {
        self.0.is_pending()
    }

    /// Forcefully closes the channel without sending a value.
    ///
    /// This will cause all receivers to receive `None` when they attempt
    /// to receive a value.
    pub fn force_close(&self) {
        self.0.cancel();
    }
}

/// Clone implementation for OneshotSender.
///
/// Cloning a sender increments the sender count. The channel will only be
/// automatically closed when all senders are dropped.
impl<T> Clone for OneshotSender<T> {
    fn clone(&self) -> OneshotSender<T> {
        self.0.add_sender();
        OneshotSender(self.0.clone())
    }
}

/// Drop implementation for OneshotSender.
///
/// When a sender is dropped, the sender count is decremented. If this was
/// the last sender, the channel is automatically closed.
impl<T> Drop for OneshotSender<T> {
    fn drop(&mut self) {
        self.0.drop_sender();
    }
}

/// The receiving half of a oneshot channel.
///
/// This type can be cloned to create multiple receivers that can all attempt
/// to receive the value. However, the value can only be consumed by a single receiver.
#[derive(Clone)]
pub struct OneshotReceiver<T>(Arc<OneshotCell<T>>);

impl<T> OneshotReceiver<T> {
    /// Blocks until a value is received or the channel is closed.
    ///
    /// Returns `Some(value)` if a value was sent and is ready.
    /// Returns `None` if a value was already received and consumed, or if the channel
    /// was closed before a value was sent.
    pub fn recv(&self) -> Option<T> {
        self.0.wait()
    }

    /// Attempts to receive a value with a timeout.
    ///
    /// Returns `Ok(Some(value))` if a value was received within the timeout,
    /// `Ok(None)` if the channel was closed, or `Err(())` if the timeout
    /// was reached while the channel was still pending.
    pub fn recv_timeout(
        &self,
        timeout: Duration,
    ) -> Result<Option<T>, std::sync::mpsc::RecvTimeoutError> {
        self.0
            .wait_for(timeout)
            .map_err(|_| std::sync::mpsc::RecvTimeoutError::Timeout)
    }

    /// Attempts to receive a value without blocking.
    ///
    /// Returns `Ok(Some(value))` if a value is immediately available,
    /// `Ok(None)` if the channel is closed, or `Err(())` if the channel
    /// is still pending.
    pub fn try_recv(&self) -> Result<Option<T>, std::sync::mpsc::TryRecvError> {
        self.0
            .try_take()
            .map_err(|_| std::sync::mpsc::TryRecvError::Empty)
    }

    /// Checks if the channel is still pending (no value received yet).
    ///
    /// Returns `true` if no value has been sent and the channel is still open,
    /// `false` otherwise.
    pub fn is_pending(&self) -> bool {
        self.0.is_pending()
    }

    /// Forcefully closes the channel.
    ///
    /// This will cause subsequent receive operations to return `None`.
    pub fn force_close(&self) {
        self.0.cancel();
    }
}

/// Internal cell structure that manages the state and synchronization
/// for the oneshot channel.
///
/// This structure uses a mutex-protected state, a condition variable for
/// blocking operations, and an atomic counter to track the number of senders.
struct OneshotCell<T> {
    value: Mutex<State<T>>,
    condvar: Condvar,
    senders: AtomicUsize,
}

impl<T> OneshotCell<T> {
    /// Creates a new pending oneshot cell.
    fn new() -> OneshotCell<T> {
        OneshotCell {
            value: Mutex::new(State::Pending),
            condvar: Condvar::new(),
            senders: AtomicUsize::new(1),
        }
    }

    /// Creates a new oneshot cell that is already resolved with a value.
    fn ready(value: T) -> OneshotCell<T> {
        OneshotCell {
            value: Mutex::new(State::Ready(value)),
            condvar: Condvar::new(),
            senders: AtomicUsize::new(1),
        }
    }

    /// Attempts to set a value in the cell.
    ///
    /// Returns `Ok(())` if successful, or `Err(value)` if the cell already
    /// has a value or has been consumed.
    fn set(&self, value: T) -> Result<(), T> {
        let res = self.value.lock().unwrap().set(value);
        self.condvar.notify_all();
        res
    }

    /// Checks if the cell is in a pending state.
    fn is_pending(&self) -> bool {
        self.value.lock().unwrap().is_pending()
    }

    /// Attempts to take a value without blocking.
    ///
    /// Returns `Ok(Some(value))` if a value is available, `Ok(None)` if
    /// the channel is closed, or `Err(())` if still pending.
    fn try_take(&self) -> Result<Option<T>, ()> {
        self.value.lock().unwrap().try_take()
    }

    /// Cancels the channel, marking it as consumed.
    fn cancel(&self) {
        self.value.lock().unwrap().cancel();
        self.condvar.notify_all();
    }

    /// Blocks until a value is available or the channel is closed.
    fn wait(&self) -> Option<T> {
        let mut guard = self.value.lock().unwrap();
        if !guard.is_pending() {
            return guard.take();
        }

        self.condvar
            .wait_while(guard, |state| state.is_pending())
            .unwrap()
            .take()
    }

    /// Waits for a value with a timeout.
    ///
    /// Returns `Ok(Some(value))` if a value was received, `Ok(None)` if
    /// the channel was closed, or `Err(())` if the timeout was reached.
    fn wait_for(&self, timeout: Duration) -> Result<Option<T>, ()> {
        let mut guard = self.value.lock().unwrap();
        if !guard.is_pending() {
            return Ok(guard.take());
        }

        let (mut guard, res) = self
            .condvar
            .wait_timeout_while(guard, timeout, |state| state.is_pending())
            .unwrap();
        if res.timed_out() {
            Err(())
        } else {
            guard.try_take()
        }
    }

    /// Increments the sender count when a sender is cloned.
    fn add_sender(&self) {
        self.senders.fetch_add(1, Ordering::SeqCst);
    }

    /// Decrements the sender count when a sender is dropped.
    /// If this was the last sender, the channel is automatically closed.
    fn drop_sender(&self) {
        if self.senders.fetch_sub(1, Ordering::SeqCst) == 1 {
            self.cancel();
        }
    }
}

/// Internal state representation for the oneshot channel.
///
/// The state transitions are:
/// - `Pending` -> `Ready(T)` when a value is sent
/// - `Pending` -> `Consumed` when the channel is cancelled
/// - `Ready(T)` -> `Consumed` when the value is taken
enum State<T> {
    /// No value has been sent yet, channel is open for sending.
    Pending,
    /// A value has been sent and is ready to be consumed.
    Ready(T),
    /// The channel has been closed or the value has been consumed.
    Consumed,
}

impl<T> State<T> {
    /// Returns true if the state is pending (no value sent yet).
    fn is_pending(&self) -> bool {
        matches!(self, State::Pending)
    }

    /// Attempts to set a value in the state.
    ///
    /// Returns `Ok(())` if the state was pending and the value was set,
    /// or `Err(value)` if the state was already ready or consumed.
    fn set(&mut self, value: T) -> Result<(), T> {
        match self {
            State::Pending => {
                *self = State::Ready(value);
                Ok(())
            }
            State::Ready(_) | State::Consumed => Err(value),
        }
    }

    /// Attempts to take a value from the state without blocking.
    ///
    /// Returns `Ok(Some(value))` if a value is available, `Ok(None)` if
    /// consumed, or `Err(())` if still pending.
    fn try_take(&mut self) -> Result<Option<T>, ()> {
        if self.is_pending() {
            Err(())
        } else {
            Ok(self.take())
        }
    }

    /// Takes the value from the state, consuming it.
    ///
    /// # Panics
    ///
    /// Panics if called when the state is still pending.
    fn take(&mut self) -> Option<T> {
        match std::mem::replace(self, State::Consumed) {
            State::Pending => panic!("State::take() unexpected: value is not ready yet"),
            State::Ready(value) => Some(value),
            State::Consumed => None,
        }
    }

    /// Transitions a `Pending` state to `Consumed`.
    fn cancel(&mut self) {
        if self.is_pending() {
            let _ = std::mem::replace(self, State::Consumed);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{cell::UnsafeCell, time::Duration};

    use crate::oneshot::{self, OneshotReceiver, OneshotSender};

    #[test]
    fn test_oneshot_send_sync() {
        fn is_send_sync<T: Send + Sync>() {}

        fn test<T: Send>() {
            is_send_sync::<OneshotReceiver<T>>();
            is_send_sync::<OneshotSender<T>>();
        }

        test::<usize>();
        test::<UnsafeCell<usize>>();
    }

    #[test]
    fn test_oneshot_basics() {
        let (tx, rx) = oneshot::channel::<usize>();
        assert!(rx.is_pending());
        tx.send(1).unwrap();
        assert_eq!(rx.recv().unwrap(), 1);
        assert!(rx.recv().is_none());

        let (tx, rx) = oneshot::channel::<usize>();
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(20));
            tx.send(1).unwrap();
        });
        assert_eq!(rx.recv().unwrap(), 1);

        let (tx, rx) = oneshot::channel::<usize>();
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(100));
            tx.send(1).unwrap();
        });
        assert!(rx.recv_timeout(Duration::from_millis(10)).is_err());
        assert!(rx.is_pending());
        assert_eq!(rx.recv().unwrap(), 1);

        let (tx, rx) = oneshot::channel::<usize>();
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(20));
            drop(tx);
        });
        assert!(rx.recv().is_none());
        assert!(!rx.is_pending());
    }
}

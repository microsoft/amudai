//! A simple multi-producer, multi-consumer (MPMC) channel implementation.
//!
//! This module provides blocking MPMC channels similar to `std::sync::mpsc` but with
//! support for multiple receivers. It includes both bounded and unbounded channel variants.
//!
//! **Note**: This implementation should be replaced with `std::sync::mpmc` once it becomes
//! stabilized in the Rust standard library.

use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex};

/// Creates a new asynchronous channel, returning the sender/receiver halves.
///
/// All data sent on the [`Sender`] will become available on the [`Receiver`] in
/// the same order as it was sent, and no [`send`] will block the calling thread
/// (this channel has an "infinite buffer", unlike [`sync_channel`], which will
/// block after its buffer limit is reached). [`recv`] will block until a message
/// is available while there is at least one [`Sender`] alive (including clones).
///
/// The [`Sender`] can be cloned to [`send`] to the same channel multiple times.
/// The [`Receiver`] also can be cloned to have multi receivers.
///
/// If the [`Receiver`] is disconnected while trying to [`send`] with the
/// [`Sender`], the [`send`] method will return a [`SendError`](std::sync::mpsc::SendError).
/// Similarly, if the [`Sender`] is disconnected while trying to [`recv`], the [`recv`] method
/// will drain the buffer, then return a [`RecvError`](std::sync::mpsc::RecvError).
///
/// [`send`]: Sender::send
/// [`recv`]: Receiver::recv
pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
    let queue = SimpleMpmcQueue::<T>::new(None);
    (Sender(queue.clone()), Receiver(queue))
}

/// Creates a new synchronous, bounded channel.
///
/// All data sent on the [`Sender`] will become available on the [`Receiver`]
/// in the same order as it was sent. Like asynchronous [`channel`]s, the
/// [`Receiver`] will block until a message becomes available.
///
/// This channel has an internal buffer on which messages will be queued.
/// `bound` specifies the buffer size. When the internal buffer becomes full,
/// future sends will *block* waiting for the buffer to open up.
///
/// **Note**: a buffer size of 0 is invalid in this implementation.
///
/// The [`Sender`] can be cloned to [`send`] to the same channel multiple
/// times. The [`Receiver`] also can be cloned to have multi receivers.
///
/// Like asynchronous channels, if the [`Receiver`] is disconnected while trying
/// to [`send`] with the [`Sender`], the [`send`] method will return a
/// [`SendError`](std::sync::mpsc::SendError). Similarly, If the [`Sender`] is disconnected
/// while trying to [`recv`], the [`recv`] method will drain the buffer, then return a
/// [`RecvError`](std::sync::mpsc::RecvError).
///
/// [`send`]: Sender::send
/// [`recv`]: Receiver::recv
pub fn sync_channel<T>(bound: usize) -> (Sender<T>, Receiver<T>) {
    assert_ne!(bound, 0, "sync_channel does not support zero bound");
    let queue = SimpleMpmcQueue::<T>::new(Some(bound));
    (Sender(queue.clone()), Receiver(queue))
}

/// The sending half of the [`channel`] (or [`sync_channel`]) type.
pub struct Sender<T>(SimpleMpmcQueue<T>);

impl<T> Sender<T> {
    /// Attempts to send a value on this channel, returning it back if it could
    /// not be sent.
    ///
    /// A successful send occurs when it is determined that the other end of
    /// the channel has not hung up already. An unsuccessful send would be one
    /// where the corresponding receiver has already been deallocated. Note
    /// that a return value of [`Err`] means that the data will never be
    /// received, but a return value of [`Ok`] does *not* mean that the data
    /// will be received. It is possible for the corresponding receiver to
    /// hang up immediately after this function returns [`Ok`].
    ///
    /// If the channel is full and not disconnected, this call will block until
    /// the send operation can proceed. If the channel becomes disconnected,
    /// this call will wake up and return an error. The returned error contains
    /// the original message.
    pub fn send(&self, msg: T) -> Result<(), std::sync::mpsc::SendError<T>> {
        self.0
            .enqueue(msg)
            .map_err(|res| std::sync::mpsc::SendError(res))
    }

    /// Attempts to send a value on this channel without blocking.
    ///
    /// This method will never block the current thread. It will either:
    /// - Successfully send the value and return `Ok(())`
    /// - Return `Err(std::sync::mpsc::TrySendError::Full(msg))` if the channel is full
    /// - Return `Err(std::sync::mpsc::TrySendError::Disconnected(msg))` if the channel is disconnected
    pub fn try_send(&self, msg: T) -> Result<(), std::sync::mpsc::TrySendError<T>> {
        match self.0.try_enqueue(msg) {
            Ok(()) => Ok(()),
            Err(TryEnqueueError::Full(msg)) => Err(std::sync::mpsc::TrySendError::Full(msg)),
            Err(TryEnqueueError::Disconnected(msg)) => {
                Err(std::sync::mpsc::TrySendError::Disconnected(msg))
            }
        }
    }
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        self.0.add_producer();
        Self(self.0.clone())
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        self.0.drop_producer();
    }
}

/// The receiving half of the [`channel`] (or [`sync_channel`]) type.
/// Different threads can share this [`Receiver`] by cloning it.
///
/// Messages sent to the channel can be retrieved using [`recv`].
///
/// [`recv`]: Receiver::recv
pub struct Receiver<T>(SimpleMpmcQueue<T>);

impl<T> Receiver<T> {
    /// Attempts to wait for a value on this receiver, returning an error if the
    /// corresponding channel has hung up.
    ///
    /// This function will always block the current thread if there is no data
    /// available and it's possible for more data to be sent (at least one sender
    /// still exists). Once a message is sent to the corresponding [`Sender`],
    /// this receiver will wake up and return that message.
    ///
    /// If the corresponding [`Sender`] has disconnected, or it disconnects while
    /// this call is blocking, this call will wake up and return [`Err`] to
    /// indicate that no more messages can ever be received on this channel.
    /// However, since channels are buffered, messages sent before the disconnect
    /// will still be properly received.
    pub fn recv(&self) -> Result<T, std::sync::mpsc::RecvError> {
        self.0.dequeue().map_err(|_| std::sync::mpsc::RecvError)
    }

    /// Attempts to receive a value on this receiver without blocking.
    ///
    /// This method will never block the current thread. It will either:
    /// - Successfully receive a value and return `Ok(value)`
    /// - Return `Err(std::sync::mpsc::TryRecvError::Empty)` if the channel is empty but not disconnected
    /// - Return `Err(std::sync::mpsc::TryRecvError::Disconnected)` if the channel is empty and disconnected
    pub fn try_recv(&self) -> Result<T, std::sync::mpsc::TryRecvError> {
        match self.0.try_dequeue() {
            Ok(value) => Ok(value),
            Err(TryDequeueError::Empty) => Err(std::sync::mpsc::TryRecvError::Empty),
            Err(TryDequeueError::Disconnected) => Err(std::sync::mpsc::TryRecvError::Disconnected),
        }
    }
}

impl<T> Clone for Receiver<T> {
    fn clone(&self) -> Self {
        self.0.add_consumer();
        Self(self.0.clone())
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        self.0.drop_consumer();
    }
}

/// An error returned from the `dequeue` method.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum DequeueError {
    /// The queue is empty and all producers have been dropped, so no more
    /// items will ever be added.
    Disconnected,
}

impl std::fmt::Display for DequeueError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DequeueError::Disconnected => write!(f, "queue is empty and disconnected"),
        }
    }
}

impl std::error::Error for DequeueError {}

/// An error returned from the `try_enqueue` method.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum TryEnqueueError<T> {
    /// The queue is full (only applies to bounded queues).
    Full(T),
    /// The queue is closed because all consumers have been dropped.
    Disconnected(T),
}

impl<T> std::fmt::Display for TryEnqueueError<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TryEnqueueError::Full(_) => write!(f, "queue is full"),
            TryEnqueueError::Disconnected(_) => write!(f, "queue is disconnected"),
        }
    }
}

impl<T: std::fmt::Debug> std::error::Error for TryEnqueueError<T> {}

/// An error returned from the `try_dequeue` method.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum TryDequeueError {
    /// The queue is empty but producers are still connected.
    Empty,
    /// The queue is empty and all producers have been dropped, so no more
    /// items will ever be added.
    Disconnected,
}

impl std::fmt::Display for TryDequeueError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TryDequeueError::Empty => write!(f, "queue is empty"),
            TryDequeueError::Disconnected => write!(f, "queue is empty and disconnected"),
        }
    }
}

impl std::error::Error for TryDequeueError {}

/// A simple, blocking, multi-producer, multi-consumer (MPMC) queue.
///
/// The queue can be cloned to create multiple handles for producers and consumers.
pub struct SimpleMpmcQueue<T> {
    inner: Arc<Inner<T>>,
}

impl<T> Clone for SimpleMpmcQueue<T> {
    /// Clones the handle to the queue.
    ///
    /// This does not create a new queue. It creates a new pointer to the same
    /// underlying queue. Note that this does NOT automatically increment the
    /// producer or consumer count. You must use `add_producer` or `add_consumer`
    /// for that purpose.
    fn clone(&self) -> Self {
        SimpleMpmcQueue {
            inner: self.inner.clone(),
        }
    }
}

impl<T> SimpleMpmcQueue<T> {
    /// Creates a new MPMC queue.
    ///
    /// # Arguments
    ///
    /// * `max_capacity`:
    ///   - `Some(n)`: Creates a bounded queue with a maximum capacity of `n`.
    ///   - `None`: Creates an unbounded queue.
    ///
    /// The queue starts with one producer and one consumer registered.
    pub fn new(max_capacity: Option<usize>) -> Self {
        assert!(
            max_capacity.unwrap_or(1) >= 1,
            "SimpleMpmcQueue does not support zero capacity"
        );

        let inner_state = InnerState {
            queue: VecDeque::new(),
            capacity: max_capacity,
            producers: 1,
            consumers: 1,
        };

        let inner = Inner {
            state: Mutex::new(inner_state),
            not_empty: Condvar::new(),
            not_full: Condvar::new(),
        };

        SimpleMpmcQueue {
            inner: Arc::new(inner),
        }
    }

    /// Registers an additional producer for this queue.
    pub fn add_producer(&self) {
        let mut state = self.inner.state.lock().unwrap();
        assert!(
            state.producers != 0,
            "Attempt to add producer when the queue is half-closed"
        );
        state.producers += 1;
    }

    /// De-registers a producer.
    ///
    /// When the producer count reaches zero, the queue is "half-closed".
    /// No more items can be enqueued, but consumers can continue to dequeue
    /// remaining items.
    pub fn drop_producer(&self) {
        let mut state = self.inner.state.lock().unwrap();

        // Panic if drop_producer is called more times than add_producer.
        // The user of the API is responsible for balancing the calls.
        assert!(
            state.producers >= 1,
            "drop_producer is called more times than add_producer"
        );

        state.producers -= 1;
        if state.producers == 0 {
            // Wake up any waiting consumers so they can check for the
            // disconnected state and exit.
            self.inner.not_empty.notify_all();
        }
    }

    /// Registers an additional consumer for this queue.
    pub fn add_consumer(&self) {
        let mut state = self.inner.state.lock().unwrap();

        assert!(
            state.consumers > 0,
            "Attempt to add consumer when the queue is closed"
        );

        state.consumers += 1;
    }

    /// De-registers a consumer.
    ///
    /// When the consumer count reaches zero, the queue is "closed".
    /// All remaining items in the queue are dropped, and any subsequent
    /// calls to `enqueue` will fail.
    pub fn drop_consumer(&self) {
        let mut state = self.inner.state.lock().unwrap();

        assert!(
            state.consumers > 0,
            "drop_consumer is called more times than add_consumer"
        );

        state.consumers -= 1;
        if state.consumers == 0 {
            // The queue is now fully closed. Clear any remaining items.
            state.queue.clear();
            // Wake up any waiting producers to notify them the queue is closed.
            self.inner.not_full.notify_all();
        }
    }

    /// Enqueues an item into the queue.
    ///
    /// - If the queue is bounded and full, this function will block until space
    ///   becomes available.
    /// - If the queue is closed (no more consumers), it returns `Err(item)`,
    ///   giving the item back to the caller.
    pub fn enqueue(&self, item: T) -> Result<(), T> {
        let mut state = self.inner.state.lock().unwrap();

        loop {
            // Condition 1: Queue is closed.
            if state.consumers == 0 {
                return Err(item);
            }

            // Condition 2: Queue is full (for bounded queues).
            if let Some(cap) = state.capacity {
                if state.queue.len() >= cap {
                    // Wait for a consumer to make space.
                    state = self.inner.not_full.wait(state).unwrap();
                    // Loop again to re-check all conditions.
                    continue;
                }
            }

            // If we reach here, we can enqueue.
            break;
        }

        state.queue.push_back(item);

        // Drop the lock before notifying to potentially reduce contention.
        drop(state);

        // Notify one waiting consumer that an item is available.
        self.inner.not_empty.notify_one();

        Ok(())
    }

    /// Dequeues an item from the queue.
    ///
    /// - If the queue is empty, this function will block until an item becomes
    ///   available.
    /// - If the queue is empty and half-closed (no more producers), it returns
    ///   `Err(DequeueError::Disconnected)`.
    pub fn dequeue(&self) -> Result<T, DequeueError> {
        let mut state = self.inner.state.lock().unwrap();

        loop {
            // Condition 1: Queue has an item.
            if let Some(item) = state.queue.pop_front() {
                // Drop the lock before notifying.
                drop(state);
                // Notify one waiting producer that space is available.
                self.inner.not_full.notify_one();
                return Ok(item);
            }

            // Condition 2: Queue is empty and disconnected.
            if state.producers == 0 {
                return Err(DequeueError::Disconnected);
            }

            // Condition 3: Queue is empty, but still connected.
            // Wait for a producer to add an item.
            state = self.inner.not_empty.wait(state).unwrap();
            // Loop again to re-check all conditions.
        }
    }

    /// Attempts to enqueue an item into the queue without blocking.
    ///
    /// - If the queue is bounded and full, returns `Err(TryEnqueueError::Full(item))`.
    /// - If the queue is closed (no more consumers), returns `Err(TryEnqueueError::Disconnected(item))`.
    /// - Otherwise, the item is enqueued and `Ok(())` is returned.
    pub fn try_enqueue(&self, item: T) -> Result<(), TryEnqueueError<T>> {
        let mut state = self.inner.state.lock().unwrap();

        // Condition 1: Queue is closed.
        if state.consumers == 0 {
            return Err(TryEnqueueError::Disconnected(item));
        }

        // Condition 2: Queue is full (for bounded queues).
        if let Some(cap) = state.capacity {
            if state.queue.len() >= cap {
                return Err(TryEnqueueError::Full(item));
            }
        }

        // If we reach here, we can enqueue.
        state.queue.push_back(item);

        // Drop the lock before notifying to potentially reduce contention.
        drop(state);

        // Notify one waiting consumer that an item is available.
        self.inner.not_empty.notify_one();

        Ok(())
    }

    /// Attempts to dequeue an item from the queue without blocking.
    ///
    /// - If the queue has an item, it is returned as `Ok(item)`.
    /// - If the queue is empty but producers are still connected, returns `Err(TryDequeueError::Empty)`.
    /// - If the queue is empty and half-closed (no more producers), returns `Err(TryDequeueError::Disconnected)`.
    pub fn try_dequeue(&self) -> Result<T, TryDequeueError> {
        let mut state = self.inner.state.lock().unwrap();

        // Condition 1: Queue has an item.
        if let Some(item) = state.queue.pop_front() {
            // Drop the lock before notifying.
            drop(state);
            // Notify one waiting producer that space is available.
            self.inner.not_full.notify_one();
            return Ok(item);
        }

        // Condition 2: Queue is empty and disconnected.
        if state.producers == 0 {
            return Err(TryDequeueError::Disconnected);
        }

        // Condition 3: Queue is empty, but still connected.
        Err(TryDequeueError::Empty)
    }
}

/// The state of the queue that is protected by the Mutex.
struct InnerState<T> {
    queue: VecDeque<T>,
    capacity: Option<usize>,
    producers: usize,
    consumers: usize,
}

/// The shared core of the queue, containing the state and condition variables.
struct Inner<T> {
    state: Mutex<InnerState<T>>,
    not_empty: Condvar, // To signal consumers that an item has been added.
    not_full: Condvar,  // To signal producers that space has become available.
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_new_and_single_thread_enqueue_dequeue() {
        let q = SimpleMpmcQueue::new(Some(5));
        q.enqueue("hello").unwrap();
        let msg = q.dequeue().unwrap();
        assert_eq!(msg, "hello");
    }

    #[test]
    fn test_unbounded_queue() {
        let q = SimpleMpmcQueue::<i32>::new(None);
        for i in 0..1000 {
            q.enqueue(i).unwrap();
        }
        for i in 0..1000 {
            assert_eq!(q.dequeue().unwrap(), i);
        }
    }

    #[test]
    fn test_dequeue_blocks_until_enqueue() {
        let q = SimpleMpmcQueue::new(Some(1));
        let q_clone = q.clone();

        let handle = thread::spawn(move || {
            // This will block because the queue is empty.
            let msg = q_clone.dequeue().unwrap();
            assert_eq!(msg, "from other thread");
        });

        // Give the spawned thread a moment to start and block on dequeue.
        thread::sleep(Duration::from_millis(50));

        q.enqueue("from other thread").unwrap();

        handle.join().unwrap();
    }

    #[test]
    fn test_zero_capacity_queue() {
        let q = SimpleMpmcQueue::<i32>::new(Some(1));
        let q_clone = q.clone();

        let handle = thread::spawn(move || {
            // This will block because the queue is empty.
            q_clone.enqueue(1).unwrap();
        });

        // Give the spawned thread a moment to start and block on dequeue.
        thread::sleep(Duration::from_millis(50));

        let res = q.dequeue().unwrap();
        assert_eq!(res, 1);

        handle.join().unwrap();
    }

    #[test]
    fn test_enqueue_blocks_on_full_queue() {
        let q = SimpleMpmcQueue::new(Some(1));
        q.enqueue("first").unwrap();

        let q_clone = q.clone();
        let handle = thread::spawn(move || {
            // This will block because the queue is full.
            q_clone.enqueue("second").unwrap();
        });

        // Give the spawned thread a moment to start and block on enqueue.
        thread::sleep(Duration::from_millis(50));

        assert_eq!(q.dequeue().unwrap(), "first");

        handle.join().unwrap();
        assert_eq!(q.dequeue().unwrap(), "second");
    }

    #[test]
    fn test_drop_producer_half_closes_queue() {
        let q = SimpleMpmcQueue::new(Some(5));
        q.enqueue(1).unwrap();
        q.enqueue(2).unwrap();

        // This is the only producer, so dropping it half-closes the queue.
        q.drop_producer();

        // Consumers can still dequeue existing items.
        assert_eq!(q.dequeue().unwrap(), 1);
        assert_eq!(q.dequeue().unwrap(), 2);

        // Now the queue is empty and disconnected.
        assert_eq!(q.dequeue(), Err(DequeueError::Disconnected));
    }

    #[test]
    fn test_drop_consumer_fully_closes_queue() {
        let q = SimpleMpmcQueue::new(Some(5));
        q.enqueue(100).unwrap();

        // This is the only consumer, so dropping it fully closes the queue.
        q.drop_consumer();

        // Enqueuing should now fail, returning the item.
        assert_eq!(q.enqueue(200), Err(200));

        // Dequeuing should fail because the queue is empty (items were dropped)
        // and there are no more producers (the initial one is still there, but
        // the check for an empty queue with no producers is the path taken).
        // Let's add a producer and drop it to be sure.
        q.add_producer();
        q.drop_producer(); // producers = 1
        q.drop_producer(); // producers = 0
        assert_eq!(q.dequeue(), Err(DequeueError::Disconnected));
    }

    #[test]
    fn test_multi_producer_multi_consumer() {
        const NUM_PRODUCERS: usize = 4;
        const NUM_CONSUMERS: usize = 3;
        const ITEMS_PER_PRODUCER: usize = 1000;
        const TOTAL_ITEMS: usize = NUM_PRODUCERS * ITEMS_PER_PRODUCER;

        let q = SimpleMpmcQueue::new(Some(10)); // Bounded queue to test blocking

        // Add the extra producers and consumers
        for _ in 0..(NUM_PRODUCERS - 1) {
            q.add_producer();
        }
        for _ in 0..(NUM_CONSUMERS - 1) {
            q.add_consumer();
        }

        thread::scope(|s| {
            // --- Spawn Producers ---
            for i in 0..NUM_PRODUCERS {
                let q_clone = q.clone();
                s.spawn(move || {
                    for j in 0..ITEMS_PER_PRODUCER {
                        // Create a unique item for each producer/iteration
                        let item = i * ITEMS_PER_PRODUCER + j;
                        q_clone.enqueue(item).unwrap();
                    }
                    // Each producer de-registers itself when done
                    q_clone.drop_producer();
                });
            }

            // --- Spawn Consumers ---
            let mut consumer_handles = vec![];
            for _ in 0..NUM_CONSUMERS {
                let q_clone = q.clone();
                let handle = s.spawn(move || {
                    let mut received_items = vec![];
                    // Loop until the queue is disconnected
                    while let Ok(item) = q_clone.dequeue() {
                        received_items.push(item);
                    }
                    // Each consumer de-registers itself when done
                    q_clone.drop_consumer();
                    received_items
                });
                consumer_handles.push(handle);
            }

            // --- Collect Results ---
            let mut all_received = vec![];
            for handle in consumer_handles {
                all_received.extend(handle.join().unwrap());
            }

            // --- Assertions ---
            assert_eq!(all_received.len(), TOTAL_ITEMS);
            // Sort to check for missing items (duplicates would fail the len check)
            all_received.sort();
            for i in 0..TOTAL_ITEMS {
                assert_eq!(all_received[i], i);
            }
        });
    }

    #[test]
    fn test_try_enqueue_and_try_dequeue() {
        let q = SimpleMpmcQueue::new(Some(2));

        // Test successful try_enqueue
        assert!(q.try_enqueue(1).is_ok());
        assert!(q.try_enqueue(2).is_ok());

        // Test try_enqueue on full queue
        match q.try_enqueue(3) {
            Err(TryEnqueueError::Full(val)) => assert_eq!(val, 3),
            _ => panic!("Expected Full error"),
        }

        // Test successful try_dequeue
        assert_eq!(q.try_dequeue().unwrap(), 1);
        assert_eq!(q.try_dequeue().unwrap(), 2);

        // Test try_dequeue on empty queue (but still connected)
        match q.try_dequeue() {
            Err(TryDequeueError::Empty) => {}
            _ => panic!("Expected Empty error"),
        }

        // Test try_dequeue on disconnected queue
        q.drop_producer();
        match q.try_dequeue() {
            Err(TryDequeueError::Disconnected) => {}
            _ => panic!("Expected Disconnected error"),
        }
    }

    #[test]
    fn test_try_enqueue_disconnected() {
        let q = SimpleMpmcQueue::new(Some(2));
        q.drop_consumer();

        match q.try_enqueue(42) {
            Err(TryEnqueueError::Disconnected(val)) => assert_eq!(val, 42),
            _ => panic!("Expected Disconnected error"),
        }
    }

    #[test]
    fn test_sender_try_send() {
        let (sender, receiver) = sync_channel::<i32>(1);

        // Test successful try_send
        assert!(sender.try_send(1).is_ok());

        // Test try_send on full channel
        match sender.try_send(2) {
            Err(std::sync::mpsc::TrySendError::Full(val)) => assert_eq!(val, 2),
            _ => panic!("Expected Full error"),
        }

        // Receive to make space
        assert_eq!(receiver.recv().unwrap(), 1);

        // Test try_send after disconnect
        drop(receiver);
        match sender.try_send(3) {
            Err(std::sync::mpsc::TrySendError::Disconnected(val)) => assert_eq!(val, 3),
            _ => panic!("Expected Disconnected error"),
        }
    }

    #[test]
    fn test_receiver_try_recv() {
        let (sender, receiver) = channel::<i32>();

        // Test try_recv on empty channel
        match receiver.try_recv() {
            Err(std::sync::mpsc::TryRecvError::Empty) => {}
            _ => panic!("Expected Empty error"),
        }

        // Test successful try_recv
        sender.send(42).unwrap();
        assert_eq!(receiver.try_recv().unwrap(), 42);

        // Test try_recv after disconnect
        drop(sender);
        match receiver.try_recv() {
            Err(std::sync::mpsc::TryRecvError::Disconnected) => {}
            _ => panic!("Expected Disconnected error"),
        }
    }

    #[test]
    fn test_unbounded_try_enqueue() {
        let q = SimpleMpmcQueue::<i32>::new(None);

        // Unbounded queue should never return Full
        for i in 0..1000 {
            assert!(q.try_enqueue(i).is_ok());
        }

        // But should still return Disconnected when consumers are dropped
        q.drop_consumer();
        match q.try_enqueue(1001) {
            Err(TryEnqueueError::Disconnected(val)) => assert_eq!(val, 1001),
            _ => panic!("Expected Disconnected error"),
        }
    }

    // Tests for channel() API
    #[test]
    fn test_channel_basic_send_recv() {
        let (sender, receiver) = channel::<&str>();

        sender.send("hello").unwrap();
        sender.send("world").unwrap();

        assert_eq!(receiver.recv().unwrap(), "hello");
        assert_eq!(receiver.recv().unwrap(), "world");
    }

    #[test]
    fn test_channel_unbounded_large_volume() {
        let (sender, receiver) = channel::<i32>();

        // Should not block on sends since it's unbounded
        for i in 0..10000 {
            sender.send(i).unwrap();
        }

        for i in 0..10000 {
            assert_eq!(receiver.recv().unwrap(), i);
        }
    }

    #[test]
    fn test_channel_clone_sender() {
        let (sender, receiver) = channel::<i32>();
        let sender2 = sender.clone();
        let sender3 = sender.clone();

        sender.send(1).unwrap();
        sender2.send(2).unwrap();
        sender3.send(3).unwrap();

        let mut received = vec![
            receiver.recv().unwrap(),
            receiver.recv().unwrap(),
            receiver.recv().unwrap(),
        ];
        received.sort();
        assert_eq!(received, vec![1, 2, 3]);
    }

    #[test]
    fn test_channel_clone_receiver() {
        let (sender, receiver) = channel::<i32>();
        let receiver2 = receiver.clone();

        sender.send(1).unwrap();
        sender.send(2).unwrap();

        // Either receiver can get either message
        let msg1 = receiver.recv().unwrap();
        let msg2 = receiver2.recv().unwrap();

        let mut received = vec![msg1, msg2];
        received.sort();
        assert_eq!(received, vec![1, 2]);
    }

    #[test]
    fn test_channel_send_after_receiver_dropped() {
        let (sender, receiver) = channel::<i32>();

        sender.send(1).unwrap();
        drop(receiver);

        match sender.send(2) {
            Err(std::sync::mpsc::SendError(val)) => assert_eq!(val, 2),
            _ => panic!("Expected SendError"),
        }
    }

    #[test]
    fn test_channel_recv_after_sender_dropped() {
        let (sender, receiver) = channel::<i32>();

        sender.send(1).unwrap();
        sender.send(2).unwrap();
        drop(sender);

        // Should still receive buffered messages
        assert_eq!(receiver.recv().unwrap(), 1);
        assert_eq!(receiver.recv().unwrap(), 2);

        // Then should get disconnected error
        match receiver.recv() {
            Err(std::sync::mpsc::RecvError) => {}
            _ => panic!("Expected RecvError"),
        }
    }

    #[test]
    fn test_channel_multi_producer_single_consumer_threaded() {
        let (sender, receiver) = channel::<i32>();
        const NUM_THREADS: usize = 4;
        const MSGS_PER_THREAD: usize = 100;

        thread::scope(|s| {
            // Spawn multiple producer threads
            for i in 0..NUM_THREADS {
                let sender = sender.clone();
                s.spawn(move || {
                    for j in 0..MSGS_PER_THREAD {
                        let value = (i * MSGS_PER_THREAD + j) as i32;
                        sender.send(value).unwrap();
                    }
                });
            }

            // Drop original sender
            drop(sender);

            // Collect all messages
            let mut received = vec![];
            while let Ok(msg) = receiver.recv() {
                received.push(msg);
            }

            received.sort();
            let expected: Vec<i32> = (0..(NUM_THREADS * MSGS_PER_THREAD) as i32).collect();
            assert_eq!(received, expected);
        });
    }

    #[test]
    fn test_channel_single_producer_multi_consumer_threaded() {
        let (sender, receiver) = channel::<i32>();
        const NUM_CONSUMERS: usize = 3;
        const TOTAL_MSGS: usize = 300;

        thread::scope(|s| {
            let consumers: Vec<_> = (0..NUM_CONSUMERS)
                .map(|_| {
                    let receiver = receiver.clone();
                    s.spawn(move || {
                        let mut received = vec![];
                        while let Ok(msg) = receiver.recv() {
                            received.push(msg);
                        }
                        received
                    })
                })
                .collect();

            // Drop original receiver
            drop(receiver);

            // Send messages
            for i in 0..TOTAL_MSGS {
                sender.send(i as i32).unwrap();
            }
            drop(sender);

            // Collect results from all consumers
            let mut all_received = vec![];
            for handle in consumers {
                all_received.extend(handle.join().unwrap());
            }

            all_received.sort();
            let expected: Vec<i32> = (0..TOTAL_MSGS as i32).collect();
            assert_eq!(all_received, expected);
        });
    }

    // Tests for sync_channel() API
    #[test]
    fn test_sync_channel_basic_send_recv() {
        let (sender, receiver) = sync_channel::<&str>(3);

        sender.send("hello").unwrap();
        sender.send("world").unwrap();

        assert_eq!(receiver.recv().unwrap(), "hello");
        assert_eq!(receiver.recv().unwrap(), "world");
    }

    #[test]
    fn test_sync_channel_capacity_blocking() {
        let (sender, receiver) = sync_channel::<i32>(2);

        // Fill the channel
        sender.send(1).unwrap();
        sender.send(2).unwrap();

        // This should block, so we test it with try_send
        match sender.try_send(3) {
            Err(std::sync::mpsc::TrySendError::Full(val)) => assert_eq!(val, 3),
            _ => panic!("Expected channel to be full"),
        }

        // Make space and try again
        assert_eq!(receiver.recv().unwrap(), 1);
        sender.send(3).unwrap();
    }

    #[test]
    fn test_sync_channel_zero_bound_panics() {
        let result = std::panic::catch_unwind(|| sync_channel::<i32>(0));
        assert!(result.is_err());
    }

    #[test]
    fn test_sync_channel_blocking_send_threaded() {
        let (sender, receiver) = sync_channel::<i32>(1);

        // Fill the channel
        sender.send(1).unwrap();

        let sender_clone = sender.clone();
        let handle = thread::spawn(move || {
            // This will block until space is available
            sender_clone.send(2).unwrap();
            sender_clone.send(3).unwrap();
        });

        thread::sleep(Duration::from_millis(50));

        // Receive to unblock the sender
        assert_eq!(receiver.recv().unwrap(), 1);
        assert_eq!(receiver.recv().unwrap(), 2);
        assert_eq!(receiver.recv().unwrap(), 3);

        handle.join().unwrap();
    }

    #[test]
    fn test_sync_channel_clone_sender_bounded() {
        let (sender, receiver) = sync_channel::<i32>(3);
        let sender2 = sender.clone();

        sender.send(1).unwrap();
        sender2.send(2).unwrap();
        sender.send(3).unwrap();

        // Channel should now be full
        match sender2.try_send(4) {
            Err(std::sync::mpsc::TrySendError::Full(val)) => assert_eq!(val, 4),
            _ => panic!("Expected channel to be full"),
        }

        let mut received = vec![
            receiver.recv().unwrap(),
            receiver.recv().unwrap(),
            receiver.recv().unwrap(),
        ];
        received.sort();
        assert_eq!(received, vec![1, 2, 3]);
    }

    #[test]
    fn test_sync_channel_clone_receiver_bounded() {
        let (sender, receiver) = sync_channel::<i32>(2);
        let receiver2 = receiver.clone();

        sender.send(1).unwrap();
        sender.send(2).unwrap();

        // Either receiver can get either message
        let msg1 = receiver.recv().unwrap();
        let msg2 = receiver2.recv().unwrap();

        let mut received = vec![msg1, msg2];
        received.sort();
        assert_eq!(received, vec![1, 2]);
    }

    #[test]
    fn test_sync_channel_send_after_receiver_dropped() {
        let (sender, receiver) = sync_channel::<i32>(2);

        sender.send(1).unwrap();
        drop(receiver);

        match sender.send(2) {
            Err(std::sync::mpsc::SendError(val)) => assert_eq!(val, 2),
            _ => panic!("Expected SendError"),
        }
    }

    #[test]
    fn test_sync_channel_recv_after_sender_dropped() {
        let (sender, receiver) = sync_channel::<i32>(2);

        sender.send(1).unwrap();
        sender.send(2).unwrap();
        drop(sender);

        // Should still receive buffered messages
        assert_eq!(receiver.recv().unwrap(), 1);
        assert_eq!(receiver.recv().unwrap(), 2);

        // Then should get disconnected error
        match receiver.recv() {
            Err(std::sync::mpsc::RecvError) => {}
            _ => panic!("Expected RecvError"),
        }
    }

    #[test]
    fn test_sync_channel_multi_producer_bounded() {
        let (sender, receiver) = sync_channel::<i32>(5);
        const NUM_PRODUCERS: usize = 3;
        const MSGS_PER_PRODUCER: usize = 10;

        thread::scope(|s| {
            // Spawn producer threads
            for i in 0..NUM_PRODUCERS {
                let sender = sender.clone();
                s.spawn(move || {
                    for j in 0..MSGS_PER_PRODUCER {
                        let value = (i * MSGS_PER_PRODUCER + j) as i32;
                        sender.send(value).unwrap();
                    }
                });
            }

            drop(sender);

            // Collect all messages
            let mut received = vec![];
            while let Ok(msg) = receiver.recv() {
                received.push(msg);
            }

            received.sort();
            let expected: Vec<i32> = (0..(NUM_PRODUCERS * MSGS_PER_PRODUCER) as i32).collect();
            assert_eq!(received, expected);
        });
    }

    #[test]
    fn test_sync_channel_try_send_try_recv_integration() {
        let (sender, receiver) = sync_channel::<String>(2);

        // Test successful operations
        assert!(sender.try_send("first".to_string()).is_ok());
        assert!(sender.try_send("second".to_string()).is_ok());

        // Channel should be full now
        match sender.try_send("third".to_string()) {
            Err(std::sync::mpsc::TrySendError::Full(val)) => assert_eq!(val, "third"),
            _ => panic!("Expected Full error"),
        }

        // Test successful receives
        assert_eq!(receiver.try_recv().unwrap(), "first");
        assert_eq!(receiver.try_recv().unwrap(), "second");

        // Channel should be empty now
        match receiver.try_recv() {
            Err(std::sync::mpsc::TryRecvError::Empty) => {}
            _ => panic!("Expected Empty error"),
        }

        // Now we can send again
        assert!(sender.try_send("third".to_string()).is_ok());
        assert_eq!(receiver.try_recv().unwrap(), "third");
    }

    #[test]
    fn test_channel_try_send_try_recv_integration() {
        let (sender, receiver) = channel::<String>();

        // Unbounded channel should never be full
        for i in 0..1000 {
            assert!(sender.try_send(format!("msg{}", i)).is_ok());
        }

        // Should be able to receive all messages
        for i in 0..1000 {
            assert_eq!(receiver.try_recv().unwrap(), format!("msg{}", i));
        }

        // Channel should be empty now
        match receiver.try_recv() {
            Err(std::sync::mpsc::TryRecvError::Empty) => {}
            _ => panic!("Expected Empty error"),
        }
    }

    #[test]
    fn test_channel_receiver_blocking_behavior() {
        let (sender, receiver) = channel::<i32>();

        let handle = thread::spawn(move || {
            // This should block until a message is sent
            receiver.recv().unwrap()
        });

        thread::sleep(Duration::from_millis(50));
        sender.send(42).unwrap();

        assert_eq!(handle.join().unwrap(), 42);
    }

    #[test]
    fn test_sync_channel_receiver_blocking_behavior() {
        let (sender, receiver) = sync_channel::<i32>(1);

        let handle = thread::spawn(move || {
            // This should block until a message is sent
            receiver.recv().unwrap()
        });

        thread::sleep(Duration::from_millis(50));
        sender.send(42).unwrap();

        assert_eq!(handle.join().unwrap(), 42);
    }

    #[test]
    fn test_mixed_send_try_send_operations() {
        let (sender, receiver) = sync_channel::<i32>(2);

        // Mix blocking and non-blocking sends
        sender.send(1).unwrap();
        assert!(sender.try_send(2).is_ok());

        // Channel is full, try_send should fail
        match sender.try_send(3) {
            Err(std::sync::mpsc::TrySendError::Full(val)) => assert_eq!(val, 3),
            _ => panic!("Expected Full error"),
        }

        // Mix blocking and non-blocking receives
        assert_eq!(receiver.try_recv().unwrap(), 1);
        assert_eq!(receiver.recv().unwrap(), 2);

        // Channel is empty, try_recv should fail
        match receiver.try_recv() {
            Err(std::sync::mpsc::TryRecvError::Empty) => {}
            _ => panic!("Expected Empty error"),
        }
    }

    #[test]
    fn test_drop_all_senders_with_pending_receivers() {
        let (sender, receiver) = channel::<i32>();
        let receiver2 = receiver.clone();

        let handle1 = thread::spawn(move || receiver.recv());
        let handle2 = thread::spawn(move || receiver2.recv());

        thread::sleep(Duration::from_millis(50));

        // Drop the sender - this should wake up all blocked receivers
        drop(sender);

        // Both receivers should get disconnected errors
        assert!(handle1.join().unwrap().is_err());
        assert!(handle2.join().unwrap().is_err());
    }

    #[test]
    fn test_drop_all_receivers_with_pending_senders() {
        let (sender, receiver) = sync_channel::<i32>(1);
        let sender2 = sender.clone();

        // Fill the channel
        sender.send(1).unwrap();

        let handle1 = thread::spawn(move || sender.send(2));
        let handle2 = thread::spawn(move || sender2.send(3));

        thread::sleep(Duration::from_millis(50));

        // Drop the receiver - this should wake up all blocked senders
        drop(receiver);

        // Both senders should get disconnected errors
        assert!(handle1.join().unwrap().is_err());
        assert!(handle2.join().unwrap().is_err());
    }
}

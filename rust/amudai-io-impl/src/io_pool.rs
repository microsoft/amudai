//! Thread pool for asynchronous background I/O operations with a bounded queue size.

use std::{
    ops::Range,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicUsize, Ordering},
    },
};

use amudai_bytes::Bytes;
use amudai_io::ReadAt;
use once_cell::sync::OnceCell;

/// Thread pool for asynchronous background I/O operations with a bounded queue size.
///
/// `IoPool` provides a fixed-size thread pool dedicated to performing I/O operations
/// asynchronously. It limits the number of queued operations to prevent unbounded
/// memory growth during high load situations.
pub struct IoPool {
    /// The underlying thread pool that executes I/O operations.
    thread_pool: rayon::ThreadPool,
    /// Atomic counter tracking the current number of queued operations.
    queue_size: AtomicUsize,
}

impl IoPool {
    /// Maximum number of I/O operations that can be queued at once.
    const MAX_QUEUED_OPS: usize = 4 * 1024;
    /// Number of threads allocated to the I/O thread pool.
    // TODO: refine NUM_THREADS, possibly make it machine-dependent.
    const NUM_THREADS: usize = 16;

    /// Returns the global shared instance of the `IoPool`.
    ///
    /// The pool is lazily initialized on first call and shared across the application.
    ///
    /// # Returns
    ///
    /// An `Arc` reference to the global `IoPool` instance.
    pub fn get() -> Arc<IoPool> {
        static POOL: OnceLock<Arc<IoPool>> = OnceLock::new();
        POOL.get_or_init(IoPool::start).clone()
    }

    /// Provides access to the underlying thread pool.
    ///
    /// # Returns
    ///
    /// A reference to the rayon `ThreadPool` used by this `IoPool`.
    pub fn thread_pool(&self) -> &rayon::ThreadPool {
        &self.thread_pool
    }

    /// Creates and initializes a new `IoPool` instance.
    ///
    /// # Returns
    ///
    /// An `Arc` containing the newly created `IoPool`.
    fn start() -> Arc<IoPool> {
        let thread_pool = rayon::ThreadPoolBuilder::new()
            .num_threads(Self::NUM_THREADS)
            .thread_name(|i| format!("amudai_io_thread_{i}"))
            .build()
            .expect("thread pool");
        Arc::new(IoPool {
            thread_pool,
            queue_size: AtomicUsize::new(0),
        })
    }

    /// Attempts to enqueue an I/O operation for asynchronous execution.
    ///
    /// If the queue has reached its maximum capacity, the operation will be
    /// rejected and this method returns `false`.
    ///
    /// # Arguments
    ///
    /// * `op` - The I/O operation to enqueue.
    ///
    /// # Returns
    ///
    /// `true` if the operation was successfully queued, `false` otherwise.
    pub fn try_enqueue(self: &Arc<Self>, op: Arc<QueuedOp>) -> bool {
        if self.queue_size.fetch_add(1, Ordering::Relaxed) > Self::MAX_QUEUED_OPS {
            self.queue_size.fetch_sub(1, Ordering::Relaxed);
            return false;
        }

        let this = self.clone();
        self.thread_pool
            .spawn_fifo(move || Self::perform_op(this, op));
        true
    }

    /// Executes the given operation and decrements the queue size counter.
    ///
    /// # Arguments
    ///
    /// * `this` - Reference to the `IoPool` instance.
    /// * `op` - The operation to execute.
    fn perform_op(this: Arc<IoPool>, op: Arc<QueuedOp>) {
        op.execute();
        this.queue_size.fetch_sub(1, Ordering::Relaxed);
    }
}

/// An enumeration of possible asynchronous I/O operations.
///
/// Currently, only read operations are supported, but this enum can be
/// extended to support other types of I/O operations in the future.
pub enum QueuedOp {
    Read(QueuedRead),
}

impl QueuedOp {
    /// Creates a new read operation.
    ///
    /// # Arguments
    ///
    /// * `range` - The byte range to read.
    /// * `reader` - The reader implementation to use for the read operation.
    ///
    /// # Returns
    ///
    /// An `Arc`-wrapped `QueuedOp` instance configured for the read operation.
    pub fn new_read(range: Range<u64>, reader: Arc<dyn ReadAt>) -> Arc<QueuedOp> {
        Arc::new(QueuedOp::Read(QueuedRead::new(range, reader)))
    }

    /// Executes the I/O operation.
    ///
    /// This dispatches to the appropriate implementation based on the operation type.
    fn execute(&self) {
        match self {
            QueuedOp::Read(read) => read.execute(),
        }
    }

    /// Cancels the I/O operation and discards any results.
    ///
    /// This is useful for abandoning operations that are no longer needed.
    pub fn discard(&self) {
        match self {
            QueuedOp::Read(read) => read.discard(),
        }
    }

    /// Gets a reference to the underlying `QueuedRead` if this is a read operation.
    ///
    /// # Returns
    ///
    /// `Some(&QueuedRead)` if this is a read operation, `None` otherwise.
    pub fn as_read(&self) -> Option<&QueuedRead> {
        match self {
            QueuedOp::Read(read) => Some(read),
        }
    }
}

/// A read operation that can be queued for asynchronous execution.
///
/// This represents a read of a specific byte range from a reader implementing
/// the `ReadAt` trait. The operation can be executed asynchronously, and the
/// result can be retrieved once the operation completes.
pub struct QueuedRead {
    /// The byte range to read.
    pub range: Range<u64>,
    /// Storage for the read result. The inner `Option` is `None` if the read failed.
    // TODO: replace with OnceLock when `wait()` is stabilized.
    result: OnceCell<Option<Bytes>>,
    /// The reader implementation to use for the read operation.
    reader: Arc<dyn ReadAt>,
}

impl QueuedRead {
    /// Creates a new queued read operation.
    ///
    /// # Arguments
    ///
    /// * `range` - The byte range to read.
    /// * `reader` - The reader implementation to use.
    ///
    /// # Returns
    ///
    /// A new `QueuedRead` instance.
    pub fn new(range: Range<u64>, reader: Arc<dyn ReadAt>) -> QueuedRead {
        QueuedRead {
            range,
            result: OnceCell::new(),
            reader,
        }
    }

    /// Checks if this read operation fully contains the specified byte range.
    ///
    /// # Arguments
    ///
    /// * `range` - The byte range to check.
    ///
    /// # Returns
    ///
    /// `true` if the specified range is completely contained within this read's range.
    pub fn contains(&self, range: &Range<u64>) -> bool {
        !range.is_empty() && self.range.start <= range.start && self.range.end >= range.end
    }

    /// Gets the total size of this read operation in bytes.
    ///
    /// # Returns
    ///
    /// The size of the read operation in bytes.
    pub fn size(&self) -> usize {
        (self.range.end - self.range.start) as usize
    }

    /// Attempts to read a subset of the data from this read operation's result.
    ///
    /// This method will block until the read operation completes. If the operation
    /// has completed successfully, it returns the requested subset of the data.
    ///
    /// # Arguments
    ///
    /// * `range` - The byte range to read, which must be contained within this operation's range.
    ///
    /// # Returns
    ///
    /// `Some(Bytes)` containing the requested data, or `None` if the read operation failed.
    ///
    /// # Panics
    ///
    /// Panics if the requested range is not contained within this read operation's range.
    pub fn try_read(&self, range: &Range<u64>) -> Option<Bytes> {
        let bytes = self.result.wait().clone()?;
        assert!(range.start >= self.range.start);
        assert!(range.end <= self.range.end);
        let start = (range.start - self.range.start) as usize;
        let end = (range.end - self.range.start) as usize;
        Some(bytes.slice(start..end))
    }

    /// Executes the read operation if it hasn't been executed already.
    ///
    /// If the operation has already been executed, this method does nothing.
    /// Otherwise, it performs the read and stores the result.
    fn execute(&self) {
        if self.result.get().is_some() {
            return;
        }
        let bytes = self.reader.read_at(self.range.clone()).ok();
        let _ = self.result.set(bytes);
    }

    /// Discards this read operation by setting its result to `None`.
    ///
    /// This is useful for canceling operations that are no longer needed.
    pub fn discard(&self) {
        let _ = self.result.set(None);
    }
}

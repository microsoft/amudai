use amudai_bytes::buffer::AlignedByteVec;
use std::{
    ops::{Deref, DerefMut},
    sync::Mutex,
};

/// Simple pool of buffers that are reused by the encoders
/// to avoid unnecessary allocations.
///
/// The `BuffersPool` maintains a collection of `AlignedByteVec` instances
/// that can be borrowed and returned to reduce memory allocation overhead
/// during encoding operations. This is particularly beneficial in high-throughput
/// scenarios where many encoding operations are performed.
///
/// # Thread Safety
///
/// The pool is thread-safe and can be shared across multiple threads.
/// Buffer checkout and return operations are protected by a mutex.
///
/// # Usage
///
/// Buffers are automatically returned to the pool when the `BufferPoolRef`
/// is dropped, ensuring proper resource management without manual intervention.
pub struct BuffersPool {
    /// Thread-safe storage for pooled buffers.
    buffers: Mutex<Vec<AlignedByteVec>>,
}

impl BuffersPool {
    /// Creates a new empty buffer pool.
    ///
    /// # Returns
    ///
    /// A new `BuffersPool` instance with no pre-allocated buffers.
    pub fn new() -> Self {
        BuffersPool {
            buffers: Mutex::new(Vec::new()),
        }
    }

    /// Retrieves a buffer from the pool or creates a new one if the pool is empty.
    ///
    /// # Returns
    ///
    /// A `BufferPoolRef` that provides access to the buffer and automatically
    /// returns it to the pool when dropped.
    pub fn get_buffer(&self) -> BufferPoolRef<'_> {
        let mut buffers = self.buffers.lock().unwrap();
        let buffer = buffers.pop().unwrap_or_default();
        BufferPoolRef { pool: self, buffer }
    }

    /// Returns a buffer to the pool for reuse.
    ///
    /// The buffer is cleared before being added back to the pool to ensure
    /// clean state for the next use.
    ///
    /// # Arguments
    ///
    /// * `buffer` - The buffer to return to the pool
    fn return_buffer(&self, mut buffer: AlignedByteVec) {
        buffer.clear();
        let mut buffers = self.buffers.lock().unwrap();
        buffers.push(buffer);
    }
}

impl Default for BuffersPool {
    fn default() -> Self {
        Self::new()
    }
}

/// A reference to a buffer borrowed from a `BuffersPool`.
///
/// This type provides access to a pooled buffer and ensures that the buffer
/// is automatically returned to the pool when the reference is dropped.
/// It implements `Deref` and `DerefMut` to provide transparent access to
/// the underlying `AlignedByteVec`.
///
/// # Lifetime
///
/// The lifetime parameter `'a` ensures that the buffer reference cannot
/// outlive the pool it was borrowed from.
pub struct BufferPoolRef<'a> {
    /// Reference to the pool that owns this buffer.
    pool: &'a BuffersPool,
    /// The actual buffer data.
    buffer: AlignedByteVec,
}

impl Deref for BufferPoolRef<'_> {
    type Target = AlignedByteVec;

    /// Provides immutable access to the underlying buffer.
    fn deref(&self) -> &Self::Target {
        &self.buffer
    }
}

impl DerefMut for BufferPoolRef<'_> {
    /// Provides mutable access to the underlying buffer.
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.buffer
    }
}

impl<T> AsRef<[T]> for BufferPoolRef<'_>
where
    T: bytemuck::AnyBitPattern,
{
    /// Provides access to the buffer as a slice of type `T`.
    ///
    /// This allows the buffer to be interpreted as different data types
    /// through the `bytemuck` crate's safe casting mechanisms.
    fn as_ref(&self) -> &[T] {
        self.buffer.typed_data()
    }
}

impl Drop for BufferPoolRef<'_> {
    /// Automatically returns the buffer to the pool when the reference is dropped.
    ///
    /// This ensures that buffers are always returned to the pool for reuse,
    /// preventing memory leaks and reducing allocation overhead.
    fn drop(&mut self) {
        let buffer = std::mem::take(&mut self.buffer);
        self.pool.return_buffer(buffer);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_buffers_pool() {
        let pool = BuffersPool::new();
        let mut buffer = pool.get_buffer();
        assert_eq!(0, buffer.len());
        buffer.push_typed(1i64);
        buffer.push_typed(2i64);
        buffer.push_typed(3i64);
        assert_eq!(24, buffer.len());
        assert_eq!(0, pool.buffers.lock().unwrap().len());
        drop(buffer);
        assert_eq!(1, pool.buffers.lock().unwrap().len());

        let mut buffer = pool.get_buffer();
        let mut buffer2 = pool.get_buffer();
        assert_eq!(0, pool.buffers.lock().unwrap().len());
        assert_eq!(0, buffer.len());
        assert_eq!(0, buffer2.len());
        buffer.push_typed(1i64);
        assert_eq!(8, buffer.len());
        buffer2.push_typed(2i8);
        assert_eq!(1, buffer2.len());
        drop(buffer);
        assert_eq!(1, pool.buffers.lock().unwrap().len());
        drop(buffer2);
        assert_eq!(2, pool.buffers.lock().unwrap().len());
    }
}

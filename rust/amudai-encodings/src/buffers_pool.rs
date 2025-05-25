use amudai_bytes::buffer::AlignedByteVec;
use std::{
    ops::{Deref, DerefMut},
    sync::Mutex,
};

/// Simple pool of buffers that are reused by the encoders
/// to avoid unnecessary allocations.
pub struct BuffersPool {
    buffers: Mutex<Vec<AlignedByteVec>>,
}

impl BuffersPool {
    pub fn new() -> Self {
        BuffersPool {
            buffers: Mutex::new(Vec::new()),
        }
    }

    pub fn get_buffer(&self) -> BufferPoolRef<'_> {
        let mut buffers = self.buffers.lock().unwrap();
        let buffer = buffers.pop().unwrap_or_else(|| AlignedByteVec::new());
        BufferPoolRef { pool: self, buffer }
    }

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

pub struct BufferPoolRef<'a> {
    pool: &'a BuffersPool,
    buffer: AlignedByteVec,
}

impl Deref for BufferPoolRef<'_> {
    type Target = AlignedByteVec;

    fn deref(&self) -> &Self::Target {
        &self.buffer
    }
}

impl DerefMut for BufferPoolRef<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.buffer
    }
}

impl<T> AsRef<[T]> for BufferPoolRef<'_>
where
    T: bytemuck::AnyBitPattern,
{
    fn as_ref(&self) -> &[T] {
        self.buffer.typed_data()
    }
}

impl Drop for BufferPoolRef<'_> {
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

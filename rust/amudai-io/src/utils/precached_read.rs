//! A reader that maintains a pre-cached region of data from an underlying reader.

use amudai_bytes::{
    Bytes,
    align::{align_down_u64, align_up_u64},
    buffer::AlignedByteVec,
};
use std::ops::Range;

use crate::{ReadAt, StorageProfile};

/// A reader that maintains a pre-cached region of data from an underlying reader.
///
/// This implementation enhances read performance by storing a portion of the underlying
/// data in memory, typically either the beginning (prefix) or the end (suffix) of the reader.
/// Reads that are entirely within the cached region are served from memory, while other
/// reads are forwarded to the underlying reader.
///
/// This reader is particularly useful when accessing small to medium-sized shard
/// artifacts, such as the shard directory itself. To avoid multiple small
/// reads, the shard access flow pre-caches a fixed-size suffix of the shard directory
/// artifact, which contains the footer and the root metadata element.
pub struct PrecachedReadAt<R> {
    /// Underlying (source) reader
    inner: R,
    /// Total size of the source reader
    size: u64,
    /// Cached fragment of the source reader
    cached_buffer: Bytes,
    /// Offset of the cached fragment within the source reader
    cached_offset: u64,
}

impl<R> PrecachedReadAt<R> {
    /// Creates a new `PrecachedReadAt` with an explicitly positioned cached buffer.
    ///
    /// # Arguments
    ///
    /// * `inner` - The underlying reader
    /// * `size` - Total size of the underlying reader
    /// * `cached_buffer` - Buffer containing the cached data
    /// * `cached_offset` - Offset position of the cached buffer in the underlying reader
    pub fn new(inner: R, size: u64, cached_buffer: Bytes, cached_offset: u64) -> Self {
        Self {
            inner,
            size,
            cached_buffer,
            cached_offset,
        }
    }
}

impl<R: ReadAt> PrecachedReadAt<R> {
    /// Creates a new `PrecachedReadAt` that caches the first `prefix_size` bytes of the reader.
    ///
    /// If `prefix_size` is larger than the reader's size, the entire reader will be cached.
    ///
    /// # Arguments
    ///
    /// * `inner` - The underlying reader
    /// * `prefix_size` - Number of bytes to cache from the start of the reader
    ///
    /// # Returns
    ///
    /// A new `PrecachedReadAt` instance with the specified prefix cached
    pub fn from_prefix(inner: R, prefix_size: u64) -> std::io::Result<Self> {
        let size = inner.size()?;
        let cache_size = align_up_u64(prefix_size, 64).min(size);
        let cached_buffer = inner.read_at(0..cache_size)?;

        Ok(Self {
            inner,
            size,
            cached_buffer,
            cached_offset: 0,
        })
    }

    /// Creates a new `PrecachedReadAt` that caches the last `suffix_size` bytes of the reader.
    ///
    /// If `suffix_size` is larger than the reader's size, the entire reader will be cached.
    ///
    /// # Arguments
    ///
    /// * `inner` - The underlying reader
    /// * `suffix_size` - Number of bytes to cache from the end of the reader
    ///
    /// # Returns
    ///
    /// A new `PrecachedReadAt` instance with the specified suffix cached
    pub fn from_suffix(inner: R, suffix_size: u64) -> std::io::Result<Self> {
        let size = inner.size()?;
        let cache_size = suffix_size.min(size);
        let cached_offset = align_down_u64(
            size.saturating_sub(cache_size),
            AlignedByteVec::DEFAULT_ALIGNMENT as u64,
        );
        let cached_buffer = inner.read_at(cached_offset..size)?;

        Ok(Self {
            inner,
            size,
            cached_buffer,
            cached_offset,
        })
    }

    /// Creates a new `PrecachedReadAt` that caches a specific range of bytes from the reader.
    ///
    /// This method allows caching an arbitrary range of the underlying reader, which is useful
    /// when you know in advance which portion of the data will be accessed frequently.
    ///
    /// If the specified range is empty or invalid (start >= end after alignment), an empty
    /// cache will be created, and all reads will be forwarded to the underlying reader.
    ///
    /// # Arguments
    ///
    /// * `inner` - The underlying reader
    /// * `range` - The byte range to cache from the reader (start..end)
    ///
    /// # Returns
    ///
    /// A new `PrecachedReadAt` instance with the specified range cached
    ///
    /// # Behavior
    ///
    /// * The start position is aligned down to the default alignment boundary
    /// * The end position is aligned up to the default alignment boundary
    /// * The range is clamped to the reader's size if it extends beyond
    /// * If the aligned range is empty, creates an instance with no cached data
    pub fn from_range(inner: R, range: Range<u64>) -> std::io::Result<Self> {
        let size = inner.size()?;
        let start = align_down_u64(range.start, AlignedByteVec::DEFAULT_ALIGNMENT as u64);
        let end = align_up_u64(range.end, AlignedByteVec::DEFAULT_ALIGNMENT as u64).min(size);
        if start >= end {
            return Ok(Self {
                inner,
                size,
                cached_buffer: Bytes::new(),
                cached_offset: 0,
            });
        }

        let cached_buffer = inner.read_at(start..end)?;
        Ok(Self {
            inner,
            size,
            cached_buffer,
            cached_offset: start,
        })
    }

    /// Returns the size of the underlying object.
    pub fn object_size(&self) -> u64 {
        self.size
    }

    /// Returns the pre-cached buffer range.
    pub fn precached_range(&self) -> Range<u64> {
        self.cached_offset..self.cached_offset + self.cached_buffer.len() as u64
    }

    /// Returns the underlying reader.
    pub fn inner(&self) -> &R {
        &self.inner
    }

    /// Unwraps this `PrecachedReadAt`, returning the underlying reader.
    pub fn into_inner(self) -> R {
        self.inner
    }
}

impl<R: ReadAt> ReadAt for PrecachedReadAt<R> {
    fn size(&self) -> std::io::Result<u64> {
        Ok(self.size)
    }

    fn read_at(&self, range: Range<u64>) -> std::io::Result<Bytes> {
        if range.start >= self.size {
            return Ok(Bytes::new());
        }
        let range = range.start..range.end.min(self.size);

        // Check if the requested range is fully contained within the preloaded buffer
        let cached_end = self.cached_offset + self.cached_buffer.len() as u64;
        if range.start >= self.cached_offset && range.end <= cached_end {
            let buffer_start = (range.start - self.cached_offset) as usize;
            let buffer_end = (range.end - self.cached_offset) as usize;
            return Ok(self.cached_buffer.slice(buffer_start..buffer_end));
        }

        // If not in cached buffer, forward to inner reader
        self.inner.read_at(range)
    }

    fn storage_profile(&self) -> StorageProfile {
        if self.cached_offset == 0 && self.cached_buffer.len() as u64 == self.size {
            // The entire reader is cached in memory, we can give a very permissive
            // min_io_size (single byte)
            StorageProfile {
                min_io_size: 1,
                ..Default::default()
            }
        } else {
            self.inner.storage_profile()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{io, sync::Arc};

    fn create_test_data(size: usize) -> Vec<u8> {
        (0..size as u8).collect()
    }

    #[test]
    fn test_prefix_cache_full_hit() -> io::Result<()> {
        let data = create_test_data(100);
        let reader = Arc::new(data.clone());

        let cached = PrecachedReadAt::from_prefix(reader, 50)?;

        // Read fully within cached region
        let result = cached.read_at(10..30)?;
        assert_eq!(&result[..], &data[10..30]);
        Ok(())
    }

    #[test]
    fn test_suffix_cache_full_hit() -> io::Result<()> {
        let data = create_test_data(100);
        let reader = Arc::new(data.clone());

        let cached = PrecachedReadAt::from_suffix(reader, 50)?;

        // Read fully within cached region (last 50 bytes)
        let result = cached.read_at(60..80)?;
        assert_eq!(&result[..], &data[60..80]);
        Ok(())
    }

    #[test]
    fn test_prefix_cache_miss() -> io::Result<()> {
        let data = create_test_data(100);
        let reader = Arc::new(data.clone());

        let cached = PrecachedReadAt::from_prefix(reader, 50)?;

        // Read outside cached region
        let result = cached.read_at(60..80)?;
        assert_eq!(&result[..], &data[60..80]);
        Ok(())
    }

    #[test]
    fn test_suffix_cache_miss() -> io::Result<()> {
        let data = create_test_data(100);
        let reader = Arc::new(data.clone());

        let cached = PrecachedReadAt::from_suffix(reader, 50)?;

        // Read outside cached region
        let result = cached.read_at(10..30)?;
        assert_eq!(&result[..], &data[10..30]);
        Ok(())
    }

    #[test]
    fn test_empty_read() -> io::Result<()> {
        let data = create_test_data(100);
        let reader = Arc::new(data);

        let cached = PrecachedReadAt::from_prefix(reader, 50)?;

        // Read empty range
        let result = cached.read_at(10..10)?;
        assert!(result.is_empty());
        Ok(())
    }

    #[test]
    fn test_out_of_bounds_read() -> io::Result<()> {
        let data = create_test_data(100);
        let reader = Arc::new(data);

        let cached = PrecachedReadAt::from_prefix(reader, 50)?;

        // Read beyond file size
        let result = cached.read_at(150..200)?;
        assert!(result.is_empty());
        Ok(())
    }

    #[test]
    fn test_cache_larger_than_file() -> io::Result<()> {
        let data = create_test_data(50);
        let reader = Arc::new(data.clone());

        // Try to cache more than file size
        let cached = PrecachedReadAt::from_prefix(reader, 100)?;

        // Verify entire file is cached
        let result = cached.read_at(0..50)?;
        assert_eq!(&result[..], &data[..]);
        Ok(())
    }

    #[test]
    fn test_partial_read_at_end() -> io::Result<()> {
        let data = create_test_data(100);
        let reader = Arc::new(data.clone());

        let cached = PrecachedReadAt::from_prefix(reader, 50)?;

        // Read range that extends beyond file size
        let result = cached.read_at(90..150)?;
        assert_eq!(&result[..], &data[90..100]);
        Ok(())
    }

    #[test]
    fn test_zero_size_cache() -> io::Result<()> {
        let data = create_test_data(100);
        let reader = Arc::new(data.clone());

        let cached = PrecachedReadAt::from_prefix(reader, 0)?;

        // Should still work, just without caching
        let result = cached.read_at(10..30)?;
        assert_eq!(&result[..], &data[10..30]);
        Ok(())
    }
}

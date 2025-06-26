//! Byte buffers for use by the Amudai infrastructure, mutable and shared immutable,
//! with built-in support for proper alignment and padding.

use std::ops::RangeBounds;

use buffer::{AlignedByteVec, Buffer};

pub mod buffer;

/// A mutable buffer of bytes, conceptually similar to a `Vec<u8>`.
///
/// This struct is designed for efficiently building an immutable [`Bytes`] instance.
/// It provides methods for growing, shrinking, and manipulating the underlying byte buffer.
#[derive(Debug)]
pub struct BytesMut(AlignedByteVec);

impl BytesMut {
    /// Creates a new empty `BytesMut`.
    pub fn new() -> BytesMut {
        Self::with_capacity(0)
    }

    /// Creates a new `BytesMut` with the specified capacity.
    ///
    /// The buffer will be able to hold at least `capacity` bytes without reallocating.
    pub fn with_capacity(capacity: usize) -> BytesMut {
        BytesMut(AlignedByteVec::with_capacity(capacity))
    }

    /// Creates a new `BytesMut` with the specified capacity and alignment.
    ///
    /// The buffer will be able to hold at least `capacity` bytes without reallocating.
    pub fn with_capacity_and_alignment(capacity: usize, alignment: usize) -> BytesMut {
        BytesMut(AlignedByteVec::with_capacity_and_alignment(
            capacity, alignment,
        ))
    }

    /// Returns the length of the buffer.
    #[inline]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns `true` if the buffer is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns the capacity of the buffer.
    ///
    /// The capacity is the amount of space allocated for the buffer in terms of bytes.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.0.capacity()
    }

    /// Creates a new `BytesMut` with the specified length, filled with zero bytes.
    pub fn zeroed(len: usize) -> BytesMut {
        BytesMut(AlignedByteVec::zeroed(len))
    }

    /// Creates a new `BytesMut` containing a copy of the provided slice.
    pub fn copy_from_slice(s: &[u8]) -> BytesMut {
        BytesMut(AlignedByteVec::copy_from_slice(s))
    }

    /// Truncates the buffer to the specified length.
    ///
    /// If `len` is greater than the current length, this has no effect.
    #[inline]
    pub fn truncate(&mut self, len: usize) {
        self.0.truncate(len);
    }

    /// Clears the buffer, removing all its contents.
    pub fn clear(&mut self) {
        self.0.clear();
    }

    /// Resizes the buffer to the specified length.
    ///
    /// If `new_len` is greater than the current length, the buffer is extended with the given `value`.
    /// If `new_len` is less than the current length, the buffer is simply truncated.
    #[inline]
    pub fn resize(&mut self, new_len: usize, value: u8) {
        self.0.resize(new_len, value);
    }

    /// Reserves capacity for at least `additional` more bytes.
    ///
    /// The buffer may reserve more space than requested to avoid frequent reallocations.
    #[inline]
    pub fn reserve(&mut self, additional: usize) {
        self.0.reserve(additional);
    }

    /// Appends all bytes from a slice to the buffer.
    #[inline]
    pub fn extend_from_slice(&mut self, extend: &[u8]) {
        self.0.extend_from_slice(extend);
    }

    /// Returns a slice of the buffer's contents.
    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        self
    }

    /// Returns a mutable slice of the buffer's contents.
    #[inline]
    pub fn as_slice_mut(&mut self) -> &mut [u8] {
        self
    }

    /// Shrinks the capacity of the buffer as much as possible.
    pub fn shrink_to_fit(&mut self) {
        self.0.shrink_to_fit();
    }

    /// Consumes the `BytesMut` and converts it into an immutable `Bytes`.
    pub fn into_bytes(self) -> Bytes {
        Bytes(Buffer::from_byte_vec(self.0))
    }

    /// Consumes the `BytesMut` and returns the underlying `MutableBuffer`.
    pub fn into_inner(self) -> AlignedByteVec {
        self.0
    }
}

impl Default for BytesMut {
    fn default() -> Self {
        Self::with_capacity(0)
    }
}

impl std::ops::Deref for BytesMut {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.0.deref()
    }
}

impl std::ops::DerefMut for BytesMut {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0.deref_mut()
    }
}

impl From<Vec<u8>> for BytesMut {
    fn from(vec: Vec<u8>) -> Self {
        Self::from(vec.as_slice())
    }
}

impl From<String> for BytesMut {
    fn from(s: String) -> Self {
        Self::from(s.into_bytes())
    }
}

impl From<&str> for BytesMut {
    fn from(s: &str) -> Self {
        Self::from(s.as_bytes())
    }
}

impl From<&[u8]> for BytesMut {
    fn from(s: &[u8]) -> Self {
        BytesMut::copy_from_slice(s)
    }
}

/// A contiguous, immutable memory region that can be shared with other buffers and across
/// thread boundaries.
///
/// `Bytes` can be sliced and cloned without copying the underlying data.
///
/// The backing buffer is guaranteed to have at least 64-byte alignment when created from
/// `BytesMut`, and at least 16-byte alignment when created from `Vec<u8>`.
#[derive(Debug, Clone)]
pub struct Bytes(Buffer);

impl Bytes {
    pub const PREFERRED_ALIGNMENT: usize = 64;

    /// Creates a new empty `Bytes`.
    #[inline]
    pub fn new() -> Self {
        Bytes(Buffer::new())
    }

    /// Returns the length of the `Bytes`.
    #[inline]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns `true` if the `Bytes` is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Creates a new `Bytes` by copying data from a slice.
    pub fn copy_from_slice(data: &[u8]) -> Bytes {
        let mut bytes = BytesMut::with_capacity(data.len());
        bytes.extend_from_slice(data);
        bytes.into_bytes()
    }

    /// Creates a new `Bytes` by slicing the current `Bytes` within the given range.
    ///
    /// This operation is zero-copy; it does not allocate new memory.
    ///
    /// # Panics
    ///
    /// Panics if the range is out of bounds.
    pub fn slice(&self, range: impl RangeBounds<usize>) -> Bytes {
        Bytes(self.0.slice(range))
    }

    /// Checks if the `Bytes` instance is aligned to the specified alignment.
    ///
    /// # Arguments
    ///
    /// * `alignment` - The alignment to check for. This must be a power of two and
    ///   no greater than `128`.
    pub fn is_aligned(&self, alignment: usize) -> bool {
        self.0.is_aligned(alignment)
    }

    /// Checks if the `Bytes` instance is aligned to the specified alignment
    /// at the given offset.
    ///
    /// # Arguments
    ///
    /// * `offset` - The offset at which to check for alignment.
    /// * `alignment` - The alignment to check for. This must be a power of two and
    ///   no greater than `128`.
    ///
    /// # Panics
    ///
    /// Panics if the offset is greater than the buffer's length.
    pub fn is_aligned_at(&self, offset: usize, alignment: usize) -> bool {
        self.0.is_aligned_at(offset, alignment)
    }

    /// Creates a new `Bytes` instance by slicing the current `Bytes` within the specified range,
    /// ensuring that the resulting `Bytes` meets the requested `alignment`.
    ///
    /// This operation may or may not be zero-copy, depending on the starting offset of the sub-slice
    /// within the buffer.
    pub fn aligned_slice(&self, range: impl RangeBounds<usize>, alignment: usize) -> Bytes {
        Bytes(self.0.aligned_slice(range, alignment))
    }

    /// Aligns the buffer to the specified alignment. If the buffer is already aligned,
    /// it returns a cloned reference to the buffer. Otherwise, it creates a new buffer
    /// with a copy of the data, aligned to the specified alignment.
    ///
    /// # Arguments
    ///
    /// * `alignment` - The alignment value to which the buffer should be aligned.
    ///   This must be a power of two and no greater than `128`.
    pub fn align(&self, alignment: usize) -> Bytes {
        Bytes(self.0.align(alignment))
    }

    /// Consumes the `Bytes` and returns the underlying `Buffer`.
    pub fn into_inner(self) -> Buffer {
        self.0
    }
}

impl std::ops::Deref for Bytes {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.0.deref()
    }
}

impl Default for Bytes {
    fn default() -> Self {
        Self::new()
    }
}

impl From<Buffer> for Bytes {
    fn from(buf: Buffer) -> Self {
        Bytes(buf)
    }
}

impl From<BytesMut> for Bytes {
    fn from(bytes: BytesMut) -> Self {
        bytes.into_bytes()
    }
}

impl From<Vec<u8>> for Bytes {
    fn from(vec: Vec<u8>) -> Self {
        // TODO: optimize for cases where Vec is already properly aligned and padded.
        Bytes::copy_from_slice(&vec)
    }
}

impl From<String> for Bytes {
    fn from(s: String) -> Self {
        Self::from(s.into_bytes())
    }
}

impl From<&str> for Bytes {
    fn from(s: &str) -> Self {
        Self::copy_from_slice(s.as_bytes())
    }
}

impl From<&[u8]> for Bytes {
    fn from(s: &[u8]) -> Self {
        Self::copy_from_slice(s)
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Range;

    use super::*;

    #[test]
    fn test_bytes_mut_new() {
        let mut buffer = BytesMut::new();
        assert_eq!(buffer.len(), 0);
        assert_eq!(buffer.capacity(), 0);
        assert!(buffer.is_empty());

        buffer.extend_from_slice(b"hello");
        assert_eq!(buffer.len(), 5);
        assert!(!buffer.is_empty());
    }

    #[test]
    fn test_bytes_mut_with_capacity() {
        let buffer = BytesMut::with_capacity(10);
        assert_eq!(buffer.len(), 0);
        assert!(buffer.capacity() >= 10);
        assert!(buffer.is_empty());
    }

    #[test]
    fn test_bytes_mut_len() {
        let mut buffer = BytesMut::new();
        assert_eq!(buffer.len(), 0);

        buffer.extend_from_slice(b"hello");
        assert_eq!(buffer.len(), 5);
    }

    #[test]
    fn test_bytes_mut_is_empty() {
        let mut buffer = BytesMut::new();
        assert!(buffer.is_empty());

        buffer.extend_from_slice(b"hello");
        assert!(!buffer.is_empty());
    }

    #[test]
    fn test_bytes_mut_capacity() {
        let buffer = BytesMut::with_capacity(10);
        assert!(buffer.capacity() >= 10);
    }

    #[test]
    fn test_bytes_mut_zeroed() {
        let buffer = BytesMut::zeroed(5);
        assert_eq!(buffer.len(), 5);
        assert!(buffer.capacity() >= 5);
        assert_eq!(&buffer[..], &[0, 0, 0, 0, 0]);
    }

    #[test]
    fn test_bytes_mut_truncate() {
        let mut buffer = BytesMut::from(b"hello".as_ref());
        buffer.truncate(3);
        assert_eq!(buffer.len(), 3);
        assert_eq!(&buffer[..], b"hel");

        buffer.truncate(5); // Truncate to a length greater than current length
        assert_eq!(buffer.len(), 3);
        assert_eq!(&buffer[..], b"hel");
    }

    #[test]
    fn test_bytes_mut_clear() {
        let mut buffer = BytesMut::from(b"hello".as_ref());
        buffer.clear();
        assert_eq!(buffer.len(), 0);
        assert!(buffer.is_empty());
    }

    #[test]
    fn test_bytes_mut_resize() {
        let mut buffer = BytesMut::new();
        buffer.resize(5, b'a');
        assert_eq!(buffer.len(), 5);
        assert_eq!(&buffer[..], b"aaaaa");

        buffer.resize(3, b'b');
        assert_eq!(buffer.len(), 3);
        assert_eq!(&buffer[..], b"aaa");
    }

    #[test]
    fn test_bytes_mut_reserve() {
        let mut buffer = BytesMut::new();
        buffer.reserve(10);
        assert!(buffer.capacity() >= 10);

        buffer.extend_from_slice(b"hello");
        buffer.reserve(10);
        assert!(buffer.capacity() >= 15);
    }

    #[test]
    fn test_bytes_mut_extend_from_slice() {
        let mut buffer = BytesMut::new();
        buffer.extend_from_slice(b"hello");
        assert_eq!(buffer.len(), 5);
        assert_eq!(&buffer[..], b"hello");
    }

    #[test]
    fn test_bytes_mut_as_slice() {
        let buffer = BytesMut::from(b"hello".as_ref());
        let slice = buffer.as_slice();
        assert_eq!(slice, b"hello");
    }

    #[test]
    fn test_bytes_mut_as_slice_mut() {
        let mut buffer = BytesMut::from(b"hello".as_ref());
        let slice = buffer.as_slice_mut();
        slice[0] = b'H';
        assert_eq!(&buffer[..], b"Hello");
    }

    #[test]
    fn test_bytes_mut_into_bytes() {
        let buffer = BytesMut::from(b"hello".as_ref());
        let bytes = buffer.into_bytes();
        assert_eq!(&bytes[..], b"hello");
    }

    #[test]
    fn test_bytes_mut_into_inner() {
        let buffer = BytesMut::from(b"hello".as_ref());
        let inner = buffer.into_inner();
        assert_eq!(&inner[..], b"hello");
    }

    #[test]
    fn test_bytes_mut_default() {
        let buffer = BytesMut::default();
        assert_eq!(buffer.len(), 0);
        assert_eq!(buffer.capacity(), 0);
        assert!(buffer.is_empty());
    }

    #[test]
    fn test_bytes_mut_deref() {
        let buffer = BytesMut::from(b"hello".as_ref());
        assert_eq!(&buffer[..], b"hello");
    }

    #[test]
    fn test_bytes_mut_deref_mut() {
        let mut buffer = BytesMut::from(b"hello".as_ref());
        buffer[0] = b'H';
        assert_eq!(&buffer[..], b"Hello");
    }

    #[test]
    fn test_bytes_new() {
        let bytes = Bytes::new();
        assert_eq!(bytes.len(), 0);
        assert!(bytes.is_empty());
    }

    #[test]
    fn test_bytes_len() {
        let bytes = Bytes::from(b"hello".to_vec());
        assert_eq!(bytes.len(), 5);
    }

    #[test]
    fn test_bytes_is_empty() {
        let bytes = Bytes::new();
        assert!(bytes.is_empty());

        let bytes = Bytes::from(b"hello".to_vec());
        assert!(!bytes.is_empty());
    }

    #[test]
    fn test_bytes_copy_from_slice() {
        let bytes = Bytes::copy_from_slice(b"hello");
        assert_eq!(&bytes[..], b"hello");
    }

    #[test]
    fn test_bytes_slice() {
        let bytes = Bytes::from(b"hello world".to_vec());

        // Slice from start to end
        let slice = bytes.slice(..);
        assert_eq!(&slice[..], b"hello world");

        // Slice from start to a specific index
        let slice = bytes.slice(..5);
        assert_eq!(&slice[..], b"hello");

        // Slice from a specific index to end
        let slice = bytes.slice(6..);
        assert_eq!(&slice[..], b"world");

        // Slice with a specific range
        let slice = bytes.slice(2..7);
        assert_eq!(&slice[..], b"llo w");

        // Empty slice
        let slice = bytes.slice(2..2);
        assert_eq!(slice.len(), 0);
        assert!(slice.is_empty());

        // Slice from start to end using unbounded range
        let slice = bytes.slice(..);
        assert_eq!(&slice[..], b"hello world");

        // Slice from start to end using unbounded range
        let slice = bytes.slice(0..bytes.len());
        assert_eq!(&slice[..], b"hello world");

        // Slice from start to end using unbounded range
        let slice = bytes.slice(0..);
        assert_eq!(&slice[..], b"hello world");

        // Slice from start to end using unbounded range
        let slice = bytes.slice(..bytes.len());
        assert_eq!(&slice[..], b"hello world");
    }

    #[test]
    #[should_panic]
    fn test_bytes_slice_panic_start_greater_than_end() {
        let bytes = Bytes::from(b"hello world".to_vec());
        bytes.slice(Range { start: 7, end: 2 });
    }

    #[test]
    #[should_panic]
    fn test_bytes_slice_panic_end_out_of_bounds() {
        let bytes = Bytes::from(b"hello world".to_vec());
        bytes.slice(2..15);
    }

    #[test]
    fn test_bytes_into_inner() {
        let bytes = Bytes::from(b"hello".to_vec());
        let inner = bytes.into_inner();
        assert_eq!(&inner[..], b"hello");
    }

    #[test]
    fn test_bytes_deref() {
        let bytes = Bytes::from(b"hello".to_vec());
        assert_eq!(&bytes[..], b"hello");
    }

    #[test]
    fn test_bytes_from_buffer() {
        let buffer = Buffer::copy_from_slice(b"hello");
        let bytes = Bytes::from(buffer);
        assert_eq!(&bytes[..], b"hello");
    }

    #[test]
    fn test_bytes_from_bytes_mut() {
        let bytes_mut = BytesMut::from(b"hello".as_ref());
        let bytes = Bytes::from(bytes_mut);
        assert_eq!(&bytes[..], b"hello");
    }

    #[test]
    fn test_bytes_from_vec() {
        let vec = b"hello".to_vec();
        let bytes = Bytes::from(vec);
        assert_eq!(&bytes[..], b"hello");
    }

    #[test]
    fn test_bytes_from_string() {
        let string = "hello".to_string();
        let bytes = Bytes::from(string);
        assert_eq!(&bytes[..], b"hello");
    }

    #[test]
    fn test_bytes_from_str() {
        let string = "hello";
        let bytes = Bytes::from(string);
        assert_eq!(&bytes[..], b"hello");
    }

    #[test]
    fn test_bytes_from_slice() {
        let slice = b"hello";
        let bytes = Bytes::from(slice.as_ref());
        assert_eq!(&bytes[..], b"hello");
    }

    #[test]
    fn test_bytes_clone() {
        let bytes = Bytes::from(b"hello".to_vec());
        let cloned_bytes = bytes.clone();
        assert_eq!(&bytes[..], &cloned_bytes[..]);
    }

    #[test]
    fn test_bytes_alignment() {
        // Test alignment when created from BytesMut (64-byte alignment)
        let bytes_mut = BytesMut::from(b"hello".as_ref());
        let bytes = bytes_mut.into_bytes();
        assert_eq!((bytes.as_ptr() as usize) % 64, 0);

        // Test alignment when created from Vec<u8>
        let vec = b"hello".to_vec();
        let bytes = Bytes::from(vec);
        assert_eq!((bytes.as_ptr() as usize) % 64, 0);
    }

    #[test]
    fn test_bytes_alignment_checks() {
        let mut b = BytesMut::with_capacity(100);
        b.extend_from_slice(b"hello");
        b.resize(100, 0);
        let bytes = b.into_bytes();

        assert!(bytes.is_aligned(64));
        assert!(bytes.is_aligned_at(32, 32));
        assert!(!bytes.is_aligned_at(1, 2));
        let bytes = bytes.slice(1..);
        assert!(!bytes.is_aligned(64));
        assert_eq!(&bytes[..4], b"ello");
        let bytes2 = bytes.align(64);
        assert!(bytes2.is_aligned(64));
        assert_eq!(&bytes2[..4], b"ello");
    }

    #[cfg(test)]
    mod truncate_tests {
        use super::*;

        #[test]
        fn test_bytes_mut_truncate_basic() {
            let mut bytes = BytesMut::copy_from_slice(b"Hello, World!");
            assert_eq!(bytes.len(), 13);

            bytes.truncate(5);
            assert_eq!(bytes.len(), 5);
            assert_eq!(bytes.as_slice(), b"Hello");
        }

        #[test]
        fn test_bytes_mut_truncate_to_zero() {
            let mut bytes = BytesMut::copy_from_slice(b"Hello, World!");
            assert_eq!(bytes.len(), 13);

            bytes.truncate(0);
            assert_eq!(bytes.len(), 0);
            assert!(bytes.is_empty());
        }

        #[test]
        fn test_bytes_mut_truncate_larger_than_length() {
            let mut bytes = BytesMut::copy_from_slice(b"Hello");
            assert_eq!(bytes.len(), 5);

            // Truncating to larger size should have no effect
            bytes.truncate(10);
            assert_eq!(bytes.len(), 5);
            assert_eq!(bytes.as_slice(), b"Hello");
        }

        #[test]
        fn test_bytes_mut_truncate_same_length() {
            let mut bytes = BytesMut::copy_from_slice(b"Hello");
            assert_eq!(bytes.len(), 5);

            bytes.truncate(5);
            assert_eq!(bytes.len(), 5);
            assert_eq!(bytes.as_slice(), b"Hello");
        }

        #[test]
        fn test_bytes_mut_truncate_empty() {
            let mut bytes = BytesMut::new();
            assert_eq!(bytes.len(), 0);

            bytes.truncate(0);
            assert_eq!(bytes.len(), 0);

            bytes.truncate(10);
            assert_eq!(bytes.len(), 0);
        }

        #[test]
        fn test_bytes_mut_truncate_multiple_times() {
            let mut bytes = BytesMut::copy_from_slice(b"0123456789ABCDEF");
            assert_eq!(bytes.len(), 16);

            bytes.truncate(12);
            assert_eq!(bytes.len(), 12);
            assert_eq!(bytes.as_slice(), b"0123456789AB");

            bytes.truncate(8);
            assert_eq!(bytes.len(), 8);
            assert_eq!(bytes.as_slice(), b"01234567");

            bytes.truncate(4);
            assert_eq!(bytes.len(), 4);
            assert_eq!(bytes.as_slice(), b"0123");

            bytes.truncate(1);
            assert_eq!(bytes.len(), 1);
            assert_eq!(bytes.as_slice(), b"0");
        }

        #[test]
        fn test_bytes_mut_truncate_after_extend() {
            let mut bytes = BytesMut::copy_from_slice(b"Hello");
            bytes.extend_from_slice(b", World!");
            assert_eq!(bytes.len(), 13);
            assert_eq!(bytes.as_slice(), b"Hello, World!");

            bytes.truncate(7);
            assert_eq!(bytes.len(), 7);
            assert_eq!(bytes.as_slice(), b"Hello, ");
        }

        #[test]
        fn test_bytes_mut_truncate_large_buffer() {
            let large_data = vec![42u8; 10000];
            let mut bytes = BytesMut::copy_from_slice(&large_data);
            assert_eq!(bytes.len(), 10000);

            bytes.truncate(5000);
            assert_eq!(bytes.len(), 5000);
            assert!(bytes.as_slice().iter().all(|&b| b == 42));

            bytes.truncate(1000);
            assert_eq!(bytes.len(), 1000);
            assert!(bytes.as_slice().iter().all(|&b| b == 42));
        }

        #[test]
        fn test_bytes_mut_truncate_and_resize() {
            let mut bytes = BytesMut::copy_from_slice(b"Hello, World!");
            bytes.truncate(5);
            assert_eq!(bytes.as_slice(), b"Hello");

            // Resize after truncate
            bytes.resize(10, b'X');
            assert_eq!(bytes.as_slice(), b"HelloXXXXX");
        }

        #[test]
        fn test_bytes_mut_truncate_boundary_conditions() {
            let mut bytes = BytesMut::with_capacity(100);
            bytes.extend_from_slice(b"Test data");

            // Truncate to exact length
            bytes.truncate(9);
            assert_eq!(bytes.len(), 9);

            // Truncate to one less
            bytes.truncate(8);
            assert_eq!(bytes.len(), 8);
            assert_eq!(bytes.as_slice(), b"Test dat");

            // Truncate to one
            bytes.truncate(1);
            assert_eq!(bytes.len(), 1);
            assert_eq!(bytes.as_slice(), b"T");
        }
    }
}

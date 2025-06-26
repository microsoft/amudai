use std::{
    ops::{Range, RangeBounds},
    sync::Arc,
};

/// A byte vector that maintains memory alignment guarantees for its underlying storage.
///
/// This vector ensures its data is aligned to 128-byte boundaries and operates in 64-byte blocks.
///
/// # Performance
/// - All operations maintain 128-byte alignment for optimal memory access
/// - Memory is allocated and managed in 64-byte blocks
/// - Growth strategy doubles capacity while maintaining alignment requirements
pub struct AlignedByteVec {
    /// The underlying byte vector, may include padding at start
    inner: Vec<u8>,
    /// Offset from start of inner vec to maintain alignment
    start: u32,
    /// Required alignment, specified during vector creation.
    alignment: u32,
}

impl AlignedByteVec {
    /// Creates a new empty vector with no capacity allocation.
    pub fn new() -> AlignedByteVec {
        AlignedByteVec {
            inner: Vec::new(),
            start: 0,
            alignment: Self::ALIGNMENT as u32,
        }
    }

    /// Creates a new vector with the specified capacity, ensuring alignment requirements are met.
    pub fn with_capacity(capacity: usize) -> AlignedByteVec {
        Self::with_capacity_and_alignment(capacity, Self::ALIGNMENT)
    }

    /// Creates a new vector with the specified capacity and alignment.
    pub fn with_capacity_and_alignment(capacity: usize, alignment: usize) -> AlignedByteVec {
        Self::make(capacity, alignment)
    }

    /// Creates a new vector of specified length, filled with zeros.
    pub fn zeroed(len: usize) -> AlignedByteVec {
        AlignedByteVec::from_value(len, 0)
    }

    /// Creates a new vector of specified length, filled with the specified value.
    pub fn from_value(len: usize, value: u8) -> AlignedByteVec {
        let mut v = AlignedByteVec::with_capacity(len);
        v.resize(len, value);
        v
    }

    /// Creates a new vector containing a copy of the provided slice.
    pub fn copy_from_slice(data: &[u8]) -> AlignedByteVec {
        let mut vec = AlignedByteVec::with_capacity(data.len());
        vec.extend_from_slice(data);
        vec
    }

    /// Returns the number of bytes in the vector.
    #[inline]
    pub fn len(&self) -> usize {
        self.inner.len() - self.start_offset()
    }

    /// Returns true if the vector contains no elements.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the number of bytes the vector can hold without reallocating.
    #[inline]
    pub fn capacity(&self) -> usize {
        round_down(
            self.inner.capacity() - self.start_offset(),
            Self::BLOCK_SIZE,
        )
    }

    /// Returns a raw pointer to the vector's buffer.
    #[inline]
    pub fn as_ptr(&self) -> *const u8 {
        unsafe { self.inner.as_ptr().add(self.start_offset()) }
    }

    /// Returns a mutable raw pointer to the vector's buffer.
    #[inline]
    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        unsafe { self.inner.as_mut_ptr().add(self.start_offset()) }
    }

    /// Returns a slice containing the entire vector.
    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.as_ptr(), self.len()) }
    }

    /// Returns a mutable slice containing the entire vector.
    #[inline]
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.as_mut_ptr(), self.len()) }
    }

    // Reserves capacity for at least `additional` more bytes.
    #[inline]
    pub fn reserve(&mut self, additional: usize) {
        if self.capacity() - self.len() >= additional {
            return;
        }
        self.grow(additional);
    }

    /// Appends a slice to the vector.
    #[inline]
    pub fn extend_from_slice(&mut self, s: &[u8]) {
        self.reserve(s.len());
        self.inner.extend_from_slice(s);
    }

    /// Resizes the vector to the specified length, filling any new space with the given value.
    pub fn resize(&mut self, new_len: usize, value: u8) {
        let len = self.len();
        if new_len > len {
            self.reserve(new_len - len);
            unsafe {
                self.as_mut_ptr().add(len).write_bytes(value, new_len - len);
                self.inner.set_len(self.start_offset() + new_len);
            }
        } else {
            self.inner.truncate(self.start_offset() + new_len);
        }
    }

    /// Sets the length of the vector directly without initializing any new elements.
    ///
    /// This function directly modifies the length of the underlying vector without
    /// initializing any memory between the current length and the new length. This is
    /// a very low-level operation that bypasses normal safety checks and should be used
    /// with extreme caution.
    ///
    /// # Arguments
    ///
    /// * `new_len` - The new length for the vector, in bytes. Must not exceed the current capacity.
    ///
    /// # Safety
    ///
    /// This function is unsafe because it can violate memory safety: if `new_len` is greater
    /// than the current length,the memory between the old and new lengths will contain
    /// uninitialized data. Accessing this memory without first initializing it is undefined
    /// behavior.
    ///
    /// # Panics
    ///
    /// Panics if `new_len` exceeds the vector's current capacity.
    pub unsafe fn set_len(&mut self, new_len: usize) {
        assert!(new_len <= self.capacity());
        unsafe {
            self.inner.set_len(self.start_offset() + new_len);
        }
    }

    /// Truncates the vector to the specified length.
    pub fn truncate(&mut self, new_len: usize) {
        self.inner.truncate(self.start_offset() + new_len);
    }

    /// Clears the vector, removing all values.
    pub fn clear(&mut self) {
        self.truncate(0);
    }

    /// Shrinks the capacity of the vector as much as possible.
    pub fn shrink_to_fit(&mut self) {
        let vec_capacity = round_up(self.len(), Self::BLOCK_SIZE)
            .checked_add(Self::ALIGNMENT)
            .expect("add");
        if vec_capacity < self.inner.capacity() {
            *self = AlignedByteVec::copy_from_slice(self.as_slice());
        }
    }

    /// Returns the total allocated size in bytes, including alignment and padding.
    pub fn heap_size(&self) -> usize {
        self.inner.capacity()
    }

    /// Checks if the buffer is aligned to the specified alignment at the given offset.
    ///
    /// # Arguments
    ///
    /// * `offset` - The offset (byte index) at which to check for alignment.
    /// * `alignment` - The alignment to check for. This must be a power of two and
    ///   no greater than `128`.
    ///
    /// # Panics
    ///
    /// Panics if the offset is greater than the buffer's length.
    pub fn is_aligned_at(&self, offset: usize, alignment: usize) -> bool {
        assert!(offset <= self.len());
        self.is_aligned_index(offset, alignment)
    }

    /// Consumes the `AlignedByteVec`, returning both the inner vector and the offset
    /// at which the aligned data starts within the vector.
    pub fn into_vec(self) -> (Vec<u8>, usize) {
        (self.inner, self.start as usize)
    }
}

impl AlignedByteVec {
    /// Appends a value of type `T` to the vector by copying its bytes.
    ///
    /// # Safety
    ///
    /// The type `T` must implement `bytemuck::NoUninit` to ensure safe memory access.
    #[inline]
    pub fn push_typed<T>(&mut self, value: T)
    where
        T: bytemuck::NoUninit,
    {
        self.extend_from_slice(bytemuck::bytes_of(&value));
    }

    /// Resizes the vector to the specified count of elements of type `T`, filling
    /// any new space with the given value.
    pub fn resize_typed<T>(&mut self, new_count: usize, value: T)
    where
        T: bytemuck::AnyBitPattern + bytemuck::NoUninit,
    {
        let count = self.len() / std::mem::size_of::<T>();
        let size = count * std::mem::size_of::<T>();
        let new_size = new_count * std::mem::size_of::<T>();
        if new_size > size {
            self.reserve(new_size - size);
            let extra_count = new_count - count;
            unsafe {
                let target = self.as_mut_ptr().add(size) as *mut T;
                for i in 0..extra_count {
                    std::ptr::write(target.add(i), value);
                }
                self.inner.set_len(self.start_offset() + new_size);
            }
        } else {
            self.inner.truncate(self.start_offset() + new_size);
        }
    }

    /// Resizes the vector to the specified count of elements of type `T`, filling
    /// any new space with zeros.
    pub fn resize_zeroed<T>(&mut self, new_count: usize)
    where
        T: bytemuck::AnyBitPattern + bytemuck::NoUninit,
    {
        let new_size = new_count * std::mem::size_of::<T>();
        self.resize(new_size, 0);
    }

    /// Appends a slice of values of type `T` to the vector by copying their bytes.
    ///
    /// # Safety
    ///
    /// The type `T` must implement `bytemuck::NoUninit` to ensure safe memory access.
    #[inline]
    pub fn extend_from_typed_slice<T>(&mut self, values: &[T])
    where
        T: bytemuck::NoUninit,
    {
        self.extend_from_slice(bytemuck::cast_slice(values));
    }

    /// Returns a slice of `T` values from the vector's data.
    ///
    /// # Safety
    ///
    /// This function performs a raw cast and relies on the caller to ensure that the underlying data
    /// is valid for the type `T`. The type `T` must implement `bytemuck::AnyBitPattern`.
    #[inline]
    pub fn typed_data<T>(&self) -> &[T]
    where
        T: bytemuck::AnyBitPattern,
    {
        bytemuck::cast_slice(self.as_slice())
    }

    /// Returns a mutable slice of `T` values from the vector's data.
    ///
    /// # Safety
    ///
    /// This function performs a raw cast and relies on the caller to ensure that the underlying data
    /// is valid for the type `T`. The type `T` must implement `bytemuck::AnyBitPattern + bytemuck::NoUninit`.
    #[inline]
    pub fn typed_data_mut<T>(&mut self) -> &mut [T]
    where
        T: bytemuck::AnyBitPattern + bytemuck::NoUninit,
    {
        bytemuck::cast_slice_mut(self.as_mut_slice())
    }
}

impl AlignedByteVec {
    /// Required alignment in bytes
    const ALIGNMENT: usize = 128;
    /// Block size for capacity calculations
    const BLOCK_SIZE: usize = 64;

    /// Creates a new vector with the specified capacity, ensuring alignment requirements.
    fn make(capacity: usize, alignment: usize) -> AlignedByteVec {
        let alignment = alignment.max(1);
        assert!(alignment.is_power_of_two());

        if capacity == 0 {
            return AlignedByteVec {
                inner: Vec::new(),
                start: 0,
                alignment: alignment as u32,
            };
        }

        let vec_capacity = round_up(capacity, Self::BLOCK_SIZE)
            .checked_add(alignment)
            .expect("add");

        let mut vec = Vec::<u8>::with_capacity(vec_capacity);

        let p = vec.as_ptr() as usize;
        let aligned = round_up(p, alignment);
        let start = aligned - p;
        if start != 0 {
            unsafe {
                vec.as_mut_ptr().write_bytes(0, start);
                vec.set_len(start);
            }
        }

        let res = AlignedByteVec {
            inner: vec,
            start: start as u32,
            alignment: alignment as u32,
        };
        assert!(res.capacity() >= capacity);
        res
    }

    /// Grows the vector's capacity to accommodate at least `additional` more bytes.
    #[cold]
    fn grow(&mut self, additional: usize) {
        let new_cap = round_up(
            self.len().checked_add(additional).expect("add"),
            Self::BLOCK_SIZE,
        );
        let new_cap = std::cmp::max(self.capacity() * 2, new_cap);
        let alignment = self.alignment as usize;
        let mut v = Self::make(new_cap, alignment);
        if !self.is_empty() {
            v.inner.extend_from_slice(self.as_slice());
        }
        *self = v;
    }

    /// Checks if the memory at the given index is aligned to the specified alignment.
    ///
    /// # Arguments
    ///
    /// * `index` - The index to check for alignment.
    /// * `alignment` - The alignment to check for.
    #[inline]
    fn is_aligned_index(&self, index: usize, alignment: usize) -> bool {
        is_aligned(unsafe { self.as_ptr().add(index) }, alignment)
    }

    #[inline]
    fn start_offset(&self) -> usize {
        self.start as usize
    }
}

impl std::ops::Deref for AlignedByteVec {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl std::ops::DerefMut for AlignedByteVec {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_mut_slice()
    }
}

impl Clone for AlignedByteVec {
    fn clone(&self) -> AlignedByteVec {
        let mut v = AlignedByteVec::with_capacity(self.len());
        if !self.is_empty() {
            v.extend_from_slice(self.as_slice());
        }
        v
    }
}

impl std::fmt::Debug for AlignedByteVec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AlignedByteVec")
            .field("values", &self.as_slice())
            .field("len", &self.len())
            .field("cap", &self.capacity())
            .field("internal_offset", &self.start)
            .field("internal_cap", &self.inner.capacity())
            .finish_non_exhaustive()
    }
}

impl Default for AlignedByteVec {
    fn default() -> Self {
        Self::new()
    }
}

impl std::io::Write for AlignedByteVec {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

unsafe impl MemoryOwner for AlignedByteVec {
    fn memory(&self) -> MemoryAllocation {
        MemoryAllocation {
            ptr: self.as_ptr(),
            len: self.len(),
            capacity: self.capacity(),
        }
    }
}

/// `Buffer` represents a contiguous, immutable memory region with shared ownership
/// semantics.
///
/// `Buffers` can be sliced and cloned without copying the underlying data.
///
/// `Buffer` provides a view into memory owned by a `MemoryOwner`, ensuring the memory
/// remains valid for the lifetime of the buffer through reference counting.
/// The underlying memory is guaranteed to be 64-byte aligned and have a valid capacity
/// that is a multiple of 64 bytes.
#[derive(Clone)]
pub struct Buffer {
    ptr: *const u8,
    len: usize,
    owner: BufOwner,
}

unsafe impl Send for Buffer {}

unsafe impl Sync for Buffer {}

impl Buffer {
    /// Creates a new empty buffer.
    pub fn new() -> Buffer {
        Self::from_byte_vec(AlignedByteVec::new())
    }

    /// Creates a new buffer that takes ownership of the provided `AlignedByteVec`.
    pub fn from_byte_vec(vec: AlignedByteVec) -> Buffer {
        let vec = Arc::new(vec);
        let ptr = vec.as_ptr();
        let len = vec.len();
        Buffer {
            ptr,
            len,
            owner: BufOwner::Vec(vec),
        }
    }

    /// Creates a new buffer from any type implementing `MemoryOwner`.
    ///
    /// # Panics
    ///
    /// Panics if:
    /// - The memory is not 64-byte aligned
    /// - The capacity is less than the length
    /// - The capacity is not a multiple of 64
    pub fn from_owner(owner: Arc<dyn MemoryOwner + Send + Sync + 'static>) -> Buffer {
        let MemoryAllocation { ptr, len, capacity } = owner.memory();
        assert!(is_aligned(ptr, 64));
        assert!(capacity >= len);
        assert_eq!(capacity % 64, 0);
        Buffer {
            ptr,
            len,
            owner: BufOwner::External(owner),
        }
    }

    /// Creates a new buffer of the specified length, initialized with zero bytes.
    pub fn zeroed(len: usize) -> Self {
        Self::from_byte_vec(AlignedByteVec::zeroed(len))
    }

    /// Creates a new buffer containing a copy of the provided slice.
    pub fn copy_from_slice(data: &[u8]) -> Buffer {
        let mut vec = AlignedByteVec::with_capacity(data.len());
        vec.extend_from_slice(data);
        Self::from_byte_vec(vec)
    }

    /// Returns the length of the buffer.
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns `true` if the buffer is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Returns a reference to the buffer contents as a byte slice.
    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }

    /// Creates a new buffer representing a subrange of this buffer.
    ///
    /// The returned buffer shares ownership of the underlying memory with the original buffer.
    ///
    /// # Panics
    ///
    /// Panics if:
    /// - The start index is greater than the end index
    /// - The end index is greater than the buffer's length
    /// - Either index arithmetic overflows
    pub fn slice(&self, range: impl RangeBounds<usize>) -> Self {
        let range = self.verify_range(range);
        self.make_slice(range)
    }

    /// Checks if the buffer is aligned to the specified alignment.
    ///
    /// # Arguments
    ///
    /// * `alignment` - The alignment to check for. This must be a power of two and
    ///   no greater than `128`.
    pub fn is_aligned(&self, alignment: usize) -> bool {
        self.is_aligned_index(0, alignment)
    }

    /// Checks if the buffer is aligned to the specified alignment at the given offset.
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
        assert!(offset <= self.len());
        self.is_aligned_index(offset, alignment)
    }

    /// Creates a new buffer that represents a subrange of the current buffer, with the slice
    /// pointer aligned to the specified `alignment`.
    ///
    /// # Parameters
    ///
    /// - `range`: A range specifying the subrange of the buffer.
    /// - `alignment`: The desired alignment for the slice pointer. It must be a power of two
    ///   and should not exceed `AlignedByteVec::ALIGNMENT`.
    ///
    /// # Returns
    ///
    /// A new buffer that is either a slice of the current buffer with the specified alignment
    /// or a copy if the alignment requirements are not met.
    pub fn aligned_slice(&self, range: impl RangeBounds<usize>, alignment: usize) -> Self {
        assert!(alignment <= AlignedByteVec::ALIGNMENT && alignment.is_power_of_two());
        let range = self.verify_range(range);
        if self.is_aligned_index(range.start, alignment) {
            self.make_slice(range)
        } else {
            Buffer::copy_from_slice(&self[range])
        }
    }

    /// Aligns the buffer to the specified alignment. If the buffer is already aligned,
    /// it returns a clone of the buffer. Otherwise, it creates a new buffer with a copy
    /// of the data, aligned to the specified alignment.
    ///
    /// # Arguments
    ///
    /// * `alignment` - The alignment value to which the buffer should be aligned.
    ///   This must be a power of two and no greater than `128`.
    pub fn align(&self, alignment: usize) -> Self {
        if self.is_aligned_index(0, alignment) {
            self.clone()
        } else {
            Buffer::copy_from_slice(self.as_slice())
        }
    }

    /// Consumes the buffer and returns the underlying memory owner.
    pub fn into_owner(self) -> Arc<dyn MemoryOwner + Send + Sync + 'static> {
        self.owner.into_owner()
    }

    /// Attempts to consume the buffer and return an `AlignedByteVec`, provided that
    /// it is the owner of the underlying memory and the buffer is not shared.
    pub fn try_into_byte_vec(self) -> Result<AlignedByteVec, Buffer> {
        self.owner.try_unwrap_vec().map_err(|owner| Buffer {
            ptr: self.ptr,
            len: self.len,
            owner,
        })
    }
}

impl Buffer {
    /// Returns a slice of `T` values from the buffer.
    ///
    /// # Safety
    ///
    /// This function performs a raw cast and relies on the caller to ensure that the underlying data
    /// is valid for the type `T`. The type `T` must implement `bytemuck::AnyBitPattern`.
    #[inline]
    pub fn typed_data<T>(&self) -> &[T]
    where
        T: bytemuck::AnyBitPattern,
    {
        bytemuck::cast_slice(self.as_slice())
    }
}

impl Buffer {
    /// Verifies that the given range is valid for this buffer.
    ///
    /// # Arguments
    ///
    /// * `range` - The range to verify.
    ///
    /// # Panics
    ///
    /// Panics if:
    /// - The start index is greater than the end index
    /// - The end index is greater than the buffer's length
    /// - Index calculation results in arithmetic overflow
    fn verify_range(&self, range: impl RangeBounds<usize>) -> Range<usize> {
        use core::ops::Bound;

        let len = self.len();

        let start = match range.start_bound() {
            Bound::Included(&n) => n,
            Bound::Excluded(&n) => n.checked_add(1).expect("out of range"),
            Bound::Unbounded => 0,
        };

        let end = match range.end_bound() {
            Bound::Included(&n) => n.checked_add(1).expect("out of range"),
            Bound::Excluded(&n) => n,
            Bound::Unbounded => len,
        };

        assert!(
            start <= end,
            "range start must not be greater than end: {:?} <= {:?}",
            start,
            end,
        );
        assert!(
            end <= len,
            "range end out of bounds: {:?} <= {:?}",
            end,
            len,
        );

        start..end
    }

    /// Creates a new buffer representing a subrange of this buffer.
    ///
    /// # Arguments
    ///
    /// * `range` - The range of the slice.
    fn make_slice(&self, range: Range<usize>) -> Buffer {
        let ptr = unsafe { self.ptr.add(range.start) };
        Buffer {
            ptr,
            len: range.end - range.start,
            owner: self.owner.clone(),
        }
    }

    /// Checks if the memory at the given index is aligned to the specified alignment.
    ///
    /// # Arguments
    ///
    /// * `index` - The index to check for alignment.
    /// * `alignment` - The alignment to check for.
    #[inline]
    fn is_aligned_index(&self, index: usize, alignment: usize) -> bool {
        is_aligned(unsafe { self.as_ptr().add(index) }, alignment)
    }
}

impl std::ops::Deref for Buffer {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl std::fmt::Debug for Buffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.as_slice().fmt(f)
    }
}

impl Default for Buffer {
    fn default() -> Self {
        Self::new()
    }
}

impl From<AlignedByteVec> for Buffer {
    fn from(vec: AlignedByteVec) -> Buffer {
        Buffer::from_byte_vec(vec)
    }
}

#[derive(Clone)]
enum BufOwner {
    Vec(Arc<AlignedByteVec>),
    External(Arc<dyn MemoryOwner + Send + Sync + 'static>),
}

impl BufOwner {
    fn into_owner(self) -> Arc<dyn MemoryOwner + Send + Sync + 'static> {
        match self {
            BufOwner::Vec(vec) => vec,
            BufOwner::External(memory_owner) => memory_owner,
        }
    }

    fn try_unwrap_vec(self) -> Result<AlignedByteVec, BufOwner> {
        match self {
            BufOwner::Vec(vec) => Arc::try_unwrap(vec).map_err(BufOwner::Vec),
            BufOwner::External(owner) => Err(BufOwner::External(owner)),
        }
    }
}

/// Represents a block of allocated memory with its size information.
#[derive(Debug, Clone)]
pub struct MemoryAllocation {
    /// Pointer to the start of the allocated memory.
    pub ptr: *const u8,
    /// Current length of the allocated memory in bytes
    pub len: usize,
    /// Total capacity of the allocated memory in bytes
    pub capacity: usize,
}

/// Trait for types that own aligned memory buffers.
///
/// # Safety
///
/// Implementors must guarantee that:
/// - The memory returned by `memory()` remains valid and immutable
///   for the entire lifetime of the owner.
/// - The memory is at least 64-byte aligned.
/// - The reported length and capacity are accurate.
/// - The capacity is a multiple of 64 bytes.
pub unsafe trait MemoryOwner {
    /// Returns information about the owned memory block.
    fn memory(&self) -> MemoryAllocation;
}

/// Rounds up a number to the next multiple of block_size.
#[inline]
fn round_up(n: usize, block_size: usize) -> usize {
    n.checked_add(block_size - 1).expect("add") & !(block_size - 1)
}

/// Rounds down a number to the previous multiple of block_size.
#[inline]
fn round_down(n: usize, block_size: usize) -> usize {
    n & !(block_size - 1)
}

#[inline]
fn is_aligned(ptr: *const u8, alignment: usize) -> bool {
    alignment.is_power_of_two() && ((ptr as usize) & (alignment - 1)) == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_buffer_new() {
        let buf = Buffer::new();
        assert!(buf.is_empty());
        assert_eq!(buf.len(), 0);
        assert_eq!(buf.as_slice(), &[]);
    }

    #[test]
    fn test_buffer_zeroed() {
        let len = 128;
        let buf = Buffer::zeroed(len);
        assert_eq!(buf.len(), len);
        assert!(!buf.is_empty());
        assert!(buf.as_slice().iter().all(|&x| x == 0));
    }

    #[test]
    fn test_buffer_copy_from_slice() {
        let data = vec![1, 2, 3, 4, 5];
        let buf = Buffer::copy_from_slice(&data);
        assert_eq!(buf.len(), data.len());
        assert_eq!(buf.as_slice(), &data);
    }

    #[test]
    fn test_buffer_slice() {
        let data = vec![1, 2, 3, 4, 5];
        let buf = Buffer::copy_from_slice(&data);

        // Test various slice ranges
        let slice1 = buf.slice(1..4);
        assert_eq!(slice1.as_slice(), &[2, 3, 4]);

        let slice2 = buf.slice(..3);
        assert_eq!(slice2.as_slice(), &[1, 2, 3]);

        let slice3 = buf.slice(3..);
        assert_eq!(slice3.as_slice(), &[4, 5]);

        let slice4 = buf.slice(..);
        assert_eq!(slice4.as_slice(), &data);
    }

    #[test]
    #[should_panic(expected = "range end out of bounds")]
    fn test_buffer_slice_out_of_bounds() {
        let buf = Buffer::copy_from_slice(&[1, 2, 3]);
        buf.slice(1..4); // Should panic
    }

    #[test]
    #[should_panic(expected = "range start must not be greater than end")]
    fn test_buffer_slice_invalid_range() {
        let buf = Buffer::copy_from_slice(&[1, 2, 3]);
        buf.slice(Range { start: 2, end: 1 }); // Should panic
    }

    #[test]
    fn test_buffer_clone() {
        let original = Buffer::copy_from_slice(&[1, 2, 3]);
        let cloned = original.clone();

        assert_eq!(original.as_slice(), cloned.as_slice());
        assert_eq!(original.len(), cloned.len());

        assert_eq!(original.ptr, cloned.ptr);
    }

    #[test]
    fn test_buffer_try_into_byte_vec() {
        let data = vec![1, 2, 3];
        let buf = Buffer::copy_from_slice(&data);

        // Test successful conversion when buffer is not shared
        match buf.try_into_byte_vec() {
            Ok(vec) => assert_eq!(vec.as_slice(), &data),
            Err(_) => panic!("Expected successful conversion"),
        }

        // Test failed conversion when buffer is shared
        let buf1 = Buffer::copy_from_slice(&data);
        let _buf2 = buf1.clone();

        match buf1.try_into_byte_vec() {
            Ok(_) => panic!("Expected failed conversion due to sharing"),
            Err(buffer) => assert_eq!(buffer.as_slice(), &data),
        }
    }

    // Custom MemoryOwner implementation for testing
    #[derive(Clone)]
    struct TestMemoryOwner {
        data: AlignedByteVec,
    }

    unsafe impl MemoryOwner for TestMemoryOwner {
        fn memory(&self) -> MemoryAllocation {
            MemoryAllocation {
                ptr: self.data.as_ptr(),
                len: self.data.len(),
                capacity: self.data.capacity(),
            }
        }
    }

    #[test]
    fn test_buffer_from_owner() {
        let data = vec![1, 2, 3, 4];
        let vec = AlignedByteVec::copy_from_slice(&data);
        let owner = Arc::new(TestMemoryOwner { data: vec });

        let buf = Buffer::from_owner(owner.clone());
        assert_eq!(buf.as_slice(), &data);

        let _owner2 = buf.into_owner();
        assert_eq!(Arc::strong_count(&owner), 2);
    }

    #[test]
    fn test_buffer_deref() {
        let data = vec![1, 2, 3];
        let buf = Buffer::copy_from_slice(&data);

        let slice: &[u8] = &buf;
        assert_eq!(slice, &data);
    }

    #[test]
    fn test_buffer_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<Buffer>();
    }

    #[test]
    fn test_zero_length_buffer() {
        let buf = Buffer::zeroed(0);
        assert!(buf.is_empty());
        assert_eq!(buf.len(), 0);

        let sliced = buf.slice(0..0);
        assert!(sliced.is_empty());
    }

    #[test]
    fn test_buffer_from_aligned_byte_vec() {
        let mut vec = AlignedByteVec::new();
        vec.extend_from_slice(&[1, 2, 3]);

        let buf: Buffer = vec.clone().into();
        assert_eq!(buf.as_slice(), &[1, 2, 3]);
    }

    #[test]
    fn test_aligned_vec_new() {
        let vec = AlignedByteVec::new();
        assert_eq!(vec.len(), 0);
        assert_eq!(vec.capacity(), 0);
        assert!(vec.is_empty());
    }

    #[test]
    fn test_aligned_vec_alignment_guarantees() {
        let mut vec = AlignedByteVec::with_capacity(1000);
        assert!(is_aligned(vec.as_ptr(), AlignedByteVec::ALIGNMENT));

        // Check alignment after various operations
        vec.extend_from_slice(&[1; 100]);
        assert!(is_aligned(vec.as_ptr(), AlignedByteVec::ALIGNMENT));

        vec.resize(500, 0);
        assert!(is_aligned(vec.as_ptr(), AlignedByteVec::ALIGNMENT));

        vec.truncate(50);
        assert!(is_aligned(vec.as_ptr(), AlignedByteVec::ALIGNMENT));

        let clone = vec.clone();
        assert!(is_aligned(clone.as_ptr(), AlignedByteVec::ALIGNMENT));
    }

    #[test]
    fn test_aligned_vec_block_size_capacity() {
        for size in [1, 63, 64, 65, 127, 128, 129, 1000] {
            let vec = AlignedByteVec::with_capacity(size);
            assert_eq!(vec.capacity() % AlignedByteVec::BLOCK_SIZE, 0);
            assert!(vec.capacity() >= size);
        }
    }

    #[test]
    fn test_aligned_vec_zeroed() {
        let size = 1000;
        let vec = AlignedByteVec::zeroed(size);
        assert_eq!(vec.len(), size);
        assert!(vec.iter().all(|&x| x == 0));
        assert!(is_aligned(vec.as_ptr(), AlignedByteVec::ALIGNMENT));
    }

    #[test]
    fn test_aligned_vec_extend_and_resize() {
        let mut vec = AlignedByteVec::new();

        let data = vec![1, 2, 3, 4, 5];
        vec.extend_from_slice(&data);
        assert_eq!(vec.as_slice(), &data);

        vec.resize(10, 0);
        assert_eq!(vec.len(), 10);
        assert_eq!(&vec[..5], &data);
        assert!(vec[5..].iter().all(|&x| x == 0));

        vec.resize(3, 0);
        assert_eq!(vec.len(), 3);
        assert_eq!(vec.as_slice(), &[1, 2, 3]);
    }

    #[test]
    fn test_aligned_vec_growth_strategy() {
        let mut vec = AlignedByteVec::new();
        let mut last_capacity = vec.capacity();

        for i in 0..10 {
            vec.extend_from_slice(&[i as u8; 100]);
            let new_capacity = vec.capacity();
            if last_capacity > 0 {
                assert!(
                    new_capacity == last_capacity || new_capacity >= last_capacity * 2,
                    "new_capacity = {new_capacity},\
                    last_capacity = {last_capacity}, i = {i}, len = {}",
                    vec.len()
                );
            }
            last_capacity = new_capacity;

            // Verify alignment is maintained after growth
            assert!(is_aligned(vec.as_ptr(), AlignedByteVec::ALIGNMENT));
        }
    }

    #[test]
    fn test_aligned_vec_clone() {
        let mut original = AlignedByteVec::with_capacity(100);
        original.extend_from_slice(&[1, 2, 3, 4, 5]);

        let clone = original.clone();
        assert_eq!(original.as_slice(), clone.as_slice());
        assert!(is_aligned(clone.as_ptr(), AlignedByteVec::ALIGNMENT));
        assert_eq!(clone.capacity() % AlignedByteVec::BLOCK_SIZE, 0);
    }

    #[test]
    fn test_aligned_vec_deref_operations() {
        let mut vec = AlignedByteVec::new();
        vec.extend_from_slice(&[1, 2, 3]);

        // Test Deref
        assert_eq!(&*vec, &[1, 2, 3]);

        // Test DerefMut
        vec[0] = 10;
        assert_eq!(&*vec, &[10, 2, 3]);
    }

    #[test]
    fn test_aligned_vec_debug_format() {
        let mut vec = AlignedByteVec::new();
        vec.extend_from_slice(&[1, 2, 3]);

        let debug_str = format!("{:?}", vec);
        assert!(debug_str.contains("values"));
        assert!(debug_str.contains("len"));
        assert!(debug_str.contains("cap"));
        assert!(debug_str.contains("internal_offset"));
    }

    #[test]
    fn test_aligned_vec_reserve() {
        let mut vec = AlignedByteVec::new();

        // Test reservation when empty
        vec.reserve(100);
        assert!(vec.capacity() >= 100);
        assert!(is_aligned(vec.as_ptr(), AlignedByteVec::ALIGNMENT));

        // Test reservation when partially filled
        vec.extend_from_slice(&[1, 2, 3]);
        let original_cap = vec.capacity();
        vec.reserve(50);
        assert!(vec.capacity() >= original_cap);
        assert_eq!(vec.as_slice(), &[1, 2, 3]);

        // Test no-op reservation when capacity is sufficient
        let cap = vec.capacity();
        vec.reserve(1);
        assert_eq!(vec.capacity(), cap);
    }

    #[test]
    fn test_aligned_vec_shrink_to_fit() {
        let mut vec = AlignedByteVec::copy_from_slice(b"abcd");
        vec.resize(4000, 17);
        vec.truncate(4);
        vec.shrink_to_fit();
        assert!(vec.capacity() < 300);
        assert_eq!(&*vec, b"abcd");
    }

    #[test]
    fn test_aligned_vec_edge_cases() {
        // Test zero capacity
        let vec = AlignedByteVec::with_capacity(0);
        assert_eq!(vec.capacity(), 0);
        assert_eq!(vec.len(), 0);

        // Test very small allocations
        let mut vec = AlignedByteVec::with_capacity(1);
        assert!(vec.capacity() >= 1);
        vec.extend_from_slice(&[1]);
        assert_eq!(vec.as_slice(), &[1]);

        // Test truncate to zero
        let mut vec = AlignedByteVec::zeroed(100);
        vec.truncate(0);
        assert_eq!(vec.len(), 0);
        assert!(vec.is_empty());
    }

    #[test]
    fn test_aligned_vec_heap_size() {
        let vec = AlignedByteVec::with_capacity(1000);
        assert!(vec.heap_size() >= 1000);
        assert!(vec.heap_size() >= vec.capacity());
    }

    #[test]
    fn test_aligned_vec_aligned_at() {
        let vec = AlignedByteVec::zeroed(100);
        assert!(vec.is_aligned_at(0, 128));
        assert!(vec.is_aligned_at(64, 64));
        assert!(vec.is_aligned_at(2, 2));
        assert!(!vec.is_aligned_at(2, 128));
        assert!(!vec.is_aligned_at(2, 4));
    }

    #[test]
    fn test_aligned_vec_custom_alignment() {
        let mut vec = AlignedByteVec::with_capacity_and_alignment(16 * 1024, 4 * 1024);
        assert!(vec.is_aligned_at(0, 4 * 1024));
        assert_eq!(vec.len(), 0);
        let cap = vec.capacity();
        assert!(cap >= 16 * 1024);
        vec.resize(16 * 1024, 17);
        assert!(vec.is_aligned_at(0, 4 * 1024));
        assert_eq!(vec.capacity(), cap);
        vec.resize(50000, 12);
        assert!(vec.capacity() >= 50000);
        assert!(vec.is_aligned_at(0, 4 * 1024));
    }

    #[test]
    fn test_rounding_functions() {
        // Test round_up
        assert_eq!(round_up(0, 64), 0);
        assert_eq!(round_up(1, 64), 64);
        assert_eq!(round_up(63, 64), 64);
        assert_eq!(round_up(64, 64), 64);
        assert_eq!(round_up(65, 64), 128);

        // Test round_down
        assert_eq!(round_down(0, 64), 0);
        assert_eq!(round_down(1, 64), 0);
        assert_eq!(round_down(63, 64), 0);
        assert_eq!(round_down(64, 64), 64);
        assert_eq!(round_down(65, 64), 64);
        assert_eq!(round_down(128, 64), 128);
    }

    #[cfg(test)]
    mod truncate_tests {
        use super::*;

        #[test]
        fn test_aligned_byte_vec_truncate_basic() {
            let mut vec = AlignedByteVec::copy_from_slice(b"Hello, World!");
            assert_eq!(vec.len(), 13);

            vec.truncate(5);
            assert_eq!(vec.len(), 5);
            assert_eq!(vec.as_slice(), b"Hello");
        }

        #[test]
        fn test_aligned_byte_vec_truncate_to_zero() {
            let mut vec = AlignedByteVec::copy_from_slice(b"Hello, World!");
            assert_eq!(vec.len(), 13);

            vec.truncate(0);
            assert_eq!(vec.len(), 0);
            assert!(vec.is_empty());
        }

        #[test]
        fn test_aligned_byte_vec_truncate_larger_than_length() {
            let mut vec = AlignedByteVec::copy_from_slice(b"Hello");
            assert_eq!(vec.len(), 5);

            // Truncating to larger size should have no effect
            vec.truncate(10);
            assert_eq!(vec.len(), 5);
            assert_eq!(vec.as_slice(), b"Hello");
        }

        #[test]
        fn test_aligned_byte_vec_truncate_same_length() {
            let mut vec = AlignedByteVec::copy_from_slice(b"Hello");
            assert_eq!(vec.len(), 5);

            vec.truncate(5);
            assert_eq!(vec.len(), 5);
            assert_eq!(vec.as_slice(), b"Hello");
        }

        #[test]
        fn test_aligned_byte_vec_truncate_empty() {
            let mut vec = AlignedByteVec::new();
            assert_eq!(vec.len(), 0);

            vec.truncate(0);
            assert_eq!(vec.len(), 0);

            vec.truncate(10);
            assert_eq!(vec.len(), 0);
        }

        #[test]
        fn test_aligned_byte_vec_truncate_multiple_times() {
            let mut vec = AlignedByteVec::copy_from_slice(b"0123456789ABCDEF");
            assert_eq!(vec.len(), 16);

            vec.truncate(12);
            assert_eq!(vec.len(), 12);
            assert_eq!(vec.as_slice(), b"0123456789AB");

            vec.truncate(8);
            assert_eq!(vec.len(), 8);
            assert_eq!(vec.as_slice(), b"01234567");

            vec.truncate(4);
            assert_eq!(vec.len(), 4);
            assert_eq!(vec.as_slice(), b"0123");

            vec.truncate(1);
            assert_eq!(vec.len(), 1);
            assert_eq!(vec.as_slice(), b"0");
        }

        #[test]
        fn test_aligned_byte_vec_truncate_after_extend() {
            let mut vec = AlignedByteVec::copy_from_slice(b"Hello");
            vec.extend_from_slice(b", World!");
            assert_eq!(vec.len(), 13);
            assert_eq!(vec.as_slice(), b"Hello, World!");

            vec.truncate(7);
            assert_eq!(vec.len(), 7);
            assert_eq!(vec.as_slice(), b"Hello, ");
        }

        #[test]
        fn test_aligned_byte_vec_truncate_large_buffer() {
            let large_data = vec![42u8; 10000];
            let mut vec = AlignedByteVec::copy_from_slice(&large_data);
            assert_eq!(vec.len(), 10000);

            vec.truncate(5000);
            assert_eq!(vec.len(), 5000);
            assert!(vec.as_slice().iter().all(|&b| b == 42));

            vec.truncate(1000);
            assert_eq!(vec.len(), 1000);
            assert!(vec.as_slice().iter().all(|&b| b == 42));
        }

        #[test]
        fn test_aligned_byte_vec_truncate_and_resize() {
            let mut vec = AlignedByteVec::copy_from_slice(b"Hello, World!");
            vec.truncate(5);
            assert_eq!(vec.as_slice(), b"Hello");

            // Resize after truncate
            vec.resize(10, b'X');
            assert_eq!(vec.as_slice(), b"HelloXXXXX");
        }

        #[test]
        fn test_aligned_byte_vec_truncate_alignment_preserved() {
            let mut vec = AlignedByteVec::with_capacity_and_alignment(1000, 64);
            let test_data = (0..500).map(|i| (i % 256) as u8).collect::<Vec<_>>();
            vec.extend_from_slice(&test_data);

            // Verify alignment before truncate
            assert!(vec.is_aligned_at(0, 64));

            vec.truncate(200);
            assert_eq!(vec.len(), 200);

            // Verify alignment is still preserved after truncate
            assert!(vec.is_aligned_at(0, 64));

            // Verify data integrity
            for (i, &byte) in vec.as_slice().iter().enumerate() {
                assert_eq!(byte, (i % 256) as u8);
            }
        }

        #[test]
        fn test_aligned_byte_vec_truncate_capacity_unchanged() {
            let mut vec = AlignedByteVec::with_capacity(1000);
            vec.extend_from_slice(&vec![42u8; 500]);

            let original_capacity = vec.capacity();
            assert_eq!(vec.len(), 500);

            vec.truncate(100);
            assert_eq!(vec.len(), 100);
            // Capacity should remain unchanged after truncate
            assert_eq!(vec.capacity(), original_capacity);
        }

        #[test]
        fn test_aligned_byte_vec_truncate_boundary_conditions() {
            let mut vec = AlignedByteVec::with_capacity(100);
            vec.extend_from_slice(b"Test data");

            // Truncate to exact length
            vec.truncate(9);
            assert_eq!(vec.len(), 9);

            // Truncate to one less
            vec.truncate(8);
            assert_eq!(vec.len(), 8);
            assert_eq!(vec.as_slice(), b"Test dat");

            // Truncate to one
            vec.truncate(1);
            assert_eq!(vec.len(), 1);
            assert_eq!(vec.as_slice(), b"T");
        }

        #[test]
        fn test_aligned_byte_vec_truncate_different_alignments() {
            for alignment in [16, 32, 64, 128] {
                let mut vec = AlignedByteVec::with_capacity_and_alignment(1000, alignment);
                let test_data = (0..200).map(|i| (i % 256) as u8).collect::<Vec<_>>();
                vec.extend_from_slice(&test_data);

                assert!(vec.is_aligned_at(0, alignment));

                vec.truncate(100);
                assert_eq!(vec.len(), 100);
                assert!(vec.is_aligned_at(0, alignment));

                // Verify data integrity up to truncation point
                for (i, &byte) in vec.as_slice().iter().enumerate() {
                    assert_eq!(byte, (i % 256) as u8);
                }
            }
        }

        #[test]
        fn test_aligned_byte_vec_truncate_stress_test() {
            let mut vec = AlignedByteVec::with_capacity(10000);

            // Fill with pattern
            for i in 0..5000 {
                vec.extend_from_slice(&[(i % 256) as u8]);
            }

            // Truncate in steps
            for target_len in (0..5000).rev().step_by(100) {
                vec.truncate(target_len);
                assert_eq!(vec.len(), target_len);

                // Verify data integrity
                for (i, &byte) in vec.as_slice().iter().enumerate() {
                    assert_eq!(byte, (i % 256) as u8);
                }
            }
        }
    }
}

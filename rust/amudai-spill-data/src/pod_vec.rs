//! POD (Plain Old Data) collections for efficient append-style building and storage dumping.
//!
//! This module provides [`PodCollector<T>`] and [`PodView<T>`], a pair of complementary types
//! designed for optimized serialization, and retrieval scenarios.
//!
//! # Type Requirements
//!
//! All element types must implement `bytemuck::Pod`, which guarantees:
//! - No padding bytes (all bit patterns are valid)
//! - Safe transmutation to/from byte arrays
//! - `Copy` semantics with no drop glue
//! - No references or non-trivial types
//!
//! Common POD types include primitive integers (`u8`, `i32`, `u64`, etc.), floats (`f32`, `f64`),
//! and simple structs composed entirely of other POD types.

use amudai_bytes::{
    Bytes,
    align::{align_down_u64, align_up_u64},
    buffer::AlignedByteVec,
};
use amudai_io::{ReadAt, WriteAt, verify};

use crate::{
    Collection, Collector, Dump, Load,
    manifest::{Manifest, PAGE_SIZE},
};

/// Vec-like mutable collection of "POD" (Plain Old Data) values geared towards append-style
/// building, optimized for "dump" (offload to external storage) operation.
///
/// `PodCollector<T>` is a vector that stores values of type `T` where `T` must implement
/// `bytemuck::Pod` (Plain Old Data). The collection is specifically designed for scenarios
/// where data needs to be efficiently serialized to external storage and later loaded back
/// as a read-only view.
///
/// # Key Features
///
/// - **Memory-aligned storage**: Uses `AlignedByteVec` with page-size alignment for optimal I/O performance
/// - **Zero-copy serialization**: Can dump data directly to storage without intermediate copying
/// - **Append-optimized**: Designed for efficient sequential appends with amortized O(1) push operations
/// - **Storage-ready**: integrates with the dump/load system for persistence
///
/// # Type Requirements
///
/// The type parameter `T` must implement `bytemuck::Pod`, which ensures:
/// - The type has no padding bytes (all bit patterns are valid)
/// - The type can be safely transmuted to/from bytes
/// - The type is `Copy` and has no drop glue
///
/// # Counterpart
///
/// Has a read-only [`PodView<T>`] counterpart for loading data back from dumps without
/// requiring mutable access or additional memory allocation.
pub struct PodCollector<T> {
    data: AlignedByteVec,
    _p: std::marker::PhantomData<T>,
}

impl<T> PodCollector<T> {
    /// Creates a new, empty `PodCollector<T>`.
    ///
    /// This is equivalent to calling `with_capacity(0)` and does not allocate any memory
    /// until elements are added.
    pub fn new() -> PodCollector<T> {
        Self::with_capacity(0)
    }

    /// Creates a new `PodCollector<T>` with the specified capacity.
    ///
    /// The collector will be able to hold at least `capacity` elements without reallocating.
    /// The underlying storage is aligned to page boundaries for optimal I/O performance.
    ///
    /// # Parameters
    ///
    /// * `capacity` - The number of elements to pre-allocate space for
    pub fn with_capacity(capacity: usize) -> PodCollector<T> {
        PodCollector {
            data: AlignedByteVec::with_capacity_and_alignment(
                capacity * std::mem::size_of::<T>(),
                PAGE_SIZE as usize,
            ),
            _p: Default::default(),
        }
    }

    /// Returns the number of elements in the collector.
    #[inline]
    pub fn len(&self) -> usize {
        self.data.len() / std::mem::size_of::<T>()
    }

    /// Returns `true` if the collector contains no elements.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the number of elements the collector can hold without reallocating.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.data.capacity() / std::mem::size_of::<T>()
    }

    /// Shortens the collector, keeping the first `len` elements and dropping the rest.
    ///
    /// If `len` is greater than the collector's current length, this has no effect.
    ///
    /// # Parameters
    ///
    /// * `len` - The new length of the collector
    pub fn truncate(&mut self, len: usize) {
        self.data.truncate(len * std::mem::size_of::<T>());
    }

    /// Clears the collector, removing all elements.
    ///
    /// Note that this method has no effect on the allocated capacity of the collector.
    pub fn clear(&mut self) {
        self.data.clear();
    }

    /// Shrinks the capacity of the collector as much as possible.
    ///
    /// It will drop down as close as possible to the length but the allocator may still
    /// inform the collector that there is space for a few more elements.
    pub fn shrink_to_fit(&mut self) {
        self.data.shrink_to_fit();
    }
}

impl<T> PodCollector<T>
where
    T: bytemuck::Pod,
{
    /// Appends an element to the back of the collector.
    #[inline]
    pub fn push(&mut self, value: T) {
        self.data.push_typed(value);
    }

    /// Resizes the collector so that `len` is equal to `new_count`.
    ///
    /// If `new_count` is greater than the current length, the collector is extended by the
    /// difference, with each additional slot filled with `value`. If `new_count` is less than
    /// the current length, the collector is simply truncated.
    ///
    /// # Parameters
    ///
    /// * `new_count` - The new length of the collector
    /// * `value` - The value to fill new slots with if extending
    pub fn resize(&mut self, new_count: usize, value: T) {
        self.data.resize_typed(new_count, value);
    }

    /// Resizes the collector so that `len` is equal to `new_count`.
    ///
    /// If `new_count` is greater than the current length, the collector is extended by the
    /// difference, with each additional slot filled with zero value. If `new_count` is less than
    /// the current length, the collector is simply truncated.
    ///
    /// # Parameters
    ///
    /// * `new_count` - The new length of the collector
    pub fn resize_zeroed(&mut self, new_count: usize) {
        self.data.resize_zeroed::<T>(new_count);
    }

    /// Extends the collector with the contents of a slice.
    ///
    /// # Parameters
    ///
    /// * `values` - Slice of values to append to the collector
    pub fn extend_from_slice(&mut self, values: &[T]) {
        self.data.extend_from_typed_slice(values);
    }

    /// Reserves capacity for at least `additional` more elements to be inserted.
    ///
    /// The collection may reserve more space to avoid frequent reallocations. After calling
    /// `reserve`, capacity will be greater than or equal to `self.len() + additional`.
    /// Does nothing if capacity is already sufficient.
    ///
    /// # Parameters
    ///
    /// * `additional` - Number of additional elements to reserve space for
    pub fn reserve(&mut self, additional: usize) {
        self.data.reserve(additional * std::mem::size_of::<T>());
    }

    /// Removes the last element from the collector and returns it, or `None` if it is empty.
    pub fn pop(&mut self) -> Option<T> {
        if self.is_empty() {
            None
        } else {
            let new_len = self.len() - 1;
            let result = self.as_slice()[new_len];
            self.truncate(new_len);
            Some(result)
        }
    }

    /// Removes an element from the collector and returns it.
    ///
    /// The removed element is replaced by the last element of the collector.
    /// This does not preserve ordering, but is O(1). If you need to maintain element order,
    /// use a different approach like `retain()`.
    ///
    /// # Parameters
    ///
    /// * `index` - The index of the element to remove
    ///
    /// # Panics
    ///
    /// Panics if `index` is out of bounds.
    pub fn swap_remove(&mut self, index: usize) -> T {
        let len = self.len();
        assert!(
            index < len,
            "swap_remove index (is {index}) should be < len (is {len})"
        );

        let slice = self.as_mut_slice();
        slice.swap(index, len - 1);
        self.pop().unwrap()
    }

    /// Returns a reference to an element at the given index, or `None` if the index is
    /// out of bounds.
    ///
    /// # Parameters
    ///
    /// * `index` - The index of the element to access
    #[inline]
    pub fn get(&self, index: usize) -> Option<&T> {
        self.as_slice().get(index)
    }

    /// Returns a mutable reference to an element at the given index, or `None` if the index
    /// is out of bounds.
    ///
    /// # Parameters
    ///
    /// * `index` - The index of the element to access
    #[inline]
    pub fn get_mut(&mut self, index: usize) -> Option<&mut T> {
        self.as_mut_slice().get_mut(index)
    }

    /// Retains only the elements specified by the predicate.
    ///
    /// In other words, remove all elements `e` such that `f(&e)` returns `false`.
    /// This method operates in place, visiting each element exactly once in the original order,
    /// and preserves the order of the retained elements.
    ///
    /// # Parameters
    ///
    /// * `f` - A closure that returns `true` for elements to keep
    pub fn retain<F>(&mut self, mut f: F)
    where
        F: FnMut(&T) -> bool,
    {
        let mut del = 0;
        let len = self.len();
        let slice = self.as_mut_slice();

        for i in 0..len {
            if !f(&slice[i]) {
                del += 1;
            } else if del > 0 {
                slice[i - del] = slice[i];
            }
        }

        if del > 0 {
            self.truncate(len - del);
        }
    }

    /// Returns an immutable slice view of the collector's contents.
    #[inline]
    pub fn as_slice(&self) -> &[T] {
        self.data.typed_data()
    }

    /// Returns a mutable slice view of the collector's contents.
    #[inline]
    pub fn as_mut_slice(&mut self) -> &mut [T] {
        self.data.typed_data_mut()
    }

    /// Converts the collector into a read-only [`PodView<T>`].
    ///
    /// This consumes the collector and returns a view that can be used to access the data
    /// without the ability to modify it. The view uses the same underlying memory.
    pub fn into_view(self) -> PodView<T> {
        PodView::new(Bytes::from(self.data))
    }
}

impl<T: bytemuck::Pod> std::ops::Index<usize> for PodCollector<T> {
    type Output = T;

    #[inline]
    fn index(&self, index: usize) -> &Self::Output {
        &self.as_slice()[index]
    }
}

impl<T: bytemuck::Pod> std::ops::IndexMut<usize> for PodCollector<T> {
    #[inline]
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        &mut self.as_mut_slice()[index]
    }
}

impl<T: bytemuck::Pod> std::ops::Deref for PodCollector<T> {
    type Target = [T];

    #[inline]
    fn deref(&self) -> &Self::Target {
        PodCollector::as_slice(self)
    }
}

impl<T: bytemuck::Pod> std::ops::DerefMut for PodCollector<T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        PodCollector::as_mut_slice(self)
    }
}

impl<T: bytemuck::Pod + std::hash::Hash> std::hash::Hash for PodCollector<T> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.as_slice().hash(state);
    }
}

impl<T: bytemuck::Pod + std::fmt::Debug> std::fmt::Debug for PodCollector<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.as_slice().fmt(f)
    }
}

impl<T: bytemuck::Pod + Clone> Clone for PodCollector<T> {
    fn clone(&self) -> Self {
        let mut new_collector = Self::with_capacity(self.len());
        new_collector.extend_from_slice(self.as_slice());
        new_collector
    }
}

impl<T: bytemuck::Pod + Default> Default for PodCollector<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: bytemuck::Pod> From<Vec<T>> for PodCollector<T> {
    fn from(vec: Vec<T>) -> Self {
        let mut collector = Self::with_capacity(vec.len());
        collector.extend_from_slice(&vec);
        collector
    }
}

impl<T: bytemuck::Pod> From<&[T]> for PodCollector<T> {
    fn from(slice: &[T]) -> Self {
        let mut collector = Self::with_capacity(slice.len());
        collector.extend_from_slice(slice);
        collector
    }
}

impl<T: bytemuck::Pod, const N: usize> From<[T; N]> for PodCollector<T> {
    fn from(array: [T; N]) -> Self {
        let mut collector = Self::with_capacity(N);
        collector.extend_from_slice(&array);
        collector
    }
}

impl<T: bytemuck::Pod> Collection for PodCollector<T> {
    #[inline]
    fn len(&self) -> usize {
        PodCollector::len(self)
    }
}

impl<T: bytemuck::Pod> Collector for PodCollector<T> {
    type Item = T;

    fn with_item_capacity(capacity: usize) -> Self {
        PodCollector::with_capacity(capacity)
    }

    fn with_data_capacity(capacity_bytes: usize) -> Self {
        PodCollector::with_capacity(capacity_bytes / std::mem::size_of::<T>())
    }

    fn reserve_items(&mut self, additional: usize) {
        self.reserve(additional);
    }

    fn reserve_data(&mut self, additional_bytes: usize) {
        self.reserve(additional_bytes / std::mem::size_of::<T>());
    }

    #[inline]
    fn push(&mut self, value: &Self::Item) {
        PodCollector::push(self, *value);
    }

    fn set_len(&mut self, new_len: usize) {
        PodCollector::resize_zeroed(self, new_len);
    }

    fn clear(&mut self) {
        PodCollector::clear(self);
    }
}

/// Implements the `Dump` trait for types that can be viewed as slices of `bytemuck::NoUninit` elements.
///
/// This enables efficient serialization of `PodCollector<T>` and other slice-like types to external storage.
/// The implementation:
///
/// 1. Computes the total size needed (aligned to page boundaries)
/// 2. Writes the data in chunks for optimal I/O performance  
/// 3. Stores metadata (length and item size) in the manifest for later loading
///
/// The data is written in chunks to respect I/O size limits while maintaining good performance.
impl<S> Dump for S
where
    S: details::AsSlice,
    S::Item: bytemuck::NoUninit,
{
    fn compute_size(&self) -> u64 {
        let size = self.as_slice().len() as u64 * (std::mem::size_of::<S::Item>() as u64);
        align_up_u64(size, PAGE_SIZE)
    }

    fn dump<W>(
        &self,
        writer: amudai_io::SlicedFile<W>,
    ) -> std::io::Result<crate::manifest::Manifest>
    where
        W: amudai_io::WriteAt + Clone,
    {
        let this = self.as_slice();
        let bytes = bytemuck::cast_slice::<_, u8>(this);
        let chunk_size = align_down_u64(writer.storage_profile().max_io_size as u64, PAGE_SIZE)
            .clamp(PAGE_SIZE * 16, 4 * 1024 * 1024) as usize;
        let mut pos = 0u64;
        for chunk in bytes.chunks(chunk_size) {
            writer.write_at(pos, chunk)?;
            pos += chunk.len() as u64;
        }
        let mut manifest = Manifest::default();
        manifest.put("len", this.len() as u64);
        manifest.put("item_size", std::mem::size_of::<S::Item>() as u64);
        Ok(manifest)
    }
}

impl<T: bytemuck::Pod> Load for PodCollector<T> {
    fn load<R>(reader: amudai_io::SlicedFile<R>, manifest: &Manifest) -> std::io::Result<Self>
    where
        R: ReadAt + Clone,
    {
        let len = manifest.get::<u64>("len")?;
        let item_size = manifest.get::<u64>("item_size")? as usize;
        verify!(item_size == std::mem::size_of::<T>());
        let end_pos = len * (std::mem::size_of::<T>() as u64);
        let bytes = if len != 0 {
            reader.read_at(0..end_pos)?
        } else {
            return Ok(PodCollector::new());
        };
        verify!(bytes.len() == end_pos as usize);
        let vec = bytes.into_vec();
        verify!(vec.len() == len as usize * std::mem::size_of::<T>());
        Ok(PodCollector {
            data: vec,
            _p: Default::default(),
        })
    }
}

/// Immutable, zero-copy view of POD (Plain Old Data) values loaded from external storage.
///
/// `PodView<T>` is the read-only counterpart to [`PodCollector<T>`], designed for efficient
/// access to POD data that has been previously serialized to external storage. It provides
/// zero-copy access to the underlying data through shared byte buffers, typically loaded
/// from external storage.
///
/// # Type Requirements
///
/// The type parameter `T` must implement `bytemuck::AnyBitPattern`, which ensures:
/// - All bit patterns are valid for the type (stronger than `Pod`)
/// - The type can be safely created from arbitrary byte data
/// - The type has no invalid bit patterns or padding requirements
///
/// # Relationship to PodCollector
///
/// `PodView<T>` is created from:
/// - `PodCollector<T>` via `into_view()` for zero-copy conversion
/// - External storage via `Load::load()` for deserialization
/// - Raw `Bytes` via `new()` for direct byte interpretation
#[derive(Clone)]
pub struct PodView<T> {
    /// Shared byte buffer containing the raw data
    bytes: Bytes,
    /// Cached pointer to the typed slice for efficient access
    slice: *const [T],
    /// Phantom data to maintain type parameter
    _p: std::marker::PhantomData<T>,
}

unsafe impl<T: Send> Send for PodView<T> {}

unsafe impl<T: Sync> Sync for PodView<T> {}

impl<T> PodView<T>
where
    T: bytemuck::AnyBitPattern,
{
    /// Creates a new `PodView<T>` from a byte buffer.
    ///
    /// This constructor interprets the provided bytes as a sequence of `T` elements.
    /// The byte length must be a multiple of `size_of::<T>()` to ensure proper alignment
    /// and prevent partial elements.
    ///
    /// # Parameters
    ///
    /// * `bytes` - Shared byte buffer containing the raw POD data
    ///
    /// # Panics
    ///
    /// Panics if `bytes.len()` is not a multiple of `std::mem::size_of::<T>()`.
    /// This ensures that the byte data can be safely interpreted as complete elements
    /// of type `T` without any partial elements at the end.
    ///
    /// # Safety
    ///
    /// This method is safe because:
    /// - `T: bytemuck::AnyBitPattern` guarantees all bit patterns are valid
    /// - Length validation ensures no partial elements
    /// - `bytemuck` provides safe transmutation from bytes to typed data
    pub fn new(bytes: Bytes) -> PodView<T> {
        assert_eq!(bytes.len() % std::mem::size_of::<T>(), 0);
        let slice = bytes.typed_data() as *const [T];
        PodView {
            bytes,
            slice,
            _p: Default::default(),
        }
    }
}

impl<T> PodView<T> {
    #[inline]
    pub fn len(&self) -> usize {
        self.as_slice().len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns an immutable slice view of the view's contents.
    ///
    /// This provides direct access to the underlying data as a standard Rust slice,
    /// enabling all slice operations and methods. The returned slice references the
    /// same memory as the view without any copying.
    ///
    /// # Returns
    ///
    /// An immutable slice `&[T]` containing all elements in the view.
    #[inline]
    pub fn as_slice(&self) -> &[T] {
        unsafe { &*self.slice }
    }

    /// Creates a sub-view of this view for the specified range.
    ///
    /// This method efficiently creates a new `PodView` that references a subset of the
    /// current view's data. The operation is zero-copy - both views share the same
    /// underlying `Bytes` buffer, with the new view having an adjusted slice pointer.
    ///
    /// # Parameters
    ///
    /// * `range` - A range specification (e.g., `1..4`, `..3`, `2..`, `..`) defining
    ///   which elements to include in the sub-view
    ///
    /// # Returns
    ///
    /// A new `PodView<T>` containing only the elements within the specified range.
    /// The new view shares the same underlying memory as the original.
    ///
    /// # Panics
    ///
    /// Panics if the range is out of bounds or if `start > end`. This includes:
    /// - Start index greater than the view length
    /// - End index greater than the view length  
    /// - Start index greater than end index
    pub fn slice<R>(&self, range: R) -> Self
    where
        R: std::ops::RangeBounds<usize>,
    {
        use std::ops::Bound::*;
        let start = match range.start_bound() {
            Included(&n) => n,
            Excluded(&n) => n + 1,
            Unbounded => 0,
        };
        let end = match range.end_bound() {
            Included(&n) => n + 1,
            Excluded(&n) => n,
            Unbounded => self.len(),
        };
        assert!(start <= end && end <= self.len(), "slice out of bounds");
        let slice = &self.as_slice()[start..end] as *const [T];
        PodView {
            bytes: self.bytes.clone(),
            slice,
            _p: Default::default(),
        }
    }
}

impl<T> Collection for PodView<T> {
    #[inline]
    fn len(&self) -> usize {
        PodView::len(self)
    }
}

impl<T: bytemuck::AnyBitPattern> std::ops::Index<usize> for PodView<T> {
    type Output = T;

    #[inline]
    fn index(&self, index: usize) -> &Self::Output {
        &self.as_slice()[index]
    }
}

impl<T> std::ops::Deref for PodView<T> {
    type Target = [T];

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl<T> AsRef<[T]> for PodView<T> {
    #[inline]
    fn as_ref(&self) -> &[T] {
        self.as_slice()
    }
}

/// Implements the `Load` trait for deserializing `PodView<T>` from external storage.
///
/// This implementation enables `PodView<T>` to be loaded from data that was previously
/// serialized from any `&[T]`-like structure using the `Dump` trait. It reconstructs
/// the view from raw bytes and validates the data integrity using manifest metadata.
///
/// # Type Requirements
///
/// The type parameter `T` must implement `bytemuck::AnyBitPattern` to ensure safe
/// interpretation of arbitrary byte patterns as valid values of type `T`.
///
/// # Loading Process
///
/// 1. Reads length and item size from the manifest
/// 2. Validates that the stored item size matches `size_of::<T>()`
/// 3. Reads the appropriate number of bytes from storage
/// 4. Validates that the byte count matches the expected size
/// 5. Creates a new `PodView<T>` wrapping the loaded bytes
///
/// # Error Handling
///
/// Returns `std::io::Error` if:
/// - Manifest is missing required metadata ("len" or "item_size")
/// - Stored item size doesn't match the current type `T`
/// - I/O error occurs during reading
/// - Loaded byte count doesn't match expected size
impl<T> Load for PodView<T>
where
    T: bytemuck::AnyBitPattern,
{
    fn load<R>(reader: amudai_io::SlicedFile<R>, manifest: &Manifest) -> std::io::Result<Self>
    where
        R: amudai_io::ReadAt + Clone,
    {
        let len = manifest.get::<u64>("len")?;
        let item_size = manifest.get::<u64>("item_size")? as usize;
        verify!(item_size == std::mem::size_of::<T>());
        let end_pos = len * (std::mem::size_of::<T>() as u64);
        let bytes = if len != 0 {
            reader.read_at(0..end_pos)?
        } else {
            Bytes::new()
        };
        verify!(bytes.len() == end_pos as usize);
        Ok(PodView::new(bytes))
    }
}

impl<T: bytemuck::AnyBitPattern> Default for PodView<T> {
    fn default() -> Self {
        Self::new(Bytes::new())
    }
}

mod details {
    use amudai_bytes::{
        Bytes, BytesMut,
        buffer::{AlignedByteVec, Buffer},
    };
    use amudai_collections::shared_vec_slice::SharedVecSlice;

    use crate::pod_vec::{PodCollector, PodView};

    pub trait AsSlice {
        type Item;

        fn as_slice(&self) -> &[Self::Item];
    }

    impl<T> AsSlice for Vec<T> {
        type Item = T;

        #[inline]
        fn as_slice(&self) -> &[Self::Item] {
            Vec::as_slice(self)
        }
    }

    impl<T: bytemuck::Pod> AsSlice for PodCollector<T> {
        type Item = T;

        #[inline]
        fn as_slice(&self) -> &[Self::Item] {
            PodCollector::as_slice(self)
        }
    }

    impl<T> AsSlice for [T] {
        type Item = T;

        #[inline]
        fn as_slice(&self) -> &[Self::Item] {
            self
        }
    }

    impl<T> AsSlice for &T
    where
        T: AsSlice + ?Sized,
    {
        type Item = T::Item;

        #[inline]
        fn as_slice(&self) -> &[Self::Item] {
            (*self).as_slice()
        }
    }

    impl<T> AsSlice for PodView<T> {
        type Item = T;

        #[inline]
        fn as_slice(&self) -> &[Self::Item] {
            PodView::as_slice(self)
        }
    }

    impl<T> AsSlice for SharedVecSlice<T> {
        type Item = T;

        #[inline]
        fn as_slice(&self) -> &[Self::Item] {
            SharedVecSlice::as_slice(self)
        }
    }

    impl AsSlice for Bytes {
        type Item = u8;

        #[inline]
        fn as_slice(&self) -> &[Self::Item] {
            self
        }
    }

    impl AsSlice for BytesMut {
        type Item = u8;

        #[inline]
        fn as_slice(&self) -> &[Self::Item] {
            self
        }
    }

    impl AsSlice for AlignedByteVec {
        type Item = u8;

        #[inline]
        fn as_slice(&self) -> &[Self::Item] {
            self
        }
    }

    impl AsSlice for Buffer {
        type Item = u8;

        #[inline]
        fn as_slice(&self) -> &[Self::Item] {
            self
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        Dump, Load,
        pod_vec::{PodCollector, PodView},
    };

    #[test]
    fn test_pod_vec_save_load_view() {
        let temp_store = amudai_io_impl::temp_file_store::create_in_memory(1000000000).unwrap();
        let file = temp_store.allocate_shared_buffer(None).unwrap();
        assert_eq!(file.size().unwrap(), 0);

        let v = (0u64..10000u64).collect::<Vec<_>>();
        let end_pos = v.dump_and_seal(file.clone()).unwrap();
        dbg!(&end_pos);

        let v = PodView::<u64>::load_sealed(file.clone()).unwrap();
        assert_eq!(v.len(), 10000);
        assert_eq!(v[0], 0);
        assert_eq!(v[1], 1);
    }

    #[test]
    fn test_pod_vec_save_load_collector() {
        let temp_store = amudai_io_impl::temp_file_store::create_in_memory(1000000000).unwrap();
        let file = temp_store.allocate_shared_buffer(None).unwrap();
        assert_eq!(file.size().unwrap(), 0);

        let v = (0u64..10000u64).collect::<Vec<_>>();
        v.dump_and_seal(file.clone()).unwrap();

        let v = PodCollector::<u64>::load_sealed(file.clone()).unwrap();
        assert_eq!(v.len(), 10000);
        assert_eq!(v[0], 0);
        assert_eq!(v[1], 1);
    }
}

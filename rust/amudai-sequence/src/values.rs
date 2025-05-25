//! A collection of values stored as bytes with alignment guarantees.

use amudai_bytes::buffer::AlignedByteVec;

/// A collection of values stored as bytes with alignment guarantees.
///
/// `Values` wraps an `AlignedByteVec` and provides methods for safely working with
/// byte representations of typed values.
#[derive(Debug, Clone)]
pub struct Values(AlignedByteVec);

impl Values {
    /// Creates a new, empty `Values` instance.
    ///
    /// # Returns
    ///
    /// A new, empty `Values` container.
    pub fn new() -> Values {
        Values(AlignedByteVec::new())
    }

    /// Creates a new `Values` instance from an existing `AlignedByteVec`.
    ///
    /// # Parameters
    ///
    /// * `vec` - The `AlignedByteVec` to create the `Values` container from.
    ///
    /// # Returns
    ///
    /// A new `Values` container with the same underlying byte storage as `vec`.
    pub fn from_vec(vec: AlignedByteVec) -> Values {
        Values(vec)
    }

    /// Creates a new `Values` instance filled with zeroed bytes for `len` elements of type `T`.
    ///
    /// # Type Parameters
    ///
    /// * `T` - The type of elements to allocate space for.
    ///
    /// # Parameters
    ///
    /// * `len` - The number of elements to allocate space for.
    ///
    /// # Returns
    ///
    /// A new `Values` container with space for `len` zeroed elements of type `T`.
    pub fn zeroed<T>(len: usize) -> Values
    where
        T: bytemuck::Zeroable,
    {
        Values(AlignedByteVec::zeroed(len * std::mem::size_of::<T>()))
    }

    /// Creates a new `Values` instance filled with zeroed bytes of the specified length.
    pub fn zeroed_bytes(bytes_len: usize) -> Values {
        Values(AlignedByteVec::zeroed(bytes_len))
    }

    /// Creates a new `Values` instance with capacity for at least `capacity` elements of type `T`.
    ///
    /// # Type Parameters
    ///
    /// * `T` - The type of elements to allocate capacity for.
    ///
    /// # Parameters
    ///
    /// * `capacity` - The number of elements to allocate capacity for.
    ///
    /// # Returns
    ///
    /// A new `Values` container with the requested capacity.
    pub fn with_capacity<T>(capacity: usize) -> Values {
        let capacity = capacity * std::mem::size_of::<T>();
        Values(AlignedByteVec::with_capacity(capacity))
    }

    /// Creates a new `Values` instance with a specified byte capacity.
    pub fn with_byte_capacity(capacity: usize) -> Values {
        Values(AlignedByteVec::with_capacity(capacity))
    }

    /// Checks if the `Values` container is empty.
    ///
    /// # Returns
    ///
    /// `true` if the container contains no elements, `false` otherwise.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns the number of elements of type `T` that can fit in the current byte length.
    ///
    /// # Type Parameters
    /// * `T` - The type to use for calculating element count.
    ///
    /// # Returns
    ///
    /// The number of complete elements of type `T` that fit in the container.
    #[inline]
    pub fn len<T>(&self) -> usize {
        self.0.len() / std::mem::size_of::<T>()
    }

    /// Returns the number of bytes in the container.
    #[inline]
    pub fn bytes_len(&self) -> usize {
        self.0.len()
    }

    /// Returns the number of elements of type `T` the container can hold without reallocating.
    #[inline]
    pub fn capacity<T>(&self) -> usize {
        self.0.capacity() / std::mem::size_of::<T>()
    }

    /// Returns a reference to the underlying bytes.
    ///
    /// # Returns
    ///
    /// A slice reference to the underlying byte storage.
    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Interprets the underlying bytes as a slice of elements of type `T`.
    ///
    /// # Type Parameters
    ///
    /// * `T` - The type to interpret the bytes as.
    ///
    /// # Returns
    ///
    /// A reference to the content interpreted as a slice of `T`.
    #[inline]
    pub fn as_slice<T>(&self) -> &[T]
    where
        T: bytemuck::AnyBitPattern,
    {
        self.0.typed_data()
    }

    /// Interprets the underlying bytes as a mutable slice of elements of type `T`.
    ///
    /// # Type Parameters
    ///
    /// * `T` - The type to interpret the bytes as.
    ///
    /// # Returns
    ///
    /// A mutable reference to the content interpreted as a slice of `T`.
    #[inline]
    pub fn as_mut_slice<T>(&mut self) -> &mut [T]
    where
        T: bytemuck::AnyBitPattern + bytemuck::NoUninit,
    {
        self.0.typed_data_mut()
    }

    /// Resizes the container to hold exactly `new_len` elements of type `T`,
    /// filling any additional space with the given `value`.
    ///
    /// # Type Parameters
    ///
    /// * `T` - The type of elements.
    ///
    /// # Parameters
    ///
    /// * `new_len` - The new number of elements.
    /// * `value` - The value to fill any additional space with.
    pub fn resize<T>(&mut self, new_len: usize, value: T)
    where
        T: bytemuck::AnyBitPattern + bytemuck::NoUninit,
    {
        self.0.resize_typed(new_len, value);
    }

    /// Resizes the container to hold exactly `new_len` elements of type `T`,
    /// filling any additional space with zeroes.
    ///
    /// # Type Parameters
    ///
    /// * `T` - The type of elements.
    ///
    /// # Parameters
    ///
    /// * `new_len` - The new number of elements.
    pub fn resize_zeroed<T>(&mut self, new_len: usize)
    where
        T: bytemuck::AnyBitPattern + bytemuck::NoUninit,
    {
        self.0.resize_zeroed::<T>(new_len);
    }

    /// Resizes the container to hold exactly `new_len` bytes, filling
    /// any additional space with zeroes.
    pub fn resize_zeroed_bytes(&mut self, new_len: usize) {
        self.0.resize(new_len, 0);
    }

    /// Appends a single element of type `T` to the end of the container.
    ///
    /// # Type Parameters
    ///
    /// * `T` - The type of the element, which must not contain uninitialized memory.
    ///
    /// # Parameters
    ///
    /// * `value` - The value to append.
    #[inline]
    pub fn push<T>(&mut self, value: T)
    where
        T: bytemuck::NoUninit,
    {
        self.0.push_typed(value);
    }

    /// Extends the container with the contents of a slice of elements of type `T`.
    ///
    /// # Type Parameters
    ///
    /// * `T` - The type of the elements, which must not contain uninitialized memory.
    ///
    /// # Parameters
    ///
    /// * `values` - The slice of values to append to the container.
    #[inline]
    pub fn extend_from_slice<T>(&mut self, values: &[T])
    where
        T: bytemuck::NoUninit,
    {
        self.0.extend_from_typed_slice(values);
    }

    /// Appends space for `count` elements of type `T` and provides a callback
    /// to write into that space.
    ///
    /// # Type Parameters
    ///
    /// * `T` - The type of elements, which must have a defined bit pattern and
    ///   not contain uninitialized memory.
    ///
    /// * `R` - The return type of the writer callback.
    ///
    /// # Parameters
    ///
    /// * `count` - The number of elements to append.
    /// * `writer` - A callback that takes a mutable slice of the newly allocated and zeroed
    ///   space and returns a value of type `R`.
    ///
    /// # Returns
    ///
    /// The value returned by the writer callback.
    pub fn append<T, R>(&mut self, count: usize, writer: impl FnOnce(&mut [T]) -> R) -> R
    where
        T: bytemuck::AnyBitPattern + bytemuck::NoUninit,
    {
        let prev_size = self.0.len();
        let prev_len = prev_size / std::mem::size_of::<T>();
        let new_size = (prev_len + count) * std::mem::size_of::<T>();
        self.0.resize(new_size, 0);
        let values = self.0.typed_data_mut::<T>();
        writer(&mut values[prev_len..])
    }

    /// Resizes the container to hold exactly `count` elements of type `T` and provides
    /// a callback to write into the entire space.
    ///
    /// # Type Parameters
    ///
    /// * `T` - The type of elements, which must have a defined bit pattern and
    ///   not contain uninitialized memory.
    /// * `R` - The return type of the writer callback.
    ///
    /// # Parameters
    ///
    /// * `count` - The number of elements to resize to.
    /// * `writer` - A callback that takes a mutable slice of the entire space
    ///   and returns a value of type `R`.
    ///
    /// # Returns
    ///
    /// The value returned by the writer callback.
    pub fn write<T, R>(&mut self, count: usize, writer: impl FnOnce(&mut [T]) -> R) -> R
    where
        T: bytemuck::AnyBitPattern + bytemuck::NoUninit,
    {
        let size = count * std::mem::size_of::<T>();
        self.0.resize(size, 0);
        let values = self.0.typed_data_mut::<T>();
        writer(values)
    }

    /// Clears the container, removing all elements.
    ///
    /// This does not affect the allocated capacity.
    pub fn clear(&mut self) {
        self.0.clear();
    }

    /// Consumes the `Values` container and returns the underlying `AlignedByteVec`.
    ///
    /// This method transfers ownership of the internal byte buffer, allowing
    /// direct access to the raw aligned bytes. After calling this method,
    /// the `Values` instance can no longer be used.
    ///
    /// # Returns
    ///
    /// The inner `AlignedByteVec` that was used to store the values.
    pub fn into_inner(self) -> AlignedByteVec {
        self.0
    }
}

impl Default for Values {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::mem::size_of;

    #[derive(Debug, Default, Clone, Copy, PartialEq)]
    #[repr(C)]
    struct TestStruct {
        x: i32,
        y: f64,
    }

    unsafe impl bytemuck::Zeroable for TestStruct {}
    unsafe impl bytemuck::Pod for TestStruct {}

    #[test]
    fn test_new() {
        let values = Values::new();
        assert!(values.is_empty());
        assert_eq!(values.as_bytes().len(), 0);
    }

    #[test]
    fn test_zeroed() {
        let values = Values::zeroed::<u32>(5);
        assert_eq!(values.len::<u32>(), 5);
        assert_eq!(values.as_slice::<u32>(), &[0, 0, 0, 0, 0]);

        let values = Values::zeroed::<TestStruct>(3);
        assert_eq!(values.len::<TestStruct>(), 3);
        let expected = vec![TestStruct::default(); 3];
        assert_eq!(values.as_slice::<TestStruct>(), expected.as_slice());
    }

    #[test]
    fn test_with_capacity() {
        let values = Values::with_capacity::<u32>(10);
        assert!(values.is_empty());
        assert!(values.capacity::<u32>() >= 10);
    }

    #[test]
    fn test_is_empty() {
        let values = Values::new();
        assert!(values.is_empty());

        let mut values = Values::zeroed::<u32>(5);
        assert!(!values.is_empty());

        values.clear();
        assert!(values.is_empty());
    }

    #[test]
    fn test_len() {
        let values = Values::new();
        assert_eq!(values.len::<u32>(), 0);
        assert_eq!(values.len::<TestStruct>(), 0);

        let values = Values::zeroed::<u32>(5);
        assert_eq!(values.len::<u32>(), 5);
        assert_eq!(values.len::<u8>(), 5 * size_of::<u32>());
        assert_eq!(
            values.len::<TestStruct>(),
            5 * size_of::<u32>() / size_of::<TestStruct>()
        );
    }

    #[test]
    fn test_as_bytes() {
        let values = Values::zeroed::<u32>(4);
        assert_eq!(values.as_bytes().len(), 16);
        assert_eq!(values.as_bytes(), &[0; 16]);

        let mut values = Values::new();
        values.push(42u32);
        let bytes = values.as_bytes();
        assert_eq!(bytes, &[42, 0, 0, 0]);
    }

    #[test]
    fn test_as_slice() {
        let mut values = Values::new();
        values.push(1u32);
        values.push(2u32);
        values.push(3u32);

        let slice = values.as_slice::<u32>();
        assert_eq!(slice, &[1, 2, 3]);

        let mut values = Values::new();
        let test_struct = TestStruct { x: 42, y: 3.14 };
        values.push(test_struct);

        let slice = values.as_slice::<TestStruct>();
        assert_eq!(slice.len(), 1);
        assert_eq!(slice[0], test_struct);
    }

    #[test]
    fn test_as_mut_slice() {
        let mut values = Values::zeroed::<u32>(3);
        {
            let mut_slice = values.as_mut_slice::<u32>();
            mut_slice[0] = 10;
            mut_slice[1] = 20;
            mut_slice[2] = 30;
        }

        assert_eq!(values.as_slice::<u32>(), &[10, 20, 30]);

        let mut values = Values::zeroed::<TestStruct>(2);
        {
            let mut_slice = values.as_mut_slice::<TestStruct>();
            mut_slice[0] = TestStruct { x: 1, y: 1.1 };
            mut_slice[1] = TestStruct { x: 2, y: 2.2 };
        }

        assert_eq!(
            values.as_slice::<TestStruct>()[0],
            TestStruct { x: 1, y: 1.1 }
        );
        assert_eq!(
            values.as_slice::<TestStruct>()[1],
            TestStruct { x: 2, y: 2.2 }
        );
    }

    #[test]
    fn test_resize() {
        let mut values = Values::zeroed::<u32>(2);
        values.resize(4, 42u32);

        assert_eq!(values.len::<u32>(), 4);
        assert_eq!(values.as_slice::<u32>(), &[0, 0, 42, 42]);

        values.resize(2, 0u32);
        assert_eq!(values.len::<u32>(), 2);
        assert_eq!(values.as_slice::<u32>(), &[0, 0]);

        let mut values = Values::zeroed::<TestStruct>(1);
        let fill_value = TestStruct { x: 100, y: 200.0 };
        values.resize(3, fill_value);

        assert_eq!(values.len::<TestStruct>(), 3);
        assert_eq!(values.as_slice::<TestStruct>()[0], TestStruct::default());
        assert_eq!(values.as_slice::<TestStruct>()[1], fill_value);
        assert_eq!(values.as_slice::<TestStruct>()[2], fill_value);
    }

    #[test]
    fn test_resize_zeroed() {
        let mut values = Values::new();
        values.push(123u32);
        values.push(456u32);

        values.resize_zeroed::<u32>(4);
        assert_eq!(values.len::<u32>(), 4);
        assert_eq!(values.as_slice::<u32>(), &[123, 456, 0, 0]);

        values.resize_zeroed::<u32>(1);
        assert_eq!(values.len::<u32>(), 1);
        assert_eq!(values.as_slice::<u32>(), &[123]);

        let mut values = Values::new();
        values.push(TestStruct { x: 1, y: 1.1 });
        values.resize_zeroed::<TestStruct>(2);

        assert_eq!(values.len::<TestStruct>(), 2);
        assert_eq!(
            values.as_slice::<TestStruct>()[0],
            TestStruct { x: 1, y: 1.1 }
        );
        assert_eq!(values.as_slice::<TestStruct>()[1], TestStruct::default());
    }

    #[test]
    fn test_push() {
        let mut values = Values::new();
        values.push(42u32);

        assert_eq!(values.len::<u32>(), 1);
        assert_eq!(values.as_slice::<u32>()[0], 42);

        values.push(123u32);
        assert_eq!(values.len::<u32>(), 2);
        assert_eq!(values.as_slice::<u32>(), &[42, 123]);

        let mut values = Values::new();
        let test_struct = TestStruct { x: 42, y: 3.14 };
        values.push(test_struct);

        assert_eq!(values.len::<TestStruct>(), 1);
        assert_eq!(values.as_slice::<TestStruct>()[0], test_struct);
    }

    #[test]
    fn test_extend_from_slice() {
        let mut values = Values::new();
        values.extend_from_slice(&[1u32, 2, 3]);

        assert_eq!(values.len::<u32>(), 3);
        assert_eq!(values.as_slice::<u32>(), &[1, 2, 3]);

        values.extend_from_slice(&[4u32, 5]);
        assert_eq!(values.len::<u32>(), 5);
        assert_eq!(values.as_slice::<u32>(), &[1, 2, 3, 4, 5]);

        let mut values = Values::new();
        let structs = vec![TestStruct { x: 1, y: 1.1 }, TestStruct { x: 2, y: 2.2 }];
        values.extend_from_slice(&structs);

        assert_eq!(values.len::<TestStruct>(), 2);
        assert_eq!(values.as_slice::<TestStruct>(), structs.as_slice());
    }

    #[test]
    fn test_append() {
        let mut values = Values::new();
        values.push(1u32);

        let result = values.append::<u32, _>(2, |slice| {
            slice[0] = 2;
            slice[1] = 3;
            "test_result"
        });

        assert_eq!(result, "test_result");
        assert_eq!(values.len::<u32>(), 3);
        assert_eq!(values.as_slice::<u32>(), &[1, 2, 3]);

        let mut values = Values::new();
        let initial = TestStruct { x: 1, y: 1.1 };
        values.push(initial);

        values.append::<TestStruct, _>(2, |slice| {
            slice[0] = TestStruct { x: 2, y: 2.2 };
            slice[1] = TestStruct { x: 3, y: 3.3 };
        });

        assert_eq!(values.len::<TestStruct>(), 3);
        assert_eq!(values.as_slice::<TestStruct>()[0], initial);
        assert_eq!(
            values.as_slice::<TestStruct>()[1],
            TestStruct { x: 2, y: 2.2 }
        );
        assert_eq!(
            values.as_slice::<TestStruct>()[2],
            TestStruct { x: 3, y: 3.3 }
        );
    }

    #[test]
    fn test_write() {
        let mut values = Values::zeroed::<u32>(3);

        let result = values.write::<u32, _>(4, |slice| {
            slice[0] = 10;
            slice[1] = 20;
            slice[2] = 30;
            slice[3] = 40;
            "write_result"
        });

        assert_eq!(result, "write_result");
        assert_eq!(values.len::<u32>(), 4);
        assert_eq!(values.as_slice::<u32>(), &[10, 20, 30, 40]);

        values.write::<u32, _>(2, |slice| {
            slice[0] = 100;
            slice[1] = 200;
        });

        assert_eq!(values.len::<u32>(), 2);
        assert_eq!(values.as_slice::<u32>(), &[100, 200]);

        let mut values = Values::new();
        values.write::<TestStruct, _>(2, |slice| {
            slice[0] = TestStruct { x: 5, y: 5.5 };
            slice[1] = TestStruct { x: 6, y: 6.6 };
        });

        assert_eq!(values.len::<TestStruct>(), 2);
        assert_eq!(
            values.as_slice::<TestStruct>()[0],
            TestStruct { x: 5, y: 5.5 }
        );
        assert_eq!(
            values.as_slice::<TestStruct>()[1],
            TestStruct { x: 6, y: 6.6 }
        );
    }

    #[test]
    fn test_clear() {
        let mut values = Values::zeroed::<u32>(5);
        assert_eq!(values.len::<u32>(), 5);

        values.clear();
        assert!(values.is_empty());
        assert_eq!(values.len::<u32>(), 0);

        values.push(42u32);
        assert_eq!(values.len::<u32>(), 1);
        assert_eq!(values.as_slice::<u32>()[0], 42);
    }

    #[test]
    fn test_mixed_types() {
        let mut values = Values::new();

        values.push(0x01020304u32);
        values.push(0x05060708u32);

        let u64_slice = values.as_slice::<u64>();
        assert_eq!(u64_slice.len(), 1);

        assert_eq!(values.as_bytes().len(), 8);

        let u8_slice = values.as_slice::<u8>();
        assert_eq!(u8_slice.len(), 8);
    }

    #[test]
    fn test_alignment() {
        let mut values = Values::new();
        values.push(TestStruct { x: 1, y: 2.0 });

        let ptr = values.as_slice::<TestStruct>().as_ptr();
        assert_eq!(ptr as usize % std::mem::align_of::<TestStruct>(), 0);
    }

    #[test]
    fn test_consecutive_operations() {
        let mut values = Values::new();

        values.push(1u32);
        values.push(2u32);

        values.extend_from_slice(&[3u32, 4, 5]);

        values.append::<u32, _>(2, |slice| {
            slice[0] = 6;
            slice[1] = 7;
        });

        values.resize(8, 8u32);

        values.write::<u32, _>(10, |slice| {
            slice[0] = 10;
            slice[1] = 11;
            // Keep original values
            slice[2] = 3;
            slice[3] = 4;
            slice[4] = 5;
            slice[5] = 6;
            slice[6] = 7;
            slice[7] = 8;
            // Add new values
            slice[8] = 9;
            slice[9] = 10;
        });

        assert_eq!(values.len::<u32>(), 10);
        assert_eq!(values.as_slice::<u32>(), &[10, 11, 3, 4, 5, 6, 7, 8, 9, 10]);

        values.resize_zeroed::<u32>(3);
        assert_eq!(values.len::<u32>(), 3);
        assert_eq!(values.as_slice::<u32>(), &[10, 11, 3]);

        values.clear();
        assert!(values.is_empty());
    }
}

//! A basic sequence of values.

use crate::{
    offsets::{FixedSizeOffsetsIter, Offsets, OffsetsIter},
    presence::{Presence, PresenceBuilder},
    sequence::Sequence,
    values::Values,
};
use amudai_format::schema::{BasicType, BasicTypeDescriptor};
use std::{borrow::Cow, ops::Range};

/// A sequence of values with optional offsets and presence information,
/// capable of representing both fixed-size and variable-size primitive types.
///
/// This is the simplest representation of a value sequence, using fully decoded,
/// contiguous buffers for storage.
///
/// For primitive types (e.g., `Int32`, `Float64`), values are stored directly in
/// the `values` buffer, with no `offsets`.
///
/// Variable-sized values (`String` and `Binary`) are stored as a concatenated,
/// contiguous byte buffer in `values`, accompanied by `offsets` (containing N+1
/// offsets for the corresponding values in the `values` buffer). The value at
/// index `i` occupies the byte range `offsets[i]..offsets[i+1]` in the `values`
/// buffer.
///
/// `FixedSizeBinary` values are similarly stored as a contiguous buffer in `values`,
/// but do not use `offsets`. Instead, `type_desc.fixed_size` specifies the uniform
/// size of each value. The value at index `i` is located at
/// `i * fixed_size..(i + 1) * fixed_size` byte range in the `values` buffer.
///
/// The `presence` field tracks which values are null or non-null.
///
/// See also `amudai-arrow-compat` crate for the `ValueSequence` to Arrow Array
/// conversion functionality.
#[derive(Debug, Clone)]
pub struct ValueSequence {
    pub values: Values,
    pub offsets: Option<Offsets>,
    pub presence: Presence,
    pub type_desc: BasicTypeDescriptor,
}

impl ValueSequence {
    /// Creates an empty `Sequence` for a given basic type.
    pub fn empty(type_desc: BasicTypeDescriptor) -> ValueSequence {
        ValueSequence {
            values: Values::new(),
            offsets: type_desc.basic_type.requires_offsets().then(Offsets::new),
            presence: Presence::Trivial(0),
            type_desc,
        }
    }

    /// Creates an empty `Sequence` with the specified capacity pre-allocated.
    ///
    /// This method pre-allocates memory for the specified number of elements without
    /// adding any actual elements to the sequence.
    ///
    /// # Arguments
    ///
    /// * `type_desc` - The basic type descriptor for the sequence
    /// * `capacity` - The number of elements to pre-allocate space for
    ///
    /// # Returns
    ///
    /// A new empty sequence with the specified capacity.
    pub fn with_capacity(type_desc: BasicTypeDescriptor, capacity: usize) -> ValueSequence {
        let elem_size = type_desc.primitive_size().unwrap_or(0);
        let values = Values::with_byte_capacity(capacity * elem_size);
        let offsets = type_desc
            .basic_type
            .requires_offsets()
            .then(|| Offsets::with_capacity(capacity));
        ValueSequence {
            values,
            offsets,
            presence: Presence::Trivial(0),
            type_desc,
        }
    }

    /// Creates a `Sequence` with a specified number of nulls.
    pub fn nulls(type_desc: BasicTypeDescriptor, len: usize) -> ValueSequence {
        let presence = Presence::Nulls(len);
        let offsets = type_desc
            .basic_type
            .requires_offsets()
            .then(|| Offsets::zeroed(len));
        let elem_size = type_desc.primitive_size().unwrap_or(0);
        let values = Values::zeroed_bytes(len * elem_size);
        ValueSequence {
            values,
            offsets,
            presence,
            type_desc,
        }
    }

    /// Returns the number of elements in the sequence.
    ///
    /// This count includes both present and null values. For example, a sequence
    /// with 3 elements where one is null will have a length of 3.
    ///
    /// # Returns
    /// The total number of logical elements in the sequence.
    #[inline]
    pub fn len(&self) -> usize {
        self.presence.len()
    }

    /// Returns `true` if the sequence contains no elements.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Appends a binary value to the sequence.
    ///
    /// This method adds a single binary value to the end of the sequence.
    /// For variable-length binary types, it updates the offsets to track the length
    /// of this binary data. For fixed-size binary types, it verifies the value has
    /// the expected length.
    ///
    /// # Arguments
    ///
    /// * `value` - A byte slice containing the binary data to append
    ///
    /// # Panics
    ///
    /// * If the sequence's basic type is not `Binary` or `FixedSizeBinary`.
    /// * For `FixedSizeBinary` type, panics if the binary data length doesn't match
    ///   the fixed size.
    pub fn push_binary(&mut self, value: &[u8]) {
        if self.type_desc.basic_type == BasicType::FixedSizeBinary {
            assert_eq!(self.type_desc.fixed_size, value.len());
        } else {
            assert_eq!(self.type_desc.basic_type, BasicType::Binary);
            self.offsets.as_mut().unwrap().push_length(value.len());
        }
        self.values.extend_from_slice(value);
        self.presence.push_non_null();
    }

    /// Appends a string value to the sequence.
    ///
    /// This method adds a single string value to the end of the sequence, updating the
    /// offsets to track the length of this string and marking it as non-null.
    ///
    /// # Panics
    ///
    /// Panics if the sequence's basic type is not `String`.
    ///
    /// # Arguments
    ///
    /// * `value` - A string slice containing the text to append
    pub fn push_str(&mut self, value: &str) {
        if self.type_desc.basic_type == BasicType::FixedSizeBinary {
            self.push_binary(value.as_bytes());
        } else {
            assert!(
                self.type_desc.basic_type == BasicType::String
                    || self.type_desc.basic_type == BasicType::Binary
            );
            self.offsets.as_mut().unwrap().push_length(value.len());
            self.values.extend_from_slice(value.as_bytes());
            self.presence.push_non_null();
        }
    }

    /// Pushes a primitive value to the end of the sequence.
    ///
    /// This method adds a single value of a primitive type (such as integers or floating-point numbers)
    /// to the end of the sequence and marks it as non-null.
    ///
    /// # Type Parameters
    ///
    /// * `T` - A primitive type that implements `bytemuck::NoUninit` (safely convertible to bytes)
    ///
    /// # Panics
    ///
    /// * If the size of type `T` doesn't match the sequence's primitive size (as defined by the type descriptor)
    /// * If the sequence has offsets (used for variable-length types like Binary or String)
    ///
    /// # Arguments
    ///
    /// * `value` - The primitive value to append
    pub fn push_value<T>(&mut self, value: T)
    where
        T: bytemuck::NoUninit,
    {
        assert_eq!(
            self.type_desc.primitive_size(),
            Some(std::mem::size_of::<T>())
        );
        assert!(self.offsets.is_none());
        self.values.push(value);
        self.presence.push_non_null();
    }

    /// Pushes a null value to the end of the sequence.
    pub fn push_null(&mut self) {
        if let Some(ref mut offsets) = self.offsets {
            offsets.push_length(0);
        }
        self.presence.push_null();
        let size = self.type_desc.primitive_size().unwrap_or(0);
        self.values
            .resize_zeroed_bytes(self.values.bytes_len() + size);
    }

    /// Pushes multiple null values to the end of the sequence.
    pub fn push_nulls(&mut self, count: usize) {
        if count == 0 {
            return;
        }

        if let Some(ref mut offsets) = self.offsets {
            offsets.push_empty(count);
        }
        self.presence.extend_with_nulls(count);
        let size = self.type_desc.primitive_size().unwrap_or(0);
        self.values
            .resize_zeroed_bytes(self.values.bytes_len() + size * count);
    }

    /// Extends this sequence by appending elements from a slice.
    ///
    /// This method appends all elements from the provided slice to the end of the sequence.
    /// The elements are treated as non-null values.
    ///
    /// # Type Requirements
    /// This method requires that the element type `T` in the slice matches the primitive type
    /// of the sequence (checked via the `primitive_size` of the sequence's type descriptor).
    ///
    /// # Panics
    /// * If the size of type `T` doesn't match the primitive size of the sequence's type descriptor.
    /// * If the sequence has offsets (variable-length types like Binary or String). For those types,
    ///   use `BinarySequenceBuilder` or `extend_from_range` instead.
    ///
    /// # Arguments
    /// * `values` - A slice containing the values to append
    pub fn extend_from_slice<T>(&mut self, values: &[T])
    where
        T: bytemuck::NoUninit,
    {
        assert_eq!(
            self.type_desc.primitive_size(),
            Some(std::mem::size_of::<T>())
        );
        assert!(self.offsets.is_none());
        self.values.extend_from_slice(values);
        self.presence.extend_with_non_nulls(values.len());
    }

    /// Extends this sequence by appending values from a range of another sequence.
    ///
    /// This method copies a subset of elements from the source sequence, starting at the specified
    /// offset and containing `len` elements. It handles different basic types appropriately, including:
    /// - Primitive numeric types (Int8, Int16, Int32, Int64, Float32, Float64)
    /// - Variable-length types (Binary, String) with the necessary offset adjustments
    /// - Fixed-size types (FixedSizeBinary, Guid) with proper size calculations
    ///
    /// Both sequences must have the same type descriptor and matching offset requirements.
    ///
    /// # Arguments
    /// * `source` - The source sequence to copy elements from
    /// * `offset` - The logical starting position in the source sequence (zero-based)
    /// * `len` - The logical number of elements to copy
    ///
    /// # Panics
    /// * If `offset + len > source.len()`
    /// * If the type descriptors of the sequences don't match
    /// * If the offset requirements of the sequences don't match
    pub fn extend_from_sequence(&mut self, source: &ValueSequence, offset: usize, len: usize) {
        assert!(offset + len <= source.len());
        assert_eq!(self.type_desc, source.type_desc);
        assert_eq!(self.offsets.is_some(), source.offsets.is_some());

        match self.type_desc.basic_type {
            BasicType::Unit => (),
            BasicType::Boolean => todo!(),
            BasicType::Int8 => self.append_values::<u8>(&source.values, offset, len),
            BasicType::Int16 => self.append_values::<u16>(&source.values, offset, len),
            BasicType::Int32 | BasicType::Float32 => {
                self.append_values::<u32>(&source.values, offset, len)
            }
            BasicType::Int64 | BasicType::Float64 | BasicType::DateTime => {
                self.append_values::<u64>(&source.values, offset, len)
            }
            BasicType::Binary | BasicType::String => {
                let offsets = source.offsets.as_ref().expect("offsets are required");
                let data_start = offsets[offset] as usize;
                let data_end = offsets[offset + len] as usize;
                self.append_values::<u8>(&source.values, data_start, data_end - data_start);
            }
            BasicType::FixedSizeBinary => {
                let size = self.type_desc.fixed_size;
                self.append_values::<u8>(&source.values, offset * size, len * size);
            }
            BasicType::Guid => {
                self.append_values::<u8>(&source.values, offset * 16, len * 16);
            }
            BasicType::List
            | BasicType::FixedSizeList
            | BasicType::Struct
            | BasicType::Map
            | BasicType::Union => (),
        }

        if let Some(ref mut offsets) = self.offsets {
            let src_offsets = source.offsets.as_ref().expect("src offsets are required");
            offsets.extend_from_offsets_range(src_offsets, offset, len);
        }

        self.presence
            .extend_from_presence_range(&source.presence, offset, len);
    }

    /// Returns iterator over variable-sized values in the sequence.
    /// Returns `None`, if the sequence doesn't contain variable-sized binary values.
    ///
    /// The method doesn't return any information on the presence of values.
    /// The caller should check the presence of values using the `presence` field.
    pub fn binary_values(&self) -> Option<BinaryValuesIter<'_, OffsetsIter<'_>>> {
        self.offsets
            .as_ref()
            .map(|offsets| BinaryValuesIter::new(&self.values, OffsetsIter::new(offsets)))
    }

    /// Returns iterator over fixed-sized values in the sequence.
    /// Returns `None`, if the sequence is not of type `FixedSizeBinary`.
    ///
    /// The method doesn't return any information on the presence of values.
    /// The caller should check the presence of values using the `presence` field.
    pub fn fixed_binary_values(&self) -> Option<BinaryValuesIter<'_, FixedSizeOffsetsIter>> {
        if self.type_desc.basic_type == BasicType::FixedSizeBinary {
            let fixed_size = self.type_desc.fixed_size;
            let count = self.values.bytes_len() / fixed_size;
            Some(BinaryValuesIter::new(
                &self.values,
                FixedSizeOffsetsIter::new(fixed_size, count),
            ))
        } else {
            None
        }
    }

    /// Returns a binary value at the specified index.
    ///
    /// # Panics
    ///
    /// Panics in the following cases:
    ///
    ///   * The sequence is not of the appropriate data type, i.e.,
    ///     does not have offsets or not of `FixedSizeBinary` type.
    ///
    ///   * Index is out of bounds.
    ///
    /// # Returns
    ///
    /// A byte slice representing the binary value at the specified index.
    ///
    pub fn binary_at(&self, index: usize) -> &[u8] {
        let range = if let Some(offsets) = self.offsets.as_ref() {
            offsets[index] as usize..offsets[index + 1] as usize
        } else {
            assert_eq!(self.type_desc.basic_type, BasicType::FixedSizeBinary);
            let start = self.type_desc.fixed_size * index;
            let end = start + self.type_desc.fixed_size;
            start..end
        };
        &self.values.as_bytes()[range]
    }

    pub fn string_at(&self, index: usize) -> &str {
        let offsets = self.offsets.as_ref().expect("missing offsets");
        let range = offsets[index] as usize..offsets[index + 1] as usize;
        std::str::from_utf8(&self.values.as_bytes()[range]).expect("invalid utf8")
    }
}

impl ValueSequence {
    fn append_values<T>(&mut self, other: &Values, offset: usize, len: usize)
    where
        T: bytemuck::NoUninit + bytemuck::AnyBitPattern,
    {
        self.values
            .extend_from_slice(&other.as_slice::<T>()[offset..offset + len]);
    }
}

impl Sequence for ValueSequence {
    fn as_any(&self) -> &(dyn std::any::Any + Send + Sync + 'static) {
        self as _
    }

    fn basic_type(&self) -> BasicTypeDescriptor {
        self.type_desc
    }

    fn len(&self) -> usize {
        ValueSequence::len(self)
    }

    fn is_empty(&self) -> bool {
        ValueSequence::is_empty(self)
    }

    fn to_value_sequence(&self) -> Cow<ValueSequence> {
        Cow::Borrowed(self)
    }
}

/// Iterator over binary values in a sequence.
pub struct BinaryValuesIter<'a, R>
where
    R: Iterator<Item = Range<usize>>,
{
    bytes: &'a [u8],
    ranges: R,
}

impl<'a, R> BinaryValuesIter<'a, R>
where
    R: Iterator<Item = Range<usize>>,
{
    pub fn new(values: &'a Values, ranges: R) -> Self {
        Self {
            bytes: values.as_bytes(),
            ranges,
        }
    }
}

impl<'a, R> Iterator for BinaryValuesIter<'a, R>
where
    R: Iterator<Item = Range<usize>>,
{
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(range) = self.ranges.next() {
            Some(&self.bytes[range])
        } else {
            None
        }
    }
}

/// Builder for sequences of variable-sized values.
pub struct BinarySequenceBuilder {
    type_desc: BasicTypeDescriptor,
    values: Values,
    offsets: Offsets,
    presence: PresenceBuilder,
}

impl BinarySequenceBuilder {
    pub fn new(type_desc: BasicTypeDescriptor) -> Self {
        Self {
            type_desc,
            values: Values::new(),
            offsets: Offsets::new(),
            presence: PresenceBuilder::new(),
        }
    }

    pub fn add_value(&mut self, value: &[u8]) {
        self.values.extend_from_slice(value);
        self.offsets.push_length(value.len());
        self.presence.add_non_null();
    }

    pub fn add_null(&mut self) {
        self.offsets.push_length(0);
        self.presence.add_null();
    }

    pub fn add_n_nulls(&mut self, n: usize) {
        self.offsets.push_empty(n);
        self.presence.add_n_nulls(n);
    }

    pub fn build(self) -> ValueSequence {
        ValueSequence {
            type_desc: self.type_desc,
            values: self.values,
            offsets: Some(self.offsets),
            presence: self.presence.build(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_binary_sequence() {
        let type_desc = BasicTypeDescriptor {
            basic_type: amudai_format::schema::BasicType::Binary,
            signed: false,
            fixed_size: 0,
        };
        let mut builder = BinarySequenceBuilder::new(type_desc);
        builder.add_value(b"hello");
        builder.add_null();
        builder.add_value(b"world");
        builder.add_n_nulls(2);

        let sequence = builder.build();
        assert_eq!(sequence.values.as_bytes(), b"helloworld");
        assert_eq!(
            sequence.offsets.as_ref().unwrap().as_slice(),
            &[0, 5, 5, 10, 10, 10]
        );
        assert_eq!(sequence.presence.len(), 5);
        match &sequence.presence {
            Presence::Bytes(presence) => {
                assert_eq!(presence.typed_data::<i8>(), &[1, 0, 1, 0, 0]);
            }
            _ => panic!("Expected presence to be Bytes"),
        }

        let binary_values = sequence.binary_values().unwrap();
        let values: Vec<&[u8]> = binary_values.collect();
        assert_eq!(values.len(), 5);
        assert_eq!(vec![b"hello" as &[u8], b"", b"world", b"", b""], values);
    }

    #[test]
    fn test_empty_sequence() {
        let type_desc = BasicTypeDescriptor {
            basic_type: amudai_format::schema::BasicType::Int32,
            signed: true,
            fixed_size: 0,
        };
        let sequence = ValueSequence::empty(type_desc);

        assert_eq!(sequence.len(), 0);
        assert!(sequence.is_empty());
        assert!(sequence.binary_values().is_none());
    }

    #[test]
    fn test_nulls_sequence() {
        let type_desc = BasicTypeDescriptor {
            basic_type: amudai_format::schema::BasicType::Int64,
            signed: true,
            fixed_size: 0,
        };
        let sequence = ValueSequence::nulls(type_desc, 5);

        assert_eq!(sequence.len(), 5);
        assert!(!sequence.is_empty());
        assert!(sequence.binary_values().is_none());

        match &sequence.presence {
            Presence::Nulls(len) => assert_eq!(*len, 5),
            _ => panic!("Expected presence to be Nulls"),
        }
    }

    #[test]
    fn test_extend_from_range() {
        let type_desc = BasicTypeDescriptor {
            basic_type: amudai_format::schema::BasicType::Binary,
            signed: false,
            fixed_size: 0,
        };

        let mut source_builder = BinarySequenceBuilder::new(type_desc.clone());
        source_builder.add_value(b"first");
        source_builder.add_value(b"second");
        source_builder.add_value(b"third");
        source_builder.add_null();
        source_builder.add_value(b"fifth");
        let source = source_builder.build();

        let mut target = ValueSequence::empty(type_desc);

        target.extend_from_sequence(&source, 1, 3);

        let values: Vec<&[u8]> = target.binary_values().unwrap().collect();
        assert_eq!(values.len(), 3);
        assert_eq!(vec![b"second" as &[u8], b"third", b""], values);

        match &target.presence {
            Presence::Bytes(presence) => {
                assert_eq!(presence.typed_data::<i8>(), &[1, 1, 0]);
            }
            _ => panic!("Expected presence to be Bytes"),
        }
    }

    #[test]
    fn test_fixed_size_binary_sequence() {
        let type_desc = BasicTypeDescriptor {
            basic_type: amudai_format::schema::BasicType::FixedSizeBinary,
            signed: false,
            fixed_size: 4,
        };

        let mut sequence = ValueSequence::empty(type_desc);

        let source_desc = BasicTypeDescriptor {
            basic_type: amudai_format::schema::BasicType::FixedSizeBinary,
            signed: false,
            fixed_size: 4,
        };
        let mut source = ValueSequence::empty(source_desc);

        source
            .values
            .extend_from_slice::<u8>(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]);
        source.presence = Presence::Trivial(3);

        sequence.extend_from_sequence(&source, 1, 2);

        assert_eq!(sequence.values.as_bytes(), &[5, 6, 7, 8, 9, 10, 11, 12]);
        assert_eq!(sequence.len(), 2);
        assert!(sequence.binary_values().is_none());
    }

    #[test]
    fn test_string_sequence() {
        let type_desc = BasicTypeDescriptor {
            basic_type: amudai_format::schema::BasicType::String,
            signed: false,
            fixed_size: 0,
        };
        let mut builder = BinarySequenceBuilder::new(type_desc);
        builder.add_value("hello".as_bytes());
        builder.add_value("world".as_bytes());

        let sequence = builder.build();
        let values: Vec<&[u8]> = sequence.binary_values().unwrap().collect();
        assert_eq!(values.len(), 2);
        assert_eq!(vec!["hello".as_bytes(), "world".as_bytes()], values);
    }

    #[test]
    fn test_push_binary() {
        let type_desc = BasicTypeDescriptor {
            basic_type: BasicType::Binary,
            signed: false,
            fixed_size: 0,
        };
        let mut sequence = ValueSequence::empty(type_desc);

        sequence.push_binary(b"first");
        sequence.push_binary(b"second");
        sequence.push_binary(b"third");

        assert_eq!(sequence.values.as_bytes(), b"firstsecondthird");

        let offsets = sequence.offsets.as_ref().unwrap();
        assert_eq!(offsets.as_slice(), &[0, 5, 11, 16]);

        assert_eq!(sequence.presence.count_non_nulls(), 3);
    }

    #[test]
    fn test_push_binary_empty_values() {
        let type_desc = BasicTypeDescriptor {
            basic_type: BasicType::Binary,
            signed: false,
            fixed_size: 0,
        };
        let mut sequence = ValueSequence::empty(type_desc);

        sequence.push_binary(b"");
        sequence.push_binary(b"");

        let offsets = sequence.offsets.as_ref().unwrap();
        assert_eq!(offsets.as_slice(), &[0, 0, 0]);

        assert_eq!(sequence.presence.len(), 2);
        assert_eq!(sequence.presence.count_non_nulls(), 2);
    }

    #[test]
    fn test_push_fixed_size_binary() {
        let type_desc = BasicTypeDescriptor {
            basic_type: BasicType::FixedSizeBinary,
            signed: false,
            fixed_size: 4,
        };
        let mut sequence = ValueSequence::empty(type_desc);

        sequence.push_binary(&[1, 2, 3, 4]);
        sequence.push_binary(&[5, 6, 7, 8]);

        assert_eq!(sequence.values.as_bytes(), &[1, 2, 3, 4, 5, 6, 7, 8]);
        assert!(sequence.offsets.is_none());
        assert_eq!(sequence.presence.len(), 2);
    }

    #[test]
    #[should_panic]
    fn test_push_fixed_size_binary_wrong_size() {
        let type_desc = BasicTypeDescriptor {
            basic_type: BasicType::FixedSizeBinary,
            signed: false,
            fixed_size: 4,
        };
        let mut sequence = ValueSequence::empty(type_desc);

        // This should panic because the length doesn't match fixed_size
        sequence.push_binary(&[1, 2, 3]);
    }

    #[test]
    #[should_panic]
    fn test_push_binary_wrong_type() {
        let type_desc = BasicTypeDescriptor {
            basic_type: BasicType::Int32,
            signed: true,
            fixed_size: 0,
        };
        let mut sequence = ValueSequence::empty(type_desc);

        // This should panic because the type is not Binary or FixedSizeBinary
        sequence.push_binary(b"test");
    }

    #[test]
    fn test_push_str() {
        let type_desc = BasicTypeDescriptor {
            basic_type: BasicType::String,
            signed: false,
            fixed_size: 0,
        };
        let mut sequence = ValueSequence::empty(type_desc);
        sequence.push_str("hello");
        sequence.push_str("world");
        sequence.push_str("rust");

        assert_eq!(sequence.values.as_bytes(), b"helloworldrust");

        let offsets = sequence.offsets.as_ref().unwrap();
        assert_eq!(offsets.as_slice(), &[0, 5, 10, 14]);

        assert_eq!(sequence.presence.len(), 3);
        assert_eq!(sequence.presence.count_non_nulls(), 3);
    }

    #[test]
    fn test_push_str_empty_and_unicode() {
        let type_desc = BasicTypeDescriptor {
            basic_type: BasicType::String,
            signed: false,
            fixed_size: 0,
        };
        let mut sequence = ValueSequence::empty(type_desc);

        sequence.push_str("");

        let unicode = "こんにちは";
        sequence.push_str(unicode);

        assert_eq!(sequence.values.as_bytes(), "こんにちは".as_bytes());

        let offsets = sequence.offsets.as_ref().unwrap();
        assert_eq!(offsets.as_slice(), &[0, 0, 15]);
        assert_eq!(sequence.presence.len(), 2);
        assert_eq!(sequence.presence.count_non_nulls(), 2);
    }

    #[test]
    fn test_push_value_i32() {
        let type_desc = BasicTypeDescriptor {
            basic_type: BasicType::Int32,
            signed: true,
            fixed_size: 0,
        };
        let mut sequence = ValueSequence::empty(type_desc);

        sequence.push_value(42i32);
        sequence.push_value(-100i32);
        sequence.push_value(0i32);

        let values = sequence.values.as_slice::<i32>();
        assert_eq!(values, &[42, -100, 0]);

        assert_eq!(sequence.presence.len(), 3);
        assert_eq!(sequence.presence.count_non_nulls(), 3);
        assert!(sequence.offsets.is_none());
    }

    #[test]
    fn test_push_value_f64() {
        let type_desc = BasicTypeDescriptor {
            basic_type: BasicType::Float64,
            signed: true,
            fixed_size: 0,
        };
        let mut sequence = ValueSequence::empty(type_desc);

        sequence.push_value(3.14159);
        sequence.push_value(-2.71828);
        sequence.push_value(0.0);

        let values = sequence.values.as_slice::<f64>();
        assert_eq!(values, &[3.14159, -2.71828, 0.0]);

        assert_eq!(sequence.presence.len(), 3);
        assert_eq!(sequence.presence.count_non_nulls(), 3);
    }

    #[test]
    fn test_push_value_u8() {
        let type_desc = BasicTypeDescriptor {
            basic_type: BasicType::Int8,
            signed: false,
            fixed_size: 0,
        };
        let mut sequence = ValueSequence::empty(type_desc);

        sequence.push_value(10u8);
        sequence.push_value(255u8);
        sequence.push_value(0u8);

        let values = sequence.values.as_slice::<u8>();
        assert_eq!(values, &[10, 255, 0]);

        assert_eq!(sequence.presence.len(), 3);
        assert_eq!(sequence.presence.count_non_nulls(), 3);
    }

    #[test]
    #[should_panic]
    fn test_push_value_wrong_size() {
        let type_desc = BasicTypeDescriptor {
            basic_type: BasicType::Int16,
            signed: true,
            fixed_size: 0,
        };
        let mut sequence = ValueSequence::empty(type_desc);

        // This should panic because we're pushing an i32 to an i16 sequence
        sequence.push_value(42i32);
    }

    #[test]
    #[should_panic]
    fn test_push_value_variable_length_type() {
        let type_desc = BasicTypeDescriptor {
            basic_type: BasicType::String,
            signed: false,
            fixed_size: 0,
        };
        let mut sequence = ValueSequence::empty(type_desc);

        // This should panic because push_value can't be used with types that need offsets
        sequence.push_value(42i32);
    }

    #[test]
    fn test_push_null_fixed_size() {
        let type_desc = BasicTypeDescriptor {
            basic_type: BasicType::Int32,
            signed: true,
            fixed_size: 0,
        };
        let mut sequence = ValueSequence::empty(type_desc);

        sequence.push_value(42i32);
        sequence.push_null();
        sequence.push_value(100i32);
        sequence.push_null();

        assert_eq!(sequence.presence.len(), 4);
        assert_eq!(sequence.presence.count_non_nulls(), 2);

        assert_eq!(sequence.values.bytes_len(), 4 * std::mem::size_of::<i32>());

        let values = sequence.values.as_slice::<i32>();
        assert_eq!(values[0], 42);
        assert_eq!(values[2], 100);
    }

    #[test]
    fn test_push_null_string() {
        let type_desc = BasicTypeDescriptor {
            basic_type: BasicType::String,
            signed: false,
            fixed_size: 0,
        };
        let mut sequence = ValueSequence::empty(type_desc);

        sequence.push_str("hello");
        sequence.push_null();
        sequence.push_str("world");
        sequence.push_null();

        assert_eq!(sequence.presence.len(), 4);
        assert_eq!(sequence.presence.count_non_nulls(), 2);

        assert_eq!(sequence.values.as_bytes(), b"helloworld");

        let offsets = sequence.offsets.as_ref().unwrap();
        assert_eq!(offsets.as_slice(), &[0, 5, 5, 10, 10]);

        let values: Vec<&[u8]> = sequence.binary_values().unwrap().collect();
        assert_eq!(values.len(), 4);
        assert_eq!(values, &[b"hello" as &[u8], b"", b"world", b""]);
    }

    #[test]
    fn test_push_null_binary() {
        let type_desc = BasicTypeDescriptor {
            basic_type: BasicType::Binary,
            signed: false,
            fixed_size: 0,
        };
        let mut sequence = ValueSequence::empty(type_desc);

        sequence.push_null();
        sequence.push_binary(b"data");
        sequence.push_null();
        sequence.push_binary(b"more");

        assert_eq!(sequence.len(), 4);
        assert_eq!(sequence.presence.count_non_nulls(), 2);

        let offsets = sequence.offsets.as_ref().unwrap();
        assert_eq!(offsets.as_slice(), &[0, 0, 4, 4, 8]);

        let values: Vec<&[u8]> = sequence.binary_values().unwrap().collect();
        assert_eq!(values, &[b"" as &[u8], b"data", b"", b"more"]);
    }

    #[test]
    fn test_push_null_fixed_size_binary() {
        let type_desc = BasicTypeDescriptor {
            basic_type: BasicType::FixedSizeBinary,
            signed: false,
            fixed_size: 4,
        };
        let mut sequence = ValueSequence::empty(type_desc);

        sequence.push_binary(&[1, 2, 3, 4]);
        sequence.push_null();
        sequence.push_binary(&[5, 6, 7, 8]);

        assert_eq!(sequence.len(), 3);
        assert_eq!(sequence.presence.count_non_nulls(), 2);
        assert_eq!(sequence.values.bytes_len(), 3 * 4);
        assert_eq!(
            sequence.values.as_bytes(),
            &[1, 2, 3, 4, 0, 0, 0, 0, 5, 6, 7, 8]
        );
    }

    #[test]
    fn test_push_multiple_values_and_nulls() {
        let type_desc = BasicTypeDescriptor {
            basic_type: BasicType::Int64,
            signed: true,
            fixed_size: 0,
        };
        let mut sequence = ValueSequence::empty(type_desc);

        sequence.push_value(1i64);
        sequence.push_null();
        sequence.push_value(2i64);
        sequence.push_value(3i64);
        sequence.push_null();
        sequence.push_null();
        sequence.push_value(4i64);

        assert_eq!(sequence.len(), 7);
        assert_eq!(sequence.presence.count_non_nulls(), 4);

        match &sequence.presence {
            Presence::Bytes(presence) => {
                assert_eq!(presence.typed_data::<i8>(), &[1, 0, 1, 1, 0, 0, 1]);
            }
            _ => panic!("Expected presence to be Bytes"),
        }
    }

    #[test]
    fn test_push_nulls() {
        let type_desc = BasicTypeDescriptor {
            basic_type: BasicType::Int64,
            signed: true,
            fixed_size: 0,
        };
        let mut sequence = ValueSequence::empty(type_desc);
        sequence.push_value(1i64);
        sequence.push_value(2i64);
        sequence.push_nulls(3);
        sequence.push_value(3i64);

        assert_eq!(sequence.len(), 6);
        assert_eq!(sequence.presence.count_non_nulls(), 3);

        match &sequence.presence {
            Presence::Bytes(presence) => {
                assert_eq!(presence.typed_data::<i8>(), &[1, 1, 0, 0, 0, 1]);
            }
            _ => panic!("Expected presence to be Bytes"),
        }

        let values = sequence.values.as_slice::<i64>();
        assert_eq!(values, &[1, 2, 0, 0, 0, 3]);
    }
}

use amudai_bytes::Bytes;

use amudai_common::{result::Result, verify_arg};

use super::schema::{BasicType, HashLookup, HashLookupRef};

impl BasicType {
    /// Returns `true` if the type is a composite (container) type.
    ///
    /// Composite types include:
    /// - `List`: Variable-length list of values of the same type
    /// - `FixedSizeList`: Fixed-length list of values of the same type
    /// - `Struct`: Collection of named fields
    /// - `Map`: Key-value pairs
    /// - `Union`: Allows values of different types
    pub fn is_composite(&self) -> bool {
        matches!(
            self,
            BasicType::List
                | BasicType::FixedSizeList
                | BasicType::Struct
                | BasicType::Map
                | BasicType::Union
        )
    }

    /// Returns the maximum allowed number of child `DataType` nodes for this type.
    ///
    /// This indicates how many child types can be associated with this type in the
    /// schema:
    /// - Primitive types: 0 (they don't have child types)
    /// - `List` and `FixedSizeList`: 1 (the element type)
    /// - `Map`: 2 (key and value types)
    /// - `Struct` and `Union`: up to 1000000
    pub fn max_children(&self) -> usize {
        match self {
            BasicType::Unit
            | BasicType::Boolean
            | BasicType::Int8
            | BasicType::Int16
            | BasicType::Int32
            | BasicType::Int64
            | BasicType::Float32
            | BasicType::Float64
            | BasicType::Binary
            | BasicType::FixedSizeBinary
            | BasicType::String
            | BasicType::Guid
            | BasicType::DateTime => 0,
            BasicType::List => 1,
            BasicType::FixedSizeList => 1,
            BasicType::Struct => 1_000_000,
            BasicType::Map => 2,
            BasicType::Union => 1_000_000,
        }
    }

    /// Returns `true` if children of this type must have names.
    ///
    /// Currently, only `Struct` types require their children to be named.
    /// Each field in a struct must have a name that identifies it.
    pub fn requires_named_children(&self) -> bool {
        matches!(self, BasicType::Struct)
    }

    /// Returns `true` if children of this type can have names.
    ///
    /// Both `Struct` and `Union` types allow named children:
    /// - For `Struct`: Names are required for all fields
    /// - For `Union`: Names are optional but can be used to identify different
    ///   variants
    pub fn allows_named_children(&self) -> bool {
        matches!(self, BasicType::Struct | BasicType::Union)
    }

    /// Returns `true` if the value sequence for this type requires offset encoding.
    ///
    /// Types that require offsets have variable-length values or collections:
    /// - `Binary`: Variable-length byte arrays
    /// - `String`: Variable-length UTF-8 encoded strings
    /// - `List`: Variable-length collections
    /// - `Map`: Key-value collections
    pub fn requires_offsets(&self) -> bool {
        matches!(
            self,
            BasicType::Binary | BasicType::String | BasicType::List | BasicType::Map
        )
    }

    /// Returns `true` if this is one of the integer types (i8, i16, i32, or i64).
    pub fn is_integer(&self) -> bool {
        matches!(
            self,
            BasicType::Int8 | BasicType::Int16 | BasicType::Int32 | BasicType::Int64
        )
    }

    /// Returns `true` if this is one of the integer-valued types (integer or DateTime).
    pub fn is_integer_or_datetime(&self) -> bool {
        self.is_integer() || *self == BasicType::DateTime
    }
}

/// Describes a basic data type, including its size and signedness.
/// For a nested `DataType` node, this struct defines the fundamental type of the node
/// itself (e.g., `Struct`, `List`, `Map`, etc.), without considering any child nodes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BasicTypeDescriptor {
    /// The underlying physical type of the value.
    pub basic_type: BasicType,
    /// The fixed size of the vector-like type, if applicable.
    /// This field is relevant only for `FixedSizeBinary` (where it indicates the size of the
    /// binary value in bytes) and `FixedSizeList` (where it specifies the number of elements
    /// in each list value).
    /// For any other basic type, this field is zero.
    pub fixed_size: usize,
    /// Indicates whether the type is signed.
    /// This can be `true` only for `Int8`, `Int16`, `Int32`, and `Int64`.
    pub signed: bool,
}

impl BasicTypeDescriptor {
    /// Returns the fixed size of the primitive basic type in bytes, or `None`
    /// if the type is variable-length or composite.
    pub fn primitive_size(&self) -> Option<usize> {
        match self.basic_type {
            BasicType::Unit | BasicType::Boolean => None,
            BasicType::Int8 => Some(1),
            BasicType::Int16 => Some(2),
            BasicType::Int32 => Some(4),
            BasicType::Int64 => Some(8),
            BasicType::Float32 => Some(4),
            BasicType::Float64 => Some(8),
            BasicType::Binary => None,
            BasicType::FixedSizeBinary => Some(self.fixed_size),
            BasicType::String => None,
            BasicType::Guid => Some(16),
            BasicType::DateTime => Some(8),
            BasicType::List
            | BasicType::FixedSizeList
            | BasicType::Struct
            | BasicType::Map
            | BasicType::Union => None,
        }
    }
}

impl Default for BasicTypeDescriptor {
    fn default() -> Self {
        Self {
            basic_type: BasicType::Unit,
            fixed_size: 0,
            signed: false,
        }
    }
}

impl HashLookup {
    /// Given `count` items and a hash function `hash_fn` where `hash_fn(i)` returns a hash value
    /// for item `i`, builds a `HashLookup` structure such that:
    ///
    /// - The number of logical buckets is a power of 2, and the length of the buckets array
    ///   is `num_buckets + 1`.
    /// - The number of entries is `count`.
    /// - For a given item `i`, the bucket index `b` is `hash_fn(i) % (buckets.len() - 1)`.
    /// - The range of relevant entries is `buckets[b]..buckets[b + 1]`.
    /// - Each of the entries in the range `entries[buckets[b]..buckets[b + 1]]` contains an index
    ///   of the item (`i`), where the bucket index for the item `i` is `b`.
    pub fn build(count: usize, hash_fn: impl Fn(usize) -> u64) -> HashLookup {
        assert!(count < (u32::MAX / 4) as usize);
        let num_buckets = (count.max(4) / 2).next_power_of_two();
        let mask = num_buckets as u64 - 1;

        let mut buckets = vec![0u32; num_buckets + 1];
        let mut counters = vec![0u32; num_buckets];

        for i in 0..count {
            let bucket_idx = (hash_fn(i) & mask) as usize;
            counters[bucket_idx] += 1;
        }

        let mut next_entry = 0;
        for i in 0..num_buckets {
            buckets[i] = next_entry;
            next_entry += counters[i];
        }
        assert_eq!(next_entry as usize, count);
        buckets[num_buckets] = next_entry;

        let mut entries = vec![0u32; count];
        for i in 0..count {
            let bucket_idx = (hash_fn(i) & mask) as usize;
            let entry_idx = buckets[bucket_idx] as usize;
            entries[entry_idx] = i as u32;
            buckets[bucket_idx] += 1;
        }
        assert_eq!(buckets[num_buckets - 1], count as u32);

        for i in (0..num_buckets).rev() {
            buckets[i + 1] = buckets[i];
        }
        buckets[0] = 0;

        HashLookup { buckets, entries }
    }
}

impl HashLookupRef<'_> {
    pub fn find(
        &self,
        hash: u64,
        verifier_fn: impl Fn(usize) -> Result<bool>,
    ) -> Result<Option<usize>> {
        let buckets = self.buckets()?;
        verify_arg!(buckets, (buckets.len() - 1).is_power_of_two());
        let entries = self.entries()?;

        let mask = (buckets.len() - 2) as u64;
        let bucket_idx = (hash & mask) as usize;

        let start = buckets.get(bucket_idx).expect("valid bucket_idx") as usize;
        let end = buckets.get(bucket_idx + 1).expect("valid bucket_idx") as usize;
        verify_arg!(start, start <= end);
        verify_arg!(end, end <= entries.len());

        for entry_idx in start..end {
            let i = entries.get(entry_idx).expect("valid entry_idx") as usize;
            if verifier_fn(i)? {
                return Ok(Some(i));
            }
        }
        Ok(None)
    }
}

pub fn hash_field_name(name: &str) -> u64 {
    xxhash_rust::xxh3::xxh3_64(name.as_bytes())
}

pub(crate) struct OwnedTableRef<RefType: details::TableRefType>(RefType::T<'static>, Bytes);

impl<RefType: details::TableRefType> OwnedTableRef<RefType> {
    pub fn new_as_root(buf: Bytes) -> Result<OwnedTableRef<RefType>>
    where
        for<'a> RefType::T<'a>: planus::ReadAsRoot<'a>,
    {
        use planus::ReadAsRoot;
        let inner = RefType::T::read_as_root(&buf)?;
        Ok(OwnedTableRef(
            #[allow(clippy::unnecessary_cast)]
            unsafe { &*(&inner as *const RefType::T<'_> as *const RefType::T<'static>) }.clone(),
            Self::clone_buf(&buf),
        ))
    }

    pub unsafe fn wrap(buf: Bytes, inner: RefType::T<'_>) -> OwnedTableRef<RefType> {
        OwnedTableRef(
            #[allow(clippy::unnecessary_cast)]
            unsafe {
                (*(&inner as *const RefType::T<'_> as *const RefType::T<'static>)).clone()
            },
            Self::clone_buf(&buf),
        )
    }

    pub fn get<'a>(&'a self) -> RefType::T<'a> {
        #[allow(clippy::unnecessary_cast)]
        unsafe { &*(&self.0 as *const RefType::T<'_> as *const RefType::T<'a>) }.clone()
    }

    pub fn map<MappedType: details::TableRefType, F>(
        &self,
        f: F,
    ) -> Result<OwnedTableRef<MappedType>>
    where
        F: for<'a> FnOnce(RefType::T<'a>, &'a ()) -> Result<MappedType::T<'a>>,
    {
        let res = f(self.0.clone(), &())?;
        Ok(unsafe { OwnedTableRef::wrap(Self::clone_buf(&self.1), res) })
    }

    fn clone_buf(src: &Bytes) -> Bytes {
        let buf = src.clone();
        assert_eq!(src.as_ptr(), buf.as_ptr());
        buf
    }
}

impl<RefType: details::TableRefType> Clone for OwnedTableRef<RefType> {
    fn clone(&self) -> Self {
        Self(self.0.clone(), Self::clone_buf(&self.1))
    }
}

pub(crate) type OwnedSchemaRef = OwnedTableRef<crate::defs::schema::Schema>;

pub(crate) type OwnedFieldRef = OwnedTableRef<crate::defs::schema::Field>;

pub(crate) type OwnedDataTypeRef = OwnedTableRef<crate::defs::schema::DataType>;

mod details {
    pub trait TableRefType {
        type T<'a>: Clone;
    }

    impl TableRefType for crate::defs::schema::Schema {
        type T<'a> = crate::defs::schema::SchemaRef<'a>;
    }

    impl TableRefType for crate::defs::schema::Field {
        type T<'a> = crate::defs::schema::FieldRef<'a>;
    }

    impl TableRefType for crate::defs::schema::DataType {
        type T<'a> = crate::defs::schema::DataTypeRef<'a>;
    }
}

#[cfg(test)]
mod tests {
    use planus::ReadAsRoot;

    use crate::defs::schema::{HashLookup, HashLookupRef};

    #[test]
    fn test_hash_lookup() {
        let numbers = (0..1000u64).map(|i| hash(i * 10)).collect::<Vec<_>>();
        let lookup = HashLookup::build(numbers.len(), |i| hash(numbers[i]));
        let lookup_bytes = planus::Builder::new().finish(&lookup, None).to_vec();
        let lookup = HashLookupRef::read_as_root(&lookup_bytes).unwrap();

        for &n in &numbers {
            let h = hash(n);
            let res = lookup.find(h, |i| Ok(numbers[i] == n)).unwrap();
            assert!(res.is_some());
            assert_eq!(numbers[res.unwrap()], n);
        }

        for n in 0..10000 {
            let h = hash(n);
            let res = lookup.find(h, |i| Ok(numbers[i] == n)).unwrap();
            assert!(res.is_none());
        }
    }

    fn hash(n: u64) -> u64 {
        xxhash_rust::xxh3::xxh3_64(&n.to_le_bytes())
    }
}

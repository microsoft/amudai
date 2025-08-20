use std::{
    collections::{HashMap, HashSet},
    hash::Hasher,
};

/// A HashMap that uses an identity hasher for primitive integer keys.
///
/// This type alias provides a HashMap implementation that uses `IdentityHasher` as the
/// hash function. It's optimized for use with primitive integer types where the hash
/// value is simply the integer value itself, avoiding the overhead of actual hashing.
///
/// # When to Use
///
/// - Keys are primitive integers with good distribution (not clustered in small ranges)
/// - Performance is critical and you want to avoid hash computation overhead
///
/// # Examples
///
/// ```
/// use amudai_collections::identity_hash::IdentityHashMap;
///
/// let mut map: IdentityHashMap<u32, String> = IdentityHashMap::default();
/// map.insert(42, "value".to_string());
/// assert_eq!(map.get(&42), Some(&"value".to_string()));
/// ```
pub type IdentityHashMap<K, V> = HashMap<K, V, std::hash::BuildHasherDefault<IdentityHasher>>;

/// A HashSet that uses an identity hasher for primitive integer keys.
///
/// This type alias provides a HashSet implementation that uses `IdentityHasher` as the
/// hash function. It's optimized for use with primitive integer types where the hash
/// value is simply the integer value itself, avoiding the overhead of actual hashing.
///
/// # When to Use
///
/// - Keys are primitive integers with good distribution (not clustered in small ranges)
/// - Performance is critical and you want to avoid hash computation overhead
///
/// # Examples
///
/// ```
/// use amudai_collections::identity_hash::IdentityHashSet;
///
/// let mut set: IdentityHashSet<u32> = IdentityHashSet::default();
/// set.insert(42);
/// assert!(set.contains(&42));
/// ```
pub type IdentityHashSet<K> = HashSet<K, std::hash::BuildHasherDefault<IdentityHasher>>;

/// A HashMap that uses an identity hasher for 128-bit integer keys.
///
/// This type alias provides a HashMap implementation that uses `IdentityHasher128` as the
/// hash function. It's optimized for use with 128-bit integer types (`u128`, `i128`) where
/// the hash value is derived directly from the integer value itself, avoiding the overhead
/// of complex hash computation.
///
/// # When to Use
///
/// - Keys are 128-bit integers (`u128`, `i128`) with good distribution
/// - Performance is critical and you want to avoid hash computation overhead
/// - Working with UUIDs, large identifiers, or other 128-bit values as keys
///
/// # Examples
///
/// ```
/// use amudai_collections::identity_hash::IdentityHashMap128;
///
/// let mut map: IdentityHashMap128<u128, String> = IdentityHashMap128::default();
/// let key: u128 = 0x123456789ABCDEF0_FEDCBA0987654321;
/// map.insert(key, "value".to_string());
/// assert_eq!(map.get(&key), Some(&"value".to_string()));
/// ```
pub type IdentityHashMap128<K, V> = HashMap<K, V, std::hash::BuildHasherDefault<IdentityHasher128>>;

/// A HashSet that uses an identity hasher for 128-bit integer keys.
///
/// This type alias provides a HashSet implementation that uses `IdentityHasher128` as the
/// hash function. It's optimized for use with 128-bit integer types (`u128`, `i128`) where
/// the hash value is derived directly from the integer value itself, avoiding the overhead
/// of complex hash computation.
///
/// # When to Use
///
/// - Keys are 128-bit integers (`u128`, `i128`) with good distribution
/// - Performance is critical and you want to avoid hash computation overhead
/// - Working with UUIDs, large identifiers, or other 128-bit values as keys
///
/// # Examples
///
/// ```
/// use amudai_collections::identity_hash::IdentityHashSet128;
///
/// let mut set: IdentityHashSet128<u128> = IdentityHashSet128::default();
/// let value: u128 = 0x123456789ABCDEF0_FEDCBA0987654321;
/// set.insert(value);
/// assert!(set.contains(&value));
/// ```
pub type IdentityHashSet128<K> = HashSet<K, std::hash::BuildHasherDefault<IdentityHasher128>>;

/// A hasher implementation that returns the input value as the hash for primitive integers.
///
/// `IdentityHasher` is designed for use with primitive integer types where the hash
/// value should be the integer value itself. This can provide performance benefits
/// when using integers as hash map keys, as it avoids the computational overhead
/// of hash functions.
///
/// # When to Use
///
/// - Keys are primitive integers with good natural distribution
/// - Performance is critical and hash computation overhead matters
///
/// # Important
///
/// This hasher should only be used with primitive integer types (`u8`, `u16`, `u32`,
/// `u64`, `usize`, `i8`, `i16`, `i32`, `i64`, `isize`). Using it with other types
/// will cause a panic.
///
/// # Examples
///
/// ```
/// use std::hash::{Hash, Hasher};
/// use amudai_collections::identity_hash::IdentityHasher;
///
/// let mut hasher = IdentityHasher::default();
/// 42u32.hash(&mut hasher);
/// assert_eq!(hasher.finish(), 42);
/// ```
pub struct IdentityHasher(u64);

impl Default for IdentityHasher {
    #[inline]
    fn default() -> IdentityHasher {
        IdentityHasher(0)
    }
}

impl Hasher for IdentityHasher {
    #[inline]
    fn finish(&self) -> u64 {
        self.0
    }

    #[inline]
    fn write(&mut self, _: &[u8]) {
        unimplemented!(
            "IdentityHasher is only implemented for primitive integer types: u8, u16, u32, u64, usize, i8, i16, i32, i64, isize"
        );
    }

    #[inline]
    fn write_u8(&mut self, i: u8) {
        self.0 = i as u64;
    }

    #[inline]
    fn write_u16(&mut self, i: u16) {
        self.0 = i as u64;
    }

    #[inline]
    fn write_u32(&mut self, i: u32) {
        self.0 = i as u64;
    }

    #[inline]
    fn write_u64(&mut self, i: u64) {
        self.0 = i;
    }

    #[inline]
    fn write_usize(&mut self, i: usize) {
        self.0 = i as u64;
    }

    #[inline]
    fn write_i8(&mut self, i: i8) {
        self.write_u8(i as u8)
    }

    #[inline]
    fn write_i16(&mut self, i: i16) {
        self.write_u16(i as u16)
    }

    #[inline]
    fn write_i32(&mut self, i: i32) {
        self.write_u32(i as u32)
    }

    #[inline]
    fn write_i64(&mut self, i: i64) {
        self.write_u64(i as u64)
    }

    #[inline]
    fn write_isize(&mut self, i: isize) {
        self.write_usize(i as usize)
    }

    #[inline]
    fn write_u128(&mut self, _i: u128) {
        unimplemented!("IdentityHasher is not implemented for u128 type");
    }

    #[inline]
    fn write_i128(&mut self, _i: i128) {
        unimplemented!("IdentityHasher is not implemented for i128 type");
    }
}

/// A hasher implementation that returns the input value as the hash for 128-bit integers.
///
/// `IdentityHasher128` is designed specifically for use with 128-bit integer types (`u128`, `i128`)
/// where the hash value should be the integer value itself. This provides performance benefits
/// when using 128-bit integers as hash map keys by avoiding hash computation overhead.
///
/// Since the standard `Hasher` trait's `finish()` method returns a `u64`, this hasher
/// combines the high and low 64-bit parts of the 128-bit value using XOR to produce
/// the final hash value.
///
/// # When to Use
///
/// - Keys are 128-bit integers (`u128`, `i128`) with good natural distribution
/// - Performance is critical and hash computation overhead matters
/// - Working with UUIDs, large identifiers, or other 128-bit values
///
/// # Important
///
/// This hasher should only be used with 128-bit integer types (`u128`, `i128`).
/// Using it with other types will cause a panic.
///
/// # Hash Collision Considerations
///
/// Since the `finish()` method must return a `u64` but operates on `u128` values,
/// there's a potential for hash collisions when two different `u128` values
/// have the same XOR of their high and low 64-bit parts. Consider this when
/// using this hasher with values that might exhibit such patterns.
///
/// # Examples
///
/// ```
/// use std::hash::{Hash, Hasher};
/// use amudai_collections::identity_hash::IdentityHasher128;
///
/// let mut hasher = IdentityHasher128::default();
/// let value: u128 = 0x123456789ABCDEF0_FEDCBA0987654321;
/// value.hash(&mut hasher);
///
/// // The hash is the XOR of the high and low 64-bit parts
/// let expected = (0x123456789ABCDEF0_u64) ^ (0xFEDCBA0987654321_u64);
/// assert_eq!(hasher.finish(), expected);
/// ```
pub struct IdentityHasher128(u128);

impl Default for IdentityHasher128 {
    #[inline]
    fn default() -> IdentityHasher128 {
        IdentityHasher128(0)
    }
}

impl Hasher for IdentityHasher128 {
    #[inline]
    fn finish(&self) -> u64 {
        let high = (self.0 >> 64) as u64;
        let low = self.0 as u64;
        high ^ low
    }

    #[inline]
    fn write(&mut self, _: &[u8]) {
        unimplemented!("IdentityHasher128 is only implemented for i128 and u128 types");
    }

    #[inline]
    fn write_u8(&mut self, _i: u8) {
        unimplemented!("IdentityHasher128 is only implemented for i128 and u128 types");
    }

    #[inline]
    fn write_u16(&mut self, _i: u16) {
        unimplemented!("IdentityHasher128 is only implemented for i128 and u128 types");
    }

    #[inline]
    fn write_u32(&mut self, _i: u32) {
        unimplemented!("IdentityHasher128 is only implemented for i128 and u128 types");
    }

    #[inline]
    fn write_u64(&mut self, _i: u64) {
        unimplemented!("IdentityHasher128 is only implemented for i128 and u128 types");
    }

    #[inline]
    fn write_usize(&mut self, _i: usize) {
        unimplemented!("IdentityHasher128 is only implemented for i128 and u128 types");
    }

    #[inline]
    fn write_i8(&mut self, _i: i8) {
        unimplemented!("IdentityHasher128 is only implemented for i128 and u128 types");
    }

    #[inline]
    fn write_i16(&mut self, _i: i16) {
        unimplemented!("IdentityHasher128 is only implemented for i128 and u128 types");
    }

    #[inline]
    fn write_i32(&mut self, _i: i32) {
        unimplemented!("IdentityHasher128 is only implemented for i128 and u128 types");
    }

    #[inline]
    fn write_i64(&mut self, _i: i64) {
        unimplemented!("IdentityHasher128 is only implemented for i128 and u128 types");
    }

    #[inline]
    fn write_isize(&mut self, _i: isize) {
        unimplemented!("IdentityHasher128 is only implemented for i128 and u128 types");
    }

    #[inline]
    fn write_u128(&mut self, i: u128) {
        self.0 = i;
    }

    #[inline]
    fn write_i128(&mut self, i: i128) {
        self.0 = i as u128;
    }
}

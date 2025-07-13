use core::panic;
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
        panic!("IdentityHasher must be used only with the primitive integer types");
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
        panic!("IdentityHasher: write_u128 is not implemented");
    }

    #[inline]
    fn write_i128(&mut self, _i: i128) {
        panic!("IdentityHasher: write_i128 is not implemented");
    }
}

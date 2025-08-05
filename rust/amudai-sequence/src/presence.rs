//! A byte buffer used to encode validity for the values in a sequence.

use amudai_bytes::buffer::AlignedByteVec;

/// A byte buffer used to encode validity for the values in a sequence.
///
/// This enum provides three different storage methods for tracking null/non-null values:
/// - `Trivial`: All values are valid (non-null) - most memory efficient for non-null data
/// - `Nulls`: All values are null - memory efficient for all-null data
/// - `Bytes`: Mixed null/non-null values using a byte array (1=present, 0=null)
#[derive(Debug, Clone)]
pub enum Presence {
    /// All values are valid (present).
    Trivial(usize),

    /// All values are null.
    Nulls(usize),

    /// Presence encoded as byte array, where a byte at position `i` indicates whether
    /// the value at position `i` is valid or not (`1` - value is present, `0` - value
    /// is null).
    Bytes(AlignedByteVec),
}

impl Presence {
    /// Returns the number of values in this presence.
    ///
    /// This represents the total count of values tracked, regardless of whether they
    /// are null or non-null.
    #[inline]
    pub fn len(&self) -> usize {
        match self {
            Self::Trivial(len) => *len,
            Self::Nulls(len) => *len,
            Self::Bytes(presence) => presence.len(),
        }
    }

    /// Returns the number of null values in this `Presence`.
    pub fn count_nulls(&self) -> usize {
        match self {
            Self::Trivial(_) => 0,
            Self::Nulls(len) => *len,
            Self::Bytes(presence) => presence.iter().filter(|&&b| b == 0).count(),
        }
    }

    /// Returns the number of non-null values in this `Presence`.
    pub fn count_non_nulls(&self) -> usize {
        match self {
            Self::Trivial(len) => *len,
            Self::Nulls(_) => 0,
            Self::Bytes(presence) => presence.iter().filter(|&&b| b != 0).count(),
        }
    }

    /// Returns `true` if the length is zero.
    ///
    /// An empty presence means there are no values being tracked.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns `true` if all values are present (non-null).
    ///
    /// This is the case when the presence is in `Trivial` mode.
    #[inline]
    pub fn is_trivial_non_null(&self) -> bool {
        matches!(self, Self::Trivial(_))
    }

    /// Returns `true` if all values are null.
    ///
    /// This is the case when the presence is in `Nulls` mode.
    #[inline]
    pub fn is_trivial_all_null(&self) -> bool {
        matches!(self, Self::Nulls(_))
    }

    /// Returns `true` if the value at the specified index is null.
    ///
    /// # Arguments
    ///
    /// * `index` - The index of the value to check
    ///
    /// # Panics
    ///
    /// Panics if `index` is out of bounds.
    #[inline]
    pub fn is_null(&self, index: usize) -> bool {
        match self {
            Self::Trivial(_) => false,
            Self::Nulls(_) => true,
            Self::Bytes(presence) => presence[index] == 0,
        }
    }

    /// Returns `true` if the value at the specified index is valid (not null).
    ///
    /// # Arguments
    ///
    /// * `index` - The index of the value to check
    ///
    /// # Panics
    ///
    /// Panics if `index` is out of bounds.
    #[inline]
    pub fn is_valid(&self, index: usize) -> bool {
        match self {
            Self::Trivial(_) => true,
            Self::Nulls(_) => false,
            Self::Bytes(presence) => presence[index] != 0,
        }
    }

    /// Creates a new `Presence` representing a range from the current one.
    ///
    /// # Arguments
    ///
    /// * `offset` - The starting index of the range
    /// * `len` - The length of the range
    ///
    /// # Returns
    ///
    /// A new `Presence` instance representing the specified range.
    ///
    /// # Panics
    ///
    /// Panics if `offset + len` exceeds the length of this presence.
    pub fn clone_range(&self, offset: usize, len: usize) -> Presence {
        assert!(offset + len <= self.len());
        match self {
            Presence::Trivial(_) => Presence::Trivial(len),
            Presence::Nulls(_) => Presence::Nulls(len),
            Presence::Bytes(bytes) => Presence::Bytes(AlignedByteVec::copy_from_slice(
                &bytes[offset..offset + len],
            )),
        }
    }

    /// Pushes a null value to this `Presence`.
    pub fn push_null(&mut self) {
        match self {
            Presence::Trivial(len) => {
                if *len > 0 {
                    let mut presence = AlignedByteVec::with_capacity(*len + 1);
                    presence.resize_typed::<i8>(*len, 1);
                    presence.push_typed::<i8>(0);
                    *self = Presence::Bytes(presence);
                } else {
                    *self = Presence::Nulls(1);
                }
            }
            Presence::Nulls(len) => *self = Presence::Nulls(*len + 1),
            Presence::Bytes(presence) => {
                presence.push_typed::<i8>(0);
            }
        }
    }

    /// Pushes a non-null value to this `Presence`.
    pub fn push_non_null(&mut self) {
        match self {
            Presence::Trivial(len) => {
                *self = Presence::Trivial(*len + 1);
            }
            Presence::Nulls(len) => {
                let mut presence = AlignedByteVec::with_capacity(*len + 1);
                presence.resize_typed::<i8>(*len, 0);
                presence.push_typed::<i8>(1);
                *self = Presence::Bytes(presence);
            }
            Presence::Bytes(presence) => {
                presence.push_typed::<i8>(1);
            }
        }
    }

    /// Extends this presence with the specified number of null values.
    ///
    /// # Arguments
    ///
    /// * `count` - The number of null values to append
    ///
    /// # Implementation Notes
    ///
    /// - For `Trivial` presence, this will convert to a `Bytes` representation
    /// - For `Nulls` presence, this will simply increase the length
    /// - For `Bytes` presence, this will append zeros
    pub fn extend_with_nulls(&mut self, count: usize) {
        if count == 0 {
            return;
        }

        if self.is_empty() {
            *self = Self::Nulls(count);
            return;
        }

        match self {
            Self::Trivial(len) => {
                let mut presence = AlignedByteVec::with_capacity(*len + count);
                presence.resize_typed::<i8>(*len, 1);
                presence.resize_typed::<i8>(*len + count, 0);
                *self = Self::Bytes(presence);
            }
            Self::Nulls(len) => {
                *len += count;
            }
            Self::Bytes(presence) => {
                presence.resize_typed::<i8>(presence.len() + count, 0);
            }
        }
    }

    /// Extends this presence with the specified number of non-null values.
    ///
    /// # Arguments
    ///
    /// * `count` - The number of non-null values to append
    ///
    /// # Implementation Notes
    ///
    /// - For `Trivial` presence, this will simply increase the length
    /// - For `Nulls` presence, this will convert to a `Bytes` representation
    /// - For `Bytes` presence, this will append ones
    pub fn extend_with_non_nulls(&mut self, count: usize) {
        if count == 0 {
            return;
        }

        if self.is_empty() {
            *self = Self::Trivial(count);
            return;
        }

        match self {
            Self::Trivial(len) => {
                *len += count;
            }
            Self::Nulls(len) => {
                let mut presence = AlignedByteVec::with_capacity(*len + count);
                presence.resize_typed::<i8>(*len, 0);
                presence.resize_typed::<i8>(*len + count, 1);
                *self = Self::Bytes(presence);
            }
            Self::Bytes(presence) => {
                presence.resize_typed::<i8>(presence.len() + count, 1);
            }
        }
    }

    /// Extends this presence with the specified array of presence bytes.
    ///
    /// The bytes should follow the same convention where 0 represents null
    /// and non-zero represents present values.
    ///
    /// # Arguments
    ///
    /// * `bytes` - The byte array to append
    ///
    /// # Implementation Notes
    ///
    /// This method will optimize storage by:
    /// - Staying in `Trivial` mode if all new bytes are non-null
    /// - Staying in `Nulls` mode if all new bytes are null
    /// - Converting to `Bytes` mode for mixed presence
    pub fn extend_with_bytes(&mut self, bytes: &[u8]) {
        if bytes.is_empty() {
            return;
        }

        if self.is_empty() {
            if bytes.iter().all(|&b| b != 0) {
                *self = Self::Trivial(bytes.len());
            } else if bytes.iter().all(|&b| b == 0) {
                *self = Self::Nulls(bytes.len());
            } else {
                *self = Self::Bytes(AlignedByteVec::copy_from_slice(bytes));
            }
            return;
        }

        match self {
            Self::Trivial(len) if bytes.iter().all(|&b| b != 0) => {
                *len += bytes.len();
            }
            Self::Nulls(len) if bytes.iter().all(|&b| b == 0) => {
                *len += bytes.len();
            }
            _ => {
                self.convert_to_bytes(self.len() + bytes.len());
                let Self::Bytes(presence) = self else {
                    panic!("Expected presence to be Bytes after conversion");
                };
                presence.extend_from_slice(bytes);
            }
        }
    }

    /// Extends this presence by copying a range from another presence.
    ///
    /// # Arguments
    ///
    /// * `src` - The source presence to copy from
    /// * `offset` - The starting position in the source presence
    /// * `len` - The number of values to copy
    ///
    /// # Implementation Notes
    ///
    /// - If this presence is empty, it will directly use a clone of the specified range
    /// - Otherwise, it will efficiently append values based on the type of the source presence
    pub fn extend_from_presence_range(&mut self, src: &Presence, offset: usize, len: usize) {
        if len == 0 {
            return;
        }

        if self.is_empty() {
            *self = src.clone_range(offset, len);
            return;
        }

        match src {
            Presence::Trivial(_) => {
                self.extend_with_non_nulls(len);
            }
            Presence::Nulls(_) => {
                self.extend_with_nulls(len);
            }
            Presence::Bytes(presence) => {
                self.extend_with_bytes(&presence[offset..offset + len]);
            }
        }
    }

    /// Truncates this `Presence` to the specified length.
    pub fn truncate(&mut self, new_len: usize) {
        match self {
            Presence::Trivial(len) => *len = std::cmp::min(*len, new_len),
            Presence::Nulls(len) => *len = std::cmp::min(*len, new_len),
            Presence::Bytes(vec) => vec.truncate(new_len),
        }
    }
}

impl Presence {
    /// Converts this presence to the `Bytes` representation, with the specified capacity.
    ///
    /// This is an internal helper method that ensures the presence is in `Bytes` mode,
    /// preserving the current values.
    ///
    /// # Arguments
    ///
    /// * `capacity` - The minimum capacity to ensure for the resulting byte vector
    fn convert_to_bytes(&mut self, capacity: usize) {
        match std::mem::take(self) {
            Self::Trivial(len) => {
                let mut presence = AlignedByteVec::with_capacity(std::cmp::max(len, capacity));
                presence.resize_typed::<i8>(len, 1);
                *self = Self::Bytes(presence);
            }
            Self::Nulls(len) => {
                let mut presence = AlignedByteVec::with_capacity(std::cmp::max(len, capacity));
                presence.resize_typed::<i8>(len, 0);
                *self = Self::Bytes(presence);
            }
            Self::Bytes(presence) => {
                *self = Self::Bytes(presence);
            }
        }
    }
}

impl PartialEq for Presence {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Trivial(left), Self::Trivial(right)) => left == right,
            (Self::Nulls(left), Self::Nulls(right)) => left == right,
            (Self::Bytes(left), Self::Bytes(right)) => left.as_slice() == right.as_slice(),
            _ => false,
        }
    }
}

impl Default for Presence {
    fn default() -> Self {
        Presence::Trivial(0)
    }
}

/// A builder for the [`Presence`] container.
pub struct PresenceBuilder {
    presence: Presence,
}

impl PresenceBuilder {
    pub fn new() -> Self {
        Self {
            presence: Presence::Trivial(0),
        }
    }

    pub fn add_null(&mut self) {
        self.presence.push_null();
    }

    pub fn add_n_nulls(&mut self, n: usize) {
        self.presence.extend_with_nulls(n);
    }

    pub fn add_non_null(&mut self) {
        self.presence.push_non_null();
    }

    pub fn add_n_non_nulls(&mut self, n: usize) {
        self.presence.extend_with_non_nulls(n);
    }

    /// Consumes this builder and returns the presence.
    pub fn build(self) -> Presence {
        self.presence
    }
}

impl Default for PresenceBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_presence_builder_all_present() {
        let mut builder = PresenceBuilder::new();
        builder.add_n_non_nulls(5);
        builder.add_non_null();
        let presence = builder.build();

        assert_eq!(presence.len(), 6);
        assert!(presence.is_trivial_non_null());
        assert!(!presence.is_trivial_all_null());
        assert!(!presence.is_null(0));
    }

    #[test]
    fn test_presence_builder_all_null() {
        let mut builder = PresenceBuilder::new();
        builder.add_n_nulls(5);
        builder.add_null();
        let presence = builder.build();

        assert_eq!(presence.len(), 6);
        assert!(!presence.is_trivial_non_null());
        assert_eq!(presence.count_non_nulls(), 0);
        assert_eq!(presence.count_nulls(), 6);
        assert!(presence.is_trivial_all_null());
        assert!(presence.is_null(0));
    }

    #[test]
    fn test_presence_builder_mixed() {
        let mut builder = PresenceBuilder::new();
        builder.add_non_null();
        builder.add_null();
        builder.add_n_nulls(3);
        builder.add_n_non_nulls(2);
        let presence = builder.build();

        assert_eq!(presence.len(), 7);
        assert!(!presence.is_trivial_non_null());
        assert!(!presence.is_trivial_all_null());
        assert!(!presence.is_null(0));
        assert!(presence.is_null(1));
        assert!(presence.is_null(2));
        assert!(presence.is_null(3));
        assert!(presence.is_null(4));
        assert!(!presence.is_null(5));
        assert!(!presence.is_null(6));
    }

    #[test]
    fn test_presence_empty() {
        let presence = Presence::default();
        assert_eq!(presence.len(), 0);
        assert!(presence.is_empty());
        assert_eq!(presence.count_nulls(), 0);
        assert_eq!(presence.count_non_nulls(), 0);
    }

    #[test]
    fn test_presence_clone_range() {
        // Clone range from trivial
        let presence = Presence::Trivial(10);
        let cloned = presence.clone_range(2, 5);
        assert_eq!(cloned, Presence::Trivial(5));
        assert_eq!(cloned.len(), 5);
        assert!(cloned.is_trivial_non_null());

        // Clone range from nulls
        let presence = Presence::Nulls(10);
        let cloned = presence.clone_range(2, 5);
        assert_eq!(cloned, Presence::Nulls(5));
        assert_eq!(cloned.len(), 5);
        assert!(cloned.is_trivial_all_null());

        // Clone range from bytes
        let mut builder = PresenceBuilder::new();
        builder.add_non_null();
        builder.add_null();
        builder.add_non_null();
        builder.add_n_nulls(3);
        builder.add_n_non_nulls(2);
        let presence = builder.build();

        let cloned = presence.clone_range(1, 4);
        assert_eq!(cloned.len(), 4);
        assert!(cloned.is_null(0)); // index 1 in original
        assert!(!cloned.is_null(1)); // index 2 in original
        assert!(cloned.is_null(2)); // index 3 in original
        assert!(cloned.is_null(3)); // index 4 in original
        assert_eq!(cloned.count_non_nulls(), 1);
        assert_eq!(cloned.count_nulls(), 3);
    }

    #[test]
    #[should_panic(expected = "offset + len <= self.len()")]
    fn test_presence_clone_range_out_of_bounds() {
        let presence = Presence::Trivial(5);
        presence.clone_range(2, 4); // 2 + 4 > 5
    }

    #[test]
    fn test_presence_extend_with_nulls() {
        // Extend empty presence
        let mut presence = Presence::default();
        presence.extend_with_nulls(3);
        assert_eq!(presence, Presence::Nulls(3));
        assert_eq!(presence.len(), 3);
        assert!(presence.is_trivial_all_null());

        // Extend trivial presence
        let mut presence = Presence::Trivial(2);
        presence.extend_with_nulls(3);
        assert_eq!(presence.len(), 5);
        assert!(!presence.is_trivial_non_null());
        assert!(!presence.is_trivial_all_null());
        assert!(!presence.is_null(0));
        assert!(!presence.is_null(1));
        assert!(presence.is_null(2));
        assert!(presence.is_null(4));

        // Extend nulls presence
        let mut presence = Presence::Nulls(2);
        presence.extend_with_nulls(3);
        assert_eq!(presence, Presence::Nulls(5));
        assert_eq!(presence.len(), 5);
        assert!(presence.is_trivial_all_null());

        // Extend bytes presence
        let mut presence = Presence::Bytes(AlignedByteVec::copy_from_slice(&[1, 0, 1]));
        presence.extend_with_nulls(2);
        assert_eq!(presence.len(), 5);
        assert!(!presence.is_null(0));
        assert!(presence.is_null(1));
        assert!(!presence.is_null(2));
        assert!(presence.is_null(3));
        assert!(presence.is_null(4));

        // Extend with zero nulls (no-op)
        let mut presence = Presence::Trivial(2);
        let original = presence.clone();
        presence.extend_with_nulls(0);
        assert_eq!(presence, original);
    }

    #[test]
    fn test_presence_extend_with_non_nulls() {
        // Extend empty presence
        let mut presence = Presence::default();
        presence.extend_with_non_nulls(3);
        assert_eq!(presence, Presence::Trivial(3));
        assert_eq!(presence.len(), 3);
        assert!(presence.is_trivial_non_null());

        // Extend trivial presence
        let mut presence = Presence::Trivial(2);
        presence.extend_with_non_nulls(3);
        assert_eq!(presence, Presence::Trivial(5));
        assert_eq!(presence.len(), 5);
        assert!(presence.is_trivial_non_null());

        // Extend nulls presence
        let mut presence = Presence::Nulls(2);
        presence.extend_with_non_nulls(3);
        assert_eq!(presence.len(), 5);
        assert!(!presence.is_trivial_non_null());
        assert!(!presence.is_trivial_all_null());
        assert!(presence.is_null(0));
        assert!(presence.is_null(1));
        assert!(!presence.is_null(2));
        assert!(!presence.is_null(4));

        // Extend bytes presence
        let mut presence = Presence::Bytes(AlignedByteVec::copy_from_slice(&[1, 0, 1]));
        presence.extend_with_non_nulls(2);
        assert_eq!(presence.len(), 5);
        assert!(!presence.is_null(0));
        assert!(presence.is_null(1));
        assert!(!presence.is_null(2));
        assert!(!presence.is_null(3));
        assert!(!presence.is_null(4));

        // Extend with zero non-nulls (no-op)
        let mut presence = Presence::Trivial(2);
        let original = presence.clone();
        presence.extend_with_non_nulls(0);
        assert_eq!(presence, original);
    }

    #[test]
    fn test_presence_extend_with_bytes() {
        // Extend empty presence with all ones
        let mut presence = Presence::default();
        presence.extend_with_bytes(&[1, 1, 1]);
        assert_eq!(presence, Presence::Trivial(3));

        // Extend empty presence with all zeros
        let mut presence = Presence::default();
        presence.extend_with_bytes(&[0, 0, 0]);
        assert_eq!(presence, Presence::Nulls(3));

        // Extend empty presence with mixed bytes
        let mut presence = Presence::default();
        presence.extend_with_bytes(&[1, 0, 1]);
        assert_eq!(presence.len(), 3);
        assert!(!presence.is_null(0));
        assert!(presence.is_null(1));
        assert!(!presence.is_null(2));

        // Extend trivial with all non-nulls
        let mut presence = Presence::Trivial(2);
        presence.extend_with_bytes(&[1, 1, 1]);
        assert_eq!(presence, Presence::Trivial(5));

        // Extend trivial with mixed bytes
        let mut presence = Presence::Trivial(2);
        presence.extend_with_bytes(&[1, 0, 1]);
        assert_eq!(presence.len(), 5);
        assert!(!presence.is_null(0));
        assert!(!presence.is_null(1));
        assert!(!presence.is_null(2));
        assert!(presence.is_null(3));
        assert!(!presence.is_null(4));

        // Extend nulls with all nulls
        let mut presence = Presence::Nulls(2);
        presence.extend_with_bytes(&[0, 0, 0]);
        assert_eq!(presence, Presence::Nulls(5));

        // Extend nulls with mixed bytes
        let mut presence = Presence::Nulls(2);
        presence.extend_with_bytes(&[1, 0, 1]);
        assert_eq!(presence.len(), 5);
        assert!(presence.is_null(0));
        assert!(presence.is_null(1));
        assert!(!presence.is_null(2));
        assert!(presence.is_null(3));
        assert!(!presence.is_null(4));

        // Extend bytes with mixed bytes
        let mut presence = Presence::Bytes(AlignedByteVec::copy_from_slice(&[1, 0, 1]));
        presence.extend_with_bytes(&[0, 1, 0]);
        assert_eq!(presence.len(), 6);
        assert!(!presence.is_null(0));
        assert!(presence.is_null(1));
        assert!(!presence.is_null(2));
        assert!(presence.is_null(3));
        assert!(!presence.is_null(4));
        assert!(presence.is_null(5));

        // Extend with empty bytes (no-op)
        let mut presence = Presence::Trivial(2);
        let original = presence.clone();
        presence.extend_with_bytes(&[]);
        assert_eq!(presence, original);
    }

    #[test]
    fn test_presence_extend_from_range() {
        // Extend empty from trivial range
        let mut presence = Presence::default();
        let source = Presence::Trivial(5);
        presence.extend_from_presence_range(&source, 1, 3);
        assert_eq!(presence, Presence::Trivial(3));

        // Extend empty from nulls range
        let mut presence = Presence::default();
        let source = Presence::Nulls(5);
        presence.extend_from_presence_range(&source, 1, 3);
        assert_eq!(presence, Presence::Nulls(3));

        // Extend empty from bytes range
        let mut presence = Presence::default();
        let source = Presence::Bytes(AlignedByteVec::copy_from_slice(&[1, 0, 1, 0, 1]));
        presence.extend_from_presence_range(&source, 1, 3);
        assert_eq!(presence.len(), 3);
        assert!(presence.is_null(0));
        assert!(!presence.is_null(1));
        assert!(presence.is_null(2));

        // Extend trivial from trivial
        let mut presence = Presence::Trivial(2);
        let source = Presence::Trivial(5);
        presence.extend_from_presence_range(&source, 1, 3);
        assert_eq!(presence, Presence::Trivial(5));

        // Extend trivial from nulls
        let mut presence = Presence::Trivial(2);
        let source = Presence::Nulls(5);
        presence.extend_from_presence_range(&source, 1, 3);
        assert_eq!(presence.len(), 5);
        assert!(!presence.is_null(0));
        assert!(!presence.is_null(1));
        assert!(presence.is_null(2));
        assert!(presence.is_null(3));
        assert!(presence.is_null(4));

        // Extend nulls from trivial
        let mut presence = Presence::Nulls(2);
        let source = Presence::Trivial(5);
        presence.extend_from_presence_range(&source, 1, 3);
        assert_eq!(presence.len(), 5);
        assert!(presence.is_null(0));
        assert!(presence.is_null(1));
        assert!(!presence.is_null(2));
        assert!(!presence.is_null(3));
        assert!(!presence.is_null(4));

        // Extend with zero length (no-op)
        let mut presence = Presence::Trivial(2);
        let original = presence.clone();
        let source = Presence::Trivial(5);
        presence.extend_from_presence_range(&source, 1, 0);
        assert_eq!(presence, original);
    }

    #[test]
    fn test_presence_equality() {
        // Same variants with same lengths
        assert_eq!(Presence::Trivial(5), Presence::Trivial(5));
        assert_eq!(Presence::Nulls(5), Presence::Nulls(5));

        // Same variants with different lengths
        assert_ne!(Presence::Trivial(5), Presence::Trivial(3));
        assert_ne!(Presence::Nulls(5), Presence::Nulls(3));

        // Different variants
        assert_ne!(Presence::Trivial(5), Presence::Nulls(5));
        assert_ne!(
            Presence::Trivial(5),
            Presence::Bytes(AlignedByteVec::zeroed(5))
        );
        assert_ne!(
            Presence::Nulls(5),
            Presence::Bytes(AlignedByteVec::zeroed(5))
        );

        // Bytes variant with same content
        let bytes1 = Presence::Bytes(AlignedByteVec::copy_from_slice(&[1, 0, 1]));
        let bytes2 = Presence::Bytes(AlignedByteVec::copy_from_slice(&[1, 0, 1]));
        assert_eq!(bytes1, bytes2);

        // Bytes variant with different content
        let bytes1 = Presence::Bytes(AlignedByteVec::copy_from_slice(&[1, 0, 1]));
        let bytes2 = Presence::Bytes(AlignedByteVec::copy_from_slice(&[1, 1, 0]));
        assert_ne!(bytes1, bytes2);
    }

    #[test]
    fn test_convert_to_bytes() {
        // Convert trivial to bytes
        let mut presence = Presence::Trivial(3);
        presence.convert_to_bytes(3);
        match presence {
            Presence::Bytes(bytes) => {
                assert_eq!(bytes.as_slice(), &[1, 1, 1]);
                assert!(bytes.capacity() >= 3);
            }
            _ => panic!("Expected Bytes variant"),
        }

        // Convert nulls to bytes
        let mut presence = Presence::Nulls(3);
        presence.convert_to_bytes(3);
        match presence {
            Presence::Bytes(bytes) => {
                assert_eq!(bytes.as_slice(), &[0, 0, 0]);
                assert!(bytes.capacity() >= 3);
            }
            _ => panic!("Expected Bytes variant"),
        }

        // Convert with larger capacity
        let mut presence = Presence::Trivial(3);
        presence.convert_to_bytes(10);
        match presence {
            Presence::Bytes(bytes) => {
                assert_eq!(bytes.as_slice(), &[1, 1, 1]);
                assert!(bytes.capacity() >= 10);
            }
            _ => panic!("Expected Bytes variant"),
        }
    }

    #[test]
    fn test_builder_methods() {
        // Test default builder
        let builder = PresenceBuilder::default();
        let presence = builder.build();
        assert_eq!(presence, Presence::Trivial(0));

        // Test adding a single null to empty builder
        let mut builder = PresenceBuilder::new();
        builder.add_null();
        let presence = builder.build();
        assert_eq!(presence, Presence::Nulls(1));

        // Test adding a single non-null to empty builder
        let mut builder = PresenceBuilder::new();
        builder.add_non_null();
        let presence = builder.build();
        assert_eq!(presence, Presence::Trivial(1));

        // Test complex builder pattern
        let mut builder = PresenceBuilder::new();
        builder.add_non_null();
        builder.add_n_non_nulls(2);
        builder.add_null();
        builder.add_n_nulls(2);
        builder.add_non_null();
        let presence = builder.build();

        assert_eq!(presence.len(), 7);
        assert!(!presence.is_null(0));
        assert!(!presence.is_null(1));
        assert!(!presence.is_null(2));
        assert!(presence.is_null(3));
        assert!(presence.is_null(4));
        assert!(presence.is_null(5));
        assert!(!presence.is_null(6));
    }
}

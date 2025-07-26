//! Additional methods for `BytesList`.

use crate::defs::common::BytesList;

impl BytesList {
    /// Returns the number of byte buffers stored in this `BytesList`.
    ///
    /// The length is calculated as `offsets.len() - 1` because the offsets array
    /// contains one more element than the number of buffers (the first offset is
    /// always 0, and each subsequent offset marks the end of a buffer).
    #[inline]
    pub fn len(&self) -> usize {
        self.offsets.len() - 1
    }

    /// Returns `true` if this `BytesList` contains no byte buffers.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the length in bytes of the buffer at the specified index.
    ///
    /// # Arguments
    /// * `index` - The zero-based index of the buffer to query
    ///
    /// # Panics
    /// Panics if `index` is out of bounds (>= `len()`).
    #[inline]
    pub fn length_at(&self, index: usize) -> usize {
        (self.offsets[index + 1] - self.offsets[index]) as usize
    }

    /// Returns a reference to the byte buffer at the specified index.
    ///
    /// The returned slice contains the bytes for the buffer at the given index,
    /// extracted from the concatenated `data` field using the offset information.
    ///
    /// # Arguments  
    /// * `index` - The zero-based index of the buffer to retrieve
    ///
    /// # Returns
    /// A byte slice containing the buffer data at the specified index.
    ///
    /// # Panics
    /// Panics if `index` is out of bounds (>= `len()`).
    #[inline]
    pub fn value_at(&self, index: usize) -> &[u8] {
        let start = self.offsets[index] as usize;
        let end = self.offsets[index + 1] as usize;
        &self.data[start..end]
    }
}

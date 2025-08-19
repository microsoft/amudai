//! A fixed-size array of bits.

use std::ops::{BitAnd, BitAndAssign, BitOr, BitOrAssign, BitXor, BitXorAssign, Not, Range};

use crate::bit_store::BitStore;

/// A fixed-size array of bits with `[u64]` storage and bitwise operations.
///
/// `BitArray` provides a space-efficient way to store and manipulate a collection of bits,
/// using `u64` words as the underlying storage. Each bit can be individually accessed,
/// modified, and the entire array supports standard bitwise operations (AND, OR, XOR, NOT).
///
/// # Storage Format
///
/// The bits are stored in little-endian order within an array of `u64` words:
/// - Bit 0 corresponds to the least significant bit (LSB) of the first word
/// - Bit 63 corresponds to the most significant bit (MSB) of the first word
/// - Bit 64 corresponds to the LSB of the second word, and so on
///
/// The number of words allocated is `len.div_ceil(64)`. Any bits beyond the array's
/// specified length in the final word are guaranteed to be 0.
///
/// # Performance
///
/// - Individual bit access: O(1)
/// - Bitwise operations: O(n/64) where n is the number of bits
/// - Count operations: O(n/64) using popcount instructions
/// - Rank/Select operations: O(n/64) in worst case
#[derive(Clone)]
pub struct BitArrayBase<S> {
    len: usize,
    bits: S,
}

impl<S: AsMut<[u64]>> BitArrayBase<S> {
    /// Constructs a bit array by wrapping existing mutable `u64` storage.
    ///
    /// The storage is interpreted as LSB-ordered words (bit 0 is the LSB of word 0).
    /// This function validates the storage size for the requested logical length and
    /// tail-masks the final word so that all bits beyond `len` are zero.
    ///
    /// Invariants after construction:
    /// - `bits.as_mut().len() == len.div_ceil(64)`
    /// - All bits beyond `len` in the last word are zero (via [`Self::mask_tail`]).
    ///
    /// Arguments
    /// - `bits`: Mutable storage of `u64` words. Its length in words must be exactly
    ///   `len.div_ceil(64)`. The storage is modified in-place to enforce tail masking.
    /// - `len`: Logical number of bits.
    ///
    /// Panics
    /// - If `bits.as_mut().len() != len.div_ceil(64)` (insufficient or excess storage).
    ///
    /// Complexity
    /// - O(1). Performs a length check and, at most, masks the final word.
    ///
    /// See also
    /// - [`BitArrayBase::wrap_lsb_words`], which can wrap read-only or owned storage and
    ///   asserts (rather than applies) tail masking.
    pub fn new(mut bits: S, len: usize) -> BitArrayBase<S> {
        let count = len.div_ceil(64);
        let words = bits.as_mut();
        assert_eq!(words.len(), count);
        Self::mask_tail(words, len);
        BitArrayBase { len, bits }
    }

    /// Creates a new bit array with all bits set to 0.
    ///
    /// # Arguments
    ///
    /// * `len` - The number of bits in the array
    pub fn empty(len: usize) -> BitArrayBase<S>
    where
        S: BitStore + AsMut<[u64]>,
    {
        let count = len.div_ceil(64);
        let bits = S::new_zeroed(count);
        BitArrayBase { len, bits }
    }

    /// Creates a new bit array with all bits set to 1.
    ///
    /// # Arguments
    ///
    /// * `len` - The number of bits in the array
    pub fn full(len: usize) -> BitArrayBase<S>
    where
        S: BitStore + AsMut<[u64]>,
    {
        Self::new_with_pattern(len, u64::MAX)
    }

    /// Creates a new bit array by repeating a 64-bit pattern.
    ///
    /// The pattern is repeated across all `u64`` words in the underlying storage.
    /// Any bits beyond the specified length are automatically masked to 0.
    ///
    /// # Arguments
    ///
    /// * `len` - The number of bits in the array
    /// * `pattern` - The 64-bit pattern to repeat across the array
    pub fn new_with_pattern(len: usize, pattern: u64) -> BitArrayBase<S>
    where
        S: BitStore + AsMut<[u64]>,
    {
        let count = len.div_ceil(64);
        let mut bits = S::new_with_pattern(count, pattern);
        Self::mask_tail(bits.as_mut(), len);
        BitArrayBase { len, bits }
    }

    /// Creates a new bit array with bits set at the specified positions.
    ///
    /// # Arguments
    ///
    /// * `positions` - An iterator over bit indices to set to 1
    /// * `len` - The total number of bits in the array
    ///
    /// # Panics
    ///
    /// Panics if any position in `positions` is >= `len` (positions must be within bounds)
    ///
    /// # Performance
    ///
    /// Time complexity: O(p) where p is the number of positions provided.
    /// Each position requires O(1) time to set.
    pub fn from_positions(positions: impl Iterator<Item = usize>, len: usize) -> BitArrayBase<S>
    where
        S: BitStore,
    {
        let mut bit_array = BitArrayBase::empty(len);
        for position in positions {
            bit_array.set(position);
        }
        bit_array
    }

    /// Creates a new bit array with bits set in the specified ranges.
    ///
    /// # Arguments
    ///
    /// * `ranges` - An iterator over ranges of bit indices to set to 1
    /// * `len` - The total number of bits in the array
    ///
    /// # Panics
    ///
    /// Panics if any range end is > `len` (ranges must be within bounds)
    ///
    /// # Performance
    ///
    /// Time complexity: O(r × w) where r is the number of ranges and w is the average
    /// number of u64 words spanned by each range. This is more efficient than setting
    /// individual bits when working with contiguous ranges.
    pub fn from_ranges(ranges: impl Iterator<Item = Range<usize>>, len: usize) -> BitArrayBase<S>
    where
        S: BitStore,
    {
        let mut bit_array = BitArrayBase::empty(len);
        for range in ranges {
            bit_array.set_range(range);
        }
        bit_array
    }

    /// Creates a new bit array from an array of u64 words in LSB (Least Significant Bit) order.
    ///
    /// The words are interpreted as the raw bit storage, where bit 0 corresponds to the LSB
    /// of the first word, bit 63 to the MSB of the first word, bit 64 to the LSB of the
    /// second word, and so on. Only the first `len.div_ceil(64)` words are used, and any
    /// bits beyond the specified length are automatically masked to 0.
    ///
    /// # Arguments
    ///
    /// * `words` - A slice of u64 words containing the bit data
    /// * `len` - The number of bits in the resulting array
    ///
    /// # Returns
    ///
    /// A new `BitArray` initialized with the provided word data
    ///
    /// # Panics
    ///
    /// Panics if `len > words.len() * 64` (not enough words to represent `len` bits)
    pub fn from_lsb_words(words: &[u64], len: usize) -> BitArrayBase<S>
    where
        S: BitStore,
    {
        assert!(len <= words.len() * 64);
        let count = len.div_ceil(64);
        let mut bits = S::new_zeroed(count);
        bits.as_mut().copy_from_slice(&words[..count]);
        Self::mask_tail(bits.as_mut(), len);
        BitArrayBase { len, bits }
    }

    /// Creates a new bit array from an array of bytes in LSB (Least Significant Bit) order.
    ///
    /// The bytes are interpreted as the raw bit storage, where bit 0 corresponds to the LSB
    /// of the first byte, bit 7 to the MSB of the first byte, bit 8 to the LSB of the
    /// second byte, and so on.
    ///
    /// # Arguments
    ///
    /// * `bytes` - A slice of `u8` containing the bit data
    /// * `len` - The number of bits in the resulting array
    ///
    /// # Returns
    ///
    /// A new `BitArray` initialized with the provided data
    pub fn from_lsb_bytes(bytes: &[u8], len: usize) -> BitArrayBase<S>
    where
        S: BitStore,
    {
        let mut bit_array = BitArrayBase::empty(len);
        let byte_len = len.div_ceil(8).min(bytes.len());
        if byte_len != 0 {
            bytemuck::cast_slice_mut::<_, u8>(bit_array.storage_mut())[..byte_len]
                .copy_from_slice(&bytes[..byte_len]);
        }
        Self::mask_tail(bit_array.storage_mut(), len);
        bit_array
    }

    /// Sets the bit at the given index to 1.
    #[inline]
    pub fn set(&mut self, index: usize) {
        debug_assert!(
            index < self.len,
            "Index {index} out of bounds (len: {})",
            self.len
        );
        let (word_index, bit_position) = Self::bit_position(index);
        self.storage_mut()[word_index] |= 1u64 << bit_position;
    }

    /// Sets the bit at the given index to 1 and returns the previous state.
    ///
    /// Returns `true` if the bit was already set (1), and `false` if it was previously
    /// unset (0). This is equivalent to:
    /// ```ignore
    /// let was_set = self.contains(index);
    /// self.set(index);
    /// was_set
    /// ```
    /// but avoids an extra read by combining the operations.
    #[inline]
    pub fn set_replace(&mut self, index: usize) -> bool {
        debug_assert!(
            index < self.len,
            "Index {index} out of bounds (len: {})",
            self.len
        );
        let (word_index, bit_position) = Self::bit_position(index);
        let mask = 1u64 << bit_position;
        let cell = &mut self.storage_mut()[word_index];
        let prev = *cell & mask;
        *cell |= 1u64 << bit_position;
        prev != 0
    }

    /// Resets the bit at the given index to 0.
    #[inline]
    pub fn reset(&mut self, index: usize) {
        debug_assert!(
            index < self.len,
            "Index {index} out of bounds (len: {})",
            self.len
        );
        let (word_index, bit_position) = Self::bit_position(index);
        self.storage_mut()[word_index] &= !(1u64 << bit_position);
    }

    /// Sets the bit at the given index to the specified value.
    #[inline]
    pub fn set_value(&mut self, index: usize, value: bool) {
        debug_assert!(
            index < self.len,
            "Index {index} out of bounds (len: {})",
            self.len
        );
        let (word_index, bit_position) = Self::bit_position(index);
        let mask = 1u64 << bit_position;
        let word = &mut self.storage_mut()[word_index];
        *word = (*word & !mask) | (mask & (-(value as i64) as u64));
    }

    /// Clears all bits (sets all to 0).
    pub fn clear(&mut self) {
        self.storage_mut().fill(0);
    }

    /// Sets all bits to 1.
    pub fn set_all(&mut self) {
        self.storage_mut().fill(u64::MAX);
        // Ensure the tail bits beyond len are properly masked
        Self::mask_tail(self.bits.as_mut(), self.len);
    }

    /// Sets all bits in the specified range to 1.
    ///
    /// # Arguments
    ///
    /// * `range` - A range of bit indices to set. The range is interpreted as `[start, end)`
    ///   where `start` is inclusive and `end` is exclusive.
    ///
    /// # Panics
    ///
    /// Panics if `range.end > self.len()` (the range extends beyond the bit array's length).
    pub fn set_range(&mut self, range: Range<usize>) {
        self.process_range(range, |word, mask| *word |= mask);
    }

    /// Resets all bits in the specified range to 0.
    ///
    /// # Arguments
    ///
    /// * `range` - A range of bit indices to set. The range is interpreted as `[start, end)`
    ///   where `start` is inclusive and `end` is exclusive.
    ///
    /// # Panics
    ///
    /// Panics if `range.end > self.len()` (the range extends beyond the bit array's length).
    pub fn reset_range(&mut self, range: Range<usize>) {
        self.process_range(range, |word, mask| *word &= !mask);
    }

    /// Applies a mask function to all bits within the specified range.
    ///
    /// This is a low-level helper method that efficiently processes ranges of bits by working
    /// directly with the underlying u64 storage words. It handles ranges that may span multiple
    /// words by breaking them into:
    /// - Partial bits in the first word (from start_bit to bit 63)
    /// - Complete middle words (all 64 bits)
    /// - Partial bits in the last word (from bit 0 to end_bit-1)
    ///
    /// The mask function is called for each affected word with a mask that has set bits
    /// corresponding to all positions within that word that fall within the specified range.
    ///
    /// # Arguments
    ///
    /// * `range` - A range of bit indices to process. The range is interpreted as `[start, end)`
    ///   where `start` is inclusive and `end` is exclusive.
    /// * `mask_fn` - A function that takes a mutable reference to a u64 word and a mask.
    ///   The mask has set bits for all positions within the word that fall within the range.
    ///   The function should apply the mask to modify the word as needed.
    ///
    /// # Panics
    ///
    /// Panics if `range.end > self.len()` (the range extends beyond the bit array's length).
    /// Empty ranges (where `start >= end`) are handled gracefully and result in no operation.
    pub fn process_range(&mut self, range: Range<usize>, mask_fn: impl Fn(&mut u64, u64)) {
        let start = range.start;
        let end = range.end;
        if start >= end {
            return;
        }
        assert!(
            end <= self.len,
            "Range end {} out of bounds (len: {})",
            end,
            self.len
        );

        let (start_word, start_bit) = Self::bit_position(start);
        let (end_word, end_bit) = Self::bit_position(end);
        let bits = self.storage_mut();

        if start_word == end_word {
            // Range is within a single word
            // Since end_bit can't be 0 when start_word == end_word, we can safely
            // create the mask
            let mask = ((1u64 << end_bit) - 1) & !((1u64 << start_bit) - 1);
            mask_fn(&mut bits[start_word], mask);
        } else {
            // Range spans multiple words

            // Process partial bits in the first word (from start_bit to bit 63)
            let first_mask = !((1u64 << start_bit) - 1);
            mask_fn(&mut bits[start_word], first_mask);

            // Process all bits in complete middle words
            for item in bits.iter_mut().take(end_word).skip(start_word + 1) {
                mask_fn(item, u64::MAX);
            }

            // Process partial bits in the last word (from bit 0 to end_bit-1)
            // Only if end_bit > 0 (if end_bit == 0, we don't touch the end_word)
            if end_bit > 0 {
                let last_mask = (1u64 << end_bit) - 1;
                mask_fn(&mut bits[end_word], last_mask);
            }
        }
    }

    /// Flips all bits in place (NOT operation).
    pub fn negate(&mut self) {
        for word in self.storage_mut().iter_mut() {
            *word = !*word;
        }
        let len = self.len;
        // Ensure the tail bits beyond len are properly masked
        Self::mask_tail(self.storage_mut(), len);
    }

    /// Returns a mutable reference to the underlying u64 storage.
    ///
    /// The storage is organized as an array of u64 words where each word contains
    /// 64 bits in LSB order. The number of words is `len.div_ceil(64)`.
    ///
    /// # Safety and Invariants
    ///
    /// When modifying the storage directly, you must ensure that bits beyond the
    /// array's length remain 0.
    #[inline]
    pub fn storage_mut(&mut self) -> &mut [u64] {
        self.bits.as_mut()
    }
}

impl<S: AsRef<[u64]>> BitArrayBase<S> {
    /// Constructs a `BitArrayBase` by wrapping existing `u64` storage without copying.
    ///
    /// This is a zero-copy constructor: it does not allocate and does not modify `bits`.
    /// The provided storage is treated as LSB-ordered words (bit 0 is the LSB of word 0),
    /// and the logical bit-length is either the supplied `len` or the full capacity
    /// (`bits.as_ref().len() * 64`) when `len` is `None`.
    ///
    /// This function validates and enforces the invariants of `BitArrayBase`:
    /// - The storage length in words must match the logical length:  
    ///   `bits.as_ref().len() == len.div_ceil(64)`, which is equivalent to
    ///   `raw_len - len < 64` where `raw_len = bits.as_ref().len() * 64`.
    ///   In other words, there can be at most a single partial word at the end,
    ///   and no completely unused trailing words.
    /// - All tail bits beyond `len` in the last word must be zero (tail-masked).
    ///
    /// No masking is performed here; the function asserts that the tail is already
    /// masked correctly. If you own/mutate the storage and need to enforce tail
    /// masking first, call `mask_tail` on the words before wrapping.
    ///
    /// # Arguments
    ///
    /// * `bits` - The existing storage to wrap (LSB-ordered `u64` words).
    /// * `len`  - Optional logical bit-length. When `None`, uses the full capacity
    ///   (`bits.as_ref().len() * 64`). When `Some(len)`, `len` must be
    ///   within the last word of `bits`.
    ///
    /// # Panics
    ///
    /// Panics if:
    /// - `len > bits.as_ref().len() * 64` (insufficient storage),
    /// - `bits.as_ref().len() * 64 - len >= 64` (storage has fully unused trailing words),
    /// - or any tail bit beyond `len` in the last word is non-zero (tail not masked).
    ///
    /// # Complexity
    ///
    /// O(1). Performs only bounds/tail checks and stores the references.
    ///
    /// # Mutability
    ///
    /// The returned `BitArrayBase<S>` is as mutable as `S` allows:
    /// - If `S: AsRef<[u64]>` only (e.g., wrapping `&[u64]`), the view is read-only.
    /// - If `S: AsMut<[u64]>` (e.g., wrapping `Box<[u64]>`), all mutating APIs are available.
    ///
    /// # Use cases
    ///
    /// - Create a read-only view over borrowed bit storage (`&[u64]`) without copying.
    /// - Wrap owned storage (`Box<[u64]>`, mmap-backed buffer) to avoid reallocation.
    /// - Zero-copy integration with memory-mapped or preformatted buffers.
    ///
    /// # Examples
    ///
    /// Read-only view over borrowed storage:
    /// ```
    /// # use amudai_position_set::bit_array::BitArrayBase;
    /// let words: &[u64] = &[0b1011, 0b1];
    /// let bits = BitArrayBase::wrap_lsb_words(words, Some(65)); // 2 words => raw_len = 128
    /// assert!(bits.contains(0));
    /// assert!(bits.contains(1));
    /// assert!(!bits.contains(2));
    /// assert!(bits.contains(3));
    /// assert!(bits.contains(64)); // LSB of second word
    /// ```
    ///
    /// Owned, mutable storage:
    /// ```
    /// # use amudai_position_set::bit_array::BitArrayBase;
    /// let mut storage = vec![0u64; 2].into_boxed_slice(); // 128-bit capacity
    /// // Caller ensures tail is masked for the chosen length; here, length is full capacity.
    /// let mut bits = BitArrayBase::wrap_lsb_words(storage, None);
    /// bits.set(5);
    /// assert!(bits.contains(5));
    /// ```
    pub fn wrap_lsb_words(bits: S, len: Option<usize>) -> BitArrayBase<S> {
        let len = len.unwrap_or(bits.as_ref().len() * 64);
        assert!(Self::is_tail_masked(bits.as_ref(), len));
        BitArrayBase { len, bits }
    }

    /// Check if the bit at the given index is set.
    #[inline]
    pub fn contains(&self, index: usize) -> bool {
        debug_assert!(
            index < self.len,
            "Index {index} out of bounds (len: {})",
            self.len
        );
        let (word_index, bit_position) = Self::bit_position(index);
        (self.storage()[word_index] & (1u64 << bit_position)) != 0
    }

    /// Counts the number of set bits (1s) in the bit array.
    pub fn count_ones(&self) -> usize {
        self.storage()
            .iter()
            .map(|word| word.count_ones() as usize)
            .sum()
    }

    /// Counts the number of unset bits (0s) in the bit array.
    pub fn count_zeros(&self) -> usize {
        self.len - self.count_ones()
    }

    /// Counts the number of runs (maximal contiguous sequences) of 1-bits.
    ///
    /// A run starts at position `i` when bit `i` is 1 and bit `i-1` is 0 (bit `-1`
    /// is treated as 0).
    ///
    /// Returns 0 for an empty bit array.
    ///
    /// Performance: O(n/64).
    pub fn count_runs(&self) -> usize {
        if self.is_empty() {
            return 0;
        }
        // A run of 1s starts at position `i` if bit `i` is 1 and bit `i - 1` is 0
        // (treat bit -1 as 0).
        // For each word, compute "run start mask" as `word & ~((word << 1) | carry)`,
        // where carry is the previous word's MSB (bit 63) brought into this word's bit 0
        // to handle cross-word runs.
        let mut runs = 0usize;
        let mut prev_word = 0u64; // Treat bit -1 as 0 for the very first bit
        for &word in self.storage().iter() {
            let shifted_prev = (word << 1) | (prev_word >> 63);
            let starts = word & !shifted_prev;
            runs += starts.count_ones() as usize;
            prev_word = word;
        }
        runs
    }

    /// Counts the number of set bits (1s) from position 0 up to and including the given
    /// position. This is also known as the rank operation.
    ///
    /// # Arguments
    /// * `index` - The position up to which to count set bits (inclusive)
    ///
    /// # Returns
    /// The number of set bits from position 0 to `index` (inclusive)
    ///
    /// # Panics
    /// Panics if `index` >= `len()`
    pub fn rank(&self, index: usize) -> usize {
        assert!(
            index < self.len,
            "Index {index} out of bounds (len: {})",
            self.len
        );
        let (word_index, bit_position) = Self::bit_position(index);
        self.storage()[..word_index]
            .iter()
            .map(|v| v.count_ones() as usize)
            .sum::<usize>()
            + (self.storage()[word_index] << (63 - bit_position)).count_ones() as usize
    }

    /// Finds the position of the n-th set bit (0-indexed). This is also known as the
    /// select operation.
    ///
    /// # Arguments
    /// * `n` - The 0-indexed position of the set bit to find (0 = first set bit,
    ///   1 = second set bit, etc.)
    ///
    /// # Returns
    /// * `Some(index)` - The 0-indexed position of the n-th set bit
    /// * `None` - If there are fewer than `n + 1` set bits in the array
    ///
    /// # Performance
    /// This operation has O(k) time complexity where k is the number of u64 words
    /// that need to be examined to find the nth set bit.
    pub fn select(&self, n: usize) -> Option<usize> {
        fn select_u64(mut value: u64, n: u64) -> usize {
            for _ in 0..n {
                value &= value - 1;
            }
            value.trailing_zeros() as usize
        }

        let mut n = n as u64;
        for (word_index, word) in self.storage().iter().copied().enumerate() {
            let len = word.count_ones() as u64;
            if n < len {
                let index = select_u64(word, n);
                return Some(64 * word_index + index);
            }
            n -= len;
        }
        None
    }

    /// Returns an iterator over the positions of set bits in the bit array.
    ///
    /// The iterator yields the 0-based indices of all bits that are set to 1,
    /// in ascending order.
    pub fn iter(&self) -> BitArrayIter<'_> {
        BitArrayIter {
            words: self.storage().iter(),
            current_word: 0,
            next_word_index: 0,
            base_index: 0,
            len: self.len,
        }
    }

    /// Returns an iterator over the positions of set bits within the specified range.
    ///
    /// * `pos_range` - A range of positions to examine. The range is interpreted as
    ///   `[start, end)` where `start` is inclusive and `end` is exclusive.
    ///
    /// # Panics
    ///
    /// Panics if `pos_range.end > self.len()` (the range extends beyond the bit array's length).
    ///
    /// # Performance
    ///
    /// This method is more efficient than using `iter().filter(|&pos| pos_range.contains(&pos))`
    /// because it starts iteration from the first word that contains bits in the range.
    pub fn iter_within(&self, pos_range: Range<usize>) -> BitArrayIter<'_> {
        assert!(pos_range.end <= self.len);
        if pos_range.start >= pos_range.end {
            return BitArrayIter::empty();
        }

        let start = std::cmp::min(pos_range.start, pos_range.end);
        let (word_index, bit_pos) = Self::bit_position(start);
        let mut words = self.storage()[word_index..].iter();
        let mut current_word = words.next().copied().unwrap_or(0);

        // Mask out bits before the start position
        current_word &= !((1u64 << bit_pos) - 1);

        BitArrayIter {
            words,
            current_word,
            next_word_index: word_index + 1,
            base_index: word_index * 64,
            // Setting len to the end of the range will handle the upper bound
            // of the pos_range in the iterator logic
            len: pos_range.end,
        }
    }

    /// Returns an iterator over contiguous ranges of 1-bits in the bit array.
    ///
    /// Each item is a half-open range [start, end) representing a maximal
    /// run of 1s. Ranges are yielded in ascending order and are non-overlapping.
    pub fn ranges_iter(&self) -> BitArrayRangesIter<'_> {
        BitArrayRangesIter {
            words: self.storage().iter(),
            current_word: 0,
            next_word_index: 0,
            base_index: 0,
            len: self.len,
        }
    }

    /// Returns an iterator over contiguous ranges of 1-bits within the specified range.
    ///
    /// The provided range is interpreted as [start, end) where start is inclusive
    /// and end is exclusive. Returned ranges will be clipped to this interval.
    ///
    /// # Panics
    ///
    /// Panics if `pos_range.end > self.len()`.
    pub fn ranges_iter_within(&self, pos_range: Range<usize>) -> BitArrayRangesIter<'_> {
        assert!(pos_range.end <= self.len);
        if pos_range.start >= pos_range.end {
            return BitArrayRangesIter::empty();
        }

        let (word_index, bit_pos) = Self::bit_position(pos_range.start);
        let end_word = pos_range.end.div_ceil(64);
        let mut words = self.storage()[word_index..end_word].iter();
        let mut current_word = words.next().copied().unwrap_or(0);
        // Mask out bits before the start position in the first word
        current_word &= !((1u64 << bit_pos) - 1);

        BitArrayRangesIter {
            words,
            current_word,
            next_word_index: word_index + 1,
            base_index: word_index * 64,
            // Use pos_range.end as the logical end bound for the iterator
            len: pos_range.end,
        }
    }

    /// Calls `f(rank, pos)` for every set bit in ascending position order.
    ///
    /// - `rank` is the 0-based ordinal among set bits (i.e., `self.iter().enumerate()`).
    /// - `pos` is the absolute bit position.
    ///
    /// # Returns
    ///
    /// The number of inspected set bits.
    pub fn for_each_set_bit(&self, mut f: impl FnMut(usize, u64)) -> usize {
        self.for_each_set_bit_while(|i, pos| {
            f(i, pos);
            true
        })
    }

    /// Like `for_each`, but stops early when the callback returns `false`.
    ///
    /// Calls `f(rank, pos)` for each set bit in ascending order and returns
    /// as soon as `f` returns `false`. Use this for early-exit scans.
    ///
    /// # Returns
    ///
    /// The number of inspected set bits.
    pub fn for_each_set_bit_while(&self, mut f: impl FnMut(usize, u64) -> bool) -> usize {
        assert!(Self::is_tail_masked(self.storage(), self.len));

        let mut i = 0usize;
        for (word_index, &w0) in self.storage().iter().enumerate() {
            let mut w = w0;
            let base = word_index * 64;
            while w != 0 {
                let tz = w.trailing_zeros() as usize;
                let pos = base + tz;
                if !f(i, pos as u64) {
                    return i + 1;
                }
                i += 1;
                // Clear the least significant set bit.
                w &= w - 1;
            }
        }
        i
    }

    /// Returns an immutable reference to the underlying u64 storage.
    ///
    /// The storage is organized as an array of u64 words where each word contains
    /// 64 bits in LSB order. The number of words is `len.div_ceil(64)`. Bits beyond
    /// the array's length in the final word are guaranteed to be 0.
    #[inline]
    pub fn storage(&self) -> &[u64] {
        self.bits.as_ref()
    }
}

impl<S> BitArrayBase<S>
where
    S: BitStore + AsRef<[u64]> + AsMut<[u64]>,
{
    pub fn into_truncated(self, len: usize) -> BitArrayBase<S> {
        assert!(len <= self.len());
        let word_count = len.div_ceil(64);
        if word_count < self.storage().len() {
            BitArrayBase::from_lsb_words(self.storage(), len)
        } else {
            assert_eq!(word_count, self.storage().len());
            let mut bits = self.bits;
            Self::mask_tail(bits.as_mut(), len);
            BitArrayBase { len, bits }
        }
    }
}

impl<S> BitArrayBase<S> {
    /// Returns the number of bits in the array.
    ///
    /// # Returns
    ///
    /// The length of the bit array in bits
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns `true` if the bit array has zero length.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

impl<S> BitArrayBase<S> {
    /// Helper function that returns the u64 index and bit position within that u64
    /// for a given bit index in the array.
    #[inline]
    fn bit_position(index: usize) -> (usize, usize) {
        let word_index = index / 64;
        let bit_position = index % 64;
        (word_index, bit_position)
    }

    /// Masks out any bits beyond the specified length in the bit array storage.
    ///
    /// This function ensures that bits beyond the logical length of the bit array
    /// are set to 0. This is crucial for maintaining invariants when the bit array
    /// length is not a multiple of 64 bits, as the underlying storage uses u64 words
    /// that may contain extra bits beyond the array's logical boundary.
    ///
    /// # Arguments
    ///
    /// * `bits` - A mutable slice of u64 words representing the bit storage
    /// * `len` - The logical length of the bit array in bits
    ///
    /// # Panics
    ///
    /// Panics if:
    /// - `bits.len() * 64 < len` (not enough storage for the specified length)
    /// - `bits.len() * 64 - len >= 64` (too much excess storage, indicates
    ///   a logic error)
    #[inline]
    pub(crate) fn mask_tail(bits: &mut [u64], len: usize) {
        let raw_len = bits.len() * 64;
        assert!(raw_len >= len, "{raw_len} >= {len}");
        assert!(raw_len - len < 64, "{raw_len} - {len}");

        if len == 0 || bits.is_empty() {
            return;
        }

        let partial = len % 64;
        // If len is perfectly aligned with u64 boundaries, no masking needed
        if partial == 0 {
            return;
        }

        let mask = (1u64 << partial) - 1; // Create mask with 'remainder' number of 1s
        *bits.last_mut().unwrap() &= mask;
    }

    /// Returns true if the storage has a valid tail mask for the given `len`.
    ///
    /// Checks that all bits beyond `len` in the last word are zero
    #[inline]
    pub(crate) fn is_tail_masked(bits: &[u64], len: usize) -> bool {
        let raw_len = bits.len() * 64;
        assert!(raw_len >= len, "{raw_len} >= {len}");
        assert!(raw_len - len < 64, "{raw_len} - {len}");

        if len == 0 || bits.is_empty() {
            return true;
        }

        let partial = len % 64;
        if partial == 0 {
            // perfectly aligned; nothing needs masking
            return true;
        }

        let mask = (1u64 << partial) - 1;
        (bits.last().unwrap() & !mask) == 0
    }
}

impl<S, S1> BitAnd<&BitArrayBase<S1>> for &BitArrayBase<S>
where
    S: BitStore + AsRef<[u64]> + AsMut<[u64]>,
    S1: AsRef<[u64]>,
{
    type Output = BitArrayBase<S>;

    fn bitand(self, rhs: &BitArrayBase<S1>) -> Self::Output {
        assert_eq!(
            self.len, rhs.len,
            "BitArrays must have the same length for bitwise AND operation: {} != {}",
            self.len, rhs.len
        );

        let mut result = BitArrayBase::<S>::empty(self.len);
        let result_bits = result.storage_mut();
        for (res, (left, right)) in result_bits
            .iter_mut()
            .zip(self.storage().iter().zip(rhs.storage().iter()))
        {
            *res = left & right;
        }

        BitArrayBase::<S>::mask_tail(result_bits, self.len);
        result
    }
}

impl<S, S1> BitOr<&BitArrayBase<S1>> for &BitArrayBase<S>
where
    S: BitStore + AsRef<[u64]> + AsMut<[u64]>,
    S1: AsRef<[u64]>,
{
    type Output = BitArrayBase<S>;

    fn bitor(self, rhs: &BitArrayBase<S1>) -> Self::Output {
        assert_eq!(
            self.len, rhs.len,
            "BitArrays must have the same length for bitwise AND operation: {} != {}",
            self.len, rhs.len
        );

        let mut result = BitArrayBase::<S>::empty(self.len);
        let result_bits = result.storage_mut();
        for (res, (left, right)) in result_bits
            .iter_mut()
            .zip(self.storage().iter().zip(rhs.storage().iter()))
        {
            *res = left | right;
        }

        BitArrayBase::<S>::mask_tail(result_bits, self.len);
        result
    }
}

impl<S, S1> BitXor<&BitArrayBase<S1>> for &BitArrayBase<S>
where
    S: BitStore + AsRef<[u64]> + AsMut<[u64]>,
    S1: AsRef<[u64]>,
{
    type Output = BitArrayBase<S>;

    fn bitxor(self, rhs: &BitArrayBase<S1>) -> Self::Output {
        assert_eq!(
            self.len, rhs.len,
            "BitArrays must have the same length for bitwise AND operation: {} != {}",
            self.len, rhs.len
        );

        let mut result = BitArrayBase::<S>::empty(self.len);
        let result_bits = result.storage_mut();
        for (res, (left, right)) in result_bits
            .iter_mut()
            .zip(self.storage().iter().zip(rhs.storage().iter()))
        {
            *res = left ^ right;
        }

        BitArrayBase::<S>::mask_tail(result_bits, self.len);
        result
    }
}

impl<S> Not for &BitArrayBase<S>
where
    S: BitStore + AsRef<[u64]> + AsMut<[u64]>,
{
    type Output = BitArrayBase<S>;

    fn not(self) -> Self::Output {
        let mut result = BitArrayBase::<S>::empty(self.len);
        let result_bits = result.storage_mut();
        for (res, this) in result_bits.iter_mut().zip(self.storage().iter()) {
            *res = !*this;
        }

        BitArrayBase::<S>::mask_tail(result_bits, self.len);
        result
    }
}

impl<S, S1> BitAndAssign<&BitArrayBase<S1>> for BitArrayBase<S>
where
    S: AsRef<[u64]> + AsMut<[u64]>,
    S1: AsRef<[u64]>,
{
    fn bitand_assign(&mut self, rhs: &BitArrayBase<S1>) {
        assert_eq!(
            self.len, rhs.len,
            "BitArrays must have the same length for bitwise AND-assign: {} != {}",
            self.len, rhs.len
        );
        for (l, r) in self.storage_mut().iter_mut().zip(rhs.storage().iter()) {
            *l &= *r;
        }
        let len = self.len;
        Self::mask_tail(self.storage_mut(), len);
    }
}

impl<S, S1> BitOrAssign<&BitArrayBase<S1>> for BitArrayBase<S>
where
    S: AsRef<[u64]> + AsMut<[u64]>,
    S1: AsRef<[u64]>,
{
    fn bitor_assign(&mut self, rhs: &BitArrayBase<S1>) {
        assert_eq!(
            self.len, rhs.len,
            "BitArrays must have the same length for bitwise OR-assign: {} != {}",
            self.len, rhs.len
        );
        for (l, r) in self.storage_mut().iter_mut().zip(rhs.storage().iter()) {
            *l |= *r;
        }
        let len = self.len;
        Self::mask_tail(self.storage_mut(), len);
    }
}

impl<S, S1> BitXorAssign<&BitArrayBase<S1>> for BitArrayBase<S>
where
    S: AsRef<[u64]> + AsMut<[u64]>,
    S1: AsRef<[u64]>,
{
    fn bitxor_assign(&mut self, rhs: &BitArrayBase<S1>) {
        assert_eq!(
            self.len, rhs.len,
            "BitArrays must have the same length for bitwise XOR-assign: {} != {}",
            self.len, rhs.len
        );
        for (l, r) in self.storage_mut().iter_mut().zip(rhs.storage().iter()) {
            *l ^= *r;
        }
        let len = self.len;
        Self::mask_tail(self.storage_mut(), len);
    }
}

/// An iterator over the positions of set bits in a `BitArray`.
///
/// This iterator yields the 0-based indices of all bits that are set to 1.
#[derive(Clone)]
pub struct BitArrayIter<'a> {
    words: std::slice::Iter<'a, u64>,
    current_word: u64,
    next_word_index: usize,
    base_index: usize,
    len: usize,
}

impl<'a> BitArrayIter<'a> {
    /// Creates an empty `BitArrayIter` (the first `next()` call returns `None`).
    pub fn empty() -> Self {
        BitArrayIter {
            words: [].iter(),
            current_word: 0,
            next_word_index: 1,
            base_index: 0,
            len: 0,
        }
    }
}

impl<'a> Iterator for BitArrayIter<'a> {
    type Item = usize;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // If current word has set bits, find and return the next one
            if self.current_word != 0 {
                // Find the position of the least significant set bit
                let bit_offset = self.current_word.trailing_zeros() as usize;
                let index = self.base_index + bit_offset;

                // Make sure we don't go beyond the bit array's length
                if index >= self.len {
                    return None;
                }

                // Clear the least significant set bit for next iteration
                self.current_word &= self.current_word - 1;

                return Some(index);
            }

            // Move to the next word
            match self.words.next() {
                Some(&word) => {
                    self.current_word = word;
                    self.base_index = self.next_word_index * 64;
                    self.next_word_index += 1;
                }
                None => return None,
            }
        }
    }
}

/// An iterator over contiguous ranges of 1-bits in a `BitArray`.
#[derive(Clone)]
pub struct BitArrayRangesIter<'a> {
    /// Iterator over the remaining `u64` storage words of the underlying bit array.
    ///
    /// Yields words that come after (or at) the current word being processed. Together
    /// with `current_word`, it represents the unprocessed suffix of the bit storage.
    /// It is advanced every time a new storage word is needed by the iterator.
    ///
    /// In the "within range" variants, this iterator starts at the first word that
    /// intersects the requested range and ends just after the end of the requested
    /// range.
    words: std::slice::Iter<'a, u64>,

    /// Scratch copy of the currently active 64-bit word.
    ///
    /// - Contains only the bits that have not yet been consumed (bits already
    ///   emitted as part of returned ranges are cleared to 0).
    /// - When a run spans to the end of this word, `current_word` is set to 0 and
    ///   the iterator pulls subsequent words from `words` to continue the run.
    /// - In the "within range" variants, all bits below the starting bit are masked
    ///   out before iteration begins.
    current_word: u64,

    /// Absolute word index (in the original storage slice) of the next word to load.
    ///
    /// This value is used to compute `base_index` as `base_index = next_word_index * 64`
    /// when the iterator advances to a new word. It also equals the count of words
    /// already fetched from `words`.
    next_word_index: usize,

    /// Base bit index (multiple of 64) corresponding to `current_word`.
    ///
    /// The absolute bit index of bit 0 in `current_word`. Returned ranges are computed
    /// by adding intra-word offsets to this base (e.g., `start = base_index + tz`).
    base_index: usize,

    /// Exclusive upper bound on absolute bit indices the iterator may return.
    ///
    /// - For `ranges_iter()`, this equals the bit array length.
    /// - For `ranges_iter_within()`, this equals the end of the caller-provided range.
    ///
    /// All produced ranges are clipped to be strictly below this limit, including the
    /// final range that may otherwise extend beyond the logical tail.
    len: usize,
}

impl<'a> BitArrayRangesIter<'a> {
    /// Creates an empty iterator that yields no ranges.
    pub fn empty() -> Self {
        BitArrayRangesIter {
            words: [].iter(),
            current_word: 0,
            next_word_index: 1,
            base_index: 0,
            len: 0,
        }
    }

    #[inline]
    fn advance_to_next_word(&mut self) -> Option<u64> {
        match self.words.next() {
            Some(&word) => {
                self.current_word = word;
                self.base_index = self.next_word_index * 64;
                self.next_word_index += 1;
                (self.base_index < self.len).then_some(word)
            }
            None => None,
        }
    }

    /// Extends a run that already reached the end of the current word across subsequent words.
    ///
    /// Assumes `start` is the absolute start of the run and `end` is the current exclusive end,
    /// aligned to a word boundary (`base_index + 64`). Advances internal state as needed and
    /// returns the finalized [start, end) range (clipped to `self.len`) when the run stops.
    #[inline]
    fn extend_run_across_words(&mut self, start: usize, mut end: usize) -> Option<Range<usize>> {
        while let Some(w) = self.advance_to_next_word() {
            if w == 0 {
                // Run stops at the boundary; nothing more in this word
                return Some(start..end.min(self.len));
            } else if w == u64::MAX {
                // Entire word continues the run
                end += 64;
                self.current_word = 0;
                // Respect the logical end bound
                if end >= self.len {
                    // We've reached or surpassed len; consume and stop
                    return Some(start..self.len);
                }
                // Keep going; nothing left in this word
                continue;
            } else {
                // Partial continuation: count leading ones from LSB
                let lead_ones = w.trailing_ones() as usize; // 0..63
                end += lead_ones;
                // Consume those ones and keep the remainder for next time
                let mask = (1u64 << lead_ones) - 1;
                self.current_word = w & !mask;
                return Some(start..end.min(self.len));
            }
        }

        (end > start).then_some(start..end.min(self.len))
    }
}

impl<'a> Iterator for BitArrayRangesIter<'a> {
    type Item = Range<usize>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.current_word == 0 {
                self.advance_to_next_word()?;
                continue;
            }

            // If current word has any set bits, emit the next run (contiguous 1s)
            let tz = self.current_word.trailing_zeros() as usize;
            let start = self.base_index + tz;
            if start >= self.len {
                return None;
            }

            // Count ones from start within this word
            let shifted = self.current_word >> tz;
            let ones_in_word = shifted.trailing_ones() as usize; // in [1..=64]

            // If the run ends within this word
            if tz + ones_in_word < 64 {
                // Clear the consumed bits and return the range
                let mask = (1u64 << ones_in_word) - 1;
                self.current_word &= !(mask << tz);
                let end = (start + ones_in_word).min(self.len);
                return Some(start..end);
            }

            // The run reaches the end of this word; consume rest of word
            // and continue into subsequent words.
            let end = self.base_index + 64;
            self.current_word = 0; // fully consumed this word from tz to 63
            // Extend across following words while they keep the run going.
            return self.extend_run_across_words(start, end);
        }
    }
}

pub type BitArray = BitArrayBase<Box<[u64]>>;

impl BitArray {
    /// Returns the number of heap-allocated bytes used by this bit array's storage.
    ///
    /// This accounts only for the backing boxed slice (`bits`) that holds the u64 words.
    /// It does not include the size of the `BitArray` struct itself or any allocator
    /// bookkeeping overhead. The result is exact for this type because `Box<[u64]>`
    /// stores exactly `len.div_ceil(64)` u64 words with no extra capacity.
    ///
    /// Equivalent to: `self.bits.len() * size_of::<u64>()`.
    pub fn heap_size_bytes(&self) -> usize {
        self.bits.len() * std::mem::size_of::<u64>()
    }
}

pub type BigBitArray = BitArrayBase<amudai_page_alloc::mmap_buffer::MmapBuffer>;

//! A thread-safe fixed-capacity bit set implementation using atomic operations.

use std::sync::atomic::{AtomicU64, Ordering};

/// A thread-safe fixed-capacity bit set implementation using atomic operations.
///
/// `AtomicBitSet` provides a concurrent data structure for managing a fixed-size collection
/// of bits where each bit can be atomically read, set, or cleared. The implementation uses
/// an array of `AtomicU64` values to store the bits, providing lock-free thread-safe access
/// to individual bits with configurable memory ordering semantics.
///
/// # Design and Implementation
///
/// The bit set is backed by a `Vec<AtomicU64>` where each `u64` stores 64 bits. The total
/// capacity is determined at construction time and cannot be changed. Bits are indexed
/// from 0 to `size - 1`, where bit index `i` is stored in:
/// - Array element: `i / 64` (or `i >> 6`)
/// - Bit position within element: `i % 64` (or `i & 63`)
///
/// All operations are implemented using atomic read-modify-write operations on the underlying
/// `AtomicU64` values.
///
/// ## Memory Ordering
///
/// - Read operations accept an `Ordering` parameter for fine-grained control
/// - Write operations use `SeqCst` ordering for strong consistency guarantees
/// - Search operations use `Relaxed` ordering
///
/// ## Concurrency Considerations
///
/// Due to the concurrent nature of the data structure, certain operations have important
/// behavioral characteristics:
///
/// - **Stale reads**: The result of [`get`] and [`try_find_unset_index`] may become outdated
///   immediately after the method returns
/// - **Race conditions**: Multiple threads calling [`try_claim_bit`] may compete for the same bits
/// - **Best-effort semantics**: [`try_claim_bit`] may fail even when unset bits exist due to contention
pub struct AtomicBitSet(Vec<AtomicU64>, usize);

impl AtomicBitSet {
    /// Creates a new `AtomicBitSet` with the specified number of bits.
    ///
    /// All bits are initially unset (false).
    ///
    /// # Arguments
    ///
    /// * `size` - The number of bits in the bit set.
    ///
    /// # Returns
    ///
    /// A new `AtomicBitSet` with all bits initialized to false.
    pub fn new(size: usize) -> Self {
        let vec_len = size.div_ceil(64);
        AtomicBitSet((0..vec_len).map(|_| AtomicU64::new(0)).collect(), size)
    }

    /// Returns the number of bits in the bit set.
    ///
    /// # Returns
    ///
    /// The total number of bits that can be stored in this bit set.
    #[inline]
    pub fn len(&self) -> usize {
        self.1
    }

    /// Checks whether the bit set has zero length.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Atomically reads the value of a bit at the specified index.
    ///
    /// # Concurrency Considerations
    ///
    /// **Important**: The result of this method is inherently stale in concurrent environments.
    /// Between the time this method returns a value and when you act on it, another thread
    /// may have changed that bit.
    ///
    /// # Arguments
    ///
    /// * `index` - The index of the bit to read. Must be less than `len()`.
    /// * `ordering` - The memory ordering for the atomic operation.
    ///
    /// # Returns
    ///
    /// `true` if the bit is set, `false` if it's unset.
    ///
    /// # Panics
    ///
    /// Panics if `index >= len()`.
    #[inline]
    pub fn get(&self, index: usize, ordering: Ordering) -> bool {
        assert!(index < self.len());
        let (num, pos) = self.bit_location(index);
        (num.load(ordering) & (1 << pos)) != 0
    }

    /// Atomically sets a bit to the specified value and returns its previous value.
    ///
    /// This operation uses `SeqCst` ordering for consistency.
    ///
    /// # Arguments
    ///
    /// * `index` - The index of the bit to set. Must be less than `len()`.
    /// * `value` - The value to set.
    ///
    /// # Returns
    ///
    /// The previous value of the bit: `true` if it was set, `false` if it was unset.
    ///
    /// # Panics
    ///
    /// Panics if `index >= len()`.
    #[inline]
    pub fn set_value(&self, index: usize, value: bool) -> bool {
        if value {
            self.set(index)
        } else {
            self.reset(index)
        }
    }

    /// Atomically sets a bit to true and returns its previous value.
    ///
    /// This operation uses `SeqCst` ordering for consistency.
    ///
    /// # Arguments
    ///
    /// * `index` - The index of the bit to set. Must be less than `len()`.
    ///
    /// # Returns
    ///
    /// The previous value of the bit: `true` if it was already set, `false` if it was unset.
    ///
    /// # Panics
    ///
    /// Panics if `index >= len()`.
    #[inline]
    pub fn set(&self, index: usize) -> bool {
        assert!(index < self.len());
        let (num, pos) = self.bit_location(index);
        (num.fetch_or(1 << pos, Ordering::SeqCst) & (1 << pos)) != 0
    }

    /// Atomically sets a bit to false and returns its previous value.
    ///
    /// This operation uses `SeqCst` ordering for consistency.
    ///
    /// # Arguments
    ///
    /// * `index` - The index of the bit to reset. Must be less than `len()`.
    ///
    /// # Returns
    ///
    /// The previous value of the bit: `true` if it was set, `false` if it was already unset.
    ///
    /// # Panics
    ///
    /// Panics if `index >= len()`.
    #[inline]
    pub fn reset(&self, index: usize) -> bool {
        assert!(index < self.len());
        let (num, pos) = self.bit_location(index);
        (num.fetch_and(!(1 << pos), Ordering::SeqCst) & (1 << pos)) != 0
    }

    /// Atomically finds and claims an available unset bit in the bit set.
    ///
    /// **This is a best-effort method** that may fail to claim a bit even when unset bits
    /// are available. The method uses a limited retry mechanism and may give up after a
    /// certain number of attempts in high-contention scenarios.
    ///
    /// This method provides a race-free way to locate an unset bit and immediately claim it
    /// for exclusive use. It combines the search and set operations with retry logic to handle
    /// concurrent access scenarios where multiple threads might be competing for the same bits.
    ///
    /// # Concurrency Safety
    ///
    /// This method is designed for high-concurrency scenarios where multiple threads need to
    /// atomically claim unique bits. The retry mechanism ensures that each successful call
    /// returns a unique bit index that no other thread can claim simultaneously.
    ///
    /// # Performance Considerations
    ///
    /// - In low-contention scenarios, this method typically succeeds on the first attempt
    /// - Under high contention, multiple retries may be necessary as threads compete for bits
    /// - **The method may fail and return `None` even when bits are available** due to:
    ///   - Excessive concurrent contention causing retry exhaustion
    ///   - Race conditions where other threads claim bits faster than this thread can retry
    ///
    /// # Returns
    ///
    /// - `Some(index)` - The index of the successfully claimed bit (now set to `true`)
    /// - `None` - **Failed to claim an unset bit** (this can happen even when unset bits exist
    ///   due to the best-effort nature of the algorithm)
    pub fn try_claim_bit(&self) -> Option<usize> {
        // The method uses an optimistic approach with retry logic:
        //  1. Calls [`try_find_unset_index`](Self::try_find_unset_index) to locate a candidate unset bit
        //  2. Attempts to claim the bit using [`set`](Self::set), which returns the previous value
        //  3. If the previous value was `false`, the claim was successful and the index is returned
        //  4. If the previous value was `true`, another thread claimed the bit first, so retry from step 1
        //  5. If no unset bits are found, returns `None`

        const RETRY_COUNT: usize = 8;
        for _ in 0..RETRY_COUNT {
            // Find the first unset bit
            let candidate_index = self.try_find_unset_index()?;

            // Try to claim it by setting the bit
            // set() returns the previous value: false = successful claim, true = already set
            if !self.set(candidate_index) {
                // Successfully claimed the bit (it was previously unset)
                return Some(candidate_index);
            }

            // Another thread claimed this bit first, retry with a new search
            // The loop will continue until either:
            // 1. We successfully claim a bit, or
            // 2. No unset bits remain (try_find_unset_index returns None), or
            // 3. Retry attempts are exhausted
        }
        None
    }

    /// Attempts to locate the first unset bit and return its index.
    ///
    /// This method scans the bit set from index 0 onwards to find the first
    /// bit that is unset (false).
    ///
    /// # Concurrency Considerations
    ///
    /// **Important**: The result of this method is inherently stale in concurrent environments.
    /// Between the time this method returns an index and when you act on it, another thread
    /// may have set that bit. For reliable concurrent operations, you must validate the result
    /// by attempting to set the bit using [`set`](Self::set) and checking the previous value.
    /// See also [`try_claim_bit`](Self::try_claim_bit).
    ///
    /// # Returns
    ///
    /// `Some(index)` where `index` is the first unset bit, or `None` if all bits are set.
    pub fn try_find_unset_index(&self) -> Option<usize> {
        for (i, num) in self.0.iter().enumerate() {
            let n = num.load(Ordering::Relaxed);
            if n == u64::MAX {
                continue;
            }
            let found = i * 64 + Self::find_zero_bit(n);
            if found < self.len() {
                return Some(found);
            }
        }
        None
    }

    fn bit_location(&self, index: usize) -> (&AtomicU64, usize) {
        (&self.0[index >> 6], index & 63)
    }

    fn find_zero_bit(n: u64) -> usize {
        assert!(n != u64::MAX);
        n.trailing_ones() as usize
    }
}

#[cfg(test)]
mod tests {
    use crate::atomic_bit_set::AtomicBitSet;

    #[test]
    fn test_atomic_bit_set() {
        let set = AtomicBitSet::new(80);
        assert!(!set.set(0));
        assert!(set.reset(0));
        assert_eq!(set.try_find_unset_index().unwrap(), 0);
        assert!(!set.set(79));
        for i in 0..set.len() {
            set.set(i);
        }
        assert!(set.try_find_unset_index().is_none());
        set.reset(77);
        assert_eq!(set.try_find_unset_index().unwrap(), 77);
        set.reset(30);
        assert_eq!(set.try_find_unset_index().unwrap(), 30);
    }

    #[test]
    fn test_try_claim_bit_basic() {
        let set = AtomicBitSet::new(10);

        // Should claim the first available bit (index 0)
        assert_eq!(set.try_claim_bit().unwrap(), 0);
        assert!(set.get(0, std::sync::atomic::Ordering::Relaxed));

        // Should claim the next available bit (index 1)
        assert_eq!(set.try_claim_bit().unwrap(), 1);
        assert!(set.get(1, std::sync::atomic::Ordering::Relaxed));

        // Fill all remaining bits
        for _ in 2..10 {
            set.try_claim_bit().unwrap();
        }

        // Should return None when all bits are claimed
        assert!(set.try_claim_bit().is_none());

        // Reset a bit and verify we can claim it again
        set.reset(5);
        assert_eq!(set.try_claim_bit().unwrap(), 5);
    }

    #[test]
    fn test_try_claim_bit_concurrent() {
        use std::sync::{Arc, Barrier};
        use std::thread;

        let set = Arc::new(AtomicBitSet::new(100));
        let num_threads = 10;
        let barrier = Arc::new(Barrier::new(num_threads));
        let mut handles = vec![];

        for _ in 0..num_threads {
            let set_clone = set.clone();
            let barrier_clone = barrier.clone();

            handles.push(thread::spawn(move || {
                barrier_clone.wait(); // Synchronize thread start

                let mut claimed_bits = Vec::new();
                // Each thread tries to claim 5 bits
                for _ in 0..5 {
                    if let Some(index) = set_clone.try_claim_bit() {
                        claimed_bits.push(index);
                    }
                }
                claimed_bits
            }));
        }

        // Collect all claimed bits from all threads
        let mut all_claimed_bits = Vec::new();
        for handle in handles {
            let thread_bits = handle.join().unwrap();
            all_claimed_bits.extend(thread_bits);
        }

        // Verify no duplicates (each bit claimed exactly once)
        all_claimed_bits.sort();
        let original_len = all_claimed_bits.len();
        all_claimed_bits.dedup();
        assert_eq!(
            original_len,
            all_claimed_bits.len(),
            "Duplicate bits were claimed"
        );

        // Verify all claimed bits are actually set
        for &index in &all_claimed_bits {
            assert!(set.get(index, std::sync::atomic::Ordering::Relaxed));
        }
    }
}

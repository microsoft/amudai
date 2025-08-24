//! Bloom filter decoder implementation.

use amudai_bytes::Bytes;
use xxhash_rust::xxh3::xxh3_64_with_seed;

/// A concrete decoder for a Split-Block Bloom Filter that owns aligned data
/// and uses XXH3-64 hash function for optimal performance.
pub struct SbbfDecoder {
    aligned_data: Bytes,
    filter_fn: sbbf_rs::FilterFn,
    num_buckets: usize,
    hash_seed: u64,
}

// SAFETY: SbbfDecoder is safe to send between threads and share references
// because:
// - Bytes is Send + Sync (immutable shared data)
// - FilterFn is Send + Sync (stateless function wrapper)
// - num_buckets is Copy and thus Send + Sync
unsafe impl Send for SbbfDecoder {}
unsafe impl Sync for SbbfDecoder {}

impl SbbfDecoder {
    /// Creates a new SBBF decoder from aligned data with a custom hash seed.
    pub fn new(aligned_data: Bytes, hash_seed: u64) -> Result<Self, String> {
        // Validate that we have data and it's properly aligned for SBBF (256-bit blocks = 32 bytes)
        if aligned_data.is_empty() || !aligned_data.is_aligned(32) {
            return Err("SBBF data must be non-empty and aligned to 32-byte blocks".to_string());
        }

        let filter_fn = sbbf_rs::FilterFn::new();
        let num_buckets = aligned_data.len() / 32; // Each bucket is 32 bytes (256 bits)

        Ok(Self {
            aligned_data,
            filter_fn,
            num_buckets,
            hash_seed,
        })
    }

    /// Creates a new decoder from aligned data with a custom hash seed.
    pub fn from_aligned_data(aligned_data: Bytes, hash_seed: u64) -> Result<Self, String> {
        Self::new(aligned_data, hash_seed)
    }

    /// Tests whether a binary value might be present in the filter.
    #[inline]
    pub fn probe(&self, value: &[u8]) -> bool {
        let hash = xxh3_64_with_seed(value, self.hash_seed);
        let ptr = self.aligned_data.as_ptr();

        // SAFETY: We validated alignment during construction
        unsafe { self.filter_fn.contains(ptr, self.num_buckets, hash) }
    }

    /// Tests whether a value with a pre-computed hash might be present in the filter.
    ///
    /// This is useful when the hash has already been computed to avoid recomputation.
    #[inline]
    pub fn probe_hash(&self, hash: u64) -> bool {
        let ptr = self.aligned_data.as_ptr();

        // SAFETY: We validated alignment during construction
        unsafe { self.filter_fn.contains(ptr, self.num_buckets, hash) }
    }
}

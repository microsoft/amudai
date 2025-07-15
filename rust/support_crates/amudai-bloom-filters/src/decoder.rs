//! Bloom filter decoder implementation.

use crate::config::BLOOM_FILTER_HASH_SEED;
use amudai_bytes::Bytes;
use amudai_common::Result;
use xxhash_rust::xxh3::xxh3_64_with_seed;

/// A concrete decoder for a Split-Block Bloom Filter that owns aligned data
/// and uses XXH3-64 hash function for optimal performance.
pub struct SbbfDecoder {
    aligned_data: Bytes,
    filter_fn: sbbf_rs::FilterFn,
    num_buckets: usize,
}

// SAFETY: SbbfDecoder is safe to send between threads and share references
// because:
// - Bytes is Send + Sync (immutable shared data)
// - FilterFn is Send + Sync (stateless function wrapper)
// - num_buckets is Copy and thus Send + Sync
unsafe impl Send for SbbfDecoder {}
unsafe impl Sync for SbbfDecoder {}

impl SbbfDecoder {
    /// Creates a new SBBF decoder from aligned data.
    pub fn new(aligned_data: Bytes) -> Result<Self> {
        // Validate that we have data and it's properly aligned for SBBF (256-bit blocks = 32 bytes)
        if aligned_data.is_empty() || !aligned_data.is_aligned(32) {
            return Err(amudai_common::error::Error::invalid_arg(
                "aligned_data".to_string(),
                "SBBF data must be non-empty and aligned to 32-byte blocks".to_string(),
            ));
        }

        let filter_fn = sbbf_rs::FilterFn::new();
        let num_buckets = aligned_data.len() / 32; // Each bucket is 32 bytes (256 bits)

        Ok(Self {
            aligned_data,
            filter_fn,
            num_buckets,
        })
    }

    /// Creates a new decoder from aligned data and hash algorithm name.
    pub fn from_aligned_data(aligned_data: Bytes) -> Result<Self> {
        Self::new(aligned_data)
    }

    /// Tests whether a binary value might be present in the filter.
    #[inline]
    pub fn probe(&self, value: &[u8]) -> bool {
        let hash = xxh3_64_with_seed(value, BLOOM_FILTER_HASH_SEED);
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

//! Core Split-Block Bloom Filter (SBBF) structures and definitions.

/// XXH3-64 hash algorithm name.
pub const XXH3_64_ALGORITHM: &str = "xxh3_64";

/// Seed value for XXH3-64 hash function used in bloom filters.
/// This seed ensures consistent hashing across different components
/// and provides deterministic behavior for bloom filter operations.
pub(crate) const BLOOM_FILTER_HASH_SEED: u64 = 0x4d75_6461_6942_4646; // "AmudaiBFF" in hex

/// Maximum number of distinct values that can be added to a bloom filter.
/// When this threshold is exceeded, the bloom filter is abandoned to prevent
/// excessive memory usage and maintain acceptable false positive rates.
pub const CARDINALITY_THRESHOLD: usize = 1024;

/// Configuration for bloom filter construction.
#[derive(Debug, Clone)]
pub struct BloomFilterConfig {
    /// Maximum number of distinct values before abandoning filter construction.
    pub cardinality_threshold: usize,
    /// Target false positive probability.
    pub target_fpp: f64,
    /// Maximum size of the filter in bytes.
    pub max_filter_size: usize,
    /// Hash algorithm to use. Only "xxh3_64" is supported.
    pub hash_algorithm: String,
    /// Hash function seed.
    pub hash_seed: u64,
}

impl Default for BloomFilterConfig {
    fn default() -> Self {
        Self {
            cardinality_threshold: CARDINALITY_THRESHOLD,
            target_fpp: 0.01,
            max_filter_size: 1_048_576, // 1MB
            hash_algorithm: XXH3_64_ALGORITHM.to_string(),
            hash_seed: BLOOM_FILTER_HASH_SEED,
        }
    }
}

impl BloomFilterConfig {
    /// Validates the configuration and returns an error if invalid.
    pub fn validate(&self) -> Result<(), String> {
        if self.hash_algorithm != XXH3_64_ALGORITHM {
            return Err(format!(
                "Unsupported hash algorithm: {}. Only xxh3_64 is supported.",
                self.hash_algorithm
            ));
        }

        if self.cardinality_threshold == 0 {
            return Err("cardinality_threshold must be greater than 0".to_string());
        }

        if self.target_fpp < 0.000001 || self.target_fpp > 0.99 {
            return Err("target_fpp must be between 0.000001 and 0.99".to_string());
        }

        if self.max_filter_size == 0 {
            return Err("max_filter_size must be greater than 0".to_string());
        }

        Ok(())
    }
}

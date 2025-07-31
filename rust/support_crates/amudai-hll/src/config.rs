/// XXH3-64 hash algorithm name.
pub(crate) const HLL_XXH3_64_ALGORITHM: &str = "xxh3_64";

/// Number of bits for HLL indexing - 128 counters = 2^7
const DEFAULT_HLL_BITS_PER_INDEX: u32 = 7;

/// Seed for XXH3-64 hash function used in HLL cardinality estimation
/// Represents "AmudHLL1" in hex to avoid collisions with other hash usage
const HLL_SKETCH_HASH_SEED: u64 = 0x416d_7564_484c_4c31; // "AmudHLL1" in hex

/// Configuration for HyperLogLog construction and reconstruction.
/// This struct contains the essential parameters needed to create or describe an HLL instance.
/// Derived values are computed on-demand to reduce memory usage and simplify serialization.
#[derive(Debug, Clone)]
pub struct HllSketchConfig {
    /// Number of bits for indexing HLL sub-streams; the number of counters is `pow(2, bits_per_index)`.
    pub bits_per_index: u32,

    /// Hash algorithm to use. Only "xxh3_64" is supported.
    pub hash_algorithm: String,

    // Hash seed
    pub hash_seed: u64,
}

impl Default for HllSketchConfig {
    fn default() -> Self {
        Self::with_all_parameters(
            DEFAULT_HLL_BITS_PER_INDEX,
            HLL_XXH3_64_ALGORITHM.to_string(),
            HLL_SKETCH_HASH_SEED,
        )
        .expect("Default bits_per_index should be valid")
    }
}

impl HllSketchConfig {
    /// Creates a fully specified HllSketchConfig with all parameters.
    pub fn with_all_parameters(
        bits_per_index: u32,
        hash_algorithm: String,
        hash_seed: u64,
    ) -> Result<Self, String> {
        let config = Self {
            bits_per_index,
            hash_algorithm,
            hash_seed,
        };
        config.validate()?;
        Ok(config)
    }

    /// Validates basic parameters before computing derived values.
    fn validate(&self) -> Result<(), String> {
        if self.hash_algorithm != HLL_XXH3_64_ALGORITHM {
            return Err(format!(
                "Unsupported hash algorithm: {}. Only xxh3_64 is supported.",
                self.hash_algorithm
            ));
        }

        if self.bits_per_index < 4 || self.bits_per_index > 18 {
            return Err("bits_per_index must be between 4 and 18".to_string());
        }

        Ok(())
    }

    /// Checks if this configuration is compatible for merging with another configuration.
    ///
    /// Two HLL configurations are compatible for merging if all their core parameters match:
    /// - bits_per_index
    /// - hash_algorithm  
    /// - bits_for_hll
    /// - m (table size)
    /// - alpha_m (bias correction factor)
    /// - algorithm_selection_threshold
    ///
    /// This ensures that the underlying HLL structures are identical and can be safely merged.
    ///
    /// # Arguments
    ///
    /// * `other` - The other configuration to check compatibility with
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if the configurations are compatible for merging,
    /// or `Err(String)` with a descriptive error message if they are not compatible.
    pub fn is_compatible_for_merge(&self, other: &HllSketchConfig) -> Result<(), String> {
        if self.bits_per_index != other.bits_per_index {
            return Err(format!(
                "Cannot merge HLL sketches with different bits_per_index: {} vs {}",
                self.bits_per_index, other.bits_per_index
            ));
        }

        if self.hash_algorithm != other.hash_algorithm {
            return Err(format!(
                "Cannot merge HLL sketches with different hash algorithms: {} vs {}",
                self.hash_algorithm, other.hash_algorithm
            ));
        }

        if self.hash_seed != other.hash_seed {
            return Err(format!(
                "Cannot merge HLL sketches with different hash seeds: {} vs {}",
                self.hash_seed, other.hash_seed
            ));
        }

        Ok(())
    }
}

//! HyperLogLog builder for cardinality estimation that can be shared
//! across different data type statistics collectors.

use crate::{config::HllSketchConfig, hll::HyperLogLog};

/// A builder for HyperLogLog cardinality estimation that can be shared
/// across different data type statistics collectors.
pub struct HllSketch {
    hll: HyperLogLog,
    config: HllSketchConfig,
}

impl Default for HllSketch {
    fn default() -> Self {
        Self::new_default()
    }
}

impl HllSketch {
    /// Creates a new HLL builder with the specified configuration.
    pub fn new(config: HllSketchConfig) -> Result<Self, String> {
        let hll = HyperLogLog::with_bits_per_index(config.bits_per_index);
        Ok(Self { hll, config })
    }

    /// Creates a new HLL builder with the default configuration.
    pub fn new_default() -> Self {
        Self::new(HllSketchConfig::default()).expect("Default configuration should be valid")
    }

    /// Gets the configuration of this HLL builder.
    pub fn get_config(&self) -> HllSketchConfig {
        self.config.clone()
    }

    /// Gets the raw counter data from the underlying HyperLogLog.
    ///
    /// This method provides access to the raw counter bytes that represent
    /// the HyperLogLog's internal state. This is useful for:
    /// - External serialization formats
    /// - Advanced analysis of the HLL state
    /// - Integration with external systems
    ///
    /// # Returns
    ///
    /// A slice of bytes representing the counter array. The length equals
    /// 2^bits_per_index (the `m` parameter from the configuration).
    pub fn get_counters(&self) -> &[u8] {
        self.hll.get_counters()
    }

    /// Creates an HLL builder from a configuration and serialized counter data.
    ///
    /// This is useful when reconstructing an HLL from external sources
    /// where you have the configuration parameters and counter data separately.
    ///
    /// # Arguments
    ///
    /// * `config` - The HLL configuration
    /// * `counter_data` - The serialized counter array
    ///
    /// # Returns
    ///
    /// A `Result` containing the `HllSketch` or an error if the parameters are invalid
    pub fn from_config_and_counters(
        config: HllSketchConfig,
        counter_data: Vec<u8>,
    ) -> Result<Self, String> {
        let hll = HyperLogLog::with_lookup_table(config.bits_per_index, counter_data)
            .map_err(|e| e.to_string())?;
        Ok(Self { hll, config })
    }

    /// Adds a byte slice to the HLL.
    #[inline]
    pub fn add(&mut self, value: &[u8]) {
        let hash = xxhash_rust::xxh3::xxh3_64_with_seed(value, self.config.hash_seed);
        self.hll.add_hash(hash);
    }

    /// Gets the estimated cardinality.
    #[inline]
    pub fn estimate_count(&self) -> usize {
        self.hll.estimate_count()
    }

    /// Merges another HLL into this one.
    pub fn merge_from(&mut self, other: &HllSketch) -> std::io::Result<()> {
        // Validate that configurations are compatible for merging
        let self_config = self.get_config();
        let other_config = other.get_config();

        // Use the configuration's compatibility check
        if let Err(error_msg) = self_config.is_compatible_for_merge(&other_config) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                error_msg,
            ));
        }

        // If compatibility check passes, perform the merge
        self.hll.merge_from(&other.hll)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hll_builder_basic() {
        let mut builder = HllSketch::new_default();

        // Add some values
        builder.add("hello".as_bytes());
        builder.add("world".as_bytes());
        builder.add("hello".as_bytes()); // Duplicate

        let count = builder.estimate_count();
        assert!(count >= 2); // Should estimate around 2 distinct values
    }

    #[test]
    fn test_hll_builder_bytes() {
        let mut builder = HllSketch::new_default();

        builder.add(b"hello");
        builder.add(b"world");
        builder.add(&[0, 1, 2, 3]);

        let count = builder.estimate_count();
        assert!(count >= 3); // Should estimate around 3 distinct values
    }

    #[test]
    fn test_hll_builder_merge() {
        let mut builder1 = HllSketch::new_default();
        builder1.add("hello".as_bytes());
        builder1.add("world".as_bytes());

        let mut builder2 = HllSketch::new_default();
        builder2.add("rust".as_bytes());
        builder2.add("world".as_bytes()); // Duplicate across builders

        builder1.merge_from(&builder2).unwrap();

        let count = builder1.estimate_count();
        assert!(count >= 3); // Should estimate around 3 distinct values
    }
}

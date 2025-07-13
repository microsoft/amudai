//! Bloom filter builder implementation.

use crate::{
    CARDINALITY_THRESHOLD,
    config::{BLOOM_FILTER_HASH_SEED, BloomFilterConfig},
};
use amudai_collections::identity_hash::IdentityHashSet;
use xxhash_rust::xxh3::xxh3_64_with_seed;

/// Computes optimal bitmap size for a bloom filter.
/// Fixed formula: m = -n * ln(p) / (ln(2)^2)
fn compute_bitmap_size(items_count: usize, fp_p: f64) -> usize {
    assert!(items_count > 0);
    assert!(fp_p > 0.0 && fp_p < 1.0);
    let log2 = std::f64::consts::LN_2;
    let log2_2 = log2 * log2;
    (-(items_count as f64) * fp_p.ln() / log2_2).ceil() as usize
}

/// Builder for creating Split-Block Bloom Filters using XXH3-64 hash.
#[derive(Debug)]
pub(crate) struct SbbfBuilder {
    hashes: IdentityHashSet<u64>,
    target_fpp: f64,
    max_filter_size: usize,
    filter: Option<sbbf_rs_safe::Filter>,
}

impl SbbfBuilder {
    /// Creates a new SBBF builder.
    pub fn new(
        initial_capacity: usize,
        target_fpp: f64,
        max_filter_size: usize,
    ) -> Result<Self, String> {
        let mut hashes = IdentityHashSet::default();
        hashes.reserve(initial_capacity);

        Ok(Self {
            hashes,
            target_fpp,
            max_filter_size,
            filter: None,
        })
    }

    /// Adds a pre-computed hash value to the filter.
    /// Returns true if this is a new distinct value, false if it's a duplicate.
    pub fn add_hash(&mut self, hash: u64) -> bool {
        self.hashes.insert(hash)
    }

    /// Returns the current number of distinct values tracked.
    pub fn distinct_count(&self) -> usize {
        self.hashes.len()
    }

    /// Builds the final bloom filter and stores it in the builder.
    /// Returns true if the filter was successfully built, false if there are no values or if the filter would be too large.
    pub fn finish(&mut self) -> bool {
        if self.hashes.is_empty() {
            return false;
        }

        // Calculate optimal bitmap size using the formula
        let bitmap_size = compute_bitmap_size(self.hashes.len(), self.target_fpp);
        let bits_per_key = (bitmap_size as f64 / self.hashes.len() as f64).ceil() as usize;
        let mut filter = sbbf_rs_safe::Filter::new(bits_per_key, self.hashes.len());

        // Check result size
        let size = filter.as_bytes().len();
        if size > self.max_filter_size {
            return false;
        }

        // Insert all hash values directly
        for hash in &self.hashes {
            filter.insert_hash(*hash);
        }

        // Store the filter directly
        self.filter = Some(filter);

        true
    }

    /// Returns the filter data as a byte slice.
    /// Returns an empty slice if the filter hasn't been built.
    pub fn filter_data_as_bytes(&self) -> &[u8] {
        self.filter.as_ref().map_or(&[], |f| f.as_bytes())
    }

    /// Returns the number of blocks in the filter.
    /// Returns 0 if the filter hasn't been built.
    pub fn num_blocks(&self) -> u64 {
        self.filter
            .as_ref()
            .map_or(0, |f| (f.as_bytes().len().div_ceil(32)) as u64)
    }

    /// Returns the number of values in the filter.
    /// Returns 0 if the filter hasn't been built.
    pub fn num_values(&self) -> u64 {
        self.filter.as_ref().map_or(0, |_| self.hashes.len() as u64)
    }
}

/// Manages distinct value tracking and bloom filter construction.
pub struct BloomFilterCollector {
    bloom_filter_builder: Option<SbbfBuilder>,
    config: BloomFilterConfig,
    is_finished: bool,
}

impl BloomFilterCollector {
    /// Creates a new bloom filter tracker with the given configuration.
    pub fn new(config: BloomFilterConfig) -> Self {
        let bloom_filter_builder = Self::create_builder(&config);

        Self {
            bloom_filter_builder,
            config,
            is_finished: false,
        }
    }

    /// Creates a builder based on the hash algorithm in the config.
    fn create_builder(config: &BloomFilterConfig) -> Option<SbbfBuilder> {
        SbbfBuilder::new(
            CARDINALITY_THRESHOLD / 4,
            config.target_fpp,
            config.max_filter_size,
        )
        .ok()
    }

    /// Processes a byte slice value for bloom filter construction.
    /// Returns true if the value was successfully added, false if filter construction was abandoned.
    pub fn process_value(&mut self, value: &[u8]) -> bool {
        if self.bloom_filter_builder.is_some() {
            let hash = xxh3_64_with_seed(value, BLOOM_FILTER_HASH_SEED);
            self.process_hash(hash)
        } else {
            // Filter construction already abandoned or not enabled
            false
        }
    }

    /// Processes a pre-computed hash value for bloom filter construction.
    /// Returns true if the hash was successfully added, false if filter construction was abandoned.
    pub fn process_hash(&mut self, hash: u64) -> bool {
        if let Some(ref mut builder) = self.bloom_filter_builder {
            let is_new_value = builder.add_hash(hash);

            if is_new_value && builder.distinct_count() > self.config.cardinality_threshold {
                // Abandon filter construction due to high cardinality
                self.bloom_filter_builder = None;
                false
            } else {
                // Continue building (whether new or duplicate value)
                true
            }
        } else {
            // Filter construction already abandoned or not enabled
            false
        }
    }

    /// Finalizes the bloom filter construction.
    /// Returns true if the filter was successfully built, false otherwise.
    pub fn finish(&mut self) -> bool {
        if let Some(ref mut builder) = self.bloom_filter_builder {
            self.is_finished = builder.finish();
            self.is_finished
        } else {
            false
        }
    }

    /// Returns the filter data as a byte slice.
    /// Returns an empty slice if the filter hasn't been successfully finished.
    pub fn filter_data_as_bytes(&self) -> &[u8] {
        if self.is_finished {
            self.bloom_filter_builder
                .as_ref()
                .map_or(&[], |b| b.filter_data_as_bytes())
        } else {
            &[]
        }
    }

    /// Returns the number of blocks in the filter.
    /// Returns 0 if the filter hasn't been successfully finished.
    pub fn num_blocks(&self) -> u64 {
        if self.is_finished {
            self.bloom_filter_builder
                .as_ref()
                .map_or(0, |b| b.num_blocks())
        } else {
            0
        }
    }

    /// Returns the target false positive probability.
    pub fn target_fpp(&self) -> f64 {
        self.config.target_fpp
    }

    /// Returns the number of values in the filter.
    /// Returns 0 if the filter hasn't been successfully finished.
    pub fn num_values(&self) -> u64 {
        if self.is_finished {
            self.bloom_filter_builder
                .as_ref()
                .map_or(0, |b| b.num_values())
        } else {
            0
        }
    }

    /// Returns the hash algorithm name.
    pub fn hash_algorithm(&self) -> &str {
        &self.config.hash_algorithm
    }
}

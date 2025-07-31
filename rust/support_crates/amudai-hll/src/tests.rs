//! Tests for the HyperLogLog cardinality estimator.
//!
//! This module contains comprehensive tests that validate the accuracy, merging behavior,
//! and serialization capabilities of the HyperLogLog implementation across different
//! configurations and scenarios.

use super::hll::HyperLogLog;
use rand::{Rng, rng};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::io::Cursor;

/// Tests basic HyperLogLog functionality across different configurations.
///
/// This test validates:
/// - Cardinality estimation accuracy within expected error bounds
/// - Merging behavior with multiple HLL instances
/// - Serialization and deserialization correctness
///
/// The test runs three scenarios:
/// 1. 10,000 distinct elements with 1 HLL using 10 bits per index
/// 2. 10,000 distinct elements distributed across 7 HLLs using 12 bits per index
/// 3. 100,000 distinct elements with 1 HLL using 17 bits per index (high accuracy)
#[test]
fn test_hll_basics() {
    run_hll_test(10000, 1, 10);
    run_hll_test(10000, 7, 12);
    run_hll_test(100000, 1, 17);
}

/// Core test function that validates HyperLogLog accuracy and functionality.
///
/// This function performs a comprehensive test of HyperLogLog capabilities by:
/// 1. Creating multiple HLL instances with the specified configuration
/// 2. Generating random alphanumeric strings as test data
/// 3. Distributing the data across multiple HLL instances (simulating distributed counting)
/// 4. Merging all HLLs to get the final cardinality estimate
/// 5. Validating that the estimation error is within theoretical bounds
/// 6. Testing serialization/deserialization round-trip accuracy
///
/// # Parameters
///
/// * `dcount` - The number of distinct elements to generate and add
/// * `num_hlls` - Number of separate HLL instances to distribute data across
/// * `bits_per_index` - HyperLogLog configuration parameter (affects accuracy and memory)
///
/// # Test Methodology
///
/// - **Data Generation**: Creates random 7-character alphanumeric strings
/// - **Distribution**: Randomly assigns each element to one of the HLL instances
/// - **Merging**: Combines all HLLs using the merge operation
/// - **Accuracy Check**: Verifies error is within 5 standard deviations (99.99% confidence)
/// - **Serialization Test**: Ensures pack/unpack operations preserve state exactly
///
/// # Error Bounds
///
/// The expected standard error for HyperLogLog is `1.04 / sqrt(2^bits_per_index)`.
/// The test allows up to 5 standard deviations, which gives very high confidence
/// that a correct implementation will pass.
fn run_hll_test(dcount: usize, num_hlls: usize, bits_per_index: u32) {
    // Create the specified number of HLL instances with identical configurations
    let mut hlls = (0..num_hlls)
        .map(|_| HyperLogLog::with_bits_per_index(bits_per_index))
        .collect::<Vec<_>>();

    let mut rng = rng();
    let mut chars = Vec::new();

    // Generate and add distinct elements to the HLL instances
    for _ in 0..dcount {
        chars.clear();
        let alphanumeric_chars = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

        // Create a random 7-character string
        for _ in 0..7 {
            chars.push(alphanumeric_chars[rng.random_range(0..alphanumeric_chars.len())]);
        }

        // Randomly distribute elements across HLL instances (simulates distributed scenario)
        let hll_idx = rng.random_range(0usize..num_hlls);
        hlls[hll_idx].add_hash(hash(chars.as_slice()));
    }

    // Merge all HLL instances to get the union cardinality
    let mut merged_hll = HyperLogLog::with_bits_per_index(bits_per_index);
    for hll in &hlls {
        merged_hll.merge_from(hll).unwrap();
    }

    // Validate estimation accuracy against theoretical error bounds
    let estimation = merged_hll.estimate_count();
    let std_error = 1.04f64 / 2.0f64.powi(bits_per_index as i32).sqrt();
    let max_error = std_error * 5.0; // 5 standard deviations for high confidence
    let error = ((estimation as f64) / (dcount as f64) - 1.0).abs();
    assert!(
        error <= max_error,
        "HLL estimation error {:.4} exceeds maximum allowed error {:.4} for {} elements with {} bits",
        error,
        max_error,
        dcount,
        bits_per_index
    );

    // Test serialization round-trip accuracy
    let mut packed = Vec::new();
    let mut merged_unpacked = HyperLogLog::with_bits_per_index(bits_per_index);
    for hll in &hlls {
        packed.clear();
        let len = hll.pack_to_writer(&mut packed).unwrap();
        assert_eq!(packed.len(), len);
        let unpacked = HyperLogLog::unpack_from_reader(&mut Cursor::new(&packed)).unwrap();
        merged_unpacked.merge_from(&unpacked).unwrap();
    }

    // Verify that serialization preserves the exact state
    assert_eq!(
        merged_hll.estimate_count(),
        merged_unpacked.estimate_count(),
        "Serialization round-trip should preserve exact cardinality estimate"
    );
}

/// Utility function to compute a 64-bit hash of any hashable value.
///
/// This function provides a convenient way to hash arbitrary values using
/// Rust's standard `DefaultHasher`. It's used throughout the tests to convert
/// test data (like strings) into the hash values that HyperLogLog requires.
///
/// # Type Parameters
///
/// * `V` - Any type that implements the `Hash` trait
///
/// # Arguments
///
/// * `value` - The value to hash
///
/// # Returns
///
/// A 64-bit hash value suitable for use with `HyperLogLog::add_hash()`
///
/// # Example
///
/// ```
/// let hash_val = hash("test string");
/// let mut hll = HyperLogLog::new(1);
/// hll.add_hash(hash_val);
/// ```
pub fn hash<V>(value: V) -> u64
where
    V: Hash,
{
    let mut h = DefaultHasher::default();
    value.hash(&mut h);
    h.finish()
}

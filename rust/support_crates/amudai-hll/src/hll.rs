//! HyperLogLog is an algorithm for the count-distinct problem, approximating the number of
//! distinct elements in a multiset. Calculating the exact cardinality of a multiset requires
//! an amount of memory proportional to the cardinality, which is impractical for very large
//! data sets. Probabilistic cardinality estimators, such as the HyperLogLog algorithm, use
//! significantly less memory than this, at the cost of obtaining only an approximation of the
//! cardinality.

use std::io::{self, Error, Read, Write};

use super::bias;
use byteorder::{LE, ReadBytesExt, WriteBytesExt};

/// HyperLogLog "distinct counter" implementation.
///
/// `HyperLogLog` has three main operations: `add_hash` to add a new element's hash to the set,
/// `estimate_count` to obtain the cardinality of the set and `merge_from` to obtain the union
/// of two sets.
pub struct HyperLogLog {
    /// Number of bits for indexing HLL sub-streams; the number of estimators
    /// is `pow(2, bits_per_index)`.
    bits_per_index: u32,

    /// Number of bits to compute the HLL estimate on.
    bits_for_hll: u32,

    /// HLL lookup table size
    m: u32,

    /// Fixed bias correction factor
    alpha_m: f64,

    /// Threshold determining whether to use LinearCounting or HyperLogLog based
    /// on an initial estimate.
    algorithm_selection_threshold: f64,

    /// Lookup table.
    counters: Box<[u8]>,
}

impl HyperLogLog {
    pub const HLL_FORMAT_MAGIC_VERSION: u32 = 0x47700001u32;

    /// Creates an instance of `HyperLogLog` with the specified number of bits per
    /// sub-stream index.
    ///
    /// The number of internal estimators is `pow(2, bits_per_index)` (each estimator
    /// costs a single byte of state), and higher number of estimators results in the
    /// better estimation accuracy.
    ///
    /// `bits_per_index` valid range is `[4, 18]`.
    ///
    /// The average relative error of the estimation is:
    ///
    ///  - ~1.6% when `bits_per_index == 12`,
    ///
    ///  - ~0.8% when `bits_per_index == 14`,
    ///
    ///  - ~0.4% when `bits_per_index == 16`,
    ///
    ///  - ~0.28% when `bits_per_index == 17`.
    ///
    ///  - ~0.2% when `bits_per_index == 18`.    
    pub fn with_bits_per_index(bits_per_index: u32) -> HyperLogLog {
        assert!(bits_per_index >= 4);
        assert!(bits_per_index <= 18);

        let bits_for_hll = 64 - bits_per_index;
        let m = HyperLogLog::get_hll_lookup_table_size(bits_per_index);
        let alpha_m = HyperLogLog::get_alpha_m(m);
        let algorithm_selection_threshold =
            HyperLogLog::get_algorithm_selection_threshold(bits_per_index);
        let counters = vec![0u8; m as usize].into_boxed_slice();

        HyperLogLog {
            bits_per_index,
            bits_for_hll,
            m,
            alpha_m,
            algorithm_selection_threshold,
            counters,
        }
    }

    /// Creates a new HyperLogLog with a predefined accuracy level.
    ///
    /// This is a convenience constructor that maps accuracy levels to specific
    /// `bits_per_index` values for common use cases.
    ///
    /// # Arguments
    ///
    /// * `accuracy` - Predefined accuracy level:
    ///   - 0: 12 bits (~1.6% error)
    ///   - 1: 14 bits (~0.8% error)
    ///   - 2: 16 bits (~0.4% error)
    ///   - 3: 17 bits (~0.28% error)
    ///   - 4: 18 bits (~0.2% error)
    ///   - Any other value defaults to 14 bits
    ///
    /// # Returns
    ///
    /// A new `HyperLogLog` instance configured for the specified accuracy level.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use amudai_hll::hll::HyperLogLog;
    ///
    /// // Create a HyperLogLog with high accuracy (~0.4% error)
    /// let hll = HyperLogLog::new(2);
    /// ```
    pub fn new(accuracy: u32) -> HyperLogLog {
        let bits_per_index: u32 = match accuracy {
            0 => 12,
            1 => 14,
            2 => 16,
            3 => 17,
            4 => 18,
            _ => 14,
        };
        HyperLogLog::with_bits_per_index(bits_per_index)
    }

    /// Creates a new HyperLogLog with a pre-existing lookup table.
    ///
    /// This constructor allows you to restore a HyperLogLog from a previously
    /// serialized state by providing the bits per index and the counter array.
    ///
    /// # Arguments
    ///
    /// * `bits_per_index` - Number of bits for indexing HLL sub-streams (must be 4-18)
    /// * `counters` - Pre-existing counter values from a previous HyperLogLog instance
    ///
    /// # Returns
    ///
    /// Returns `Ok(HyperLogLog)` if the parameters are valid, or an `Err` if the
    /// lookup table size doesn't match the expected size for the given `bits_per_index`.
    ///
    /// # Errors
    ///
    /// Returns an `InvalidInput` error if the counters vector length doesn't match
    /// the expected lookup table size (2^bits_per_index).
    pub fn with_lookup_table(bits_per_index: u32, counters: Vec<u8>) -> std::io::Result<Self> {
        let mut hll = Self::with_bits_per_index(bits_per_index);
        if hll.m as usize != counters.len() {
            return Err(invalid_arg_err(
                "HyperLogLog::with_lookup_table: lookup table doesn't match its size",
            ));
        }
        hll.counters = counters.into_boxed_slice();
        Ok(hll)
    }

    /// Gets the bits_per_index configuration parameter.
    ///
    /// This method returns the number of bits used for indexing HLL sub-streams,
    /// which determines the number of internal estimators (2^bits_per_index).
    ///
    /// # Returns
    ///
    /// The bits_per_index value used when creating this HyperLogLog instance.
    pub fn get_bits_per_index(&self) -> u32 {
        self.bits_per_index
    }

    /// Merges the internal state of another HLL into this one.
    ///
    /// After the merge, this HyperLogLog represents the approximation of the union
    /// of the two sets. The merge operation takes the maximum value for each counter
    /// position, which corresponds to the HyperLogLog union operation.
    ///
    /// # Arguments
    ///
    /// * `other` - The HyperLogLog instance to merge into this one
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` on successful merge, or an `Err` if the HLL parameters
    /// (bits_per_index, lookup table size) are incompatible.
    ///
    /// # Errors
    ///
    /// Returns an `InvalidInput` error if:
    /// - The lookup table sizes don't match
    /// - The HyperLogLog configurations are incompatible
    ///
    /// # Example
    ///
    /// ```no_run
    /// use amudai_hll::hll::HyperLogLog;
    /// use std::collections::hash_map::DefaultHasher;
    /// use std::hash::{Hash, Hasher};
    ///
    /// let mut hll1 = HyperLogLog::new(1);
    /// let mut hll2 = HyperLogLog::new(1);
    ///
    /// // Add some elements to each HLL
    /// for i in 0..100 {
    ///     let mut hasher = DefaultHasher::new();
    ///     i.hash(&mut hasher);
    ///     hll1.add_hash(hasher.finish());
    /// }
    ///
    /// for i in 50..150 {
    ///     let mut hasher = DefaultHasher::new();
    ///     i.hash(&mut hasher);
    ///     hll2.add_hash(hasher.finish());
    /// }
    ///
    /// // Merge hll2 into hll1
    /// hll1.merge_from(&hll2).expect("Merge should succeed");
    /// ```
    pub fn merge_from(&mut self, other: &HyperLogLog) -> ::std::io::Result<()> {
        if self.m != other.m {
            return Err(invalid_arg_err(
                "HyperLogLog::merge_from: incompatible HLL parameters",
            ));
        }

        assert_eq!(self.bits_per_index, other.bits_per_index);
        assert_eq!(self.bits_for_hll, other.bits_for_hll);
        assert_eq!(self.counters.len(), other.counters.len());

        for (dst, src) in self.counters.iter_mut().zip(other.counters.iter()) {
            *dst = ::std::cmp::max(*dst, *src);
        }

        Ok(())
    }

    /// Adds a 64-bit hash of an element to the HyperLogLog set.
    ///
    /// This is the core operation for adding elements to the HyperLogLog. The hash
    /// value is used to update the internal counters that track the maximum number
    /// of leading zeros observed for each sub-stream.
    ///
    /// # Arguments
    ///
    /// * `hash` - A 64-bit hash value of the element to add
    ///
    /// # Important Notes
    ///
    /// The caller is responsible for ensuring that the supplied `hash` value is a
    /// high-quality hash of the actual element, not a simple sequence like consecutive
    /// integers. Poor hash quality will significantly degrade the accuracy of the
    /// cardinality estimate.
    ///
    /// # Example
    ///
    /// ```
    /// use amudai_hll::hll::HyperLogLog;
    /// use std::collections::hash_map::DefaultHasher;
    /// use std::hash::{Hash, Hasher};
    ///
    /// let mut hll = HyperLogLog::new(1);
    ///
    /// // Add a string element
    /// let element = "hello world";
    /// let mut hasher = DefaultHasher::new();
    /// element.hash(&mut hasher);
    /// hll.add_hash(hasher.finish());
    ///
    /// // The HLL now contains information about this element
    /// let estimate = hll.estimate_count();
    /// ```
    #[inline]
    pub fn add_hash(&mut self, hash: u64) {
        let stream_idx = (hash >> self.bits_for_hll) as usize;
        debug_assert!(stream_idx < self.m as usize);
        let sigma = HyperLogLog::get_sigma(hash, self.bits_for_hll);
        let counter = unsafe { self.counters.get_unchecked_mut(stream_idx) };
        *counter = ::std::cmp::max(*counter, sigma);
    }

    /// Computes the distinct count approximation from the HyperLogLog internal state.
    ///
    /// This method analyzes the internal counters to estimate the number of distinct
    /// elements that have been added to the set. The algorithm automatically chooses
    /// between HyperLogLog estimation and LinearCounting based on the estimated value
    /// and configured thresholds to optimize accuracy across different cardinality ranges.
    ///
    /// # Returns
    ///
    /// The estimated number of distinct elements as a `usize`. The accuracy depends
    /// on the `bits_per_index` configuration used when creating the HyperLogLog.
    ///
    /// # Algorithm Details
    ///
    /// - Uses bias correction for estimates ≤ 5m (where m is the number of buckets)
    /// - Switches to LinearCounting for small cardinalities (below algorithm selection threshold)
    /// - Applies the standard HyperLogLog formula for larger cardinalities
    ///
    /// # Example
    ///
    /// ```
    /// use amudai_hll::hll::HyperLogLog;
    /// use std::collections::hash_map::DefaultHasher;
    /// use std::hash::{Hash, Hasher};
    ///
    /// let mut hll = HyperLogLog::new(1);
    ///
    /// // Add 1000 distinct elements
    /// for i in 0..1000 {
    ///     let mut hasher = DefaultHasher::new();
    ///     i.hash(&mut hasher);
    ///     hll.add_hash(hasher.finish());
    /// }
    ///
    /// let estimate = hll.estimate_count();
    /// println!("Estimated cardinality: {}", estimate);
    /// // Should be close to 1000, within the expected error range
    /// ```
    pub fn estimate_count(&self) -> usize {
        let mut z_inverse = 0f64;
        let mut v = 0f64;
        let m = self.m as f64;

        for &sigma in self.counters.iter() {
            z_inverse += 1.0 / ((1u64 << (sigma as u64)) as f64);
            if sigma == 0 {
                v += 1.0;
            }
        }

        let mut e = self.alpha_m * m * m / z_inverse;

        if e <= 5.0 * (self.m as f64) {
            e = bias::correct_bias(e, self.bits_per_index);
        }

        let h = if v > 0.0 { m * (m / v).ln() } else { e };

        if h <= self.algorithm_selection_threshold {
            h.round() as usize
        } else {
            e.round() as usize
        }
    }

    /// Serializes the HyperLogLog state to a writer.
    ///
    /// This method writes the complete HyperLogLog state to any type that implements
    /// the `Write` trait, allowing for persistence to files, network streams, or
    /// in-memory buffers.
    ///
    /// # Arguments
    ///
    /// * `w` - A mutable reference to any writer that implements `Write`
    ///
    /// # Returns
    ///
    /// Returns `Ok(usize)` with the number of bytes written on success, or an
    /// `Err(io::Error)` if the write operation fails.
    ///
    /// # Format
    ///
    /// The serialized format includes:
    /// - Magic version number for format validation
    /// - Configuration parameters (bits_per_index, bits_for_hll, etc.)
    /// - Complete counter array
    ///
    /// # Example
    ///
    /// ```
    /// use amudai_hll::hll::HyperLogLog;
    /// use std::io::Cursor;
    ///
    /// let hll = HyperLogLog::new(1);
    /// let mut buffer = Vec::new();
    /// let bytes_written = hll.pack_to_writer(&mut buffer)
    ///     .expect("Writing to Vec should not fail");
    /// println!("Serialized {} bytes", bytes_written);
    /// ```
    pub fn pack_to_writer<W>(&self, w: &mut W) -> io::Result<usize>
    where
        W: Write + ?Sized,
    {
        w.write_u32::<LE>(Self::HLL_FORMAT_MAGIC_VERSION)?;
        w.write_u32::<LE>(self.bits_per_index)?;
        w.write_u32::<LE>(self.bits_for_hll)?;
        w.write_u32::<LE>(self.m)?;
        w.write_f64::<LE>(self.alpha_m)?;
        w.write_f64::<LE>(self.algorithm_selection_threshold)?;
        w.write_u32::<LE>(self.counters.len() as u32)?;
        w.write_all(self.counters.as_ref())?;
        Ok(4 * 4 + 8 * 2 + 4 + self.counters.len())
    }

    /// Serializes the HyperLogLog state to a byte vector.
    ///
    /// This is a convenience method that creates a new `Vec<u8>` and writes the
    /// HyperLogLog state to it. This is useful when you need the serialized data
    /// as a owned byte vector.
    ///
    /// # Returns
    ///
    /// A `Vec<u8>` containing the complete serialized HyperLogLog state.
    ///
    /// # Panics
    ///
    /// This method should not panic under normal circumstances since writing to
    /// a `Vec<u8>` is infallible.
    ///
    /// # Example
    ///
    /// ```
    /// use amudai_hll::hll::HyperLogLog;
    ///
    /// let hll = HyperLogLog::new(1);
    /// let serialized = hll.pack_to_vec();
    /// println!("Serialized to {} bytes", serialized.len());
    ///
    /// // Can be later restored with unpack_from_slice
    /// let restored = HyperLogLog::unpack_from_slice(&serialized)
    ///     .expect("Should be able to deserialize");
    /// ```
    pub fn pack_to_vec(&self) -> Vec<u8> {
        let mut v = Vec::with_capacity(self.counters.len() + 36);
        self.pack_to_writer(&mut v)
            .expect("Packing to a vec should not fail");
        v
    }

    /// Deserializes a HyperLogLog from a reader.
    ///
    /// This method reconstructs a HyperLogLog instance from data that was previously
    /// serialized using `pack_to_writer`. It can read from any source that implements
    /// the `Read` trait.
    ///
    /// # Arguments
    ///
    /// * `r` - A mutable reference to any reader that implements `Read`
    ///
    /// # Returns
    ///
    /// Returns `Ok(HyperLogLog)` with the restored instance on success, or an
    /// `Err(io::Error)` if deserialization fails.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The data format is invalid (wrong magic number)
    /// - The configuration parameters are out of valid ranges
    /// - The counter array size is invalid
    /// - The read operation fails
    ///
    /// # Example
    ///
    /// ```
    /// use amudai_hll::hll::HyperLogLog;
    /// use std::io::Cursor;
    ///
    /// // Serialize an HLL
    /// let original = HyperLogLog::new(1);
    /// let data = original.pack_to_vec();
    ///
    /// // Deserialize from the data
    /// let mut cursor = Cursor::new(data);
    /// let restored = HyperLogLog::unpack_from_reader(&mut cursor)
    ///     .expect("Should be able to deserialize");
    /// ```
    pub fn unpack_from_reader<R>(r: &mut R) -> io::Result<HyperLogLog>
    where
        R: Read + ?Sized,
    {
        let magic = r.read_u32::<LE>()?;
        if magic != Self::HLL_FORMAT_MAGIC_VERSION {
            return Err(invalid_arg_err("Invalid HLL binary format (signature)"));
        }

        let bits_per_index = r.read_u32::<LE>()?;
        if bits_per_index > 64 {
            return Err(invalid_arg_err(
                "Invalid HLL binary format (bits_per_index)",
            ));
        }
        let bits_for_hll = r.read_u32::<LE>()?;
        if bits_for_hll > 64 {
            return Err(invalid_arg_err("Invalid HLL binary format (bits_for_hll)"));
        }
        let m = r.read_u32::<LE>()?;
        let alpha_m = r.read_f64::<LE>()?;
        let algorithm_selection_threshold = r.read_f64::<LE>()?;
        let counters_len = r.read_u32::<LE>()? as usize;
        if counters_len > 16 * 1024 * 1024 || counters_len == 0 {
            return Err(invalid_arg_err("Invalid HLL binary format (counters_len)"));
        }
        let mut counters = vec![0u8; counters_len];
        r.read_exact(&mut counters)?;

        Ok(HyperLogLog {
            bits_per_index,
            bits_for_hll,
            m,
            alpha_m,
            algorithm_selection_threshold,
            counters: counters.into_boxed_slice(),
        })
    }

    /// Deserializes a HyperLogLog from a byte slice.
    ///
    /// This is a convenience method that creates a cursor from the provided byte
    /// slice and deserializes the HyperLogLog from it. This is useful when you
    /// have the serialized data as a byte array.
    ///
    /// # Arguments
    ///
    /// * `s` - A byte slice containing serialized HyperLogLog data
    ///
    /// # Returns
    ///
    /// Returns `Ok(HyperLogLog)` with the restored instance on success, or an
    /// `Err(io::Error)` if deserialization fails.
    ///
    /// # Errors
    ///
    /// Same error conditions as `unpack_from_reader`.
    ///
    /// # Example
    ///
    /// ```
    /// use amudai_hll::hll::HyperLogLog;
    ///
    /// // Create and serialize an HLL
    /// let original = HyperLogLog::new(1);
    /// let data = original.pack_to_vec();
    ///
    /// // Deserialize from the byte slice
    /// let restored = HyperLogLog::unpack_from_slice(&data)
    ///     .expect("Should be able to deserialize");
    /// ```
    pub fn unpack_from_slice(s: &[u8]) -> io::Result<HyperLogLog> {
        Self::unpack_from_reader(&mut io::Cursor::new(s))
    }

    #[inline]
    fn get_sigma(hash: u64, bits_to_count: u32) -> u8 {
        let significant_hash_bits = hash & ((1u64 << (bits_to_count as u64)) - 1);
        if significant_hash_bits == 0 {
            bits_to_count as u8 + 1
        } else {
            let highest_set_bit = 63 - significant_hash_bits.leading_zeros();
            (bits_to_count - highest_set_bit) as u8
        }
    }

    /// Computes the HLL lookup table size (2^bits_per_index).
    pub fn get_hll_lookup_table_size(bits_per_index: u32) -> u32 {
        1u32 << bits_per_index
    }

    /// Computes the alpha_m bias correction factor.
    pub fn get_alpha_m(m: u32) -> f64 {
        match m {
            16 => 0.673,
            32 => 0.697,
            64 => 0.709,
            _ => 0.7213 / (1.0 + 1.079 / m as f64),
        }
    }

    /// Computes the algorithm selection threshold for LinearCounting vs HyperLogLog.
    pub fn get_algorithm_selection_threshold(bits_per_index: u32) -> f64 {
        match bits_per_index {
            4 => 10f64,
            5 => 20f64,
            6 => 40f64,
            7 => 80f64,
            8 => 220f64,
            9 => 400f64,
            10 => 900f64,
            11 => 1800f64,
            12 => 3100f64,
            13 => 6500f64,
            14 => 11500f64,
            15 => 20000f64,
            16 => 50000f64,
            17 => 120000f64,
            18 => 350000f64,
            _ => panic!("HyperLogLog::get_algorithm_selection_threshold: invalid number of bits"),
        }
    }

    /// Returns a reference to the internal counter array.
    ///
    /// This method provides read-only access to the raw counter values that
    /// make up the HyperLogLog's internal state. Each counter represents the
    /// maximum number of leading zeros observed for elements that hash to
    /// that particular sub-stream.
    ///
    /// # Returns
    ///
    /// A slice of bytes representing the counter array. The length of this
    /// slice is 2^bits_per_index.
    ///
    /// # Use Cases
    ///
    /// This is primarily useful for:
    /// - Advanced analysis of the HyperLogLog state
    /// - Custom serialization implementations
    /// - Debugging and testing
    /// - Integration with external systems that need raw counter access
    ///
    /// # Example
    ///
    /// ```
    /// use amudai_hll::hll::HyperLogLog;
    ///
    /// let hll = HyperLogLog::new(1);  // Uses 14 bits, so 2^14 = 16384 counters
    /// let counters = hll.get_counters();
    /// println!("HyperLogLog has {} counters", counters.len());
    /// ```
    pub fn get_counters(&self) -> &[u8] {
        self.counters.as_ref()
    }
}

fn invalid_arg_err(msg: &str) -> ::std::io::Error {
    use std::io::ErrorKind;
    Error::new(ErrorKind::InvalidInput, msg.to_string())
}

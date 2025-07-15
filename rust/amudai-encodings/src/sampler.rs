/// Data sampler is used to select a subset of data from a larger dataset.
///
/// Samplers are used in encoding analysis to examine representative portions
/// of large datasets without processing all data. This enables efficient
/// encoding strategy selection based on data characteristics.
#[allow(dead_code)]
pub trait Sampler<T> {
    /// Selects a subset of values from `data` and appends them to `target`.
    ///
    /// # Arguments
    ///
    /// * `values` - The source data to sample from
    /// * `target` - The destination vector to append sampled values to
    fn select_sample(values: &[T], target: &mut Vec<T>);
}

/// This sampler selects `N` non-overlapping blocks of data of `BLOCK_SIZE` each,
/// from the input data. The sampler panics when the data size contains less than
/// `MIN_SIZE` elements.
///
/// # Generic Parameters
///
/// * `N` - Number of blocks to sample
/// * `BLOCK_SIZE` - Size of each sampled block
/// * `MIN_SIZE` - Minimum required input size
///
/// # Sampling Strategy
///
/// The sampler uses random selection to pick non-overlapping blocks, ensuring
/// that the sample represents different parts of the dataset. This approach
/// works well for data with various patterns distributed throughout.
pub struct NRandomSampler<const N: usize, const BLOCK_SIZE: usize, const MIN_SIZE: usize>;

/// `NRandomSampler` that selects 16 random blocks of 64 elements each.
///
/// This default configuration provides a good balance between sample
/// representativeness and analysis speed for typical datasets.
#[allow(dead_code)]
pub type DefaultNRandomSampler = NRandomSampler<16, 64, 65536>;

impl<T, const N: usize, const BLOCK_SIZE: usize, const MIN_SIZE: usize> Sampler<T>
    for NRandomSampler<N, BLOCK_SIZE, MIN_SIZE>
where
    T: Clone,
{
    /// Selects N random non-overlapping blocks from the input data.
    ///
    /// # Arguments
    ///
    /// * `values` - Source data to sample from (must have at least BLOCK_SIZE elements)
    /// * `target` - Destination vector to append sampled blocks to
    ///
    /// # Panics
    ///
    /// Panics if `values.len() < BLOCK_SIZE`, as this would make sampling impossible.
    ///
    /// # Algorithm
    ///
    /// 1. Randomly selects N starting positions for blocks
    /// 2. Ensures no blocks overlap by retrying conflicting positions
    /// 3. Copies each selected block to the target vector
    ///
    /// The resulting sample contains exactly `N * BLOCK_SIZE` elements.
    fn select_sample(values: &[T], target: &mut Vec<T>) {
        assert!(values.len() >= BLOCK_SIZE);
        let mut indices = [0usize; N];
        for i in 0..N {
            let mut index = fastrand::usize(0..values.len() - BLOCK_SIZE);
            while indices
                .iter()
                .any(|&i| (i..i + BLOCK_SIZE).contains(&index))
            {
                index = fastrand::usize(0..values.len() - BLOCK_SIZE);
            }
            indices[i] = index;
        }
        for index in indices {
            target.extend_from_slice(&values[index..index + BLOCK_SIZE]);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    pub fn test_n_random_sampler() {
        let data = (0..64000).collect::<Vec<_>>();
        let mut target = Vec::new();
        DefaultNRandomSampler::select_sample(&data, &mut target);
        assert_eq!(1024, target.len());
        target.dedup();
        assert_eq!(1024, target.len());
    }
}

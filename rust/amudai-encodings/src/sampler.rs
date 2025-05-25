/// Data sampler is used to select a subset of data from a larger dataset.
#[allow(dead_code)]
pub trait Sampler<T> {
    /// Selects a subset of values from `data` and appends them to `target`.
    fn select_sample(values: &[T], target: &mut Vec<T>);
}

/// This sampler selects `N` non-overlaping blocks of data of `BLOCK_SIZE` each,
/// from the input data. The sampler panics when the data size contains less than
/// `MIN_SIZE` elements.
pub struct NRandomSampler<const N: usize, const BLOCK_SIZE: usize, const MIN_SIZE: usize>;

// `NRandomSampler` that selects 16 random blocks of 64 elements each.
#[allow(dead_code)]
pub type DefaultNRandomSampler = NRandomSampler<16, 64, 65536>;

impl<T, const N: usize, const BLOCK_SIZE: usize, const MIN_SIZE: usize> Sampler<T>
    for NRandomSampler<N, BLOCK_SIZE, MIN_SIZE>
where
    T: Clone,
{
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

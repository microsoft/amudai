use amudai_common::{Result, verify_arg};

use crate::defs::schema::{HashLookup, HashLookupRef};

impl HashLookup {
    /// Given `count` items and a hash function `hash_fn` where `hash_fn(i)` returns a hash value
    /// for item `i`, builds a `HashLookup` structure such that:
    ///
    /// - The number of logical buckets is a power of 2, and the length of the buckets array
    ///   is `num_buckets + 1`.
    /// - The number of entries is `count`.
    /// - For a given item `i`, the bucket index `b` is `hash_fn(i) % (buckets.len() - 1)`.
    /// - The range of relevant entries is `buckets[b]..buckets[b + 1]`.
    /// - Each of the entries in the range `entries[buckets[b]..buckets[b + 1]]` contains an index
    ///   of the item (`i`), where the bucket index for the item `i` is `b`.
    pub fn build(count: usize, hash_fn: impl Fn(usize) -> u64) -> HashLookup {
        assert!(count < (u32::MAX / 4) as usize);
        let num_buckets = (count.max(4) / 2).next_power_of_two();
        let mask = num_buckets as u64 - 1;

        let mut buckets = vec![0u32; num_buckets + 1];
        let mut counters = vec![0u32; num_buckets];

        for i in 0..count {
            let bucket_idx = (hash_fn(i) & mask) as usize;
            counters[bucket_idx] += 1;
        }

        let mut next_entry = 0;
        for i in 0..num_buckets {
            buckets[i] = next_entry;
            next_entry += counters[i];
        }
        assert_eq!(next_entry as usize, count);
        buckets[num_buckets] = next_entry;

        let mut entries = vec![0u32; count];
        for i in 0..count {
            let bucket_idx = (hash_fn(i) & mask) as usize;
            let entry_idx = buckets[bucket_idx] as usize;
            entries[entry_idx] = i as u32;
            buckets[bucket_idx] += 1;
        }
        assert_eq!(buckets[num_buckets - 1], count as u32);

        for i in (0..num_buckets).rev() {
            buckets[i + 1] = buckets[i];
        }
        buckets[0] = 0;

        HashLookup { buckets, entries }
    }
}

impl HashLookupRef<'_> {
    pub fn find(
        &self,
        hash: u64,
        verifier_fn: impl Fn(usize) -> Result<bool>,
    ) -> Result<Option<usize>> {
        let buckets = self.buckets()?;
        verify_arg!(buckets, (buckets.len() - 1).is_power_of_two());
        let entries = self.entries()?;

        let mask = (buckets.len() - 2) as u64;
        let bucket_idx = (hash & mask) as usize;

        let start = buckets.get(bucket_idx).expect("valid bucket_idx") as usize;
        let end = buckets.get(bucket_idx + 1).expect("valid bucket_idx") as usize;
        verify_arg!(start, start <= end);
        verify_arg!(end, end <= entries.len());

        for entry_idx in start..end {
            let i = entries.get(entry_idx).expect("valid entry_idx") as usize;
            if verifier_fn(i)? {
                return Ok(Some(i));
            }
        }
        Ok(None)
    }
}

pub fn hash_field_name(name: &str) -> u64 {
    xxhash_rust::xxh3::xxh3_64(name.as_bytes())
}

#[cfg(test)]
mod tests {
    use planus::ReadAsRoot;

    use crate::defs::schema::{HashLookup, HashLookupRef};

    #[test]
    fn test_hash_lookup() {
        let numbers = (0..1000u64).map(|i| hash(i * 10)).collect::<Vec<_>>();
        let lookup = HashLookup::build(numbers.len(), |i| hash(numbers[i]));
        let lookup_bytes = planus::Builder::new().finish(&lookup, None).to_vec();
        let lookup = HashLookupRef::read_as_root(&lookup_bytes).unwrap();

        for &n in &numbers {
            let h = hash(n);
            let res = lookup.find(h, |i| Ok(numbers[i] == n)).unwrap();
            assert!(res.is_some());
            assert_eq!(numbers[res.unwrap()], n);
        }

        for n in 0..10000 {
            let h = hash(n);
            let res = lookup.find(h, |i| Ok(numbers[i] == n)).unwrap();
            assert!(res.is_none());
        }
    }

    fn hash(n: u64) -> u64 {
        xxhash_rust::xxh3::xxh3_64(&n.to_le_bytes())
    }
}

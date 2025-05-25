use super::BinaryValuesSequence;
use crate::encodings::{EncodingKind, NullMask};
use bitflags::bitflags;
use std::{collections::HashMap, hash::Hash};

bitflags! {
    /// Flags to specify which statistics to collect.
    #[derive(Clone, Copy)]
    pub struct BinaryStatsCollectorFlags: u32 {
        const MIN_MAX_LEN = 1;
        const UNIQUE_VALUES = 2;
    }
}

/// Statistics collected from a sequence of values.
#[derive(Default)]
pub struct BinaryStats {
    /// Size of the data in bytes including offsets.
    pub original_size: u32,
    /// Number of values in the data including null values.
    pub values_count: u32,
    /// Number of nulls in the data.
    pub nulls_count: u32,
    /// Unique values with the number of occurrences.
    pub unique_values: Option<HashMap<Vec<u8>, u32>>,
    /// Minimal value length in bytes.
    #[allow(dead_code)]
    pub min_len: Option<u32>,
    /// Maximal value length in bytes.
    #[allow(dead_code)]
    pub max_len: Option<u32>,
}

impl BinaryStats {
    /// If the unique values count is known, returns whether there's single value
    /// in the data.
    pub fn is_single_value(&self) -> bool {
        self.unique_values
            .as_ref()
            .is_some_and(|unique_values| unique_values.len() == 1)
    }

    /// Global encodings prefilter based on the statistics.
    pub fn is_encoding_allowed(&self, encoding: EncodingKind) -> bool {
        if self.is_single_value() {
            return encoding == EncodingKind::SingleValue;
        }
        true
    }
}

/// Collects statistics from sequence of numeric values, as specified
/// by `StringStatsCollectorFlags`.
pub struct BinaryStatsCollector;

impl BinaryStatsCollector {
    pub fn new() -> Self {
        Self
    }

    pub fn collect(
        &self,
        values: &BinaryValuesSequence,
        null_mask: &NullMask,
        mut flags: BinaryStatsCollectorFlags,
    ) -> BinaryStats {
        let values_count = values.len();
        assert!(values_count > 0);

        let mut min_len: Option<u32> = None;
        let mut max_len: Option<u32> = None;
        let mut values_counts = HashMap::<&[u8], u32>::new();

        for i in 0..values_count {
            if !null_mask.is_null(i) {
                let value = values.get(i);

                if flags.contains(BinaryStatsCollectorFlags::MIN_MAX_LEN) {
                    if let Some(min) = min_len {
                        min_len = Some(min.min(value.len() as _));
                    } else {
                        min_len = Some(value.len() as _);
                    }
                    if let Some(max) = max_len {
                        max_len = Some(max.max(value.len() as _));
                    } else {
                        max_len = Some(value.len() as _);
                    }
                }

                if flags.contains(BinaryStatsCollectorFlags::UNIQUE_VALUES) {
                    *values_counts.entry(value).or_default() += 1;
                    if values_counts.len() > values_count / 2 {
                        // Stop collecting unique values if there are too many of them.
                        flags.remove(BinaryStatsCollectorFlags::UNIQUE_VALUES);
                        values_counts.clear();
                    }
                }
            }
        }

        let unique_values = if flags.contains(BinaryStatsCollectorFlags::UNIQUE_VALUES) {
            Some(
                values_counts
                    .into_iter()
                    .map(|(k, v)| (k.to_vec(), v))
                    .collect(),
            )
        } else {
            None
        };

        BinaryStats {
            original_size: values.data_size(),
            values_count: values_count as u32,
            nulls_count: null_mask.null_count() as _,
            unique_values,
            min_len,
            max_len,
        }
    }
}

//! Extension traits for HllSketch to provide additional functionality
//! needed by the data-stats crate.

use amudai_format::defs::shard::{CardinalityInfo, HllSketchV1};
use amudai_hll::HllSketch;

/// Extension trait for HllSketch to add methods needed by data-stats collectors.
pub(crate) trait SketchBuilderExt {
    /// Converts to protobuf CardinalityInfo format with HllSketchV1.
    fn to_cardinality_info(&self) -> CardinalityInfo;

    /// Convert HLL sketch to protobuf HllSketchV1 format
    fn to_hll_sketch_v1(&self) -> HllSketchV1;
}

impl SketchBuilderExt for HllSketch {
    fn to_cardinality_info(&self) -> CardinalityInfo {
        CardinalityInfo {
            count: Some(self.estimate_count() as u64),
            is_estimate: true,
            hll_sketch: Some(self.to_hll_sketch_v1()),
        }
    }

    fn to_hll_sketch_v1(&self) -> HllSketchV1 {
        let config = self.get_config();
        HllSketchV1 {
            hash_algorithm: config.hash_algorithm,
            bits_per_index: config.bits_per_index,
            hash_seed: config.hash_seed,
            counters: self.get_counters().to_vec(),
        }
    }
}

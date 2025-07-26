//! Extension methods for shard-related format definitions.
//!
//! This module provides utility methods and implementations for shard format structures,
//! offering convenient ways to inspect and work with shard properties and metadata.

use crate::defs::shard::ShardProperties;

impl ShardProperties {
    /// Returns `true` if the shard properties contain no data.
    ///
    /// A shard properties instance is considered empty when:
    /// - No creation time range is set (both `creation_min` and `creation_max` are `None`)
    /// - No standard properties are defined
    /// - No custom properties are defined
    pub fn is_empty(&self) -> bool {
        self.creation_min.is_none()
            && self.creation_max.is_none()
            && self.standard_properties.is_empty()
            && self.custom_properties.is_empty()
    }
}

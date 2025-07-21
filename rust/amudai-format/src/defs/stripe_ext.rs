//! Extension methods for stripe-related format definitions.
//!
//! This module provides utility methods and implementations for stripe format structures,
//! offering convenient ways to inspect and work with stripe properties and metadata.

use crate::defs::shard::StripeProperties;

impl StripeProperties {
    /// Returns `true` if the stripe properties contain no data.
    ///
    /// A stripe properties instance is considered empty when:
    /// - No standard properties are defined
    /// - No custom properties are defined
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use amudai_format::defs::shard::StripeProperties;
    /// let properties = StripeProperties::default();
    /// assert!(properties.is_empty());
    /// ```
    pub fn is_empty(&self) -> bool {
        self.standard_properties.is_empty() && self.custom_properties.is_empty()
    }
}

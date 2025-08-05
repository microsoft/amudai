use amudai_format::{
    defs::{
        common::DateTimeUtc,
        shard::{FieldDescriptor, ShardProperties, StripeProperties},
    },
    property_bag::PropertyBagBuilder,
};

/// A builder for constructing shard-level properties and metadata.
///
/// `ShardPropertiesBuilder` provides a convenient way to build shard properties
/// that include both standard metadata (creation timestamps) and custom properties.
/// It delegates custom property operations to an internal `PropertyBagBuilder`
/// through `Deref` and `DerefMut` implementations.
///
/// # Standard Properties
///
/// Shard standard properties include:
/// - Creation time range (minimum and maximum timestamps)
/// - Standard property name-value pairs (accessed via dedicated methods)
///
/// # Custom Properties
///
/// Custom properties can be set directly on the builder instance through the
/// delegated `PropertyBagBuilder` interface.
#[derive(Debug, Clone, Default)]
pub struct ShardPropertiesBuilder {
    standard: PropertyBagBuilder,
    custom: PropertyBagBuilder,
    creation_min: Option<DateTimeUtc>,
    creation_max: Option<DateTimeUtc>,
}

impl ShardPropertiesBuilder {
    /// Creates a new empty `ShardPropertiesBuilder`.
    ///
    /// # Returns
    ///
    /// A new builder instance ready to accept property assignments and metadata.
    pub fn new() -> ShardPropertiesBuilder {
        ShardPropertiesBuilder {
            standard: PropertyBagBuilder::new(),
            custom: PropertyBagBuilder::new(),
            creation_min: None,
            creation_max: None,
        }
    }

    /// Sets both the minimum and maximum creation time to the same value.
    ///
    /// This is convenient when all data in the shard has the same timestamp.
    ///
    /// # Arguments
    ///
    /// * `utc_ticks` - The creation timestamp in 100-ns UTC ticks since 0001-01-01
    pub fn set_creation_time(&mut self, utc_ticks: u64) {
        self.set_creation_range(utc_ticks, utc_ticks);
    }

    /// Sets the creation time range for the shard.
    ///
    /// This establishes the time bounds for data contained within the shard,
    /// which can be useful for time-based queries and partitioning.
    ///
    /// # Arguments
    ///
    /// * `utc_ticks_min` - The minimum creation timestamp in ticks
    /// * `utc_ticks_max` - The maximum creation timestamp in ticks
    pub fn set_creation_range(&mut self, utc_ticks_min: u64, utc_ticks_max: u64) {
        self.creation_min = Some(DateTimeUtc {
            ticks: utc_ticks_min,
        });
        self.creation_max = Some(DateTimeUtc {
            ticks: utc_ticks_max,
        });
    }

    /// Returns the current minimum creation timestamp, if set.
    ///
    /// # Returns
    ///
    /// `Some(DateTimeUtc)` if a minimum creation time has been set, `None` otherwise.
    pub fn creation_min(&self) -> Option<DateTimeUtc> {
        self.creation_min
    }

    /// Returns the current maximum creation timestamp, if set.
    ///
    /// # Returns
    ///
    /// `Some(DateTimeUtc)` if a maximum creation time has been set, `None` otherwise.
    pub fn creation_max(&self) -> Option<DateTimeUtc> {
        self.creation_max
    }

    /// Consumes the builder and returns the constructed shard properties.
    ///
    /// This method converts all stored properties and metadata into a
    /// `ShardProperties` protobuf structure suitable for serialization.
    ///
    /// # Returns
    ///
    /// A `ShardProperties` containing all properties and metadata.
    pub fn finish(self) -> ShardProperties {
        ShardProperties {
            creation_min: self.creation_min,
            creation_max: self.creation_max,
            standard_properties: self.standard.finish(),
            custom_properties: self.custom.finish(),
        }
    }
}

impl std::ops::Deref for ShardPropertiesBuilder {
    type Target = PropertyBagBuilder;

    fn deref(&self) -> &Self::Target {
        &self.custom
    }
}

impl std::ops::DerefMut for ShardPropertiesBuilder {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.custom
    }
}

/// A builder for constructing stripe-level properties.
///
/// `StripePropertiesBuilder` provides a convenient way to build stripe properties
/// that include both standard metadata and custom properties. It delegates custom
/// property operations to an internal `PropertyBagBuilder` through `Deref` and
/// `DerefMut` implementations.
///
/// # Standard Properties
///
/// Stripe standard properties include standard property name-value pairs that
/// are accessed through dedicated methods.
///
/// # Custom Properties
///
/// Custom properties can be set directly on the builder instance through the
/// delegated `PropertyBagBuilder` interface.
#[derive(Debug, Clone, Default)]
pub struct StripePropertiesBuilder {
    standard: PropertyBagBuilder,
    custom: PropertyBagBuilder,
}

impl StripePropertiesBuilder {
    /// Creates a new empty `StripePropertiesBuilder`.
    ///
    /// # Returns
    ///
    /// A new builder instance ready to accept property assignments.
    pub fn new() -> StripePropertiesBuilder {
        StripePropertiesBuilder {
            standard: PropertyBagBuilder::new(),
            custom: PropertyBagBuilder::new(),
        }
    }

    /// Consumes the builder and returns the constructed stripe properties.
    ///
    /// This method converts all stored properties into a `StripeProperties`
    /// protobuf structure suitable for serialization.
    ///
    /// # Returns
    ///
    /// A `StripeProperties` containing all properties.
    pub fn finish(self) -> StripeProperties {
        StripeProperties {
            standard_properties: self.standard.finish(),
            custom_properties: self.custom.finish(),
        }
    }
}

impl std::ops::Deref for StripePropertiesBuilder {
    type Target = PropertyBagBuilder;

    fn deref(&self) -> &Self::Target {
        &self.custom
    }
}

impl std::ops::DerefMut for StripePropertiesBuilder {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.custom
    }
}

/// A builder for constructing field-level properties.
///
/// `FieldPropertiesBuilder` provides a convenient way to build field properties
/// that include both standard metadata and custom properties. It delegates custom
/// property operations to an internal `PropertyBagBuilder` through `Deref` and
/// `DerefMut` implementations.
///
/// Unlike other property builders, `FieldPropertiesBuilder` uses `finish_to()`
/// instead of `finish()` to directly populate a `FieldDescriptor` structure.
///
/// # Standard Properties
///
/// Field standard properties include standard property name-value pairs that
/// are accessed through dedicated methods.
///
/// # Custom Properties
///
/// Custom properties can be set directly on the builder instance through the
/// delegated `PropertyBagBuilder` interface.
#[derive(Debug, Clone, Default)]
pub struct FieldPropertiesBuilder {
    standard: PropertyBagBuilder,
    custom: PropertyBagBuilder,
}

impl FieldPropertiesBuilder {
    /// Creates a new empty `FieldPropertiesBuilder`.
    ///
    /// # Returns
    ///
    /// A new builder instance ready to accept property assignments.
    pub fn new() -> FieldPropertiesBuilder {
        FieldPropertiesBuilder {
            standard: PropertyBagBuilder::new(),
            custom: PropertyBagBuilder::new(),
        }
    }

    /// Consumes the builder and populates the given field descriptor with properties.
    ///
    /// This method transfers all stored properties into the provided `FieldDescriptor`,
    /// setting both the standard and custom property collections.
    ///
    /// # Arguments
    ///
    /// * `descriptor` - The field descriptor to populate with properties
    pub fn finish_to(self, descriptor: &mut FieldDescriptor) {
        descriptor.standard_properties = self.standard.finish();
        descriptor.custom_properties = self.custom.finish();
    }
}

impl std::ops::Deref for FieldPropertiesBuilder {
    type Target = PropertyBagBuilder;

    fn deref(&self) -> &Self::Target {
        &self.custom
    }
}

impl std::ops::DerefMut for FieldPropertiesBuilder {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.custom
    }
}

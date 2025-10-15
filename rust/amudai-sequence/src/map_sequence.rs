//! /// A sequence representing `Map` field data with key-value pairs.

use std::borrow::Cow;

use amudai_common::{Result, verify_arg};
use amudai_format::{
    projection::TypeProjectionRef,
    schema::{BasicType, BasicTypeDescriptor},
};

use crate::{
    offsets::Offsets,
    presence::Presence,
    sequence::{Sequence, ValueSequence},
};

/// A sequence representing `Map` field data with key-value pairs.
///
/// `MapSequence` stores a collection of maps where each map contains zero or more
/// key-value pairs. The keys and values are stored flattened in separate child
/// sequences, with offsets defining the boundaries of each map.
pub struct MapSequence {
    /// /// DataType slice for this sequence and its child `key` and `value` sequences.
    pub data_type: Option<TypeProjectionRef>,
    /// Flattened keys
    pub key: Option<Box<dyn Sequence>>,
    /// Flattened values
    pub value: Option<Box<dyn Sequence>>,
    /// Offsets into `item` sequence.
    pub offsets: Offsets,
    /// Presence of the list value.
    pub presence: Presence,
}

impl MapSequence {
    /// Creates a new `MapSequence` with validation.
    ///
    /// This is a convenience wrapper around [`try_new`](Self::try_new) that panics on validation errors.
    pub fn new(
        data_type: Option<TypeProjectionRef>,
        key: Option<Box<dyn Sequence>>,
        value: Option<Box<dyn Sequence>>,
        offsets: Offsets,
        presence: Presence,
    ) -> MapSequence {
        Self::try_new(data_type, key, value, offsets, presence).expect("try_new")
    }

    pub fn empty() -> MapSequence {
        MapSequence {
            data_type: None,
            key: None,
            value: None,
            offsets: Offsets::new(),
            presence: Presence::Trivial(0),
        }
    }

    /// Creates a new `MapSequence` with validation.
    ///
    /// Validates that:
    /// - The key header's basic type matches the key sequence's basic type
    /// - The value header's basic type matches the value sequence's basic type
    /// - The presence length equals the number of maps (offset item count)
    /// - The offsets are within bounds of the key and value sequences
    /// - The key and value sequences have the same length
    ///
    /// # Errors
    ///
    /// Returns an error if any validation checks fail.
    pub fn try_new(
        data_type: Option<TypeProjectionRef>,
        key: Option<Box<dyn Sequence>>,
        value: Option<Box<dyn Sequence>>,
        offsets: Offsets,
        presence: Presence,
    ) -> Result<MapSequence> {
        verify_arg!(presence, presence.len() == offsets.item_count());
        Ok(MapSequence {
            data_type,
            key,
            value,
            offsets,
            presence,
        })
    }
}

impl Clone for MapSequence {
    fn clone(&self) -> Self {
        Self {
            data_type: self.data_type.clone(),
            key: self.key.as_ref().map(|key| key.clone_boxed()),
            value: self.value.as_ref().map(|value| value.clone_boxed()),
            offsets: self.offsets.clone(),
            presence: self.presence.clone(),
        }
    }
}

impl Sequence for MapSequence {
    fn as_any(&self) -> &(dyn std::any::Any + Send + Sync + 'static) {
        self
    }

    fn into_any(self: Box<Self>) -> Box<dyn std::any::Any + Send + Sync + 'static> {
        self
    }

    fn type_id(&self) -> std::any::TypeId {
        std::any::TypeId::of::<Self>()
    }

    fn clone_boxed(&self) -> Box<dyn Sequence> {
        Box::new(self.clone())
    }

    fn basic_type(&self) -> BasicTypeDescriptor {
        BasicTypeDescriptor {
            basic_type: BasicType::Map,
            ..Default::default()
        }
    }

    fn len(&self) -> usize {
        self.presence.len()
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn to_value_sequence(&self) -> Cow<'_, ValueSequence> {
        let values = self.offsets.clone().into_inner();
        Cow::Owned(ValueSequence {
            values,
            offsets: None,
            presence: self.presence.clone(),
            type_desc: self.basic_type(),
        })
    }
}

//! A sequence representing `List` field data with lists of variable-length elements.

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

/// A sequence representing `List` field data with lists of variable-length elements.
///
/// `ListSequence` stores a collection of lists where each list contains zero or more
/// elements of the same type. The elements are stored flattened in a single child
/// sequence, with offsets defining the boundaries of each list.
pub struct ListSequence {
    /// /// Schema sub-tree rooted at this sequence: DataType for this sequence
    /// and its child `item`.
    pub data_type: Option<TypeProjectionRef>,
    /// Flattened list of elements.
    pub item: Option<Box<dyn Sequence>>,
    /// Offsets into the `item` sequence.
    pub offsets: Offsets,
    /// Presence of the list value.
    pub presence: Presence,
}

impl ListSequence {
    /// Creates a new `ListSequence` with validation.
    ///
    /// This is a convenience wrapper around [`try_new`](Self::try_new) that panics on validation errors.
    pub fn new(
        data_type: Option<TypeProjectionRef>,
        item: Option<Box<dyn Sequence>>,
        offsets: Offsets,
        presence: Presence,
    ) -> ListSequence {
        Self::try_new(data_type, item, offsets, presence).expect("try_new")
    }

    /// Creates an empty `ListSequence`.
    pub fn empty() -> ListSequence {
        ListSequence {
            data_type: None,
            item: None,
            offsets: Offsets::new(),
            presence: Presence::Trivial(0),
        }
    }

    /// Creates a new `ListSequence` with validation.
    ///
    /// Validates that:
    /// - The item header's basic type matches the child sequence's basic type
    /// - The presence length equals the number of lists (offset item count)
    /// - The offsets are within bounds of the child sequence
    ///
    /// # Errors
    ///
    /// Returns an error if any validation checks fail.
    pub fn try_new(
        data_type: Option<TypeProjectionRef>,
        item: Option<Box<dyn Sequence>>,
        offsets: Offsets,
        presence: Presence,
    ) -> Result<ListSequence> {
        verify_arg!(presence, presence.len() == offsets.item_count());
        Ok(ListSequence {
            data_type,
            item,
            offsets,
            presence,
        })
    }
}

impl Clone for ListSequence {
    fn clone(&self) -> Self {
        Self {
            data_type: self.data_type.clone(),
            item: self.item.as_ref().map(|item| item.clone_boxed()),
            offsets: self.offsets.clone(),
            presence: self.presence.clone(),
        }
    }
}

impl Sequence for ListSequence {
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
            basic_type: BasicType::List,
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

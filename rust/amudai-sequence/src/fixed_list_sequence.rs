//! A sequence representing fixed-size lists where each list has the same
//! number of elements.

use std::borrow::Cow;

use amudai_common::{Result, verify_arg};
use amudai_format::{
    projection::TypeProjectionRef,
    schema::{BasicType, BasicTypeDescriptor},
};

use crate::{
    presence::Presence,
    sequence::{Sequence, ValueSequence},
    values::Values,
};

/// A sequence representing fixed-size lists where each list has the same
/// number of elements.
///
/// Each list has exactly `list_size` elements, with items stored in a flattened
/// sequence.
/// For `N` lists of size `S`, the item sequence contains `N * S` elements.
pub struct FixedListSequence {
    /// Schema sub-tree rooted at this sequence. Contains field metadata for the
    /// list and its child item, including name and type information.
    pub data_type: Option<TypeProjectionRef>,
    /// The fixed number of elements in each list.
    pub list_size: usize,
    /// Flattened sequence containing all list elements in order.
    pub item: Option<Box<dyn Sequence>>,
    /// Presence information indicating which list values are null.
    pub presence: Presence,
}

impl FixedListSequence {
    /// Creates a new fixed list sequence with explicit item header.
    ///
    /// # Panics
    /// Panics if validation fails (type mismatch or length mismatch).
    pub fn new(
        data_type: Option<TypeProjectionRef>,
        list_size: usize,
        item: Option<Box<dyn Sequence>>,
        presence: Presence,
    ) -> FixedListSequence {
        Self::try_new(data_type, list_size, item, presence).expect("try_new")
    }

    pub fn empty(list_size: usize) -> FixedListSequence {
        FixedListSequence {
            data_type: None,
            list_size,
            item: None,
            presence: Presence::Trivial(0),
        }
    }

    /// Creates a new fixed list sequence from an item sequence with default header.
    ///
    /// Generates a header named "item" based on the item sequence's type.
    pub fn from_item(
        list_size: usize,
        item: Box<dyn Sequence>,
        presence: Presence,
    ) -> FixedListSequence {
        FixedListSequence::new(None, list_size, Some(item), presence)
    }

    /// Creates a new fixed list sequence with validation.
    ///
    /// Validates that:
    /// - Item header type matches the item sequence type
    /// - Item sequence length equals presence length multiplied by list size
    ///
    /// # Errors
    /// Returns an error if validation fails.
    pub fn try_new(
        data_type: Option<TypeProjectionRef>,
        list_size: usize,
        item: Option<Box<dyn Sequence>>,
        presence: Presence,
    ) -> Result<FixedListSequence> {
        if let Some(item) = item.as_ref() {
            verify_arg!(item, item.len() == presence.len() * list_size);
        }
        Ok(FixedListSequence {
            data_type,
            list_size,
            item,
            presence,
        })
    }
}

/// Deep clone implementation that clones the item header, item sequence, and presence.
impl Clone for FixedListSequence {
    fn clone(&self) -> Self {
        Self {
            data_type: self.data_type.clone(),
            list_size: self.list_size,
            item: self.item.as_ref().map(|item| item.clone_boxed()),
            presence: self.presence.clone(),
        }
    }
}

/// Sequence trait implementation for fixed-size list sequences.
impl Sequence for FixedListSequence {
    fn as_any(&self) -> &(dyn std::any::Any + Send + Sync + 'static) {
        self
    }

    fn into_any(self: Box<Self>) -> Box<(dyn std::any::Any + Send + Sync + 'static)> {
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
            basic_type: BasicType::FixedSizeList,
            fixed_size: self.list_size as u32,
            ..Default::default()
        }
    }

    fn len(&self) -> usize {
        self.presence.len()
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn to_value_sequence(&self) -> Cow<ValueSequence> {
        Cow::Owned(ValueSequence {
            values: Values::new(),
            offsets: None,
            presence: self.presence.clone(),
            type_desc: self.basic_type(),
        })
    }
}

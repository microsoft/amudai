//! A sequence representing `Struct` field data with named fields.

use std::borrow::Cow;

use amudai_common::Result;
use amudai_format::{
    projection::TypeProjectionRef,
    schema::{BasicType, BasicTypeDescriptor},
};

use crate::{
    presence::Presence,
    sequence::{Sequence, ValueSequence},
    values::Values,
};

/// A sequence representing `Struct` field data with named fields.
///
/// Each field is a sequence itself, and all fields must have the same length
/// as determined by the presence information.
pub struct StructSequence {
    pub data_type: Option<TypeProjectionRef>,
    /// The field sequences, one per header
    pub fields: Vec<Box<dyn Sequence>>,
    /// Presence information indicating which struct values (rows) are null
    pub presence: Presence,
}

impl StructSequence {
    /// Creates a new struct sequence with the given headers, fields, and presence.
    ///
    /// # Panics
    /// Panics if validation fails (headers/fields length mismatch, field length
    /// mismatch, or type mismatch).
    pub fn new(
        data_type: Option<TypeProjectionRef>,
        fields: Vec<Box<dyn Sequence>>,
        presence: Presence,
    ) -> StructSequence {
        Self::try_new(data_type, fields, presence).expect("try_new")
    }

    pub fn empty() -> StructSequence {
        StructSequence {
            data_type: None,
            fields: Vec::new(),
            presence: Presence::Trivial(0),
        }
    }

    /// Creates a new struct sequence with validation.
    ///
    /// Validates that:
    /// - Headers count matches fields count
    /// - All fields have the same length as the presence
    /// - Each header's type matches its corresponding field's type
    ///
    /// # Errors
    /// Returns an error if any validation fails.
    pub fn try_new(
        data_type: Option<TypeProjectionRef>,
        fields: Vec<Box<dyn Sequence>>,
        presence: Presence,
    ) -> Result<StructSequence> {
        Ok(StructSequence {
            data_type,
            fields,
            presence,
        })
    }
}

/// Manual Clone implementation (deep clone) that clones each boxed field sequence.
impl Clone for StructSequence {
    fn clone(&self) -> Self {
        Self {
            data_type: self.data_type.clone(),
            fields: self
                .fields
                .iter()
                .map(|field| field.clone_boxed())
                .collect(),
            presence: self.presence.clone(),
        }
    }
}

/// Sequence trait implementation for StructSequence.
impl Sequence for StructSequence {
    /// Returns a reference to self as Any for downcasting.
    fn as_any(&self) -> &(dyn std::any::Any + Send + Sync + 'static) {
        self
    }

    /// Converts the boxed self into a boxed Any for downcasting.
    fn into_any(self: Box<Self>) -> Box<(dyn std::any::Any + Send + Sync + 'static)> {
        self
    }

    /// Returns the TypeId of StructSequence.
    fn type_id(&self) -> std::any::TypeId {
        std::any::TypeId::of::<Self>()
    }

    /// Creates a boxed clone of this sequence.
    fn clone_boxed(&self) -> Box<dyn Sequence> {
        Box::new(self.clone())
    }

    /// Returns the basic type descriptor for struct types.
    fn basic_type(&self) -> BasicTypeDescriptor {
        BasicTypeDescriptor {
            basic_type: BasicType::Struct,
            ..Default::default()
        }
    }

    /// Returns the number of rows in this struct sequence.
    fn len(&self) -> usize {
        self.presence.len()
    }

    /// Returns true if the sequence has no rows.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Converts to a value sequence with empty values (structs are not stored as values).
    fn to_value_sequence(&self) -> std::borrow::Cow<ValueSequence> {
        Cow::Owned(ValueSequence {
            values: Values::new(),
            offsets: None,
            presence: self.presence.clone(),
            type_desc: self.basic_type(),
        })
    }
}

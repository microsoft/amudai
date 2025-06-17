//! Stripe field builder, encompassing all nested data types.

use std::sync::Arc;

use ahash::AHashMap;
use amudai_common::{Result, error::Error};
use amudai_encodings::block_encoder::BlockEncodingProfile;
use amudai_format::defs::common::DataRef;
use amudai_format::defs::shard;
use amudai_format::{
    defs::schema_ext::BasicTypeDescriptor,
    schema::{BasicType, DataType, FieldList, FieldLocator, SchemaId},
};
use amudai_io::temp_file_store::TemporaryFileStore;
use amudai_keyed_vector::KeyFromValue;
use amudai_objectstore::url::ObjectUrl;
use arrow_array::{Array, ArrayRef, cast::AsArray};

use crate::write::field_encoder::FieldEncoderParams;
use crate::write::stripe_builder::create_field_locators;

use super::format_elements_ext::CompactDataRefs;
use super::{
    artifact_writer::ArtifactWriter,
    field_encoder::{EncodedField, FieldEncoder},
};
use crate::write::field_descriptor;

/// A builder for a single field within a stripe.
///
/// This struct manages the construction of a field, including its nested
/// children, and provides access to the underlying encoder.
pub struct FieldBuilder {
    params: FieldBuilderParams,
    basic_type: BasicTypeDescriptor,
    encoder: FieldEncoder,
    children: FieldBuilders,
}

impl FieldBuilder {
    /// Creates a new `FieldBuilder`.
    ///
    /// # Arguments
    ///
    /// * `params` - Parameters for the field builder, including the data type and object store.
    ///
    /// # Returns
    ///
    /// A new `FieldBuilder` instance, or an error if the child field builders cannot be created.
    pub fn new(params: FieldBuilderParams) -> Result<FieldBuilder> {
        let basic_type = params.data_type.describe()?;
        let encoder_params = FieldEncoderParams {
            basic_type,
            temp_store: params.temp_store.clone(),
            encoding_profile: params.encoding_profile,
        };
        let encoder = FieldEncoder::new(encoder_params)?;
        let children = FieldBuilders::new(&params.data_type)?;
        Ok(FieldBuilder {
            params,
            basic_type,
            encoder,
            children,
        })
    }

    /// Returns a mutable reference to the field encoder.
    ///
    /// # Returns
    ///
    /// A mutable reference to the `FieldEncoder` associated with this field.
    pub fn encoder(&mut self) -> &mut FieldEncoder {
        &mut self.encoder
    }

    /// Returns the data type of the field being built.
    ///
    /// # Returns
    ///
    /// A reference to the `DataType` of this field.
    pub fn data_type(&self) -> &DataType {
        &self.params.data_type
    }

    /// Returns the `BasicTypeDescriptor` of the field being built.
    pub fn basic_type(&self) -> &BasicTypeDescriptor {
        &self.basic_type
    }

    /// Retrieves or creates a child field builder based on the provided locator.
    ///
    /// # Arguments
    ///
    /// * `locator` - The location of the child field, either by ordinal index or name.
    ///
    /// # Returns
    ///
    /// A mutable reference to the child `FieldBuilder`, or an error if the child cannot be created.
    pub fn get_child(&mut self, locator: &FieldLocator) -> Result<&mut FieldBuilder> {
        self.children.get_or_create(locator, |index, child_type| {
            Self::create_child(&self.params, index, child_type)
        })
    }

    /// Pushes an array of data into the field builder.
    ///
    /// # Arguments
    ///
    /// * `array` - An Arrow array containing the values to be pushed.
    pub fn push_array(&mut self, array: Arc<dyn Array>) -> Result<()> {
        match self.basic_type.basic_type {
            BasicType::Unit
            | BasicType::Boolean
            | BasicType::Int8
            | BasicType::Int16
            | BasicType::Int32
            | BasicType::Int64
            | BasicType::Float32
            | BasicType::Float64
            | BasicType::Binary
            | BasicType::FixedSizeBinary
            | BasicType::String
            | BasicType::Guid
            | BasicType::DateTime => {
                if array.data_type().is_nested() {
                    return Err(Error::invalid_arg(
                        "array",
                        format!("expected primitive array, got {:?}", array.data_type()),
                    ));
                }
                self.encoder.push_array(array)
            }
            BasicType::List => self.push_list_array(array),
            BasicType::FixedSizeList => self.push_fixed_size_list_array(array),
            BasicType::Struct => self.push_struct_array(array),
            BasicType::Map => self.push_map_array(array),
            BasicType::Union => self.push_union_array(array),
        }
    }

    /// Synchronizes this field builder and its encoder with the logical position of its parent
    /// and siblings by appending the necessary number of null value slots to the encoder.
    ///
    /// Data is fed to the encoders in a lazy manner: if there is no data for a field
    /// in the current record batch, `push_array()` will not be called for that field.
    /// Consider a struct node `S` with two child nodes `C1` and `C2`. During data ingestion,
    /// some record batches might not contain data for `C2`, resulting in `C2`'s `push_array()`
    /// not being invoked. When data for `C2` eventually appears (or upon completion when sealing
    /// the encoders), `C2`'s state may be inconsistent with the other encoders. This method
    /// resolves such inconsistencies, allowing the encoder to efficiently handle sparse data.
    pub fn sync_with_parent(&mut self, parent_pos: usize) -> Result<()> {
        let logical_pos = self.encoder.logical_len();
        if parent_pos > logical_pos {
            self.encoder.push_nulls(parent_pos - logical_pos)?;
        }
        Ok(())
    }

    /// Completes the construction of the field by consuming the `FieldBuilder`.
    /// This method appends the `PreparedField` instances of both the current field
    /// and all its child fields to the specified vector.
    ///
    /// # Arguments
    ///
    /// * `prepared_fields` - A mutable reference to a vector where the resulting
    ///   `PreparedField` objects will be appended.
    ///
    /// # Returns
    ///
    /// * `Result<()>` - Returns an empty `Result` indicating success or an error
    ///   if the operation fails.
    pub fn finish(self, prepared_fields: &mut Vec<PreparedStripeField>) -> Result<()> {
        let FieldBuilder {
            params,
            basic_type,
            encoder,
            children,
        } = self;

        // For `Struct`, `FixedSizeList` and `Union` fields, the children should synchronize
        // with the parent's position before completion (similar to the normal `push_array`
        // flow).
        let logical_pos = match basic_type.basic_type {
            BasicType::FixedSizeList | BasicType::Struct | BasicType::Union => {
                Some(encoder.logical_len())
            }
            _ => None,
        };

        children.finish(logical_pos, prepared_fields)?;
        let position_count = encoder.logical_len() as u64;
        let encoded_field = encoder.finish()?;

        // Create field descriptor with statistics populated from encoded field
        let descriptor = field_descriptor::create_with_stats(position_count, &encoded_field);

        let prepared_field = PreparedStripeField {
            schema_id: params.data_type.schema_id()?,
            data_type: params.data_type,
            descriptor,
            encodings: vec![encoded_field],
        };

        prepared_fields.push(prepared_field);
        Ok(())
    }
}

impl FieldBuilder {
    fn push_list_array(&mut self, array: Arc<dyn Array>) -> Result<()> {
        // Push list offsets and presence data to the encoder.
        self.encoder.push_array(array.clone())?;

        // Slice the child array using the start/end offset.
        let values = match array.data_type() {
            arrow_schema::DataType::List(_) => {
                let array = array.as_list::<i32>();
                Self::slice_list_values(array.value_offsets(), array.values())
            }
            arrow_schema::DataType::LargeList(_) => {
                let array = array.as_list::<i64>();
                Self::slice_list_values(array.value_offsets(), array.values())
            }
            arrow_schema::DataType::Int32 | arrow_schema::DataType::Int64 => {
                // Assume we're processing list offsets only (no child item array).
                return Ok(());
            }
            _ => {
                return Err(Error::invalid_arg(
                    "array",
                    format!(
                        "push_list_array: expected list, got {:?}",
                        array.data_type()
                    ),
                ));
            }
        };

        let child = self.get_child(&FieldLocator::Ordinal(0))?;
        // Note: in case of variable-sized list (or map), the child's logical position
        // is not synced to the parent's one, as it is tracked by the parent explicitly
        // through the offset encoding.
        child.push_array(values)
    }

    fn push_struct_array(&mut self, array: Arc<dyn Array>) -> Result<()> {
        // Capture the logical position of the struct node before proceeding,
        // it is used to synchronize the child nodes position with the parent.
        let logical_pos = self.encoder.logical_len();

        // Push struct presence data.
        self.encoder.push_array(array.clone())?;

        let arrow_schema::DataType::Struct(fields) = array.data_type() else {
            return Err(Error::invalid_arg(
                "array",
                format!("expected Struct array, got {:?}", array.data_type()),
            ));
        };

        let array = array.as_struct();
        if array.columns().len() > self.children.len() {
            // Possible data loss.
            return Err(Error::invalid_arg(
                "array",
                format!(
                    "struct array has more fields ({}) than field builder ({})",
                    array.columns().len(),
                    self.children.len()
                ),
            ));
        }

        let field_locators = create_field_locators(fields.len(), |i| fields[i].name().as_str());
        for (i, locator) in field_locators.enumerate() {
            let field = self.get_child(&locator)?;
            field.sync_with_parent(logical_pos)?;
            field.push_array(array.column(i).clone())?;
        }
        Ok(())
    }

    fn push_fixed_size_list_array(&mut self, array: Arc<dyn Array>) -> Result<()> {
        let list_size = match array.data_type() {
            arrow_schema::DataType::FixedSizeList(_, size) => *size as usize,
            _ => {
                return Err(Error::invalid_arg(
                    "array",
                    format!("expected Struct array, got {:?}", array.data_type()),
                ));
            }
        };

        // Calculate the nominal child logical position before proceeding,
        // based on the parent position.
        let logical_pos = self.encoder.logical_len();
        let child_logical_pos = logical_pos
            .checked_mul(list_size)
            .ok_or_else(|| Error::invalid_arg("array", "logical position overflow"))?;

        // Push list presence data.
        self.encoder.push_array(array.clone())?;

        let array = array.as_fixed_size_list();
        let values = array.values().clone();
        let field = self.get_child(&FieldLocator::Ordinal(0))?;
        field.sync_with_parent(child_logical_pos)?;
        field.push_array(values)?;
        Ok(())
    }

    fn push_map_array(&mut self, array: Arc<dyn Array>) -> Result<()> {
        // Push map offsets and presence data to the encoder.
        self.encoder.push_array(array.clone())?;

        let Some(map) = array.as_map_opt() else {
            return Err(Error::invalid_arg(
                "array",
                format!("expected Map array, got {:?}", array.data_type()),
            ));
        };

        let keys = Self::slice_list_values(map.value_offsets(), map.keys());
        let values = Self::slice_list_values(map.value_offsets(), map.values());
        self.get_child(&FieldLocator::Ordinal(0))?
            .push_array(keys)?;
        self.get_child(&FieldLocator::Ordinal(1))?
            .push_array(values)?;
        Ok(())
    }

    fn push_union_array(&mut self, _array: Arc<dyn Array>) -> Result<()> {
        todo!()
    }

    fn create_child(
        parent_params: &FieldBuilderParams,
        _child_index: usize,
        child_type: &DataType,
    ) -> Result<FieldBuilder> {
        let params = FieldBuilderParams {
            data_type: child_type.clone(),
            temp_store: parent_params.temp_store.clone(),
            encoding_profile: parent_params.encoding_profile,
        };
        FieldBuilder::new(params)
    }

    fn slice_list_values<O: arrow_array::OffsetSizeTrait>(
        offsets: &[O],
        values: &ArrayRef,
    ) -> ArrayRef {
        assert!(!offsets.is_empty());
        let start = offsets[0].as_usize();
        let end = offsets[offsets.len() - 1].as_usize();
        let length = end - start;
        values.slice(start, length)
    }
}

/// Represents a prepared stripe field that is ready to be written into the final
/// artifact in the object store. This includes the stripe field descriptor, which
/// will be added to the stripe directory.
#[derive(Debug)]
pub struct PreparedStripeField {
    pub data_type: DataType,
    pub schema_id: SchemaId,
    pub descriptor: shard::FieldDescriptor,
    pub encodings: Vec<EncodedField>,
}

impl PreparedStripeField {
    /// Flushes all temporary data encoding buffers of this stripe field
    /// into the specified artifact writer (typically a stripe storage blob) and returns
    /// a sealed stripe field descriptor.
    ///
    /// # Arguments
    ///
    /// * `writer` - A mutable reference to an `ArtifactWriter` where the encoded data
    ///   will be written.
    pub fn seal(self, writer: &mut ArtifactWriter) -> Result<SealedStripeField> {
        let mut encodings = Vec::with_capacity(self.encodings.len());
        for encoding in self.encodings {
            let encoding = encoding.seal(writer)?;
            encodings.push(encoding);
        }
        Ok(SealedStripeField {
            schema_id: self.schema_id,
            descriptor: shard::StripeFieldDescriptor {
                encodings,
                field: Some(self.descriptor),
            },
        })
    }
}

impl KeyFromValue<SchemaId> for PreparedStripeField {
    fn key(&self) -> SchemaId {
        self.schema_id
    }
}

/// Represents a stripe field with the data committed into the object store.
#[derive(Debug)]
pub struct SealedStripeField {
    /// Shard schema id of this field.
    pub schema_id: SchemaId,
    pub descriptor: shard::StripeFieldDescriptor,
}

impl SealedStripeField {
    /// Writes the stripe field descriptor of this field to the specified artifact.
    /// Returns a `DataRef` pointing to the written message.
    pub fn write_descriptor(&self, writer: &mut ArtifactWriter) -> Result<DataRef> {
        let msg_ref = writer.write_message(&self.descriptor)?;
        Ok(msg_ref)
    }
}

impl CompactDataRefs for SealedStripeField {
    fn collect_urls(&self, set: &mut ahash::AHashSet<String>) {
        self.descriptor
            .encodings
            .iter()
            .for_each(|enc| enc.collect_urls(set));
    }

    fn compact_data_refs(&mut self, shard_url: &ObjectUrl) {
        self.descriptor
            .encodings
            .iter_mut()
            .for_each(|enc| enc.compact_data_refs(shard_url));
    }
}

impl KeyFromValue<SchemaId> for SealedStripeField {
    fn key(&self) -> SchemaId {
        self.schema_id
    }
}

/// Manages a collection of lazily instantiated child field builders.
///
/// These builders are accessible using a `FieldLocator`, either by ordinal index or name.
pub(in crate::write) struct FieldBuilders {
    len: usize,
    schema: FieldList,
    fields: Vec<Option<FieldBuilder>>,
    lookup: AHashMap<String, usize>,
}

impl FieldBuilders {
    /// Creates a new `FieldBuilders` instance.
    ///
    /// # Arguments
    ///
    /// * `schema` - The schema of the fields.
    pub fn new(schema: impl Into<FieldList>) -> Result<FieldBuilders> {
        let schema = schema.into();
        Ok(FieldBuilders {
            len: schema.len()?,
            schema,
            fields: Vec::new(),
            lookup: AHashMap::new(),
        })
    }

    /// Total number of fields.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Finalizes the encoding for all instantiated child fields and adds them to the
    /// `prepared_fields` list.
    ///
    /// If `parent_pos` is not `None`, synchronizes the child field with the parent's
    /// position before completing the process.
    pub fn finish(
        self,
        parent_pos: Option<usize>,
        prepared_fields: &mut Vec<PreparedStripeField>,
    ) -> Result<()> {
        // Ignore all fields that were not initialized: since no values were added to them
        // during the stripe encoding process, they should not be represented in the stripe
        // at all.
        let mut builders = self.fields.into_iter().flatten().collect::<Vec<_>>();
        if let Some(parent_pos) = parent_pos {
            for builder in &mut builders {
                builder.sync_with_parent(parent_pos)?;
            }
        }

        for builder in builders {
            builder.finish(prepared_fields)?;
        }
        Ok(())
    }

    /// Retrieves or creates a child field builder based on the provided locator.
    ///
    /// # Arguments
    ///
    /// * `locator` - The location of the child field, either by ordinal index or name.
    /// * `create_fn` - A closure that creates a new `FieldBuilder` if one does not exist.
    ///
    /// # Returns
    ///
    /// A mutable reference to the child `FieldBuilder`, or an error if the child cannot be created.
    pub fn get_or_create(
        &mut self,
        locator: &FieldLocator,
        create_fn: impl FnOnce(usize, &DataType) -> Result<FieldBuilder>,
    ) -> Result<&mut FieldBuilder> {
        match locator {
            FieldLocator::Ordinal(index) => self.get_or_create_at(*index, create_fn),
            FieldLocator::Name(name) => self.get_or_create_named(name, create_fn),
        }
    }

    fn get_or_create_named(
        &mut self,
        name: &str,
        create_fn: impl FnOnce(usize, &DataType) -> Result<FieldBuilder>,
    ) -> Result<&mut FieldBuilder> {
        let index = if let Some(index) = self.lookup.get(name) {
            *index
        } else {
            let (index, _) = self
                .schema
                .find(name)?
                .ok_or_else(|| Error::invalid_arg("name", format!("unknown field name {name}")))?;
            self.lookup.insert(name.to_string(), index);
            index
        };
        self.get_or_create_at(index, create_fn)
    }

    fn get_or_create_at(
        &mut self,
        index: usize,
        create_fn: impl FnOnce(usize, &DataType) -> Result<FieldBuilder>,
    ) -> Result<&mut FieldBuilder> {
        if index >= self.fields.len() {
            self.fields.resize_with(index + 1, || None);
        }
        if self.fields[index].is_none() {
            self.create_at(index, create_fn)?;
        }
        Ok(self.fields[index].as_mut().unwrap())
    }

    fn create_at(
        &mut self,
        index: usize,
        create_fn: impl FnOnce(usize, &DataType) -> Result<FieldBuilder>,
    ) -> Result<()> {
        let data_type = self.schema.get_at(index)?;
        let builder = create_fn(index, &data_type)?;
        self.fields[index] = Some(builder);
        Ok(())
    }
}

/// Parameters for creating a `FieldBuilder`.
#[derive(Clone)]
pub struct FieldBuilderParams {
    /// The data type of the field.
    pub data_type: DataType,
    /// The temp file store to use when building the field.
    pub temp_store: Arc<dyn TemporaryFileStore>,
    /// Encoding profile for a field.
    /// See [`ShardBuilderParams::encoding_profile`](`crate::write::shard_builder::ShardBuilderParams::encoding_profile`)
    pub encoding_profile: BlockEncodingProfile,
}

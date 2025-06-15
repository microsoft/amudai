//! `List` and `Map` field encoders.

use std::sync::Arc;

use amudai_blockstream::write::{
    bit_buffer::{BitBufferEncoder, EncodedBitBuffer},
    primitive_buffer::PrimitiveBufferEncoder,
    staging_buffer::PrimitiveStagingBuffer,
};
use amudai_common::{Result, error::Error};
use amudai_encodings::block_encoder::{
    BlockChecksum, BlockEncodingParameters, BlockEncodingPolicy, BlockSizeConstraints,
    PresenceEncoding,
};
use amudai_format::{
    defs::{schema_ext::BasicTypeDescriptor, shard},
    schema::BasicType,
};
use arrow_array::{Array, cast::AsArray};

use crate::write::field_encoder::FieldEncoderParams;

use super::{EncodedField, FieldEncoderOps};

/// Encoder for fields representing multi-item containers, such as `List` or `Map`.
///
/// Encoded values are offsets into the child container along with presence mask.
///
/// This encoder stores the offsets of the child elements within the container.
/// For example, for a `List<i32>` field, it stores the starting index of each list within
/// the flattened `i32` array. It also keeps track of the last offset to correctly calculate
/// the absolute offsets within the stripe when appending new arrays.
pub struct ListContainerFieldEncoder {
    /// Field encoder parameters (temp store, encoding profile).
    params: FieldEncoderParams,
    /// A staging buffer to store the encoded offsets.
    staging: PrimitiveStagingBuffer,
    /// The last offset used when encoding the previous array.
    /// This is the starting offset of the next list element to be pushed.
    last_offset: u64,
    /// Offset buffer encoder.
    offsets_encoder: PrimitiveBufferEncoder,
    /// Presence buffer encoder.
    presence_encoder: BitBufferEncoder,
}

impl ListContainerFieldEncoder {
    pub(crate) fn create(params: &FieldEncoderParams) -> Result<Box<dyn FieldEncoderOps>> {
        let mut staging = PrimitiveStagingBuffer::new(arrow_schema::DataType::UInt64);
        staging.append(Arc::new(arrow_array::PrimitiveArray::from(vec![0u64])));

        Ok(Box::new(ListContainerFieldEncoder {
            params: params.clone(),
            staging,
            last_offset: 0,
            offsets_encoder: PrimitiveBufferEncoder::new(
                BlockEncodingPolicy {
                    parameters: BlockEncodingParameters {
                        presence: PresenceEncoding::Disabled,
                        checksum: BlockChecksum::Enabled,
                    },
                    profile: params.encoding_profile,
                    size_constraints: Some(BlockSizeConstraints {
                        value_count: Self::DEFAULT_BLOCK_SIZE..Self::DEFAULT_BLOCK_SIZE + 1,
                        data_size: Self::DEFAULT_BLOCK_SIZE * 8..Self::DEFAULT_BLOCK_SIZE * 8 + 1,
                    }),
                },
                BasicTypeDescriptor {
                    basic_type: BasicType::Int64,
                    fixed_size: 0,
                    signed: false,
                },
                params.temp_store.clone(),
            )?,
            presence_encoder: BitBufferEncoder::new(
                params.temp_store.clone(),
                params.encoding_profile,
            ),
        }))
    }
}

impl ListContainerFieldEncoder {
    /// Default (fallback) block value count when it cannot be inferred
    /// by the block encoder.
    const DEFAULT_BLOCK_SIZE: usize = 1024;

    fn flush(&mut self, force: bool) -> Result<()> {
        if self.staging.is_empty() {
            return Ok(());
        }
        let block_size = self.get_preferred_block_size_from_sample()?;
        assert_ne!(block_size, 0);
        let min_block_size = if force { 1 } else { block_size };

        while self.staging.len() >= min_block_size {
            let values = self.staging.dequeue(block_size)?;
            self.offsets_encoder.encode_block(&values)?;
        }

        Ok(())
    }

    fn may_flush(&self) -> bool {
        self.staging.len() >= PrimitiveStagingBuffer::DEFAULT_LEN_THRESHOLD
    }

    fn get_preferred_block_size_from_sample(&mut self) -> Result<usize> {
        let sample_size = self
            .offsets_encoder
            .sample_size()
            .map_or(0, |size| size.value_count.end.max(1) - 1);
        let block_size = if sample_size != 0 {
            let sample = self.staging.sample(sample_size)?;
            if sample.is_empty() {
                Self::DEFAULT_BLOCK_SIZE
            } else {
                self.offsets_encoder
                    .analyze_sample(&sample)?
                    .value_count
                    .end
                    .max(2)
                    - 1
            }
        } else {
            Self::DEFAULT_BLOCK_SIZE
        };
        Ok(block_size)
    }
}

impl FieldEncoderOps for ListContainerFieldEncoder {
    fn push_array(&mut self, array: Arc<dyn Array>) -> Result<()> {
        let (offsets, next_offset) = make_offsets_array(&array, self.last_offset)?;
        self.last_offset = next_offset;
        let slot_count = offsets.len();
        self.staging.append(offsets);

        if let Some(presence) = array.nulls() {
            self.presence_encoder.append(presence.inner())?;
        } else {
            self.presence_encoder.append_repeated(slot_count, true)?;
        }

        if self.may_flush() {
            self.flush(false)?;
        }
        Ok(())
    }

    fn push_nulls(&mut self, count: usize) -> Result<()> {
        let offsets = Arc::new(arrow_array::PrimitiveArray::from(vec![
            self.last_offset;
            count
        ]));
        self.staging.append(offsets);
        self.presence_encoder.append_repeated(count, false)?;

        if self.may_flush() {
            self.flush(false)?;
        }
        Ok(())
    }

    fn finish(mut self: Box<Self>) -> Result<EncodedField> {
        if !self.staging.is_empty() {
            self.flush(true)?;
        }

        let mut buffers = Vec::new();

        let mut encoded_offsets = self.offsets_encoder.finish()?;
        encoded_offsets.descriptor.kind = shard::BufferKind::Offsets as i32;
        encoded_offsets.descriptor.embedded_presence = false;
        encoded_offsets.descriptor.embedded_offsets = false;
        buffers.push(encoded_offsets);

        match self.presence_encoder.finish()? {
            EncodedBitBuffer::Constant(value, count) => {
                if !value {
                    // TODO: do not write out any buffers, mark the field as constant Null
                    // in the field descriptor.
                    let mut presence = BitBufferEncoder::encode_blocks(
                        count,
                        value,
                        self.params.temp_store.clone(),
                    )?;
                    presence.descriptor.kind = shard::BufferKind::Presence as i32;
                    presence.descriptor.embedded_presence = false;
                    presence.descriptor.embedded_offsets = false;
                    buffers.push(presence);
                }

                // The field has a trivial presence in all stripe positions, no need to write
                // out dedicated presence buffer.
            }
            EncodedBitBuffer::Blocks(mut presence) => {
                presence.descriptor.kind = shard::BufferKind::Presence as i32;
                presence.descriptor.embedded_presence = false;
                presence.descriptor.embedded_offsets = false;
                buffers.push(presence);
            }
        }

        Ok(EncodedField {
            buffers,
            statistics: None, // Container fields don't collect primitive statistics
        })
    }
}

/// Calculates the next offsets chunk for a given array of list/map values to be appended
/// to the staging buffer.
///
/// This function takes an array and the current last offset and returns a new array of offsets
/// rebased to the last offset, along with the next offset.
///
/// Note: The offsets within an Arrow List or Map array are relative to the start of the 'values'
/// array within that specific List/Map array instance. They are not absolute offsets within the
/// entire data stream, therefore the `rebase_offsets` calculation is performed as part of this
/// function.
pub fn make_offsets_array(source: &dyn Array, last_offset: u64) -> Result<(Arc<dyn Array>, u64)> {
    fn rebase_offsets<O: arrow_array::OffsetSizeTrait>(
        last_offset: u64,
        offsets: &[O],
    ) -> Vec<u64> {
        assert!(!offsets.is_empty());
        let mut buf = Vec::<u64>::with_capacity(offsets.len() - 1);
        let first = offsets[0].as_usize() as u64;
        buf.extend(
            offsets[1..]
                .iter()
                .map(|&o| (o.as_usize() as u64) - first + last_offset),
        );
        buf
    }

    let offsets = match source.data_type() {
        arrow_schema::DataType::List(_) => {
            rebase_offsets(last_offset, source.as_list::<i32>().value_offsets())
        }
        arrow_schema::DataType::LargeList(_) => {
            rebase_offsets(last_offset, source.as_list::<i64>().value_offsets())
        }
        arrow_schema::DataType::Map(_, _) => {
            rebase_offsets(last_offset, source.as_map().value_offsets())
        }
        _ => {
            return Err(Error::invalid_arg(
                "array",
                "source array does not have offsets",
            ));
        }
    };
    let next_offset = offsets.last().copied().unwrap_or(last_offset);
    let array = Arc::new(arrow_array::PrimitiveArray::from(offsets));
    Ok((array, next_offset))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{
        Array, Int32Array, PrimitiveArray,
        builder::{Int32Builder, ListBuilder, MapBuilder},
        cast::AsArray,
        types::{Int32Type, UInt64Type},
    };

    use crate::write::field_encoder::list_container::make_offsets_array;

    #[test]
    fn test_make_offsets_empty_list() {
        let list_array = make_list::<_, _>(std::iter::empty::<Vec<i32>>());
        let result = make_offsets_array(&*list_array, 0).unwrap();
        let offsets = result
            .0
            .as_any()
            .downcast_ref::<PrimitiveArray<UInt64Type>>()
            .unwrap();
        assert_eq!(offsets.values(), &[]);
        assert_eq!(result.1, 0);
    }

    #[test]
    fn test_make_offsets_list_with_single_empty_list() {
        let list_array = make_list(vec![vec![]]);
        let result = make_offsets_array(&*list_array, 0).unwrap();
        let offsets = result
            .0
            .as_any()
            .downcast_ref::<PrimitiveArray<UInt64Type>>()
            .unwrap();
        assert_eq!(offsets.values(), &[0]);
        assert_eq!(result.1, 0);
    }

    #[test]
    fn test_make_offsets_list_with_single_non_empty_list() {
        let list_array = make_list(vec![vec![1, 2, 3]]);
        let result = make_offsets_array(&*list_array, 0).unwrap();
        let offsets = result
            .0
            .as_any()
            .downcast_ref::<PrimitiveArray<UInt64Type>>()
            .unwrap();
        assert_eq!(offsets.values(), &[3]);
        assert_eq!(result.1, 3);
    }

    #[test]
    fn test_make_offsets_list_with_multiple_lists() {
        let list_array = make_list(vec![vec![1, 2], vec![3, 4, 5], vec![]]);
        let result = make_offsets_array(&*list_array, 0).unwrap();
        let offsets = result
            .0
            .as_any()
            .downcast_ref::<PrimitiveArray<UInt64Type>>()
            .unwrap();
        assert_eq!(offsets.values(), &[2, 5, 5]);
        assert_eq!(result.1, 5);
    }

    #[test]
    fn test_make_offsets_list_with_last_offset() {
        let list_array = make_list(vec![vec![1, 2], vec![3, 4, 5]]);
        let result = make_offsets_array(&*list_array, 10).unwrap();
        let offsets = result
            .0
            .as_any()
            .downcast_ref::<PrimitiveArray<UInt64Type>>()
            .unwrap();
        assert_eq!(offsets.values(), &[12, 15]);
        assert_eq!(result.1, 15);
    }

    #[test]
    fn test_make_offsets_empty_map() {
        let map_array = make_map::<_, _, _>(std::iter::empty::<(Vec<i32>, Vec<i32>)>());
        let result = make_offsets_array(&*map_array, 0).unwrap();
        let offsets = result
            .0
            .as_any()
            .downcast_ref::<PrimitiveArray<UInt64Type>>()
            .unwrap();
        assert_eq!(offsets.values(), &[]);
        assert_eq!(result.1, 0);
    }

    #[test]
    fn test_make_offsets_map_with_single_empty_map() {
        let map_array = make_map(vec![(vec![], vec![])]);
        let result = make_offsets_array(&*map_array, 0).unwrap();
        let offsets = result
            .0
            .as_any()
            .downcast_ref::<PrimitiveArray<UInt64Type>>()
            .unwrap();
        assert_eq!(offsets.values(), &[0]);
        assert_eq!(result.1, 0);
    }

    #[test]
    fn test_make_offsets_map_with_single_non_empty_map() {
        let map_array = make_map(vec![(vec![1, 2], vec![3, 4])]);
        let result = make_offsets_array(&*map_array, 0).unwrap();
        let offsets = result
            .0
            .as_any()
            .downcast_ref::<PrimitiveArray<UInt64Type>>()
            .unwrap();
        assert_eq!(offsets.values(), &[2]);
        assert_eq!(result.1, 2);
    }

    #[test]
    fn test_make_offsets_map_with_multiple_maps() {
        let map_array = make_map(vec![
            (vec![1, 2], vec![3, 4]),
            (vec![5, 6, 7], vec![8, 9, 10]),
            (vec![], vec![]),
        ]);
        let result = make_offsets_array(&*map_array, 0).unwrap();
        let offsets = result
            .0
            .as_any()
            .downcast_ref::<PrimitiveArray<UInt64Type>>()
            .unwrap();
        assert_eq!(offsets.values(), &[2, 5, 5]);
        assert_eq!(result.1, 5);
    }

    #[test]
    fn test_make_offsets_map_with_last_offset() {
        let map_array = make_map(vec![
            (vec![1, 2], vec![3, 4]),
            (vec![5, 6, 7], vec![8, 9, 10]),
        ]);
        let result = make_offsets_array(&*map_array, 10).unwrap();
        let offsets = result
            .0
            .as_any()
            .downcast_ref::<PrimitiveArray<UInt64Type>>()
            .unwrap();
        assert_eq!(offsets.values(), &[12, 15]);
        assert_eq!(result.1, 15);
    }

    #[test]
    fn test_make_offsets_invalid_array_type() {
        let int_array = Arc::new(Int32Array::from(vec![1, 2, 3])) as Arc<dyn Array>;
        let result = make_offsets_array(&*int_array, 0);
        assert!(result.is_err());
        assert_eq!(
            result.err().unwrap().to_string(),
            "invalid argument array: source array does not have offsets"
        );
    }

    fn make_map<K, V, I>(iter: I) -> Arc<dyn Array>
    where
        K: IntoIterator<Item = i32>,
        V: IntoIterator<Item = i32>,
        I: IntoIterator<Item = (K, V)>,
    {
        let mut map_builder = MapBuilder::new(None, Int32Builder::new(), Int32Builder::new());

        for (keys, values) in iter {
            let key_builder = map_builder.keys();

            for key in keys {
                key_builder.append_value(key);
            }

            let value_builder = map_builder.values();
            for value in values {
                value_builder.append_value(value);
            }
            map_builder.append(true).unwrap();
        }

        Arc::new(map_builder.finish())
    }

    fn make_list<P, I>(iter: I) -> Arc<dyn Array>
    where
        P: IntoIterator<Item = i32>,
        I: IntoIterator<Item = P>,
    {
        let mut list_builder = ListBuilder::new(Int32Builder::new());

        for inner_iter in iter {
            let inner_builder = list_builder.values();
            for value in inner_iter {
                inner_builder.append_value(value);
            }
            list_builder.append(true); // Append a valid list
        }

        Arc::new(list_builder.finish())
    }

    #[test]
    fn test_make_list() {
        let data = vec![vec![1, 2, 3], vec![4, 5], vec![6, 7, 8, 9]];

        let list_array = make_list(data.clone());
        let list_array = list_array.as_list::<i32>();

        assert_eq!(list_array.len(), 3);

        let v0 = list_array.value(0);
        let first_list_values = v0.as_primitive::<Int32Type>();
        assert_eq!(first_list_values.len(), 3);
        assert_eq!(first_list_values.value(0), 1);
        assert_eq!(first_list_values.value(1), 2);
        assert_eq!(first_list_values.value(2), 3);

        let v1 = list_array.value(1);
        let second_list_values = v1.as_primitive::<Int32Type>();
        assert_eq!(second_list_values.len(), 2);
        assert_eq!(second_list_values.value(0), 4);
        assert_eq!(second_list_values.value(1), 5);

        let v2 = list_array.value(2);
        let third_list_values = v2.as_primitive::<Int32Type>();
        assert_eq!(third_list_values.len(), 4);
        assert_eq!(third_list_values.value(0), 6);
        assert_eq!(third_list_values.value(1), 7);
        assert_eq!(third_list_values.value(2), 8);
        assert_eq!(third_list_values.value(3), 9);
    }
}

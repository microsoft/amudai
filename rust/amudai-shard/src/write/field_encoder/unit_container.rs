//! `Struct` and `FixedSizeList` field encoder.

use std::sync::Arc;

use amudai_common::{error::Error, Result};
use amudai_format::defs::schema_ext::BasicTypeDescriptor;
use arrow_array::{cast::AsArray, Array};

use super::{boolean::BooleanBufferEncoder, EncodedField, FieldEncoderOps};

/// Encoder for fields that represent single-item containers, such as
/// `Struct` or `FixedSizeList` (in contrast to multi-item containers
/// like `List` or `Map`).
///
/// The "values" encoded by this encoder are validity bits, which indicate
/// presence or non-null status.
pub struct UnitContainerFieldEncoder {
    _basic_type: BasicTypeDescriptor,
    presence: BooleanBufferEncoder,
}

impl UnitContainerFieldEncoder {
    pub fn create(basic_type: BasicTypeDescriptor) -> Result<Box<dyn FieldEncoderOps>> {
        Ok(Box::new(UnitContainerFieldEncoder {
            _basic_type: basic_type,
            presence: BooleanBufferEncoder::new(),
        }))
    }
}

impl FieldEncoderOps for UnitContainerFieldEncoder {
    fn push_array(&mut self, array: Arc<dyn Array>) -> Result<()> {
        let buf_opt = if array.data_type().is_nested() {
            array.nulls().map(arrow_buffer::NullBuffer::inner)
        } else if matches!(array.data_type(), arrow_schema::DataType::Boolean) {
            Some(array.as_boolean().values())
        } else {
            return Err(Error::invalid_arg(
                "array",
                format!("unexpected array type {:?}", array.data_type()),
            ));
        };

        if let Some(buf) = buf_opt {
            self.presence.append_buffer(buf.clone())?;
        } else {
            self.presence.append_repeated(array.len(), true)?;
        }
        Ok(())
    }

    fn push_nulls(&mut self, count: usize) -> Result<()> {
        self.presence.append_repeated(count, false)?;
        Ok(())
    }

    fn finish(self: Box<Self>) -> Result<EncodedField> {
        Ok(EncodedField { buffers: vec![] })
    }
}

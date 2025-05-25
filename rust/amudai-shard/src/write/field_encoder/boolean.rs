//! Boolean buffer and field encoders, essentially bitmap builders.

use std::sync::Arc;

use amudai_common::Result;
use amudai_format::defs::schema_ext::BasicTypeDescriptor;
use arrow_array::Array;
use arrow_buffer::BooleanBuffer;

use super::{EncodedField, FieldEncoderOps};

pub struct BooleanFieldEncoder {
    _basic_type: BasicTypeDescriptor,
}

impl BooleanFieldEncoder {
    /// Creates a `BooleanFieldEncoder`.
    ///
    /// # Arguments
    ///
    /// * `basic_type`: The basic type descriptor.
    pub fn create(basic_type: BasicTypeDescriptor) -> Result<Box<dyn FieldEncoderOps>> {
        Ok(Box::new(BooleanFieldEncoder {
            _basic_type: basic_type,
        }))
    }
}

impl FieldEncoderOps for BooleanFieldEncoder {
    fn push_array(&mut self, _array: Arc<dyn Array>) -> Result<()> {
        todo!()
    }

    fn push_nulls(&mut self, _count: usize) -> Result<()> {
        todo!()
    }

    fn finish(self: Box<Self>) -> Result<EncodedField> {
        todo!()
    }
}

pub struct BooleanBufferEncoder {}

impl BooleanBufferEncoder {
    pub fn new() -> BooleanBufferEncoder {
        BooleanBufferEncoder {}
    }

    pub fn append_buffer(&mut self, _buf: BooleanBuffer) -> Result<()> {
        Ok(())
    }

    pub fn append_repeated(&mut self, _count: usize, _value: bool) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use arrow_buffer::BooleanBufferBuilder;

    #[test]
    fn test_bool_buffer() {
        let mut builder = BooleanBufferBuilder::new(0);
        builder.append_n(10, true);
        builder.append_n(20, false);
        builder.append_n(15, true);
        let buf = builder.finish();
        dbg!(&buf);
        let slices = buf.set_slices().collect::<Vec<_>>();
        dbg!(&slices);
    }
}

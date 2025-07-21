use super::FieldEncoderOps;

pub struct EmptyFieldEncoder;

impl EmptyFieldEncoder {
    pub(crate) fn create() -> Box<dyn FieldEncoderOps> {
        Box::new(EmptyFieldEncoder)
    }
}

impl FieldEncoderOps for EmptyFieldEncoder {
    fn push_array(
        &mut self,
        _array: std::sync::Arc<dyn arrow_array::Array>,
    ) -> amudai_common::Result<()> {
        Ok(())
    }

    fn push_nulls(&mut self, _count: usize) -> amudai_common::Result<()> {
        Ok(())
    }

    fn finish(self: Box<Self>) -> amudai_common::Result<super::EncodedField> {
        Ok(super::EncodedField {
            buffers: vec![],
            statistics: super::EncodedFieldStatistics::Missing,
            dictionary_size: None,
            constant_value: None,
        })
    }
}
